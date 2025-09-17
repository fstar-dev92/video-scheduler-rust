//! SCTE-35 ad insertion handler implementation

use crate::error::{Error, Result};
use crate::pipeline::common::PipelineCommon;
use crate::types::{PipelineConfig, Scte35Message};
use gstreamer::prelude::*;
use gstreamer::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// SCTE-35 ad insertion handler
pub struct Scte35Handler {
    input_host: String,
    input_port: u16,
    output_host: String,
    output_port: u16,
    ad_source: String,
    main_pipeline: Arc<Mutex<Option<Pipeline>>>,
    ad_pipeline: Arc<Mutex<Option<Pipeline>>>,
    compositor: Arc<Mutex<Option<Element>>>,
    audiomixer: Arc<Mutex<Option<Element>>>,
    handler_id: String,
    current_ad_playing: Arc<Mutex<bool>>,
    ad_duration: Duration,
    verbose: bool,
}

impl Scte35Handler {
    /// Create a new SCTE-35 handler
    pub fn new(
        input_host: String,
        input_port: u16,
        output_host: String,
        output_port: u16,
        ad_source: String,
        verbose: bool,
    ) -> Self {
        Self {
            input_host,
            input_port,
            output_host,
            output_port,
            ad_source,
            main_pipeline: Arc::new(Mutex::new(None)),
            ad_pipeline: Arc::new(Mutex::new(None)),
            compositor: Arc::new(Mutex::new(None)),
            audiomixer: Arc::new(Mutex::new(None)),
            handler_id: Uuid::new_v4().to_string(),
            current_ad_playing: Arc::new(Mutex::new(false)),
            ad_duration: Duration::from_secs(30), // Default 30 seconds
            verbose,
        }
    }

    /// Start the SCTE-35 handler
    pub async fn start(&mut self) -> Result<()> {
        info!("[{}] Starting SCTE-35 handler", self.handler_id);

        // Create main pipeline
        let main_pipeline = self.create_main_pipeline().await?;
        {
            let mut pipeline_guard = self.main_pipeline.lock().await;
            *pipeline_guard = Some(main_pipeline.clone());
        }

        // Create dummy source pipeline
        let dummy_pipeline = self.create_dummy_source_pipeline().await?;

        // Start dummy source pipeline first
        PipelineCommon::set_element_state(&dummy_pipeline, State::Playing)?;

        // Wait a moment for dummy source to be ready
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Start main pipeline
        PipelineCommon::set_element_state(&main_pipeline, State::Playing)?;

        // Start monitoring
        self.start_monitoring().await;

        info!("[{}] SCTE-35 handler started successfully", self.handler_id);
        Ok(())
    }

    /// Stop the SCTE-35 handler
    pub async fn stop(&mut self) -> Result<()> {
        info!("[{}] Stopping SCTE-35 handler", self.handler_id);

        // Stop main pipeline
        {
            let mut pipeline_guard = self.main_pipeline.lock().await;
            if let Some(pipeline) = pipeline_guard.take() {
                PipelineCommon::set_element_state(&pipeline, State::Null)?;
            }
        }

        // Stop ad pipeline
        {
            let mut pipeline_guard = self.ad_pipeline.lock().await;
            if let Some(pipeline) = pipeline_guard.take() {
                PipelineCommon::set_element_state(&pipeline, State::Null)?;
            }
        }

        info!("[{}] SCTE-35 handler stopped", self.handler_id);
        Ok(())
    }

    /// Create the main pipeline with compositor and audiomixer
    async fn create_main_pipeline(&self) -> Result<Pipeline> {
        let pipeline = Pipeline::with_name(&format!("main-pipeline-{}", self.handler_id))?;

        // Create UDP source for input RTP stream
        let udp_src = PipelineCommon::create_element("udpsrc", &format!("udpsrc_{}", self.handler_id), None)?;
        udp_src.set_property("address", &self.input_host);
        udp_src.set_property("port", self.input_port);
        udp_src.set_property("buffer-size", 524_288i32);
        udp_src.set_property("auto-multicast", true);

        // Create RTP caps filter
        let rtp_caps_filter = PipelineCommon::create_capsfilter(&format!("rtpCapsFilter_{}", self.handler_id), "application/x-rtp")?;

        // Create RTP jitter buffer
        let rtp_jitter_buffer = PipelineCommon::create_element("rtpjitterbuffer", &format!("rtpjitterbuffer_{}", self.handler_id), None)?;
        rtp_jitter_buffer.set_property("latency", 200u32);

        // Create RTP MPEG-TS depayloader
        let rtp_mp2t_depay = PipelineCommon::create_element("rtpmp2tdepay", &format!("rtpmp2tdepay_{}", self.handler_id), None)?;

        // Create demuxer
        let demux = PipelineCommon::create_element("tsdemux", &format!("tsdemux_{}", self.handler_id), None)?;

        // Create inter elements for main stream (input1)
        let inter_video_sink1 = PipelineCommon::create_intervideosink(&format!("intervideosink1_{}", self.handler_id), &format!("input1{}", self.handler_id))?;
        let inter_audio_sink1 = PipelineCommon::create_interaudiosink(&format!("interaudiosink1_{}", self.handler_id), &format!("audio1{}", self.handler_id))?;
        let inter_video_src1 = PipelineCommon::create_intervideosrc(&format!("intervideosrc1_{}", self.handler_id), &format!("input1{}", self.handler_id))?;
        let inter_audio_src1 = PipelineCommon::create_interaudiosrc(&format!("interaudiosrc1_{}", self.handler_id), &format!("audio1{}", self.handler_id))?;

        // Create inter elements for ad stream (input2)
        let inter_video_src2 = PipelineCommon::create_intervideosrc(&format!("intervideosrc2_{}", self.handler_id), &format!("input2{}", self.handler_id))?;
        let inter_audio_src2 = PipelineCommon::create_interaudiosrc(&format!("interaudiosrc2_{}", self.handler_id), &format!("audio2{}", self.handler_id))?;

        // Create compositor
        let compositor = PipelineCommon::create_element("compositor", &format!("compositor_{}", self.handler_id), None)?;
        compositor.set_property("background", 1i32); // black background
        compositor.set_property("zero-size-is-unscaled", true);

        // Configure compositor sink pads
        if let Some(pad1) = compositor.static_pad("sink_0") {
            pad1.set_property("xpos", 0i32);
            pad1.set_property("ypos", 0i32);
            pad1.set_property("width", 1920i32);
            pad1.set_property("height", 1080i32);
            pad1.set_property("alpha", 1.0f64); // input1 visible
        }

        if let Some(pad2) = compositor.static_pad("sink_1") {
            pad2.set_property("xpos", 0i32);
            pad2.set_property("ypos", 0i32);
            pad2.set_property("width", 1920i32);
            pad2.set_property("height", 1080i32);
            pad2.set_property("alpha", 0.0f64); // input2 hidden
        }

        // Create audio mixer
        let audiomixer = PipelineCommon::create_audiomixer(&format!("audiomixer_{}", self.handler_id))?;

        // Store compositor and audiomixer for later use
        {
            let mut compositor_guard = self.compositor.lock().await;
            *compositor_guard = Some(compositor.clone());
        }

        {
            let mut audiomixer_guard = self.audiomixer.lock().await;
            *audiomixer_guard = Some(audiomixer.clone());
        }

        // Create video processing elements
        let video_queue1 = PipelineCommon::create_video_queue(&format!("videoQueue1_{}", self.handler_id))?;
        let video_queue2 = PipelineCommon::create_video_queue(&format!("videoQueue2_{}", self.handler_id))?;
        let h264_parse1 = PipelineCommon::create_element("h264parse", &format!("h264parse1_{}", self.handler_id), None)?;
        let avdec_h264 = PipelineCommon::create_element("avdec_h264", &format!("avdec_h264_{}", self.handler_id), None)?;
        let video_convert = PipelineCommon::create_element("videoconvert", &format!("videoconvert_{}", self.handler_id), None)?;
        let h264_enc = PipelineCommon::create_x264_encoder(&format!("h264enc_{}", self.handler_id))?;
        let h264_parse2 = PipelineCommon::create_element("h264parse", &format!("h264parse2_{}", self.handler_id), None)?;

        // Create audio processing elements
        let audio_queue1 = PipelineCommon::create_audio_queue(&format!("audioQueue1_{}", self.handler_id))?;
        let audio_queue2 = PipelineCommon::create_audio_queue(&format!("audioQueue2_{}", self.handler_id))?;
        let aac_parse1 = PipelineCommon::create_element("aacparse", &format!("aacparse1_{}", self.handler_id), None)?;
        let avdec_aac = PipelineCommon::create_element("avdec_aac", &format!("avdec_aac_{}", self.handler_id), None)?;
        let audio_convert = PipelineCommon::create_element("audioconvert", &format!("audioconvert_{}", self.handler_id), None)?;
        let aac_enc = PipelineCommon::create_aac_encoder(&format!("voaacenc_{}", self.handler_id))?;
        let aac_parse2 = PipelineCommon::create_element("aacparse", &format!("aacparse2_{}", self.handler_id), None)?;

        // Create muxer and output elements
        let mpegts_mux = PipelineCommon::create_mpegts_muxer(&format!("mpegtsmux_{}", self.handler_id))?;
        let rtp_mp2t_pay = PipelineCommon::create_rtp_mp2t_payloader(&format!("rtpmp2tpay_{}", self.handler_id))?;
        let udp_sink = PipelineCommon::create_udp_sink(&format!("udpsink_{}", self.handler_id), &self.output_host, self.output_port)?;

        // Add all elements to pipeline
        let elements = vec![
            &udp_src, &rtp_caps_filter, &rtp_jitter_buffer, &rtp_mp2t_depay, &demux,
            &inter_video_sink1, &inter_audio_sink1,
            &inter_video_src1, &inter_video_src2, &compositor,
            &inter_audio_src1, &inter_audio_src2, &audiomixer,
            &video_queue1, &video_queue2, &h264_parse1, &avdec_h264, &video_convert, &h264_enc, &h264_parse2,
            &audio_queue1, &audio_queue2, &aac_parse1, &avdec_aac, &audio_convert, &aac_enc, &aac_parse2,
            &mpegts_mux, &rtp_mp2t_pay, &udp_sink,
        ];

        for element in elements {
            pipeline.add(element)?;
        }

        // Link elements
        PipelineCommon::link_elements(&udp_src, &rtp_caps_filter)?;
        PipelineCommon::link_elements(&rtp_caps_filter, &rtp_jitter_buffer)?;
        PipelineCommon::link_elements(&rtp_jitter_buffer, &rtp_mp2t_depay)?;
        PipelineCommon::link_elements(&rtp_mp2t_depay, &demux)?;

        // Link video elements
        PipelineCommon::link_elements(&inter_video_src1, &video_queue1)?;
        PipelineCommon::link_elements(&video_queue1, &compositor)?;
        PipelineCommon::link_elements(&inter_video_src2, &video_queue2)?;
        PipelineCommon::link_elements(&video_queue2, &compositor)?;
        PipelineCommon::link_elements(&compositor, &h264_parse1)?;
        PipelineCommon::link_elements(&h264_parse1, &avdec_h264)?;
        PipelineCommon::link_elements(&avdec_h264, &video_convert)?;
        PipelineCommon::link_elements(&video_convert, &h264_enc)?;
        PipelineCommon::link_elements(&h264_enc, &h264_parse2)?;
        PipelineCommon::link_elements(&h264_parse2, &mpegts_mux)?;

        // Link audio elements
        PipelineCommon::link_elements(&inter_audio_src1, &audio_queue1)?;
        PipelineCommon::link_elements(&audio_queue1, &audiomixer)?;
        PipelineCommon::link_elements(&inter_audio_src2, &audio_queue2)?;
        PipelineCommon::link_elements(&audio_queue2, &audiomixer)?;
        PipelineCommon::link_elements(&audiomixer, &aac_parse1)?;
        PipelineCommon::link_elements(&aac_parse1, &avdec_aac)?;
        PipelineCommon::link_elements(&avdec_aac, &audio_convert)?;
        PipelineCommon::link_elements(&audio_convert, &aac_enc)?;
        PipelineCommon::link_elements(&aac_enc, &aac_parse2)?;
        PipelineCommon::link_elements(&aac_parse2, &mpegts_mux)?;

        // Link output elements
        PipelineCommon::link_elements(&mpegts_mux, &rtp_mp2t_pay)?;
        PipelineCommon::link_elements(&rtp_mp2t_pay, &udp_sink)?;

        // Set up dynamic pad handling
        self.setup_demuxer_pad_handling(&demux, &inter_video_sink1, &inter_audio_sink1).await?;

        // Set up bus message handling
        self.setup_bus_handler(&pipeline).await;

        info!("[{}] Main pipeline created successfully", self.handler_id);
        Ok(pipeline)
    }

    /// Create dummy source pipeline
    async fn create_dummy_source_pipeline(&self) -> Result<Pipeline> {
        let pipeline = Pipeline::with_name(&format!("dummy-source-pipeline-{}", self.handler_id))?;

        // Create dummy video source (black video)
        let video_src = PipelineCommon::create_element("videotestsrc", &format!("dummyVideoSrc_{}", self.handler_id), None)?;
        video_src.set_property("pattern", 0i32); // black
        video_src.set_property("is-live", true);

        // Create dummy audio source (silence)
        let audio_src = PipelineCommon::create_element("audiotestsrc", &format!("dummyAudioSrc_{}", self.handler_id), None)?;
        audio_src.set_property("wave", 0i32); // silence
        audio_src.set_property("is-live", true);

        // Create intervideosink
        let inter_video_sink = PipelineCommon::create_intervideosink(&format!("dummyInterVideoSink_{}", self.handler_id), &format!("input1{}", self.handler_id))?;

        // Create interaudiosink
        let inter_audio_sink = PipelineCommon::create_interaudiosink(&format!("dummyInterAudioSink_{}", self.handler_id), &format!("audio1{}", self.handler_id))?;

        // Create video converter and capsfilter
        let video_conv = PipelineCommon::create_element("videoconvert", &format!("dummyVideoConv_{}", self.handler_id), None)?;
        let video_caps = PipelineCommon::create_capsfilter(&format!("dummyVideoCaps_{}", self.handler_id), "video/x-raw, width=1920, height=1080, framerate=30/1")?;

        // Create audio converter and capsfilter
        let audio_conv = PipelineCommon::create_element("audioconvert", &format!("dummyAudioConv_{}", self.handler_id), None)?;
        let audio_caps = PipelineCommon::create_capsfilter(&format!("dummyAudioCaps_{}", self.handler_id), "audio/x-raw, format=S16LE, layout=interleaved, rate=48000, channels=2")?;

        // Add all elements to pipeline
        pipeline.add(&video_src)?;
        pipeline.add(&video_conv)?;
        pipeline.add(&video_caps)?;
        pipeline.add(&inter_video_sink)?;
        pipeline.add(&audio_src)?;
        pipeline.add(&audio_conv)?;
        pipeline.add(&audio_caps)?;
        pipeline.add(&inter_audio_sink)?;

        // Link video elements
        PipelineCommon::link_elements(&video_src, &video_conv)?;
        PipelineCommon::link_elements(&video_conv, &video_caps)?;
        PipelineCommon::link_elements(&video_caps, &inter_video_sink)?;

        // Link audio elements
        PipelineCommon::link_elements(&audio_src, &audio_conv)?;
        PipelineCommon::link_elements(&audio_conv, &audio_caps)?;
        PipelineCommon::link_elements(&audio_caps, &inter_audio_sink)?;

        info!("[{}] Dummy source pipeline created successfully", self.handler_id);
        Ok(pipeline)
    }

    /// Set up demuxer pad handling
    async fn setup_demuxer_pad_handling(
        &self,
        demux: &Element,
        inter_video_sink: &Element,
        inter_audio_sink: &Element,
    ) -> Result<()> {
        let handler_id = self.handler_id.clone();
        
        demux.connect_pad_added(move |_, pad| {
            debug!("[{}] Demuxer pad added: {}", handler_id, pad.name());
            
            let pad_name = pad.name();
            if pad_name.starts_with("video") {
                debug!("[{}] Linking video pad to intervideosink", handler_id);
                
                if let Some(sink_pad) = inter_video_sink.static_pad("sink") {
                    if let Err(e) = pad.link(&sink_pad) {
                        error!("[{}] Failed to link video pad: {}", handler_id, e);
                    } else {
                        debug!("[{}] Successfully linked video pad to intervideosink", handler_id);
                    }
                }
            } else if pad_name.starts_with("audio") {
                debug!("[{}] Linking audio pad to interaudiosink", handler_id);
                
                if let Some(sink_pad) = inter_audio_sink.static_pad("sink") {
                    if let Err(e) = pad.link(&sink_pad) {
                        error!("[{}] Failed to link audio pad: {}", handler_id, e);
                    } else {
                        debug!("[{}] Successfully linked audio pad to interaudiosink", handler_id);
                    }
                }
            } else {
                debug!("[{}] Unknown demuxer pad: {}", handler_id, pad_name);
            }
        });

        Ok(())
    }

    /// Set up bus message handling
    async fn setup_bus_handler(&self, pipeline: &Pipeline) {
        let handler_id = self.handler_id.clone();
        
        PipelineCommon::setup_bus_handler(
            pipeline,
            &handler_id,
            move |error_msg| {
                error!("[{}] Pipeline error: {}", handler_id, error_msg);
            },
            move |warning_msg| {
                warn!("[{}] Pipeline warning: {}", handler_id, warning_msg);
            },
            move |info_msg| {
                info!("[{}] Pipeline info: {}", handler_id, info_msg);
            },
        );

        // Handle SCTE-35 events
        let bus = pipeline.bus().unwrap();
        bus.add_signal_watch();
        bus.connect_message(move |_, msg| {
            match msg.view() {
                MessageView::Element(element_msg) => {
                    if let Some(structure) = element_msg.structure() {
                        if structure.name() == "scte35" {
                            info!("[{}] Received SCTE-35 event!", handler_id);
                            
                            // Parse SCTE-35 message
                            if let Some(scte35_msg) = Self::parse_scte35_message(&structure) {
                                info!("[{}] SCTE-35 message parsed: CommandType={}, SpliceTime={}", 
                                    handler_id, scte35_msg.splice_command_type, scte35_msg.splice_time);
                                
                                // Trigger ad insertion
                                if scte35_msg.is_immediate_splice_insert() {
                                    info!("[{}] Immediate SCTE-35 splice insert detected, triggering ad insertion", handler_id);
                                    // TODO: Trigger ad insertion
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        });
    }

    /// Parse SCTE-35 message from GStreamer structure
    fn parse_scte35_message(structure: &StructureRef) -> Option<Scte35Message> {
        let mut msg = Scte35Message::new(0, 0);
        
        if let Some(table_id) = structure.get::<u8>("table-id") {
            msg.table_id = table_id;
        }
        
        if let Some(section_length) = structure.get::<u16>("section-length") {
            msg.section_length = section_length;
        }
        
        if let Some(splice_command_type) = structure.get::<u8>("splice-command-type") {
            msg.splice_command_type = splice_command_type;
        }
        
        if let Some(pts_adjustment) = structure.get::<u64>("pts-adjustment") {
            msg.pts_adjustment = pts_adjustment;
        }
        
        if let Some(splice_time) = structure.get::<u64>("splice-time") {
            msg.splice_time = splice_time;
        } else if let Some(splice_time) = structure.get::<u64>("pts_time") {
            msg.splice_time = splice_time;
        } else if let Some(splice_time) = structure.get::<u64>("pts-time") {
            msg.splice_time = splice_time;
        }
        
        Some(msg)
    }

    /// Start monitoring tasks
    async fn start_monitoring(&self) {
        let handler_id = self.handler_id.clone();
        
        // Basic monitoring - could be extended with more sophisticated health checks
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                debug!("[{}] Monitoring pipeline health", handler_id);
                // TODO: Add health checks
            }
        });
    }

    /// Insert ad
    pub async fn insert_ad(&self) -> Result<()> {
        info!("[{}] Starting ad insertion", self.handler_id);

        let mut current_ad_playing = self.current_ad_playing.lock().await;
        if *current_ad_playing {
            info!("[{}] Ad already playing, ignoring insert request", self.handler_id);
            return Ok(());
        }

        // Create and start ad pipeline
        let ad_pipeline = self.create_ad_pipeline().await?;
        
        {
            let mut ad_pipeline_guard = self.ad_pipeline.lock().await;
            *ad_pipeline_guard = Some(ad_pipeline.clone());
        }

        // Start ad pipeline
        PipelineCommon::set_element_state(&ad_pipeline, State::Playing)?;

        *current_ad_playing = true;

        // Switch compositor to show ad (input2)
        {
            let compositor_guard = self.compositor.lock().await;
            if let Some(compositor) = compositor_guard.as_ref() {
                if let Some(pad1) = compositor.static_pad("sink_0") {
                    pad1.set_property("alpha", 0.0f64); // Hide input1 (main stream)
                }
                if let Some(pad2) = compositor.static_pad("sink_1") {
                    pad2.set_property("alpha", 1.0f64); // Show input2 (ad)
                }
            }
        }

        // Set up timer to stop ad after duration
        let handler_id = self.handler_id.clone();
        let ad_pipeline = self.ad_pipeline.clone();
        let current_ad_playing = self.current_ad_playing.clone();
        let compositor = self.compositor.clone();
        
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await; // Default 30 seconds
            
            info!("[{}] Stopping ad after duration", handler_id);
            
            // Stop ad pipeline
            {
                let mut ad_pipeline_guard = ad_pipeline.lock().await;
                if let Some(pipeline) = ad_pipeline_guard.take() {
                    let _ = PipelineCommon::set_element_state(&pipeline, State::Null);
                }
            }

            // Update state
            {
                let mut playing_guard = current_ad_playing.lock().await;
                *playing_guard = false;
            }

            // Switch compositor back to main stream (input1)
            {
                let compositor_guard = compositor.lock().await;
                if let Some(compositor) = compositor_guard.as_ref() {
                    if let Some(pad1) = compositor.static_pad("sink_0") {
                        pad1.set_property("alpha", 1.0f64); // Show input1 (main stream)
                    }
                    if let Some(pad2) = compositor.static_pad("sink_1") {
                        pad2.set_property("alpha", 0.0f64); // Hide input2 (ad)
                    }
                }
            }
        });

        info!("[{}] Ad insertion started", self.handler_id);
        Ok(())
    }

    /// Create ad pipeline
    async fn create_ad_pipeline(&self) -> Result<Pipeline> {
        let pipeline = Pipeline::with_name(&format!("ad-pipeline-{}", self.handler_id))?;

        // Create playbin for ad file
        let playbin = PipelineCommon::create_element("playbin3", &format!("playbin_{}", self.handler_id), None)?;
        playbin.set_property("uri", &format!("file://{}", self.ad_source));

        // Create video sink
        let inter_video_sink = PipelineCommon::create_intervideosink(&format!("adInterVideoSink_{}", self.handler_id), &format!("input2{}", self.handler_id))?;

        // Create audio sink
        let inter_audio_sink = PipelineCommon::create_interaudiosink(&format!("adInterAudioSink_{}", self.handler_id), &format!("audio2{}", self.handler_id))?;

        // Set sinks on playbin
        playbin.set_property("video-sink", &inter_video_sink);
        playbin.set_property("audio-sink", &inter_audio_sink);

        // Add playbin to pipeline
        pipeline.add(&playbin)?;

        // Set up bus watch
        self.setup_ad_bus_handler(&pipeline).await;

        info!("[{}] Ad pipeline created successfully", self.handler_id);
        Ok(pipeline)
    }

    /// Set up bus message handling for ad pipeline
    async fn setup_ad_bus_handler(&self, pipeline: &Pipeline) {
        let handler_id = self.handler_id.clone();
        
        PipelineCommon::setup_bus_handler(
            pipeline,
            &handler_id,
            move |error_msg| {
                error!("[{}] Ad pipeline error: {}", handler_id, error_msg);
            },
            move |warning_msg| {
                warn!("[{}] Ad pipeline warning: {}", handler_id, warning_msg);
            },
            move |info_msg| {
                info!("[{}] Ad pipeline info: {}", handler_id, info_msg);
            },
        );

        // Handle EOS message
        let bus = pipeline.bus().unwrap();
        bus.add_signal_watch();
        bus.connect_message(move |_, msg| {
            match msg.view() {
                MessageView::Eos(_) => {
                    info!("[{}] Ad finished", handler_id);
                    // TODO: Stop ad
                }
                _ => {}
            }
        });
    }

    /// Set ad source file
    pub fn set_ad_source(&mut self, source: String) {
        self.ad_source = source;
    }

    /// Set ad duration
    pub fn set_ad_duration(&mut self, duration: Duration) {
        self.ad_duration = duration;
    }
}

//! Stream scheduler implementation for managing multiple video assets

use crate::error::{Error, Result};
use crate::pipeline::common::PipelineCommon;
use crate::types::{PipelineConfig, PipelineState};
use gstreamer::prelude::*;
use gstreamer::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Stream item representing a video asset to be scheduled
#[derive(Debug, Clone)]
pub struct StreamItem {
    pub path: String,
    pub duration: Duration,
    pub start_time: Option<Duration>,
}

/// Stream scheduler for managing multiple video assets
pub struct StreamScheduler {
    pipeline: Arc<Mutex<Option<Pipeline>>>,
    source_pipelines: Arc<Mutex<Vec<Pipeline>>>,
    config: PipelineConfig,
    scheduler_id: String,
    state: Arc<Mutex<PipelineState>>,
    stream_items: Arc<Mutex<Vec<StreamItem>>>,
    current_item_index: Arc<Mutex<usize>>,
    stop_tx: tokio::sync::mpsc::UnboundedSender<()>,
    stop_rx: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<()>>>>,
}

impl StreamScheduler {
    /// Create a new stream scheduler
    pub fn new(config: PipelineConfig) -> Result<Self> {
        let scheduler_id = format!("scheduler_{}", Uuid::new_v4());
        let (stop_tx, stop_rx) = tokio::sync::mpsc::unbounded_channel();

        Ok(Self {
            pipeline: Arc::new(Mutex::new(None)),
            source_pipelines: Arc::new(Mutex::new(Vec::new())),
            config,
            scheduler_id,
            state: Arc::new(Mutex::new(PipelineState::Stopped)),
            stream_items: Arc::new(Mutex::new(Vec::new())),
            current_item_index: Arc::new(Mutex::new(0)),
            stop_tx,
            stop_rx: Arc::new(Mutex::new(Some(stop_rx))),
        })
    }

    /// Add a stream item to the schedule
    pub async fn add_stream_item(&self, item: StreamItem) -> Result<()> {
        info!("[{}] Adding stream item: {}", self.scheduler_id, item.path);
        
        let mut items_guard = self.stream_items.lock().await;
        items_guard.push(item);
        
        info!("[{}] Stream item added. Total items: {}", self.scheduler_id, items_guard.len());
        Ok(())
    }

    /// Start the stream scheduler
    pub async fn start(&mut self) -> Result<()> {
        info!("[{}] Starting stream scheduler", self.scheduler_id);

        // Create the main pipeline
        let pipeline = self.create_main_pipeline().await?;
        
        // Store the pipeline
        {
            let mut pipeline_guard = self.pipeline.lock().await;
            *pipeline_guard = Some(pipeline.clone());
        }

        // Set state to starting
        {
            let mut state_guard = self.state.lock().await;
            *state_guard = PipelineState::Starting;
        }

        // Start the pipeline
        PipelineCommon::set_element_state(&pipeline, State::Playing)?;

        // Set state to running
        {
            let mut state_guard = self.state.lock().await;
            *state_guard = PipelineState::Running;
        }

        // Start scheduling loop
        self.start_scheduling_loop().await;

        info!("[{}] Stream scheduler started successfully", self.scheduler_id);
        Ok(())
    }

    /// Stop the stream scheduler
    pub async fn stop(&mut self) -> Result<()> {
        info!("[{}] Stopping stream scheduler", self.scheduler_id);

        // Set state to stopping
        {
            let mut state_guard = self.state.lock().await;
            *state_guard = PipelineState::Stopping;
        }

        // Stop all source pipelines
        {
            let mut source_pipelines_guard = self.source_pipelines.lock().await;
            for pipeline in source_pipelines_guard.drain(..) {
                let _ = PipelineCommon::set_element_state(&pipeline, State::Null);
            }
        }

        // Stop main pipeline
        {
            let mut pipeline_guard = self.pipeline.lock().await;
            if let Some(pipeline) = pipeline_guard.take() {
                PipelineCommon::set_element_state(&pipeline, State::Null)?;
            }
        }

        // Send stop signal
        let _ = self.stop_tx.send(());

        // Set state to stopped
        {
            let mut state_guard = self.state.lock().await;
            *state_guard = PipelineState::Stopped;
        }

        info!("[{}] Stream scheduler stopped", self.scheduler_id);
        Ok(())
    }

    /// Create the main pipeline with compositor
    async fn create_main_pipeline(&self) -> Result<Pipeline> {
        let pipeline = Pipeline::with_name(&format!("scheduler-pipeline-{}", self.scheduler_id))?;

        // Create inter elements for input1
        let inter_video_src1 = PipelineCommon::create_intervideosrc(&format!("intervideosrc1_{}", self.scheduler_id), &format!("input1{}", self.scheduler_id))?;
        let inter_audio_src1 = PipelineCommon::create_interaudiosrc(&format!("interaudiosrc1_{}", self.scheduler_id), &format!("audio1{}", self.scheduler_id))?;

        // Create inter elements for input2
        let inter_video_src2 = PipelineCommon::create_intervideosrc(&format!("intervideosrc2_{}", self.scheduler_id), &format!("input2{}", self.scheduler_id))?;
        let inter_audio_src2 = PipelineCommon::create_interaudiosrc(&format!("interaudiosrc2_{}", self.scheduler_id), &format!("audio2{}", self.scheduler_id))?;

        // Create compositor
        let compositor = PipelineCommon::create_element("compositor", &format!("compositor_{}", self.scheduler_id), None)?;
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
        let audiomixer = PipelineCommon::create_audiomixer(&format!("audiomixer_{}", self.scheduler_id))?;

        // Create video processing elements
        let video_queue1 = PipelineCommon::create_video_queue(&format!("videoQueue1_{}", self.scheduler_id))?;
        let video_queue2 = PipelineCommon::create_video_queue(&format!("videoQueue2_{}", self.scheduler_id))?;
        let h264_parse1 = PipelineCommon::create_element("h264parse", &format!("h264parse1_{}", self.scheduler_id), None)?;
        let avdec_h264 = PipelineCommon::create_element("avdec_h264", &format!("avdec_h264_{}", self.scheduler_id), None)?;
        let video_convert = PipelineCommon::create_element("videoconvert", &format!("videoconvert_{}", self.scheduler_id), None)?;
        let h264_enc = PipelineCommon::create_x264_encoder(&format!("h264enc_{}", self.scheduler_id))?;
        let h264_parse2 = PipelineCommon::create_element("h264parse", &format!("h264parse2_{}", self.scheduler_id), None)?;

        // Create audio processing elements
        let audio_queue1 = PipelineCommon::create_audio_queue(&format!("audioQueue1_{}", self.scheduler_id))?;
        let audio_queue2 = PipelineCommon::create_audio_queue(&format!("audioQueue2_{}", self.scheduler_id))?;
        let aac_parse1 = PipelineCommon::create_element("aacparse", &format!("aacparse1_{}", self.scheduler_id), None)?;
        let avdec_aac = PipelineCommon::create_element("avdec_aac", &format!("avdec_aac_{}", self.scheduler_id), None)?;
        let audio_convert = PipelineCommon::create_element("audioconvert", &format!("audioconvert_{}", self.scheduler_id), None)?;
        let aac_enc = PipelineCommon::create_aac_encoder(&format!("voaacenc_{}", self.scheduler_id))?;
        let aac_parse2 = PipelineCommon::create_element("aacparse", &format!("aacparse2_{}", self.scheduler_id), None)?;

        // Create muxer and output elements
        let mpegts_mux = PipelineCommon::create_mpegts_muxer(&format!("mpegtsmux_{}", self.scheduler_id))?;
        let rtp_mp2t_pay = PipelineCommon::create_rtp_mp2t_payloader(&format!("rtpmp2tpay_{}", self.scheduler_id))?;
        let udp_sink = PipelineCommon::create_udp_sink(&format!("udpsink_{}", self.scheduler_id), &self.config.output_host, self.config.output_port)?;

        // Add all elements to pipeline
        let elements = vec![
            &inter_video_src1, &inter_video_src2, &compositor,
            &inter_audio_src1, &inter_audio_src2, &audiomixer,
            &video_queue1, &video_queue2, &h264_parse1, &avdec_h264, &video_convert, &h264_enc, &h264_parse2,
            &audio_queue1, &audio_queue2, &aac_parse1, &avdec_aac, &audio_convert, &aac_enc, &aac_parse2,
            &mpegts_mux, &rtp_mp2t_pay, &udp_sink,
        ];

        for element in elements {
            pipeline.add(element)?;
        }

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

        // Set up bus message handling
        self.setup_bus_handler(&pipeline).await;

        info!("[{}] Main pipeline created successfully", self.scheduler_id);
        Ok(pipeline)
    }

    /// Set up bus message handling
    async fn setup_bus_handler(&self, pipeline: &Pipeline) {
        let scheduler_id = self.scheduler_id.clone();
        
        PipelineCommon::setup_bus_handler(
            pipeline,
            &scheduler_id,
            move |error_msg| {
                error!("[{}] Pipeline error: {}", scheduler_id, error_msg);
            },
            move |warning_msg| {
                warn!("[{}] Pipeline warning: {}", scheduler_id, warning_msg);
            },
            move |info_msg| {
                info!("[{}] Pipeline info: {}", scheduler_id, info_msg);
            },
        );
    }

    /// Start the scheduling loop
    async fn start_scheduling_loop(&self) {
        let scheduler_id = self.scheduler_id.clone();
        let stream_items = self.stream_items.clone();
        let current_item_index = self.current_item_index.clone();
        let source_pipelines = self.source_pipelines.clone();
        let stop_rx = self.stop_rx.clone();

        tokio::spawn(async move {
            let mut current_index = 0;
            
            loop {
                // Check for stop signal
                if let Some(rx) = stop_rx.lock().await.as_mut() {
                    if rx.try_recv().is_ok() {
                        info!("[{}] Received stop signal, exiting scheduling loop", scheduler_id);
                        break;
                    }
                }

                // Get current stream items
                let items = stream_items.lock().await;
                if items.is_empty() {
                    debug!("[{}] No stream items available, waiting...", scheduler_id);
                    drop(items);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }

                let item = &items[current_index];
                info!("[{}] Starting stream item {}: {}", scheduler_id, current_index, item.path);

                // Create and start source pipeline for current item
                if let Ok(source_pipeline) = Self::create_source_pipeline(&scheduler_id, &item.path, current_index).await {
                    // Start the source pipeline
                    if let Err(e) = PipelineCommon::set_element_state(&source_pipeline, State::Playing) {
                        error!("[{}] Failed to start source pipeline: {}", scheduler_id, e);
                    } else {
                        // Add to source pipelines list
                        {
                            let mut pipelines_guard = source_pipelines.lock().await;
                            pipelines_guard.push(source_pipeline);
                        }

                        // Wait for item duration
                        info!("[{}] Playing item for {:?}", scheduler_id, item.duration);
                        sleep(item.duration).await;

                        // Stop the source pipeline
                        {
                            let mut pipelines_guard = source_pipelines.lock().await;
                            if let Some(pipeline) = pipelines_guard.pop() {
                                let _ = PipelineCommon::set_element_state(&pipeline, State::Null);
                            }
                        }

                        info!("[{}] Finished stream item {}", scheduler_id, current_index);
                    }
                } else {
                    error!("[{}] Failed to create source pipeline for item {}", scheduler_id, current_index);
                }

                // Move to next item
                current_index = (current_index + 1) % items.len();
                
                // Update current item index
                {
                    let mut index_guard = current_item_index.lock().await;
                    *index_guard = current_index;
                }
            }
        });
    }

    /// Create a source pipeline for a specific stream item
    async fn create_source_pipeline(scheduler_id: &str, path: &str, index: usize) -> Result<Pipeline> {
        let pipeline = Pipeline::with_name(&format!("source-pipeline-{}-{}", scheduler_id, index))?;

        // Create playbin for the video file
        let playbin = PipelineCommon::create_element("playbin3", &format!("playbin_{}_{}", scheduler_id, index), None)?;
        playbin.set_property("uri", &format!("file://{}", path));
        playbin.set_property("async", false);
        playbin.set_property("sync", true);
        playbin.set_property("buffer-size", 1_048_576i32);
        playbin.set_property("buffer-duration", 5_000_000_000i64);
        playbin.set_property("rate", 1.0f64);
        playbin.set_property("max-lateness", 20_000_000i64);
        playbin.set_property("qos", true);

        // Create video sink
        let inter_video_sink = PipelineCommon::create_intervideosink(&format!("sourceInterVideoSink_{}_{}", scheduler_id, index), &format!("input{}", (index % 2) + 1))?;

        // Create audio sink
        let inter_audio_sink = PipelineCommon::create_interaudiosink(&format!("sourceInterAudioSink_{}_{}", scheduler_id, index), &format!("audio{}", (index % 2) + 1))?;

        // Set sinks on playbin
        playbin.set_property("video-sink", &inter_video_sink);
        playbin.set_property("audio-sink", &inter_audio_sink);

        // Add playbin to pipeline
        pipeline.add(&playbin)?;

        // Set up bus watch
        Self::setup_source_bus_handler(&pipeline, scheduler_id, index).await;

        info!("[{}] Source pipeline {} created successfully", scheduler_id, index);
        Ok(pipeline)
    }

    /// Set up bus message handling for source pipeline
    async fn setup_source_bus_handler(pipeline: &Pipeline, scheduler_id: &str, index: usize) {
        PipelineCommon::setup_bus_handler(
            pipeline,
            scheduler_id,
            move |error_msg| {
                error!("[{}] Source pipeline {} error: {}", scheduler_id, index, error_msg);
            },
            move |warning_msg| {
                warn!("[{}] Source pipeline {} warning: {}", scheduler_id, index, warning_msg);
            },
            move |info_msg| {
                info!("[{}] Source pipeline {} info: {}", scheduler_id, index, info_msg);
            },
        );

        // Handle EOS message
        let bus = pipeline.bus().unwrap();
        bus.add_signal_watch();
        bus.connect_message(move |_, msg| {
            match msg.view() {
                MessageView::Eos(_) => {
                    info!("[{}] Source pipeline {} finished", scheduler_id, index);
                }
                _ => {}
            }
        });
    }

    /// Get current stream item index
    pub async fn get_current_item_index(&self) -> usize {
        let index_guard = self.current_item_index.lock().await;
        *index_guard
    }

    /// Get total number of stream items
    pub async fn get_stream_item_count(&self) -> usize {
        let items_guard = self.stream_items.lock().await;
        items_guard.len()
    }

    /// Clear all stream items
    pub async fn clear_stream_items(&self) -> Result<()> {
        info!("[{}] Clearing all stream items", self.scheduler_id);
        
        let mut items_guard = self.stream_items.lock().await;
        items_guard.clear();
        
        info!("[{}] All stream items cleared", self.scheduler_id);
        Ok(())
    }
}

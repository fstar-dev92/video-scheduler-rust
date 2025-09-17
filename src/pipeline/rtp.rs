//! RTP (Real-time Transport Protocol) pipeline implementation

use crate::error::{Error, Result};
use crate::pipeline::common::PipelineCommon;
use crate::types::{InputSource, PipelineConfig, PipelineState, Scte35Message};
use gstreamer::prelude::*;
use gstreamer::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// RTP GStreamer pipeline with video mixing and audio mixing capabilities
pub struct RtpPipeline {
    pipeline: Arc<Mutex<Option<Pipeline>>>,
    asset_pipeline: Arc<Mutex<Option<Pipeline>>>,
    config: PipelineConfig,
    pipeline_id: String,
    state: Arc<Mutex<PipelineState>>,
    current_input: Arc<Mutex<InputSource>>,
    asset_playing: Arc<Mutex<bool>>,
    rtp_connected: Arc<Mutex<bool>>,
    stop_tx: tokio::sync::mpsc::UnboundedSender<()>,
    stop_rx: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<()>>>>,
    current_pts: Arc<Mutex<u64>>,
    scte35_message: Arc<Mutex<Option<Scte35Message>>>,
}

impl RtpPipeline {
    /// Create a new RTP pipeline
    pub fn new(config: PipelineConfig) -> Result<Self> {
        let pipeline_id = format!("rtp_pipeline_{}", Uuid::new_v4());
        let (stop_tx, stop_rx) = tokio::sync::mpsc::unbounded_channel();

        Ok(Self {
            pipeline: Arc::new(Mutex::new(None)),
            asset_pipeline: Arc::new(Mutex::new(None)),
            config,
            pipeline_id,
            state: Arc::new(Mutex::new(PipelineState::Stopped)),
            current_input: Arc::new(Mutex::new(InputSource::Main)),
            asset_playing: Arc::new(Mutex::new(false)),
            rtp_connected: Arc::new(Mutex::new(false)),
            stop_tx,
            stop_rx: Arc::new(Mutex::new(Some(stop_rx))),
            current_pts: Arc::new(Mutex::new(0)),
            scte35_message: Arc::new(Mutex::new(None)),
        })
    }

    /// Start the RTP pipeline
    pub async fn start(&mut self) -> Result<()> {
        info!("[{}] Starting RTP pipeline", self.pipeline_id);

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

        // Start monitoring tasks
        self.start_monitoring_tasks().await;

        info!("[{}] RTP pipeline started successfully", self.pipeline_id);
        Ok(())
    }

    /// Stop the RTP pipeline
    pub async fn stop(&mut self) -> Result<()> {
        info!("[{}] Stopping RTP pipeline", self.pipeline_id);

        // Set state to stopping
        {
            let mut state_guard = self.state.lock().await;
            *state_guard = PipelineState::Stopping;
        }

        // Stop asset pipeline if running
        {
            let mut asset_pipeline_guard = self.asset_pipeline.lock().await;
            if let Some(asset_pipeline) = asset_pipeline_guard.take() {
                PipelineCommon::set_element_state(&asset_pipeline, State::Null)?;
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

        info!("[{}] RTP pipeline stopped", self.pipeline_id);
        Ok(())
    }

    /// Create the main RTP pipeline
    async fn create_main_pipeline(&self) -> Result<Pipeline> {
        let pipeline = Pipeline::with_name(&format!("rtp-pipeline-{}", self.pipeline_id))?;

        // Create UDP source
        let udp_src = PipelineCommon::create_element("udpsrc", &format!("udpsrc_{}", self.pipeline_id), None)?;
        udp_src.set_property("address", &self.config.input_host);
        udp_src.set_property("port", self.config.input_port);
        udp_src.set_property("caps", &Caps::from_string("application/x-rtp")?);
        udp_src.set_property("timeout", 5_000_000_000u64); // 5 seconds
        udp_src.set_property("auto-multicast", true);
        udp_src.set_property("buffer-size", 65_536i32);

        // Create RTP caps filter
        let rtp_caps_filter = PipelineCommon::create_capsfilter(&format!("rtpCapsFilter_{}", self.pipeline_id), "application/x-rtp")?;

        // Create RTP jitter buffer
        let rtp_jitter_buffer = PipelineCommon::create_element("rtpjitterbuffer", &format!("rtpjitterbuffer_{}", self.pipeline_id), None)?;
        rtp_jitter_buffer.set_property("latency", 200u32);

        // Create RTP MPEG-TS depayloader
        let rtp_mp2t_depay = PipelineCommon::create_element("rtpmp2tdepay", &format!("rtpmp2tdepay_{}", self.pipeline_id), None)?;
        rtp_mp2t_depay.set_property("pt", 33i32);
        rtp_mp2t_depay.set_property("mtu", 1400i32);
        rtp_mp2t_depay.set_property("perfect-rtptime", true);

        // Create TS demuxer
        let ts_demux = PipelineCommon::create_element("tsdemux", &format!("tsdemux_{}", self.pipeline_id), None)?;
        ts_demux.set_property("send-scte35-events", true);
        ts_demux.set_property("latency", 1000i32);

        // Add elements to pipeline
        pipeline.add(&udp_src)?;
        pipeline.add(&rtp_caps_filter)?;
        pipeline.add(&rtp_jitter_buffer)?;
        pipeline.add(&rtp_mp2t_depay)?;
        pipeline.add(&ts_demux)?;

        // Link elements
        PipelineCommon::link_elements(&udp_src, &rtp_caps_filter)?;
        PipelineCommon::link_elements(&rtp_caps_filter, &rtp_jitter_buffer)?;
        PipelineCommon::link_elements(&rtp_jitter_buffer, &rtp_mp2t_depay)?;
        PipelineCommon::link_elements(&rtp_mp2t_depay, &ts_demux)?;

        // Set up dynamic pad handling
        self.setup_demuxer_pad_handling(&ts_demux).await?;

        // Set up bus message handling
        self.setup_bus_handler(&pipeline).await;

        // Configure pipeline latency
        pipeline.set_property("latency", self.config.latency as i64);

        info!("[{}] RTP pipeline created successfully", self.pipeline_id);
        Ok(pipeline)
    }

    /// Set up demuxer pad handling
    async fn setup_demuxer_pad_handling(&self, ts_demux: &Element) -> Result<()> {
        let pipeline_id = self.pipeline_id.clone();
        let rtp_connected = self.rtp_connected.clone();
        
        ts_demux.connect_pad_added(move |_, pad| {
            debug!("[{}] TS demuxer pad added: {}", pipeline_id, pad.name());
            
            let pad_name = pad.name();
            if pad_name.starts_with("video") {
                debug!("[{}] Creating video pipeline for RTP stream", pipeline_id);
                
                // Mark RTP as connected
                tokio::spawn(async move {
                    let mut connected = rtp_connected.lock().await;
                    *connected = true;
                });
            } else if pad_name.starts_with("audio") {
                debug!("[{}] Creating audio pipeline for RTP stream", pipeline_id);
                
                // Mark RTP as connected
                tokio::spawn(async move {
                    let mut connected = rtp_connected.lock().await;
                    *connected = true;
                });
            } else {
                debug!("[{}] Unknown demuxer pad: {}", pipeline_id, pad_name);
            }
        });

        Ok(())
    }

    /// Set up bus message handling
    async fn setup_bus_handler(&self, pipeline: &Pipeline) {
        let pipeline_id = self.pipeline_id.clone();
        let scte35_message = self.scte35_message.clone();
        
        PipelineCommon::setup_bus_handler(
            pipeline,
            &pipeline_id,
            move |error_msg| {
                error!("[{}] Pipeline error: {}", pipeline_id, error_msg);
            },
            move |warning_msg| {
                warn!("[{}] Pipeline warning: {}", pipeline_id, warning_msg);
            },
            move |info_msg| {
                info!("[{}] Pipeline info: {}", pipeline_id, info_msg);
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
                            info!("[{}] Received SCTE-35 event!", pipeline_id);
                            
                            // Parse SCTE-35 message
                            if let Some(scte35_msg) = Self::parse_scte35_message(&structure) {
                                info!("[{}] SCTE-35 message parsed: CommandType={}, SpliceTime={}", 
                                    pipeline_id, scte35_msg.splice_command_type, scte35_msg.splice_time);
                                
                                tokio::spawn(async move {
                                    let mut msg_guard = scte35_message.lock().await;
                                    *msg_guard = Some(scte35_msg);
                                });
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
    async fn start_monitoring_tasks(&self) {
        let pipeline_id = self.pipeline_id.clone();
        let rtp_connected = self.rtp_connected.clone();

        // RTP connection timeout monitoring
        tokio::spawn(async move {
            sleep(Duration::from_secs(30)).await;
            
            let connected = rtp_connected.lock().await;
            if !*connected {
                warn!("[{}] RTP connection timeout - no packets received within 30 seconds", pipeline_id);
            }
        });
    }

    /// Switch to asset video
    pub async fn switch_to_asset(&self) -> Result<()> {
        info!("[{}] Switching to asset video: {}", self.pipeline_id, self.config.asset_path);
        // TODO: Implement asset switching
        Ok(())
    }

    /// Switch back to RTP stream
    pub async fn switch_to_rtp(&self) -> Result<()> {
        info!("[{}] Switching back to RTP stream", self.pipeline_id);
        // TODO: Implement RTP switching
        Ok(())
    }
}

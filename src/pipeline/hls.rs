//! HLS (HTTP Live Streaming) pipeline implementation

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

/// HLS GStreamer pipeline with video mixing and audio mixing capabilities
pub struct HlsPipeline {
    pipeline: Arc<Mutex<Option<Pipeline>>>,
    asset_pipeline: Arc<Mutex<Option<Pipeline>>>,
    config: PipelineConfig,
    pipeline_id: String,
    state: Arc<Mutex<PipelineState>>,
    current_input: Arc<Mutex<InputSource>>,
    asset_playing: Arc<Mutex<bool>>,
    hls_connected: Arc<Mutex<bool>>,
    stop_tx: tokio::sync::mpsc::UnboundedSender<()>,
    stop_rx: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<()>>>>,
    current_pts: Arc<Mutex<u64>>,
    scte35_message: Arc<Mutex<Option<Scte35Message>>>,
}

impl HlsPipeline {
    /// Create a new HLS pipeline
    pub fn new(config: PipelineConfig) -> Result<Self> {
        let pipeline_id = format!("hls_pipeline_{}", Uuid::new_v4());
        let (stop_tx, stop_rx) = tokio::sync::mpsc::unbounded_channel();

        Ok(Self {
            pipeline: Arc::new(Mutex::new(None)),
            asset_pipeline: Arc::new(Mutex::new(None)),
            config,
            pipeline_id,
            state: Arc::new(Mutex::new(PipelineState::Stopped)),
            current_input: Arc::new(Mutex::new(InputSource::Main)),
            asset_playing: Arc::new(Mutex::new(false)),
            hls_connected: Arc::new(Mutex::new(false)),
            stop_tx,
            stop_rx: Arc::new(Mutex::new(Some(stop_rx))),
            current_pts: Arc::new(Mutex::new(0)),
            scte35_message: Arc::new(Mutex::new(None)),
        })
    }

    /// Start the HLS pipeline
    pub async fn start(&mut self) -> Result<()> {
        info!("[{}] Starting HLS pipeline", self.pipeline_id);

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

        info!("[{}] HLS pipeline started successfully", self.pipeline_id);
        Ok(())
    }

    /// Stop the HLS pipeline
    pub async fn stop(&mut self) -> Result<()> {
        info!("[{}] Stopping HLS pipeline", self.pipeline_id);

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

        info!("[{}] HLS pipeline stopped", self.pipeline_id);
        Ok(())
    }

    /// Create the main HLS pipeline
    async fn create_main_pipeline(&self) -> Result<Pipeline> {
        let pipeline = Pipeline::with_name(&format!("hls-pipeline-{}", self.pipeline_id))?;

        // Create HLS source
        let hls_src = PipelineCommon::create_element("souphttpsrc", &format!("hlsSrc_{}", self.pipeline_id), None)?;
        hls_src.set_property("location", &self.config.hls_url.as_ref().unwrap());
        hls_src.set_property("timeout", 10_000_000_000u64); // 10 seconds
        hls_src.set_property("retries", 3i32);
        hls_src.set_property("do-timestamp", true);
        hls_src.set_property("http-log-level", 3i32);

        // Create HLS demuxer
        let hls_demux = PipelineCommon::create_element("hlsdemux", &format!("hlsDemux_{}", self.pipeline_id), None)?;
        hls_demux.set_property("timeout", 5_000_000_000u64); // 5 seconds
        hls_demux.set_property("max-errors", 5i32);
        hls_demux.set_property("async", false);
        hls_demux.set_property("sync", false);

        // Create TS demuxer for HLS output
        let ts_demux = PipelineCommon::create_element("tsdemux", &format!("tsdemux_{}", self.pipeline_id), None)?;
        ts_demux.set_property("send-scte35-events", true);
        ts_demux.set_property("latency", 1000i32);

        // Add elements to pipeline
        pipeline.add(&hls_src)?;
        pipeline.add(&hls_demux)?;
        pipeline.add(&ts_demux)?;

        // Link elements
        PipelineCommon::link_elements(&hls_src, &hls_demux)?;

        // Set up dynamic pad handling
        self.setup_demuxer_pad_handling(&hls_demux, &ts_demux).await?;

        // Set up bus message handling
        self.setup_bus_handler(&pipeline).await;

        info!("[{}] HLS pipeline created successfully", self.pipeline_id);
        Ok(pipeline)
    }

    /// Set up demuxer pad handling
    async fn setup_demuxer_pad_handling(
        &self,
        hls_demux: &Element,
        ts_demux: &Element,
    ) -> Result<()> {
        let pipeline_id = self.pipeline_id.clone();
        
        hls_demux.connect_pad_added(move |_, pad| {
            debug!("[{}] HLS demuxer pad added: {}", pipeline_id, pad.name());
            
            // Link HLS demuxer output to tsdemux
            if let Some(ts_demux_sink_pad) = ts_demux.static_pad("sink") {
                if !ts_demux_sink_pad.is_linked() {
                    if let Err(e) = pad.link(&ts_demux_sink_pad) {
                        error!("[{}] Failed to link HLS demuxer to tsdemux: {}", pipeline_id, e);
                    } else {
                        debug!("[{}] Successfully linked HLS demuxer to tsdemux", pipeline_id);
                    }
                }
            }
        });

        Ok(())
    }

    /// Set up bus message handling
    async fn setup_bus_handler(&self, pipeline: &Pipeline) {
        let pipeline_id = self.pipeline_id.clone();
        
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
    }

    /// Start monitoring tasks
    async fn start_monitoring_tasks(&self) {
        let pipeline_id = self.pipeline_id.clone();
        let hls_connected = self.hls_connected.clone();

        // HLS connection timeout monitoring
        tokio::spawn(async move {
            sleep(Duration::from_secs(30)).await;
            
            let connected = hls_connected.lock().await;
            if !*connected {
                warn!("[{}] HLS connection timeout - no packets received within 30 seconds", pipeline_id);
            }
        });
    }

    /// Switch to asset video
    pub async fn switch_to_asset(&self) -> Result<()> {
        info!("[{}] Switching to asset video: {}", self.pipeline_id, self.config.asset_path);
        // TODO: Implement asset switching
        Ok(())
    }

    /// Switch back to HLS stream
    pub async fn switch_to_hls(&self) -> Result<()> {
        info!("[{}] Switching back to HLS stream", self.pipeline_id);
        // TODO: Implement HLS switching
        Ok(())
    }
}
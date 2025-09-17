//! Recorder implementation for recording multicast UDP streams

use crate::error::{Error, Result};
use crate::pipeline::common::PipelineCommon;
use crate::types::{PipelineConfig, PipelineState};
use gstreamer::prelude::*;
use gstreamer::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Recorder for recording multicast UDP streams to local TS files
pub struct Recorder {
    pipeline: Arc<Mutex<Option<Pipeline>>>,
    config: PipelineConfig,
    recorder_id: String,
    state: Arc<Mutex<PipelineState>>,
    output_file: String,
}

impl Recorder {
    /// Create a new recorder
    pub fn new(config: PipelineConfig, output_file: String) -> Result<Self> {
        let recorder_id = format!("recorder_{}", Uuid::new_v4());

        Ok(Self {
            pipeline: Arc::new(Mutex::new(None)),
            config,
            recorder_id,
            state: Arc::new(Mutex::new(PipelineState::Stopped)),
            output_file,
        })
    }

    /// Start recording
    pub async fn start(&mut self) -> Result<()> {
        info!("[{}] Starting recorder", self.recorder_id);

        // Create the recording pipeline
        let pipeline = self.create_recording_pipeline().await?;
        
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

        info!("[{}] Recorder started successfully", self.recorder_id);
        Ok(())
    }

    /// Stop recording
    pub async fn stop(&mut self) -> Result<()> {
        info!("[{}] Stopping recorder", self.recorder_id);

        // Set state to stopping
        {
            let mut state_guard = self.state.lock().await;
            *state_guard = PipelineState::Stopping;
        }

        // Stop pipeline
        {
            let mut pipeline_guard = self.pipeline.lock().await;
            if let Some(pipeline) = pipeline_guard.take() {
                PipelineCommon::set_element_state(&pipeline, State::Null)?;
            }
        }

        // Set state to stopped
        {
            let mut state_guard = self.state.lock().await;
            *state_guard = PipelineState::Stopped;
        }

        info!("[{}] Recorder stopped", self.recorder_id);
        Ok(())
    }

    /// Create the recording pipeline
    async fn create_recording_pipeline(&self) -> Result<Pipeline> {
        let pipeline = Pipeline::with_name(&format!("recorder-pipeline-{}", self.recorder_id))?;

        // Create UDP source
        let udp_src = PipelineCommon::create_element("udpsrc", &format!("udpsrc_{}", self.recorder_id), None)?;
        udp_src.set_property("address", &self.config.input_host);
        udp_src.set_property("port", self.config.input_port);
        udp_src.set_property("buffer-size", 524_288i32);
        udp_src.set_property("auto-multicast", true);

        // Create TS demuxer
        let ts_demux = PipelineCommon::create_element("tsdemux", &format!("tsdemux_{}", self.recorder_id), None)?;
        ts_demux.set_property("send-scte35-events", true);
        ts_demux.set_property("latency", 1000i32);

        // Create MPEG-TS muxer
        let mpegts_mux = PipelineCommon::create_mpegts_muxer(&format!("mpegtsmux_{}", self.recorder_id))?;

        // Create file sink
        let file_sink = PipelineCommon::create_element("filesink", &format!("filesink_{}", self.recorder_id), None)?;
        file_sink.set_property("location", &self.output_file);
        file_sink.set_property("sync", false);

        // Add all elements to pipeline
        pipeline.add(&udp_src)?;
        pipeline.add(&ts_demux)?;
        pipeline.add(&mpegts_mux)?;
        pipeline.add(&file_sink)?;

        // Link static elements
        PipelineCommon::link_elements(&udp_src, &ts_demux)?;
        PipelineCommon::link_elements(&mpegts_mux, &file_sink)?;

        // Set up dynamic pad handling for TS demuxer
        self.setup_demuxer_pad_handling(&ts_demux, &mpegts_mux).await?;

        // Set up bus message handling
        self.setup_bus_handler(&pipeline).await;

        info!("[{}] Recording pipeline created successfully", self.recorder_id);
        Ok(pipeline)
    }

    /// Set up demuxer pad handling
    async fn setup_demuxer_pad_handling(
        &self,
        ts_demux: &Element,
        mpegts_mux: &Element,
    ) -> Result<()> {
        let recorder_id = self.recorder_id.clone();
        
        ts_demux.connect_pad_added(move |_, pad| {
            debug!("[{}] TS demuxer pad added: {}", recorder_id, pad.name());
            
            let pad_name = pad.name();
            if pad_name.starts_with("video") {
                debug!("[{}] Creating video pipeline for recording", recorder_id);
                
                // Create video queue
                let video_queue = match PipelineCommon::create_element("queue", &format!("videoQueue_{}", recorder_id), None) {
                    Ok(queue) => queue,
                    Err(e) => {
                        error!("[{}] Failed to create video queue: {}", recorder_id, e);
                        return;
                    }
                };

                // Add queue to pipeline
                if let Err(e) = ts_demux.pipeline().unwrap().add(&video_queue) {
                    error!("[{}] Failed to add video queue to pipeline: {}", recorder_id, e);
                    return;
                }

                // Set queue state to playing
                if let Err(e) = PipelineCommon::set_element_state(&video_queue, State::Playing) {
                    error!("[{}] Failed to set video queue state: {}", recorder_id, e);
                    return;
                }

                // Link demuxer pad to queue
                if let Some(queue_sink_pad) = video_queue.static_pad("sink") {
                    if let Err(e) = pad.link(&queue_sink_pad) {
                        error!("[{}] Failed to link video pad to queue: {}", recorder_id, e);
                        return;
                    }
                }

                // Link queue to muxer
                if let Some(queue_src_pad) = video_queue.static_pad("src") {
                    if let Some(muxer_sink_pad) = mpegts_mux.request_pad_simple("sink_%d") {
                        if let Err(e) = queue_src_pad.link(&muxer_sink_pad) {
                            error!("[{}] Failed to link video queue to muxer: {}", recorder_id, e);
                        } else {
                            debug!("[{}] Successfully linked video pipeline for recording", recorder_id);
                        }
                    }
                }
            } else if pad_name.starts_with("audio") {
                debug!("[{}] Creating audio pipeline for recording", recorder_id);
                
                // Create audio queue
                let audio_queue = match PipelineCommon::create_element("queue", &format!("audioQueue_{}", recorder_id), None) {
                    Ok(queue) => queue,
                    Err(e) => {
                        error!("[{}] Failed to create audio queue: {}", recorder_id, e);
                        return;
                    }
                };

                // Add queue to pipeline
                if let Err(e) = ts_demux.pipeline().unwrap().add(&audio_queue) {
                    error!("[{}] Failed to add audio queue to pipeline: {}", recorder_id, e);
                    return;
                }

                // Set queue state to playing
                if let Err(e) = PipelineCommon::set_element_state(&audio_queue, State::Playing) {
                    error!("[{}] Failed to set audio queue state: {}", recorder_id, e);
                    return;
                }

                // Link demuxer pad to queue
                if let Some(queue_sink_pad) = audio_queue.static_pad("sink") {
                    if let Err(e) = pad.link(&queue_sink_pad) {
                        error!("[{}] Failed to link audio pad to queue: {}", recorder_id, e);
                        return;
                    }
                }

                // Link queue to muxer
                if let Some(queue_src_pad) = audio_queue.static_pad("src") {
                    if let Some(muxer_sink_pad) = mpegts_mux.request_pad_simple("sink_%d") {
                        if let Err(e) = queue_src_pad.link(&muxer_sink_pad) {
                            error!("[{}] Failed to link audio queue to muxer: {}", recorder_id, e);
                        } else {
                            debug!("[{}] Successfully linked audio pipeline for recording", recorder_id);
                        }
                    }
                }
            } else {
                debug!("[{}] Unknown demuxer pad: {}", recorder_id, pad_name);
            }
        });

        Ok(())
    }

    /// Set up bus message handling
    async fn setup_bus_handler(&self, pipeline: &Pipeline) {
        let recorder_id = self.recorder_id.clone();
        
        PipelineCommon::setup_bus_handler(
            pipeline,
            &recorder_id,
            move |error_msg| {
                error!("[{}] Pipeline error: {}", recorder_id, error_msg);
            },
            move |warning_msg| {
                warn!("[{}] Pipeline warning: {}", recorder_id, warning_msg);
            },
            move |info_msg| {
                info!("[{}] Pipeline info: {}", recorder_id, info_msg);
            },
        );
    }

    /// Get the output file path
    pub fn get_output_file(&self) -> &str {
        &self.output_file
    }

    /// Set a new output file path
    pub fn set_output_file(&mut self, output_file: String) {
        self.output_file = output_file;
    }

    /// Get the current state
    pub async fn get_state(&self) -> PipelineState {
        let state_guard = self.state.lock().await;
        *state_guard
    }
}

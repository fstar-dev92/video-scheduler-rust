//! Common pipeline utilities and helper functions

use crate::error::{Error, Result};
use gstreamer::prelude::*;
use gstreamer::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Common pipeline operations and utilities
pub struct PipelineCommon;

impl PipelineCommon {
    /// Create a GStreamer element with error handling
    pub fn create_element(
        factory_name: &str,
        name: &str,
        properties: Option<&[(&str, &dyn ToValue)]>,
    ) -> Result<Element> {
        let element = if let Some(props) = properties {
            ElementFactory::make_with_name(factory_name, Some(name))
                .ok_or_else(|| Error::element_creation(factory_name, "Element factory not found"))?
                .build_with_properties(props)
                .map_err(|e| Error::element_creation(factory_name, &e.to_string()))?
        } else {
            ElementFactory::make_with_name(factory_name, Some(name))
                .ok_or_else(|| Error::element_creation(factory_name, "Element factory not found"))?
                .build()
                .map_err(|e| Error::element_creation(factory_name, &e.to_string()))?
        };

        debug!("Created element: {} ({})", name, factory_name);
        Ok(element)
    }

    /// Link two GStreamer elements with error handling
    pub fn link_elements(from: &Element, to: &Element) -> Result<()> {
        from.link(to)
            .map_err(|e| Error::element_linking(
                from.name(),
                to.name(),
                &e.to_string()
            ))?;
        
        debug!("Linked elements: {} -> {}", from.name(), to.name());
        Ok(())
    }

    /// Set element state with error handling
    pub fn set_element_state(element: &Element, state: State) -> Result<StateChangeReturn> {
        let result = element.set_state(state)
            .map_err(|e| Error::state_change(element.name(), &e.to_string()))?;
        
        debug!("Set element {} state to {:?}: {:?}", element.name(), state, result);
        Ok(result)
    }

    /// Create a queue element with optimized settings
    pub fn create_optimized_queue(name: &str, max_buffers: u32, max_time_ns: u64) -> Result<Element> {
        let queue = Self::create_element("queue", name, None)?;
        
        queue.set_property("max-size-buffers", max_buffers);
        queue.set_property("max-size-time", max_time_ns);
        queue.set_property("min-threshold-time", 20_000_000u64); // 20ms
        queue.set_property("sync", false);
        queue.set_property("leaky", 2); // Leak downstream (newer frames)
        queue.set_property("max-size-bytes", 0u32);
        queue.set_property("silent", false);
        
        Ok(queue)
    }

    /// Create an audio queue with settings optimized for seamless playback
    pub fn create_audio_queue(name: &str) -> Result<Element> {
        let queue = Self::create_element("queue", name, None)?;
        
        // Much larger buffer settings for audio continuity - prevent cutting
        queue.set_property("max-size-buffers", 2000u32);
        queue.set_property("max-size-time", 3_000_000_000u64); // 3 seconds
        queue.set_property("min-threshold-time", 500_000_000u64); // 500ms
        queue.set_property("sync", true); // Enable sync for audio
        queue.set_property("leaky", 0); // Don't leak audio frames
        queue.set_property("max-size-bytes", 0u32);
        queue.set_property("silent", false);
        
        Ok(queue)
    }

    /// Create a video queue with settings optimized for high frame rate
    pub fn create_video_queue(name: &str) -> Result<Element> {
        let queue = Self::create_element("queue", name, None)?;
        
        // Optimize for seamless video playback
        queue.set_property("max-size-buffers", 50u32);
        queue.set_property("max-size-time", 500_000_000u64); // 500ms
        queue.set_property("min-threshold-time", 20_000_000u64); // 20ms
        queue.set_property("sync", false);
        queue.set_property("leaky", 2); // Leak downstream (newer frames)
        queue.set_property("max-size-bytes", 0u32);
        queue.set_property("silent", false);
        
        Ok(queue)
    }

    /// Create a capsfilter with the specified caps
    pub fn create_capsfilter(name: &str, caps_str: &str) -> Result<Element> {
        let capsfilter = Self::create_element("capsfilter", name, None)?;
        let caps = Caps::from_string(caps_str)
            .map_err(|e| Error::element_creation("capsfilter", &e.to_string()))?;
        capsfilter.set_property("caps", &caps);
        Ok(capsfilter)
    }

    /// Create an intervideosink with optimized settings
    pub fn create_intervideosink(name: &str, channel: &str) -> Result<Element> {
        let sink = Self::create_element("intervideosink", name, None)?;
        sink.set_property("channel", channel);
        sink.set_property("sync", false);
        sink.set_property("max-lateness", 20_000_000i64); // 20ms
        sink.set_property("qos", true);
        sink.set_property("async", false);
        Ok(sink)
    }

    /// Create an interaudiosink with optimized settings
    pub fn create_interaudiosink(name: &str, channel: &str) -> Result<Element> {
        let sink = Self::create_element("interaudiosink", name, None)?;
        sink.set_property("channel", channel);
        sink.set_property("sync", false);
        sink.set_property("max-lateness", 20_000_000i64); // 20ms
        sink.set_property("qos", true);
        sink.set_property("async", false);
        Ok(sink)
    }

    /// Create an intervideosrc with optimized settings
    pub fn create_intervideosrc(name: &str, channel: &str) -> Result<Element> {
        let src = Self::create_element("intervideosrc", name, None)?;
        src.set_property("channel", channel);
        src.set_property("do-timestamp", true);
        Ok(src)
    }

    /// Create an interaudiosrc with optimized settings
    pub fn create_interaudiosrc(name: &str, channel: &str) -> Result<Element> {
        let src = Self::create_element("interaudiosrc", name, None)?;
        src.set_property("channel", channel);
        src.set_property("do-timestamp", true);
        Ok(src)
    }

    /// Create a videomixer with optimized settings
    pub fn create_videomixer(name: &str) -> Result<Element> {
        let mixer = Self::create_element("videomixer", name, None)?;
        Ok(mixer)
    }

    /// Create an audiomixer with optimized settings
    pub fn create_audiomixer(name: &str) -> Result<Element> {
        let mixer = Self::create_element("audiomixer", name, None)?;
        
        // Audio mixer settings optimized for seamless audio
        mixer.set_property("latency", 200_000_000i64); // 200ms
        mixer.set_property("silent", false);
        mixer.set_property("resampler", "audioconvert");
        mixer.set_property("sync", true);
        mixer.set_property("async", false);
        mixer.set_property("max-lateness", 50_000_000i64); // 50ms
        mixer.set_property("qos", true);
        
        Ok(mixer)
    }

    /// Create a volume control element
    pub fn create_volume(name: &str, initial_volume: f64) -> Result<Element> {
        let volume = Self::create_element("volume", name, None)?;
        volume.set_property("volume", initial_volume);
        Ok(volume)
    }

    /// Create an x264 encoder with optimized settings
    pub fn create_x264_encoder(name: &str) -> Result<Element> {
        let encoder = Self::create_element("x264enc", name, None)?;
        
        // Optimize for low latency
        encoder.set_property("tune", "zerolatency");
        encoder.set_property("speed-preset", "ultrafast");
        encoder.set_property("key-int-max", 30i32);
        encoder.set_property("bframes", 0i32);
        encoder.set_property("ref", 1i32);
        
        Ok(encoder)
    }

    /// Create an AAC encoder with optimized settings
    pub fn create_aac_encoder(name: &str) -> Result<Element> {
        let encoder = Self::create_element("avenc_aac", name, None)?;
        
        encoder.set_property("bitrate", 192_000i32);
        encoder.set_property("channels", 2i32);
        encoder.set_property("quality", 2i32);
        encoder.set_property("profile", 2i32);
        encoder.set_property("afterburner", true);
        encoder.set_property("sync", true);
        
        Ok(encoder)
    }

    /// Create an MPEG-TS muxer with optimized settings
    pub fn create_mpegts_muxer(name: &str) -> Result<Element> {
        let muxer = Self::create_element("mpegtsmux", name, None)?;
        
        muxer.set_property("alignment", 7i32);
        muxer.set_property("pat-interval", 27_000_000_000i64);
        muxer.set_property("pmt-interval", 27_000_000_000i64);
        muxer.set_property("pcr-interval", 2_700_000_000i64);
        muxer.set_property("start-time", 500_000_000i64);
        muxer.set_property("si-interval", 500i32);
        
        Ok(muxer)
    }

    /// Create an RTP MPEG-TS payloader
    pub fn create_rtp_mp2t_payloader(name: &str) -> Result<Element> {
        let payloader = Self::create_element("rtpmp2tpay", name, None)?;
        
        payloader.set_property("mtu", 1400i32);
        payloader.set_property("pt", 33i32);
        payloader.set_property("perfect-rtptime", true);
        
        Ok(payloader)
    }

    /// Create a UDP sink with optimized settings
    pub fn create_udp_sink(name: &str, host: &str, port: u16) -> Result<Element> {
        let sink = Self::create_element("udpsink", name, None)?;
        
        sink.set_property("host", host);
        sink.set_property("port", port);
        sink.set_property("sync", false);
        sink.set_property("buffer-size", 1_048_576i32); // 1MB
        sink.set_property("auto-multicast", true);
        sink.set_property("async", false);
        
        Ok(sink)
    }

    /// Create an RTP bin with optimized settings
    pub fn create_rtp_bin(name: &str) -> Result<Element> {
        let rtpbin = Self::create_element("rtpbin", name, None)?;
        
        rtpbin.set_property("latency", 200u32);
        rtpbin.set_property("do-retransmission", true);
        rtpbin.set_property("rtp-profile", 2i32);
        rtpbin.set_property("ntp-sync", true);
        rtpbin.set_property("ntp-time-source", 3i32);
        rtpbin.set_property("max-rtcp-rtp-time-diff", 1000i32);
        rtpbin.set_property("max-dropout-time", 45_000i32);
        rtpbin.set_property("max-misorder-time", 5_000i32);
        rtpbin.set_property("buffer-mode", 1i32);
        rtpbin.set_property("do-lost", true);
        rtpbin.set_property("rtcp-sync-send-time", true);
        
        Ok(rtpbin)
    }

    /// Set up bus message handling for a pipeline
    pub fn setup_bus_handler(
        pipeline: &Pipeline,
        pipeline_id: &str,
        on_error: impl Fn(String) + Send + 'static,
        on_warning: impl Fn(String) + Send + 'static,
        on_info: impl Fn(String) + Send + 'static,
    ) {
        let bus = pipeline.bus().unwrap();
        let pipeline_id = pipeline_id.to_string();
        
        bus.add_signal_watch();
        bus.connect_message(move |_, msg| {
            match msg.view() {
                MessageView::Error(err) => {
                    error!("[{}] Pipeline error: {}", pipeline_id, err.error());
                    on_error(err.error().to_string());
                }
                MessageView::Warning(warn) => {
                    warn!("[{}] Pipeline warning: {}", pipeline_id, warn.warning());
                    on_warning(warn.warning().to_string());
                }
                MessageView::Info(info) => {
                    info!("[{}] Pipeline info: {}", pipeline_id, info.info());
                    on_info(info.info().to_string());
                }
                MessageView::StateChanged(state_changed) => {
                    let old_state = state_changed.old();
                    let new_state = state_changed.current();
                    debug!("[{}] Pipeline state changed: {:?} -> {:?}", pipeline_id, old_state, new_state);
                }
                MessageView::Eos(_) => {
                    info!("[{}] Pipeline reached end of stream", pipeline_id);
                }
                _ => {}
            }
        });
    }
}

//! Common types and data structures used throughout the video scheduler

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

/// Stream input types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamType {
    /// HLS (HTTP Live Streaming)
    Hls,
    /// SRT (Secure Reliable Transport)
    Srt,
    /// RTP (Real-time Transport Protocol)
    Rtp,
    /// Local file
    File,
}

/// Stream item for scheduling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamItem {
    /// Unique identifier for this stream item
    pub id: Uuid,
    /// Type of stream
    pub stream_type: StreamType,
    /// Source URL or file path
    pub source: String,
    /// When this item should start
    pub start_time: DateTime<Utc>,
    /// Duration of this item
    pub duration: Duration,
    /// Offset within the source to start from
    pub offset: Duration,
    /// Whether this item requires a break (for SCTE-35)
    pub need_break: bool,
    /// Additional metadata
    pub metadata: serde_json::Value,
}

impl StreamItem {
    /// Create a new stream item
    pub fn new(
        stream_type: StreamType,
        source: String,
        start_time: DateTime<Utc>,
        duration: Duration,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            stream_type,
            source,
            start_time,
            duration,
            offset: Duration::ZERO,
            need_break: false,
            metadata: serde_json::Value::Null,
        }
    }

    /// Set the offset for this stream item
    pub fn with_offset(mut self, offset: Duration) -> Self {
        self.offset = offset;
        self
    }

    /// Mark this item as needing a break
    pub fn with_break(mut self, need_break: bool) -> Self {
        self.need_break = need_break;
        self
    }

    /// Add metadata to this stream item
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// SCTE-35 message structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scte35Message {
    /// Table ID
    pub table_id: u8,
    /// Section syntax indicator
    pub section_syntax_indicator: bool,
    /// Private indicator
    pub private_indicator: bool,
    /// Section length
    pub section_length: u16,
    /// Protocol version
    pub protocol_version: u8,
    /// Encrypted packet flag
    pub encrypted_packet: bool,
    /// Encryption algorithm
    pub encryption_algorithm: u8,
    /// PTS adjustment
    pub pts_adjustment: u64,
    /// CW index
    pub cw_index: u8,
    /// Tier
    pub tier: u16,
    /// Splice command length
    pub splice_command_length: u16,
    /// Splice command type
    pub splice_command_type: u8,
    /// Splice time (PTS when splice should occur)
    pub splice_time: u64,
}

impl Scte35Message {
    /// Create a new SCTE-35 message
    pub fn new(splice_command_type: u8, splice_time: u64) -> Self {
        Self {
            table_id: 0xFC,
            section_syntax_indicator: false,
            private_indicator: false,
            section_length: 0,
            protocol_version: 0,
            encrypted_packet: false,
            encryption_algorithm: 0,
            pts_adjustment: 0,
            cw_index: 0,
            tier: 0,
            splice_command_length: 0,
            splice_command_type,
            splice_time,
        }
    }

    /// Check if this is an immediate splice insert (command type 0x05)
    pub fn is_immediate_splice_insert(&self) -> bool {
        self.splice_command_type == 0x05
    }

    /// Check if this is a splice null (command type 0x06)
    pub fn is_splice_null(&self) -> bool {
        self.splice_command_type == 0x06
    }

    /// Check if this is a splice schedule (command type 0x07)
    pub fn is_splice_schedule(&self) -> bool {
        self.splice_command_type == 0x07
    }
}

/// Pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Input host/address
    pub input_host: String,
    /// Input port
    pub input_port: u16,
    /// Output host/address
    pub output_host: String,
    /// Output port
    pub output_port: u16,
    /// Asset video file path
    pub asset_path: String,
    /// HLS URL (for HLS pipelines)
    pub hls_url: Option<String>,
    /// SRT URL (for SRT pipelines)
    pub srt_url: Option<String>,
    /// Pipeline latency in nanoseconds
    pub latency: u64,
    /// Buffer size in bytes
    pub buffer_size: u32,
    /// Enable verbose logging
    pub verbose: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            input_host: "239.9.9.9".to_string(),
            input_port: 5000,
            output_host: "239.8.8.8".to_string(),
            output_port: 6000,
            asset_path: "/tmp/input_cut.mp4".to_string(),
            hls_url: None,
            srt_url: None,
            latency: 200_000_000, // 200ms
            buffer_size: 524_288, // 512KB
            verbose: false,
        }
    }
}

/// Recording configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingConfig {
    /// Multicast group for input
    pub multicast_group: String,
    /// Port for input
    pub port: u16,
    /// Network interface
    pub multicast_interface: String,
    /// Program number
    pub program_number: u16,
    /// Output file path
    pub output_file: String,
}

impl Default for RecordingConfig {
    fn default() -> Self {
        Self {
            multicast_group: "224.2.3.13".to_string(),
            port: 10500,
            multicast_interface: "eno2".to_string(),
            program_number: 3,
            output_file: "output.ts".to_string(),
        }
    }
}

/// Scheduler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    /// Output host
    pub host: String,
    /// Output port
    pub port: u16,
    /// Stream items to schedule
    pub items: Vec<StreamItem>,
}

impl SchedulerConfig {
    /// Create a new scheduler configuration
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            items: Vec::new(),
        }
    }

    /// Add a stream item to the schedule
    pub fn add_item(&mut self, item: StreamItem) {
        self.items.push(item);
    }
}

/// Current pipeline state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineState {
    /// Pipeline is not running
    Stopped,
    /// Pipeline is starting up
    Starting,
    /// Pipeline is running normally
    Running,
    /// Pipeline is paused
    Paused,
    /// Pipeline is stopping
    Stopping,
    /// Pipeline encountered an error
    Error(String),
}

/// Input source type for compositor
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InputSource {
    /// Main stream (RTP/HLS)
    Main,
    /// Asset stream (local file)
    Asset,
}

impl InputSource {
    /// Get the channel name for this input source
    pub fn channel_name(&self) -> &'static str {
        match self {
            InputSource::Main => "input1",
            InputSource::Asset => "input2",
        }
    }

    /// Get the audio channel name for this input source
    pub fn audio_channel_name(&self) -> &'static str {
        match self {
            InputSource::Main => "audio1",
            InputSource::Asset => "audio2",
        }
    }
}

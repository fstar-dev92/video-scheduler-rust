//! Integration tests for the video scheduler application

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PipelineConfig, StreamItem, StreamType, Scte35Message};
    use chrono::Utc;
    use std::time::Duration;

    #[test]
    fn test_pipeline_config_default() {
        let config = PipelineConfig::default();
        assert_eq!(config.input_host, "239.9.9.9");
        assert_eq!(config.input_port, 5000);
        assert_eq!(config.output_host, "239.8.8.8");
        assert_eq!(config.output_port, 6000);
        assert_eq!(config.latency, 200_000_000);
        assert_eq!(config.buffer_size, 524_288);
        assert!(!config.verbose);
    }

    #[test]
    fn test_stream_item_creation() {
        let item = StreamItem::new(
            StreamType::File,
            "/path/to/video.mp4".to_string(),
            Utc::now(),
            Duration::from_secs(30),
        );

        assert_eq!(item.stream_type, StreamType::File);
        assert_eq!(item.source, "/path/to/video.mp4");
        assert_eq!(item.duration, Duration::from_secs(30));
        assert_eq!(item.offset, Duration::ZERO);
        assert!(!item.need_break);
    }

    #[test]
    fn test_stream_item_with_offset() {
        let item = StreamItem::new(
            StreamType::Hls,
            "https://example.com/stream.m3u8".to_string(),
            Utc::now(),
            Duration::from_secs(60),
        ).with_offset(Duration::from_secs(10));

        assert_eq!(item.offset, Duration::from_secs(10));
    }

    #[test]
    fn test_stream_item_with_break() {
        let item = StreamItem::new(
            StreamType::Rtp,
            "udp://239.1.1.1:5000".to_string(),
            Utc::now(),
            Duration::from_secs(45),
        ).with_break(true);

        assert!(item.need_break);
    }

    #[test]
    fn test_scte35_message_creation() {
        let msg = Scte35Message::new(0x05, 12345);
        
        assert_eq!(msg.table_id, 0xFC);
        assert_eq!(msg.splice_command_type, 0x05);
        assert_eq!(msg.splice_time, 12345);
        assert!(msg.is_immediate_splice_insert());
    }

    #[test]
    fn test_scte35_message_types() {
        let immediate_msg = Scte35Message::new(0x05, 0);
        let null_msg = Scte35Message::new(0x06, 0);
        let schedule_msg = Scte35Message::new(0x07, 0);

        assert!(immediate_msg.is_immediate_splice_insert());
        assert!(!immediate_msg.is_splice_null());
        assert!(!immediate_msg.is_splice_schedule());

        assert!(!null_msg.is_immediate_splice_insert());
        assert!(null_msg.is_splice_null());
        assert!(!null_msg.is_splice_schedule());

        assert!(!schedule_msg.is_immediate_splice_insert());
        assert!(!schedule_msg.is_splice_null());
        assert!(schedule_msg.is_splice_schedule());
    }

    #[test]
    fn test_input_source_channel_names() {
        use crate::types::InputSource;

        assert_eq!(InputSource::Main.channel_name(), "input1");
        assert_eq!(InputSource::Asset.channel_name(), "input2");
        assert_eq!(InputSource::Main.audio_channel_name(), "audio1");
        assert_eq!(InputSource::Asset.audio_channel_name(), "audio2");
    }

    #[test]
    fn test_pipeline_state() {
        use crate::types::PipelineState;

        let states = vec![
            PipelineState::Stopped,
            PipelineState::Starting,
            PipelineState::Running,
            PipelineState::Paused,
            PipelineState::Stopping,
            PipelineState::Error("test error".to_string()),
        ];

        for state in states {
            match state {
                PipelineState::Stopped => assert_eq!(state, PipelineState::Stopped),
                PipelineState::Starting => assert_eq!(state, PipelineState::Starting),
                PipelineState::Running => assert_eq!(state, PipelineState::Running),
                PipelineState::Paused => assert_eq!(state, PipelineState::Paused),
                PipelineState::Stopping => assert_eq!(state, PipelineState::Stopping),
                PipelineState::Error(msg) => assert_eq!(msg, "test error"),
            }
        }
    }

    #[test]
    fn test_stream_type_serialization() {
        use serde_json;

        let stream_types = vec![
            StreamType::Hls,
            StreamType::Srt,
            StreamType::Rtp,
            StreamType::File,
        ];

        for stream_type in stream_types {
            let serialized = serde_json::to_string(&stream_type).unwrap();
            let deserialized: StreamType = serde_json::from_str(&serialized).unwrap();
            assert_eq!(stream_type, deserialized);
        }
    }

    #[test]
    fn test_pipeline_config_serialization() {
        use serde_json;

        let config = PipelineConfig {
            input_host: "192.168.1.100".to_string(),
            input_port: 8080,
            output_host: "192.168.1.200".to_string(),
            output_port: 9090,
            asset_path: "/tmp/test.mp4".to_string(),
            hls_url: Some("https://test.com/stream.m3u8".to_string()),
            srt_url: None,
            latency: 100_000_000,
            buffer_size: 1024_000,
            verbose: true,
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: PipelineConfig = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(config.input_host, deserialized.input_host);
        assert_eq!(config.input_port, deserialized.input_port);
        assert_eq!(config.output_host, deserialized.output_host);
        assert_eq!(config.output_port, deserialized.output_port);
        assert_eq!(config.asset_path, deserialized.asset_path);
        assert_eq!(config.hls_url, deserialized.hls_url);
        assert_eq!(config.srt_url, deserialized.srt_url);
        assert_eq!(config.latency, deserialized.latency);
        assert_eq!(config.buffer_size, deserialized.buffer_size);
        assert_eq!(config.verbose, deserialized.verbose);
    }
}

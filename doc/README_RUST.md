# Video Scheduler GStreamer - Rust Implementation

A high-performance, memory-safe video streaming application built in Rust using GStreamer for HLS/RTP processing, SCTE-35 ad insertion, video compositing, and stream scheduling.

## Features

- **HLS/RTP Stream Processing**: Handle HTTP Live Streaming and Real-time Transport Protocol streams
- **SCTE-35 Ad Insertion**: Automatic ad insertion based on SCTE-35 cue messages
- **Video Compositing**: Mix multiple video streams using GStreamer's compositor
- **Audio Mixing**: Combine multiple audio streams with volume controls
- **Stream Scheduling**: Schedule and play multiple video assets in sequence
- **Recording**: Record multicast UDP streams to local TS files
- **Dynamic Pad Handling**: Automatic handling of dynamic pads from demuxers
- **Error Recovery**: Robust error handling and pipeline recovery
- **Async/Await**: Built on Tokio for high-performance async operations

## Architecture

The application is organized into several modules:

- **`pipeline/`**: Core pipeline implementations (HLS, RTP)
- **`scte35/`**: SCTE-35 ad insertion handler
- **`scheduler/`**: Stream scheduling and management
- **`recorder/`**: Recording functionality
- **`types/`**: Common data structures and types
- **`error/`**: Error handling and custom error types

## Prerequisites

- Rust 1.70+ with Cargo
- GStreamer 1.18+ with development headers
- pkg-config
- Build tools (gcc, make)

### Ubuntu/Debian Installation

```bash
sudo apt update
sudo apt install -y \
    build-essential \
    pkg-config \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libgstreamer-plugins-bad1.0-dev \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    gstreamer1.0-tools
```

### macOS Installation

```bash
brew install gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad gst-plugins-ugly gst-libav
```

## Building

```bash
# Clone the repository
git clone https://github.com/fstar-dev92/video-scheduler-rust.git
cd video-scheduler-rust

# Build the project
cargo build --release

# Run tests
cargo test

# Run with examples
cargo run --release -- --help
```

## Usage

The application supports multiple operation modes:

### HLS Mode

Process HLS streams with automatic switching to local assets:

```bash
cargo run --release -- \
    --mode hls \
    --input-host 239.9.9.9 \
    --input-port 5000 \
    --output-host 239.8.8.8 \
    --output-port 6000 \
    --asset-path /path/to/asset.mp4 \
    --hls-url https://example.com/stream.m3u8
```

### RTP Mode

Process RTP streams with video/audio mixing:

```bash
cargo run --release -- \
    --mode rtp \
    --input-host 239.9.9.9 \
    --input-port 5000 \
    --output-host 239.8.8.8 \
    --output-port 6000 \
    --asset-path /path/to/asset.mp4
```

### SCTE-35 Mode

Handle SCTE-35 ad insertion:

```bash
cargo run --release -- \
    --mode scte35 \
    --input-host 239.9.9.9 \
    --input-port 5000 \
    --output-host 239.8.8.8 \
    --output-port 6000 \
    --ad-source /path/to/ad.mp4 \
    --verbose
```

### Scheduler Mode

Schedule multiple video assets:

```bash
cargo run --release -- \
    --mode scheduler \
    --output-host 239.8.8.8 \
    --output-port 6000
```

### Recorder Mode

Record multicast streams to file:

```bash
cargo run --release -- \
    --mode recorder \
    --input-host 239.9.9.9 \
    --input-port 5000 \
    --output-file /path/to/recording.ts
```

## Configuration

### Pipeline Configuration

The `PipelineConfig` struct allows you to configure:

- Input/output hosts and ports
- Asset file paths
- HLS/SRT URLs
- Latency settings
- Buffer sizes
- Verbose logging

### Stream Items

For scheduler mode, you can define `StreamItem` objects with:

- Stream type (HLS, SRT, RTP, File)
- Source URL or file path
- Start time and duration
- Offset within the source
- Break requirements for SCTE-35
- Custom metadata

### SCTE-35 Messages

The application handles SCTE-35 messages with support for:

- Immediate splice inserts (0x05)
- Splice nulls (0x06)
- Splice schedules (0x07)
- PTS adjustment and timing

## API Reference

### Core Types

```rust
// Pipeline configuration
pub struct PipelineConfig {
    pub input_host: String,
    pub input_port: u16,
    pub output_host: String,
    pub output_port: u16,
    pub asset_path: String,
    pub hls_url: Option<String>,
    pub latency: u64,
    pub verbose: bool,
}

// Stream item for scheduling
pub struct StreamItem {
    pub id: Uuid,
    pub stream_type: StreamType,
    pub source: String,
    pub start_time: DateTime<Utc>,
    pub duration: Duration,
    pub offset: Duration,
    pub need_break: bool,
    pub metadata: serde_json::Value,
}

// SCTE-35 message
pub struct Scte35Message {
    pub table_id: u8,
    pub splice_command_type: u8,
    pub splice_time: u64,
    pub pts_adjustment: u64,
    // ... other fields
}
```

### Pipeline Operations

```rust
// Create and start HLS pipeline
let mut pipeline = HlsPipeline::new(config)?;
pipeline.start().await?;

// Switch to asset video
pipeline.switch_to_asset().await?;

// Switch back to HLS
pipeline.switch_to_hls().await?;

// Stop pipeline
pipeline.stop().await?;
```

### SCTE-35 Handler

```rust
// Create SCTE-35 handler
let mut handler = Scte35Handler::new(
    input_host,
    input_port,
    output_host,
    output_port,
    ad_source,
    verbose,
);

// Start handler
handler.start().await?;

// Insert ad manually
handler.insert_ad().await?;

// Stop handler
handler.stop().await?;
```

### Stream Scheduler

```rust
// Create scheduler
let mut scheduler = StreamScheduler::new(config)?;

// Add stream items
let item = StreamItem::new(
    StreamType::File,
    "/path/to/video.mp4".to_string(),
    Utc::now(),
    Duration::from_secs(30),
);
scheduler.add_stream_item(item).await?;

// Start scheduler
scheduler.start().await?;
```

## Error Handling

The application uses a custom error type `AppError` with support for:

- Pipeline errors
- Validation errors
- GStreamer errors
- I/O errors
- Configuration errors

All errors are properly propagated and logged with context information.

## Performance Considerations

- **Memory Safety**: Rust's ownership system prevents memory leaks and buffer overflows
- **Zero-Cost Abstractions**: Minimal runtime overhead compared to C/C++
- **Async I/O**: Non-blocking operations using Tokio
- **Efficient GStreamer Integration**: Direct bindings without unnecessary allocations
- **Pipeline Optimization**: Optimized queue sizes and buffer configurations

## Logging

The application uses the `tracing` crate for structured logging:

```rust
use tracing::{info, warn, error, debug};

// Enable debug logging
RUST_LOG=debug cargo run --release
```

Log levels:
- `ERROR`: Critical errors that prevent operation
- `WARN`: Warnings about potential issues
- `INFO`: General information about pipeline state
- `DEBUG`: Detailed debugging information

## Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_hls_pipeline
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Migration from Go

This Rust implementation provides equivalent functionality to the original Go version with several improvements:

- **Memory Safety**: No garbage collection pauses or memory leaks
- **Performance**: Better performance characteristics and lower latency
- **Type Safety**: Compile-time guarantees about data structures
- **Error Handling**: More robust error handling with proper propagation
- **Concurrency**: Better async/await support with Tokio

### Key Differences

1. **Error Handling**: Uses `Result<T, E>` instead of Go's error returns
2. **Concurrency**: Uses `async/await` with Tokio instead of goroutines
3. **Memory Management**: Ownership-based memory management instead of GC
4. **Type System**: Stronger type system with compile-time checks
5. **Dependencies**: Uses `gstreamer` crate instead of `go-gst`

## Troubleshooting

### Common Issues

1. **GStreamer not found**: Ensure GStreamer development headers are installed
2. **Plugin not found**: Install required GStreamer plugins
3. **Permission denied**: Check file permissions for asset files
4. **Network issues**: Verify multicast addresses and ports are accessible

### Debug Mode

Run with verbose logging to diagnose issues:

```bash
RUST_LOG=debug cargo run --release -- --verbose
```

### GStreamer Debug

Enable GStreamer debug output:

```bash
GST_DEBUG=3 cargo run --release
```

## Support

For issues and questions:

1. Check the [troubleshooting section](#troubleshooting)
2. Review the [API documentation](#api-reference)
3. Open an issue on GitHub
4. Check GStreamer documentation for pipeline-specific issues

# Video Scheduler GStreamer

A high-performance, memory-safe video streaming application built in Rust using GStreamer for HLS/RTP processing, SCTE-35 ad insertion, video compositing, and stream scheduling.

[![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg)](https://www.rust-lang.org/)
[![GStreamer](https://img.shields.io/badge/gstreamer-1.18+-blue.svg)](https://gstreamer.freedesktop.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🚀 Features

- **HLS/RTP Stream Processing** - Handle HTTP Live Streaming and Real-time Transport Protocol streams
- **SCTE-35 Ad Insertion** - Automatic ad insertion based on SCTE-35 cue messages
- **Video Compositing** - Mix multiple video streams using GStreamer's compositor
- **Audio Mixing** - Combine multiple audio streams with volume controls
- **Stream Scheduling** - Schedule and play multiple video assets in sequence
- **Recording** - Record multicast UDP streams to local TS files
- **Memory Safe** - Built in Rust for memory safety and performance
- **Async/Await** - High-performance async operations with Tokio

## 📚 Documentation

- **[Rust Implementation Guide](doc/README_RUST.md)** - Complete guide for the Rust version
- **[GStreamer Pipeline with Compositor](doc/README_COMPOSITOR.md)** - Detailed compositor and audio mixer documentation
- **[SCTE-35 Handler Documentation](doc/README_SCTE35.md)** - SCTE-35 ad insertion implementation details

## 🏗️ Architecture

The application is organized into several modules:

- **`pipeline/`** - Core pipeline implementations (HLS, RTP)
- **`scte35/`** - SCTE-35 ad insertion handler
- **`scheduler/`** - Stream scheduling and management
- **`recorder/`** - Recording functionality
- **`types/`** - Common data structures and types
- **`error/`** - Error handling and custom error types

## 🛠️ Quick Start

### Prerequisites

- Rust 1.70+ with Cargo
- GStreamer 1.18+ with development headers
- pkg-config
- Build tools (gcc, make)

### Installation

#### Ubuntu/Debian
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

#### macOS
```bash
brew install gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad gst-plugins-ugly gst-libav
```

### Building

```bash
# Clone the repository
git clone https://github.com/fstar-dev92/video-scheduler-rust.git
cd video-scheduler-rust

# Build the project
cargo build --release

# Run tests
cargo test
```

## 🎯 Usage Examples

### HLS Stream Processing
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

### SCTE-35 Ad Insertion
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

### Stream Recording
```bash
cargo run --release -- \
    --mode recorder \
    --input-host 239.9.9.9 \
    --input-port 5000 \
    --output-file /path/to/recording.ts
```

## 🔧 Operation Modes

| Mode | Description | Required Args |
|------|-------------|---------------|
| `hls` | Process HLS streams with asset switching | `--asset-path`, `--hls-url` |
| `rtp` | Process RTP streams with video/audio mixing | `--asset-path` |
| `scte35` | Handle SCTE-35 ad insertion | `--ad-source` |
| `scheduler` | Schedule multiple video assets | None |
| `recorder` | Record multicast streams to file | `--output-file` |

## 🧪 Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_hls_pipeline
```

## 📊 Performance

- **Memory Safety** - Rust's ownership system prevents memory leaks and buffer overflows
- **Zero-Cost Abstractions** - Minimal runtime overhead compared to C/C++
- **Async I/O** - Non-blocking operations using Tokio
- **Efficient GStreamer Integration** - Direct bindings without unnecessary allocations

## 🔍 Debugging

Enable verbose logging:
```bash
RUST_LOG=debug cargo run --release -- --verbose
```

Enable GStreamer debug output:
```bash
GST_DEBUG=3 cargo run --release
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆚 Migration from Go

This Rust implementation provides equivalent functionality to the original Go version with several improvements:

- **Memory Safety** - No garbage collection pauses or memory leaks
- **Performance** - Better performance characteristics and lower latency
- **Type Safety** - Compile-time guarantees about data structures
- **Error Handling** - More robust error handling with proper propagation
- **Concurrency** - Better async/await support with Tokio

## 🐛 Troubleshooting

### Common Issues

1. **GStreamer not found** - Ensure GStreamer development headers are installed
2. **Plugin not found** - Install required GStreamer plugins
3. **Permission denied** - Check file permissions for asset files
4. **Network issues** - Verify multicast addresses and ports are accessible

### Getting Help

1. Check the [troubleshooting section](doc/README_RUST.md#troubleshooting)
2. Review the [API documentation](doc/README_RUST.md#api-reference)
3. Open an issue on GitHub
4. Check GStreamer documentation for pipeline-specific issues

## 📈 Roadmap

- [ ] Web-based configuration interface
- [ ] Docker containerization
- [ ] Kubernetes deployment manifests
- [ ] Prometheus metrics integration
- [ ] WebRTC support
- [ ] Advanced SCTE-35 message types
- [ ] Dynamic bitrate adaptation

## 🙏 Acknowledgments

- [GStreamer](https://gstreamer.freedesktop.org/) - The multimedia framework
- [Rust](https://www.rust-lang.org/) - The programming language
- [Tokio](https://tokio.rs/) - The async runtime
- [Clap](https://github.com/clap-rs/clap) - Command line argument parsing

---

**Made with ❤️ in Rust**

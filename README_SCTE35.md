# SCTE-35 Ad Insertion Handler

This Go application provides SCTE-35 message handling and automatic ad insertion for UDP video streams. It listens for SCTE-35 messages on a UDP port and automatically inserts ads when triggered.

## Features

- **UDP Input/Output**: Receives UDP video streams and outputs processed streams via UDP
- **SCTE-35 Parsing**: Parses SCTE-35 messages to detect ad insertion triggers
- **Automatic Ad Insertion**: Automatically switches to ad content when SCTE-35 messages are received
- **GStreamer Pipeline**: Uses GStreamer for video processing and streaming
- **Configurable**: Adjustable ad duration and source files

## Architecture

The system consists of two main pipelines:

1. **Main Pipeline**: Processes the normal video stream (input → output)
2. **Ad Pipeline**: Plays ad content when triggered by SCTE-35 messages

When a SCTE-35 message is received:
1. The main stream is paused
2. The ad pipeline starts playing the ad content
3. After the ad duration, the main stream resumes

## Usage

### Basic Usage

```go
// Create SCTE-35 handler
handler, err := NewAdInsertionHandler(
    "239.1.1.5",  // Input multicast address
    5000,         // Input port
    "239.1.1.6",  // Output multicast address  
    5002,         // Output port
    "/path/to/ad.mp4" // Ad source file
)
if err != nil {
    log.Fatal(err)
}

// Configure ad duration (optional)
handler.SetAdDuration(30 * time.Second)

// Start the handler
if err := handler.Start(); err != nil {
    log.Fatal(err)
}

// ... your application logic ...

// Stop the handler
handler.Stop()
```

### Port Configuration

- **Main Stream Input**: `inputHost:inputPort` (e.g., 239.1.1.5:5000)
- **SCTE-35 Messages**: `inputHost:(inputPort+1000)` (e.g., 239.1.1.5:6000)
- **Output Stream**: `outputHost:outputPort` (e.g., 239.1.1.6:5002)

## SCTE-35 Message Format

The handler currently supports these SCTE-35 command types:

- `0x05`: Splice Insert - Triggers ad insertion
- `0x06`: Splice Null - No action
- `0x07`: Splice Schedule - Triggers ad insertion

### Test SCTE-35 Message

To test ad insertion, you can send a simple SCTE-35 message:

```bash
# Example using netcat
echo -n '\xFC\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x05' | nc -u 239.1.1.5 6000
```

## Requirements

- Go 1.24.2 or later
- GStreamer with required plugins:
  - `gst-plugins-base`
  - `gst-plugins-good`
  - `gst-plugins-bad`
  - `gst-plugins-ugly`
  - `gst-libav`

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   go mod tidy
   ```
3. Build the application:
   ```bash
   go build -o scte35-handler .
   ```

## Running

1. Start the SCTE-35 handler:
   ```bash
   ./scte35-handler
   ```

2. Send SCTE-35 messages to trigger ad insertion

3. Monitor the output stream for ad insertions

## Configuration

### Ad Source File

The ad source file should be a video file compatible with GStreamer (MP4, AVI, etc.). The file should have:
- Video: H.264 encoded
- Audio: AAC encoded
- Duration: Should match or be shorter than the configured ad duration

### Network Configuration

- Use multicast addresses for input/output streams
- Ensure firewall allows UDP traffic on configured ports
- SCTE-35 messages use a separate port (input port + 1000)

## Troubleshooting

### Common Issues

1. **Pipeline Creation Failed**: Check GStreamer installation and plugins
2. **Network Issues**: Verify multicast addresses and firewall settings
3. **Ad Not Playing**: Check ad file path and format compatibility
4. **SCTE-35 Not Detected**: Verify SCTE-35 message format and port configuration

### Debugging

Enable debug output by setting the `GST_DEBUG` environment variable:

```bash
export GST_DEBUG=3
./scte35-handler
```

## Example Integration

See `example_scte35.go` for a complete example of how to integrate the SCTE-35 handler into your application.

## License

This project is part of the video-scheduler-gstreamer project. 
# GStreamer Pipeline with Compositor and Audio Mixer

This enhanced GStreamer pipeline now includes compositor and audio mixer functionality for input switching, with support for playing local asset video files.

## Features

- **Compositor**: Video mixing with seamless switching between RTP stream and local asset
- **Audio Mixer**: Audio mixing for smooth transitions between inputs
- **Automatic Switching**: Automatically switches to local asset after 1 minute
- **Input Switching**: Manual control to switch between RTP stream and local asset
- **Seamless Transitions**: Uses alpha blending for smooth video transitions

## Architecture

The pipeline uses the following key components:

1. **Intervideosink/Intervideosrc**: For video stream routing between pipelines
2. **Interaudiosink/Interaudiosrc**: For audio stream routing between pipelines
3. **Compositor**: Video mixer with alpha blending for seamless switching
4. **Audiomixer**: Audio mixer for combining multiple audio streams
5. **Playbin3**: For playing local asset video files

## Usage

### Basic Usage

```bash
go run . -input-host 127.0.0.1 -input-port 5004 -output-host 127.0.0.1 -output-port 5006 -asset /path/to/your/video.mp4
```

### Command Line Options

- `-input-host`: Input RTP stream host (default: 127.0.0.1)
- `-input-port`: Input RTP stream port (default: 5004)
- `-output-host`: Output RTP stream host (default: 127.0.0.1)
- `-output-port`: Output RTP stream port (default: 5006)
- `-asset`: Path to local asset video file (required)

### Example

```bash
# Start pipeline with local asset
go run . -input-host 239.1.1.1 -input-port 5002 -output-host 239.2.2.2 -output-port 6000 -asset /home/user/videos/promo.mp4
```

## Pipeline Flow

1. **Startup**: Pipeline starts with RTP stream as input
2. **1 Minute Timer**: After 1 minute, automatically switches to local asset
3. **Asset Playback**: Local asset plays until completion
4. **Return to RTP**: Automatically returns to RTP stream when asset finishes
5. **Manual Control**: Can manually switch between inputs using provided methods

## API Methods

### GStreamerPipeline Methods

- `switchToAsset()`: Manually switch to local asset video
- `switchToRTP()`: Manually switch back to RTP stream
- `stopAsset()`: Stop asset playback and return to RTP
- `Start()`: Start the pipeline (includes 1-minute timer)
- `Stop()`: Stop the pipeline and cleanup

### Example Programmatic Usage

```go
// Create pipeline
pipeline, err := NewGStreamerPipeline("127.0.0.1", 5004, "127.0.0.1", 5006, "/path/to/video.mp4")
if err != nil {
    log.Fatal(err)
}

// Start pipeline (will auto-switch to asset after 1 minute)
err = pipeline.Start()
if err != nil {
    log.Fatal(err)
}

// Manual switching (optional)
time.Sleep(30 * time.Second)
pipeline.switchToAsset()  // Switch to asset immediately

time.Sleep(10 * time.Second)
pipeline.switchToRTP()    // Switch back to RTP

// Stop pipeline
pipeline.Stop()
```

## Video Format Requirements

The local asset video file should be in a format supported by GStreamer's playbin3:

- **Video**: H.264, MPEG-2, or other common formats
- **Audio**: AAC, MP3, or other common formats
- **Container**: MP4, AVI, MOV, or other common containers

## Audio Mixing

The audio mixer automatically handles:
- Audio format conversion
- Sample rate matching
- Channel count alignment
- Smooth transitions between inputs

## Video Compositing

The compositor provides:
- Full-screen video switching
- Alpha blending for smooth transitions
- 1920x1080 output resolution
- Black background for unused inputs

## Error Handling

The pipeline includes comprehensive error handling:
- Asset file validation
- Pipeline state monitoring
- Automatic fallback to RTP on asset errors
- Graceful shutdown handling

## Performance Considerations

- **Memory**: Each input stream uses separate queues for buffering
- **CPU**: Video transcoding and audio mixing require CPU resources
- **Network**: RTP streams should have sufficient bandwidth
- **Storage**: Local asset files should be on fast storage

## Troubleshooting

### Common Issues

1. **Asset file not found**: Ensure the asset path is correct and file exists
2. **Pipeline won't start**: Check GStreamer installation and dependencies
3. **No video output**: Verify input RTP stream is active
4. **Audio issues**: Check audio format compatibility

### Debug Mode

Enable verbose logging by setting the GST_DEBUG environment variable:

```bash
export GST_DEBUG=3
go run . -asset /path/to/video.mp4
```

## Dependencies

- GStreamer 1.0+ with the following plugins:
  - gst-plugins-base
  - gst-plugins-good
  - gst-plugins-bad
  - gst-plugins-ugly
  - gst-libav

## License

This project is part of the video-scheduler-gstreamer project. 
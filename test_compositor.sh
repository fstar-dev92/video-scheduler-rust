#!/bin/bash

# Test script for GStreamer Pipeline with Compositor and Audio Mixer

echo "=== GStreamer Pipeline with Compositor and Audio Mixer Test ==="
echo ""

# Check if asset file is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <path_to_video_file>"
    echo "Example: $0 /path/to/your/video.mp4"
    exit 1
fi

ASSET_FILE="$1"

# Check if asset file exists
if [ ! -f "$ASSET_FILE" ]; then
    echo "Error: Asset file '$ASSET_FILE' does not exist"
    exit 1
fi

echo "Asset file: $ASSET_FILE"
echo ""

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "Error: Go is not installed"
    exit 1
fi

# Check if GStreamer is installed
if ! command -v gst-launch-1.0 &> /dev/null; then
    echo "Error: GStreamer is not installed"
    exit 1
fi

echo "Dependencies check passed"
echo ""

# Set GStreamer debug level for verbose output
export GST_DEBUG=2

echo "Starting GStreamer Pipeline..."
echo "Input: 127.0.0.1:5004"
echo "Output: 127.0.0.1:5006"
echo "Asset: $ASSET_FILE"
echo "Pipeline will switch to asset after 1 minute"
echo ""
echo "Press Ctrl+C to stop the pipeline"
echo ""

# Run the pipeline
go run . -input-host 127.0.0.1 -input-port 5004 -output-host 127.0.0.1 -output-port 5006 -asset "$ASSET_FILE" 
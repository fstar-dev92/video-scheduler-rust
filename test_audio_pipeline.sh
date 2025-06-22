#!/bin/bash

echo "Testing GStreamer audio pipeline components..."

# Check if required audio elements are available
echo "Checking for required audio elements:"

# Check AAC decoder
if gst-inspect-1.0 avdec_aac > /dev/null 2>&1; then
    echo "✓ avdec_aac is available"
else
    echo "✗ avdec_aac is NOT available"
fi

# Check AAC encoder
if gst-inspect-1.0 voaacenc > /dev/null 2>&1; then
    echo "✓ voaacenc is available"
else
    echo "✗ voaacenc is NOT available"
fi

# Check AAC parser
if gst-inspect-1.0 aacparse > /dev/null 2>&1; then
    echo "✓ aacparse is available"
else
    echo "✗ aacparse is NOT available"
fi

# Check audio converter
if gst-inspect-1.0 audioconvert > /dev/null 2>&1; then
    echo "✓ audioconvert is available"
else
    echo "✗ audioconvert is NOT available"
fi

# Check audio resampler
if gst-inspect-1.0 audioresample > /dev/null 2>&1; then
    echo "✓ audioresample is available"
else
    echo "✗ audioresample is NOT available"
fi

# Check MPEG-TS muxer
if gst-inspect-1.0 mpegtsmux > /dev/null 2>&1; then
    echo "✓ mpegtsmux is available"
else
    echo "✗ mpegtsmux is NOT available"
fi

echo ""
echo "Testing audio pipeline with a simple test..."

# Create a simple test pipeline to verify audio processing
TEST_PIPELINE="audiotestsrc ! audioconvert ! audioresample ! voaacenc ! aacparse ! fakesink"

if gst-launch-1.0 --gst-debug=2 $TEST_PIPELINE > /dev/null 2>&1; then
    echo "✓ Audio pipeline test passed"
else
    echo "✗ Audio pipeline test failed"
    echo "Running with debug output:"
    gst-launch-1.0 --gst-debug=2 $TEST_PIPELINE
fi

echo ""
echo "Audio pipeline test completed." 
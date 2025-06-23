package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/go-gst/go-gst/gst"
)

// Setup logging and panic recovery
func init() {
	// Set up logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(os.Stdout)

	// Set up panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC RECOVERED: %v", r)
			log.Printf("Stack trace: %s", debug.Stack())
			os.Exit(1)
		}
	}()
}

// Debug helper functions
func logMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("Memory Stats - Alloc: %d MB, TotalAlloc: %d MB, Sys: %d MB, NumGC: %d",
		m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.NumGC)
}

func logGoroutineCount() {
	log.Printf("Goroutines: %d", runtime.NumGoroutine())
}

// Safe element creation with logging
func safeNewElement(factoryName string) (*gst.Element, error) {
	log.Printf("Creating element: %s", factoryName)
	element, err := gst.NewElement(factoryName)
	if err != nil {
		log.Printf("Failed to create element %s: %v", factoryName, err)
		return nil, err
	}
	log.Printf("Successfully created element: %s", factoryName)
	return element, nil
}

// Safe element linking with logging
func safeLink(src, sink *gst.Element, description string) error {
	log.Printf("Linking: %s", description)
	if err := src.Link(sink); err != nil {
		log.Printf("Failed to link %s: %v", description, err)
		return err
	}
	log.Printf("Successfully linked: %s", description)
	return nil
}

// GStreamerPipeline represents a GStreamer pipeline for RTP stream processing with compositor and audio mixer
type GStreamerPipeline struct {
	pipeline       *gst.Pipeline
	demux          *gst.Element
	mux            *gst.Element
	compositor     *gst.Element
	audiomixer     *gst.Element
	assetPipeline  *gst.Pipeline
	mutex          sync.Mutex
	padMutex       sync.Mutex // Mutex for protecting pad operations
	assetVideoPath string
	currentInput   string // "rtp" or "asset"
	assetPlaying   bool
	stopChan       chan struct{}
	running        bool
	rtpConnected   bool        // Track if RTP stream is connected
	rtpTimeout     *time.Timer // Timeout for RTP connection
	pipelineID     string      // Add this field
}

// NewGStreamerPipeline creates a new GStreamer pipeline for RTP processing with compositor and audio mixer
func NewGStreamerPipeline(inputHost string, inputPort int, outputHost string, outputPort int, assetVideoPath string) (*GStreamerPipeline, error) {
	// Generate a unique pipeline ID (you could also accept this as a parameter)
	pipelineID := fmt.Sprintf("pipeline_%d", time.Now().UnixNano())

	// Create pipeline with unique name
	pipeline, err := gst.NewPipeline(fmt.Sprintf("rtp-pipeline-%s", pipelineID))
	if err != nil {
		return nil, fmt.Errorf("failed to create pipeline: %v", err)
	}

	// Create the pipeline instance
	gp := &GStreamerPipeline{
		pipeline:       pipeline,
		assetVideoPath: assetVideoPath,
		currentInput:   "rtp",
		assetPlaying:   false,
		stopChan:       make(chan struct{}),
		running:        false,
		rtpConnected:   false,
		rtpTimeout:     time.NewTimer(30 * time.Second),
		pipelineID:     pipelineID, // Set the pipeline ID
	}

	// Create elements with unique names
	udpsrc, err := gst.NewElementWithProperties("udpsrc", map[string]interface{}{
		"name": fmt.Sprintf("udpsrc_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create udpsrc: %v", err)
	}

	// Create capsfilter for UDP source to ensure proper format negotiation
	udpCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"name": fmt.Sprintf("udpCapsfilter_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create udp capsfilter: %v", err)
	}

	rtpmp2tdepay, err := gst.NewElementWithProperties("rtpmp2tdepay", map[string]interface{}{
		"name": fmt.Sprintf("rtpmp2tdepay_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create rtpmp2tdepay: %v", err)
	}

	tsdemux, err := gst.NewElementWithProperties("tsdemux", map[string]interface{}{
		"name":               fmt.Sprintf("tsdemux_%s", pipelineID),
		"send-scte35-events": true,
		"latency":            1000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create tsdemux: %v", err)
	}

	// Create intervideosink for RTP stream (input1)
	intervideosink1, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel":      "input1",
		"sync":         false, // Disable sync for better real-time performance
		"name":         fmt.Sprintf("intervideosink1_%s", pipelineID),
		"max-lateness": int64(20 * 1000000),
		"qos":          true,
		"async":        false, // Disable async for better timing
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create intervideosink1: %v", err)
	}

	// Create interaudiosink for RTP stream (audio1)
	interaudiosink1, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel":      "audio1",
		"sync":         false, // Disable sync for better real-time performance
		"name":         fmt.Sprintf("interaudiosink1_%s", pipelineID),
		"max-lateness": int64(20 * 1000000),
		"qos":          true,
		"async":        false, // Disable async for better timing
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create interaudiosink1: %v", err)
	}

	// Create intervideosrc for RTP stream (input1)
	intervideo1, err := gst.NewElementWithProperties("intervideosrc", map[string]interface{}{
		"channel":      "input1",
		"do-timestamp": true,
		"name":         fmt.Sprintf("intervideosrc1_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create intervideosrc1: %v", err)
	}

	// Create intervideosrc for asset stream (input2)
	intervideo2, err := gst.NewElementWithProperties("intervideosrc", map[string]interface{}{
		"channel":      "input2",
		"do-timestamp": true,
		"name":         fmt.Sprintf("intervideosrc2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create intervideosrc2: %v", err)
	}

	// Create interaudiosrc for RTP stream (audio1)
	interaudio1, err := gst.NewElementWithProperties("interaudiosrc", map[string]interface{}{
		"channel":      "audio1",
		"do-timestamp": true,
		"name":         fmt.Sprintf("interaudiosrc1_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create interaudiosrc1: %v", err)
	}

	// Create interaudiosrc for asset stream (audio2)
	interaudio2, err := gst.NewElementWithProperties("interaudiosrc", map[string]interface{}{
		"channel":      "audio2",
		"do-timestamp": true,
		"name":         fmt.Sprintf("interaudiosrc2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create interaudiosrc2: %v", err)
	}

	// Create video mixer (compositor)
	compositor, err := gst.NewElementWithProperties("compositor", map[string]interface{}{
		"background":            1,
		"zero-size-is-unscaled": true,
		"name":                  fmt.Sprintf("compositor_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create compositor: %v", err)
	}

	// Configure compositor sink pads for proper positioning with null checks
	pad1 := compositor.GetStaticPad("sink_0")
	if pad1 != nil {
		pad1.SetProperty("xpos", 0)
		pad1.SetProperty("ypos", 0)
		pad1.SetProperty("width", 1920)
		pad1.SetProperty("height", 1080)
		pad1.SetProperty("alpha", 1.0) // input1 visible
	}

	pad2 := compositor.GetStaticPad("sink_1")
	if pad2 != nil {
		pad2.SetProperty("xpos", 0)
		pad2.SetProperty("ypos", 0)
		pad2.SetProperty("width", 1920)
		pad2.SetProperty("height", 1080)
		pad2.SetProperty("alpha", 0.0) // input2 hidden
	}

	// Create audio mixer
	audiomixer, err := gst.NewElementWithProperties("audiomixer", map[string]interface{}{
		"name": fmt.Sprintf("audiomixer_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audiomixer: %v", err)
	}

	// Create video processing elements for RTP stream (input1)
	videoQueue1, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("videoQueue1_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create video queue 1: %v", err)
	}
	videoQueue1.SetProperty("max-size-buffers", 100)
	videoQueue1.SetProperty("max-size-time", uint64(700*1000000)) // 200ms instead of 500ms
	videoQueue1.SetProperty("min-threshold-time", uint64(50*1000000))

	// Create video processing elements for asset stream (input2)
	videoQueue2, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("videoQueue2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create video queue 2: %v", err)
	}
	videoQueue2.SetProperty("max-size-buffers", 100)
	videoQueue2.SetProperty("max-size-time", uint64(700*1000000)) // 200ms instead of 500ms
	videoQueue2.SetProperty("min-threshold-time", uint64(50*1000000))

	// Create video processing chain elements
	videoconvert, err := gst.NewElementWithProperties("videoconvert", map[string]interface{}{
		"name": fmt.Sprintf("videoconvert_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create videoconvert: %v", err)
	}

	x264enc, err := gst.NewElementWithProperties("x264enc", map[string]interface{}{
		"name": fmt.Sprintf("x264enc_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create x264enc: %v", err)
	}

	h264parse2, err := gst.NewElementWithProperties("h264parse", map[string]interface{}{
		"name": fmt.Sprintf("h264parse2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create h264parse2: %v", err)
	}

	// Video queue before muxer for better synchronization
	videoMuxerQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("videoMuxerQueue_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create video muxer queue: %v", err)
	}
	videoMuxerQueue.SetProperty("max-size-buffers", 500) // Increase buffer limit
	videoMuxerQueue.SetProperty("max-size-time", uint64(500*1000000))
	videoMuxerQueue.SetProperty("min-threshold-time", uint64(50*1000000))
	videoMuxerQueue.SetProperty("sync", false)       // Disable sync for real-time
	videoMuxerQueue.SetProperty("leaky", 0)          // No leaking
	videoMuxerQueue.SetProperty("max-size-bytes", 0) // No byte limit

	// Create audio processing elements for RTP stream (audio1)
	audioQueue1, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("audioQueue1_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audio queue 1: %v", err)
	}
	audioQueue1.SetProperty("max-size-buffers", 100)
	audioQueue1.SetProperty("max-size-time", uint64(700*1000000)) // 200ms instead of 500ms
	audioQueue1.SetProperty("min-threshold-time", uint64(50*1000000))

	// Create audio processing elements for asset stream (audio2)
	audioQueue2, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("audioQueue2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audio queue 2: %v", err)
	}
	audioQueue2.SetProperty("max-size-buffers", 100)
	audioQueue2.SetProperty("max-size-time", uint64(700*1000000)) // 200ms instead of 500ms
	audioQueue2.SetProperty("min-threshold-time", uint64(50*1000000))

	// Create audio processing chain elements
	aacparse1, err := gst.NewElementWithProperties("aacparse", map[string]interface{}{
		"name": fmt.Sprintf("aacparse1_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create aacparse1: %v", err)
	}

	audioconvert, err := gst.NewElementWithProperties("audioconvert", map[string]interface{}{
		"name": fmt.Sprintf("audioconvert_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audioconvert: %v", err)
	}

	audioresample, err := gst.NewElementWithProperties("audioresample", map[string]interface{}{
		"name": fmt.Sprintf("audioresample_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audioresample: %v", err)
	}

	voaacenc, err := gst.NewElementWithProperties("avenc_aac", map[string]interface{}{
		"name": fmt.Sprintf("voaacenc_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create voaacenc: %v", err)
	}

	aacparse2, err := gst.NewElementWithProperties("aacparse", map[string]interface{}{
		"name": fmt.Sprintf("aacparse2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create aacparse2: %v", err)
	}

	// Audio queue before muxer
	audioMuxerQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("audioMuxerQueue_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audio muxer queue: %v", err)
	}
	audioMuxerQueue.SetProperty("max-size-buffers", 250) // Increase buffer limit
	audioMuxerQueue.SetProperty("max-size-time", uint64(500*1000000))
	audioMuxerQueue.SetProperty("min-threshold-time", uint64(50*1000000))
	audioMuxerQueue.SetProperty("sync", false)       // Disable sync for real-time
	audioMuxerQueue.SetProperty("leaky", 0)          // No leaking
	audioMuxerQueue.SetProperty("max-size-bytes", 0) // No byte limit

	// Muxer and output elements
	mpegtsmux, err := gst.NewElementWithProperties("mpegtsmux", map[string]interface{}{
		"name": fmt.Sprintf("mpegtsmux_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create mpegtsmux: %v", err)
	}

	rtpmp2tpay, err := gst.NewElementWithProperties("rtpmp2tpay", map[string]interface{}{
		"name": fmt.Sprintf("rtpmp2tpay_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create rtpmp2tpay: %v", err)
	}

	rtpbin, err := gst.NewElementWithProperties("rtpbin", map[string]interface{}{
		"latency":                uint(400), // Increase latency to handle jitter
		"do-retransmission":      true,      // Enable retransmission
		"rtp-profile":            2,         // AVP profile
		"ntp-sync":               true,      // Enable NTP sync
		"ntp-time-source":        3,         // Use running time
		"max-rtcp-rtp-time-diff": 1000,      // Max allowed drift
		"max-dropout-time":       45000,     // Max time to wait for missing packets
		"max-misorder-time":      5000,      // Max reordering time
		"buffer-mode":            1,         // Slave receiver mode
		"do-lost":                true,      // Handle packet loss
		"rtcp-sync-send-time":    true,      // Send RTCP sync packets
		"name":                   fmt.Sprintf("rtpbin_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create rtpbin: %v", err)
	}

	udpsink, err := gst.NewElementWithProperties("udpsink", map[string]interface{}{
		"host":           outputHost,
		"port":           outputPort,
		"sync":           false, // Disable sync for better real-time performance
		"buffer-size":    524288,
		"auto-multicast": true,
		"name":           fmt.Sprintf("udpsink_%s", pipelineID),
		"async":          false, // Disable async for better timing
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create udpsink: %v", err)
	}

	// Set properties
	udpsrc.SetProperty("address", inputHost)
	udpsrc.SetProperty("port", inputPort)
	// Use more flexible caps to avoid negotiation issues - accept any RTP stream
	udpsrc.SetProperty("caps", gst.NewCapsFromString("application/x-rtp"))
	udpsrc.SetProperty("timeout", uint64(5*1000000000)) // 5 second timeout
	udpsrc.SetProperty("retrieve-sender-address", false)
	udpsrc.SetProperty("auto-multicast", true)
	udpsrc.SetProperty("loop", false)
	udpsrc.SetProperty("buffer-size", 65536) // Increase buffer size

	// Set caps for UDP capsfilter - more flexible to handle different payload types
	udpCapsfilter.SetProperty("caps", gst.NewCapsFromString("application/x-rtp"))

	// Configure RTP depayloader for better compatibility
	rtpmp2tdepay.SetProperty("pt", 33) // MPEG-TS payload type
	rtpmp2tdepay.SetProperty("mtu", 1400)
	rtpmp2tdepay.SetProperty("perfect-rtptime", true)

	// Configure pipeline latency for proper synchronization
	// Use a more conservative latency setting to avoid conflicts
	pipeline.SetProperty("latency", int64(500*1000000)) // 500ms pipeline latency for live streaming

	x264enc.SetProperty("tune", "zerolatency")

	// Audio encoder properties
	voaacenc.SetProperty("bitrate", 128000) // 128 kbps
	voaacenc.SetProperty("channels", 2)     // Stereo

	// Audio resampler properties for better compatibility
	audioresample.SetProperty("quality", 10) // Highest quality

	// AAC parser properties
	aacparse1.SetProperty("outputformat", 0) // ADTS format
	aacparse2.SetProperty("outputformat", 0) // ADTS format

	// Configure MPEG-TS muxer for better compatibility
	mpegtsmux.SetProperty("alignment", 7)
	mpegtsmux.SetProperty("pat-interval", int64(27000*1000000)) // 100ms
	mpegtsmux.SetProperty("pmt-interval", int64(27000*1000000)) // 100ms
	mpegtsmux.SetProperty("pcr-interval", int64(2700*1000000))  // 20ms
	mpegtsmux.SetProperty("start-time", int64(500000000))
	mpegtsmux.SetProperty("si-interval", int64(500))

	// Configure RTP payloader for MPEG-TS output
	rtpmp2tpay.SetProperty("mtu", 1400)
	rtpmp2tpay.SetProperty("pt", 33)                // MPEG-TS payload type
	rtpmp2tpay.SetProperty("perfect-rtptime", true) // Perfect RTP timing

	// Create rtpbin for RTP session management
	rtpbin, err = gst.NewElementWithProperties("rtpbin", map[string]interface{}{
		"latency":                uint(400), // Increase latency to handle jitter
		"do-retransmission":      true,      // Enable retransmission
		"rtp-profile":            2,         // AVP profile
		"ntp-sync":               true,      // Enable NTP sync
		"ntp-time-source":        3,         // Use running time
		"max-rtcp-rtp-time-diff": 1000,      // Max allowed drift
		"max-dropout-time":       45000,     // Max time to wait for missing packets
		"max-misorder-time":      5000,      // Max reordering time
		"buffer-mode":            1,         // Slave receiver mode
		"do-lost":                true,      // Handle packet loss
		"rtcp-sync-send-time":    true,      // Send RTCP sync packets
		"name":                   fmt.Sprintf("rtpbin_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create rtpbin: %v", err)
	}

	// Add elements to pipeline
	elements := []*gst.Element{
		udpsrc, udpCapsfilter, rtpmp2tdepay, tsdemux,
		intervideosink1, interaudiosink1,
		intervideo1, intervideo2, compositor,
		interaudio1, interaudio2, audiomixer,
		videoQueue1, videoQueue2, videoconvert, x264enc, h264parse2, videoMuxerQueue,
		audioQueue1, audioQueue2, aacparse1, audioconvert, audioresample, voaacenc, aacparse2,
		audioMuxerQueue, mpegtsmux, rtpmp2tpay,
		rtpbin, udpsink,
	}

	for _, element := range elements {
		if err := pipeline.Add(element); err != nil {
			return nil, fmt.Errorf("failed to add element to pipeline: %v", err)
		}
	}

	// Link elements (except demuxer which needs dynamic linking)
	if err := udpsrc.Link(udpCapsfilter); err != nil {
		return nil, fmt.Errorf("failed to link udpsrc to udpCapsfilter: %v", err)
	}

	if err := udpCapsfilter.Link(rtpmp2tdepay); err != nil {
		return nil, fmt.Errorf("failed to link udpCapsfilter to rtpmp2tdepay: %v", err)
	}

	if err := rtpmp2tdepay.Link(tsdemux); err != nil {
		return nil, fmt.Errorf("failed to link rtpmp2tdepay to tsdemux: %v", err)
	}

	// Link video elements directly (like in scheduler.go)
	if err := intervideo1.Link(videoQueue1); err != nil {
		return nil, fmt.Errorf("failed to link intervideo1 to videoQueue1: %v", err)
	}
	if err := intervideo2.Link(videoQueue2); err != nil {
		return nil, fmt.Errorf("failed to link intervideo2 to videoQueue2: %v", err)
	}

	// Link video processing chain
	if err := videoQueue1.Link(compositor); err != nil {
		return nil, fmt.Errorf("failed to link videoQueue1 to compositor: %v", err)
	}
	if err := videoQueue2.Link(compositor); err != nil {
		return nil, fmt.Errorf("failed to link videoQueue2 to compositor: %v", err)
	}
	if err := compositor.Link(videoconvert); err != nil {
		return nil, fmt.Errorf("failed to link compositor to videoconvert: %v", err)
	}
	if err := videoconvert.Link(x264enc); err != nil {
		return nil, fmt.Errorf("failed to link videoconvert to x264enc: %v", err)
	}
	if err := x264enc.Link(h264parse2); err != nil {
		return nil, fmt.Errorf("failed to link x264enc to h264parse2: %v", err)
	}
	if err := h264parse2.Link(videoMuxerQueue); err != nil {
		return nil, fmt.Errorf("failed to link h264parse2 to videoMuxerQueue: %v", err)
	}
	if err := videoMuxerQueue.Link(mpegtsmux); err != nil {
		return nil, fmt.Errorf("failed to link videoMuxerQueue to mpegtsmux: %v", err)
	}

	// Link audio elements directly (like in scheduler.go)
	if err := interaudio1.Link(audioQueue1); err != nil {
		return nil, fmt.Errorf("failed to link interaudio1 to audioQueue1: %v", err)
	}
	if err := interaudio2.Link(audioQueue2); err != nil {
		return nil, fmt.Errorf("failed to link interaudio2 to audioQueue2: %v", err)
	}

	// Link audio processing chain
	if err := audioQueue1.Link(audiomixer); err != nil {
		return nil, fmt.Errorf("failed to link audioQueue1 to audiomixer: %v", err)
	}
	if err := audioQueue2.Link(audiomixer); err != nil {
		return nil, fmt.Errorf("failed to link audioQueue2 to audiomixer: %v", err)
	}
	if err := audiomixer.Link(audioconvert); err != nil {
		return nil, fmt.Errorf("failed to link audiomixer to audioconvert: %v", err)
	}
	if err := audioconvert.Link(audioresample); err != nil {
		return nil, fmt.Errorf("failed to link audioconvert to audioresample: %v", err)
	}
	if err := audioresample.Link(voaacenc); err != nil {
		return nil, fmt.Errorf("failed to link audioresample to voaacenc: %v", err)
	}
	if err := voaacenc.Link(aacparse2); err != nil {
		return nil, fmt.Errorf("failed to link voaacenc to aacparse2: %v", err)
	}
	if err := aacparse2.Link(audioMuxerQueue); err != nil {
		return nil, fmt.Errorf("failed to link aacparse2 to audioMuxerQueue: %v", err)
	}
	if err := audioMuxerQueue.Link(mpegtsmux); err != nil {
		return nil, fmt.Errorf("failed to link audioMuxerQueue to mpegtsmux: %v", err)
	}

	// Link output branch
	if err := mpegtsmux.Link(rtpmp2tpay); err != nil {
		return nil, fmt.Errorf("failed to link mpegtsmux to rtpmp2tpay: %v", err)
	}

	sendRtpSinkPad := rtpbin.GetRequestPad("send_rtp_sink_0")
	if sendRtpSinkPad == nil {
		return nil, fmt.Errorf("could not get 'send_rtp_sink_0' pad from rtpbin")
	}

	rtpmp2tpaySrcPad := rtpmp2tpay.GetStaticPad("src")
	if rtpmp2tpaySrcPad == nil {
		return nil, fmt.Errorf("could not get 'src' pad from rtpmp2tpay")
	}
	if rtpmp2tpaySrcPad.Link(sendRtpSinkPad) != gst.PadLinkOK {
		return nil, fmt.Errorf("failed to link rtpmp2tpay src to rtpbin send_rtp_sink_0")
	}

	rtpSrcPad := rtpbin.GetStaticPad("send_rtp_src_0")
	if rtpSrcPad == nil {
		return nil, fmt.Errorf("could not get 'send_rtp_src_0' pad from rtpbin")
	}
	if rtpSrcPad.Link(udpsink.GetStaticPad("sink")) != gst.PadLinkOK {
		return nil, fmt.Errorf("failed to link rtpbin send_rtp_src_0 to udpsink")
	}

	// Set up dynamic pad-added signal for tsdemux with safer error handling
	tsdemux.Connect("pad-added", func(self *gst.Element, pad *gst.Pad) {
		fmt.Printf("[%s] ##############################################\n", pipelineID)
		// Use a goroutine to handle the pad addition asynchronously to avoid blocking the signal
		go func() {
			// Add a small delay to ensure the pad is fully created
			time.Sleep(10 * time.Millisecond)

			// Lock the pad mutex to ensure thread safety
			gp.padMutex.Lock()
			defer gp.padMutex.Unlock()

			// Check if pipeline is still running
			if !gp.running {
				fmt.Printf("[%s] Pipeline is not running, ignoring pad addition\n", pipelineID)
				return
			}

			if pad == nil {
				fmt.Printf("[%s] Warning: Received nil pad in pad-added signal\n", pipelineID)
				return
			}

			padName := pad.GetName()
			if padName == "" {
				fmt.Printf("[%s] Warning: Received pad with empty name\n", pipelineID)
				return
			}

			fmt.Printf("[%s] Demuxer pad added: %s\n", pipelineID, padName)

			// Check pad name to determine if it's video or audio
			if len(padName) >= 5 && padName[:5] == "video" {
				fmt.Printf("[%s] Creating video pipeline: demux -> queue -> input_capsfilter -> h264parse -> parse_queue -> capsfilter -> decoder -> output_capsfilter -> intervideosink\n", pipelineID)

				// Create queue for video demux output with better settings for high frame rate
				videoDemuxQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
					"name": fmt.Sprintf("videoDemuxQueue_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video demux queue: %v\n", pipelineID, err)
					return
				}
				// Increase buffer sizes for high frame rate (59.94 fps)
				videoDemuxQueue.SetProperty("max-size-buffers", 200)                   // Increased from 50
				videoDemuxQueue.SetProperty("max-size-time", uint64(500*1000000))      // Increased to 500ms
				videoDemuxQueue.SetProperty("min-threshold-time", uint64(100*1000000)) // Increased to 100ms
				videoDemuxQueue.SetProperty("sync", false)                             // Disable sync for real-time
				videoDemuxQueue.SetProperty("leaky", 2)                                // Leak downstream for better real-time performance

				// Create input capsfilter to ensure proper H.264 format before parsing
				videoInputCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
					"name": fmt.Sprintf("videoInputCapsfilter_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video input capsfilter: %v\n", pipelineID, err)
					return
				}
				// Set more flexible caps for H.264 video to handle various formats
				videoInputCapsfilter.SetProperty("caps", gst.NewCapsFromString("video/x-h264"))

				// Create h264parse for video demux output with better configuration
				videoH264parse, err := gst.NewElementWithProperties("h264parse", map[string]interface{}{
					"name": fmt.Sprintf("videoH264parse_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video h264parse: %v\n", pipelineID, err)
					return
				}
				// Use only valid h264parse properties
				videoH264parse.SetProperty("config-interval", -1)        // Send SPS/PPS with every IDR frame
				videoH264parse.SetProperty("disable-passthrough", false) // Allow passthrough when possible
				videoH264parse.SetProperty("update-timecode", true)      // Update timecode values

				// Create a buffer queue before h264parse to collect parameter sets
				videoParseQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
					"name": fmt.Sprintf("videoParseQueue_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video parse queue: %v\n", pipelineID, err)
					return
				}
				// Larger buffer to ensure we collect enough data including parameter sets
				videoParseQueue.SetProperty("max-size-buffers", 100)                   // Increased buffer size
				videoParseQueue.SetProperty("max-size-time", uint64(500*1000000))      // 500ms to collect more data
				videoParseQueue.SetProperty("min-threshold-time", uint64(200*1000000)) // 200ms threshold
				videoParseQueue.SetProperty("sync", false)                             // Disable sync for real-time
				videoParseQueue.SetProperty("leaky", 0)                                // No leaking to ensure parameter sets are preserved

				// Create capsfilter to ensure proper H.264 format
				videoCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
					"name": fmt.Sprintf("videoCapsfilter_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video capsfilter: %v\n", pipelineID, err)
					return
				}
				// Set caps for H.264 video with proper format
				videoCapsfilter.SetProperty("caps", gst.NewCapsFromString("video/x-h264,alignment=au,stream-format=byte-stream"))

				// Create H.264 decoder for video with better configuration
				videoDecoder, err := gst.NewElementWithProperties("avdec_h264", map[string]interface{}{
					"name": fmt.Sprintf("videoDecoder_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video decoder: %v\n", pipelineID, err)
					return
				}
				videoDecoder.SetProperty("sync", false) // Disable sync for real-time

				// Create output capsfilter for proper format - more flexible to handle actual video format
				videoOutputCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
					"name": fmt.Sprintf("videoOutputCapsfilter_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video output capsfilter: %v\n", pipelineID, err)
					return
				}
				// Set output caps for raw video - very flexible format to avoid conversion issues
				videoOutputCapsfilter.SetProperty("caps", gst.NewCapsFromString("video/x-raw"))

				// Add elements to pipeline
				if err := gp.pipeline.Add(videoDemuxQueue); err != nil {
					fmt.Printf("[%s] Failed to add video demux queue to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(videoInputCapsfilter); err != nil {
					fmt.Printf("[%s] Failed to add video input capsfilter to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(videoH264parse); err != nil {
					fmt.Printf("[%s] Failed to add video h264parse to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(videoParseQueue); err != nil {
					fmt.Printf("[%s] Failed to add video parse queue to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(videoCapsfilter); err != nil {
					fmt.Printf("[%s] Failed to add video capsfilter to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(videoDecoder); err != nil {
					fmt.Printf("[%s] Failed to add video decoder to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(videoOutputCapsfilter); err != nil {
					fmt.Printf("[%s] Failed to add video output capsfilter to pipeline: %v\n", pipelineID, err)
					return
				}

				// Set elements to PLAYING state
				if err := videoDemuxQueue.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set video demux queue state: %v\n", pipelineID, err)
					return
				}
				if err := videoInputCapsfilter.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set video input capsfilter state: %v\n", pipelineID, err)
					return
				}
				if err := videoH264parse.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set video h264parse state: %v\n", pipelineID, err)
					return
				}
				if err := videoParseQueue.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set video parse queue state: %v\n", pipelineID, err)
					return
				}
				if err := videoCapsfilter.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set video capsfilter state: %v\n", pipelineID, err)
					return
				}
				if err := videoDecoder.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set video decoder state: %v\n", pipelineID, err)
					return
				}
				if err := videoOutputCapsfilter.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set video output capsfilter state: %v\n", pipelineID, err)
					return
				}

				// Link the chain: demux -> queue -> input_capsfilter -> h264parse -> capsfilter -> decoder -> output_capsfilter -> intervideosink
				if pad.Link(videoDemuxQueue.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link video pad to queue\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked video pad to queue\n", pipelineID)

				if videoDemuxQueue.GetStaticPad("src").Link(videoInputCapsfilter.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link queue to input capsfilter\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked queue to input capsfilter\n", pipelineID)

				if videoInputCapsfilter.GetStaticPad("src").Link(videoH264parse.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link input capsfilter to h264parse\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked input capsfilter to h264parse\n", pipelineID)

				if videoH264parse.GetStaticPad("src").Link(videoParseQueue.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link h264parse to parse queue\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked h264parse to parse queue\n", pipelineID)

				if videoParseQueue.GetStaticPad("src").Link(videoCapsfilter.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link parse queue to capsfilter\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked parse queue to capsfilter\n", pipelineID)

				if videoCapsfilter.GetStaticPad("src").Link(videoDecoder.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link capsfilter to decoder\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked capsfilter to decoder\n", pipelineID)

				if videoDecoder.GetStaticPad("src").Link(videoOutputCapsfilter.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link decoder to output capsfilter\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked decoder to output capsfilter\n", pipelineID)

				if videoOutputCapsfilter.GetStaticPad("src").Link(intervideosink1.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link output capsfilter to intervideosink\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked output capsfilter to intervideosink\n", pipelineID)

				fmt.Printf("[%s] Successfully linked video pipeline: demux -> queue -> input_capsfilter -> h264parse -> parse_queue -> capsfilter -> decoder -> output_capsfilter -> intervideosink\n", pipelineID)
				// Mark RTP as connected if we haven't already
				if !gp.rtpConnected {
					gp.rtpConnected = true
					gp.rtpTimeout.Stop() // Stop the timeout timer
					fmt.Printf("[%s] RTP stream connected successfully\n", pipelineID)
				}

			} else if len(padName) >= 5 && padName[:5] == "audio" {
				fmt.Printf("[%s] Creating audio pipeline: demux -> queue -> aacparse -> decoder -> audioconvert -> capsfilter -> interaudiosink\n", pipelineID)

				// Create queue for audio demux output with better settings
				audioDemuxQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
					"name": fmt.Sprintf("audioDemuxQueue_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio demux queue: %v\n", pipelineID, err)
					return
				}
				// Better queue settings for audio
				audioDemuxQueue.SetProperty("max-size-buffers", 100)                   // Increased from 50
				audioDemuxQueue.SetProperty("max-size-time", uint64(300*1000000))      // Increased to 300ms
				audioDemuxQueue.SetProperty("min-threshold-time", uint64(100*1000000)) // Increased to 100ms
				audioDemuxQueue.SetProperty("sync", false)                             // Disable sync for real-time
				audioDemuxQueue.SetProperty("leaky", 2)                                // Leak downstream for better real-time performance

				// Create aacparse for audio demux output with better configuration
				audioAacparse, err := gst.NewElementWithProperties("aacparse", map[string]interface{}{
					"name": fmt.Sprintf("audioAacparse_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio aacparse: %v\n", pipelineID, err)
					return
				}
				audioAacparse.SetProperty("sync", false) // Disable sync for real-time

				// Create AAC decoder for audio
				audioDecoder, err := gst.NewElementWithProperties("avdec_aac", map[string]interface{}{
					"name": fmt.Sprintf("audioDecoder_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio decoder: %v\n", pipelineID, err)
					return
				}
				audioDecoder.SetProperty("sync", false) // Disable sync for real-time

				// Create audioconvert for format conversion if needed
				audioConvert, err := gst.NewElementWithProperties("audioconvert", map[string]interface{}{
					"name": fmt.Sprintf("audioConvert_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio convert: %v\n", pipelineID, err)
					return
				}
				audioConvert.SetProperty("sync", false) // Disable sync for real-time

				// Create audio resampler for rate conversion
				audioResample, err := gst.NewElementWithProperties("audioresample", map[string]interface{}{
					"name": fmt.Sprintf("audioResample_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio resample: %v\n", pipelineID, err)
					return
				}
				audioResample.SetProperty("sync", false) // Disable sync for real-time

				// Create audio capsfilter for format conversion
				audioCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
					"name": fmt.Sprintf("audioCapsfilter_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio capsfilter: %v\n", pipelineID, err)
					return
				}
				// Use standard audio format compatible with audiomixer - handle both mono and stereo
				audioCapsfilter.SetProperty("caps", gst.NewCapsFromString("audio/x-raw,format=S16LE,rate=48000,channels=2,layout=interleaved"))

				// Add elements to pipeline
				if err := gp.pipeline.Add(audioDemuxQueue); err != nil {
					fmt.Printf("[%s] Failed to add audio demux queue to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(audioAacparse); err != nil {
					fmt.Printf("[%s] Failed to add audio aacparse to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(audioDecoder); err != nil {
					fmt.Printf("[%s] Failed to add audio decoder to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(audioConvert); err != nil {
					fmt.Printf("[%s] Failed to add audio convert to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(audioResample); err != nil {
					fmt.Printf("[%s] Failed to add audio resample to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := gp.pipeline.Add(audioCapsfilter); err != nil {
					fmt.Printf("[%s] Failed to add audio capsfilter to pipeline: %v\n", pipelineID, err)
					return
				}

				// Set elements to PLAYING state
				if err := audioDemuxQueue.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audio demux queue state: %v\n", pipelineID, err)
					return
				}
				if err := audioAacparse.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audio aacparse state: %v\n", pipelineID, err)
					return
				}
				if err := audioDecoder.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audio decoder state: %v\n", pipelineID, err)
					return
				}
				if err := audioConvert.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audio convert state: %v\n", pipelineID, err)
					return
				}
				if err := audioResample.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audio resample state: %v\n", pipelineID, err)
					return
				}
				if err := audioCapsfilter.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audio capsfilter state: %v\n", pipelineID, err)
					return
				}

				// Link the chain: demux -> queue -> aacparse -> decoder -> audioconvert -> audioresample -> capsfilter -> interaudiosink
				if pad.Link(audioDemuxQueue.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link audio pad to queue\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked audio pad to queue\n", pipelineID)

				if audioDemuxQueue.GetStaticPad("src").Link(audioAacparse.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link queue to aacparse\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked queue to aacparse\n", pipelineID)

				if audioAacparse.GetStaticPad("src").Link(audioDecoder.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link aacparse to decoder\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked aacparse to decoder\n", pipelineID)

				if audioDecoder.GetStaticPad("src").Link(audioConvert.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link decoder to audioconvert\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked decoder to audioconvert\n", pipelineID)

				if audioConvert.GetStaticPad("src").Link(audioResample.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link audioconvert to audioresample\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked audioconvert to audioresample\n", pipelineID)

				if audioResample.GetStaticPad("src").Link(audioCapsfilter.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link audioresample to capsfilter\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked audioresample to capsfilter\n", pipelineID)

				if audioCapsfilter.GetStaticPad("src").Link(interaudiosink1.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link capsfilter to interaudiosink\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked capsfilter to interaudiosink\n", pipelineID)

				fmt.Printf("[%s] Successfully linked audio pipeline: demux -> queue -> aacparse -> decoder -> audioconvert -> audioresample -> capsfilter -> interaudiosink\n", pipelineID)
				// Mark RTP as connected if we haven't already
				if !gp.rtpConnected {
					gp.rtpConnected = true
					gp.rtpTimeout.Stop() // Stop the timeout timer
					fmt.Printf("[%s] RTP stream connected successfully\n", pipelineID)
				}

			} else {
				fmt.Printf("[%s] Unknown demuxer pad: %s\n", pipelineID, padName)
			}
		}()
	})

	// Set up bus watch for the main pipeline with safer message handling
	bus := pipeline.GetBus()
	go func() {
		for {
			msg := bus.TimedPop(gst.ClockTimeNone)
			if msg == nil {
				break
			}

			switch msg.Type() {
			case gst.MessageStateChanged:
				oldState, newState := msg.ParseStateChanged()
				fmt.Printf("[%s] Pipeline state changed: %s -> %s\n", pipelineID, oldState.String(), newState.String())

				// If pipeline fails to go to PLAYING state, it might be due to no RTP data
				if newState == gst.StatePaused && oldState == gst.StateReady {
					fmt.Printf("[%s] Pipeline stuck in PAUSED state - likely no RTP data received\n", pipelineID)
					// Give it a bit more time, then switch to asset if still no RTP
					go func() {
						time.Sleep(5 * time.Second)
						if gp.running && !gp.rtpConnected {
							fmt.Printf("[%s] Pipeline still not receiving RTP data, switching to asset\n", pipelineID)
							gp.switchToAsset()
						}
					}()
				}

				// If pipeline goes from PLAYING to PAUSED, it might be due to no RTP data
				if newState == gst.StatePaused && oldState == gst.StatePlaying {
					fmt.Printf("[%s] Pipeline went from PLAYING to PAUSED - checking for RTP data issues\n", pipelineID)
					go func() {
						time.Sleep(2 * time.Second)
						if gp.running && !gp.rtpConnected {
							fmt.Printf("[%s] Pipeline paused due to no RTP data, switching to asset\n", pipelineID)
							gp.switchToAsset()
						}
					}()
				}
			case gst.MessageError:
				gerr := msg.ParseError()
				fmt.Printf("[%s] Pipeline error: %s\n", pipelineID, gerr.Error())

				// Check if error is related to missing RTP data or negotiation issues
				errorMsg := gerr.Error()
				if strings.Contains(errorMsg, "not-negotiated") || strings.Contains(errorMsg, "Internal data stream error") {
					fmt.Printf("[%s] Pipeline negotiation error detected - this may be due to no RTP data or format issues\n", pipelineID)
					// Give it a bit more time to see if RTP data arrives
					go func() {
						time.Sleep(3 * time.Second)
						if gp.running && !gp.rtpConnected {
							fmt.Printf("[%s] Still no RTP connection after negotiation error, switching to asset\n", pipelineID)
							gp.switchToAsset()
						}
					}()
				} else if !gp.rtpConnected && (strings.Contains(errorMsg, "no pads") || strings.Contains(errorMsg, "not linked") || strings.Contains(errorMsg, "streaming")) {
					fmt.Printf("[%s] Pipeline error likely due to no RTP data - switching to asset\n", pipelineID)
					gp.switchToAsset()
				}
			case gst.MessageWarning:
				gwarn := msg.ParseWarning()
				fmt.Printf("[%s] Pipeline warning: %s\n", pipelineID, gwarn.Error())
			case gst.MessageInfo:
				ginfo := msg.ParseInfo()
				fmt.Printf("[%s] Pipeline info: %s\n", pipelineID, ginfo.Error())
			case gst.MessageEOS:
				fmt.Printf("[%s] Pipeline reached end of stream\n", pipelineID)
			}
		}
	}()

	// Assign elements to the pipeline instance
	gp.demux = tsdemux
	gp.mux = mpegtsmux
	gp.compositor = compositor
	gp.audiomixer = audiomixer
	// gp.pipeline.SetProperty("clock", gst.NewSystemClock())
	fmt.Printf("[%s] Pipeline created successfully\n", pipelineID)
	return gp, nil
}

// createAssetPipeline creates a pipeline for playing the local asset video file
func (gp *GStreamerPipeline) createAssetPipeline() error {
	pipeline, err := gst.NewPipeline(fmt.Sprintf("asset-pipeline-%s", gp.pipelineID))
	if err != nil {
		return fmt.Errorf("failed to create asset pipeline: %v", err)
	}

	// Create playbin for asset file
	playbin, err := gst.NewElementWithProperties("playbin3", map[string]interface{}{
		"uri":  fmt.Sprintf("file://%s", gp.assetVideoPath),
		"name": fmt.Sprintf("playbin_%s", gp.pipelineID),
	})
	if err != nil {
		return fmt.Errorf("failed to create playbin: %v", err)
	}

	// Create video sink
	intervideosink, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel": "input2",
		"sync":    false,
		"name":    fmt.Sprintf("asset_intervideosink_%s", gp.pipelineID),
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosink: %v", err)
	}

	// Create audio sink
	interaudiosink, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel": "audio2",
		"sync":    false,
		"name":    fmt.Sprintf("asset_interaudiosink_%s", gp.pipelineID),
	})
	if err != nil {
		return fmt.Errorf("failed to create interaudiosink: %v", err)
	}

	// Set sinks on playbin
	playbin.SetProperty("video-sink", intervideosink)
	playbin.SetProperty("audio-sink", interaudiosink)

	// Add playbin to pipeline
	if err := pipeline.Add(playbin); err != nil {
		return fmt.Errorf("failed to add playbin to pipeline: %v", err)
	}

	// Set up bus watch with safer message handling
	bus := pipeline.GetBus()
	go func() {
		for {
			msg := bus.TimedPop(gst.ClockTimeNone)
			if msg == nil {
				break
			}

			switch msg.Type() {
			case gst.MessageStateChanged:
				oldState, newState := msg.ParseStateChanged()
				if newState == gst.StatePlaying && oldState != gst.StatePaused {
					fmt.Printf("[%s] Asset pipeline is now PLAYING\n", gp.pipelineID)
				}
			case gst.MessageError:
				gerr := msg.ParseError()
				fmt.Printf("[%s] Asset pipeline error: %s\n", gp.pipelineID, gerr.Error())
				gp.stopAsset()
			case gst.MessageEOS:
				fmt.Printf("[%s] Asset finished, switching back to RTP\n", gp.pipelineID)
				gp.switchToRTP()
			}
		}
	}()

	fmt.Printf("[%s] Asset pipeline created successfully\n", gp.pipelineID)
	gp.assetPipeline = pipeline
	return nil
}

// switchToAsset switches the compositor and audio mixer to show the asset video
func (gp *GStreamerPipeline) switchToAsset() {
	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	if gp.currentInput == "asset" {
		fmt.Printf("[%s] Already playing asset, ignoring switch request\n", gp.pipelineID)
		return
	}

	fmt.Printf("[%s] Switching to asset video: %s\n", gp.pipelineID, gp.assetVideoPath)

	// Create and start asset pipeline
	err := gp.createAssetPipeline()
	if err != nil {
		fmt.Printf("[%s] Failed to create asset pipeline: %v\n", gp.pipelineID, err)
		return
	}

	// Set asset pipeline to PLAYING
	if err := gp.assetPipeline.SetState(gst.StatePlaying); err != nil {
		fmt.Printf("[%s] Failed to set asset pipeline state: %v\n", gp.pipelineID, err)
		return
	}

	gp.currentInput = "asset"
	gp.assetPlaying = true

	// Switch compositor to show asset (input2) with null checks
	pad1 := gp.compositor.GetStaticPad("sink_0")
	pad2 := gp.compositor.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 0.0) // Hide input1 (RTP stream)
		pad2.SetProperty("alpha", 1.0) // Show input2 (asset)
		fmt.Printf("[%s] Switched compositor to asset content\n", gp.pipelineID)
	}
}

// switchToRTP switches the compositor and audio mixer back to the RTP stream
func (gp *GStreamerPipeline) switchToRTP() {
	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	if gp.currentInput == "rtp" {
		fmt.Printf("[%s] Already playing RTP, ignoring switch request\n", gp.pipelineID)
		return
	}

	fmt.Printf("[%s] Switching back to RTP stream\n", gp.pipelineID)

	// Stop asset pipeline
	if gp.assetPipeline != nil {
		if err := gp.assetPipeline.SetState(gst.StateNull); err != nil {
			fmt.Printf("[%s] Failed to stop asset pipeline: %v\n", gp.pipelineID, err)
		}
		gp.assetPipeline = nil
	}

	gp.currentInput = "rtp"
	gp.assetPlaying = false

	// Reset RTP connection status and restart timeout
	gp.rtpConnected = false
	if gp.rtpTimeout != nil {
		gp.rtpTimeout.Stop()
	}
	gp.rtpTimeout = time.NewTimer(30 * time.Second)

	// Start monitoring for RTP timeout again
	go func() {
		select {
		case <-gp.rtpTimeout.C:
			if gp.running && !gp.rtpConnected {
				fmt.Printf("[%s] RTP connection timeout after switch back - no packets received within 30 seconds\n", gp.pipelineID)
				fmt.Printf("[%s] Switching to asset video due to RTP timeout\n", gp.pipelineID)
				gp.switchToAsset()
			}
		case <-gp.stopChan:
			gp.rtpTimeout.Stop()
			return
		}
	}()

	// Switch compositor back to RTP stream (input1) with null checks
	pad1 := gp.compositor.GetStaticPad("sink_0")
	pad2 := gp.compositor.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 1.0) // Show input1 (RTP stream)
		pad2.SetProperty("alpha", 0.0) // Hide input2 (asset)
		fmt.Printf("[%s] Switched compositor back to RTP content\n", gp.pipelineID)
	}
}

// stopAsset stops the currently playing asset
func (gp *GStreamerPipeline) stopAsset() {
	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	if !gp.assetPlaying {
		return
	}

	fmt.Printf("[%s] Stopping asset playback\n", gp.pipelineID)

	// Stop asset pipeline
	if gp.assetPipeline != nil {
		if err := gp.assetPipeline.SetState(gst.StateNull); err != nil {
			fmt.Printf("[%s] Failed to stop asset pipeline: %v\n", gp.pipelineID, err)
		}
		gp.assetPipeline = nil
	}

	gp.assetPlaying = false
	gp.currentInput = "rtp"

	// Reset RTP connection status and restart timeout
	gp.rtpConnected = false
	if gp.rtpTimeout != nil {
		gp.rtpTimeout.Stop()
	}
	gp.rtpTimeout = time.NewTimer(30 * time.Second)

	// Start monitoring for RTP timeout again
	go func() {
		select {
		case <-gp.rtpTimeout.C:
			if gp.running && !gp.rtpConnected {
				fmt.Printf("[%s] RTP connection timeout after asset stop - no packets received within 30 seconds\n", gp.pipelineID)
				fmt.Printf("[%s] Switching to asset video due to RTP timeout\n", gp.pipelineID)
				gp.switchToAsset()
			}
		case <-gp.stopChan:
			gp.rtpTimeout.Stop()
			return
		}
	}()

	// Switch compositor back to RTP stream (input1) with null checks
	pad1 := gp.compositor.GetStaticPad("sink_0")
	pad2 := gp.compositor.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 1.0) // Show input1 (RTP stream)
		pad2.SetProperty("alpha", 0.0) // Hide input2 (asset)
		fmt.Printf("[%s] Switched compositor back to RTP content\n", gp.pipelineID)
	}
}

// Start starts the GStreamer pipeline
func (gp *GStreamerPipeline) Start() error {
	// Set pipeline state to playing (like in scheduler.go)
	if err := gp.pipeline.SetState(gst.StatePlaying); err != nil {
		return fmt.Errorf("failed to set pipeline state to playing: %v", err)
	}

	gp.running = true
	fmt.Printf("[%s] GStreamer pipeline started successfully\n", gp.pipelineID)

	// Start RTP timeout monitoring
	go func() {
		select {
		case <-gp.rtpTimeout.C:
			if gp.running && !gp.rtpConnected {
				fmt.Printf("[%s] RTP connection timeout - no packets received within 30 seconds\n", gp.pipelineID)
				fmt.Printf("[%s] Switching to asset video due to RTP timeout\n", gp.pipelineID)
				gp.switchToAsset()
			}
		case <-gp.stopChan:
			gp.rtpTimeout.Stop()
			return
		}
	}()

	// Start a timer to switch to asset after 1 minute (only if RTP is connected)
	// go func() {
	// 	time.Sleep(1 * time.Minute)
	// 	if gp.running && gp.rtpConnected {
	// 		fmt.Printf("[%s] 1 minute elapsed, switching to asset video\n", gp.pipelineID)
	// 		gp.switchToAsset()
	// 	}
	// }()

	return nil
}

// Stop stops the GStreamer pipeline
func (gp *GStreamerPipeline) Stop() {
	fmt.Println("[", gp.pipelineID, "] Stopping GStreamer pipeline...")

	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	gp.running = false
	close(gp.stopChan)

	// Stop RTP timeout timer
	if gp.rtpTimeout != nil {
		gp.rtpTimeout.Stop()
	}

	// Stop asset pipeline if running
	if gp.assetPipeline != nil {
		if err := gp.assetPipeline.SetState(gst.StateNull); err != nil {
			fmt.Printf("[%s] Failed to stop asset pipeline: %v\n", gp.pipelineID, err)
		}
		gp.assetPipeline = nil
	}

	// Set pipeline state to null
	if gp.pipeline != nil {
		if err := gp.pipeline.SetState(gst.StateNull); err != nil {
			fmt.Printf("[%s] Failed to stop main pipeline: %v\n", gp.pipelineID, err)
		}
	}

	fmt.Println("[", gp.pipelineID, "] GStreamer pipeline stopped")
}

// RunGStreamerPipeline runs the GStreamer pipeline with the specified parameters
func RunGStreamerPipeline(inputHost string, inputPort int, outputHost string, outputPort int, assetVideoPath string) error {
	fmt.Printf("Starting GStreamer pipeline: %s:%d -> %s:%d with asset: %s\n", inputHost, inputPort, outputHost, outputPort, assetVideoPath)

	// Create pipeline
	pipeline, err := NewGStreamerPipeline(inputHost, inputPort, outputHost, outputPort, assetVideoPath)
	if err != nil {
		return fmt.Errorf("failed to create pipeline: %v", err)
	}
	// Start pipeline
	if err := pipeline.Start(); err != nil {
		return fmt.Errorf("failed to start pipeline: %v", err)
	}

	// Keep the pipeline running
	select {}
}

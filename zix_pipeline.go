package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-gst/go-gst/gst"
)

// GStreamerPipeline represents a GStreamer pipeline for RTP stream processing with compositor and audio mixer
type GStreamerPipeline struct {
	pipeline       *gst.Pipeline
	demux          *gst.Element
	mux            *gst.Element
	compositor     *gst.Element
	audiomixer     *gst.Element
	assetPipeline  *gst.Pipeline
	mutex          sync.Mutex
	assetVideoPath string
	currentInput   string // "rtp" or "asset"
	assetPlaying   bool
	stopChan       chan struct{}
	running        bool
}

// NewGStreamerPipeline creates a new GStreamer pipeline for RTP processing with compositor and audio mixer
func NewGStreamerPipeline(inputHost string, inputPort int, outputHost string, outputPort int, assetVideoPath string) (*GStreamerPipeline, error) {
	// Create pipeline
	pipeline, err := gst.NewPipeline("rtp-pipeline")
	if err != nil {
		return nil, fmt.Errorf("failed to create pipeline: %v", err)
	}

	// Create elements
	udpsrc, err := gst.NewElement("udpsrc")
	if err != nil {
		return nil, fmt.Errorf("failed to create udpsrc: %v", err)
	}

	rtpjitterbuffer, err := gst.NewElement("rtpjitterbuffer")
	if err != nil {
		return nil, fmt.Errorf("failed to create rtpjitterbuffer: %v", err)
	}

	rtpmp2tdepay, err := gst.NewElement("rtpmp2tdepay")
	if err != nil {
		return nil, fmt.Errorf("failed to create rtpmp2tdepay: %v", err)
	}

	tsdemux, err := gst.NewElement("tsdemux")
	if err != nil {
		return nil, fmt.Errorf("failed to create tsdemux: %v", err)
	}

	// Create intervideosink for RTP stream (input1)
	intervideosink1, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel":      "input1",
		"sync":         true,
		"name":         "intervideosink1",
		"max-lateness": int64(20 * 1000000), // 20ms max lateness
		"qos":          true,                // Enable QoS
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create intervideosink1: %v", err)
	}

	// Create interaudiosink for RTP stream (audio1)
	interaudiosink1, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel":      "audio1",
		"sync":         true,
		"name":         "interaudiosink1",
		"max-lateness": int64(20 * 1000000), // 20ms max lateness
		"qos":          true,                // Enable QoS
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create interaudiosink1: %v", err)
	}

	// Create intervideosrc for RTP stream (input1)
	intervideo1, err := gst.NewElementWithProperties("intervideosrc", map[string]interface{}{
		"channel":      "input1",
		"do-timestamp": true,
		"name":         "intervideosrc1",
		"is-live":      true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create intervideosrc1: %v", err)
	}

	// Create intervideosrc for asset stream (input2)
	intervideo2, err := gst.NewElementWithProperties("intervideosrc", map[string]interface{}{
		"channel":      "input2",
		"do-timestamp": true,
		"name":         "intervideosrc2",
		"is-live":      true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create intervideosrc2: %v", err)
	}

	// Create interaudiosrc for RTP stream (audio1)
	interaudio1, err := gst.NewElementWithProperties("interaudiosrc", map[string]interface{}{
		"channel":      "audio1",
		"do-timestamp": true,
		"name":         "interaudiosrc1",
		"is-live":      true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create interaudiosrc1: %v", err)
	}

	// Create interaudiosrc for asset stream (audio2)
	interaudio2, err := gst.NewElementWithProperties("interaudiosrc", map[string]interface{}{
		"channel":      "audio2",
		"do-timestamp": true,
		"name":         "interaudiosrc2",
		"is-live":      true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create interaudiosrc2: %v", err)
	}

	// Create video mixer (compositor)
	compositor, err := gst.NewElementWithProperties("compositor", map[string]interface{}{
		"background":            1, // black background
		"zero-size-is-unscaled": true,
		"name":                  "compositor",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create compositor: %v", err)
	}

	// Configure compositor sink pads for proper positioning
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
	audiomixer, err := gst.NewElement("audiomixer")
	audiomixer.SetProperty("name", "audiomixer")
	if err != nil {
		return nil, fmt.Errorf("failed to create audiomixer: %v", err)
	}

	// Video processing elements
	videoQueue1, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create video queue 1: %v", err)
	}
	videoQueue1.SetProperty("max-size-buffers", 100)
	videoQueue1.SetProperty("max-size-time", uint64(500*1000000))     // 500ms
	videoQueue1.SetProperty("min-threshold-time", uint64(50*1000000)) // 50ms

	videoQueue2, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create video queue 2: %v", err)
	}
	videoQueue2.SetProperty("max-size-buffers", 100)
	videoQueue2.SetProperty("max-size-time", uint64(500*1000000))     // 500ms
	videoQueue2.SetProperty("min-threshold-time", uint64(50*1000000)) // 50ms

	videoconvert, err := gst.NewElement("videoconvert")
	if err != nil {
		return nil, fmt.Errorf("failed to create videoconvert: %v", err)
	}

	x264enc, err := gst.NewElement("x264enc")
	if err != nil {
		return nil, fmt.Errorf("failed to create x264enc: %v", err)
	}

	h264parse2, err := gst.NewElement("h264parse")
	if err != nil {
		return nil, fmt.Errorf("failed to create h264parse2: %v", err)
	}

	// Audio processing elements
	audioQueue1, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create audio queue 1: %v", err)
	}
	audioQueue1.SetProperty("max-size-buffers", 100)
	audioQueue1.SetProperty("max-size-time", uint64(500*1000000))     // 500ms
	audioQueue1.SetProperty("min-threshold-time", uint64(50*1000000)) // 50ms

	audioQueue2, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create audio queue 2: %v", err)
	}
	audioQueue2.SetProperty("max-size-buffers", 100)
	audioQueue2.SetProperty("max-size-time", uint64(500*1000000))     // 500ms
	audioQueue2.SetProperty("min-threshold-time", uint64(50*1000000)) // 50ms

	aacparse1, err := gst.NewElement("aacparse")
	if err != nil {
		return nil, fmt.Errorf("failed to create aacparse1: %v", err)
	}

	audioconvert, err := gst.NewElement("audioconvert")
	if err != nil {
		return nil, fmt.Errorf("failed to create audioconvert: %v", err)
	}

	// Add audio resampler for better compatibility
	audioresample, err := gst.NewElement("audioresample")
	if err != nil {
		return nil, fmt.Errorf("failed to create audioresample: %v", err)
	}

	voaacenc, err := gst.NewElement("voaacenc")
	if err != nil {
		return nil, fmt.Errorf("failed to create voaacenc: %v", err)
	}

	aacparse2, err := gst.NewElement("aacparse")
	if err != nil {
		return nil, fmt.Errorf("failed to create aacparse2: %v", err)
	}

	// Audio queue before muxer
	audioMuxerQueue, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create audio muxer queue: %v", err)
	}
	audioMuxerQueue.SetProperty("max-size-buffers", 50)
	audioMuxerQueue.SetProperty("max-size-time", uint64(200*1000000))     // 200ms
	audioMuxerQueue.SetProperty("min-threshold-time", uint64(50*1000000)) // 50ms

	// Muxer and output elements
	mpegtsmux, err := gst.NewElement("mpegtsmux")
	if err != nil {
		return nil, fmt.Errorf("failed to create mpegtsmux: %v", err)
	}

	// Configure MPEG-TS muxer for better compatibility
	mpegtsmux.SetProperty("alignment", 7)
	mpegtsmux.SetProperty("pat-interval", int64(100*1000000)) // 100ms
	mpegtsmux.SetProperty("pmt-interval", int64(100*1000000)) // 100ms
	mpegtsmux.SetProperty("pcr-interval", int64(20*1000000))  // 20ms
	mpegtsmux.SetProperty("muxrate", 10080000)                // 10.08 Mbps

	rtpmp2tpay, err := gst.NewElement("rtpmp2tpay")
	if err != nil {
		return nil, fmt.Errorf("failed to create rtpmp2tpay: %v", err)
	}

	udpsink, err := gst.NewElementWithProperties("udpsink", map[string]interface{}{
		"host":           outputHost,
		"port":           outputPort,
		"sync":           true,
		"buffer-size":    524288,
		"auto-multicast": true,
		"name":           "udpsink",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create udpsink: %v", err)
	}

	// Set properties
	udpsrc.SetProperty("address", inputHost)
	udpsrc.SetProperty("port", inputPort)
	udpsrc.SetProperty("caps", gst.NewCapsFromString("application/x-rtp"))

	// Improved RTP jitter buffer configuration for better clock handling
	rtpjitterbuffer.SetProperty("latency", 300)           // Increased latency for better buffering
	rtpjitterbuffer.SetProperty("do-lost", true)          // Handle lost packets
	rtpjitterbuffer.SetProperty("drop-on-latency", true)  // Drop packets if too late
	rtpjitterbuffer.SetProperty("max-dropout-time", 5000) // 5 seconds max dropout
	rtpjitterbuffer.SetProperty("max-misorder-time", 500) // 500ms max misorder

	x264enc.SetProperty("tune", "zerolatency")

	// Audio encoder properties
	voaacenc.SetProperty("bitrate", 128000) // 128 kbps
	voaacenc.SetProperty("channels", 2)     // Stereo

	// Audio resampler properties for better compatibility
	audioresample.SetProperty("quality", 10) // Highest quality

	// AAC parser properties
	aacparse1.SetProperty("outputformat", 0) // ADTS format
	aacparse2.SetProperty("outputformat", 0) // ADTS format

	rtpmp2tpay.SetProperty("mtu", 1400)
	rtpmp2tpay.SetProperty("pt", 33)
	rtpmp2tpay.SetProperty("perfect-rtptime", true)

	// Add elements to pipeline
	elements := []*gst.Element{
		udpsrc, rtpjitterbuffer, rtpmp2tdepay, tsdemux,
		intervideosink1, interaudiosink1,
		intervideo1, intervideo2, compositor,
		interaudio1, interaudio2, audiomixer,
		videoQueue1, videoQueue2, videoconvert, x264enc, h264parse2,
		audioQueue1, audioQueue2, audioconvert, audioresample, voaacenc, aacparse2,
		audioMuxerQueue, mpegtsmux, rtpmp2tpay, udpsink,
	}

	for _, element := range elements {
		if err := pipeline.Add(element); err != nil {
			return nil, fmt.Errorf("failed to add element to pipeline: %v", err)
		}
	}

	// Link elements (except demuxer which needs dynamic linking)
	if err := udpsrc.Link(rtpjitterbuffer); err != nil {
		return nil, fmt.Errorf("failed to link udpsrc to rtpjitterbuffer: %v", err)
	}

	if err := rtpjitterbuffer.Link(rtpmp2tdepay); err != nil {
		return nil, fmt.Errorf("failed to link rtpjitterbuffer to rtpmp2tdepay: %v", err)
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
	if err := h264parse2.Link(mpegtsmux); err != nil {
		return nil, fmt.Errorf("failed to link h264parse2 to mpegtsmux: %v", err)
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

	if err := rtpmp2tpay.Link(udpsink); err != nil {
		return nil, fmt.Errorf("failed to link rtpmp2tpay to udpsink: %v", err)
	}

	// Set up dynamic pad-added signal for tsdemux
	tsdemux.Connect("pad-added", func(self *gst.Element, pad *gst.Pad) {
		fmt.Printf("Demuxer pad added: %s\n", pad.GetName())

		padName := pad.GetName()
		var targetElement *gst.Element
		var capsFilter *gst.Element
		var converter *gst.Element
		var queue *gst.Element
		var err error

		// Check pad name to determine if it's video or audio
		if len(padName) >= 5 && padName[:5] == "video" {
			targetElement = intervideosink1
			fmt.Printf("Linking video pad %s to intervideosink1\n", padName)

			// Create video caps filter - more flexible for different H.264 formats
			capsFilter, err = gst.NewElement("capsfilter")
			if err != nil {
				fmt.Printf("Failed to create video caps filter: %v\n", err)
				return
			}
			capsFilter.SetProperty("caps", gst.NewCapsFromString("video/x-h264"))

			// Create H.264 parser for better compatibility
			converter, err = gst.NewElement("h264parse")
			if err != nil {
				fmt.Printf("Failed to create h264parse: %v\n", err)
				return
			}

			// Create queue for video buffering
			queue, err = gst.NewElement("queue")
			if err != nil {
				fmt.Printf("Failed to create video queue: %v\n", err)
				return
			}
			queue.SetProperty("max-size-buffers", 50)
			queue.SetProperty("max-size-time", uint64(200*1000000))     // 200ms
			queue.SetProperty("min-threshold-time", uint64(50*1000000)) // 50ms

		} else if len(padName) >= 5 && padName[:5] == "audio" {
			targetElement = interaudiosink1
			fmt.Printf("Linking audio pad %s to interaudiosink1\n", padName)

			// Create audio caps filter - more flexible for different audio formats
			capsFilter, err = gst.NewElement("capsfilter")
			if err != nil {
				fmt.Printf("Failed to create audio caps filter: %v\n", err)
				return
			}
			capsFilter.SetProperty("caps", gst.NewCapsFromString("audio/mpeg"))

			// Create AAC parser for better compatibility
			converter, err = gst.NewElement("aacparse")
			if err != nil {
				fmt.Printf("Failed to create aacparse: %v\n", err)
				return
			}

			// Create queue for audio buffering
			queue, err = gst.NewElement("queue")
			if err != nil {
				fmt.Printf("Failed to create audio queue: %v\n", err)
				return
			}
			queue.SetProperty("max-size-buffers", 50)
			queue.SetProperty("max-size-time", uint64(200*1000000))     // 200ms
			queue.SetProperty("min-threshold-time", uint64(50*1000000)) // 50ms

		} else {
			fmt.Printf("Unknown demuxer pad: %s\n", padName)
			return
		}

		// Add elements to pipeline
		if err := pipeline.Add(capsFilter); err != nil {
			fmt.Printf("Failed to add caps filter to pipeline: %v\n", err)
			return
		}
		if err := pipeline.Add(converter); err != nil {
			fmt.Printf("Failed to add converter to pipeline: %v\n", err)
			return
		}
		if err := pipeline.Add(queue); err != nil {
			fmt.Printf("Failed to add queue to pipeline: %v\n", err)
			return
		}

		// Get the sink pad from the target element
		sinkPad := targetElement.GetStaticPad("sink")
		if sinkPad == nil {
			fmt.Printf("Failed to get sink pad from target element\n")
			return
		}

		// Set elements to PLAYING state
		capsFilter.SetState(gst.StatePlaying)
		converter.SetState(gst.StatePlaying)
		queue.SetState(gst.StatePlaying)

		// Link the chain: demuxer pad -> caps filter -> converter -> queue -> target element
		if pad.Link(capsFilter.GetStaticPad("sink")) != gst.PadLinkOK {
			fmt.Printf("Failed to link demuxer pad to caps filter\n")
			// Try to get more information about the pad
			padCaps := pad.GetCurrentCaps()
			if padCaps != nil {
				fmt.Printf("Demuxer pad caps: %s\n", padCaps.String())
			}
			return
		}

		if capsFilter.GetStaticPad("src").Link(converter.GetStaticPad("sink")) != gst.PadLinkOK {
			fmt.Printf("Failed to link caps filter to converter\n")
			return
		}

		if converter.GetStaticPad("src").Link(queue.GetStaticPad("sink")) != gst.PadLinkOK {
			fmt.Printf("Failed to link converter to queue\n")
			return
		}

		if queue.GetStaticPad("src").Link(sinkPad) == gst.PadLinkOK {
			fmt.Printf("Successfully linked demuxer pad %s to %s via caps filter, converter, and queue\n", padName, targetElement.GetName())
		} else {
			fmt.Printf("Failed to link queue to target element\n")
			// Try to get more information about the target pad
			sinkPadCaps := sinkPad.GetCurrentCaps()
			if sinkPadCaps != nil {
				fmt.Printf("Target sink pad caps: %s\n", sinkPadCaps.String())
			}

			// Try direct linking as fallback
			fmt.Printf("Attempting direct linking as fallback...\n")
			if pad.Link(sinkPad) == gst.PadLinkOK {
				fmt.Printf("Successfully linked demuxer pad %s directly to %s\n", padName, targetElement.GetName())
			} else {
				fmt.Printf("Direct linking also failed for demuxer pad %s\n", padName)
			}
		}
	})

	// Set up bus watch for the main pipeline
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
				fmt.Printf("Pipeline state changed: %s -> %s\n", oldState.String(), newState.String())
			case gst.MessageError:
				gerr := msg.ParseError()
				fmt.Printf("Pipeline error: %s\n", gerr.Error())
			case gst.MessageWarning:
				gwarn := msg.ParseWarning()
				fmt.Printf("Pipeline warning: %s\n", gwarn.Error())
			case gst.MessageInfo:
				ginfo := msg.ParseInfo()
				fmt.Printf("Pipeline info: %s\n", ginfo.Error())
			}
		}
	}()

	return &GStreamerPipeline{
		pipeline:       pipeline,
		demux:          tsdemux,
		mux:            mpegtsmux,
		compositor:     compositor,
		audiomixer:     audiomixer,
		assetVideoPath: assetVideoPath,
		currentInput:   "rtp",
		assetPlaying:   false,
		stopChan:       make(chan struct{}),
		running:        false,
	}, nil
}

// createAssetPipeline creates a pipeline for playing the local asset video file
func (gp *GStreamerPipeline) createAssetPipeline() error {
	pipeline, err := gst.NewPipeline("asset-pipeline")
	if err != nil {
		return fmt.Errorf("failed to create asset pipeline: %v", err)
	}

	// Create playbin for asset file
	playbin, err := gst.NewElementWithProperties("playbin3", map[string]interface{}{
		"uri": fmt.Sprintf("file://%s", gp.assetVideoPath),
	})
	if err != nil {
		return fmt.Errorf("failed to create playbin: %v", err)
	}

	// Create video sink
	intervideosink, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel": "input2",
		"sync":    true,
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosink: %v", err)
	}

	// Create audio sink
	interaudiosink, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel": "audio2",
		"sync":    true,
	})
	if err != nil {
		return fmt.Errorf("failed to create interaudiosink: %v", err)
	}

	// Set sinks on playbin
	playbin.SetProperty("video-sink", intervideosink)
	playbin.SetProperty("audio-sink", interaudiosink)

	// Add playbin to pipeline
	pipeline.Add(playbin)

	// Set up bus watch
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
					fmt.Printf("Asset pipeline is now PLAYING\n")
				}
			case gst.MessageError:
				gerr := msg.ParseError()
				fmt.Printf("Asset pipeline error: %s\n", gerr.Error())
				gp.stopAsset()
			case gst.MessageEOS:
				fmt.Printf("Asset finished, switching back to RTP\n")
				gp.switchToRTP()
			}
		}
	}()

	gp.assetPipeline = pipeline
	return nil
}

// switchToAsset switches the compositor and audio mixer to show the asset video
func (gp *GStreamerPipeline) switchToAsset() {
	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	if gp.currentInput == "asset" {
		fmt.Printf("Already playing asset, ignoring switch request\n")
		return
	}

	fmt.Printf("Switching to asset video: %s\n", gp.assetVideoPath)

	// Create and start asset pipeline
	err := gp.createAssetPipeline()
	if err != nil {
		fmt.Printf("Failed to create asset pipeline: %v\n", err)
		return
	}

	// Set asset pipeline to PLAYING
	gp.assetPipeline.SetState(gst.StatePlaying)

	gp.currentInput = "asset"
	gp.assetPlaying = true

	// Switch compositor to show asset (input2)
	pad1 := gp.compositor.GetStaticPad("sink_0")
	pad2 := gp.compositor.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 0.0) // Hide input1 (RTP stream)
		pad2.SetProperty("alpha", 1.0) // Show input2 (asset)
		fmt.Printf("Switched compositor to asset content\n")
	}
}

// switchToRTP switches the compositor and audio mixer back to the RTP stream
func (gp *GStreamerPipeline) switchToRTP() {
	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	if gp.currentInput == "rtp" {
		fmt.Printf("Already playing RTP, ignoring switch request\n")
		return
	}

	fmt.Printf("Switching back to RTP stream\n")

	// Stop asset pipeline
	if gp.assetPipeline != nil {
		gp.assetPipeline.SetState(gst.StateNull)
		gp.assetPipeline = nil
	}

	gp.currentInput = "rtp"
	gp.assetPlaying = false

	// Switch compositor back to RTP stream (input1)
	pad1 := gp.compositor.GetStaticPad("sink_0")
	pad2 := gp.compositor.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 1.0) // Show input1 (RTP stream)
		pad2.SetProperty("alpha", 0.0) // Hide input2 (asset)
		fmt.Printf("Switched compositor back to RTP content\n")
	}
}

// stopAsset stops the currently playing asset
func (gp *GStreamerPipeline) stopAsset() {
	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	if !gp.assetPlaying {
		return
	}

	fmt.Printf("Stopping asset playback\n")

	// Stop asset pipeline
	if gp.assetPipeline != nil {
		gp.assetPipeline.SetState(gst.StateNull)
		gp.assetPipeline = nil
	}

	gp.assetPlaying = false
	gp.currentInput = "rtp"

	// Switch compositor back to RTP stream (input1)
	pad1 := gp.compositor.GetStaticPad("sink_0")
	pad2 := gp.compositor.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 1.0) // Show input1 (RTP stream)
		pad2.SetProperty("alpha", 0.0) // Hide input2 (asset)
		fmt.Printf("Switched compositor back to RTP content\n")
	}
}

// Start starts the GStreamer pipeline
func (gp *GStreamerPipeline) Start() error {
	// Set pipeline state to playing (like in scheduler.go)
	if err := gp.pipeline.SetState(gst.StatePlaying); err != nil {
		return fmt.Errorf("failed to set pipeline state to playing: %v", err)
	}

	gp.running = true
	fmt.Printf("GStreamer pipeline started successfully\n")

	// Start a timer to switch to asset after 1 minute
	go func() {
		time.Sleep(1 * time.Minute)
		if gp.running {
			fmt.Printf("1 minute elapsed, switching to asset video\n")
			gp.switchToAsset()
		}
	}()

	return nil
}

// Stop stops the GStreamer pipeline
func (gp *GStreamerPipeline) Stop() {
	fmt.Println("Stopping GStreamer pipeline...")

	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	gp.running = false
	close(gp.stopChan)

	// Stop asset pipeline if running
	if gp.assetPipeline != nil {
		gp.assetPipeline.SetState(gst.StateNull)
		gp.assetPipeline = nil
	}

	// Set pipeline state to null
	if gp.pipeline != nil {
		gp.pipeline.SetState(gst.StateNull)
	}

	fmt.Println("GStreamer pipeline stopped")
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

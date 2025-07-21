package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-gst/go-gst/gst"
	"github.com/google/uuid"
)

// AdInsertionHandler handles SCTE-35 messages and ad insertion using compositor/audiomixer
type AdInsertionHandler struct {
	inputHost           string
	inputPort           int
	outputHost          string
	outputPort          int
	adSource            string
	mainPipeline        *gst.Pipeline
	adPipeline          *gst.Pipeline
	compositor          *gst.Element
	audiomixer          *gst.Element
	mutex               sync.Mutex
	stopChan            chan struct{}
	running             bool
	handlerID           string
	currentAdPlaying    bool
	adDuration          time.Duration
	mainStreamPaused    bool
	scheduledAds        map[gst.ClockTime]bool // Track scheduled ad insertions
	dummySourcePipeline *gst.Pipeline
	videoPadLinked      chan struct{}
	audioPadLinked      chan struct{}
	videoPadLinkedOnce  sync.Once
	audioPadLinkedOnce  sync.Once
	verbose             bool
}

// NewAdInsertionHandler creates a new SCTE-35 handler
func NewAdInsertionHandler(inputHost string, inputPort int, outputHost string, outputPort int, adSource string, verbose bool) (*AdInsertionHandler, error) {
	gst.Init(nil)

	// Set GStreamer debug level if verbose mode is enabled
	if verbose {
		// Set environment variable for GStreamer debug output
		// This is equivalent to gst-launch-1.0 -v
		os.Setenv("GST_DEBUG", "3") // 3 = GST_LEVEL_DEBUG
		fmt.Println("GStreamer debug logging enabled (equivalent to gst-launch-1.0 -v)")
		fmt.Println("Debug level: 3 (GST_LEVEL_DEBUG)")
	} else {
		// Set to warning level by default
		os.Setenv("GST_DEBUG", "1") // 1 = GST_LEVEL_WARNING
	}

	return &AdInsertionHandler{
		inputHost:      inputHost,
		inputPort:      inputPort,
		outputHost:     outputHost,
		outputPort:     outputPort,
		adSource:       adSource,
		stopChan:       make(chan struct{}),
		handlerID:      uuid.New().String(),
		adDuration:     30 * time.Second, // Default ad duration
		scheduledAds:   make(map[gst.ClockTime]bool),
		videoPadLinked: make(chan struct{}),
		audioPadLinked: make(chan struct{}),
		verbose:        verbose,
	}, nil
}

// logVerbose logs messages only when verbose mode is enabled
func (h *AdInsertionHandler) logVerbose(format string, args ...interface{}) {
	if h.verbose {
		fmt.Printf(format, args...)
	}
}

// Start begins the SCTE-35 handler
func (h *AdInsertionHandler) Start() error {
	h.mutex.Lock()
	if h.running {
		h.mutex.Unlock()
		return fmt.Errorf("handler is already running")
	}
	h.running = true
	h.mutex.Unlock()

	// Create main pipeline for normal stream
	err := h.createMainPipeline()
	if err != nil {
		return fmt.Errorf("failed to create main pipeline: %v", err)
	}

	// Create a dummy source pipeline to feed into intervideosink and interaudiosink
	// This ensures the sinks have data to pull from when the main pipeline starts
	err = h.createDummySourcePipeline()
	if err != nil {
		return fmt.Errorf("failed to create dummy source pipeline: %v", err)
	}

	// Create synchronization channels for pipeline readiness
	mainPipelineReady := make(chan bool, 1)
	dummySourceReady := make(chan bool, 1)

	// Set up bus watch for main pipeline
	bus := h.mainPipeline.GetBus()
	go func() {
		for {
			msg := bus.TimedPop(gst.ClockTimeNone)
			if msg == nil {
				break
			}
			switch msg.Type() {
			case gst.MessageStateChanged:
				_, newState := msg.ParseStateChanged()
				if newState == gst.StatePlaying {
					h.logVerbose("Main pipeline is now PLAYING and ready\n")
					select {
					case mainPipelineReady <- true:
						// Successfully sent ready signal
					default:
						// Channel is full, ignore
					}
				}
			case gst.MessageError:
				gerr := msg.ParseError()
				fmt.Printf("Main pipeline error during startup: %s\n", gerr.Error())
				// Don't send ready signal on error
			}
		}
	}()

	// Set up bus watch for dummy source pipeline
	dummyBus := h.dummySourcePipeline.GetBus()
	go func() {
		for {
			msg := dummyBus.TimedPop(gst.ClockTimeNone)
			if msg == nil {
				break
			}
			switch msg.Type() {
			case gst.MessageStateChanged:
				oldState, newState := msg.ParseStateChanged()
				if newState == gst.StatePlaying && oldState != gst.StatePaused {
					h.logVerbose("Dummy source pipeline is now PLAYING and ready\n")
					select {
					case dummySourceReady <- true:
						// Successfully sent ready signal
					default:
						// Channel is full, ignore
					}
				}
			case gst.MessageError:
				gerr := msg.ParseError()
				h.logVerbose("Dummy source pipeline error: %s\n", gerr.Error())
			}
		}
	}()

	// Start dummy source pipeline first
	h.logVerbose("Starting dummy source pipeline...\n")
	h.dummySourcePipeline.SetState(gst.StatePlaying)

	// Wait for dummy source to be ready
	select {
	case <-dummySourceReady:
		h.logVerbose("Dummy source pipeline is ready\n")
	case <-time.After(5 * time.Second):
		fmt.Printf("Warning: Dummy source pipeline ready timeout\n")
	}

	// Wait for inter pads to be linked before starting main pipeline
	h.logVerbose("Waiting for inter pads to be linked...\n")
	select {
	case <-h.videoPadLinked:
		h.logVerbose("Video pad linked successfully\n")
	case <-time.After(10 * time.Second):
		fmt.Printf("Warning: Video pad linking timeout\n")
	}

	select {
	case <-h.audioPadLinked:
		h.logVerbose("Audio pad linked successfully\n")
	case <-time.After(10 * time.Second):
		fmt.Printf("Warning: Audio pad linking timeout\n")
	}

	// Now start main pipeline
	h.logVerbose("Starting main pipeline...\n")
	h.mainPipeline.SetState(gst.StatePlaying)

	// Wait for main pipeline to be ready with timeout
	select {
	case <-mainPipelineReady:
		h.logVerbose("Main pipeline is ready, continuing...\n")
	case <-time.After(10 * time.Second):
		fmt.Printf("Warning: Main pipeline ready timeout, continuing anyway...\n")
	}

	// Start monitoring goroutine
	go h.monitorPipelines()

	return nil
}

// createMainPipeline creates the main pipeline with compositor and audiomixer
func (h *AdInsertionHandler) createMainPipeline() error {
	pipeline, err := gst.NewPipeline("main-pipeline" + h.handlerID)
	if err != nil {
		return fmt.Errorf("failed to create main pipeline: %v", err)
	}

	// Create UDP source for input RTP stream
	udpsrc, err := gst.NewElementWithProperties("udpsrc", map[string]interface{}{
		"address":        h.inputHost,
		"port":           h.inputPort,
		"buffer-size":    524288,
		"auto-multicast": true,
		"name":           "udpsrc" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create udpsrc: %v", err)
	}

	// Create RTP caps filter
	rtpCapsFilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": gst.NewCapsFromString("application/x-rtp"),
		"name": "rtpCapsFilter" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create rtpCapsFilter: %v", err)
	}

	// Create RTP jitter buffer
	rtpjitterbuffer, err := gst.NewElementWithProperties("rtpjitterbuffer", map[string]interface{}{
		"latency": 200,
		"name":    "rtpjitterbuffer" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create rtpjitterbuffer: %v", err)
	}

	// Create RTP MPEG-TS depayloader
	rtpmp2tdepay, err := gst.NewElement("rtpmp2tdepay")
	if err != nil {
		return fmt.Errorf("failed to create rtpmp2tdepay: %v", err)
	}

	// Create demuxer for main stream processing
	demux, err := gst.NewElementWithProperties("tsdemux", map[string]interface{}{
		"name": "tsdemux" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create tsdemux: %v", err)
	}

	// Create intervideosink for main stream (input1)
	intervideosink1, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel": "input1" + h.handlerID,
		"sync":    true,
		"name":    "intervideosink1" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosink1: %v", err)
	}

	// Create intervideosrc for main stream (input1)
	intervideo1, err := gst.NewElementWithProperties("intervideosrc", map[string]interface{}{
		"channel":      "input1" + h.handlerID,
		"do-timestamp": true,
		"name":         "intervideosrc1" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosrc1: %v", err)
	}

	// Create intervideosrc for ad stream (input2)
	intervideo2, err := gst.NewElementWithProperties("intervideosrc", map[string]interface{}{
		"channel":      "input2" + h.handlerID,
		"do-timestamp": true,
		"name":         "intervideosrc2" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosrc2: %v", err)
	}

	// Create video mixer (compositor)
	h.compositor, err = gst.NewElementWithProperties("compositor", map[string]interface{}{
		"background":            1, // black background
		"zero-size-is-unscaled": true,
		"name":                  "compositor" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create compositor: %v", err)
	}

	// Configure compositor sink pads for proper positioning
	pad1 := h.compositor.GetStaticPad("sink_0")
	if pad1 != nil {
		pad1.SetProperty("xpos", 0)
		pad1.SetProperty("ypos", 0)
		pad1.SetProperty("width", 1920)
		pad1.SetProperty("height", 1080)
		pad1.SetProperty("alpha", 1.0) // input1 visible
	}

	pad2 := h.compositor.GetStaticPad("sink_1")
	if pad2 != nil {
		pad2.SetProperty("xpos", 0)
		pad2.SetProperty("ypos", 0)
		pad2.SetProperty("width", 1920)
		pad2.SetProperty("height", 1080)
		pad2.SetProperty("alpha", 0.0) // input2 hidden
	}

	// Create interaudiosink for main stream (audio1)
	interaudiosink1, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel": "audio1" + h.handlerID,
		"sync":    true,
		"name":    "interaudiosink1" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create interaudiosink1: %v", err)
	}

	// Create interaudiosrc for main stream (audio1)
	interaudio1, err := gst.NewElementWithProperties("interaudiosrc", map[string]interface{}{
		"channel":      "audio1" + h.handlerID,
		"do-timestamp": true,
		"name":         "interaudiosrc1" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create interaudiosrc1: %v", err)
	}

	// Create interaudiosrc for ad stream (audio2)
	interaudio2, err := gst.NewElementWithProperties("interaudiosrc", map[string]interface{}{
		"channel":      "audio2" + h.handlerID,
		"do-timestamp": true,
		"name":         "interaudiosrc2" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create interaudiosrc2: %v", err)
	}

	// Create audio mixer
	h.audiomixer, err = gst.NewElement("audiomixer")
	h.audiomixer.SetProperty("name", "audiomixer"+h.handlerID)
	if err != nil {
		return fmt.Errorf("failed to create audiomixer: %v", err)
	}

	// Create video processing elements
	videoQueue1, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0,
		"name":               "videoQueue1" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create videoQueue1: %v", err)
	}

	videoQueue2, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0,
		"name":               "videoQueue2" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create videoQueue2: %v", err)
	}

	h264parse1, err := gst.NewElement("h264parse")
	if err != nil {
		return fmt.Errorf("failed to create h264parse1: %v", err)
	}

	avdec_h264, err := gst.NewElement("avdec_h264")
	if err != nil {
		return fmt.Errorf("failed to create avdec_h264: %v", err)
	}

	videoconvert, err := gst.NewElement("videoconvert")
	if err != nil {
		return fmt.Errorf("failed to create videoconvert: %v", err)
	}

	h264enc, err := gst.NewElementWithProperties("x264enc", map[string]interface{}{
		"tune":    0x00000004, // zerolatency
		"bitrate": 2000,       // 2 Mbps
		"name":    "h264enc" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create h264enc: %v", err)
	}

	h264parse2, err := gst.NewElement("h264parse")
	if err != nil {
		return fmt.Errorf("failed to create h264parse2: %v", err)
	}

	// Create audio processing elements
	audioQueue1, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0,
		"name":               "audioQueue1" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audioQueue1: %v", err)
	}

	audioQueue2, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0,
		"name":               "audioQueue2" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audioQueue2: %v", err)
	}

	aacparse1, err := gst.NewElement("aacparse")
	if err != nil {
		return fmt.Errorf("failed to create aacparse1: %v", err)
	}

	avdec_aac, err := gst.NewElement("avdec_aac")
	if err != nil {
		return fmt.Errorf("failed to create avdec_aac: %v", err)
	}

	audioconvert, err := gst.NewElement("audioconvert")
	if err != nil {
		return fmt.Errorf("failed to create audioconvert: %v", err)
	}

	voaacenc, err := gst.NewElement("voaacenc")
	if err != nil {
		return fmt.Errorf("failed to create voaacenc: %v", err)
	}

	aacparse2, err := gst.NewElement("aacparse")
	if err != nil {
		return fmt.Errorf("failed to create aacparse2: %v", err)
	}

	// Create muxer and output elements
	mpegtsmux, err := gst.NewElementWithProperties("mpegtsmux", map[string]interface{}{
		"alignment":    7,
		"pat-interval": int64(100 * 1000000),
		"pmt-interval": int64(100 * 1000000),
		"pcr-interval": int64(20 * 1000000),
		"name":         "mpegtsmux" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create mpegtsmux: %v", err)
	}

	rtpmp2tpay, err := gst.NewElementWithProperties("rtpmp2tpay", map[string]interface{}{
		"pt":              33,
		"mtu":             1400,
		"perfect-rtptime": true,
		"name":            "rtpmp2tpay" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create rtpmp2tpay: %v", err)
	}

	udpsink, err := gst.NewElementWithProperties("udpsink", map[string]interface{}{
		"host":           h.outputHost,
		"port":           h.outputPort,
		"sync":           true,
		"buffer-size":    524288,
		"auto-multicast": true,
		"name":           "udpsink" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create udpsink: %v", err)
	}

	// Add all elements to the pipeline
	pipeline.AddMany(udpsrc, rtpCapsFilter, rtpjitterbuffer, rtpmp2tdepay, demux)
	pipeline.AddMany(intervideosink1, interaudiosink1)
	pipeline.AddMany(intervideo1, intervideo2, h.compositor)
	pipeline.AddMany(interaudio1, interaudio2, h.audiomixer)
	pipeline.AddMany(videoQueue1, videoQueue2, h264parse1, avdec_h264, videoconvert, h264enc, h264parse2)
	pipeline.AddMany(audioQueue1, audioQueue2, aacparse1, avdec_aac, audioconvert, voaacenc, aacparse2)
	pipeline.AddMany(mpegtsmux, rtpmp2tpay, udpsink)

	// Link the main pipeline elements
	udpsrc.Link(rtpCapsFilter)
	rtpCapsFilter.Link(rtpjitterbuffer)
	rtpjitterbuffer.Link(rtpmp2tdepay)
	rtpmp2tdepay.Link(demux)

	// Set up dynamic pad-added signal for demuxer
	demux.Connect("pad-added", func(element *gst.Element, pad *gst.Pad) {

		padName := pad.GetName()
		h.logVerbose("##########################################Demuxer pad added: %s\n", padName)

		if len(padName) >= 5 && padName[:5] == "video" {
			h.logVerbose("Linking video pad to intervideosink1\n")
			// Link video pad to existing intervideosink
			result := pad.Link(intervideosink1.GetStaticPad("sink"))
			if result != gst.PadLinkOK {
				fmt.Printf("Failed to link video pad: %v\n", result)
			} else {
				h.logVerbose("Successfully linked video pad to intervideosink\n")
			}
		} else if len(padName) >= 5 && padName[:5] == "audio" {
			h.logVerbose("Linking audio pad to interaudiosink1\n")
			// Link audio pad to existing interaudiosink
			result := pad.Link(interaudiosink1.GetStaticPad("sink"))
			if result != gst.PadLinkOK {
				fmt.Printf("Failed to link audio pad: %v\n", result)
			} else {
				h.logVerbose("Successfully linked audio pad to interaudiosink\n")
				h.audioPadLinkedOnce.Do(func() {
					close(h.audioPadLinked)
				})
			}
		} else {
			h.logVerbose("Unknown demuxer pad: %s\n", padName)
		}
	})

	// Link video elements with dynamic pad handling
	intervideo1.Connect("pad-added", func(element *gst.Element, pad *gst.Pad) {
		h.logVerbose("intervideosrc1 pad added: %s\n", pad.GetName())
		if pad.GetName() == "src" {
			ok := pad.Link(videoQueue1.GetStaticPad("sink"))
			if ok != gst.PadLinkOK {
				fmt.Printf("Failed to link intervideo1 -> videoQueue1: %v\n", ok)
			} else {
				h.logVerbose("Successfully linked intervideo1 -> videoQueue1\n")
				h.videoPadLinkedOnce.Do(func() {
					close(h.videoPadLinked)
				})
			}
		}
	})

	intervideo2.Connect("pad-added", func(element *gst.Element, pad *gst.Pad) {
		h.logVerbose("intervideosrc2 pad added: %s\n", pad.GetName())
		if pad.GetName() == "src" {
			ok := pad.Link(videoQueue2.GetStaticPad("sink"))
			if ok != gst.PadLinkOK {
				fmt.Printf("Failed to link intervideo2 -> videoQueue2: %v\n", ok)
			} else {
				h.logVerbose("Successfully linked intervideo2 -> videoQueue2\n")
			}
		}
	})

	// Link video processing chain
	videoQueue1.Link(h.compositor)
	videoQueue2.Link(h.compositor)
	h.compositor.Link(h264parse1)
	h264parse1.Link(avdec_h264)
	avdec_h264.Link(videoconvert)
	videoconvert.Link(h264enc)
	h264enc.Link(h264parse2)
	h264parse2.Link(mpegtsmux)

	// Link audio elements
	interaudio1.Link(audioQueue1)
	audioQueue1.Link(h.audiomixer)

	interaudio2.Link(audioQueue2)
	audioQueue2.Link(h.audiomixer)

	// Link audio processing chain
	h.audiomixer.Link(aacparse1)
	aacparse1.Link(avdec_aac)
	avdec_aac.Link(audioconvert)
	audioconvert.Link(voaacenc)
	voaacenc.Link(aacparse2)
	aacparse2.Link(mpegtsmux)

	// Link muxer to RTP and UDP sink
	mpegtsmux.Link(rtpmp2tpay)
	rtpmp2tpay.Link(udpsink)

	// Set up bus watch
	bus := pipeline.GetBus()
	go func() {
		for {
			msg := bus.TimedPop(gst.ClockTimeNone)
			if msg == nil {
				break
			}

			switch msg.Type() {
			case gst.MessageError:
				gerr := msg.ParseError()
				elementName := msg.Source()
				fmt.Printf("Error from element %s: %s\n", elementName, gerr.Error())

			case gst.MessageWarning:
				gerr := msg.ParseWarning()
				fmt.Printf("Warning from element %s: %s\n", msg.Source(), gerr.Error())
			case gst.MessageEOS:
				fmt.Printf("End of stream received\n")
			case gst.MessageElement:
				// Handle element messages (which include SCTE-35 events)
				structure := msg.GetStructure()
				if structure != nil && structure.Name() == "scte35" {
					fmt.Println("Received SCTE-35 event via bus message!")

					// Get current pipeline time for scheduling
					currentPTS := h.getCurrentPTS()
					fmt.Printf("SCTE-35 event at pipeline time: %v\n", currentPTS)

					// For now, just trigger ad insertion when SCTE-35 is detected
					// In a real implementation, you would parse the structure data
					fmt.Printf("SCTE-35 structure: %v\n", structure)
					h.insertAd()
				}
			case gst.MessageStateChanged:
				// Log state changes for debugging
				_, newState := msg.ParseStateChanged()
				if newState == gst.StatePlaying {
					h.logVerbose("Element %s is now PLAYING\n", msg.Source())
				}
			}
		}
	}()

	h.mainPipeline = pipeline
	return nil
}

// createAdPipeline creates a pipeline for playing ads
func (h *AdInsertionHandler) createAdPipeline() error {
	pipeline, err := gst.NewPipeline("ad-pipeline" + h.handlerID)
	if err != nil {
		return fmt.Errorf("failed to create ad pipeline: %v", err)
	}

	// Create playbin for ad file
	playbin, err := gst.NewElementWithProperties("playbin3", map[string]interface{}{
		"uri": fmt.Sprintf("file://%s", h.adSource),
	})
	if err != nil {
		return fmt.Errorf("failed to create playbin: %v", err)
	}

	// Create video sink
	intervideosink, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel": "input2" + h.handlerID,
		"sync":    true,
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosink: %v", err)
	}

	// Create audio sink
	interaudiosink, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel": "audio2" + h.handlerID,
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

	// Create synchronization channel for ad pipeline readiness
	adPipelineReady := make(chan bool, 1)

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
					fmt.Printf("Ad pipeline is now PLAYING and ready\n")
					select {
					case adPipelineReady <- true:
						// Successfully sent ready signal
					default:
						// Channel is full, ignore
					}
				}
			case gst.MessageError:
				gerr := msg.ParseError()
				fmt.Printf("Ad pipeline error: %s\n", gerr.Error())
				h.stopAd()
			case gst.MessageEOS:
				fmt.Printf("Ad finished\n")
				h.stopAd()
			}
		}
	}()

	h.adPipeline = pipeline
	return nil
}

// insertAd starts playing an ad using compositor/audiomixer switching
func (h *AdInsertionHandler) insertAd() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.currentAdPlaying {
		fmt.Printf("Ad already playing, ignoring insert request\n")
		return
	}

	// Get current pipeline time for logging
	var currentTime gst.ClockTime
	if h.mainPipeline != nil {
		clock := h.mainPipeline.GetClock()
		if clock != nil {
			currentTime = clock.GetTime()
		}
	}

	h.logVerbose("Starting ad insertion at pipeline time: %v\n", currentTime)

	// Create and start ad pipeline
	err := h.createAdPipeline()
	if err != nil {
		fmt.Printf("Failed to create ad pipeline: %v\n", err)
		return
	}

	// Create synchronization channel for ad pipeline readiness
	adPipelineReady := make(chan bool, 1)

	// Set up bus watch for ad pipeline
	bus := h.adPipeline.GetBus()
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
					h.logVerbose("Ad pipeline is now PLAYING and ready\n")
					select {
					case adPipelineReady <- true:
						// Successfully sent ready signal
					default:
						// Channel is full, ignore
					}
				}
			case gst.MessageError:
				gerr := msg.ParseError()
				fmt.Printf("Ad pipeline error: %s\n", gerr.Error())
				h.stopAd()
			case gst.MessageEOS:
				h.logVerbose("Ad finished\n")
				h.stopAd()
			}
		}
	}()

	// Set ad pipeline to PLAYING
	h.logVerbose("Starting ad pipeline...\n")
	h.adPipeline.SetState(gst.StatePlaying)

	// Wait for ad pipeline to be ready with timeout
	select {
	case <-adPipelineReady:
		h.logVerbose("Ad pipeline is ready, switching to ad content\n")
	case <-time.After(5 * time.Second):
		fmt.Printf("Warning: Ad pipeline ready timeout, switching anyway...\n")
	}

	h.currentAdPlaying = true

	// Switch compositor to show ad (input2)
	pad1 := h.compositor.GetStaticPad("sink_0")
	pad2 := h.compositor.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 0.0) // Hide input1 (main stream)
		pad2.SetProperty("alpha", 1.0) // Show input2 (ad)
		h.logVerbose("Switched compositor to ad content\n")
	}

	// Set up timer to stop ad after duration
	time.AfterFunc(h.adDuration, func() {
		h.stopAd()
	})
}

// stopAd stops the currently playing ad
func (h *AdInsertionHandler) stopAd() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.currentAdPlaying {
		return
	}

	// Get current pipeline time for logging
	var currentTime gst.ClockTime
	if h.mainPipeline != nil {
		clock := h.mainPipeline.GetClock()
		if clock != nil {
			currentTime = clock.GetTime()
		}
	}

	h.logVerbose("Stopping ad at pipeline time: %v\n", currentTime)

	// Stop ad pipeline
	if h.adPipeline != nil {
		h.adPipeline.SetState(gst.StateNull)
		h.adPipeline = nil
	}

	h.currentAdPlaying = false

	// Switch compositor back to main stream (input1)
	pad1 := h.compositor.GetStaticPad("sink_0")
	pad2 := h.compositor.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 1.0) // Show input1 (main stream)
		pad2.SetProperty("alpha", 0.0) // Hide input2 (ad)
		h.logVerbose("Switched compositor back to main content\n")
	}
}

// monitorPipelines monitors the health of pipelines
func (h *AdInsertionHandler) monitorPipelines() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for h.running {
		select {
		case <-ticker.C:
			// Basic monitoring - could be extended with more sophisticated health checks
			if h.mainPipeline == nil {
				fmt.Printf("Main pipeline is nil, attempting restart...\n")
				h.restartMainPipeline()
			}
			if h.dummySourcePipeline == nil {
				fmt.Printf("Dummy source pipeline is nil, attempting restart...\n")
				h.restartDummySourcePipeline()
			}
		case <-h.stopChan:
			return
		}
	}
}

// restartMainPipeline restarts the main pipeline
func (h *AdInsertionHandler) restartMainPipeline() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.mainPipeline != nil {
		h.mainPipeline.SetState(gst.StateNull)
	}

	err := h.createMainPipeline()
	if err != nil {
		fmt.Printf("Failed to restart main pipeline: %v\n", err)
		return
	}

	h.mainPipeline.SetState(gst.StatePlaying)
}

// restartDummySourcePipeline restarts the dummy source pipeline
func (h *AdInsertionHandler) restartDummySourcePipeline() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.dummySourcePipeline != nil {
		h.dummySourcePipeline.SetState(gst.StateNull)
	}

	err := h.createDummySourcePipeline()
	if err != nil {
		fmt.Printf("Failed to restart dummy source pipeline: %v\n", err)
		return
	}

	h.dummySourcePipeline.SetState(gst.StatePlaying)
}

// Stop stops the SCTE-35 handler
func (h *AdInsertionHandler) Stop() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if !h.running {
		return
	}

	h.running = false
	close(h.stopChan)

	// Stop pipelines
	if h.mainPipeline != nil {
		h.mainPipeline.SetState(gst.StateNull)
	}

	if h.adPipeline != nil {
		h.adPipeline.SetState(gst.StateNull)
	}

	if h.dummySourcePipeline != nil {
		h.dummySourcePipeline.SetState(gst.StateNull)
	}
}

// SetAdSource sets the ad source file
func (h *AdInsertionHandler) SetAdSource(source string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.adSource = source
}

// SetAdDuration sets the ad duration
func (h *AdInsertionHandler) SetAdDuration(duration time.Duration) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.adDuration = duration
}

// getCurrentPTS gets the current PTS from the pipeline
func (h *AdInsertionHandler) getCurrentPTS() gst.ClockTime {
	if h.mainPipeline == nil {
		return gst.ClockTimeNone
	}

	clock := h.mainPipeline.GetClock()
	if clock == nil {
		return gst.ClockTimeNone
	}

	return clock.GetTime()
}

// convertPTSToDuration converts PTS (90kHz clock) to time.Duration
func (h *AdInsertionHandler) convertPTSToDuration(pts uint64) time.Duration {
	// PTS is in 90kHz clock units
	// 90kHz = 90,000 Hz = 11.111... microseconds per PTS unit
	return time.Duration(pts) * 11111 * time.Nanosecond / 1000000
}

// createDummySourcePipeline creates a dummy source pipeline to feed into intervideosink and interaudiosink
func (h *AdInsertionHandler) createDummySourcePipeline() error {
	pipeline, err := gst.NewPipeline("dummy-source-pipeline" + h.handlerID)
	if err != nil {
		return fmt.Errorf("failed to create dummy source pipeline: %v", err)
	}

	// Create dummy video source (black video)
	videoSrc, err := gst.NewElementWithProperties("videotestsrc", map[string]interface{}{
		"pattern": 0, // black
		"is-live": true,
		"name":    "dummyVideoSrc" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create dummy video source: %v", err)
	}

	// Create dummy audio source (silence)
	audioSrc, err := gst.NewElementWithProperties("audiotestsrc", map[string]interface{}{
		"wave":    0, // silence
		"is-live": true,
		"name":    "dummyAudioSrc" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create dummy audio source: %v", err)
	}

	// Create intervideosink to feed into main pipeline
	intervideosink, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel": "input1" + h.handlerID,
		"sync":    true,
		"name":    "dummyInterVideoSink" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create dummy intervideosink: %v", err)
	}

	// Create interaudiosink to feed into main pipeline
	interaudiosink, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel": "audio1" + h.handlerID,
		"sync":    true,
		"name":    "dummyInterAudioSink" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create dummy interaudiosink: %v", err)
	}

	// Create video converter and capsfilter
	videoConv, err := gst.NewElement("videoconvert")
	if err != nil {
		return fmt.Errorf("failed to create video converter: %v", err)
	}

	videoCaps, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": gst.NewCapsFromString("video/x-raw, width=1920, height=1080, framerate=30/1"),
		"name": "dummyVideoCaps" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create video caps filter: %v", err)
	}

	// Create audio converter and capsfilter
	audioConv, err := gst.NewElement("audioconvert")
	if err != nil {
		return fmt.Errorf("failed to create audio converter: %v", err)
	}

	audioCaps, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": gst.NewCapsFromString("audio/x-raw, format=S16LE, layout=interleaved, rate=48000, channels=2"),
		"name": "dummyAudioCaps" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audio caps filter: %v", err)
	}

	// Add all elements to pipeline
	pipeline.AddMany(videoSrc, videoConv, videoCaps, intervideosink)
	pipeline.AddMany(audioSrc, audioConv, audioCaps, interaudiosink)

	// Link video elements
	videoSrc.Link(videoConv)
	videoConv.Link(videoCaps)
	videoCaps.Link(intervideosink)

	// Link audio elements
	audioSrc.Link(audioConv)
	audioConv.Link(audioCaps)
	audioCaps.Link(interaudiosink)

	h.dummySourcePipeline = pipeline
	return nil
}

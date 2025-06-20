package main

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/go-gst/go-gst/gst"
	"github.com/google/uuid"
)

// SCTE35Message represents a parsed SCTE-35 message
type SCTE35Message struct {
	TableID                uint8
	SectionSyntaxIndicator bool
	PrivateIndicator       bool
	SectionLength          uint16
	ProtocolVersion        uint8
	EncryptedPacket        bool
	EncryptionAlgorithm    uint8
	PTSAdjustment          uint64
	CWIndex                uint8
	Tier                   uint16
	SpliceCommandLength    uint16
	SpliceCommandType      uint8
}

// AdInsertionHandler handles SCTE-35 messages and ad insertion using compositor/audiomixer
type AdInsertionHandler struct {
	inputHost        string
	inputPort        int
	outputHost       string
	outputPort       int
	adSource         string
	mainPipeline     *gst.Pipeline
	adPipeline       *gst.Pipeline
	compositor       *gst.Element
	audiomixer       *gst.Element
	mutex            sync.Mutex
	stopChan         chan struct{}
	running          bool
	handlerID        string
	currentAdPlaying bool
	adDuration       time.Duration
	mainStreamPaused bool
	scheduledAds     map[gst.ClockTime]bool // Track scheduled ad insertions
}

// NewAdInsertionHandler creates a new SCTE-35 handler
func NewAdInsertionHandler(inputHost string, inputPort int, outputHost string, outputPort int, adSource string) (*AdInsertionHandler, error) {
	gst.Init(nil)

	return &AdInsertionHandler{
		inputHost:    inputHost,
		inputPort:    inputPort,
		outputHost:   outputHost,
		outputPort:   outputPort,
		adSource:     adSource,
		stopChan:     make(chan struct{}),
		handlerID:    uuid.New().String(),
		adDuration:   30 * time.Second, // Default ad duration
		scheduledAds: make(map[gst.ClockTime]bool),
	}, nil
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

	// Start the main pipeline
	h.mainPipeline.SetState(gst.StatePlaying)

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

	// Create RTP depayloader
	rtpdepay, err := gst.NewElement("rtpmp2tdepay")
	if err != nil {
		return fmt.Errorf("failed to create rtpmp2tdepay: %v", err)
	}

	// Create demuxer for main stream processing with SCTE-35 event forwarding
	demux, err := gst.NewElementWithProperties("tsdemux", map[string]interface{}{
		"name": "tsdemux" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create tsdemux: %v", err)
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

	// Create video converter and encoder
	videoconv, err := gst.NewElement("videoconvert")
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

	// Create audio converter and encoder
	audioconv, err := gst.NewElement("audioconvert")
	audioconv.SetProperty("name", "audioconv"+h.handlerID)
	if err != nil {
		return fmt.Errorf("failed to create audioconvert: %v", err)
	}

	aacenc, err := gst.NewElementWithProperties("avenc_aac", map[string]interface{}{
		"bitrate": 128000,
		"name":    "aacenc" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create aacenc: %v", err)
	}

	// Create muxer and RTP elements
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

	// Create queues for latency management
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

	videoMixerQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0,
		"name":               "videoMixerQueue" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create videoMixerQueue: %v", err)
	}

	audioMixerQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0,
		"name":               "audioMixerQueue" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audioMixerQueue: %v", err)
	}

	muxerQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   200,
		"max-size-time":      uint64(1 * time.Second),
		"min-threshold-time": uint64(100 * time.Millisecond),
		"leaky":              0,
		"name":               "muxerQueue" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create muxerQueue: %v", err)
	}

	// Create audio converter elements for each input
	audioconv1, err := gst.NewElement("audioconvert")
	audioconv1.SetProperty("name", "audioconv1"+h.handlerID)
	if err != nil {
		return fmt.Errorf("failed to create audioconv1: %v", err)
	}

	audioconv2, err := gst.NewElement("audioconvert")
	audioconv2.SetProperty("name", "audioconv2"+h.handlerID)
	if err != nil {
		return fmt.Errorf("failed to create audioconv2: %v", err)
	}

	// Create audioresample elements
	audioresample1, err := gst.NewElement("audioresample")
	audioresample1.SetProperty("name", "audioresample1"+h.handlerID)
	if err != nil {
		return fmt.Errorf("failed to create audioresample1: %v", err)
	}

	audioresample2, err := gst.NewElement("audioresample")
	audioresample2.SetProperty("name", "audioresample2"+h.handlerID)
	if err != nil {
		return fmt.Errorf("failed to create audioresample2: %v", err)
	}

	// Create capsfilters for audio format
	audiocaps1, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": gst.NewCapsFromString("audio/x-raw, format=S16LE, layout=interleaved, rate=48000, channels=2"),
		"name": "audiocaps1" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audiocaps1: %v", err)
	}

	audiocaps2, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": gst.NewCapsFromString("audio/x-raw, format=S16LE, layout=interleaved, rate=48000, channels=2"),
		"name": "audiocaps2" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audiocaps2: %v", err)
	}

	// Add all elements to the pipeline
	rtpCaps := gst.NewCapsFromString("application/x-rtp, media=video, clock-rate=90000, encoding-name=MP2T, payload=33")
	capsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": rtpCaps,
		"name": "rtpCapsFilter" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create capsfilter: %v", err)
	}
	pipeline.Add(capsfilter)
	udpsrc.Link(capsfilter)
	capsfilter.Link(rtpdepay)
	pipeline.AddMany(demux)
	pipeline.AddMany(intervideo1, intervideo2, h.compositor, videoconv, h264enc)
	pipeline.AddMany(interaudio1, interaudio2, h.audiomixer, audioconv, aacenc)
	pipeline.AddMany(mpegtsmux, rtpmp2tpay, udpsink)
	pipeline.AddMany(videoQueue1, videoQueue2, audioQueue1, audioQueue2)
	pipeline.AddMany(videoMixerQueue, audioMixerQueue, muxerQueue)
	pipeline.AddMany(audioconv1, audioconv2, audioresample1, audioresample2, audiocaps1, audiocaps2)

	// Create intervideosink and interaudiosink elements upfront
	intervideosink1, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel": "input1" + h.handlerID,
		"sync":    true,
		"name":    "intervideosink1" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosink1: %v", err)
	}

	interaudiosink1, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel": "audio1" + h.handlerID,
		"sync":    true,
		"name":    "interaudiosink1" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create interaudiosink1: %v", err)
	}

	// Add the sink elements to pipeline
	pipeline.AddMany(intervideosink1, interaudiosink1)

	// Link elements
	rtpdepay.Link(demux)

	// Set up dynamic pad-added signal for demuxer
	demux.Connect("pad-added", func(element *gst.Element, pad *gst.Pad) {
		padName := pad.GetName()
		fmt.Printf("Dynamic pad added: %s\n", padName)

		if padName == "video_0" {
			// Link video pad to existing intervideosink
			result := pad.Link(intervideosink1.GetStaticPad("sink"))
			if result != gst.PadLinkOK {
				fmt.Printf("Failed to link video pad: %v\n", result)
			} else {
				fmt.Printf("Successfully linked video pad to intervideosink\n")
			}
		} else if padName == "audio_0" {
			// Link audio pad to existing interaudiosink
			result := pad.Link(interaudiosink1.GetStaticPad("sink"))
			if result != gst.PadLinkOK {
				fmt.Printf("Failed to link audio pad: %v\n", result)
			} else {
				fmt.Printf("Successfully linked audio pad to interaudiosink\n")
			}
		}
	})

	// Link video elements
	intervideo1.Link(videoQueue1)
	videoQueue1.Link(h.compositor)
	intervideo2.Link(videoQueue2)
	videoQueue2.Link(h.compositor)
	h.compositor.Link(videoMixerQueue)
	videoMixerQueue.Link(videoconv)
	videoconv.Link(h264enc)
	h264enc.Link(muxerQueue)

	// Link audio elements
	interaudio1.Link(audioQueue1)
	audioQueue1.Link(audioconv1)
	audioconv1.Link(audioresample1)
	audioresample1.Link(audiocaps1)
	audiocaps1.Link(h.audiomixer)

	interaudio2.Link(audioQueue2)
	audioQueue2.Link(audioconv2)
	audioconv2.Link(audioresample2)
	audioresample2.Link(audiocaps2)
	audiocaps2.Link(h.audiomixer)

	h.audiomixer.Link(audioMixerQueue)
	audioMixerQueue.Link(audioconv)
	audioconv.Link(aacenc)
	aacenc.Link(mpegtsmux)

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

				// Handle specific inter element errors
				if elementName == "intervideosrc1"+h.handlerID || elementName == "intervideosrc2"+h.handlerID {
					fmt.Printf("Video inter source error - this may be normal during startup\n")
				} else if elementName == "interaudiosrc1"+h.handlerID || elementName == "interaudiosrc2"+h.handlerID {
					fmt.Printf("Audio inter source error - this may be normal during startup\n")
				}

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
					fmt.Printf("Element %s is now PLAYING\n", msg.Source())
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

// handleSCTE35Event handles SCTE-35 events detected from the stream
func (h *AdInsertionHandler) handleSCTE35Event(event *gst.Event) {
	fmt.Printf("Handling SCTE-35 event\n")

	// For now, we'll trigger ad insertion on any event
	// In a real implementation, you would parse the event data to extract SCTE-35 information
	fmt.Printf("SCTE-35 event detected - inserting ad\n")
	h.insertAd()
}

// handleSCTE35EventWithPTS handles SCTE-35 events with PTS timing information
func (h *AdInsertionHandler) handleSCTE35EventWithPTS(data []byte, pts uint64, duration uint64) {

	// Parse SCTE-35 message
	message := h.parseSCTE35Message(data)
	if message == nil {
		fmt.Println("Failed to parse SCTE-35 message")
		return
	}

	fmt.Printf("SCTE-35 Command Type: %d\n", message.SpliceCommandType)

	switch message.SpliceCommandType {
	case 0x05: // Splice Insert
		fmt.Printf("Splice Insert command detected - scheduling ad insertion\n")
		h.scheduleAdInsertion(message.PTSAdjustment, message)
	case 0x06: // Splice Null
		fmt.Printf("Splice Null command detected\n")
	case 0x07: // Splice Schedule
		fmt.Printf("Splice Schedule command detected - scheduling ad insertion\n")
		h.scheduleAdInsertion(message.PTSAdjustment, message)
	default:
		fmt.Printf("Unknown SCTE-35 command type: %d\n", message.SpliceCommandType)
	}
}

// scheduleAdInsertion schedules ad insertion at the correct PTS time
func (h *AdInsertionHandler) scheduleAdInsertion(pts uint64, message *SCTE35Message) {
	// Get current PTS from pipeline
	currentPTS := h.getCurrentPTS()

	// Convert PTS to time.Duration (PTS is in 90kHz clock units)
	ptsDuration := h.convertPTSToDuration(pts)

	// Calculate when to insert the ad based on PTS adjustment
	// PTS adjustment is the offset from the current PTS to when the splice should occur
	ptsAdjustmentDuration := h.convertPTSToDuration(message.PTSAdjustment)
	insertTime := ptsDuration + ptsAdjustmentDuration

	fmt.Printf("Scheduling ad insertion - Current PTS: %v, Event PTS: %v, PTS Adjustment: %v, Insert Time: %v\n",
		currentPTS, pts, message.PTSAdjustment, insertTime)

	// Schedule the ad insertion
	time.AfterFunc(insertTime, func() {
		h.insertAd()
	})
}

// parseSCTE35Message parses a SCTE-35 message from raw data
func (h *AdInsertionHandler) parseSCTE35Message(data []byte) *SCTE35Message {
	if len(data) < 8 {
		return nil
	}

	// Basic SCTE-35 parsing
	message := &SCTE35Message{
		TableID:                data[0],
		SectionSyntaxIndicator: (data[1] & 0x80) != 0,
		PrivateIndicator:       (data[1] & 0x40) != 0,
		SectionLength:          binary.BigEndian.Uint16(data[1:3]) & 0x0FFF,
	}

	// Check if this is a SCTE-35 splice insert command
	if len(data) >= 19 && data[0] == 0xFC { // SCTE-35 table ID
		message.ProtocolVersion = data[3]
		message.EncryptedPacket = (data[4] & 0x80) != 0
		message.EncryptionAlgorithm = data[4] & 0x3F
		message.PTSAdjustment = binary.BigEndian.Uint64(append([]byte{0}, data[5:13]...))
		message.CWIndex = data[13]
		message.Tier = binary.BigEndian.Uint16(data[14:16])
		message.SpliceCommandLength = binary.BigEndian.Uint16(data[16:18])
		message.SpliceCommandType = data[18]

		return message
	}

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

	fmt.Printf("Starting ad insertion at pipeline time: %v\n", currentTime)

	// Create and start ad pipeline
	err := h.createAdPipeline()
	if err != nil {
		fmt.Printf("Failed to create ad pipeline: %v\n", err)
		return
	}

	h.adPipeline.SetState(gst.StatePlaying)
	h.currentAdPlaying = true

	// Switch compositor to show ad (input2)
	pad1 := h.compositor.GetStaticPad("sink_0")
	pad2 := h.compositor.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 0.0) // Hide input1 (main stream)
		pad2.SetProperty("alpha", 1.0) // Show input2 (ad)
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

	fmt.Printf("Stopping ad at pipeline time: %v\n", currentTime)

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

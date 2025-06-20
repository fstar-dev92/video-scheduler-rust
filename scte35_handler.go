package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
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
	// Add more fields as needed for your specific SCTE-35 implementation
}

// AdInsertionHandler handles SCTE-35 messages and ad insertion
type AdInsertionHandler struct {
	inputHost        string
	inputPort        int
	outputHost       string
	outputPort       int
	adSource         string
	mainPipeline     *gst.Pipeline
	adPipeline       *gst.Pipeline
	mutex            sync.Mutex
	stopChan         chan struct{}
	running          bool
	handlerID        string
	currentAdPlaying bool
	adDuration       time.Duration
	mainStreamPaused bool
}

// NewAdInsertionHandler creates a new SCTE-35 handler
func NewAdInsertionHandler(inputHost string, inputPort int, outputHost string, outputPort int, adSource string) (*AdInsertionHandler, error) {
	gst.Init(nil)

	return &AdInsertionHandler{
		inputHost:  inputHost,
		inputPort:  inputPort,
		outputHost: outputHost,
		outputPort: outputPort,
		adSource:   adSource,
		stopChan:   make(chan struct{}),
		handlerID:  uuid.New().String(),
		adDuration: 30 * time.Second, // Default ad duration
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

	// Start UDP listener for SCTE-35 messages
	go h.startUDPListener()

	// Start monitoring goroutine
	go h.monitorPipelines()

	return nil
}

// createMainPipeline creates the main pipeline for normal stream processing
func (h *AdInsertionHandler) createMainPipeline() error {
	pipeline, err := gst.NewPipeline("main-pipeline" + h.handlerID)
	if err != nil {
		return fmt.Errorf("failed to create main pipeline: %v", err)
	}

	// Create UDP source
	udpsrc, err := gst.NewElementWithProperties("udpsrc", map[string]interface{}{
		"host":           h.inputHost,
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

	// Create demuxer
	demux, err := gst.NewElement("tsdemux")
	if err != nil {
		return fmt.Errorf("failed to create tsdemux: %v", err)
	}

	// Create video and audio parsers
	videoParser, err := gst.NewElement("h264parse")
	if err != nil {
		return fmt.Errorf("failed to create h264parse: %v", err)
	}

	audioParser, err := gst.NewElement("aacparse")
	if err != nil {
		return fmt.Errorf("failed to create aacparse: %v", err)
	}

	// Create video and audio encoders
	videoEnc, err := gst.NewElementWithProperties("x264enc", map[string]interface{}{
		"tune":    0x00000004, // zerolatency
		"bitrate": 2000,       // 2 Mbps
		"name":    "h264enc" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create h264enc: %v", err)
	}

	audioEnc, err := gst.NewElementWithProperties("avenc_aac", map[string]interface{}{
		"bitrate": 128000,
		"name":    "aacenc" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create aacenc: %v", err)
	}

	// Create muxer
	muxer, err := gst.NewElementWithProperties("mpegtsmux", map[string]interface{}{
		"alignment":    7,
		"pat-interval": int64(100 * 1000000),
		"pmt-interval": int64(100 * 1000000),
		"pcr-interval": int64(20 * 1000000),
		"name":         "mpegtsmux" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create mpegtsmux: %v", err)
	}

	// Create RTP payloader
	rtppay, err := gst.NewElementWithProperties("rtpmp2tpay", map[string]interface{}{
		"pt":              33,
		"mtu":             1400,
		"perfect-rtptime": true,
		"name":            "rtpmp2tpay" + h.handlerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create rtpmp2tpay: %v", err)
	}

	// Create UDP sink
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

	// Add elements to pipeline
	pipeline.AddMany(udpsrc, rtpdepay, demux, videoParser, audioParser, videoEnc, audioEnc, muxer, rtppay, udpsink)

	// Link elements
	udpsrc.Link(rtpdepay)
	rtpdepay.Link(demux)

	// Set up dynamic pad-added signal for demuxer
	demux.Connect("pad-added", func(element *gst.Element, pad *gst.Pad) {
		padName := pad.GetName()
		fmt.Printf("Dynamic pad added: %s\n", padName)

		if padName == "video_0" {
			// Link video pad
			pad.Link(videoParser.GetStaticPad("sink"))
			videoParser.Link(videoEnc)
			videoEnc.Link(muxer)
		} else if padName == "audio_0" {
			// Link audio pad
			pad.Link(audioParser.GetStaticPad("sink"))
			audioParser.Link(audioEnc)
			audioEnc.Link(muxer)
		}
	})

	// Link muxer to output
	muxer.Link(rtppay)
	rtppay.Link(udpsink)

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
				fmt.Printf("Error from element %s: %s\n", msg.Source(), gerr.Error())
			case gst.MessageWarning:
				gerr := msg.ParseWarning()
				fmt.Printf("Warning from element %s: %s\n", msg.Source(), gerr.Error())
			case gst.MessageEOS:
				fmt.Printf("End of stream received\n")
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
		"channel": "ad-video" + h.handlerID,
		"sync":    true,
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosink: %v", err)
	}

	// Create audio sink
	interaudiosink, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel": "ad-audio" + h.handlerID,
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

// startUDPListener starts listening for SCTE-35 messages on UDP
func (h *AdInsertionHandler) startUDPListener() {
	addr := fmt.Sprintf("%s:%d", h.inputHost, h.inputPort+1000) // Use different port for SCTE-35
	conn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.ParseIP(h.inputHost),
		Port: h.inputPort + 1000,
	})
	if err != nil {
		log.Printf("Failed to listen for SCTE-35 messages: %v", err)
		return
	}
	defer conn.Close()

	fmt.Printf("Listening for SCTE-35 messages on %s\n", addr)

	buffer := make([]byte, 4096)
	for h.running {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if h.running {
				log.Printf("Error reading SCTE-35 message: %v", err)
			}
			continue
		}

		// Parse SCTE-35 message
		message := h.parseSCTE35Message(buffer[:n])
		if message != nil {
			h.handleSCTE35Message(message)
		}
	}
}

// parseSCTE35Message parses a SCTE-35 message from UDP data
func (h *AdInsertionHandler) parseSCTE35Message(data []byte) *SCTE35Message {
	if len(data) < 8 {
		return nil
	}

	// Basic SCTE-35 parsing (simplified)
	// In a real implementation, you would need more sophisticated parsing
	message := &SCTE35Message{
		TableID:                data[0],
		SectionSyntaxIndicator: (data[1] & 0x80) != 0,
		PrivateIndicator:       (data[1] & 0x40) != 0,
		SectionLength:          binary.BigEndian.Uint16(data[1:3]) & 0x0FFF,
	}

	// Check if this is a SCTE-35 splice insert command
	if len(data) >= 14 && data[0] == 0xFC { // SCTE-35 table ID
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

// handleSCTE35Message handles a parsed SCTE-35 message
func (h *AdInsertionHandler) handleSCTE35Message(message *SCTE35Message) {
	fmt.Printf("Received SCTE-35 message: TableID=%d, CommandType=%d\n",
		message.TableID, message.SpliceCommandType)

	// Handle different SCTE-35 command types
	switch message.SpliceCommandType {
	case 0x05: // Splice Insert
		fmt.Printf("Splice Insert command received - inserting ad\n")
		h.insertAd()
	case 0x06: // Splice Null
		fmt.Printf("Splice Null command received\n")
	case 0x07: // Splice Schedule
		fmt.Printf("Splice Schedule command received\n")
		h.insertAd()
	default:
		fmt.Printf("Unknown SCTE-35 command type: %d\n", message.SpliceCommandType)
	}
}

// insertAd starts playing an ad
func (h *AdInsertionHandler) insertAd() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.currentAdPlaying {
		fmt.Printf("Ad already playing, ignoring insert request\n")
		return
	}

	fmt.Printf("Starting ad insertion\n")

	// Pause main stream
	if h.mainPipeline != nil {
		h.mainPipeline.SetState(gst.StatePaused)
		h.mainStreamPaused = true
	}

	// Create and start ad pipeline
	err := h.createAdPipeline()
	if err != nil {
		fmt.Printf("Failed to create ad pipeline: %v\n", err)
		// Resume main stream
		if h.mainPipeline != nil {
			h.mainPipeline.SetState(gst.StatePlaying)
			h.mainStreamPaused = false
		}
		return
	}

	h.adPipeline.SetState(gst.StatePlaying)
	h.currentAdPlaying = true

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

	fmt.Printf("Stopping ad\n")

	// Stop ad pipeline
	if h.adPipeline != nil {
		h.adPipeline.SetState(gst.StateNull)
		h.adPipeline = nil
	}

	h.currentAdPlaying = false

	// Resume main stream
	if h.mainPipeline != nil && h.mainStreamPaused {
		h.mainPipeline.SetState(gst.StatePlaying)
		h.mainStreamPaused = false
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

package scheduler

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-gst/go-glib/glib"
	"github.com/go-gst/go-gst/gst"
)

// StreamItem represents a scheduled video item
type StreamItem struct {
	Type     string        // "file" or "test" (test pattern)
	Source   string        // File path for "file" type
	Start    time.Time     // When to start playing this item
	Duration time.Duration // How long to play this item
}

// StreamScheduler manages a GStreamer pipeline for scheduled playback
type StreamScheduler struct {
	host      string
	port      int
	items     []StreamItem
	pipeline  *gst.Pipeline
	mainLoop  *glib.MainLoop
	vselector *gst.Element
	aselector *gst.Element
	mutex     sync.Mutex
	stopChan  chan struct{}
	running   bool
	sources   map[int][]*gst.Element // Track sources by index
	baseTime      int64        // Base time for continuous timestamps
	currentOffset int64        // Current offset for timestamps
}

// NewStreamScheduler creates a new stream scheduler
func NewStreamScheduler(host string, port int) (*StreamScheduler, error) {
	return &StreamScheduler{
		host:     host,
		port:     port,
		items:    make([]StreamItem, 0),
		stopChan: make(chan struct{}),
		sources:  make(map[int][]*gst.Element),
	}, nil
}

// AddItem adds a scheduled item to play
func (s *StreamScheduler) AddItem(item StreamItem) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.items = append(s.items, item)
}

// Start begins the scheduling and streaming
func (s *StreamScheduler) Start() error {
	s.mutex.Lock()
	if s.running {
		s.mutex.Unlock()
		return fmt.Errorf("scheduler is already running")
	}
	s.running = true
	s.mutex.Unlock()

	// Create the pipeline
	var err error
	s.pipeline, err = gst.NewPipeline("streaming-pipeline")
	if err != nil {
		return fmt.Errorf("failed to create pipeline: %v", err)
	}

	// Create elements for output (RTP streaming)
	s.vselector, err = gst.NewElement("input-selector")
	if err != nil {
		return fmt.Errorf("failed to create vselector: %v", err)
	}
	s.vselector.SetProperty("name", "vselector")

	s.aselector, err = gst.NewElement("input-selector")
	if err != nil {
		return fmt.Errorf("failed to create aselector: %v", err)
	}
	s.aselector.SetProperty("name", "aselector")

	// Add identity elements to reset timestamps
	videoIdentity, err := gst.NewElement("identity")
	if err != nil {
		return fmt.Errorf("failed to create video identity: %v", err)
	}
	videoIdentity.SetProperty("reset-pts", true)
	videoIdentity.SetProperty("sync", true)

	audioIdentity, err := gst.NewElement("identity")
	if err != nil {
		return fmt.Errorf("failed to create audio identity: %v", err)
	}
	audioIdentity.SetProperty("reset-pts", true)
	audioIdentity.SetProperty("sync", true)

	videoconv, err := gst.NewElement("videoconvert")
	if err != nil {
		return fmt.Errorf("failed to create videoconv: %v", err)
	}

	// Add a videoscale element to handle resolution
	videoscale, err := gst.NewElement("videoscale")
	if err != nil {
		return fmt.Errorf("failed to create videoscale: %v", err)
	}

	// Add capsfilter to limit video size and framerate
	videocaps, err := gst.NewElement("capsfilter")
	if err != nil {
		return fmt.Errorf("failed to create videocaps: %v", err)
	}

	// Set video to 720p max resolution at 30fps with more flexible format settings
	capstr := "video/x-raw,width=(int)[1,1920],height=(int)[1,1080],framerate=(fraction)[1/1,30/1]"
	caps := gst.NewCapsFromString(capstr)
	videocaps.SetProperty("caps", caps)

	// Change to h264 encoder with more compatible settings
	h264enc, err := gst.NewElement("x264enc")
	if err != nil {
		return fmt.Errorf("failed to create h264enc: %v", err)
	}

	// Add h264 parser to ensure proper stream formatting
	h264parse, err := gst.NewElement("h264parse")
	if err != nil {
		return fmt.Errorf("failed to create h264parse: %v", err)
	}

	// Add audioconvert and audioresample before the selector to handle format differences
	audioconv, err := gst.NewElement("audioconvert")
	if err != nil {
		return fmt.Errorf("failed to create audioconv: %v", err)
	}

	audioresample, err := gst.NewElement("audioresample")
	if err != nil {
		return fmt.Errorf("failed to create audioresample: %v", err)
	}

	// Add audio caps filter to standardize audio format
	audiocaps, err := gst.NewElement("capsfilter")
	if err != nil {
		return fmt.Errorf("failed to create audiocaps: %v", err)
	}
	audiocapsstr := "audio/x-raw,rate=44100,channels=2"
	acaps := gst.NewCapsFromString(audiocapsstr)
	audiocaps.SetProperty("caps", acaps)

	aacenc, err := gst.NewElement("avenc_aac")
	if err != nil {
		return fmt.Errorf("failed to create aacenc: %v", err)
	}

	// Add MPEG-TS muxer
	mpegtsmux, err := gst.NewElement("mpegtsmux")
	if err != nil {
		return fmt.Errorf("failed to create mpegtsmux: %v", err)
	}
	mpegtsmux.SetProperty("name", "mux")
	
	// Add RTP payloader for MPEG-TS
	rtpmp2tpay, err := gst.NewElement("rtpmp2tpay")
	if err != nil {
		return fmt.Errorf("failed to create rtpmp2tpay: %v", err)
	}
	
	// UDP sink for RTP MPEG-TS
	udpsink, err := gst.NewElement("udpsink")
	if err != nil {
		return fmt.Errorf("failed to create udpsink: %v", err)
	}
	
	// Configure elements for better latency handling
	videoIdentity.SetProperty("sync", true)
	audioIdentity.SetProperty("sync", true)
	finalIdentity, err := gst.NewElement("identity")
	if err != nil {
		return fmt.Errorf("failed to create final identity: %v", err)
	}
	finalIdentity.SetProperty("single-segment", true)
	finalIdentity.SetProperty("sync", true)
	
	// Set max-lateness property on udpsink to handle late buffers better
	udpsink.SetProperty("max-lateness", 10000000) // 10ms max lateness
	udpsink.SetProperty("buffer-size", 2097152)   // 2MB buffer size
	
	// Configure input-selector elements for better synchronization
	s.vselector.SetProperty("sync-streams", true)
	s.vselector.SetProperty("sync-mode", 1) // 1 = sync-to-clock
	s.aselector.SetProperty("sync-streams", true)
	s.aselector.SetProperty("sync-mode", 1)

	// Configure H264 encoder for better compatibility and performance
	h264enc.SetProperty("tune", "zerolatency")
	h264enc.SetProperty("bitrate", 2000)             // Set bitrate to 2Mbps for better quality
	h264enc.SetProperty("key-int-max", 30)           // Key frame every 30 frames
	h264enc.SetProperty("byte-stream", true)         // Use byte stream format for NAL units
	h264enc.SetProperty("speed-preset", "superfast") // Faster encoding
	h264enc.SetProperty("threads", 4)                // Use 4 threads for encoding

	aacenc.SetProperty("bitrate", 128000) // 128kbps audio

	// Configure RTP MPEG-TS payloader
	rtpmp2tpay.SetProperty("pt", 33)  // Payload type for MPEG-TS
	
	// Set UDP properties for RTP MPEG-TS multicast
	udpsink.SetProperty("host", s.host)
	udpsink.SetProperty("port", s.port)
	udpsink.SetProperty("auto-multicast", true)

	// Configure mpegtsmux for better timestamp handling
	mpegtsmux.SetProperty("alignment", 7)  // 7 = GST_MPEG_TS_MUX_ALIGNMENT_ALIGNED
	
	// Use nanoseconds for time values (1 ms = 1,000,000 ns)
	mpegtsmux.SetProperty("pat-interval", int64(100 * 1000000))  // 100 ms
	mpegtsmux.SetProperty("pmt-interval", int64(100 * 1000000))  // 100 ms
	mpegtsmux.SetProperty("pcr-interval", int64(20 * 1000000))   // 20 ms
	
	// Add a tsmux property to handle timestamp discontinuities better
	mpegtsmux.SetProperty("dts-delta", int64(1000 * 1000000))    // 1000 ms (1 second)

	// Add elements to pipeline
	s.pipeline.Add(s.vselector)
	s.pipeline.Add(s.aselector)
	s.pipeline.Add(videoIdentity)
	s.pipeline.Add(audioIdentity)
	s.pipeline.Add(videoconv)
	s.pipeline.Add(videoscale)
	s.pipeline.Add(videocaps)
	s.pipeline.Add(h264enc)
	s.pipeline.Add(h264parse)
	s.pipeline.Add(audioconv)
	s.pipeline.Add(audioresample)
	s.pipeline.Add(audiocaps)
	s.pipeline.Add(aacenc)
	s.pipeline.Add(mpegtsmux)
	s.pipeline.Add(finalIdentity)
	s.pipeline.Add(rtpmp2tpay)
	s.pipeline.Add(udpsink)

	// Link static elements for video path to muxer
	s.vselector.Link(videoIdentity)
	videoIdentity.Link(videoconv)
	videoconv.Link(videoscale)
	videoscale.Link(videocaps)
	videocaps.Link(h264enc)
	h264enc.Link(h264parse)
	h264parse.Link(mpegtsmux)

	// Link static elements for audio path to muxer
	s.aselector.Link(audioIdentity)
	audioIdentity.Link(audioconv)
	audioconv.Link(audioresample)
	audioresample.Link(audiocaps)
	audiocaps.Link(aacenc)
	aacenc.Link(mpegtsmux)

	// Link muxer to final identity to RTP payloader to UDP sink
	mpegtsmux.Link(finalIdentity)
	finalIdentity.Link(rtpmp2tpay)
	rtpmp2tpay.Link(udpsink)

	// Add pad probes to drop late buffers
	vsrcpad := s.vselector.GetStaticPad("src")
	if vsrcpad != nil {
		vsrcpad.AddProbe(gst.PadProbeTypeBuffer, func(pad *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
			return gst.PadProbeOK
		})
	}

	asrcpad := s.aselector.GetStaticPad("src")
	if asrcpad != nil {
		asrcpad.AddProbe(gst.PadProbeTypeBuffer, func(pad *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
			return gst.PadProbeOK
		})
	}

	// Add pad probes to monitor caps negotiation
	vsrcpad = s.vselector.GetStaticPad("src")
	if vsrcpad != nil {
		vsrcpad.AddProbe(gst.PadProbeTypeEventDownstream, func(pad *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
			event := info.GetEvent()
			if event != nil && event.Type() == gst.EventTypeCaps {
				caps := event.ParseCaps()
				if caps != nil {
					fmt.Printf("Video caps from selector: %s\n", caps.String())
				}
			}
			return gst.PadProbeOK
		})
	}

	// Create a main loop
	s.mainLoop = glib.NewMainLoop(nil, false)

	// Create sources for each scheduled item
	s.mutex.Lock()
	items := make([]StreamItem, len(s.items))
	copy(items, s.items)
	s.mutex.Unlock()

	// Add sources to pipeline
	for i, item := range items {
		if item.Type == "file" {
			err := s.addFileSource(i, item.Source)
			if err != nil {
				return fmt.Errorf("failed to add source %d: %v", i, err)
			}
		}
		// Could add other source types here (test pattern, etc.)
	}

	// Set up the schedule
	go s.runSchedule()

	// Start the pipeline
	s.pipeline.SetState(gst.StatePlaying)
	fmt.Printf("Pipeline is running. Streaming RTP MPEG-TS to %s:%d\n", s.host, s.port)

	// Run the main loop in a separate goroutine
	go s.mainLoop.Run()

	s.baseTime = 0
	s.currentOffset = 0

	// Add a probe on the final identity element to adjust timestamps
	finalSrcPad := finalIdentity.GetStaticPad("src")
	if finalSrcPad != nil {
		finalSrcPad.AddProbe(gst.PadProbeTypeBuffer, func(pad *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
			// We can't modify the buffer directly in go-gst, but we can log information
			return gst.PadProbeOK
		})
	}

	return nil
}

// addFileSource adds a file source to the pipeline
func (s *StreamScheduler) addFileSource(index int, filePath string) error {
	fmt.Printf("Adding file source for %s at index %d\n", filePath, index)
	
	// Instead of uridecodebin, let's use a more explicit pipeline for better control
	filesrc, err := gst.NewElement("filesrc")
	if err != nil {
		return fmt.Errorf("failed to create filesrc: %v", err)
	}
	filesrc.SetProperty("name", fmt.Sprintf("filesrc%d", index))
	
	// Make sure to handle relative paths
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %v", err)
	}
	
	filesrc.SetProperty("location", absPath)
	filesrc.SetProperty("buffer-size", 10485760) // 10MB buffer
	
	// Add typefind to detect file type
	typefind, err := gst.NewElement("typefind")
	if err != nil {
		return fmt.Errorf("failed to create typefind: %v", err)
	}
	
	// Add decodebin to handle decoding
	decodebin, err := gst.NewElement("decodebin")
	if err != nil {
		return fmt.Errorf("failed to create decodebin: %v", err)
	}
	decodebin.SetProperty("name", fmt.Sprintf("decode%d", index))
	
	// Add elements to pipeline
	s.pipeline.Add(filesrc)
	s.pipeline.Add(typefind)
	s.pipeline.Add(decodebin)
	
	// Link filesrc -> typefind -> decodebin
	filesrc.Link(typefind)
	typefind.Link(decodebin)
	
	// Set elements to playing state initially, then pause them
	// This ensures they're fully initialized and ready to play
	filesrc.SetState(gst.StatePlaying)
	typefind.SetState(gst.StatePlaying)
	decodebin.SetState(gst.StatePlaying)
	
	// After a short delay, pause the source if it's not the first one
	if index > 0 {
		go func() {
			time.Sleep(500 * time.Millisecond) // Give it time to initialize
			filesrc.SetState(gst.StatePaused)
		}()
	}
	
	// Connect to typefind's "have-type" signal for debugging
	typefind.Connect("have-type", func(self *gst.Element, probability uint, caps *gst.Caps) {
		fmt.Printf("File type detected: %s (probability: %d)\n", caps.String(), probability)
	})
	
	// Connect to decodebin's pad-added signal
	decodebin.Connect("pad-added", func(self *gst.Element, pad *gst.Pad) {
		caps := pad.CurrentCaps()
		if caps == nil {
			fmt.Printf("Warning: Pad has no caps\n")
			return
		}
		
		structure := caps.GetStructureAt(0)
		if structure == nil {
			fmt.Printf("Warning: Caps has no structure\n")
			return
		}
		
		name := structure.Name()
		fmt.Printf("Decodebin pad added with caps: %s\n", caps.String())
		
		if len(name) >= 5 && name[:5] == "video" {
			// Handle video pad
			padName := fmt.Sprintf("sink_%d", index)
			sinkPad := s.vselector.GetRequestPad(padName)
			if sinkPad == nil {
				fmt.Printf("Failed to get request video pad %s\n", padName)
				return
			}
			
			linkResult := pad.Link(sinkPad)
			if linkResult != gst.PadLinkOK {
				fmt.Printf("Video pad link failed: %v\n", linkResult)
				return
			}
			fmt.Printf("Linked video decode%d to vselector.%s\n", index, padName)
			
		} else if len(name) >= 5 && name[:5] == "audio" {
			// Handle audio pad
			padName := fmt.Sprintf("sink_%d", index)
			sinkPad := s.aselector.GetRequestPad(padName)
			if sinkPad == nil {
				fmt.Printf("Failed to get request audio pad %s\n", padName)
				return
			}
			
			linkResult := pad.Link(sinkPad)
			if linkResult != gst.PadLinkOK {
				fmt.Printf("Audio pad link failed: %v\n", linkResult)
				return
			}
			fmt.Printf("Linked audio decode%d to aselector.%s\n", index, padName)
		} else {
			fmt.Printf("Ignoring pad with caps: %s\n", caps.String())
		}
	})
	
	// Connect to decodebin's no-more-pads signal for debugging
	decodebin.Connect("no-more-pads", func(self *gst.Element) {
		fmt.Printf("Decodebin has no more pads\n")
	})
	
	// Connect to decodebin's autoplug-select signal to help with format selection
	decodebin.Connect("autoplug-select", func(self *gst.Element, pad *gst.Pad, caps *gst.Caps, factory *gst.ElementFactory) int {
		fmt.Printf("Autoplug select for %s: %s\n", factory.GetName(), caps.String())
		return 0 // GST_AUTOPLUG_SELECT_TRY
	})
	
	// Store sources for later reference
	s.sources[index] = append(s.sources[index], filesrc, typefind, decodebin)
	
	return nil
}

// runSchedule manages the timing of the scheduled items
func (s *StreamScheduler) runSchedule() {
	s.mutex.Lock()
	items := make([]StreamItem, len(s.items))
	copy(items, s.items)
	s.mutex.Unlock()

	// Sort items by start time if needed
	
	var lastEndTime int64 = 0  // Track the end time of the last video
	var nextItemIndex = 1      // Index of the next item to prepare

	for i, item := range items {
		// Calculate how long to wait until this item should start
		waitTime := time.Until(item.Start)
		if waitTime > 0 {
			select {
			case <-time.After(waitTime):
				// Time to switch to this source
				s.switchToSource(i)
				
				// Update the current offset for continuous timestamps
				s.currentOffset = lastEndTime
				
			case <-s.stopChan:
				// Scheduler is stopping
				return
			}
		} else {
			// Start time is in the past, switch immediately
			s.switchToSource(i)
			
			// Update the current offset for continuous timestamps
			s.currentOffset = lastEndTime
		}

		// Prepare the next item if available
		if nextItemIndex < len(items) {
			// Calculate when to prepare the next item (e.g., 5 seconds before current item ends)
			prepareTime := item.Duration - 5*time.Second
			if prepareTime < 0 {
				prepareTime = item.Duration / 2  // If item is short, prepare halfway through
			}
			
			go func(nextIdx int) {
				select {
				case <-time.After(prepareTime):
					fmt.Printf("Preparing next source %d\n", nextIdx)
					// Prepare the next source in advance
					s.prepareSource(nextIdx)
				case <-s.stopChan:
					return
				}
			}(nextItemIndex)
			
			nextItemIndex++
		}

		// Wait for the duration of this item
		select {
		case <-time.After(item.Duration):
			// Item duration complete
			lastEndTime += item.Duration.Nanoseconds()  // Update the end time
			continue
		case <-s.stopChan:
			// Scheduler is stopping
			return
		}
	}
}

// prepareSource prepares a source for playback without actually playing it
func (s *StreamScheduler) prepareSource(index int) {
	fmt.Printf("Preparing source %d for playback\n", index)
	
	glib.IdleAdd(func() bool {
		elements, exists := s.sources[index]
		if !exists || len(elements) < 1 {
			return false
		}
		
		// Get the filesrc element
		filesrc := elements[0]
		
		// Make sure it's in the paused state
		filesrc.SetState(gst.StatePaused)
		
		// Seek to beginning
		s.seekSourceToBeginning(index)
		
		fmt.Printf("Source %d is prepared and ready to play\n", index)
		return false
	})
}

// pauseSource pauses the specified source
func (s *StreamScheduler) pauseSource(index int) {
	glib.IdleAdd(func() bool {
		elements, exists := s.sources[index]
		if !exists || len(elements) < 1 {
			return false
		}
		
		// Pause the first element (filesrc) to effectively pause the source
		filesrc := elements[0]
		filesrc.SetState(gst.StatePaused)
		return false
	})
}

// playSource plays the specified source
func (s *StreamScheduler) playSource(index int) {
	glib.IdleAdd(func() bool {
		elements, exists := s.sources[index]
		if !exists || len(elements) < 1 {
			return false
		}
		
		// Play the first element (filesrc) to effectively play the source
		filesrc := elements[0]
		
		// First make sure it's in paused state
		filesrc.SetState(gst.StatePaused)
		
		// Then seek to beginning to ensure we start from the beginning
		s.seekSourceToBeginning(index)
		
		// Finally set to playing
		filesrc.SetState(gst.StatePlaying)
		
		fmt.Printf("Source %d is now playing\n", index)
		return false
	})
}

// seekSourceToBeginning seeks the specified source to the beginning (0s)
func (s *StreamScheduler) seekSourceToBeginning(index int) {
	fmt.Printf("Seeking source %d to beginning\n", index)

	// This needs to be executed in the main loop's context
	glib.IdleAdd(func() bool {
		// Get the decodebin element for this source
		elements, exists := s.sources[index]
		if !exists || len(elements) < 3 {
			fmt.Printf("Error: Source elements for index %d not found\n", index)
			return false
		}
		
		// The decodebin is the third element in our sources array
		decodebin := elements[2]
		
		// Create a seek event
		seekEvent := gst.NewSeekEvent(
			1.0,                                      // rate
			gst.FormatTime,                           // format
			gst.SeekFlagFlush|gst.SeekFlagAccurate,   // flags
			gst.SeekTypeSet,                          // start_type
			0,                                        // start
			gst.SeekTypeNone,                         // stop_type
			-1,								                        // stop
		)
		
		// Send the seek event to the element
		result := decodebin.SendEvent(seekEvent)
		
		if !result {
			fmt.Printf("Failed to seek source %d to beginning\n", index)
		} else {
			fmt.Printf("Successfully sought source %d to beginning\n", index)
		}
		
		return false
	})
}

// switchToSource switches the pipeline to use the specified source
func (s *StreamScheduler) switchToSource(index int) {
	fmt.Printf("Switching to source %d\n", index)

	// This needs to be executed in the main loop's context
	glib.IdleAdd(func() bool {
		// Switch the selectors to the new source first
		sinkVideo := s.vselector.GetStaticPad(fmt.Sprintf("sink_%d", index))
		if sinkVideo != nil {
			s.vselector.SetProperty("active-pad", sinkVideo)
		}

		sinkAudio := s.aselector.GetStaticPad(fmt.Sprintf("sink_%d", index))
		if sinkAudio != nil {
			s.aselector.SetProperty("active-pad", sinkAudio)
		}
		
		// Now play the source we switched to
		elements, exists := s.sources[index]
		if !exists || len(elements) < 1 {
			return false
		}
		
		// Get the filesrc element
		filesrc := elements[0]
		
		// Set to playing state
		filesrc.SetState(gst.StatePlaying)
		
		// Pause all other sources to save resources
		for idx := range s.sources {
			if idx != index {
				s.pauseSource(idx)
			}
		}
		
		fmt.Printf("Source %d is now playing\n", index)
		return false
	})
}

// Stop stops the scheduler and pipeline
func (s *StreamScheduler) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return
	}

	// Signal scheduler to stop
	close(s.stopChan)
	s.running = false

	// Stop the pipeline
	if s.pipeline != nil {
		s.pipeline.SetState(gst.StateNull)
	}

	// Quit the main loop
	if s.mainLoop != nil {
		s.mainLoop.Quit()
	}
}

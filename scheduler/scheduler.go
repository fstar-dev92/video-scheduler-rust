package scheduler

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-gst/go-gst/gst"
)

type StreamItem struct {
	Type     string
	Source   string
	Start    time.Time
	Duration time.Duration
}

type StreamScheduler struct {
	pipeline     *gst.Pipeline
	selector     *gst.Element
	items        []StreamItem
	mu           sync.Mutex
	isRunning    bool
	stopChan     chan struct{}
	outputHost   string
	outputPort   int
	currentPad   *gst.Pad
	nextPad      *gst.Pad
	nextPipeline *gst.Pipeline
}

func NewStreamScheduler(outputHost string, outputPort int) (*StreamScheduler, error) {
	gst.Init(nil)

	pipeline, err := gst.NewPipeline("")
	if err != nil {
		return nil, fmt.Errorf("failed to create pipeline: %v", err)
	}

	// Create input-selector element
	selector, err := gst.NewElement("input-selector")
	if err != nil {
		return nil, fmt.Errorf("failed to create input-selector: %v", err)
	}

	// Set selector properties for smooth switching
	selector.SetProperty("sync-streams", true)
	selector.SetProperty("sync-mode", 1) // 1 = sync-streams mode

	return &StreamScheduler{
		pipeline:   pipeline,
		selector:   selector,
		stopChan:   make(chan struct{}),
		outputHost: outputHost,
		outputPort: outputPort,
	}, nil
}

func (s *StreamScheduler) AddItem(item StreamItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, item)
}

func (s *StreamScheduler) createInputBin(source string, isRTP bool) (*gst.Pipeline, error) {
	var pipelineStr string
	if isRTP {
		pipelineStr = fmt.Sprintf(`
            udpsrc uri=%s caps="application/x-rtp" !
            rtpjitterbuffer !
            rtph264depay !
            h264parse !
            avdec_h264 !
            queue max-size-buffers=4 max-size-time=200000000 max-size-bytes=0 !
            identity silent=false sync=true !
            fakesink name=sink sync=true
        `, source)
	} else {
		// For file source, use a pipeline that matches the gst-launch command
		// but also includes a fakesink for the scheduler to work with
		pipelineStr = fmt.Sprintf(`
            filesrc location=%s !
            decodebin !
            videoconvert !
            queue max-size-buffers=4 max-size-time=200000000 max-size-bytes=0 !
            identity silent=false sync=true !
            fakesink name=sink sync=true
        `, source)
	}

	// Create a pipeline for this input
	pipeline, err := gst.NewPipelineFromString(pipelineStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create input pipeline: %v", err)
	}

	return pipeline, nil
}

func (s *StreamScheduler) setupMainPipeline() error {
	// Create a simple pipeline that matches the gst-launch command:
	// gst-launch-1.0 filesrc location=input.mp4 ! decodebin ! videoconvert ! x264enc ! mpegtsmux ! rtpmp2tpay ! udpsink host=239.1.1.2 port=5000 auto-multicast=true

	// Video encoder
	x264enc, err := gst.NewElement("x264enc")
	if err != nil {
		return fmt.Errorf("failed to create x264enc: %v", err)
	}

	// MPEG-TS muxer
	mpegtsmux, err := gst.NewElement("mpegtsmux")
	if err != nil {
		return fmt.Errorf("failed to create mpegtsmux: %v", err)
	}

	// RTP payloader for MPEG-TS
	rtpmp2tpay, err := gst.NewElement("rtpmp2tpay")
	if err != nil {
		return fmt.Errorf("failed to create rtpmp2tpay: %v", err)
	}

	// UDP sink
	udpsink, err := gst.NewElement("udpsink")
	if err != nil {
		return fmt.Errorf("failed to create udpsink: %v", err)
	}
	udpsink.SetProperty("host", s.outputHost)
	udpsink.SetProperty("port", s.outputPort)
	udpsink.SetProperty("auto-multicast", true)

	// Add elements to main pipeline
	s.pipeline.Add(s.selector)
	s.pipeline.Add(x264enc)
	s.pipeline.Add(mpegtsmux)
	s.pipeline.Add(rtpmp2tpay)
	s.pipeline.Add(udpsink)

	// Link elements
	srcPad := s.selector.GetStaticPad("src")
	if srcPad == nil {
		return fmt.Errorf("failed to get src pad from selector")
	}

	sinkPad := x264enc.GetStaticPad("sink")
	if sinkPad == nil {
		return fmt.Errorf("failed to get sink pad from x264enc")
	}

	// Link the selector to x264enc
	ret := srcPad.Link(sinkPad)
	if ret != 0 {
		return fmt.Errorf("failed to link selector to x264enc: %v", ret)
	}

	// Link the remaining elements
	err = gst.ElementLinkMany(x264enc, mpegtsmux, rtpmp2tpay, udpsink)
	if err != nil {
		return fmt.Errorf("failed to link elements: %v", err)
	}

	// Print pipeline structure for debugging
	fmt.Println("Pipeline structure:")
	fmt.Println("selector -> x264enc -> mpegtsmux -> rtpmp2tpay -> udpsink")
	fmt.Printf("Output: MPEG-TS over RTP to %s:%d\n", s.outputHost, s.outputPort)
	fmt.Println("To play in VLC: Media > Open Network Stream > enter: rtp://@" + s.outputHost + ":" + fmt.Sprintf("%d", s.outputPort))
	fmt.Println("To play with GStreamer: gst-launch-1.0 udpsrc address=" + s.outputHost + " port=" + fmt.Sprintf("%d", s.outputPort) +
		" multicast-group=" + s.outputHost + " ! application/x-rtp,media=video,payload=33,clock-rate=90000,encoding-name=MP2T ! " +
		"rtpmp2tdepay ! tsdemux ! h264parse ! avdec_h264 ! autovideosink")

	return nil
}

func (s *StreamScheduler) prepareNextSource(item *StreamItem) error {
	// Create input pipeline for the next source
	inputPipeline, err := s.createInputBin(item.Source, item.Type == "rtp")
	if err != nil {
		return err
	}

	// Verify that the fakesink element exists
	_, err = inputPipeline.GetElementByName("sink")
	if err != nil {
		return fmt.Errorf("failed to get fakesink element: %v", err)
	}

	// Get request pad from selector
	selectorPad := s.selector.GetRequestPad("sink_%u")
	if selectorPad == nil {
		return fmt.Errorf("failed to get request pad from selector")
	}

	// Store the new pad
	s.nextPad = selectorPad

	// Start the input pipeline
	inputPipeline.SetState(gst.StatePlaying)

	// Store the pipeline in a field so it doesn't get garbage collected
	s.nextPipeline = inputPipeline

	return nil
}

func (s *StreamScheduler) switchToNextSource() {
	if s.nextPad != nil {
		// Switch selector to next pad
		s.selector.SetProperty("active-pad", s.nextPad)

		// Clean up old pad if exists
		if s.currentPad != nil {
			s.selector.ReleaseRequestPad(s.currentPad)
		}

		s.currentPad = s.nextPad
		s.nextPad = nil
	}
}

func (s *StreamScheduler) Start() error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return fmt.Errorf("scheduler is already running")
	}
	s.isRunning = true
	s.mu.Unlock()

	if err := s.setupMainPipeline(); err != nil {
		return err
	}

	// Set pipeline to playing
	s.pipeline.SetState(gst.StatePlaying)

	// Print the RTP URL for debugging
	fmt.Printf("Streaming MPEG-TS over RTP to: rtp://%s:%d\n", s.outputHost, s.outputPort)
	fmt.Println("To play in VLC: Media > Open Network Stream > enter: rtp://@" + s.outputHost + ":" + fmt.Sprintf("%d", s.outputPort))

	go func() {
		var currentItem *StreamItem
		var nextItemPrepared bool

		for {
			select {
			case <-s.stopChan:
				s.pipeline.SetState(gst.StateNull)
				return
			default:
				s.mu.Lock()
				now := time.Now()

				// Find current and next items
				var activeItem, nextItem *StreamItem
				for i := range s.items {
					item := &s.items[i]
					if now.After(item.Start) && now.Before(item.Start.Add(item.Duration)) {
						activeItem = item
						// Find next item
						if i < len(s.items)-1 {
							nextItem = &s.items[i+1]
						}
						break
					}
				}

				// Prepare next source if needed
				if nextItem != nil && !nextItemPrepared {
					timeUntilNext := nextItem.Start.Sub(now)
					if timeUntilNext <= 5*time.Second {
						if err := s.prepareNextSource(nextItem); err != nil {
							fmt.Printf("Error preparing next source: %v\n", err)
						} else {
							nextItemPrepared = true
						}
					}
				}

				// Switch to next source if needed
				if activeItem != currentItem {
					s.switchToNextSource()
					currentItem = activeItem
					nextItemPrepared = false
					if activeItem != nil {
						fmt.Printf("Switched to %s source: %s\n", activeItem.Type, activeItem.Source)
					} else {
						fmt.Println("No active item to switch to")
					}
				}

				s.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	return nil
}

func (s *StreamScheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning {
		close(s.stopChan)
		s.pipeline.SetState(gst.StateNull)
		s.isRunning = false
	}
}

// GenerateSDPFile creates an SDP file that can be used with media players like VLC
func (s *StreamScheduler) GenerateSDPFile(filename string) error {
	sdpContent := fmt.Sprintf(`v=0
o=- 0 0 IN IP4 127.0.0.1
s=MPEG-TS RTP Stream
c=IN IP4 %s
t=0 0
m=video %d RTP/AVP 33
a=rtpmap:33 MP2T/90000
`, s.outputHost, s.outputPort)

	return os.WriteFile(filename, []byte(sdpContent), 0644)
}

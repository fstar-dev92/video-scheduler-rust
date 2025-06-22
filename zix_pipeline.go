package main

import (
	"fmt"

	"github.com/go-gst/go-gst/gst"
)

// GStreamerPipeline represents a GStreamer pipeline for RTP stream processing
type GStreamerPipeline struct {
	pipeline *gst.Pipeline
	demux    *gst.Element
	mux      *gst.Element
}

// NewGStreamerPipeline creates a new GStreamer pipeline for RTP processing
func NewGStreamerPipeline(inputHost string, inputPort int, outputHost string, outputPort int) (*GStreamerPipeline, error) {
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

	// Video processing elements
	videoQueue, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create video queue: %v", err)
	}
	videoQueue.SetProperty("max-size-buffers", 100)
	videoQueue.SetProperty("max-size-time", uint64(500*1000000))     // 500ms
	videoQueue.SetProperty("min-threshold-time", uint64(50*1000000)) // 50ms

	h264parse1, err := gst.NewElement("h264parse")
	if err != nil {
		return nil, fmt.Errorf("failed to create h264parse1: %v", err)
	}

	avdecH264, err := gst.NewElement("avdec_h264")
	if err != nil {
		return nil, fmt.Errorf("failed to create avdec_h264: %v", err)
	}

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
	audioQueue, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create audio queue: %v", err)
	}
	audioQueue.SetProperty("max-size-buffers", 100)
	audioQueue.SetProperty("max-size-time", uint64(500*1000000))     // 500ms
	audioQueue.SetProperty("min-threshold-time", uint64(50*1000000)) // 50ms

	aacparse1, err := gst.NewElement("aacparse")
	if err != nil {
		return nil, fmt.Errorf("failed to create aacparse1: %v", err)
	}

	avdecAac, err := gst.NewElement("avdec_aac")
	if err != nil {
		return nil, fmt.Errorf("failed to create avdec_aac: %v", err)
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

	rtpjitterbuffer.SetProperty("latency", 200)

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
		videoQueue, h264parse1, avdecH264, videoconvert, x264enc, h264parse2,
		audioQueue, aacparse1, avdecAac, audioconvert, audioresample, voaacenc, aacparse2,
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

	// Link video branch
	if err := videoQueue.Link(h264parse1); err != nil {
		return nil, fmt.Errorf("failed to link videoQueue to h264parse1: %v", err)
	}
	fmt.Printf("Linked videoQueue to h264parse1\n")

	if err := h264parse1.Link(avdecH264); err != nil {
		return nil, fmt.Errorf("failed to link h264parse1 to avdec_h264: %v", err)
	}
	fmt.Printf("Linked h264parse1 to avdec_h264\n")

	if err := avdecH264.Link(videoconvert); err != nil {
		return nil, fmt.Errorf("failed to link avdec_h264 to videoconvert: %v", err)
	}
	fmt.Printf("Linked avdec_h264 to videoconvert\n")

	if err := videoconvert.Link(x264enc); err != nil {
		return nil, fmt.Errorf("failed to link videoconvert to x264enc: %v", err)
	}
	fmt.Printf("Linked videoconvert to x264enc\n")

	if err := x264enc.Link(h264parse2); err != nil {
		return nil, fmt.Errorf("failed to link x264enc to h264parse2: %v", err)
	}
	fmt.Printf("Linked x264enc to h264parse2\n")

	if err := h264parse2.Link(mpegtsmux); err != nil {
		return nil, fmt.Errorf("failed to link h264parse2 to mpegtsmux: %v", err)
	}
	fmt.Printf("Linked h264parse2 to mpegtsmux\n")

	// Link audio branch
	if err := audioQueue.Link(aacparse1); err != nil {
		return nil, fmt.Errorf("failed to link audioQueue to aacparse1: %v", err)
	}
	fmt.Printf("Linked audioQueue to aacparse1\n")

	if err := aacparse1.Link(avdecAac); err != nil {
		return nil, fmt.Errorf("failed to link aacparse1 to avdec_aac: %v", err)
	}
	fmt.Printf("Linked aacparse1 to avdec_aac\n")

	if err := avdecAac.Link(audioconvert); err != nil {
		return nil, fmt.Errorf("failed to link avdec_aac to audioconvert: %v", err)
	}
	fmt.Printf("Linked avdec_aac to audioconvert\n")

	if err := audioconvert.Link(audioresample); err != nil {
		return nil, fmt.Errorf("failed to link audioconvert to audioresample: %v", err)
	}
	fmt.Printf("Linked audioconvert to audioresample\n")

	if err := audioresample.Link(voaacenc); err != nil {
		return nil, fmt.Errorf("failed to link audioresample to voaacenc: %v", err)
	}
	fmt.Printf("Linked audioresample to voaacenc\n")

	if err := voaacenc.Link(aacparse2); err != nil {
		return nil, fmt.Errorf("failed to link voaacenc to aacparse2: %v", err)
	}
	fmt.Printf("Linked voaacenc to aacparse2\n")

	if err := aacparse2.Link(audioMuxerQueue); err != nil {
		return nil, fmt.Errorf("failed to link aacparse2 to audioMuxerQueue: %v", err)
	}
	fmt.Printf("Linked aacparse2 to audioMuxerQueue\n")

	// Link audio queue to muxer
	if err := audioMuxerQueue.Link(mpegtsmux); err != nil {
		return nil, fmt.Errorf("failed to link audioMuxerQueue to mpegtsmux: %v", err)
	}
	fmt.Printf("Linked audioMuxerQueue to mpegtsmux\n")

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

		// Check pad name to determine if it's video or audio
		if len(padName) >= 5 && padName[:5] == "video" {
			targetElement = videoQueue
			fmt.Printf("Linking video pad %s to video queue\n", padName)
		} else if len(padName) >= 5 && padName[:5] == "audio" {
			targetElement = audioQueue
			fmt.Printf("Linking audio pad %s to audio queue\n", padName)
		} else {
			fmt.Printf("Unknown demuxer pad: %s\n", padName)
			return
		}

		// Get the sink pad from the target element
		sinkPad := targetElement.GetStaticPad("sink")
		if sinkPad == nil {
			fmt.Printf("Failed to get sink pad from target element\n")
			return
		}

		// Link the demuxer pad to the target element
		if pad.Link(sinkPad) == gst.PadLinkOK {
			fmt.Printf("Successfully linked demuxer pad %s to %s\n", padName, targetElement.GetName())
		} else {
			fmt.Printf("Failed to link demuxer pad %s to %s\n", padName, targetElement.GetName())
		}
	})

	return &GStreamerPipeline{
		pipeline: pipeline,
		demux:    tsdemux,
		mux:      mpegtsmux,
	}, nil
}

// Start starts the GStreamer pipeline
func (gp *GStreamerPipeline) Start() error {
	// Set pipeline state to playing
	if err := gp.pipeline.SetState(gst.StatePlaying); err != nil {
		return fmt.Errorf("failed to set pipeline state to playing: %v", err)
	}

	fmt.Printf("GStreamer pipeline started successfully\n")
	return nil
}

// Stop stops the GStreamer pipeline
func (gp *GStreamerPipeline) Stop() {
	fmt.Println("Stopping GStreamer pipeline...")

	// Set pipeline state to null
	if gp.pipeline != nil {
		gp.pipeline.SetState(gst.StateNull)
	}

	fmt.Println("GStreamer pipeline stopped")
}

// RunGStreamerPipeline runs the GStreamer pipeline with the specified parameters
func RunGStreamerPipeline(inputHost string, inputPort int, outputHost string, outputPort int) error {
	fmt.Printf("Starting GStreamer pipeline: %s:%d -> %s:%d\n", inputHost, inputPort, outputHost, outputPort)

	// Create pipeline
	pipeline, err := NewGStreamerPipeline(inputHost, inputPort, outputHost, outputPort)
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

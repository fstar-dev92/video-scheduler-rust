package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-gst/go-gst/gst"
)

func start() {
	// --- CONFIGURE THESE ---
	multicastGroup := "224.2.3.13"
	port := 10500
	multicastIface := "eno2"
	programNumber := 3 // <-- set your desired program number
	outputFile := "output.ts"
	// -----------------------

	gst.Init(nil)

	pipeline, err := gst.NewPipeline("recorder-pipeline")
	if err != nil {
		log.Fatalf("Failed to create pipeline: %v", err)
	}

	udpsrc, _ := gst.NewElementWithProperties("udpsrc", map[string]interface{}{
		"multicast-group": multicastGroup,
		"port":            port,
		"buffer-size":     40000000,
		"multicast-iface": multicastIface,
	})
	queue2, _ := gst.NewElementWithProperties("queue2", map[string]interface{}{
		"max-size-buffers": 0,
		"max-size-bytes":   0,
		"max-size-time":    10000000000,
	})
	tsdemux, _ := gst.NewElementWithProperties("tsdemux", map[string]interface{}{
		"program-number": programNumber,
	})
	mpegtsmux, _ := gst.NewElement("mpegtsmux")
	filesink, _ := gst.NewElementWithProperties("filesink", map[string]interface{}{
		"location": outputFile,
	})

	// Add static elements to pipeline
	pipeline.AddMany(udpsrc, queue2, tsdemux, mpegtsmux, filesink)

	// Link udpsrc -> queue2 -> tsdemux
	if err := udpsrc.Link(queue2); err != nil {
		log.Fatalf("Failed to link udpsrc to queue2: %v", err)
	}
	if err := queue2.Link(tsdemux); err != nil {
		log.Fatalf("Failed to link queue2 to tsdemux: %v", err)
	}

	// Link mpegtsmux -> filesink
	if err := mpegtsmux.Link(filesink); err != nil {
		log.Fatalf("Failed to link mpegtsmux to filesink: %v", err)
	}

	tsdemux.Connect("pad-added", func(self *gst.Element, pad *gst.Pad) {
		padName := pad.GetName()
		caps := pad.GetCurrentCaps().String()
		fmt.Println("Pad name:", padName, "Caps:", caps)
		if len(padName) >= 5 && padName[:5] == "video" {
			var parser *gst.Element
			var queue *gst.Element
			queue, _ = gst.NewElement("queue")
			if strings.Contains(caps, "video/x-h264") {
				parser, _ = gst.NewElement("h264parse")
			} else if strings.Contains(caps, "video/mpeg") {
				parser, _ = gst.NewElement("mpegvideoparse")
			} else if strings.Contains(caps, "video/x-h265") {
				parser, _ = gst.NewElement("h265parse")
			} else {
				log.Println("Unsupported video caps:", caps)
				return
			}
			pipeline.AddMany(queue, parser)
			queue.SyncStateWithParent()
			parser.SyncStateWithParent()
			if pad.Link(queue.GetStaticPad("sink")) != gst.PadLinkOK {
				log.Println("Failed to link tsdemux video pad to queue")
				return
			}
			if err := queue.Link(parser); err != nil {
				log.Println("Failed to link queue to parser:", err)
			}
			if err := parser.Link(mpegtsmux); err != nil {
				log.Println("Failed to link parser to mpegtsmux:", err)
			}
		} else if len(padName) >= 5 && padName[:5] == "audio" {
			var parser *gst.Element
			var queue *gst.Element
			queue, _ = gst.NewElement("queue")
			if strings.Contains(caps, "audio/mpeg") {
				parser, _ = gst.NewElement("aacparse")
			} else if strings.Contains(caps, "audio/x-ac3") {
				parser, _ = gst.NewElement("ac3parse")
			} else if strings.Contains(caps, "audio/x-eac3") {
				parser, _ = gst.NewElement("eac3parse")
			} else if strings.Contains(caps, "audio/x-opus") {
				parser, _ = gst.NewElement("opusparse")
			} else {
				log.Println("Unsupported audio caps:", caps)
				return
			}
			pipeline.AddMany(queue, parser)
			queue.SyncStateWithParent()
			parser.SyncStateWithParent()
			if pad.Link(queue.GetStaticPad("sink")) != gst.PadLinkOK {
				log.Println("Failed to link tsdemux audio pad to queue")
				return
			}
			if err := queue.Link(parser); err != nil {
				log.Println("Failed to link queue to parser:", err)
				return
			}
			// Request a new sink pad from mpegtsmux for this audio stream
			muxSinkPad := mpegtsmux.GetRequestPad("sink_%d")
			if muxSinkPad == nil {
				log.Println("Failed to get request pad from mpegtsmux for audio")
				return
			}
			parserSrcPad := parser.GetStaticPad("src")
			if parserSrcPad.Link(muxSinkPad) != gst.PadLinkOK {
				log.Println("Failed to link parser to mpegtsmux sink pad")
			}
		}
	})

	if err := pipeline.SetState(gst.StatePlaying); err != nil {
		log.Fatalf("Failed to set pipeline to PLAYING: %v", err)
	}
	fmt.Println("Recording to TS... Press Ctrl+C to stop.")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("Stopping pipeline...")
	pipeline.SetState(gst.StateNull)
}

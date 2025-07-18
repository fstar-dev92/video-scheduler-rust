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
	// Video branch
	videoQueue, _ := gst.NewElement("queue")
	h264parse, _ := gst.NewElement("h264parse")
	mpegvideoparse, _ := gst.NewElement("mpegvideoparse")

	// Audio branch
	audioQueue, _ := gst.NewElement("queue")
	ac3parse, _ := gst.NewElement("ac3parse")
	// Muxer and sink
	mpegtsmux, _ := gst.NewElement("mpegtsmux")
	filesink, _ := gst.NewElementWithProperties("filesink", map[string]interface{}{
		"location": outputFile,
	})

	// Add all elements to pipeline
	pipeline.AddMany(udpsrc, queue2, tsdemux, videoQueue, h264parse, mpegvideoparse, audioQueue, ac3parse, mpegtsmux, filesink)

	// Link udpsrc -> queue2 -> tsdemux
	if err := udpsrc.Link(queue2); err != nil {
		log.Fatalf("Failed to link udpsrc to queue2: %v", err)
	}
	if err := queue2.Link(tsdemux); err != nil {
		log.Fatalf("Failed to link queue2 to tsdemux: %v", err)
	}

	// Handle dynamic pads from tsdemux
	tsdemux.Connect("pad-added", func(self *gst.Element, pad *gst.Pad) {
		padName := pad.GetName()
		caps := pad.GetCurrentCaps().String()
		fmt.Println("Pad name:", padName, "Caps:", caps)
		if len(padName) >= 5 && padName[:5] == "video" {
			if strings.Contains(caps, "video/x-h264") {
				parserSinkPad := h264parse.GetStaticPad("sink")
				if !parserSinkPad.IsLinked() {
					if pad.Link(parserSinkPad) != gst.PadLinkOK {
						log.Println("Failed to link tsdemux video pad to h264parse")
					}
					if err := h264parse.Link(mpegtsmux); err != nil {
						log.Println("Failed to link h264parse to mpegtsmux:", err)
					}
				}
			} else if strings.Contains(caps, "video/mpeg") {
				parserSinkPad := mpegvideoparse.GetStaticPad("sink")
				if !parserSinkPad.IsLinked() {
					if pad.Link(parserSinkPad) != gst.PadLinkOK {
						log.Println("Failed to link tsdemux video pad to mpegvideoparse")
					}
					if err := mpegvideoparse.Link(mpegtsmux); err != nil {
						log.Println("Failed to link mpegvideoparse to mpegtsmux:", err)
					}
				}
			} else {
				log.Println("Unsupported video caps:", caps)
			}
		} else if len(padName) >= 5 && padName[:5] == "audio" {
			audioSinkPad := audioQueue.GetStaticPad("sink")
			if !audioSinkPad.IsLinked() {
				if pad.Link(audioSinkPad) != gst.PadLinkOK {
					log.Println("Failed to link tsdemux audio pad to audio queue")
				}
			}
		}
	})

	// Link audio branch: audioQueue -> ac3parse -> mpegtsmux
	if err := audioQueue.Link(ac3parse); err != nil {
		log.Fatalf("Failed to link audioQueue to ac3parse: %v", err)
	}
	if err := ac3parse.Link(mpegtsmux); err != nil {
		log.Fatalf("Failed to link ac3parse to mpegtsmux: %v", err)
	}

	// Link mpegtsmux -> filesink
	if err := mpegtsmux.Link(filesink); err != nil {
		log.Fatalf("Failed to link mpegtsmux to filesink: %v", err)
	}

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

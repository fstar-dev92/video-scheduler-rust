package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-gst/go-gst/gst"

	"scheduler-rtp/scheduler"
)

func main() {
	// Parse command line flags
	var verbose bool
	flag.BoolVar(&verbose, "v", false, "Enable verbose logging")
	flag.Parse()

	// Initialize GStreamer
	gst.Init(nil)

	if len(os.Args) > 1 && os.Args[1] == "scte35" {
		// Check for help flag
		if len(os.Args) > 2 && (os.Args[2] == "-h" || os.Args[2] == "--help") {
			fmt.Println("SCTE-35 Handler Usage:")
			fmt.Println("  go run . scte35 [-v]")
			fmt.Println("")
			fmt.Println("Options:")
			fmt.Println("  -v    Enable verbose logging and GStreamer debug output (like gst-launch-1.0 -v)")
			fmt.Println("  -h    Show this help message")
			fmt.Println("")
			fmt.Println("Examples:")
			fmt.Println("  go run . scte35          # Run with normal logging")
			fmt.Println("  go run . scte35 -v       # Run with GStreamer debug output")
			return
		}
		// Run SCTE-35 handler mode
		runSCTE35Example(verbose)
		return
	}
	// Create a new stream scheduler with output to localhost:5000
	streamScheduler, err := scheduler.NewStreamScheduler("239.1.1.5", 5000)
	if err != nil {
		log.Fatalf("Failed to create scheduler: %v", err)
	}

	// Get current time to schedule items relative to now
	now := time.Now()

	// Add test file items
	// We'll schedule a test pattern for 10 seconds, then a file for 10 seconds, then back to test pattern
	streamScheduler.AddItem(scheduler.StreamItem{
		Type:     "file",
		Source:   "/home/ubuntu/Dev/video-scheduler-gstreamer/videos/output.mp4", // First video file
		Start:    now,
		Duration: 30 * time.Second,
	})

	streamScheduler.AddItem(scheduler.StreamItem{
		Type:     "file",
		Source:   "/home/ubuntu/Dev/video-scheduler-gstreamer/videos/output2.mp4", // Second video file
		Start:    now.Add(10 * time.Second),
		Duration: 60 * time.Second,
	})

	// Start the scheduler
	if err := streamScheduler.RunSchedule(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}

	// Direct UDP URL for VLC
	fmt.Printf("To play in VLC: Open VLC and go to Media > Open Network Stream > enter udp://@%s:%d\n",
		"239.1.1.5", 5000)

	fmt.Println("Scheduler1 started. Press Ctrl+C to exit.")

	// Create a new stream scheduler with output to localhost:5000
	streamScheduler1, err := scheduler.NewStreamScheduler("239.1.1.8", 5001)
	if err != nil {
		log.Fatalf("Failed to create scheduler1: %v", err)
	}

	// Add test file items
	// We'll schedule a test pattern for 10 seconds, then a file for 10 seconds, then back to test pattern
	streamScheduler1.AddItem(scheduler.StreamItem{
		Type:     "file",
		Source:   "/home/ubuntu/Dev/video-scheduler-gstreamer/videos/output3.mp4", // First video file
		Start:    now,
		Duration: 30 * time.Second,
	})

	streamScheduler1.AddItem(scheduler.StreamItem{
		Type:     "file",
		Source:   "/home/ubuntu/Dev/video-scheduler-gstreamer/videos/New_Filler.mp4", // Second video file
		Start:    now.Add(10 * time.Second),
		Duration: 60 * time.Second,
	})

	// Start the scheduler
	if err := streamScheduler1.RunSchedule(); err != nil {
		log.Fatalf("Failed to start scheduler1: %v", err)
	}

	// Direct UDP URL for VLC
	fmt.Printf("To play in VLC: Open VLC and go to Media > Open Network Stream > enter udp://@%s:%d\n",
		"239.1.1.8", 5001)

	fmt.Println("Scheduler started. Press Ctrl+C to exit.")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Stop the scheduler
	streamScheduler.Stop()
	fmt.Println("Scheduler stopped.")
}

func runSCTE35Example(verbose bool) {
	fmt.Println("Starting SCTE-35 Handler Example...")
	if verbose {
		fmt.Println("Verbose logging enabled")
	}

	// Create handler with example parameters
	handler, err := NewAdInsertionHandler(
		"239.1.1.1", // input host
		5002,        // input port
		"239.1.1.5", // output host
		5006,        // output port
		"/home/fstar/work/video-scheduler-gstreamer/videos/input.mp4", // ad source file
		verbose, // verbose flag
	)
	if err != nil {
		fmt.Printf("Failed to create handler: %v\n", err)
		return
	}

	// Set ad duration
	handler.SetAdDuration(10 * time.Second)

	// Start the handler
	err = handler.Start()
	if err != nil {
		fmt.Printf("Failed to start handler: %v\n", err)
		return
	}

	fmt.Println("SCTE-35 Handler started successfully!")
	fmt.Println("Waiting for SCTE-35 messages...")

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Simulate SCTE-35 detection after 5 seconds
	go func() {
		time.Sleep(5 * time.Second)
		fmt.Println("Simulating SCTE-35 message detection...")
		handler.insertAd()
	}()

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("\nShutting down SCTE-35 Handler...")
	handler.Stop()
	fmt.Println("SCTE-35 Handler stopped.")
}

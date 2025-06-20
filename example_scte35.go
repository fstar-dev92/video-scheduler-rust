package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func runSCTE35Example() {
	// Example configuration
	inputHost := "239.1.1.5"           // Input multicast address
	inputPort := 5000                  // Input port for main stream
	outputHost := "239.1.1.6"          // Output multicast address
	outputPort := 5002                 // Output port for processed stream
	adSource := "/path/to/your/ad.mp4" // Path to your ad file

	// Create SCTE-35 handler
	handler, err := NewAdInsertionHandler(inputHost, inputPort, outputHost, outputPort, adSource)
	if err != nil {
		log.Fatalf("Failed to create SCTE-35 handler: %v", err)
	}

	// Configure ad duration (optional)
	handler.SetAdDuration(30 * time.Second)

	// Start the handler
	if err := handler.Start(); err != nil {
		log.Fatalf("Failed to start SCTE-35 handler: %v", err)
	}

	fmt.Printf("SCTE-35 Handler started\n")
	fmt.Printf("Input stream: udp://%s:%d\n", inputHost, inputPort)
	fmt.Printf("Output stream: udp://%s:%d\n", outputHost, outputPort)
	fmt.Printf("SCTE-35 messages: udp://%s:%d\n", inputHost, inputPort+1000)
	fmt.Printf("Ad source: %s\n", adSource)
	fmt.Printf("Ad duration: %v\n", 30*time.Second)
	fmt.Println("Press Ctrl+C to exit")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Stop the handler
	handler.Stop()
	fmt.Println("SCTE-35 Handler stopped")
}

// Example of how to send a test SCTE-35 message
func sendTestSCTE35Message(host string, port int) {
	// This is a simplified example of sending a test SCTE-35 message
	// In a real implementation, you would send proper SCTE-35 packets

	fmt.Printf("To test ad insertion, you can send SCTE-35 messages to %s:%d\n", host, port+1000)
	fmt.Println("Example using netcat:")
	fmt.Printf("echo -n '\\xFC\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x05' | nc -u %s %d\n", host, port+1000)
}

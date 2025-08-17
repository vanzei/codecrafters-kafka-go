package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	fmt.Println("Connecting to Kafka server...")

	// Connect to the Kafka server
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected successfully!")

	// Send a simple test message
	testMessage := []byte("Hello Kafka!")
	_, err = conn.Write(testMessage)
	if err != nil {
		fmt.Printf("Failed to write: %v\n", err)
		return
	}

	fmt.Printf("Sent message: %s\n", testMessage)

	// Read response
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Printf("Failed to read response: %v\n", err)
		return
	}

	fmt.Printf("Received response (%d bytes): %v\n", n, buf[:n])
}


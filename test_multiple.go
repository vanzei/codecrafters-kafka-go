package main

import (
	"fmt"
	"net"
	"sync"
	"time"
)

func main() {
	fmt.Println("Testing multiple concurrent connections...")

	var wg sync.WaitGroup
	numConnections := 5

	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", "localhost:9092")
			if err != nil {
				fmt.Printf("Connection %d failed: %v\n", id, err)
				return
			}
			defer conn.Close()

			message := fmt.Sprintf("Message from client %d", id)
			_, err = conn.Write([]byte(message))
			if err != nil {
				fmt.Printf("Client %d write failed: %v\n", id, err)
				return
			}

			buf := make([]byte, 1024)
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			n, err := conn.Read(buf)
			if err != nil {
				fmt.Printf("Client %d read failed: %v\n", id, err)
				return
			}

			fmt.Printf("Client %d received: %v\n", id, buf[:n])
		}(i)
	}

	wg.Wait()
	fmt.Println("All concurrent tests completed!")
}


package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}

type Header struct {
	Size          int32
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
}

const (
	CORRELATION_ID_LENGHT    = 4
	API_VERSION_LENGHT       = 2
	REQUEST_API_KEY_SIZE     = 2
	REQUEST_API_VERSION_SIZE = 2
	MESSAGE_SIZE             = 4
)

func handleConnection(conn net.Conn) {

	defer conn.Close()
	for {
		buf := make([]byte, 1024)

		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			break
		}

		response := make([]byte, 10)
		var header Header
		rdr := bytes.NewReader(buf[:n])
		if err := binary.Read(rdr, binary.BigEndian, &header); err != nil {
			fmt.Println("Failed to parse header:", err)
			continue
		}
		fmt.Println(buf[:n])

		// Build base response: placeholder + correlation id
		binary.BigEndian.PutUint32(response[0:4], uint32(0))
		binary.BigEndian.PutUint32(response[4:8], uint32(header.CorrelationID))
		// Always set error_code = 35 for this stage
		binary.BigEndian.PutUint16(response[8:10], uint16(35))
		fmt.Println(response)

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("Error writing to connection", err)
			return
		}

	}
}

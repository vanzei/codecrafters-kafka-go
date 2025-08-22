package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
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

func handleConnection(conn net.Conn) {

	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(ReadTimeout))
	conn.SetWriteDeadline(time.Now().Add(WriteTimeout))

	fmt.Println("New connection from %s: %v]\n", conn.RemoteAddr())

	request, err := readRequest(conn)
	if err != nil {
		fmt.Println("Error reading Resuqe from %s: %v\n", conn.RemoteAddr(), err)
	}

	err = handleKafkaRequest(conn, request)
	if err != nil {
		fmt.Println("Error handling request from %s: %v\n", conn.RemoteAddr(), err)
	}

}

func handleKafkaRequest(conn net.Conn, request *KafkaRequest) error {
	switch request.RequestAPIKey {
	case ApiVersionAPIKEY:
		return handleApiVersionsRequest(conn, request)
	default:
		return fmt.Errorf("Unsupported API Key: %d", RequestAPIKeyLenght)
	}
}

func handleApiVersionsRequest(conn net.Conn, request *KafkaRequest) error {
	minVersion := int16(0)
	maxVersion := int16(4)

	response := &ApiVersionV4Response{
		CorrelationID:  request.CorrelationID,
		ErrorCode:      NoneCode,
		ApiKeys:        []int16{ApiVersionAPIKEY, minVersion, maxVersion},
		MinVersion:     minVersion,
		MaxVersion:     maxVersion,
		TaggedFields:   byte(0),
		ThrottleTimeMs: 0,
	}

	if request.RequestAPIVersion < 0 || request.RequestAPIVersion > 4 {
		response.ErrorCode = UnsupportedVersionCode
	}
	return writeApiVersionsResponse(conn, response)
}
func writeApiVersionsResponse(conn net.Conn, response *ApiVersionV4Response) error {
	fmt.Printf("Writing ApiVersions response: %+v\n", response)
	// Serialize the response

	buff := make([]byte, 0)

	// Write the correlation ID
	corrId := make([]byte, 4)
	binary.BigEndian.PutUint32(corrId, uint32(response.CorrelationID))
	buff = append(buff, corrId...)

	// Write the error code
	errorCode := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCode, uint16(response.ErrorCode))
	buff = append(buff, errorCode...)

	// ApiKeys array (length + each key/version pair)
	apiKeysLen := make([]byte, 4)
	binary.BigEndian.PutUint32(apiKeysLen, uint32(len(response.ApiKeys)))
	buff = append(buff, apiKeysLen...)
	for _, apiKey := range response.ApiKeys {
		key := make([]byte, 2)
		binary.BigEndian.PutUint16(key, uint16(apiKey))
		buff = append(buff, key...)
		// Add min/max version for each key if needed
	}

	// TaggedFields
	buff = append(buff, response.TaggedFields)

	// ThrottleTimeMs (if needed)
	throttle := make([]byte, 4)
	binary.BigEndian.PutUint32(throttle, uint32(response.ThrottleTimeMs))
	buff = append(buff, throttle...)

	// Prepend the total length
	totalLen := make([]byte, 4)
	binary.BigEndian.PutUint32(totalLen, uint32(len(buff)))
	buff = append(totalLen, buff...)

	// Write to connection
	_, err := conn.Write(buff)
	return err
}

func readRequest(conn net.Conn) (*KafkaRequest, error) {
	// Read the request from the connection
	lenBuff := make([]byte, 4)
	if _, err := conn.Read(lenBuff); err != nil {
		return nil, fmt.Errorf("failed to read message size: %w", err)
	}

	// calculate message size
	lenght := int(binary.BigEndian.Uint32(lenBuff))

	msgBuff := make([]byte, lenght)
	if _, err := conn.Read(msgBuff); err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	// Parse the request
	offset := 0
	apiKey := int16(binary.BigEndian.Uint16(msgBuff[offset : offset+2]))
	offset += 2
	apiVersion := int16(binary.BigEndian.Uint16(msgBuff[offset : offset+2]))
	offset += 2
	correlationID := int32(binary.BigEndian.Uint32(msgBuff[offset : offset+4]))
	offset += 4
	clientIdLength := int16(binary.BigEndian.Uint16(msgBuff[offset : offset+2]))
	offset += 2
	clientID := string(msgBuff[offset : offset+int(clientIdLength)])

	if clientIdLength > 0 {
		clientID = string(msgBuff[offset : offset+int(clientIdLength)])
		offset += int(clientIdLength)
	}

	return &KafkaRequest{
		RequestAPIKey:     apiKey,
		RequestAPIVersion: apiVersion,
		CorrelationID:     correlationID,
		ClientID:          clientID,
	}, nil

	// Currently not part of the solution
	// // Empty tagged fields for now
	// taggedFields := byte(0)
	// offset += 1

	// bodyClientIdLength := int(msgBuff[offset])
	// offset += 1

	// bodyContent := string(msgBuff[offset : offset+int(bodyClientIdLength)])
	// offset += bodyClientIdLength

	// clientSWVersion := int(msgBuff[offset])
	// offset += 1

	// clientSWVersionString := string(msgBuff[offset:lenght])

}

package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var (
	_ = net.Listen
	_ = os.Exit
)

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

	fmt.Printf("New connection from %s]\n", conn.RemoteAddr())
	for {
		request, err := readRequest(conn)
		if err != nil {
			fmt.Printf("Error reading Resuqe from %s: %v\n", conn.RemoteAddr(), err)
			break
		}

		err = handleKafkaRequest(conn, request)
		if err != nil {
			fmt.Printf("Error handling request from %s: %v\n", conn.RemoteAddr(), err)
			break
		}
	}
}

func handleKafkaRequest(conn net.Conn, request *KafkaRequest) error {
	fmt.Println("Handling request: ", request)
	switch request.RequestAPIKey {
	case ApiVersionAPIKEY:
		return handleApiVersionsRequest(conn, request)
	case DescribeTopicPartitionsAPIKEY:
		return handleDescribeTopicPartitionRequest(conn, request)
	default:
		return fmt.Errorf("unsupported API Key: %d", RequestAPIKeyLenght)
	}
}

func handleApiVersionsRequest(conn net.Conn, request *KafkaRequest) error {
	// List all supported API keys
	apiKeys := []ApiKeyVersion{
		{ApiKey: 18, MinVersion: 0, MaxVersion: 4}, // ApiVersions
		{ApiKey: 75, MinVersion: 0, MaxVersion: 0},
	}

	response := &ApiVersionV4Response{
		CorrelationID:         request.CorrelationID,
		ErrorCode:             NoneCode,
		ApiVersionArrayLength: byte(len(apiKeys)),
		ApiKeys:               apiKeys,
		TaggedFields:          byte(0),
		ThrottleTimeMs:        0,
	}

	if request.RequestAPIVersion < 0 || request.RequestAPIVersion > 4 {
		response.ErrorCode = UnsupportedVersionCode
	}
	return writeAPIVersionsResponse(conn, response)
}

func handleDescribeTopicPartitionRequest(conn net.Conn, request *KafkaRequest) error {
	fmt.Println("Handling DescribeTopicPartition request", request)
	topicsParsed, err := parseDescribeTopicPartitionsRequest(request.RawBody)
	if err != nil {
		return err
	}

	fmt.Printf("Parsed topic name: %s\n", topicsParsed)

	topics := []DescribeTopicPartitionTopic{}
	for _, topicName := range topicsParsed {
		topics = append(topics, DescribeTopicPartitionTopic{
			TopicName:    topicName,
			TopicID:      [16]byte{},
			ErrorCode:    3,
			Partitions:   []DescribeTopicPartitionPartition{},
			TaggedFields: 0,
		})
	}

	response := &DescribeTopicPartitionResponse{
		CorrelationID: request.CorrelationID,
		Topics:        topics,
		TaggedFields:  0,
	}

	return writeDescribeTopicPartitionResponse(conn, response)
}

func parseDescribeTopicPartitionsRequest(body []byte) ([]string, error) {
	offset := 1
	topics := []string{}
	// topics array length (compact/VARINT)
	topicsLen := int(body[offset])
	offset += 1

	numTopic := topicsLen - 1
	if numTopic < 1 {
		return []string{}, fmt.Errorf("no topics in request")
	}
	// topic name (COMPACT_STRING)
	for i := 0; i < numTopic; i++ {
		nameLen := int(body[offset])
		offset += 1
		topicName := string(body[offset : offset+nameLen])
		offset += nameLen
		offset += 1
		topics = append(topics, topicName)

	}

	offset += 1 // TAG_BUFFER

	partitionLimit := int(binary.BigEndian.Uint32(body[offset : offset+4]))
	offset += 4
	fmt.Printf("Partition limit: %d\n", partitionLimit)

	return topics, nil
}

func writeAPIVersionsResponse(conn net.Conn, response *ApiVersionV4Response) error {
	fmt.Printf("Writing ApiVersions response:\n %+v\n", response)
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

	// ApiKeys array (compact array: varint length, then each key/min/max)
	// For one key, varint is just 1
	buff = append(buff, byte(len(response.ApiKeys)+1)) // compact array length (VARINT, 1 key)
	for _, apiKey := range response.ApiKeys {
		key := make([]byte, 2)
		binary.BigEndian.PutUint16(key, uint16(apiKey.ApiKey))
		buff = append(buff, key...)

		minV := make([]byte, 2)
		binary.BigEndian.PutUint16(minV, uint16(apiKey.MinVersion))
		buff = append(buff, minV...)

		maxV := make([]byte, 2)
		binary.BigEndian.PutUint16(maxV, uint16(apiKey.MaxVersion))
		buff = append(buff, maxV...)

		buff = append(buff, 0) // TAG_BUFFER

		fmt.Println(buff)
	}

	// TaggedFields (VARINT, usually 0)
	buff = append(buff, 0)

	// ThrottleTimeMs
	throttle := make([]byte, 4)
	binary.BigEndian.PutUint32(throttle, uint32(response.ThrottleTimeMs))
	buff = append(buff, throttle...)

	// Calculate the size before prepending
	totalLen := make([]byte, 4)
	binary.BigEndian.PutUint32(totalLen, uint32(len(buff)))
	finalBuff := append(totalLen, buff...)

	fmt.Printf("Writing response of length %d: %v\n", len(buff), buff)
	// Write to connection
	_, err := conn.Write(finalBuff)
	return err
}

func writeDescribeTopicPartitionResponse(conn net.Conn, response *DescribeTopicPartitionResponse) error {
	// Response format
	// MessageSize 4bytes
	// ResposeHeader [ CorrelationID + tag buffer] 4 + 1
	// Describe part
	// Throttle Time 4 bytes
	// Topics Array [Array Len + Topics ] Array len N+1
	// Topics -> Error Code + TopicName -> [Len + Content]  + TopicId 16 bytes 0 + isInternal 0 +
	// Partitions array 1 byte
	// Authorized Operations 0
	// TopicTag 1 byte 0
	// Next Cursor Null 1byte value 15
	// Response tag buffer

	fmt.Printf("Writing ApiVersions response:\n %+v\n", response)
	// Serialize the response

	buff := make([]byte, 0)

	// Write the correlation ID
	corrId := make([]byte, 4)
	binary.BigEndian.PutUint32(corrId, uint32(response.CorrelationID))
	buff = append(buff, corrId...)

	//fmt.Println("Value after correlationID")
	//fmt.Println(buff)

	buff = append(buff, 0) // tag buffer
	throttleTime := make([]byte, 4)
	binary.BigEndian.PutUint32(throttleTime, uint32(0))
	buff = append(buff, throttleTime...)

	//fmt.Println("Value after throttleTime")
	//fmt.Println(buff)

	buff = append(buff, byte(len(response.Topics)+1))

	//fmt.Println("Value after topics array length")
	//fmt.Println(buff)
	for _, topic := range response.Topics {
		errorCode := make([]byte, 2)
		binary.BigEndian.PutUint16(errorCode, uint16(3))
		buff = append(buff, errorCode...)

		//fmt.Println("Value after topic error code")
		//fmt.Println(buff)

		buff = append(buff, byte(len(topic.TopicName)+1))
		buff = append(buff, []byte(topic.TopicName)...)
		buff = append(buff, 0) // tag buffer after topic name

		//fmt.Println("Value after topic name length")
		//fmt.Println(buff)

		buff = append(buff, topic.TopicID[:]...)

		//fmt.Println("Value after topic ID")
		//fmt.Println(buff)

		buff = append(buff, 0) // isInternal

		//fmt.Println("Value after isInternal")
		//fmt.Println(buff)

		buff = append(buff, 1) // partitionArray

		//fmt.Println("Value after partition array length")
		//fmt.Println(buff)

		authorizedOperation := make([]byte, 4)
		binary.BigEndian.PutUint32(authorizedOperation, uint32(0))
		buff = append(buff, authorizedOperation...)

		//fmt.Print("Value after authorized operation")
		//fmt.Println(buff)

		buff = append(buff, 0) // tagbuffer

		//fmt.Println("Value after topic tag buffer")
		//fmt.Println(buff)
	}

	// Write next_cursor as a valid empty COMPACT_STRING
	buff = append(buff, 1) // length = 1 (VARINT)
	buff = append(buff, 0) // tag buffer

	// Write partition_index (INT32, 4 bytes, usually 0)
	partitionIndex := make([]byte, 4)
	binary.BigEndian.PutUint32(partitionIndex, 0)
	buff = append(buff, partitionIndex...)

	// Write tag buffer after partition_index
	buff = append(buff, 0)

	// Calculate the size before prepending
	totalLen := make([]byte, 4)
	binary.BigEndian.PutUint32(totalLen, uint32(len(buff)))
	finalBuff := append(totalLen, buff...)

	fmt.Println("Final buffer: ", finalBuff)
	_, err := conn.Write(finalBuff)
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

	clientID := ""
	if clientIdLength > 0 {
		clientID = string(msgBuff[offset : offset+int(clientIdLength)])
		offset += int(clientIdLength)
	}

	body := msgBuff[offset:]
	//fmt.Printf("Parsed request: API Key: %d, Version: %d, Correlation ID: %d, Client ID: %s\n", apiKey, apiVersion, correlationID, clientID)
	return &KafkaRequest{
		RequestAPIKey:     apiKey,
		RequestAPIVersion: apiVersion,
		CorrelationID:     correlationID,
		ClientID:          clientID,
		RawBody:           body,
	}, nil
}

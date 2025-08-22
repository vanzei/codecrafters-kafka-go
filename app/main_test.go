package main

import (
	"bytes"
	"net"
	"testing"
)

func TestApiVersionsResponseSerialization(t *testing.T) {
	testData := []byte{
		// .MessageSize
		0x00, 0x00, 0x00, 0x23, // MessageSize
		// .RequestHeader
		0x00, 0x12, // APIKey (18)
		0x00, 0x04, // APIVersion (4)
		0x00, 0x00, 0x00, 0x07, // CorrelationId (7)
		0x00, 0x09, // ClientId - Length (9)
		0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, // ClientId - Content "kafka-cli"
		0x00, // TagBuffer
		// .RequestBody
		0x09,                                                 // ClientId - Length (9)
		0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, // ClientId - Content "kafka-cli"
		0x03,             // ClientSoftwareVersion - Length (3)
		0x30, 0x2e, 0x31, // ClientSoftwareVersion - contents "0.1"
		0x00, // TagBuffer
	}

	// Parse request
	// Simulate sending testData over a net.Conn using a pipe
	pr, pw := net.Pipe()
	go func() {
		pw.Write(testData)
		pw.Close()
	}()
	req, err := readRequest(pr)
	if err != nil {
		t.Fatalf("Failed to parse request: %v", err)
	}

	// Build response
	resp := &ApiVersionV4Response{
		CorrelationID:  req.CorrelationID,
		ErrorCode:      0,
		ApiKeys:        []ApiKeyVersion{{ApiKey: 18, MinVersion: 0, MaxVersion: 4}},
		TaggedFields:   0,
		ThrottleTimeMs: 0,
	}

	// Serialize response
	pw2, pr2 := net.Pipe()
	go func() {
		writeApiVersionsResponse(pw2, resp)
		pw2.Close()
	}()
	actualBytes := make([]byte, 1024)
	n, _ := pr2.Read(actualBytes)
	actualBytes = actualBytes[:n]

	// Expected bytes (from your test)
	expectedBytes := []byte{
		// .MessageSize
		0x00, 0x00, 0x00, 0x13, // MessageSize (19)
		// .ResponseHeader (v0)
		0x00, 0x00, 0x00, 0x07, // CorrelationId (7)
		// .ResponseBody
		0x00, 0x00, // ErrorCode (0)
		// .ApiVersionsArray
		0x02, // Length (length of 1)
		// .ApiVersion
		0x00, 0x12, // APIKey
		0x00, 0x00, // MinSupportedVersion
		0x00, 0x04, // MaxSupportedVersion
		0x00, // TagBuffer

		0x00, 0x00, 0x00, 0x00, // .ThrottleTime
		0x00, // TagBuffer
	}

	if !bytes.Equal(actualBytes, expectedBytes) {
		t.Errorf("Response bytes do not match.\nExpected: %v\nActual:   %v", expectedBytes, actualBytes)
	}
}

package main

type KafkaRequest struct {
	MessageSize       int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientID          string
}

type ApiVersionV4Response struct {
	CorrelationID         int32
	ErrorCode             int16
	ApiVersionArrayLength byte
	ApiKeys               []ApiKeyVersion
	TaggedFields          byte
	ThrottleTimeMs        int32
}

type ApiKeyVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type DescribeTopic struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type DescribeTopicResponse struct {
	CorrelationID int32
	MinVersion    int16
	MaxVersion    int16
	// Add other fields as required by the protocol
}

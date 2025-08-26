package main

type KafkaRequest struct {
	MessageSize       int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientID          string
	RawBody           []byte
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

type DescribeTopicPartitionResponse struct {
	CorrelationID int32
	TopicName     string
}

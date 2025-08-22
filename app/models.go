package main

type KafkaRequest struct {
	MessageSize       int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientID          string
}

type ApiVersionV4Response struct {
	CorrelationID  int32
	ErrorCode      int16
	ApiKeys        []int16
	ApiKey         int16
	MinVersion     int16
	MaxVersion     int16
	TaggedFields   byte
	ThrottleTimeMs int32
}

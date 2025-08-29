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
	Topics        []DescribeTopicPartitionTopic
	TaggedFields  byte // VARINT, usually 0
}

type DescribeTopicPartitionTopic struct {
	TopicName    string
	TopicID      [16]byte // UUID, all zeros for unknown topic
	ErrorCode    int16    // 3 for unknown topic
	Partitions   []DescribeTopicPartitionPartition
	TaggedFields byte // VARINT, usually 0
}

type DescribeTopicPartitionPartition struct {
	// For unknown topic, this array is empty
	// If implemented, would include partition_index, error_code, etc.
	// For this stage, leave empty
}

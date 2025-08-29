package main

import "time"

const (
	MessageSizeLengh        = 4
	RequestAPIKeyLenght     = 2
	RequestAPIVersionLenght = 2
	CorrelationIDLenght     = 4

	ReadTimeout  = 5 * time.Second
	WriteTimeout = 5 * time.Second

	MaxMessageSize = 2 * 1024 * 1024 // 2MB

	ApiVersionAPIKEY              = 18
	DescribeTopicPartitionsAPIKEY = 75

	NoneCode               = int16(0)
	UnsupportedVersionCode = int16(35)
)

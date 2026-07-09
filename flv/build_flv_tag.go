package flv

import (
	"encoding/binary"
)

// E.4.1 FLV Tag 참조
func BuildFLVTag(msgType byte, timestamp uint32, payload []byte) []byte {
	payloadLen := uint32(len(payload))

	// 11 bytes 헤더 + payload + 4bytes
	tagHeader := make([]byte, 11)

	// Reserved(should be 0, 2bits) + Filter(0: No preprocessing required, 1bits) + TagType: 8 or 9 (5 bits)
	tagHeader[0] = msgType

	// DataSize
	tagHeader[1] = byte(payloadLen >> 16)
	tagHeader[2] = byte(payloadLen >> 8)
	tagHeader[3] = byte(payloadLen)

	// Timestamp
	tagHeader[4] = byte(timestamp >> 16)
	tagHeader[5] = byte(timestamp >> 8)
	tagHeader[6] = byte(timestamp)

	// TimestampExtended
	tagHeader[7] = byte(timestamp >> 24) // ext timestamp

	// StreamID: Always 0.
	tagHeader[8] = 0
	tagHeader[9] = 0
	tagHeader[10] = 0

	// 태그 전체 크기
	tagFooter := make([]byte, 4)
	binary.BigEndian.PutUint32(tagFooter, 11+payloadLen)

	result := make([]byte, 0, 11+payloadLen+4)
	result = append(result, tagHeader...)
	// AudioTagHeader, VideoTagHeader가 포함됨
	// EncryptionHeader, FilterParams 는 OBS에서 안줌 그래서 생략
	result = append(result, payload...)
	result = append(result, tagFooter...)

	return result
}

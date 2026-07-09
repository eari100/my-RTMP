package rtmp

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// 맨앞에 1byte 마커를 읽고, 그에 맞는 바디 파서를 호출하는
func ReadAMF0(r io.Reader) (interface{}, error) {
	marker := make([]byte, 1)
	if _, err := io.ReadFull(r, marker); err != nil {
		return nil, err
	}

	switch marker[0] {
	// Number (float64)
	case 0x00:
		return readAMF0Number(r)
	// Boolean
	//case 0x01:
	// String
	case 0x02:
		return readAMF0String(r)
	// Object (Map 구조)
	case 0x03:
		return readAMF0Obj(r)
	// Null
	case 0x05:
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown chunk marker: 0x%02x", marker[0])
	}
}

// 2.2 Number Type
// An AMF 0 Number type is used to encode an ActionScript Number. The data following a Number type marker is always an 8 byte IEEE-754 double precision floating point value in network byte order (sign bit in low memory).
// number-type = number-marker
func readAMF0Number(r io.Reader) (float64, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}

	bits := binary.BigEndian.Uint64(buf)

	val := math.Float64frombits(bits)

	return val, nil
}

// AMF0 문자열을 읽어오는 헬퍼
// string-type        = string-marker UTF-8
// UTF-8-string = u16 UTF-8-data
func readAMF0String(r io.Reader) (string, error) {
	// unsigned 16비트 정수(BigEndian)는 문자열의 길이
	lenBuf := make([]byte, 2)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return "", err
	}
	strLen := binary.BigEndian.Uint16(lenBuf)

	// 길이만큼 문자열 데이터 읽기
	strBuf := make([]byte, strLen)
	if _, err := io.ReadFull(r, strBuf); err != nil {
		return "", err
	}

	return string(strBuf), nil
}

// readAMF0StringBody: 이미 타입 마커(0x02)를 확인한 후,
// 문자열의 길이(2바이트)를 읽고, 그만큼의 데이터를 문자열로 반환합니다.
func readAMF0StringBody(r io.Reader) (string, error) {
	// 1. 길이 정보 읽기 (2바이트, Big-Endian)
	lenBuf := make([]byte, 2)
	_, err := io.ReadFull(r, lenBuf)
	if err != nil {
		return "", err
	}

	strLen := binary.BigEndian.Uint16(lenBuf)

	// 2. 문자열 내용 읽기
	strBuf := make([]byte, strLen)
	_, err = io.ReadFull(r, strBuf)
	if err != nil {
		return "", err
	}

	return string(strBuf), nil
}

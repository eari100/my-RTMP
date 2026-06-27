package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
)

// ChunkStream은 각 CSID별로 이전 헤더 정보와 누적된 데이터를 저장
type ChunkStream struct {
	Fmt            byte   // 0: 시작,탐색 1: 타임스탬프 델타와 페이로드의 길이 포함, 2: 스트림 ID와 타임스탬프가 이전 청크와 완전히 동일, 3: 헤더가 없음, 이전 청크와 메세지 크기가 모두 같을 때 사용
	CSID           uint32 // chunk Stream ID (오디오, 비디오, 제어 메시지), 1 ~ 3byte
	Timestamp      uint32
	TimestampDelta uint32 // 이전 청크 간의 시각 차이를 ms 단위로 나타냄 (3bytes)
	MsgLength      uint32
	MsgTypeID      byte // 1: 청크 크기 설정, 2: 바이트 확인, 3: 확인 응답, 4: 윈도우 확인 크기 설정, 5: 피드백 대역폭 설정
	// 8: 오디오, 9: 비디오, 15: 사용자 정의, 18: AFM0 인코딩 데이터, 19: AMF0 인코딩 명령어 ( connect, createStream, publish, _result, _error)
	MsgStreamID uint32 // ex) 0 방송시작, 종료 제어, 1 ~ n : 영상 및 소리 (얼굴, 게임 화면 등)
	FullPayload []byte // 완전히 조립될 때까지 데이터 조각을 모으는 버퍼
	UseExtTS    bool
}

type StreamSession struct {
	Conn                net.Conn
	VideoSequenceHeader []byte
	AudioSequenceHeader []byte
	ChunkStreams        map[uint32]*ChunkStream
	ChunkSize           uint32
	Hub                 *Hub
}

// 한명의 BJ 방송에 할당되는 Hub
type Hub struct {
	// 실시간 시청자
	Consumers map[chan []byte]bool

	// FLV tag
	Broadcast chan []byte

	// 시청자 입장
	Register chan chan []byte

	// 시청자 퇴장
	Unregister chan chan []byte
}

type RoomManager struct {
	sync.RWMutex
	Rooms map[string]*StreamSession
}

var manager = &RoomManager{
	Rooms: make(map[string]*StreamSession),
}

func NewHub() *Hub {
	return &Hub{
		Consumers:  make(map[chan []byte]bool),
		Broadcast:  make(chan []byte, 1024), // 버퍼 줘서 통신을 유연하게 만듦
		Register:   make(chan chan []byte),
		Unregister: make(chan chan []byte),
	}
}

func (h *Hub) Run() {
	log.Println("[허브 서버] 가동")
	for {
		select {
		// 시청자 입장
		case consumer := <-h.Register:
			h.Consumers[consumer] = true
			log.Printf("[허브] 시청자 등록 (현재: %d명)", len(h.Consumers))
		// 시청자 퇴장
		case consumer := <-h.Unregister:
			if _, exists := h.Consumers[consumer]; exists {
				delete(h.Consumers, consumer)
				close(consumer)
				log.Printf("[허브] 시청자 퇴장 (현재: %d명)", len(h.Consumers))
			}
		// FLV
		case flvTag := <-h.Broadcast:
			for consumer := range h.Consumers {
				select {
				case consumer <- flvTag:
				default:
					// 논블로킹 방어
				}
			}
		}
	}
}

func (s *StreamSession) Handle() {
	defer s.Conn.Close()
	log.Printf("새로운 BJ 연결됨: %s", s.Conn.RemoteAddr().String())

	// 1. 핸드쉐이크
	doHandshake(s.Conn)

	headerBuf := make([]byte, 11) // 헤더 읽기용 임시 버퍼

	for {
		// --- [단계 1] Basic Header 읽기 ---
		basicHeader := make([]byte, 1)
		if _, err := io.ReadFull(s.Conn, basicHeader); err != nil {
			log.Printf("연결 종료 또는 읽기 실패: %v", err)
			return
		}

		// 8bis 라 시프트 연산만 수행
		fmtBytes := basicHeader[0] >> 6
		// uint32: usid = 65599 overflow 방어
		csid := uint32(basicHeader[0] & 0x3F)

		if csid == 0 { // csid range: 64-319
			extCSID := make([]byte, 1)
			io.ReadFull(s.Conn, extCSID)
			csid = uint32(extCSID[0]) + 64
		} else if csid == 1 { // csid range: 64-65599
			extCSID := make([]byte, 2)
			io.ReadFull(s.Conn, extCSID)
			csid = uint32(binary.BigEndian.Uint16(extCSID)) + 64
		}
		// 참고: csid 2는 Chunk Size 변경 등 프로토콜 제어용으로 예약

		// 청크를 맵에 할당 or 호출
		state, exists := s.ChunkStreams[csid]
		if !exists {
			state = &ChunkStream{CSID: csid}
			s.ChunkStreams[csid] = state
		}
		state.Fmt = fmtBytes

		// --- [단계 2] fmt에 따른 Message Header 읽기 및 복원 ---
		switch fmtBytes {
		case 0: // 11 bytes 완전한 헤더
			if _, err := io.ReadFull(s.Conn, headerBuf[:11]); err != nil {
				return
			}

			state.Timestamp = uint32(headerBuf[0])<<16 | uint32(headerBuf[1])<<8 | uint32(headerBuf[2])
			state.MsgLength = uint32(headerBuf[3])<<16 | uint32(headerBuf[4])<<8 | uint32(headerBuf[5])
			state.MsgTypeID = headerBuf[6]
			// 특이사항: MsgStreamID는 Little Endian 규격
			state.MsgStreamID = binary.LittleEndian.Uint32(headerBuf[7:11])
			// Extended Timestamp 처리 (타임스탬프가 0xFFFFFF 이면 뒤에 4 bytes 생김)
			state.UseExtTS = state.Timestamp == 0xFFFFFF

			if state.UseExtTS {
				extTS := make([]byte, 4)
				if _, err := io.ReadFull(s.Conn, extTS); err != nil {
					return
				}

				state.Timestamp = binary.BigEndian.Uint32(extTS)
			}

		case 1: // 7 bytes 헤더 (MsgStreamID는 이전 것 재사용)
			if _, err := io.ReadFull(s.Conn, headerBuf[:7]); err != nil {
				return
			}

			state.TimestampDelta = uint32(headerBuf[0])<<16 | uint32(headerBuf[1])<<8 | uint32(headerBuf[2])
			state.MsgLength = uint32(headerBuf[3])<<16 | uint32(headerBuf[4])<<8 | uint32(headerBuf[5])
			state.MsgTypeID = headerBuf[6]
			state.UseExtTS = state.TimestampDelta == 0xFFFFFF

			if state.UseExtTS {
				extTS := make([]byte, 4)
				if _, err := io.ReadFull(s.Conn, extTS); err != nil {
					return
				}
				state.TimestampDelta = binary.BigEndian.Uint32(extTS)
			}

			state.Timestamp += state.TimestampDelta

		case 2: // 3 bytes 헤더 (Length, Type, StreamID 모두 이전 것 재사용)
			if _, err := io.ReadFull(s.Conn, headerBuf[:3]); err != nil {
				return
			}

			state.TimestampDelta = uint32(headerBuf[0])<<16 | uint32(headerBuf[1])<<8 | uint32(headerBuf[2])
			state.UseExtTS = state.TimestampDelta == 0xFFFFFF

			if state.UseExtTS {
				extTS := make([]byte, 4)
				if _, err := io.ReadFull(s.Conn, extTS); err != nil {
					return
				}
				state.TimestampDelta = binary.BigEndian.Uint32(extTS)
			}
			state.Timestamp += state.TimestampDelta

		case 3: // 0 byte 헤더 (이전 헤더 속성 완벽히 재사용)

			if state.UseExtTS {
				extTS := make([]byte, 4)
				if _, err := io.ReadFull(s.Conn, extTS); err != nil {
					return
				}

				if len(state.FullPayload) == 0 {
					state.TimestampDelta = binary.BigEndian.Uint32(extTS)
				}
			}

			// fmt 3이고 영상 데이터 도중이라면 TimestampDelta 만큼 더해줌
			if len(state.FullPayload) == 0 { // 0 이라면 분할 시작이라는 뜻이 되니까 그때만 timestamp 증가 시킴
				state.Timestamp += state.TimestampDelta
			}
		}

		// --- [단계 3] 현재 청크 크기만큼만 정확하게 잘라서 읽기 ---
		remains := int(state.MsgLength) - len(state.FullPayload)
		readSize := int(s.ChunkSize)
		if remains < readSize {
			readSize = remains
		}

		chunkData := make([]byte, readSize)
		if _, err := io.ReadFull(s.Conn, chunkData); err != nil {
			log.Printf("페이로드 읽기 실패: %v", err)
			return
		}

		// 청크 데이터 버퍼에 누적
		state.FullPayload = append(state.FullPayload, chunkData...)

		// --- [단계 4] 데이터가 다 모였을 때만 완전한 메시지로 처리 ---
		if len(state.FullPayload) == int(state.MsgLength) {
			// 스펙문서: Protocol control message 1, Set Chunk Size, is used to notify the    peer of a new maximum chunk size
			if state.CSID == 2 && state.MsgTypeID == 1 {
				s.ChunkSize = binary.BigEndian.Uint32(state.FullPayload)
				log.Printf("⚙️ OBS 요청으로 Chunk Size 변경됨: %d 바이트", s.ChunkSize)
			}

			switch state.MsgTypeID {
			// todo:  7.1.1.  Command Message (20, 17)
			// AMF 3 (아마 안 쓸듯)
			case 17:
				log.Printf("AMF3는 패싱할께요")

			// AMF 0
			case 20:
				reader := bytes.NewReader(state.FullPayload)

				// 1. Command Name
				cmdObj, err := ReadAMF0(reader)
				if err != nil {
					return
				}
				cmd, ok := cmdObj.(string)
				if !ok {
					return
				}

				// 2. Transaction ID
				txObj, err := ReadAMF0(reader)
				if err != nil {
					return
				}
				tx, _ := txObj.(float64)

				switch cmd {
				case "connect":
					// 3. Command Object
					metaObj, err := ReadAMF0(reader)
					if err != nil {
						return
					}

					metaMap, ok := metaObj.(map[string]interface{})
					if !ok {
						log.Printf("connect 메타데이터 구조가 올바르지 않습니다.")
						return
					}

					// 4. Optional User Arguments
					// 생략

					log.Printf("connect 종합 분석 완료 -> 명령어: %s, ID: %.0f, 앱이름: %v", cmd, tx, metaMap)

					sendWindowAckSize(s.Conn, 2_500_000)
					sendSetPeerBandwidth(s.Conn, 2_500_000, 2)
					// 컴퓨터가 알아듣기 좋은 사이즈: 4096 byte
					sendSetChunkSize(s.Conn, 4096)
					sendConnectResult(s.Conn, tx)

				case "releaseStream":
					log.Printf("🧹 OBS가 스트림 청소를 요청함 (releaseStream) -> 안전하게 패스")

				case "FCPublish":
					log.Printf("📢 OBS가 방송 송출 예고를 보냄 (FCPublish) -> 안전하게 패스")

				case "createStream":
					log.Printf("🏗️ OBS가 새로운 스트림 통로 개설을 요청함! (TxID: %.0f)", tx)
					sendCreateStreamResult(s.Conn, tx)

				case "publish":
					// 3. Command Object
					metaObj, err := ReadAMF0(reader)
					if err != nil {
						return
					}

					// 4. Publishing Name
					pubName, err := ReadAMF0(reader)
					if err != nil {
						return
					}

					// 5. Publishing Type
					pubType, err := ReadAMF0(reader)
					if err != nil {
						return
					}
					log.Printf("🚀 [Publish] 방송 송출 요청 분석 완료 -> 명령어: %s, ID: %.0f, 스트림 키(Stream Key): %s, 송출 타입: %s (CommandObj: %v)", cmd, tx, pubName, pubType, metaObj)

					sendOnStatus(s.Conn, tx)

				default:
					log.Printf("알 수 없는 AMF0 명령어: %s", cmd)
				}

			// AMF 0 metadata (화면 비율, 영상 규격)
			case 18:
				log.Printf("ℹ️ [Metadata] 방송 스펙 정보 도착 (크기: %d 바이트)", state.MsgLength)
			// AMF 3 metadata
			case 15:
				log.Printf("AMF 3 metadata 패싱")
			// video
			case 9:
				// SPS/PPS 저장
				if len(state.FullPayload) >= 2 && state.FullPayload[0] == 0x17 && state.FullPayload[1] == 0x00 {
					if len(s.VideoSequenceHeader) == 0 {
						s.VideoSequenceHeader = s.buildFLVTag(state.MsgTypeID, state.Timestamp, state.FullPayload)
						log.Println("SPS/PPS 저장")
					}
				}

				flvTag := s.buildFLVTag(state.MsgTypeID, state.Timestamp, state.FullPayload)

				if s.Hub != nil {
					s.Hub.Broadcast <- flvTag
				}
			// audio
			case 8:
				// AAC Config 저장
				if len(state.FullPayload) >= 2 && state.FullPayload[0] == 0xAF && state.FullPayload[1] == 0x00 {
					if len(s.AudioSequenceHeader) == 0 {
						s.AudioSequenceHeader = s.buildFLVTag(state.MsgTypeID, state.Timestamp, state.FullPayload)
						log.Println("AAC Config 저장")
					}
				}

				flvTag := s.buildFLVTag(state.MsgTypeID, state.Timestamp, state.FullPayload)

				if s.Hub != nil {
					s.Hub.Broadcast <- flvTag
				}

			default:
				log.Printf("msgType: %v", state.MsgTypeID)
			}

			// 다음 패킷을 받기 위해 버퍼 초기화
			state.FullPayload = nil
		}

		log.Printf("🔍 [State 변환] Fmt: %d | CSID: %d | MsgType: %d | MsgLen: %d | TS: %d (Delta: %d) | StreamID: %d | ExtTS: %t | PayloadCollected: %d/%d",
			state.Fmt,
			state.CSID,
			state.MsgTypeID,
			state.MsgLength,
			state.Timestamp,
			state.TimestampDelta,
			state.MsgStreamID,
			state.UseExtTS,
			len(state.FullPayload), // 현재까지 모인 바이트 수
			state.MsgLength,        // 모아야 하는 총 바이트 수
		)
	}
}

// E.4.1 FLV Tag 참조
func (s *StreamSession) buildFLVTag(msgType byte, timestamp uint32, payload []byte) []byte {
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

func readAMF0Obj(r io.Reader) (map[string]interface{}, error) {
	obj := make(map[string]interface{})

	for {
		// key 길이
		var keyLen uint16
		// 리플렉션으로 keyLen의 type을 알아냄
		if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}

		if keyLen == 0 {
			var endMarker byte
			if err := binary.Read(r, binary.BigEndian, &endMarker); err != nil {
				return nil, err
			}
			if endMarker == 0x09 {
				break
			}
		}

		// key 문자열 값
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			return nil, err
		}
		key := string(keyBuf)

		// value
		val, err := ReadAMF0(r)
		if err != nil {
			return nil, err
		}

		obj[key] = val
	}

	return obj, nil
}

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

func sendWindowAckSize(w io.Writer, size uint32) error {
	buf := new(bytes.Buffer)

	// 1. Chunk Basic Header (1 byte)
	// fmt = 0, CSID=2
	buf.WriteByte(0x02)

	// 2. Chunk Message Header (11 bytes)
	// 2-1. Timestamp: 3bytes (0으로 일단)
	buf.Write([]byte{0x00, 0x00, 0x00})

	// 2-2. Message Length: 3 bytes
	// payload의 크기가 4니까
	buf.Write([]byte{0x00, 0x00, 0x04})

	// 2-3. Message Type ID: 1 byte
	buf.WriteByte(0x05)

	// 2-4. Message Stream ID (제어 메시지는 무조건 0번 스트림)
	binary.Write(buf, binary.LittleEndian, uint32(0))

	// 3. Message Payload (4 바이트)
	// ⚠️ 주의: 페이로드 내부 데이터는 빅 엔디언입니다.
	// 우리가 설정할 실제 윈도우 크기 값(예: 2500000)을 채워 넣습니다.
	binary.Write(buf, binary.BigEndian, size)

	_, err := w.Write(buf.Bytes())
	if err != nil {
		log.Printf("WindowAckSize 전송 실패: %v", err)
		return err
	}

	log.Printf("➡️ OBS에게 WindowAckSize(%d) 대답 전송 완료!", size)
	return nil
}

func sendSetPeerBandwidth(conn net.Conn, size uint32, limitType byte) error {
	// Fmt 0 헤더(12 bytes, 12+5)
	buf := make([]byte, 12+5)

	// 1. Basic Header (Fmt:0, CSID: 2)
	buf[0] = 0x02

	// 2. Message Header (11 bytes)
	// 2-1. Timestamp (3 bytes)
	buf[1] = 0
	buf[2] = 0
	buf[3] = 0

	// 2-2. MsgLength (3 bytes, payload = 5)
	buf[4] = 0
	buf[5] = 0
	buf[6] = 5

	// 2-3. MsgTypeID (1 byte, Set Peer Bandwidth = 6)
	buf[7] = 6

	// 2-4. MsgStreamID
	binary.LittleEndian.PutUint32(buf[8:12], 0)

	// 3. Payload (5 bytes)
	binary.BigEndian.PutUint32(buf[12:16], size)

	// 4. limitType: 0(Hard), 1(Soft), 2(Dynamic)
	_, err := conn.Write(buf)
	if err != nil {
		log.Printf("SetPeerBandwidth 전송 실패: %v", err)
		return err
	}

	log.Printf("➡️ OBS에게 SetPeerBandwidth(%d, Type: %d) 대답 전송 완료!", size, limitType)

	return nil
}

// 0x02 + length(2 bytes) + string
func appendAMFString(buf []byte, s string) []byte {
	buf = append(buf, 0x02)                          // AMF0: string
	buf = append(buf, byte(len(s)>>8), byte(len(s))) // big endian
	buf = append(buf, s...)

	return buf
}

// 0x00 + 8 bytes Float64
func appendAMFNumber(buf []byte, n float64) []byte {
	buf = append(buf, 0x00)
	bits := math.Float64bits(n)
	buf = append(buf, byte(bits>>56), byte(bits>>48), byte(bits>>40), byte(bits>>32),
		byte(bits>>24), byte(bits>>16), byte(bits>>8), byte(bits))

	return buf
}

func appendObjKey(buf []byte, k string) []byte {
	buf = append(buf, byte(len(k)>>8), byte(len(k)))
	buf = append(buf, k...)

	return buf
}

// OBS와 서버 간의 청크의 최대 크기 변경
// default는 128 bytes 입니다.
func sendSetChunkSize(conn net.Conn, chunkSize uint32) error {
	packet := make([]byte, 16)

	// [Fmt: 0 (00)] + [CSID: 2 (000010)] ➡️ 0x02
	// 💡 CSID 2번은 프로토콜 저수준 제어 전용 차선
	packet[0] = 0x02

	// MsgLength (3바이트, 4바이트짜리 uint32 숫자가 들어가므로 길이는 무조건 '4')
	packet[4] = 0
	packet[5] = 0
	packet[6] = 4

	// MsgTypeID (1바이트, 청크 크기 설정 명령은 규격서상 '1번')
	packet[7] = 1

	// MsgStreamID (4바이트, 제어용 통로는 항상 0번 채널, Little Endian)
	binary.LittleEndian.PutUint32(packet[8:12], 0)

	binary.BigEndian.PutUint32(packet[12:16], chunkSize)
	_, err := conn.Write(packet)
	if err != nil {
		log.Printf("Set Chunk Size 전송 실패: %v", err)
		return err
	}

	log.Printf("➡️ OBS에게 Set Chunk Size (%d 바이트) 설정 명령 전송 완료!", chunkSize)
	return nil

}
func make_RTMP_header(csid byte, payloadLen uint32, msgTypeID byte, streamID uint32) []byte {
	header := make([]byte, 12)
	// ex) Fmt 0, CSID: 3 (명령 제어)
	header[0] = csid

	// Timestamp (3 bytes, 0)
	header[1], header[2], header[3] = 0, 0, 0

	// MsgLength (3 bytes, big endian)
	header[4] = byte(payloadLen >> 16)
	header[5] = byte(payloadLen >> 8)
	header[6] = byte(payloadLen)

	// MsgTypeID (1 byte)
	header[7] = msgTypeID

	// MsgStreamID (4 byte, 제어는 0, 방송은 1, Little Endian)
	binary.LittleEndian.PutUint32(header[8:12], streamID)

	return header
}

func sendConnectResult(conn net.Conn, txID float64) error {
	// 왜 200 인가?
	// Command Name: 10 bytes (마커 1 + 길이 2 + 글자 데이터 7)
	// Transaction ID: 9 bytes (마커 1 + 숫자 데이터 8)

	// Properties: fmsVer + capabilities
	// fmsVer: 24 bytes(key 8 + value 16), capabilities: 23 bytes(key 14 + value 9)

	// Information: 95 byres
	// level (key(7) + value(9))
	// code (key(6) + value(32))
	// description (key(13) + value(24))
	// 10 + 9 + 51 + 95 = 165 bytes (200 안넘음)
	p := make([]byte, 0, 200)

	// 1. Command Name("_result"/"_error")
	p = appendAMFString(p, "_result")

	// 2. Transaction ID (value: 1)
	p = appendAMFNumber(p, txID)

	// 3. Properties ("fmsVer", "capabilities")
	p = append(p, 0x03)
	p = appendObjKey(p, "fmsVer")
	// todo: 정말 obs 내에서 "FMS/3,0,1,123"로 받아야 되는 지 볼 것
	p = appendAMFString(p, "FMS/3,0,1,123")

	p = appendObjKey(p, "capabilities")
	// 31 -> 1 1 1 1 1(2)
	// 1번째 비트: 오디오/비디오 스트리밍 지원
	// 2번째 비트: AMF3 포맷 이해 가능
	// 3번째 비트: 재연결 및 스트림 제어 명령을 지원
	// 4번째 비트: 대역폭 관리 및 윈도우 Ack 사이즈 조절 가능
	// 5번째 비트: RTMP 프로토콜 확장 기능을 지원 (차세대 고화질 코덱 지원, 보안, 대규모 인프라 지원)
	p = appendAMFNumber(p, 31)
	p = append(p, 0x00, 0x00, 0x09) // Obj End 마커

	// 4. Information ("code", "level", "description", ...)
	p = append(p, 0x03) // Object Start 마커
	p = appendObjKey(p, "level")
	p = appendAMFString(p, "status")
	p = appendObjKey(p, "code")
	p = appendAMFString(p, "NetConnection.Connect.Success")
	p = appendObjKey(p, "description")
	p = appendAMFString(p, "Connection succeeded.")
	p = append(p, 0x00, 0x00, 0x09) // Object End 마커

	finalPacket := append(make_RTMP_header(0x03, uint32(len(p)), 0x14, 0), p...)
	_, err := conn.Write(finalPacket)
	if err != nil {
		log.Printf("_result 전송 실패: %v", err)
		return err
	}

	log.Printf("➡️ OBS에게 connect 성공 응답(_result, TxID: %.0f) 전송 완료!", txID)

	return nil
}

func sendCreateStreamResult(conn net.Conn, txID float64) error {
	p := make([]byte, 0, 30)

	// _result
	p = appendAMFString(p, "_result")

	// Transaction ID
	p = appendAMFNumber(p, txID)

	// command obj
	p = append(p, 0x05) // null

	// stream ID
	p = appendAMFNumber(p, 1.0)

	packet := append(make_RTMP_header(0x03, uint32(len(p)), 0x14, 0), p...)
	_, err := conn.Write(packet)
	if err != nil {
		log.Printf("createStream 응답 전송 실패: %v", err)
		return err
	}

	log.Printf("➡️ OBS에게 createStream 성공 응답(_result, StreamID: 1, TxID: %.0f) 전송 완료!", txID)

	return nil
}

func sendOnStatus(conn net.Conn, txID float64) error {
	// cmd name
	p := make([]byte, 0, 30)
	p = appendAMFString(p, "onStatus")

	// tx ID
	p = appendAMFNumber(p, txID)

	// cmd obj (null)
	p = append(p, 0x05)

	// Info Object

	p = append(p, 0x03)

	// 1. level: "warning" | "status" | "error"
	p = appendObjKey(p, "level")
	p = appendAMFString(p, "status")

	// 2. code: "NetStream.Play.Start" (시청 시작 승인) | "NetStream.Publish.Start" (송출 시작 승인)
	p = appendObjKey(p, "code")
	p = appendAMFString(p, "NetStream.Publish.Start")

	// 3. description: "(자유)"
	p = appendObjKey(p, "description")
	p = appendAMFString(p, "Stream is up.")

	p = append(p, 0x00, 0x00, 0x09)

	last_p := append(make_RTMP_header(0x03, uint32(len(p)), 0x14, 1), p...)

	_, err := conn.Write(last_p)
	if err != nil {
		log.Printf("❌ onStatus 응답 전송 실패: %v", err)
		return err
	}

	log.Println("➡️ OBS에게 방송 송출 승인(onStatus: NetStream.Publish.Start) 완료!")
	return nil
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

func doHandshake(conn net.Conn) {
	log.Print("OBS 접속 확인! 핸드셰이크 시작...")

	// 1. C0 + C1 읽기 (1 + 1536 = 1536 bytes
	c0c1 := make([]byte, 1537)
	_, err := io.ReadFull(conn, c0c1)
	if err != nil {
		log.Println("c0/c1 읽기 실패:", err)
		return
	}

	version := c0c1[0]
	log.Printf("받은 RTMP 버전: 0x%02x\n", version)

	// 2. S0 + S1 +S2 보내기
	// S0: 1 바이트 (c0와 동일한 버전)
	s0 := []byte{version}

	// S1: 1536 바이트 (간단하게 0으로 채우거나 랜덤 데이터)
	s1 := make([]byte, 1536)

	// s2: 1536 바이트 (C1의 데이터 중 앞 1바이트를 제외한 1536 바이트를 그대로 echo)
	s2 := c0c1[1:1537]

	// 서버 응답 한번에 뭉쳐서 전송
	var serverResponse bytes.Buffer
	serverResponse.Write(s0)
	serverResponse.Write(s1)
	serverResponse.Write(s2)

	_, err = conn.Write(serverResponse.Bytes())
	if err != nil {
		log.Println("S0/S1/S2 전송 실패:", err)
		return
	}

	// C2 읽기 (1536 바이트)
	c2 := make([]byte, 1536)
	_, err = io.ReadFull(conn, c2)
	if err != nil {
		log.Println("C2 읽기 실패:", err)
		return
	}

	log.Println("핸드셰이크 성공!")
}

func PlayerHandle(w http.ResponseWriter, r *http.Request, s *StreamSession) {
	log.Printf("[HTTP] 새로운 시청자가 브라우저로 접속했습니다: %s", r.RemoteAddr)

	if s == nil || s.Hub == nil {
		http.Error(w, "방송이 오프라인 상태", http.StatusNotFound)
		return
	}

	// CORS 방화벽 해제
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	// HTTP 연결을 "실시간 스트리밍 모드(Chunked Transfer Encoding) 전환
	// 서버가 전송하는 걸 받도록 설정
	w.Header().Set("Content-Type", "video/x-flv")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	// 9 bytes header +4 bytes PreviousTagSize0 = 13 bytes
	flvHeader := []byte{
		0x46, 0x4c, 0x56, 0x01, // 'F', 'L' 'V' Version 1
		0x05,                   // Audio + Video
		0x00, 0x00, 0x00, 0x09, // header size (9 bytes)
		0x00, 0x00, 0x00, 0x00, // PreviousTagSize0 (4 bytes)
	}
	w.Write(flvHeader)

	// 8 bytes 시청자 주소 파이프
	myChan := make(chan []byte, 512)

	s.Hub.Register <- myChan

	// 브라우저 종료
	defer func() {
		s.Hub.Unregister <- myChan
	}()

	if len(s.VideoSequenceHeader) > 0 {
		w.Write(s.VideoSequenceHeader)
	}

	if len(s.AudioSequenceHeader) > 0 {
		w.Write(s.AudioSequenceHeader)
	}

	// 브라우저로 video, audio 규격 flush
	if flush, ok := w.(http.Flusher); ok {
		flush.Flush()
	}

	for {
		select {
		case data := <-myChan:
			_, err := w.Write(data)
			if err != nil {
				return // 시청자가 나가면 루프 탈출
			}
			// 즉시 인터넷선으로 밀어내기
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		case <-r.Context().Done():
			return // 브라우저 창 닫으면 탈출
		}
	}
}

func main() {

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		htmlData, err := os.ReadFile("index.html")
		if err != nil {
			// 만약 파일 이름이 틀렸거나 없다면 500 에러를 뱉습니다.
			http.Error(w, "HTML 파일을 찾을 수 없습니다.", http.StatusInternalServerError)
			return
		}

		// 웹페이지(HTML)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(htmlData)
	})

	http.HandleFunc("/live/", func(w http.ResponseWriter, r *http.Request) {
		roomName := strings.TrimPrefix(r.URL.Path, "/live/")

		if roomName == "" {
			http.Error(w, "방 이름을 입력해주세요.", http.StatusBadRequest)
			return
		}

		manager.RLock() // BJ Rock
		targetSession := manager.Rooms[roomName]
		manager.RUnlock()

		PlayerHandle(w, r, targetSession)
	})

	log.Println("🌐 [HTTP] 웹 시청자용 HTTP-FLV 서버 가동 준비 (포트: 8080)")

	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("HTTP 서버 가동 실패: %v", err)
		}
	}()

	listener, err := net.Listen("tcp", ":1935")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("1935 포트에서 RTMP 대기 중...")

	for {
		// 클라의 연결 요청 수락, 해당 클라와 통신할 수 있는 새로운 소켓 반환
		conn, err := listener.Accept()

		if err != nil {
			continue
		}

		go func(conn net.Conn) {
			// 임시
			roomName := "tmp-wook"

			roomHub := NewHub()
			go roomHub.Run()

			session := &StreamSession{
				Conn:         conn,
				ChunkSize:    128,
				ChunkStreams: make(map[uint32]*ChunkStream),
				Hub:          roomHub,
			}

			manager.Lock()
			manager.Rooms[roomName] = session
			manager.Unlock()

			session.Handle()

			manager.Lock()
			delete(manager.Rooms, roomName)
			manager.Unlock()
		}(conn)
	}
}

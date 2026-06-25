package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net"
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

//// todo:
//func sendMessage(w io.Writer, size uint32, fmt uint8, csid uint8) error {
//	buf := new(bytes.Buffer)
//
//	// 1. Chunk Basic Header (1 byte)
//	basicHeader := (fmt << 6) | uint8(csid)
//	buf.WriteByte(basicHeader)
//
//	// 2. Chunk Message Header 처리
//	if fmt == 0 {
//		// Timestamp: 3 bytes
//		// 관례상 0을 넣음 (나중에 매개변수로 처리해야되나?)
//		buf.Write([]byte{0x00, 0x00, 0x00})
//
//		// Message Length (3 bytes, Big Endian)
//		size := uint32(len(payload))
//
//	} else if fmt == 1 {
//
//	} else if fmt == 2 {
//
//	} else if fmt == 3 {
//
//	}
//	_, err := w.Write(buf.Bytes())
//	return err
//}

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

func processCompleteMessage(conn net.Conn, stream *ChunkStream) {
	switch stream.MsgTypeID {
	// todo:  7.1.1.  Command Message (20, 17)
	// AMF 3 (아마 안 쓸듯)
	case 17:
		log.Printf("AMF3는 패싱할께요")

	// AMF 0
	case 20:
		reader := bytes.NewReader(stream.FullPayload)

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

			sendWindowAckSize(conn, 2_500_000)
			sendSetPeerBandwidth(conn, 2_500_000, 2)
			// 컴퓨터가 알아듣기 좋은 사이즈: 4096 byte
			sendSetChunkSize(conn, 4096)
			sendConnectResult(conn, tx)

		case "releaseStream":
			log.Printf("🧹 OBS가 스트림 청소를 요청함 (releaseStream) -> 안전하게 패스")

		case "FCPublish":
			log.Printf("📢 OBS가 방송 송출 예고를 보냄 (FCPublish) -> 안전하게 패스")

		case "createStream":
			log.Printf("🏗️ OBS가 새로운 스트림 통로 개설을 요청함! (TxID: %.0f)", tx)
			sendCreateStreamResult(conn, tx)

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

			sendOnStatus(conn, tx)

		default:
			log.Printf("알 수 없는 AMF0 명령어: %s", cmd)
		}

	default:
		log.Printf("msgType: %v", stream.MsgTypeID)
	}

}

// RTMP 청크 리더 루프
// 기존의 바이트로 읽던 루프를 교체, fmt 규격 0,1,2,3 에 따라 헤더를 유연하게 복원
func handleClient(conn net.Conn) {
	defer conn.Close()
	log.Printf("새로운 BJ 연결됨: %s", conn.RemoteAddr().String())

	// 1. 핸드쉐이크
	// doHandshake(conn)

	// 청크 스트림 상태를 저장할 맵
	chunkStreams := make(map[uint32]*ChunkStream)

	// RTMP 기본 청크 크기는 128바이트로 시작하지만,
	// OBS가 Set Chunk Size(Type 1)를 보내면 이 변수를 업데이트해야 합니다.
	chunkSize := uint32(128)

	headerBuf := make([]byte, 11) // 헤더 읽기용 임시 버퍼

	for {
		// --- [단계 1] Basic Header 읽기 ---
		basicHeader := make([]byte, 1)
		if _, err := io.ReadFull(conn, basicHeader); err != nil {
			log.Printf("연결 종료 또는 읽기 실패: %v", err)
			return
		}

		// 8bis 라 시프트 연산만 수행
		fmtBytes := basicHeader[0] >> 6
		// uint32: usid = 65599 overflow 방어
		csid := uint32(basicHeader[0] & 0x3F)

		if csid == 0 { // csid range: 64-319
			extCSID := make([]byte, 1)
			io.ReadFull(conn, extCSID)
			csid = uint32(extCSID[0]) + 64
		} else if csid == 1 { // csid range: 64-65599
			extCSID := make([]byte, 2)
			io.ReadFull(conn, extCSID)
			csid = uint32(binary.BigEndian.Uint16(extCSID)) + 64
		}
		// 참고: csid 2는 Chunk Size 변경 등 프로토콜 제어용으로 예약

		// 청크를 맵에 할당 or 호출
		state, exists := chunkStreams[csid]
		if !exists {
			state = &ChunkStream{CSID: csid}
			chunkStreams[csid] = state
		}
		state.Fmt = fmtBytes

		// --- [단계 2] fmt에 따른 Message Header 읽기 및 복원 ---
		switch fmtBytes {
		case 0: // 11 bytes 완전한 헤더
			if _, err := io.ReadFull(conn, headerBuf[:11]); err != nil {
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
				if _, err := io.ReadFull(conn, extTS); err != nil {
					return
				}

				state.Timestamp = binary.BigEndian.Uint32(extTS)
			}

		case 1: // 7 bytes 헤더 (MsgStreamID는 이전 것 재사용)
			if _, err := io.ReadFull(conn, headerBuf[:7]); err != nil {
				return
			}

			state.TimestampDelta = uint32(headerBuf[0])<<16 | uint32(headerBuf[1])<<8 | uint32(headerBuf[2])
			state.MsgLength = uint32(headerBuf[3])<<16 | uint32(headerBuf[4])<<8 | uint32(headerBuf[5])
			state.MsgTypeID = headerBuf[6]
			state.UseExtTS = state.TimestampDelta == 0xFFFFFF

			if state.UseExtTS {
				extTS := make([]byte, 4)
				if _, err := io.ReadFull(conn, extTS); err != nil {
					return
				}
				state.TimestampDelta = binary.BigEndian.Uint32(extTS)
			}

			state.Timestamp += state.TimestampDelta

		case 2: // 3 bytes 헤더 (Length, Type, StreamID 모두 이전 것 재사용)
			if _, err := io.ReadFull(conn, headerBuf[:3]); err != nil {
				return
			}

			state.TimestampDelta = uint32(headerBuf[0])<<16 | uint32(headerBuf[1])<<8 | uint32(headerBuf[2])
			state.UseExtTS = state.TimestampDelta == 0xFFFFFF

			if state.UseExtTS {
				extTS := make([]byte, 4)
				if _, err := io.ReadFull(conn, extTS); err != nil {
					return
				}
				state.TimestampDelta = binary.BigEndian.Uint32(extTS)
			}
			state.Timestamp += state.TimestampDelta

		case 3: // 0 byte 헤더 (이전 헤더 속성 완벽히 재사용)

			if state.UseExtTS {
				extTS := make([]byte, 4)
				if _, err := io.ReadFull(conn, extTS); err != nil {
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
		readSize := int(chunkSize)
		if remains < readSize {
			readSize = remains
		}

		chunkData := make([]byte, readSize)
		if _, err := io.ReadFull(conn, chunkData); err != nil {
			log.Printf("페이로드 읽기 실패: %v", err)
			return
		}

		// 청크 데이터 버퍼에 누적
		state.FullPayload = append(state.FullPayload, chunkData...)

		// --- [단계 4] 데이터가 다 모였을 때만 완전한 메시지로 처리 ---
		if len(state.FullPayload) == int(state.MsgLength) {
			// 스펙문서: Protocol control message 1, Set Chunk Size, is used to notify the    peer of a new maximum chunk size
			if state.CSID == 2 && state.MsgTypeID == 1 {
				chunkSize = binary.BigEndian.Uint32(state.FullPayload)
				log.Printf("⚙️ OBS 요청으로 Chunk Size 변경됨: %d 바이트", chunkSize)
			}

			processCompleteMessage(conn, state)

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

func handleRTMPChunkStream(conn net.Conn) {
	// OBS는 처음에 청크 크기를 128 bytes로 설정해서 보냅니다.
	chunkSize := 128
	_ = chunkSize

	for {
		// 1. Basic Header 읽기 (1byte)
		// Basic Header (1~3 바이트): 청크의 스트림 ID와 포맷을 알려줍니다.
		basicHeader := make([]byte, 1)
		if _, err := io.ReadFull(conn, basicHeader); err != nil {
			log.Println("연결 종료 or error:", err)
			return
		}

		fmt.Printf("\n--- [새 청크 수신] ---\n")
		fmt.Printf("Basic Header Byte: 0x%02x\n", basicHeader[0])

		bfmt := basicHeader[0] >> 6   // 앞 2비트 (Format)
		csid := basicHeader[0] & 0x3F // 뒤 6비트 (Chunk Stream ID), 0x3F는 001111 임
		fmt.Printf("Chunk Format (fmt): %d, Chunk Stream ID (csid): %d\n", bfmt, csid)

		var msgLength uint32
		var msgTypeID byte

		//2. Message Header 읽기 (fmt 값에 따라 크기가 다름)
		//AMF0 data type marker
		//0x02: string (명령어 이름이나 스트림 키)
		//0x00: Number (go 에서는 float64)
		//0x03: object
		if bfmt == 0 { // connect 나 publish는 0에 들어있음
			// fmt 0는 11bytes 짜리 가장 무거운 헤더입니다. (새로운 메세지 시작 알림)
			msgHeader := make([]byte, 11)
			if _, err := io.ReadFull(conn, msgHeader); err != nil {
				log.Println("Message Header 읽기 실패:", err)
				return
			}

			// msgHeader[0:3] = Timestamp (3bytes)

			// msgHeader[3:6] = Message Length (3bytes, BigEndian)
			msgLength = uint32(msgHeader[3])<<16 | uint32(msgHeader[4])<<8 | uint32(msgHeader[5])

			// msgHeader[6] = Message Type ID (1byte)
			msgTypeID = msgHeader[6]

			// msgHeader[7:11] = Message Stream ID (4바이트, LittleEndian인 경우가 많음)

			fmt.Printf("Message Length: %d 바이트, Message Type ID: %d (0x%02x)\n", msgLength, msgTypeID, msgTypeID)
		} else if bfmt == 1 { // Chunk Format 1 (7 byte header), createStream은 fmt == 1
			header := make([]byte, 7)
			_, err := io.ReadFull(conn, header)
			if err != nil {
				log.Println("fmt 1 헤더 읽기 에러:", err)
				break
			}

			// index 3,4,5는 Message Length
			msgLength := int(header[3]<<16 | header[4]<<8 | header[5])
			// index 6는 Message Type ID
			msgTypeID := int(header[6])

			fmt.Printf("msg Length: %d 바이트, msg Type ID: %d (0x%02x)\n", msgLength, msgTypeID, msgTypeID)

			// 이 청크의 데이터만큼만 읽을 수 있는 Reader 생성
			msgReader := io.LimitReader(conn, int64(msgLength))

			if msgTypeID == 20 {
				cmdName, err := readAMF0String(msgReader)
				if err == nil {
					fmt.Printf("[AF0 Command] 명령어 감지: %s\n", cmdName)

					// 🚨 바로 이곳! fmt=1 블록 안에서 createStream을 잡아야 합니다.
					if cmdName == "createStream" {
						fmt.Println("📺 OBS가 방송 채널(Stream) 생성을 요청했습니다. 1번 채널을 할당합니다.")

						// 남은 데이터 비우기
						io.Copy(io.Discard, msgReader)

						// createStream 승인 패킷 (1번 채널 부여, Transaction ID 4.0 하드코딩)
						createStreamResponse := []byte{
							0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1d, 0x14, 0x00, 0x00, 0x00, 0x00,
							0x02, 0x00, 0x07, 0x5f, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74,
							0x00, 0x40, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Number: 4.0
							0x05,                                                 // Null
							0x00, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Number: 1.0 (Stream ID)
						}

						_, err := conn.Write(createStreamResponse)
						if err != nil {
							log.Println("createStream 응답 전송 실패:", err)
						} else {
							fmt.Println("👉 채널 할당 완료! 이제 진짜 publish가 옵니다!")
						}

						// 명령어를 처리했으니 다음 루프로 넘어갑니다.
						continue
					}
				}
			}

			io.Copy(io.Discard, msgReader)
		} else {
			// fmt가 2,3 일 때는 이전 헤더 정보를 재사용하므로 간략화된 헤더가 옵니다.
			// 로우 레벨 구현 시에는 이전 상태를 구조체에 저장하고 매칭해야 됩니다.
			fmt.Printf("간략화된 청크 포맷(fmt:%d)입니다. 일단 생략하고 다음 바이트로 이동합니다.\n", bfmt)
		}

		// 3. 메세지 타입별 처리
		switch msgTypeID {
		case 1:
			// Protocol Control Message: Set Chunk Size
			// OBS가 청크 4096바이트 단위로 쪼개서 크기를 바꾸겠다는 패킷
			sizeBuf := make([]byte, 4)
			io.ReadFull(conn, sizeBuf)
			chunkSize = int(binary.BigEndian.Uint32(sizeBuf))
			fmt.Printf("OBS가 청크 크기를 변경함: %d bytes\n", chunkSize)

		case 8:
			// 오디오 데이터 처리
			fmt.Printf("🎧 오디오 패킷 수신: %d bytes\n", msgLength)
			io.CopyN(io.Discard, conn, int64(msgLength)) // 일단은 버리기
		case 9:
			// 비디오 데이터 처리 (H.264 / AVC)
			if msgLength == 0 {
				continue
			}

			// 1. 전체 비디오 페이로드 읽기
			videoBuf := make([]byte, msgLength)
			if _, err := io.ReadFull(conn, videoBuf); err != nil {
				log.Printf("비디오 패킷 읽기 실패: %v", err)
				return
			}

			// 데이터가 너무 작으면 파싱 불가
			if len(videoBuf) < 5 {
				continue
			}

			// 2. 첫 번째 바이트 분석 (FrameType & CodecID)
			firstByte := videoBuf[0]
			frameType := firstByte >> 4 // 상위 4비트
			codecID := firstByte & 0x0F // 하위 4비트

			// 3. 두 번째 바이트 분석 (AVCPacketType)
			avcPacketType := videoBuf[1]

			// 4. 컴포지션 타임 분석 (3바이트, BigEndian 형태로 계산)
			compositionTime := uint32(videoBuf[2])<<16 | uint32(videoBuf[3])<<8 | uint32(videoBuf[4])

			// 로그 출력으로 데이터 확인하기
			var frameTypeName string
			switch frameType {
			case 1:
				frameTypeName = "🔑 Keyframe (I-Frame)"
			case 2:
				frameTypeName = "🎬 Inter-frame (P/B-Frame)"
			default:
				frameTypeName = "❓ Unknown Frame"
			}

			var codecName string
			if codecID == 7 {
				codecName = "H.264 (AVC)"
			} else {
				codecName = fmt.Sprintf("Other (%d)", codecID)
			}

			fmt.Printf("\n🎥 [비디오 패킷 분석] 크기: %d bytes | 코덱: %s | 타입: %s\n", msgLength, codecName, frameTypeName)

			// AVC 패킷 타입에 따른 분기 처리
			switch avcPacketType {
			case 0:
				fmt.Println("   ➡️ [AVC Sequence Header] 디코더 설정 데이터(SPS/PPS) 수신 완료! (HLS 필수 품목)")
				// TODO: 이 바이트 배열을 메모리에 잘 보관해 두어야 나중에 .ts 파일을 만들 때 헤더에 박아넣을 수 있습니다.
			case 1:
				// 실제 영상 프레임 데이터 (NALU)
				// videoBuf[5:] 에 진짜 H.264 데이터가 들어있습니다.
				fmt.Printf("   ➡️ [AVC NALU] 실제 비디오 프레임 데이터 수신 중... (CompositionTime: %d)\n", compositionTime)
			case 2:
				fmt.Println("   ➡️ [AVC End of Sequence] 방송 종료 신호")
			}
		case 18:
			// 메타데이터(MetaData) 처리
			fmt.Printf("📊 메타데이터 수신: %d bytes\n", msgLength)
			io.CopyN(io.Discard, conn, int64(msgLength)) // 일단은 버리기

		case 20:
			// 0x14: AF0 Command Message (connect, publish 명령어 포함)
			msgReader := io.LimitReader(conn, int64(msgLength))

			// 첫 번째 AF0 데이터는 항상 '명령어 이름(String)' 입니다.
			cmdName, err := readAMF0String(msgReader)
			if err != nil {
				log.Println("명령어 이름 파싱 에러:", err)
				continue
			}
			fmt.Printf("[AMF0 Command] 명령어 감지: %s\n", cmdName)

			// OBS connect -> releaseStream -> FCPublish -> createStream -> publish 순으로 보냅니다.

			if cmdName == "connect" {
				fmt.Println("OBS 연결 요청. 승인 응답(_result) 전송")

				// msgReader 바이트 비움
				io.Copy(io.Discard, msgReader)

				// 임시
				// 1. Window Acknowledgement Size (Type 5)
				// 2. Set Peer Bandwidth (Type 6)
				// 3. Set Chunk Size (Type 1)
				// 4. _result (NetConnection.Connect.Success) AMF0 객체

				// todo: 지금은 한명을 위한 임시 ResultBytes 하드 코딩, 추후 다중 BJ 채널 대상으로 동적으로 만들어주도록 수정해야 됨
				connectResultBytes := []byte{
					// Window Ack Size (2500000)
					// 데이터 2.5MB 받을 때마다 신호 줌
					0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00,
					// Set Peer Bandwidth (2500000, Dynamic)
					// OBS도 2.5MB 제한 걸고 보내
					0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0x00, 0x00, 0x02,
					// Set Chunk Size (4096)
					// 이제부터 청크 단위 4096으로 설정
					0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00,

					// _result (Transaction ID 1, Connect.Success)
					// connect 를 승인

					// Chunk Header start
					// 0x03: Basic Header (fmt=0, csid=3) -> 지금부터 생판 처음 보내는 완전한 헤더 정보(11 byte) 시작된다
					0x03,
					// 타임스탬프: 0초
					0x00, 0x00, 0x00,
					// 메세지 길이(0x4e == 78): 이 헤더 뒤에 나오는 데이터(payload)는 117 bytes다
					0x00, 0x00, 0x4e,
					// Message Type ID: 0x14==20
					0x14,
					// Message Stream ID: 0번 스트림
					0x00, 0x00, 0x00, 0x00,

					// Chunk Header Start
					// AMF0 String 마커: 문자열 나온다
					0x02,
					// 문자열 길이: 0x07 7byte
					0x00, 0x07,

					// _result 문자열을 아스키코드로 변환
					0x5f, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74,

					// NetConnection.Connect.Success 아스키코드
					0x00, 0x3f, 0xf0, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x05, 0x03, 0x00, 0x05, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x02, 0x00, 0x06, 0x73,
					0x74, 0x61, 0x74, 0x75, 0x73, 0x00, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x02, 0x00, 0x1d, 0x4e, 0x65,
					0x74, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x43, 0x6f, 0x6e, 0x6e,
					0x65, 0x63, 0x74, 0x2e, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x00, 0x00, 0x09,
				}

				// OBS에 승인 메세지 전송
				_, err := conn.Write(connectResultBytes)
				if err != nil {
					log.Println("connect 응답 전송 실패:", err)
					return
				}
				fmt.Println("응답 전송 완료. 이제 OBS가 다음 명령어를 보내는 지 체크")

			} else if cmdName == "createStream" {
				fmt.Println("OBS가 방송 채널(Stream) 생성 요청했습니다. 1번 채널을 할당합니다.")

				// 데이터 비우기
				io.Copy(io.Discard, msgReader)

				createStreamResponse := []byte{
					// [Chunk Header] 11바이트
					0x03,             // Basic Header (fmt=0, csid=3)
					0x00, 0x00, 0x00, // 타임스탬프 0
					0x00, 0x00, 0x1d, // Message Length: 29 바이트 (0x1d)
					0x14,                   // Message Type ID: 20 (AMF0)
					0x00, 0x00, 0x00, 0x00, // Message Stream ID: 0

					// [Payload] 29바이트
					// 1. String: "_result"
					0x02, 0x00, 0x07, 0x5f, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74,
					// 2. Number: Transaction ID (4.0)
					// OBS는 보통 connect(1)->releaseStream(2)->FCPublish(3)->createStream(4) 순으로 번호를 매깁니다.
					0x00, 0x40, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

					// 3. Null: 명령어 객체 없음
					0x05,

					// 4. Number: Stream ID (1.0) - "너에게 1번 채널을 줄게!"
					0x00, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				}

				// 3. 응답 전송!
				_, err := conn.Write(createStreamResponse)
				if err != nil {
					log.Println("createStream 응답 전송 실패:", err)
					return
				}

			} else if cmdName == "publish" {
				fmt.Println("publish 명령어 찾았습니다. 스트림 키 추출")

				// 1. Transaction ID (Number 타입, 9 bytes) 건너뛰기
				// AF0에서 Number = 1byte(type) + 8bytes(값) = 9bytes
				msgReader.Read(make([]byte, 9))

				// 2. Command Object (Null type, 1byte) 건너뛰기
				msgReader.Read(make([]byte, 1))

				// 3. 여기에 stream key(string) 있을껄?
				for {
					marker := make([]byte, 1)
					_, err := msgReader.Read(marker)
					if err != nil {
						break
					}

					// marker가 0x02(string)일 때만 읽기 시도
					if marker[0] == 0x02 {
						// string 타입 읽기 (앞의 길이를 읽고 그만큼 문자열 읽기)
						str, err := readAMF0StringBody(msgReader)
						if err != nil {
							// 더 이상 읽을 String이 없으면 종료
							break
						}

						// 'live' 같은 애플리케이션 이름 뒤에 오는 고유 문자열이 스트림 키입니다.
						if str != "" && str != "live" {
							fmt.Printf("최종 추출된 스트림 키: %s\n", str)
						}
					}
				}

				publishResponse := []byte{
					// [1. Chunk Header] 12 bytes (Basic Header 1 byte + Msg Header 11 bytes)
					0x03,             // Basic Header (fmt=0, csid=3)
					0x00, 0x00, 0x00, // Timestamp (0)
					0x00, 0x00, 0x49, // Message Length: 73 bytes (0x49)
					0x14,                   // Message Type ID (20 = AMF0 Command)
					0x01, 0x00, 0x00, 0x00, // Message Stream ID: 1 (Little Endian) *** 처음에 임시로 1로 했으니

					// [2. Payload] 73 bytes
					// String: "onStatus" (8 bytes) ⭐️ 길이 수정됨
					0x02, 0x00, 0x08, 0x6f, 0x6e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73,

					// Number: Transaction ID (0.0) - onStatus는 보통 0을 사용합니다.
					0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

					// Null (명령어 객체 없음)
					0x05,

					// Object 시작
					0x03,
					// Property: "level" -> "status"
					0x00, 0x05, 0x6c, 0x65, 0x76, 0x65, 0x6c, // String: "level"
					0x02, 0x00, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, // String: "status"

					// Property: "code" -> "NetStream.Publish.Start" ⭐️ 정확한 길이와 아스키코드 적용
					0x00, 0x04, 0x63, 0x6f, 0x64, 0x65, // String: "code"
					0x02, 0x00, 0x17, 0x4e, 0x65, 0x74, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x2e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x2e, 0x53, 0x74, 0x61, 0x72, 0x74,

					// Object 종료 마커
					0x00, 0x00, 0x09,
				}

				_, err = conn.Write(publishResponse)
				if err != nil {
					log.Println("publish 응답 전송 실패:", err)
				} else {
					fmt.Println("✅ [방송 시작 승인] OBS에게 Publish Start 신호를 보냈습니다.")
				}

			} else {
				// 다른 명령어(connect 등)일 때는 남은 메세지 바이트를 비워줌
				// 그래야 다음 청크를 읽을 수 있음
				io.Copy(io.Discard, msgReader)
			}

		default:
			if msgLength > 0 {
				fmt.Printf("📦 데이터 패킷 수신 (Type: %d, Length: %d)\n", msgTypeID, msgLength)
				// 데이터를 읽어들이는 코드가 실제로 실행되는지 확인이 필요합니다.
				data := make([]byte, msgLength)
				_, err := io.ReadFull(conn, data)
				if err != nil {
					log.Printf("데이터 읽기 실패: %v", err)
				}
			}
		}
	}
}

func handleRTMP(conn net.Conn) {
	defer conn.Close()
	fmt.Println("OBS 접속 확인! 핸드셰이크 시작...")

	// 1. C0 + C1 읽기 (1 + 1536 = 1536 bytes
	c0c1 := make([]byte, 1537)
	_, err := io.ReadFull(conn, c0c1)
	if err != nil {
		log.Println("c0/c1 읽기 실패:", err)
		return
	}

	version := c0c1[0]
	fmt.Printf("받은 RTMP 버전: 0x%02x\n", version)

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

	fmt.Println("핸드셰이크 성공!")
	// 바이너리 데이터(chunk)를 읽어 AMF 포맷을 파싱
	//handleRTMPChunkStream(conn)

	handleClient(conn)
}

func main() {
	listener, err := net.Listen("tcp", ":1935")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("1935 포트에서 RTMP 대기 중...")

	for {
		// 클라의 연결 요청 수락, 해당 클라와 통신할 수 있는 새로운 소켓 반환
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleRTMP(conn)
	}
}

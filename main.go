package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
)

// AMF0 문자열을 읽어오는 헬퍼

func readAMF0String(r io.Reader) (string, error) {
	marker := make([]byte, 1)
	if _, err := io.ReadFull(r, marker); err != nil {
		return "", err
	}

	// 0x02가 아니면 AMF0 String이 아님
	if marker[0] != 0x02 {
		return "", fmt.Errorf("AMF0 String marker(0x02)가 아닙니다: 0x%02x", marker[0])
	}

	// 그 뒤 2bytes는 문자열의 길이 (BigEndian)
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
			// 비디오 데이터 처리
			fmt.Printf("🎥 비디오 패킷 수신: %d bytes\n", msgLength)
			io.CopyN(io.Discard, conn, int64(msgLength)) // 일단은 버리기
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
	handleRTMPChunkStream(conn)
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

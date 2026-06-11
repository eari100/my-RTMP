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
		if bfmt == 0 {
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
		} else {
			// fmt가 1,2,3 일 때는 이전 헤더 정보를 재사용하므로 간략화된 헤더가 옵니다.
			// 로우 레벨 구현 시에는 이전 상태를 구조체에 저장하고 매칭해야 됩니다.
			fmt.Printf("간략화된 청크 포맷(fmt:%d)입니다. 일단 생략하고 다음 바이트로 이동합니다.\n", bfmt)
			continue
		}

		// 3. 메서지 타입별 처리
		switch msgTypeID {
		case 1:
			// Protocol Control Message: Set Chunk Size
			// OBS가 청크 4096바이트 단위로 쪼개서 크기를 바꾸겠다는 패킷
			sizeBuf := make([]byte, 4)
			io.ReadFull(conn, sizeBuf)
			chunkSize = int(binary.BigEndian.Uint32(sizeBuf))
			fmt.Printf("OBS가 청크 크기를 변경함: %d bytes\n", chunkSize)

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

			if cmdName == "publish" {
				fmt.Println("publish 명령어 찾았습니다. 스트림 키 추출")
				for {
					str, err := readAMF0String(msgReader)
					if err != nil {
						// 더 이상 읽을 String이 없으면 종료
						break
					}

					// 'live' 같은 애플리케이션 이름 뒤에 오는 고유 문자열이 스트림 키입니다.
					if str != "" && str != "live" {
						fmt.Println("최종 추출된 스트림 키: %s\n", str)
						return
					}
				}
			} else {
				// 다른 명령어(connect 등)일 때는 남은 메세지 바이트를 비워줌
				// 그래야 다음 청크를 읽을 수 있음
				io.Copy(io.Discard, msgReader)
			}

		default:
			// 오디오(8), 비디오(9) 등 파싱 필요 없는건 건너뜀
			io.CopyN(io.Discard, conn, int64(msgLength))
			fmt.Printf("미디어/기타 데이터 %d 바이트 패스\n", msgLength)
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
	// todo: 바이너리 데이터(chunk)를 읽어 AMF 포맷을 파싱
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

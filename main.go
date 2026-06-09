package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
)

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

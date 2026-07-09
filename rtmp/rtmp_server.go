package rtmp

import (
	"log"
	"net"
)

func Serve() {
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
			roomHub := NewHub()
			go roomHub.Run()

			session := &StreamSession{
				Conn:         conn,
				ChunkSize:    128,
				ChunkStreams: make(map[uint32]*ChunkStream),
				Hub:          roomHub,
			}

			session.Handle()
		}(conn)
	}
}

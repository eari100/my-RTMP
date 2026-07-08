package chat

import (
	"fmt"
	"log"
	"math/rand"
	"my-RTMP/auth"
	"my-RTMP/database"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true }, // 테스트 용도로 전체 허용
}

func GenerateGuestName() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	guestNum := r.Intn(9000) + 1000
	return fmt.Sprintf("손님#%d", guestNum)
}

func ServeChatWs(w http.ResponseWriter, r *http.Request) {
	streamerID := r.URL.Query().Get("room")
	if streamerID == "" {
		http.Error(w, "방송방 ID가 필요합니다.", http.StatusBadRequest)
		return
	}

	// 현재 접속한 방이 존재하는 지 확인
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM rooms WHERE id = ? AND is_live = true)"
	err := database.DB.QueryRow(query, streamerID).Scan(&exists)

	if err != nil {
		log.Println("방 존재 여부 조회 중 DB 에러:", err)
		http.Error(w, "서버 내부 오류", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "존재하지 않는 방송방입니다.", http.StatusNotFound)
		return
	}

	user := auth.GetLoggedInUser(r)
	nickname := ""
	isMaster := false
	if user.ID == "" {
		nickname = GenerateGuestName()
	} else {
		nickname = user.Name

		if user.ID == streamerID {
			isMaster = true
		}
	}

	// 해당 방 hub 가져오기
	GlobalChatManager.mu.Lock()
	hub, ok := GlobalChatManager.Hubs[streamerID]

	if !ok {
		hub = NewHub(streamerID)
		GlobalChatManager.Hubs[streamerID] = hub
		go hub.Run()
	}
	GlobalChatManager.mu.Unlock()

	// http -> websocket 업그레이드
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("채팅 웹소켓 업그레이드 실패", err)
		return
	}

	// 시청자 해당 방에 등록
	client := &Client{
		Hub:      hub,
		Conn:     conn,
		Send:     make(chan []byte, 256),
		Nickname: nickname,
		IsMaster: isMaster,
	}
	// 임계구역은 Hubs (Map)뿐이다.
	// client는 별개의 메모리 파이프라인이라 unlock 밖에서 작업해준다.
	// unlock 안에 작성하면 오히려 Register 작업 중 문제가 생기면 deadlock 이 걸린다.
	// ㄴ이것은 예측이고 실제로는 테스트해봐야 된다.
	client.Hub.Register <- client

	// 채팅 읽기 쓰기 고루틴
	go client.WritePump()
	go client.ReadPump()
}

func InitChatHanders() {
	http.HandleFunc("GET /ws/chat", ServeChatWs)
}

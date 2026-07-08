package chat

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"log"
)

type ChatMessage struct {
	Nickname string `json:"nickname"`
	Message  string `json:"message"`
	IsMaster bool   `json:"is_master"`
}

// 시청자 1명
type Client struct {
	Hub      *Hub
	Conn     *websocket.Conn
	Send     chan []byte
	Nickname string
	IsMaster bool
}

// 채팅방 1개에 대한 허브
type Hub struct {
	StreamerID string
	Clients    map[*Client]bool // 중복 체크
	Broadcast  chan []byte
	Register   chan *Client
	Unregister chan *Client
}

func NewHub(streamerID string) *Hub {
	return &Hub{
		StreamerID: streamerID,
		Clients:    make(map[*Client]bool),
		Broadcast:  make(chan []byte),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
	}
}

func (h *Hub) Run() {
	for {
		select {
		// 입장
		case client := <-h.Register:
			h.Clients[client] = true
		// 퇴장
		case client := <-h.Unregister:
			if _, ok := h.Clients[client]; ok {
				delete(h.Clients, client)
				close(client.Send)
			}
		// 메시지 발송
		case message := <-h.Broadcast:
			for client := range h.Clients {
				select {
				case client.Send <- message:
				default:
					close(client.Send)
					delete(h.Clients, client)
				}
			}
		}
	}
}

// 채팅을 Broadcast에 넣음
func (c *Client) ReadPump() {
	defer func() {
		c.Hub.Unregister <- c
		c.Conn.Close()
	}()

	for {
		_, msg, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}

		chatMsg := ChatMessage{
			Nickname: c.Nickname,  // 웹소켓 연결 시 부여받은 닉네임!
			Message:  string(msg), // 사용자가 입력한 대화 내용
			IsMaster: c.IsMaster,  // 방장 여부
		}

		jsonBytes, err := json.Marshal(chatMsg)
		if err != nil {
			log.Println("JSON 마샬링 실패:", err)
			continue
		}

		c.Hub.Broadcast <- jsonBytes
	}
}

// Broadcast -> 브라우저
func (c *Client) WritePump() {
	defer func() {
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.Conn.WriteMessage(websocket.TextMessage, message)
		}
	}
}

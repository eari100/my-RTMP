package rtmp

import "log"

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

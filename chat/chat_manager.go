package chat

import (
	"sync"
)

type ChatManager struct {
	mu   sync.Mutex
	Hubs map[string]*Hub
}

var GlobalChatManager = &ChatManager{
	Hubs: make(map[string]*Hub),
}

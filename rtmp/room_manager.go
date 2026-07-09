package rtmp

import (
	"sync"
)

type RoomManager struct {
	sync.RWMutex
	Rooms map[string]*StreamSession
}

// todo: 실시간으로 manager 상태를 볼 수 있는 기능이 있으면 좋을듯
var GlobalRoomManager = &RoomManager{
	Rooms: make(map[string]*StreamSession),
}

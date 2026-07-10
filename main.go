package main

import (
	"log"
	"my-RTMP/auth"
	"my-RTMP/chat"
	"my-RTMP/database"
	"my-RTMP/file"
	"my-RTMP/rtmp"
	"my-RTMP/view"
	"net/http"
)

func main() {
	database.Connect()
	auth.InitOAuthHandlers()
	file.FileRegisterRoutes()

	chat.InitChatHanders()
	rtmp.InitRoomHandlers()
	view.InitViewHandlers()

	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("HTTP 서버 가동 실패: %v", err)
		}
	}()
	log.Println("8080 포트에서 웹서버 대기 중")

	rtmp.Serve()
}

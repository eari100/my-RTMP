package rtmp

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"my-RTMP/auth"
	"my-RTMP/database"
	"net/http"
	"strings"
)

// live sse 체크 chan
var LiveStatusChan = make(chan bool)

// 추후 flv 패키지 안으로 넎는 것을 고려해볼 것.. 안해도 되고 뭐
func playerHandle(w http.ResponseWriter, r *http.Request, s *StreamSession) {
	log.Printf("[HTTP] 새로운 시청자가 브라우저로 접속했습니다: %s", r.RemoteAddr)

	if s == nil || s.Hub == nil {
		http.Error(w, "방송이 오프라인 상태", http.StatusNotFound)
		return
	}

	// CORS 방화벽 해제
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	// HTTP 연결을 "실시간 스트리밍 모드(Chunked Transfer Encoding) 전환
	// 서버가 전송하는 걸 받도록 설정
	w.Header().Set("Content-Type", "video/x-flv")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	// 9 bytes header +4 bytes PreviousTagSize0 = 13 bytes
	flvHeader := []byte{
		0x46, 0x4c, 0x56, 0x01, // 'F', 'L' 'V' Version 1
		0x05,                   // Audio + Video
		0x00, 0x00, 0x00, 0x09, // header size (9 bytes)
		0x00, 0x00, 0x00, 0x00, // PreviousTagSize0 (4 bytes)
	}
	w.Write(flvHeader)

	// 8 bytes 시청자 주소 파이프
	myChan := make(chan []byte, 512)

	s.Hub.Register <- myChan

	// 브라우저 종료
	defer func() {
		s.Hub.Unregister <- myChan
	}()

	// 1. 메타데이터 (해상도, 코덱)
	if len(s.Metadata) > 0 {
		w.Write(s.Metadata)
	}

	// 비디오 설정 (SPS/PPS)
	if len(s.VideoSequenceHeader) > 0 {
		w.Write(s.VideoSequenceHeader)
	}

	// AAC Config
	if len(s.AudioSequenceHeader) > 0 {
		w.Write(s.AudioSequenceHeader)
	}

	// 브라우저로 video, audio 규격 flush
	if flush, ok := w.(http.Flusher); ok {
		flush.Flush()
	}

	for {
		select {
		case data := <-myChan:
			_, err := w.Write(data)
			if err != nil {
				return // 시청자가 나가면 루프 탈출
			}
			// 즉시 인터넷선으로 밀어내기
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		case <-r.Context().Done():
			return // 브라우저 창 닫으면 탈출
		}
	}
}

func InitRoomHandlers() {
	http.HandleFunc("/live/", func(w http.ResponseWriter, r *http.Request) {
		roomName := strings.TrimPrefix(r.URL.Path, "/live/")

		if roomName == "" {
			http.Error(w, "방 이름을 입력해주세요.", http.StatusBadRequest)
			return
		}

		GlobalRoomManager.RLock() // BJ Rock
		targetSession, exists := GlobalRoomManager.Rooms[roomName]
		GlobalRoomManager.RUnlock()

		if !exists || targetSession == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "현재 방송 중이 아니거나 유효한 방이 아닙니다."})
			return
		}

		playerHandle(w, r, targetSession)
	})

	http.HandleFunc("GET /api/rooms/my", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		loggedInUser := auth.GetLoggedInUser(r)
		if loggedInUser.ID == "" {
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "로그인이 필요합니다."})
			return
		}

		var room struct {
			Title     string `json:"title"`
			StreamKey string `json:"stream_key"`
			is_live   bool   `json:"is_live"`
		}

		err := database.DB.QueryRow("SELECT title, stream_key, is_live FROM rooms WHERE id = ?", loggedInUser.ID).
			Scan(&room.Title, &room.StreamKey, &room.is_live)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "데이터베이스 조회 실패"})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(room)
	})

	// 방 데이터 수정
	http.HandleFunc("POST /api/rooms/my", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		loggedInUser := auth.GetLoggedInUser(r)
		if loggedInUser.ID == "" {
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "로그인이 필요합니다."})
			return
		}

		var requestData struct {
			Title string `json:"title"`
		}

		err := json.NewDecoder(r.Body).Decode(&requestData)
		if err != nil || requestData.Title == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "잘못된 요청 데이터입니다."})
			return
		}

		_, err = database.DB.Exec("UPDATE rooms SET title = ? WHERE id = ?", requestData.Title, loggedInUser.ID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "데이터베이스 수정 실패"})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"message": "방송 제목이 성공적으로 변경되었습니다."})
	})

	http.HandleFunc("POST /api/rooms/my/reset-key", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		loggedInUser := auth.GetLoggedInUser(r)
		if loggedInUser.ID == "" {
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "로그인이 필요합니다."})
			return
		}

		type ResetKeyResponse struct {
			StreamKey string `json:"stream_key"`
		}

		newKey := generateStreamKey()

		// todo : 기존 연결 끊어주기

		_, err := database.DB.Exec("UPDATE rooms SET stream_key = ? WHERE user_id = ?", newKey, loggedInUser.ID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "데이터베이스 수정 실패"})
			return
		}

		fmt.Println("새로 발급된 스트림 키:", newKey)

		resp := ResetKeyResponse{StreamKey: newKey}
		json.NewEncoder(w).Encode(resp)
	})

	// 메인화면 방송 리스트 조회
	http.HandleFunc("GET /api/rooms/live", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		isLiveParam := r.URL.Query().Get("is_live")
		targetStatus := true
		if isLiveParam == "false" {
			targetStatus = false
		}

		// 🎯 닉네임과 프로필 사진을 담을 필드 추가
		var rooms []struct {
			UserID   string `json:"user_id"`
			Title    string `json:"title"`
			IsLive   bool   `json:"is_live"`
			UserName string `json:"user_name"`
			Picture  string `json:"picture"`
		}

		query := `
        SELECT r.id, r.title, r.is_live, u.name, u.picture 
        FROM rooms r
        INNER JOIN users u ON r.id = u.id
        WHERE r.is_live = ?
    `

		rows, err := database.DB.Query(query, targetStatus)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "데이터베이스 조회 실패"})
			return
		}
		defer rows.Close()

		for rows.Next() {
			var room struct {
				UserID   string `json:"user_id"`
				Title    string `json:"title"`
				IsLive   bool   `json:"is_live"`
				UserName string `json:"user_name"`
				Picture  string `json:"picture"`
			}

			if err := rows.Scan(&room.UserID, &room.Title, &room.IsLive, &room.UserName, &room.Picture); err != nil {
				continue
			}
			rooms = append(rooms, room)
		}

		if rooms == nil {
			rooms = []struct {
				UserID   string `json:"user_id"`
				Title    string `json:"title"`
				IsLive   bool   `json:"is_live"`
				UserName string `json:"user_name"`
				Picture  string `json:"picture"`
			}{}
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(rooms)
	})

	// 라이브 중인지 알려주는 SSE
	http.HandleFunc("/api/rooms/my/stream-status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// 브라우저 새로 고침 감지
		ctx := r.Context()

		loggedInUser := auth.GetLoggedInUser(r)
		GlobalRoomManager.RLock()
		_, isRunning := GlobalRoomManager.Rooms[loggedInUser.ID]
		GlobalRoomManager.RUnlock()

		// 연결 직후 상태 체크
		fmt.Fprintf(w, "data: {\"is_live\": %t}\n\n", isRunning)
		w.(http.Flusher).Flush()

		for {
			select {
			case <-ctx.Done():
				// 브라우저가 나감 (연결 종료)
				return
			case isLive := <-LiveStatusChan:
				// 방송 상태가 바뀌면 이쪽으로 데이터가 들어옴
				fmt.Fprintf(w, "data: {\"is_live\": %t}\n\n", isLive)
				w.(http.Flusher).Flush() // 바로 브라우저로 밀어버리기
			}
		}
	})

}

func generateStreamKey() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("live_%x", b) // 예: live_a1b2c3d4e5f6...
}

package view

import (
	"database/sql"
	"fmt"
	"html/template"
	"log"
	"my-RTMP/auth"
	"my-RTMP/database"
	"net/http"
	"strings"
)

func InitViewHandlers() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		tmpl, err := template.ParseFiles("view/index.html", "view/header.html", "view/global_css.html")
		if err != nil {
			http.Error(w, "템플릿 로드 에러", http.StatusInternalServerError)
			return
		}

		data := struct {
			User interface{}
		}{
			User: auth.GetLoggedInUser(r),
		}

		tmpl.Execute(w, data)
	})

	http.HandleFunc("/watch/{id}", func(w http.ResponseWriter, r *http.Request) {
		watchID := r.PathValue("id")

		w.Header().Set("Content-Type", "text/html; charset=utf-8")

		if watchID == "" || strings.TrimSpace(watchID) == "" {

			w.WriteHeader(http.StatusBadRequest) // 400 Bad Request

			fmt.Fprint(w, `
            <script>
                alert("잘못된 접근입니다. 올바른 방송 ID를 입력해주세요. 🧐");
                window.location.href = "/";
            </script>
        `)
			return
		}

		tmpl, tmplErr := template.ParseFiles("view/watch.html", "view/header.html", "view/global_css.html")
		if tmplErr != nil {
			log.Println("🚨 시청 화면 템플릿 파싱 실패:", tmplErr)
			http.Error(w, "시청 화면을 로드하는 중 오류가 발생했습니다.", http.StatusInternalServerError)
			return
		}

		var title string
		var isLive bool
		var createdAt, startedAt sql.NullTime

		query := "SELECT title, is_live, created_at, started_at FROM rooms WHERE id = ?"
		dbErr := database.DB.QueryRow(query, watchID).Scan(&title, &isLive, &createdAt, &startedAt)

		if dbErr != nil {
			log.Printf("🚨 방송 정보 조회 실패 (ID: %s): %v", watchID, dbErr)
			http.Error(w, "존재하지 않는 방송입니다.", http.StatusNotFound)
			return
		}

		if !isLive {
			fmt.Fprint(w, `<h1>방송이 종료되었습니다.</h1><a href="/">메인으로 돌아가기</a>`)
			return
		}

		data := struct {
			User  interface{}
			Title string
			ID    string
		}{
			User:  auth.GetLoggedInUser(r),
			Title: title,
			ID:    watchID,
		}

		tmplErr = tmpl.Execute(w, data)

		if tmplErr != nil {
			log.Println("🚨 시청 화면 렌더링 실패:", tmplErr)
			http.Error(w, "화면 표시 실패", http.StatusInternalServerError)
			return
		}
	})

	http.HandleFunc("GET /studio", func(w http.ResponseWriter, r *http.Request) {
		tmpl, _ := template.ParseFiles("view/studio.html", "view/global_css.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")

		data := struct {
			User interface{}
		}{
			User: auth.GetLoggedInUser(r),
		}

		tmpl.Execute(w, data)
	})

}

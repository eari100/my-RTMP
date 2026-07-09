package view

import (
	"fmt"
	"html/template"
	"log"
	"my-RTMP/auth"
	"net/http"
	"strings"
)

func InitViewHandlers() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		loggedInUser := auth.GetLoggedInUser(r)

		tmpl, err := template.ParseFiles("view/index.html", "view/header.html", "view/global_css.html")
		if err != nil {
			http.Error(w, "템플릿 로드 에러", http.StatusInternalServerError)
			return
		}

		tmpl.Execute(w, loggedInUser)
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

		tmpl, err := template.ParseFiles("view/watch.html", "view/header.html", "view/global_css.html")
		if err != nil {
			log.Println("🚨 시청 화면 템플릿 파싱 실패:", err)
			http.Error(w, "시청 화면을 로드하는 중 오류가 발생했습니다.", http.StatusInternalServerError)
			return
		}

		loggedInUser := auth.GetLoggedInUser(r)

		err = tmpl.Execute(w, loggedInUser)
		if err != nil {
			log.Println("🚨 시청 화면 렌더링 실패:", err)
			http.Error(w, "화면 표시 실패", http.StatusInternalServerError)
			return
		}
	})

	http.HandleFunc("GET /studio", func(w http.ResponseWriter, r *http.Request) {
		tmpl, _ := template.ParseFiles("view/studio.html", "view/global_css.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")

		tmpl.Execute(w, auth.GetLoggedInUser(r))
	})

}

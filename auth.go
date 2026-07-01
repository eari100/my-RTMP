package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/joho/godotenv"
)

// todo: 쿠키 기반의 동적 state 발급
// CSRF 공격 방지
const oauthStateString = "random_state_string_for_jw-tv"

func initOAuthHandlers() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("⚠️ .env 파일을 로드하는 데 실패했습니다. 파일 위치를 확인하세요.")
	}

	googleOauthConfig := &oauth2.Config{
		RedirectURL:  "http://localhost:8080/auth/google/callback",
		ClientID:     os.Getenv("GOOGLE_CLIENT_ID"),
		ClientSecret: os.Getenv("GOOGLE_CLIENT_SECRET"),
		Scopes:       []string{"https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile"},
		Endpoint:     google.Endpoint,
	}

	// 구글 로그인 리다이렉트 핸들러
	http.HandleFunc("/auth/google/login", func(w http.ResponseWriter, r *http.Request) {
		url := googleOauthConfig.AuthCodeURL(oauthStateString)
		http.Redirect(w, r, url, http.StatusTemporaryRedirect)
	})

	// 구글 로그인 콜백 핸들러
	http.HandleFunc("/auth/google/callback", func(w http.ResponseWriter, r *http.Request) {
		if r.FormValue("state") != oauthStateString { // state 검증
			log.Println("유효하지 않은 OAuth state 값입니다.")
			http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
			return
		}

		code := r.FormValue("code")
		token, err := googleOauthConfig.Exchange(context.Background(), code)
		if err != nil {
			http.Error(w, "구글 토큰 교환 실패: "+err.Error(), http.StatusInternalServerError)
			return
		}

		resp, err := http.Get("https://www.googleapis.com/oauth2/v2/userinfo?access_token=" + token.AccessToken)
		if err != nil {
			http.Error(w, "유저 정보 가져오기 실패: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		data, _ := io.ReadAll(resp.Body)

		fmt.Fprintf(w, "구글 로그인 성공! 유저 정보: %s", string(data))
	})
}

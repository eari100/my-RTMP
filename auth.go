package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"

	"github.com/google/uuid"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/joho/godotenv"
)

var db *sql.DB

type GoogleUser struct {
	ID            string `json:"id"`
	Email         string `json:"email"`
	VerifiedEmail bool   `json:"verified_email"`
	Name          string `json:"name"`
	Picture       string `json:"picture"`
}

// todo: 쿠키 기반의 동적 state 발급
// CSRF 공격 방지
const oauthStateString = "random_state_string_for_jw-tv"

func initOAuthHandlers() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("⚠️ .env 파일을 로드하는 데 실패했습니다. 파일 위치를 확인하세요.")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("🚨 MySQL 드라이버 초기화 실패:", err)
	}

	// 🎯 [추가] 2. 연결이 진짜 잘 됐는지 네트워크 핑을 때려봅니다.
	err = db.Ping()
	if err != nil {
		log.Fatal("🚨 MySQL 데이터베이스 접속 실패 (Ping 에러):", err)
	}
	log.Println("🐬 MySQL 데이터베이스 연결 성공!")

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

		// 구글 파싱
		var googleUser GoogleUser
		if err := json.Unmarshal(data, &googleUser); err != nil {
			http.Error(w, "JSON 파싱 실패", http.StatusInternalServerError)
			return
		}

		// 이메일 조회
		var userId string
		var userEmail string

		err = db.QueryRow("SELECT id, email FROM users WHERE email = ?", googleUser.Email).Scan(&userId, &userEmail)

		if err == sql.ErrNoRows {
			log.Println("🆕 새로운 유저 발견! 회원가입을 진행합니다:", googleUser.Email)

			userId = uuid.New().String()

			query := "INSERT INTO users (id, email, name, picture) VALUES (?, ?, ?, ?)"

			_, err := db.Exec(query, userId, googleUser.Email, googleUser.Name, googleUser.Picture)
			if err != nil {
				log.Println("🚨 회원가입 DB 저장 실패:", err)
				http.Error(w, "회원가입 실패", http.StatusInternalServerError)
				return
			}
			log.Println("🎉 회원가입 완료! 생성된 UUID:", userId)
		} else if err != nil {
			log.Println("🚨 DB 조회 중 에러 발생:", err)
			http.Error(w, "DB 오류", http.StatusInternalServerError)
			return
		} else {
			log.Println("🔑 기존 유저 로그인 성공:", userEmail, " (ID:", userId, ")")
		}

		//fmt.Fprintf(w, "처리 완료! 로그인된 유저 UUID: %s, 이메일: %s", userId, googleUser.Email)
	})
}

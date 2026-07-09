package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/golang-jwt/jwt/v5"
	"io"
	"log"
	"my-RTMP/database"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/google/uuid"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

var jwtKey []byte

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

func InitOAuthHandlers() {
	googleOauthConfig := &oauth2.Config{
		RedirectURL:  "http://localhost:8080/auth/google/callback",
		ClientID:     os.Getenv("GOOGLE_CLIENT_ID"),
		ClientSecret: os.Getenv("GOOGLE_CLIENT_SECRET"),
		Scopes:       []string{"https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile"},
		Endpoint:     google.Endpoint,
	}

	// 구글 로그인 리다이렉트 핸들러
	http.HandleFunc("/auth/google/login", func(w http.ResponseWriter, r *http.Request) {
		returnURL := r.Referer()
		if returnURL == "" {
			returnURL = "/"
		}

		http.SetCookie(w, &http.Cookie{
			Name:     "return_to",
			Value:    returnURL,
			Path:     "/",
			MaxAge:   300, // 5분동안 유효
			HttpOnly: true,
		})

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
		// todo: 객체에 합칠까?
		var userId string
		var userEmail string

		err = database.DB.QueryRow("SELECT id, email FROM users WHERE email = ?", googleUser.Email).Scan(&userId, &userEmail)

		if err == sql.ErrNoRows {
			log.Println("🆕 새로운 유저 발견! 회원가입을 진행합니다:", googleUser.Email)

			userId = uuid.New().String()
			query := "INSERT INTO users (id, email, name, picture) VALUES (?, ?, ?, ?)"
			_, err := database.DB.Exec(query, userId, googleUser.Email, googleUser.Name, googleUser.Picture)

			if err != nil {
				log.Println("🚨 회원가입 DB 저장 실패:", err)
				http.Error(w, "회원가입 실패", http.StatusInternalServerError)
				return
			}

			log.Println("🎉 회원가입 완료! 생성된 UUID:", userId)

			// 스트림키 생성
			streamKey := "live_" + uuid.New().String()

			// 초기 제목 세팅
			defaultTitle := googleUser.Name + "님의 방송국"

			// rooms 테이블 삽입
			roomQuery := "INSERT INTO rooms (id, user_id, stream_key, title, is_live) VALUES (?, ?, ?, ?, FALSE)"
			_, roomErr := database.DB.Exec(roomQuery, userId, userId, streamKey, defaultTitle)

			if roomErr != nil {
				log.Println("🚨 회원가입은 되었으나, 최초 방 생성 실패:", roomErr)
				http.Error(w, "방송방 생성 실패", http.StatusInternalServerError)
				return
			}

			log.Println("📺 유저의 방송 고유 방 생성 완료! 스트림키 발급 완료.")

		} else if err != nil {
			log.Println("🚨 DB 조회 중 에러 발생:", err)
			http.Error(w, "DB 오류", http.StatusInternalServerError)
			return
		} else {
			log.Println("🔑 기존 유저 로그인 성공:", userEmail, " (ID:", userId, ")")
		}
		expirationTime := time.Now().Add(24 * time.Hour) // 토큰 유효기간: 24시간

		claims := jwt.MapClaims{
			"user_id":   userId,
			"user_name": googleUser.Name,
			"picture":   googleUser.Picture,
			"email":     googleUser.Email,
			"exp":       expirationTime.Unix(), // 만료 시간 초 단위 등록
		}

		jwtToken := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		jwtKey = []byte(os.Getenv("JWT_KEY"))
		tokenString, err := jwtToken.SignedString(jwtKey)
		if err != nil {
			log.Println("JWT 서명 실패", err)
			http.Error(w, "토큰 생성 실패", http.StatusInternalServerError)
			return
		}

		http.SetCookie(w, &http.Cookie{
			Name:     "access_token",
			Value:    tokenString,
			Path:     "/",
			MaxAge:   86400, // 24 시간
			HttpOnly: true,  // js 접근 x
		})

		finalURL := "/"
		if cookie, err := r.Cookie("return_to"); err == nil {
			finalURL = cookie.Value

			// 다 쓰고 폐기
			http.SetCookie(w, &http.Cookie{
				Name:     "return_to",
				Value:    "",
				Path:     "/",
				MaxAge:   -1,
				HttpOnly: true,
			})

			http.Redirect(w, r, finalURL, http.StatusTemporaryRedirect)
		}
	})

	http.HandleFunc("/auth/logout", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{
			Name:     "access_token",
			Value:    "",
			Path:     "/",
			MaxAge:   -1, // 즉시 삭제
			HttpOnly: true,
		})

		http.Redirect(w, r, "/", http.StatusSeeOther)
	})
}

func GetLoggedInUser(r *http.Request) *GoogleUser {
	tokenCookie, err := r.Cookie("access_token")
	defaultUser := &GoogleUser{}

	if err != nil || tokenCookie == nil {
		return defaultUser
	}

	// 토큰 검증 및 파싱
	token, parseErr := jwt.Parse(tokenCookie.Value, func(token *jwt.Token) (interface{}, error) {
		return jwtKey, nil // main.go 등에 정의된 전역 jwtKey 사용
	})

	if parseErr != nil || token == nil || !token.Valid {
		return defaultUser
	}

	// 토큰 안의 유저 데이터 꺼내기
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return defaultUser
	}

	userId, _ := claims["user_id"].(string)
	userName, _ := claims["user_name"].(string)
	picture, _ := claims["picture"].(string)
	email, _ := claims["email"].(string)

	return &GoogleUser{
		ID:      userId,
		Name:    userName,
		Picture: picture,
		Email:   email,
	}
}

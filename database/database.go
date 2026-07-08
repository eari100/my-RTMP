package database

import (
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"os"
)

var DB *sql.DB

func Connect() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("database: .env 파일을 로드하는 데 실패했습니다. 환경변수를 확인하세요.")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)

	DB, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("MySQL 드라이버 초기화 실패:", err)
	}

	err = DB.Ping()
	if err != nil {
		log.Fatal("MySQL 데이터베이스 접속 실패 (Ping 에러):", err)
	}
	log.Println("MySQL 데이터베이스 연결 성공!")
}

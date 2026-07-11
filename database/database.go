package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
)

var DB *sql.DB

func Connect() {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)

	var dbErr error
	DB, dbErr = sql.Open("mysql", dsn)
	if dbErr != nil {
		log.Fatal("MySQL 드라이버 초기화 실패:", dbErr)
	}

	dbErr = DB.Ping()
	if dbErr != nil {
		log.Fatal("MySQL 데이터베이스 접속 실패 (Ping 에러):", dbErr)
	}
	log.Println("MySQL 데이터베이스 연결 성공!")
}

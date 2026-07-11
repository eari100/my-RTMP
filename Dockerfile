# 1. 가벼운 Go 언어 이미지를 가져옵니다.
FROM golang:alpine

# 기존의 개별 COPY 명령어들을 지우고 아래처럼 수정하세요
WORKDIR /app

# 1. 모듈 파일 먼저 복사
COPY go.mod go.sum ./

RUN go mod tidy
RUN go mod download

# 2. 프로젝트의 모든 파일과 폴더를 한 번에 복사
COPY . .

# 3. 빌드
RUN go build -o main .

CMD ["./main"]
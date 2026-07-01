# 1. 가벼운 Go 언어 이미지를 가져옵니다.
FROM golang:1.21-alpine

# 2. 컨테이너 안에서 작업을 진행할 폴더를 만듭니다.
WORKDIR /app

# 3. 소스 코드를 컨테이너 안으로 복사합니다.
COPY . .

# 4. Go 프로그램을 빌드합니다.
RUN go build -o main .

# 5. 실행할 명령어를 지정합니다.
CMD ["./main"]
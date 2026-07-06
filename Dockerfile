# 1. 가벼운 Go 언어 이미지를 가져옵니다.
FROM golang:alpine

# 2. 컨테이너 안에서 작업을 진행할 폴더를 만듭니다.
WORKDIR /app

# 3. 소스 코드를 컨테이너 안으로 복사합니다.
COPY *.go ./
COPY view/index.html ./
COPY view/watch.html ./
COPY go.mod go.sum* ./

# 의존성 다운로드
RUN go mod tidy
RUN go mod download

# 4. Go 프로그램을 빌드합니다.
RUN go build -o main .

# 5. 실행할 명령어를 지정합니다.
CMD ["./main"]
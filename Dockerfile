FROM golang:alpine

# 나중에 ffmpeg 버전 확인
# docker-compose exec backend ffmpeg -version
# 폰트 추가
RUN apk add --no-cache ffmpeg ttf-dejavu

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod tidy
RUN go mod download

COPY . .

# 3. 빌드
RUN go build -o main .

CMD ["./main"]
# performance

### 동시 송출 500개 (성공)
### (썸네일 추출 로직만 제외)
| 단계              |  CPU 점유율   |   메모리 사용량   |      네트워크 (Inbound I/O)       |
|:----------------|:----------:|:-----------:|:-----------------------------:|
| **Idle**   |    6~9%    |   15.6 GB   |          0~200 Mbps           |
| **Peak** | **최고 89%** |   19.6 GB   |    **1,009 Mbps (1 Gbps)**    |
| **Steady**      | **65~70%** | **18.3 GB** | **602 ~ 1,009 Mbps (1 Gbps)** |


### os: windows 10 Enterprise 64 bit (10.0, build 19045)
### device: LG Electronics 15UD50P-KX90X
### CPU: 11th Gen Intel(R) Core(TM) i7-1165G7
### memory: 32GB
### GPU: NVIDIA GeForce MX450

## start.ps1
```azure
$StreamCount = 500
$VideoFile = "sample.mp4"
# RTMP 서버 주소
$RtmpUrl = "rtmp://localhost/live"

Write-Host "rtmp start: $StreamCount 개 시작..." -ForegroundColor Green

for ($i = 1; $i -le $StreamCount; $i++) {
    # -WindowStyle Hidden: FFmpeg 검은색 창 50개가 화면을 뒤덮지 않도록 숨겨서 실행합니다.
    Start-Process -FilePath "ffmpeg" -ArgumentList "-re -stream_loop -1 -nostdin -i `"$VideoFile`" -c:v copy -c:a copy -f flv `"$RtmpUrl/stream_$i`"" -WindowStyle Hidden

    # 서버와 CPU 부담을 줄이기 위해 0.1초 간격으로 순차 실행
    Start-Sleep -Milliseconds 100
}

Write-Host "500 stream finish (stream_1 ~ stream_500)" -ForegroundColor Cyan

```
## stop.ps1
```azure
Write-Host "FFmpeg finish..." -ForegroundColor Yellow

# 백그라운드에서 실행 중인 ffmpeg 프로세스를 모두 작업 표시줄에서 내리고 강제 종료합니다.
Stop-Process -Name "ffmpeg" -Force -ErrorAction SilentlyContinue

Write-Host "done" -ForegroundColor Green

```


# todo

## 영상 재생 시 음성 나오도록/백그라운드에서도 재생 잘 되도록
## 방송 해상도 변경
## obs 이외에 프로그램 지원
## HLS 지원 및 기존 flv와의 양다리 파이프라인
## 후원 기능
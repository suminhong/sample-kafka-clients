package main

import (
	"fmt"
	"html/template"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// 전역 변수로 Producer 상태 관리
var (
	producer     *kafka.Producer
	isRunning    bool
	runningMutex sync.Mutex
	counter      int
	topic        string
)

// 웹 페이지 HTML 템플릿
const htmlTemplate = `
<!DOCTYPE html>
<html>
<head>
    <title>Kafka Producer 컨트롤러</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; }
        .container { max-width: 800px; margin: 0 auto; }
        .status { margin: 20px 0; padding: 10px; border-radius: 5px; }
        .running { background-color: #d4edda; color: #155724; }
        .stopped { background-color: #f8d7da; color: #721c24; }
        button { padding: 10px 15px; margin-right: 10px; cursor: pointer; }
        .log { height: 300px; overflow-y: scroll; background-color: #f8f9fa; 
               border: 1px solid #ddd; padding: 10px; margin-top: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Kafka Producer 컨트롤러</h1>
        
        <div class="status {{if .IsRunning}}running{{else}}stopped{{end}}">
            현재 상태: {{if .IsRunning}}실행 중{{else}}중지됨{{end}}
        </div>
        
        <div>
            <form method="POST">
                {{if .IsRunning}}
                    <button type="submit" name="action" value="stop">중지</button>
                {{else}}
                    <button type="submit" name="action" value="start">시작</button>
                {{end}}
            </form>
        </div>
        
        <div>
            <h3>설정 정보:</h3>
            <p>Kafka 브로커: {{.BootstrapServers}}</p>
            <p>토픽: {{.Topic}}</p>
            <p>전송된 메시지 수: {{.Counter}}</p>
        </div>
    </div>
</body>
</html>
`

// 웹 페이지에 표시할 데이터 구조체
type PageData struct {
	IsRunning        bool
	BootstrapServers string
	Topic            string
	Counter          int
}

func main() {
	// 환경변수에서 Kafka 브로커 주소와 토픽 이름 가져오기
	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		bootstrapServers = "localhost:9092" // 기본값
	}

	topic = os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "test-topic" // 기본값
	}

	// Producer 설정
	config := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"client.id":         "go-producer",
		"acks":              "all", // 모든 복제본이 메시지를 받았는지 확인
	}

	// Producer 생성
	var err error
	producer, err = kafka.NewProducer(config)
	if err != nil {
		fmt.Printf("Producer 생성 실패: %s\n", err)
		os.Exit(1)
	}

	// 프로그램 종료 시 Producer 닫기
	defer producer.Close()

	// 배달 보고서 채널 모니터링을 위한 고루틴
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				// 메시지 전송 결과 확인
				if ev.TopicPartition.Error != nil {
					fmt.Printf("메시지 전송 실패: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("메시지 전송 성공: 토픽=%s, 파티션=%d, 오프셋=%v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	// 인터럽트 시그널 처리
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// 웹 서버 핸들러 설정
	tmpl := template.Must(template.New("index").Parse(htmlTemplate))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			action := r.FormValue("action")
			runningMutex.Lock()
			if action == "start" && !isRunning {
				isRunning = true
				go produceMessages(bootstrapServers)
			} else if action == "stop" && isRunning {
				isRunning = false
			}
			runningMutex.Unlock()
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}

		// 페이지 데이터 준비
		runningMutex.Lock()
		data := PageData{
			IsRunning:        isRunning,
			BootstrapServers: bootstrapServers,
			Topic:            topic,
			Counter:          counter,
		}
		runningMutex.Unlock()

		// 템플릿 렌더링
		tmpl.Execute(w, data)
	})

	// 웹 서버 시작
	go func() {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080" // 기본 포트
		}
		fmt.Printf("웹 서버 시작: http://localhost:%s\n", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			fmt.Printf("웹 서버 시작 실패: %v\n", err)
			os.Exit(1)
		}
	}()

	// 시그널 대기
	sig := <-sigchan
	fmt.Printf("시그널 수신: %v, 종료합니다...\n", sig)

	// 모든 메시지가 전송될 때까지 대기
	producer.Flush(15 * 1000)
	fmt.Println("Producer 종료")
}

// 메시지 생성 및 전송 함수
func produceMessages(bootstrapServers string) {
	fmt.Println("메시지 전송 시작")

	for {
		runningMutex.Lock()
		if !isRunning {
			runningMutex.Unlock()
			fmt.Println("메시지 전송 중지")
			break
		}

		// 메시지 생성
		counter++
		currentCounter := counter
		runningMutex.Unlock()

		value := fmt.Sprintf("메시지 #%d", currentCounter)

		// 메시지 전송
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(value),
			Key:            []byte(fmt.Sprintf("key-%d", currentCounter)),
		}, nil)

		if err != nil {
			fmt.Printf("메시지 큐잉 실패: %v\n", err)
		} else {
			fmt.Printf("메시지 큐잉 성공: %s\n", value)
		}

		// 잠시 대기
		time.Sleep(1 * time.Second)
	}
}

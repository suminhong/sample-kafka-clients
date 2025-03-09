package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Kafka 브로커 주소 설정
	// 실제 환경에 맞게 수정해주세요
	bootstrapServers := "localhost:9092"

	// 메시지를 받을 토픽 이름
	topic := "test-topic"

	// 컨슈머 그룹 ID
	groupID := "go-consumer-group"

	// Consumer 설정
	config := &kafka.ConfigMap{
		"bootstrap.servers":       bootstrapServers,
		"group.id":                groupID,
		"auto.offset.reset":       "earliest", // 처음부터 메시지 소비
		"enable.auto.commit":      true,       // 자동 커밋 활성화
		"auto.commit.interval.ms": 5000,       // 5초마다 오프셋 커밋
		"session.timeout.ms":      30000,      // 세션 타임아웃
	}

	// Consumer 생성
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Printf("Consumer 생성 실패: %s\n", err)
		os.Exit(1)
	}

	// 프로그램 종료 시 Consumer 닫기
	defer consumer.Close()

	// 토픽 구독
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("토픽 구독 실패: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("토픽 '%s'을(를) 구독합니다...\n", topic)

	// 인터럽트 시그널 처리
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// 메시지 소비 루프
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("시그널 수신: %v, 종료합니다...\n", sig)
			run = false
		default:
			// 메시지 폴링 (100ms 타임아웃)
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				// 메시지 수신
				fmt.Printf("메시지 수신: 토픽=%s, 파티션=%d, 오프셋=%d\n",
					*e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)
				fmt.Printf("키: %s, 값: %s\n", string(e.Key), string(e.Value))

			case kafka.Error:
				// 에러 처리
				fmt.Printf("에러: %v\n", e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}

			default:
				// 기타 이벤트 처리
				fmt.Printf("이벤트 무시: %v\n", e)
			}
		}
	}

	fmt.Println("Consumer 종료")
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
)

const (
	MaterializeUrl = "postgres://materialize@localhost:6875/materialize?sslmode=disable"

	ConsumersCount = 2

	KafkaAddress                        = "localhost:9092"
	KafkaTriggersEventsConsumersGroupId = "triggers_consumer"
	KafkaTriggersTopic                  = "triggers"
)

type SinkResult struct {
	After struct {
		Row TriggerEvent `json:"row"`
	} `json:"after"`
}

type (
	TriggerEvent struct {
		ViewName   string `json:"view_name"`
		SinkName   string `json:"sink_name"`
		WorkflowId string `json:"workflow_id"`
		Body       string `json:"body"`
		Timestamp  int    `json:"timestamp"`
	}

	Counter struct {
		sync.Mutex
		i int
	}
)

func (c *Counter) increase() {
	c.Lock()
	defer c.Unlock()
	c.i++
}

func main() {
	ctx := context.Background()
	conn, err := pgxpool.Connect(ctx, MaterializeUrl)
	if err != nil {
		log.Fatal(err)
	}

	counter := Counter{i: 0}

	defer func() {
		conn.Close()
		log.Println("total triggered:", counter.i)
	}()

	wg := sync.WaitGroup{}
	for i := 0; i < ConsumersCount; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers: []string{KafkaAddress},
				GroupID: KafkaTriggersEventsConsumersGroupId,
				Topic:   KafkaTriggersTopic,
			})

			defer func() {
				if err := r.Close(); err != nil {
					log.Fatal("failed to close reader:", err)
				}
			}()

			log.Println("consumer started")

			for {
				m, err := r.ReadMessage(context.Background())
				if err != nil {
					log.Println(err)
					break
				}

				var result SinkResult
				if err := json.Unmarshal(m.Value, &result); err != nil {
					log.Println(err)
					continue
				}

				viewName := result.After.Row.ViewName
				sinkName := result.After.Row.SinkName

				if err := deleteListener(ctx, conn, viewName, sinkName); err != nil {
					log.Printf("listener [%s %s] deleting error: %s\n", viewName, sinkName, err)
					continue
				}

				log.Printf("listener [%s %s] deleted\n", viewName, sinkName)

				counter.increase()
			}
		}(i)
	}

	wg.Wait()
}

func deleteListener(ctx context.Context, conn *pgxpool.Pool, viewName, sinkName string) error {
	dropViewSQL := fmt.Sprintf(`DROP VIEW %s;`, viewName)

	_, err := conn.Exec(ctx, dropViewSQL)
	if err != nil {
		return err
	}

	dropSinkSQL := fmt.Sprintf(`DROP SINK %s;`, sinkName)

	_, err = conn.Exec(ctx, dropSinkSQL)
	if err != nil {
		return err
	}

	return nil
}

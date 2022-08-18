package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"github.com/semichkin-gopkg/uuid"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	KafkaAddress    = "localhost:9092"
	GoroutinesCount = 16
)

var (
	directions = []string{
		"in",
		"out",
	}
	resources = []uuid.UUID{
		"1a34b742-1ec4-11ed-861d-0242ac120002",
		"2a4aad70-1ec4-11ed-861d-0242ac120002",
	}
	leads = []uuid.UUID{
		"1f486320-1ec4-11ed-861d-0242ac120002",
		"24d36d76-1ec4-11ed-861d-0242ac120002",
	}
	types = []string{
		"tg_send_text",
		"tg_start",
	}
)

type KafkaEvent struct {
	Direction  string         `json:"direction"`
	ResourceId uuid.UUID      `json:"resource_id"`
	LeadId     uuid.UUID      `json:"lead_id"`
	Type       string         `json:"type"`
	ExternalId uuid.UUID      `json:"external_id"`
	Body       map[string]any `json:"body"`
	Timestamp  int64          `json:"timestamp"`
}

func main() {
	w := &kafka.Writer{
		Addr:     kafka.TCP(KafkaAddress),
		Topic:    "events",
		Balancer: &kafka.LeastBytes{},
	}

	defer func() {
		if err := w.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}()

	wg := sync.WaitGroup{}

	for i := 0; i < GoroutinesCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				event := generateEvent()

				jsonEvent, err := json.Marshal(event)
				if err != nil {
					log.Fatalln("failed to marshal event")
				}

				if err := w.WriteMessages(context.Background(),
					kafka.Message{
						Value: jsonEvent,
					},
				); err != nil {
					log.Fatal("failed to write messages:", err)
				}

				log.Println("event produced:", event)
			}
		}()
	}

	wg.Wait()
}

func generateEvent() KafkaEvent {
	direction := getDirection()
	resourceId := getResourceId()
	leadId := getLeadId()
	_type := getType(direction)
	body := getBody(leadId, _type)

	return KafkaEvent{
		Direction:  direction,
		ResourceId: resourceId,
		LeadId:     leadId,
		Type:       _type,
		ExternalId: resourceId,
		Body:       body,
		Timestamp:  time.Now().Unix(),
	}
}

func getDirection() string {
	n := rand.Intn(len(resources) + 10)
	if n <= len(resources)+7 {
		return directions[0]
	}

	return directions[1]
}

func getResourceId() uuid.UUID {
	return resources[rand.Intn(len(resources))]
}

func getLeadId() uuid.UUID {
	return leads[rand.Intn(len(leads))]
}

func getType(direction string) string {
	if direction == "in" {
		return types[rand.Intn(len(types))]
	}
	return types[0]
}

func getBody(leadId uuid.UUID, _type string) map[string]any {
	body := map[string]any{}

	switch _type {
	case "tg_start":
		body["text"] = "/start"
	default:
		body["text"] = "any text"
	}

	body["chat_id"] = leadId.String()[:1]

	return body
}

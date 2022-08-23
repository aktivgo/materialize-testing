package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/semichkin-gopkg/uuid"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	MaterializeUrl = "postgres://materialize@localhost:6875/materialize?sslmode=disable"

	TriggersCount   = 10000
	GoroutinesCount = 16

	KafkaAddress       = "redpanda:29092"
	KafkaTriggersTopic = "triggers"
)

var (
	resources = []uuid.UUID{
		"1a34b742-1ec4-11ed-861d-0242ac120002",
		"2a4aad70-1ec4-11ed-861d-0242ac120002",
		"3a4aad70-1ec4-11ed-861d-0242ac120002",
		"4a4aad70-1ec4-11ed-861d-0242ac120002",
		"5a4aad70-1ec4-11ed-861d-0242ac120002",
		"6a4aad70-1ec4-11ed-861d-0242ac120002",
		"7a4aad70-1ec4-11ed-861d-0242ac120002",
		"8a4aad70-1ec4-11ed-861d-0242ac120002",
		"9a4aad70-1ec4-11ed-861d-0242ac120002",
		"0a4aad70-1ec4-11ed-861d-0242ac120002",
	}
	leads = []uuid.UUID{
		"1f486320-1ec4-11ed-861d-0242ac120002",
		"24d36d76-1ec4-11ed-861d-0242ac120002",
		"34d36d76-1ec4-11ed-861d-0242ac120002",
		"44d36d76-1ec4-11ed-861d-0242ac120002",
		"54d36d76-1ec4-11ed-861d-0242ac120002",
		"64d36d76-1ec4-11ed-861d-0242ac120002",
		"74d36d76-1ec4-11ed-861d-0242ac120002",
		"84d36d76-1ec4-11ed-861d-0242ac120002",
		"94d36d76-1ec4-11ed-861d-0242ac120002",
		"04d36d76-1ec4-11ed-861d-0242ac120002",
	}
	types = []string{
		"'tg_send_text'",
		"'tg_send_text', 'tg_start'",
		"'tg_start'",
	}
)

type (
	ListenerParams struct {
		ViewName   string    `json:"view_name"`
		SinkName   string    `json:"sink_name"`
		ResourceId uuid.UUID `json:"resource_id"`
		LeadId     uuid.UUID `json:"lead_id"`
		Type       string    `json:"type"`
		Timestamp  int64     `json:"timestamp"`
	}

	TailResult struct {
		MzTimestamp int64     `json:"mz_timestamp"`
		MzDiff      int       `json:"mz_diff"`
		ResourceId  uuid.UUID `json:"resource_id"`
		LeadId      uuid.UUID `json:"lead_id"`
		Type        string    `json:"type"`
		Body        string    `json:"body"`
		Timestamp   int64     `json:"timestamp"`
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

	counterSuccess := Counter{i: 0}
	counterAll := Counter{i: 0}

	defer func() {
		conn.Close()
		log.Println("total created:", counterSuccess.i)
	}()

	wg := sync.WaitGroup{}
	for i := 0; i < GoroutinesCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				if counterAll.i == TriggersCount {
					break
				}

				listenerParams := generateListenerParams()
				if err := createListener(ctx, conn, listenerParams); err != nil {
					log.Println(err)
					continue
				}

				log.Printf("listener [%s %s] created\n", listenerParams.ViewName, listenerParams.SinkName)

				counterSuccess.increase()
				counterAll.increase()
			}
		}()
	}

	wg.Wait()
}

func generateListenerParams() ListenerParams {
	viewName := "view_" + base64.StdEncoding.EncodeToString([]byte(uuid.New()))
	sinkName := "sink_" + base64.StdEncoding.EncodeToString([]byte(uuid.New()))
	resourceId := resources[rand.Intn(len(resources))]
	leadId := leads[rand.Intn(len(leads))]
	_type := types[rand.Intn(len(types))]
	timeDiff := rand.Int63n(10)

	return ListenerParams{
		ViewName:   viewName,
		SinkName:   sinkName,
		ResourceId: resourceId,
		LeadId:     leadId,
		Type:       _type,
		Timestamp:  time.Now().Unix() - timeDiff,
	}
}

func createListener(ctx context.Context, conn *pgxpool.Pool, params ListenerParams) error {
	workflowId := uuid.New()

	createViewSQL := fmt.Sprintf(`CREATE MATERIALIZED VIEW %s AS
                    SELECT
						'%s' AS view_name,
						'%s' AS workflow_id,
						data->>'body' AS body,
						(data->>'timestamp')::int AS timestamp
                    FROM (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM events_source)
					WHERE 
						data->>'direction' = 'in' AND
						data->>'resource_id' = '%s' AND
						data->>'lead_id' = '%s' AND
						data->>'type' IN (%s) AND
						(data->>'timestamp')::int >= %d
					;`,
		params.ViewName,
		params.ViewName,
		workflowId,
		params.ResourceId,
		params.LeadId,
		params.Type,
		params.Timestamp,
	)

	_, err := conn.Exec(ctx, createViewSQL)
	if err != nil {
		return err
	}

	createSinkSQL := fmt.Sprintf(`CREATE SINK %s
					FROM %s
					INTO KAFKA BROKER '%s' TOPIC '%s'
					FORMAT JSON
					;`,
		params.SinkName,
		params.ViewName,
		KafkaAddress,
		KafkaTriggersTopic,
	)

	_, err = conn.Exec(ctx, createSinkSQL)
	if err != nil {
		return err
	}

	return nil
}

func tail(ctx context.Context, conn *pgxpool.Pool, viewName string) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Println(err)
		return
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE c CURSOR FOR TAIL %s", viewName))
	if err != nil {
		log.Println(err)
		return
	}

	for {
		rows, err := tx.Query(ctx, "FETCH ALL c")
		if err != nil {
			tx.Rollback(ctx)
			log.Println(err)
			return
		}

		for rows.Next() {
			var r TailResult
			if err := rows.Scan(
				&r.MzTimestamp,
				&r.MzDiff,
				&r.ResourceId,
				&r.LeadId,
				&r.Type,
				&r.Body,
				&r.Timestamp,
			); err != nil {
				log.Println(err)
				return
			}
			rows.Close()

			break
		}

		break
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Println(err)
		return
	}
}

func dropListener(ctx context.Context, conn *pgxpool.Pool, viewName string) {
	dropViewSQL := fmt.Sprintf(`DROP VIEW %s;`, viewName)

	_, err := conn.Exec(ctx, dropViewSQL)
	if err != nil {
		log.Println(err)
		return
	}
}

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/semichkin-gopkg/uuid"
	"log"
	"sync"
	"time"
)

const (
	GoroutinesCount = 1
	MaterializeUrl  = "postgres://materialize@localhost:6875/materialize?sslmode=disable"
)

var (
	resources = []uuid.UUID{
		"1a34b742-1ec4-11ed-861d-0242ac120002",
		"2a4aad70-1ec4-11ed-861d-0242ac120002",
	}
	leads = []uuid.UUID{
		"1f486320-1ec4-11ed-861d-0242ac120002",
		"24d36d76-1ec4-11ed-861d-0242ac120002",
	}
	types = []string{
		"'tg_send_text'",
		"'tg_send_text', 'tg_start'",
		"'tg_start'",
	}
)

type (
	ViewParams struct {
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
)

func main() {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, MaterializeUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	wg := sync.WaitGroup{}
	for i := 0; i < GoroutinesCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			params := generateParams()
			viewName, err := createView(ctx, conn, params)
			if err != nil {
				log.Fatalln(err)
			}

			log.Printf("view_name [%s]\n", viewName)

			tail(ctx, conn, viewName)
		}()
	}

	wg.Wait()
}

func generateParams() ViewParams {
	return ViewParams{
		ResourceId: resources[0],
		LeadId:     leads[0],
		Type:       types[0],
		Timestamp:  time.Now().Unix() - 60,
	}
}

func createView(ctx context.Context, conn *pgx.Conn, params ViewParams) (string, error) {
	listenerId := "listener_" + base64.StdEncoding.EncodeToString([]byte(uuid.New()))

	createViewSQL := fmt.Sprintf(`CREATE OR REPLACE MATERIALIZED VIEW %s AS
                    SELECT
                        data->>'resource_id' AS resource_id,
						data->>'lead_id' AS lead_id,
						data->>'type' AS type,
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
		listenerId,
		params.ResourceId,
		params.LeadId,
		params.Type,
		params.Timestamp,
	)

	_, err := conn.Exec(ctx, createViewSQL)
	if err != nil {
		return "", err
	}

	return listenerId, nil
}

func tail(ctx context.Context, conn *pgx.Conn, viewName string) {
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
			if err := rows.Scan(&r.MzTimestamp, &r.MzDiff, &r.ResourceId, &r.LeadId, &r.Type, &r.Body, &r.Timestamp); err != nil {
				log.Println(err)
				return
			}
			fmt.Printf("%+v\n", r)
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

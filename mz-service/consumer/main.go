package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/semichkin-gopkg/uuid"
	"log"
	"sync"
)

const (
	GoroutinesCount = 1
	MaterializeUrl  = "postgres://materialize@localhost:6875/materialize?sslmode=disable"
)

type (
	ViewParams struct {
		ResourceId uuid.UUID `json:"resource_id"`
		LeadId     uuid.UUID `json:"lead_id"`
		Type       string    `json:"type"`
		Timestamp  int64     `json:"timestamp"`
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

			if err := tail(ctx, conn, viewName.String()); err != nil {
				log.Fatalln(err)
			}
		}()
	}

	wg.Wait()
}

func generateParams() ViewParams {
	return ViewParams{}
}

func createView(ctx context.Context, conn *pgx.Conn, params ViewParams) (uuid.UUID, error) {
	createViewSQL := `CREATE VIEW listener_$1 AS
                    SELECT
                        *
                    FROM (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM events_source)
					WHERE 
						data->>'resource_id' = $2 AND
						data->>'lead_id' = $3 AND
						data->>'type' = $4 AND
						data->>'timestamp' >= $5
					;`
	listenerId := uuid.New()
	_, err := conn.Exec(ctx, createViewSQL, listenerId, params.ResourceId, params.LeadId, params.Type, params.Timestamp)
	if err != nil {
		return uuid.Nil(), err
	}

	return listenerId, nil
}

func tail(ctx context.Context, conn *pgx.Conn, viewName string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE c CURSOR FOR TAIL %s", viewName))
	if err != nil {
		return err
	}

	for {
		rows, err := tx.Query(ctx, "FETCH ALL c")
		if err != nil {
			tx.Rollback(ctx)
			return err
		}

		for rows.Next() {
			var r map[string]any
			if err := rows.Scan(&r); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%+v\n", r)
		}

		break
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	return nil
}

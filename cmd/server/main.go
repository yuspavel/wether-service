package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-co-op/gocron/v2"
	"github.com/jackc/pgx/v5"
	"github.com/yussup/wether-service/internal/client/http/geocoding"
	"github.com/yussup/wether-service/internal/client/http/openmeteo"
)

const (
	httpPort = ":3000"
	city     = "moscow"
)

type Measure struct {
	Name        string    `db:"name"`
	Timestamp   time.Time `db:"timestamp"`
	Temperature float64   `db:"temperature"`
}

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, "postgresql://yuspavel:qwerty123@localhost:54321/wether")
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	r.Get("/{city}", func(w http.ResponseWriter, r *http.Request) {
		cityName := chi.URLParam(r, "city")
		fmt.Println("City:", cityName)

		var measure Measure
		err = conn.QueryRow(ctx, "select name,timestamp,temperature from measures where name=$1 order by timestamp desc limit 5", cityName).Scan(&measure.Name, &measure.Timestamp, &measure.Temperature)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(fmt.Appendf(nil, "%s not found", cityName)))
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
			return
		}

		b, err := json.Marshal(measure)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
			return
		}

		if _, err = w.Write(b); err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
			return
		}

	})

	scheduler, err := gocron.NewScheduler()
	if err != nil {
		panic(err)
	}

	jobs, err := initJobs(ctx, scheduler, conn)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		fmt.Printf("starting server at %s port\n", httpPort)
		if err := http.ListenAndServe(httpPort, r); err != nil {
			panic(err)
		}
	}()

	// start the scheduler
	go func() {
		defer wg.Done()
		fmt.Printf("starting job #%v\n", jobs[0].ID())
		scheduler.Start()
	}()

	wg.Wait()
}

func initJobs(ctx context.Context, scheduler gocron.Scheduler, conn *pgx.Conn) ([]gocron.Job, error) {

	job, err := scheduler.NewJob(
		gocron.DurationJob(
			10*time.Second,
		),
		gocron.NewTask(
			func() {
				httpClient := &http.Client{
					Timeout: 5 * time.Second,
				}

				geocodingClient := geocoding.NewClient(httpClient)
				openmeteoClient := openmeteo.NewClient(httpClient)
				geoResp, err := geocodingClient.GetCoords(city)
				if err != nil {
					log.Println(err)
					return
				}

				openResp, err := openmeteoClient.GetTemperature(geoResp.Latitude, geoResp.Longitude)
				if err != nil {
					log.Println(err)
					return
				}

				timestamp, err := time.Parse("2006-01-02T15:04", openResp.Current.Time)
				if err != nil {
					log.Println(err)
					return
				}

				_, err = conn.Exec(ctx, "INSERT INTO measures (name,timestamp,temperature) VALUES ($1,$2,$3)", city, timestamp, openResp.Current.Temperature2m)
				if err != nil {
					log.Println(err)
					return
				}
				fmt.Printf("%v: uploaded data for city: %s\n", timestamp, city)
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return []gocron.Job{job}, nil
}

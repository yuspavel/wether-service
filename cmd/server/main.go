package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-co-op/gocron/v2"
	"github.com/yussup/wether-service/internal/client/http/geocoding"
	"github.com/yussup/wether-service/internal/client/http/openmeteo"
)

const (
	httpPort = ":3000"
	city     = "moscow"
)

type Measure struct {
	Timestamp   time.Time
	Temperature float64
}

type Storage struct {
	data map[string][]Measure
	mu   sync.RWMutex
}

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)

	storage := &Storage{
		data: make(map[string][]Measure),
	}
	r.Get("/{city}", func(w http.ResponseWriter, r *http.Request) {
		cityName := chi.URLParam(r, "city")
		fmt.Println("City:", cityName)

		storage.mu.RLock()
		defer storage.mu.RUnlock()

		c, ok := storage.data[cityName]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("note found"))
			return
		}

		b, err := json.Marshal(c)
		if err != nil {
			log.Println(err)
			return
		}

		if _, err = w.Write(b); err != nil {
			log.Println(err)
			return
		}

	})

	s, err := gocron.NewScheduler()
	if err != nil {
		panic(err)
	}

	jobs, err := initJobs(s, storage)
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
		s.Start()
	}()

	wg.Wait()
}

func initJobs(scheduler gocron.Scheduler, s *Storage) ([]gocron.Job, error) {

	job, err := scheduler.NewJob(
		gocron.DurationJob(
			10*time.Second,
		),
		gocron.NewTask(
			func() {
				httpClient := &http.Client{
					Timeout: 10 * time.Second,
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

				s.mu.Lock()
				defer s.mu.Unlock()
				timestamp, err := time.Parse("2006-01-02T15:04", openResp.Current.Time)
				if err != nil {
					log.Println(err)
					return
				}
				s.data[city] = append(s.data[city], Measure{Timestamp: timestamp, Temperature: openResp.Current.Temperature2m})

				fmt.Printf("%v: uploaded data for city: %s", timestamp, city)
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return []gocron.Job{job}, nil
}

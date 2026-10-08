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

const httpPort = ":3000"

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	geocodingClient := geocoding.NewClient(httpClient)
	openmeteoClient := openmeteo.NewClient(httpClient)

	r.Get("/{city}", func(w http.ResponseWriter, r *http.Request) {
		city := chi.URLParam(r, "city")
		fmt.Println("City:", city)
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
		b, err := json.Marshal(openResp)
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

	jobs, err := initJobs(s)
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

func initJobs(scheduler gocron.Scheduler) ([]gocron.Job, error) {

	j, err := scheduler.NewJob(
		gocron.DurationJob(
			10*time.Second,
		),
		gocron.NewTask(
			func() {
				fmt.Println("cron print") // do things
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return []gocron.Job{j}, nil
}

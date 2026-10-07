package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-co-op/gocron/v2"
)

const httpPort = ":3000"

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte("welcome")); err != nil {
			log.Println(err)
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
	// each job has a unique id
	//fmt.Println(j.ID())

	// block until you are ready to shut down
	/*select {
	case <-time.After(time.Minute):
	}

	// when you're done, shut it down
	err = scheduler.Shutdown()
	// or for context-aware teardown:
	// err = s.ShutdownWithContext(ctx)
	if err != nil {
		// handle error
	}*/
	return []gocron.Job{j}, nil
}

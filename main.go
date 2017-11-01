package main

import (
	"time"
	"fmt"
	"net/http"
	"math/rand"
)

type WorkerJob struct {
	job		string
	respChannel	chan string
}

type Worker struct {
	id	int
}

const num_workers = 2
const job_queue_length = 10
var jobQueue chan WorkerJob
var availableWorkers chan Worker

func setupChannels() {
	jobQueue = make(chan WorkerJob, job_queue_length)
	availableWorkers = make(chan Worker, num_workers)
}

func startWorkers() {
	for i:=0; i < num_workers; i++ {
		availableWorkers <- NewWorker(i)
	}
}

func jobsProcessor() {
	go func() {
		for {
			// wait on a worker becoming available
			worker := <- availableWorkers

			// wait on a job
			workerJob := <- jobQueue

			// fire the job
			go worker.doWork(workerJob)
		}
	}()
}

func (w Worker) doWork(workerJob WorkerJob) {
	fmt.Printf("# %d Got job %s\n", w.id, workerJob.job)

	// do work
	time.Sleep(time.Duration(rand.Int63n(5000)) * time.Millisecond)
	fmt.Printf("# %d Done job %s\n", w.id, workerJob.job)

	// let the caller know we are done
	workerJob.respChannel <- workerJob.job + " Response"

	// put the worker back on the available to process more jobs
	availableWorkers <- w
}

func NewWorker(id int) Worker {
	return Worker{id: id}
}

func NewWorkerJob(job string) WorkerJob {
	return WorkerJob{
		job: job,
		respChannel: make(chan string)}
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	// create a WorkerJob and put it on the queue
	workerJob := NewWorkerJob(string(time.Now().Format("2006-01-02 15:04:05.000000000")))
	jobQueue <- workerJob

	// wait on a response
	resp := <- workerJob.respChannel

	// send an ok response
	js := "{\"status\" : " + resp + " }\n"
	// if err != nil {
	// 	http.Error(w, err.Error(), http.StatusInternalServerError)
	// 	return
	// }

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(js))
}

func main() {
	setupChannels()
	startWorkers()
	jobsProcessor()
	http.HandleFunc("/query", queryHandler)
	http.ListenAndServe("127.0.0.1:3000", nil)
}

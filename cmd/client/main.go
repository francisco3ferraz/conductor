package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/francisco3ferraz/conductor/internal/tracing"
	"github.com/francisco3ferraz/conductor/pkg/client"
)

func main() {
	addr := flag.String("addr", "localhost:9000", "Master server address")
	action := flag.String("action", "submit", "Action: submit, status, list, cancel")
	jobID := flag.String("job", "", "Job ID for status/cancel")
	jobType := flag.String("type", "image_processing", "Job type")
	payload := flag.String("payload", "test data", "Job payload")
	timeoutSeconds := flag.Int64("timeout", 0, "Job timeout in seconds (0 = no timeout)")
	flag.Parse()

	// Initialize distributed tracing
	tracingConfig := tracing.DefaultConfig()
	tracingConfig.ServiceName = "conductor-client"
	tracingConfig.ServiceVersion = "1.0.0"

	ctx := context.Background()
	tracingShutdown, err := tracing.Initialize(ctx, tracingConfig)
	if err != nil {
		// Tracing is optional for client
		log.Printf("Warning: Failed to initialize tracing: %v", err)
	} else {
		defer tracingShutdown()
	}

	// Create client
	c, err := client.NewClient(*addr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch *action {
	case "submit":
		var jobID string
		var err error

		if *timeoutSeconds > 0 {
			jobID, err = c.SubmitJobWithTimeout(ctx, *jobType, []byte(*payload), 5, 3, *timeoutSeconds)
		} else {
			jobID, err = c.SubmitJob(ctx, *jobType, []byte(*payload), 5, 3)
		}

		if err != nil {
			log.Fatalf("SubmitJob failed: %v", err)
		}
		fmt.Printf("Job submitted: %s\n", jobID)
		fmt.Printf("Status: success\n")
		fmt.Printf("Message: Job submitted successfully\n")
		fmt.Printf("Job ID: %s\n", jobID)
		if *timeoutSeconds > 0 {
			fmt.Printf("Timeout: %d seconds\n", *timeoutSeconds)
		}

	case "status":
		if *jobID == "" {
			log.Fatal("Job ID required for status")
		}
		job, err := c.GetJobStatus(ctx, *jobID)
		if err != nil {
			log.Fatalf("GetJobStatus failed: %v", err)
		}
		fmt.Printf("Job: %s\n", job.Id)
		fmt.Printf("Type: %s\n", job.Type)
		fmt.Printf("Status: %s\n", job.Status)
		fmt.Printf("Priority: %d\n", job.Priority)
		fmt.Printf("Created: %v\n", job.CreatedAt.AsTime())
		if job.AssignedTo != "" {
			fmt.Printf("Assigned to: %s\n", job.AssignedTo)
		}
		if job.ErrorMessage != "" {
			fmt.Printf("Error: %s\n", job.ErrorMessage)
		}

	case "list":
		jobs, total, err := c.ListJobs(ctx, 10, 0)
		if err != nil {
			log.Fatalf("ListJobs failed: %v", err)
		}
		fmt.Printf("Total jobs: %d\n", total)
		for i, job := range jobs {
			fmt.Printf("%d. ID=%s Type=%s Status=%s Priority=%d\n",
				i+1, job.Id, job.Type, job.Status, job.Priority)
		}

	case "cancel":
		if *jobID == "" {
			log.Fatal("Job ID required for cancel")
		}
		err := c.CancelJob(ctx, *jobID)
		if err != nil {
			log.Fatalf("CancelJob failed: %v", err)
		}
		fmt.Printf("Success: true\n")
		fmt.Printf("Message: Job cancelled successfully\n")

	default:
		log.Fatalf("Unknown action: %s", *action)
	}
}

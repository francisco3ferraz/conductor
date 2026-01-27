package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:9000", "Master server address")
	action := flag.String("action", "submit", "Action: submit, status, list, cancel")
	jobID := flag.String("job", "", "Job ID for status/cancel")
	jobType := flag.String("type", "image_processing", "Job type")
	payload := flag.String("payload", "test data", "Job payload")
	flag.Parse()

	// Connect to master
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := proto.NewMasterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch *action {
	case "submit":
		resp, err := client.SubmitJob(ctx, &proto.SubmitJobRequest{
			Type:       *jobType,
			Payload:    []byte(*payload),
			Priority:   5,
			MaxRetries: 3,
		})
		if err != nil {
			log.Fatalf("SubmitJob failed: %v", err)
		}
		fmt.Printf("Job submitted: %s\n", resp.JobId)
		fmt.Printf("Status: %s\n", resp.Status)
		fmt.Printf("Message: %s\n", resp.Message)

	case "status":
		if *jobID == "" {
			log.Fatal("Job ID required for status")
		}
		resp, err := client.GetJobStatus(ctx, &proto.GetJobStatusRequest{
			JobId: *jobID,
		})
		if err != nil {
			log.Fatalf("GetJobStatus failed: %v", err)
		}
		fmt.Printf("Job: %s\n", resp.Job.Id)
		fmt.Printf("Type: %s\n", resp.Job.Type)
		fmt.Printf("Status: %s\n", resp.Job.Status)
		fmt.Printf("Priority: %d\n", resp.Job.Priority)
		fmt.Printf("Created: %v\n", resp.Job.CreatedAt.AsTime())
		if resp.Job.AssignedTo != "" {
			fmt.Printf("Assigned to: %s\n", resp.Job.AssignedTo)
		}
		if resp.Job.ErrorMessage != "" {
			fmt.Printf("Error: %s\n", resp.Job.ErrorMessage)
		}

	case "list":
		resp, err := client.ListJobs(ctx, &proto.ListJobsRequest{
			Limit:  10,
			Offset: 0,
		})
		if err != nil {
			log.Fatalf("ListJobs failed: %v", err)
		}
		fmt.Printf("Total jobs: %d\n", resp.Total)
		for i, job := range resp.Jobs {
			fmt.Printf("%d. ID=%s Type=%s Status=%s Priority=%d\n",
				i+1, job.Id, job.Type, job.Status, job.Priority)
		}

	case "cancel":
		if *jobID == "" {
			log.Fatal("Job ID required for cancel")
		}
		resp, err := client.CancelJob(ctx, &proto.CancelJobRequest{
			JobId: *jobID,
		})
		if err != nil {
			log.Fatalf("CancelJob failed: %v", err)
		}
		fmt.Printf("Success: %v\n", resp.Success)
		fmt.Printf("Message: %s\n", resp.Message)

	default:
		log.Fatalf("Unknown action: %s", *action)
	}
}

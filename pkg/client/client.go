package client

import (
	"context"
	"fmt"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client provides a high-level interface to interact with the Conductor master node
type Client struct {
	conn   *grpc.ClientConn
	client proto.MasterServiceClient
}

// NewClient creates a new Conductor client
func NewClient(masterAddr string) (*Client, error) {
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %w", err)
	}

	return &Client{
		conn:   conn,
		client: proto.NewMasterServiceClient(conn),
	}, nil
}

// Close closes the connection to the master
func (c *Client) Close() error {
	return c.conn.Close()
}

// SubmitJob submits a new job to the scheduler
func (c *Client) SubmitJob(ctx context.Context, jobType string, payload []byte, priority, maxRetries int32) (string, error) {
	resp, err := c.client.SubmitJob(ctx, &proto.SubmitJobRequest{
		Type:       jobType,
		Payload:    payload,
		Priority:   priority,
		MaxRetries: maxRetries,
	})
	if err != nil {
		return "", fmt.Errorf("failed to submit job: %w", err)
	}

	if resp.Status != "success" {
		return "", fmt.Errorf("job submission failed: %s", resp.Message)
	}

	return resp.JobId, nil
}

// GetJobStatus retrieves the status of a specific job
func (c *Client) GetJobStatus(ctx context.Context, jobID string) (*proto.Job, error) {
	resp, err := c.client.GetJobStatus(ctx, &proto.GetJobStatusRequest{
		JobId: jobID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get job status: %w", err)
	}

	return resp.Job, nil
}

// ListJobs retrieves a list of jobs
func (c *Client) ListJobs(ctx context.Context, limit, offset int32) ([]*proto.Job, int32, error) {
	resp, err := c.client.ListJobs(ctx, &proto.ListJobsRequest{
		Limit:  limit,
		Offset: offset,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list jobs: %w", err)
	}

	return resp.Jobs, resp.Total, nil
}

// CancelJob cancels a job
func (c *Client) CancelJob(ctx context.Context, jobID string) error {
	resp, err := c.client.CancelJob(ctx, &proto.CancelJobRequest{
		JobId: jobID,
	})
	if err != nil {
		return fmt.Errorf("failed to cancel job: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("cancel failed: %s", resp.Message)
	}

	return nil
}

// SubmitJobWithDefaults submits a job with default priority and retry settings
func (c *Client) SubmitJobWithDefaults(ctx context.Context, jobType string, payload []byte) (string, error) {
	return c.SubmitJob(ctx, jobType, payload, 5, 3)
}

// WaitForJobCompletion polls job status until it completes or context is cancelled
func (c *Client) WaitForJobCompletion(ctx context.Context, jobID string, pollInterval time.Duration) (*proto.Job, error) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			job, err := c.GetJobStatus(ctx, jobID)
			if err != nil {
				return nil, err
			}

			// Check if job is in terminal state
			switch job.Status {
			case "completed", "failed", "cancelled":
				return job, nil
			}
		}
	}
}

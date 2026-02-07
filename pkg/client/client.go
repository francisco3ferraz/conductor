package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"github.com/francisco3ferraz/conductor/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client provides a high-level interface to interact with the Conductor master node
type Client struct {
	conn   *grpc.ClientConn
	client proto.MasterServiceClient
}

// ClientOption defines a functional option for configuring the client
type ClientOption func(*clientOptions)

type clientOptions struct {
	dialOptions []grpc.DialOption
	tlsConfig   *tlsConfig
	token       string
}

type tlsConfig struct {
	enabled    bool
	caFile     string
	certFile   string
	keyFile    string
	skipVerify bool
}

// WithToken adds a Bearer token to requests
func WithToken(token string) ClientOption {
	return func(o *clientOptions) {
		o.token = token
	}
}

// WithTLS enables TLS with the provided configuration
func WithTLS(caFile, certFile, keyFile string, skipVerify bool) ClientOption {
	return func(o *clientOptions) {
		o.tlsConfig = &tlsConfig{
			enabled:    true,
			caFile:     caFile,
			certFile:   certFile,
			keyFile:    keyFile,
			skipVerify: skipVerify,
		}
	}
}

// WithDialOptions adds custom gRPC dial options
func WithDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(o *clientOptions) {
		o.dialOptions = append(o.dialOptions, opts...)
	}
}

// NewClient creates a new Conductor client
func NewClient(masterAddr string, opts ...ClientOption) (*Client, error) {
	// Default options
	options := &clientOptions{
		dialOptions: rpc.GetClientInterceptors(),
	}

	// Apply options
	for _, opt := range opts {
		opt(options)
	}

	// Configure TLS if enabled
	if options.tlsConfig != nil && options.tlsConfig.enabled {
		var creds credentials.TransportCredentials
		var err error

		if options.tlsConfig.caFile != "" && options.tlsConfig.certFile != "" && options.tlsConfig.keyFile != "" {
			// mTLS
			cert, err := tls.LoadX509KeyPair(options.tlsConfig.certFile, options.tlsConfig.keyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load client cert/key: %w", err)
			}

			caCert, err := os.ReadFile(options.tlsConfig.caFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA cert: %w", err)
			}

			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to append CA cert to pool")
			}

			tlsConfig := &tls.Config{
				Certificates:       []tls.Certificate{cert},
				RootCAs:            caCertPool,
				InsecureSkipVerify: options.tlsConfig.skipVerify,
			}
			creds = credentials.NewTLS(tlsConfig)
		} else if options.tlsConfig.caFile != "" {
			// TLS with custom CA (no client auth)
			creds, err = credentials.NewClientTLSFromFile(options.tlsConfig.caFile, "")
			if err != nil {
				return nil, fmt.Errorf("failed to load CA cert: %w", err)
			}
		} else {
			// System TLS
			creds = credentials.NewTLS(&tls.Config{
				InsecureSkipVerify: options.tlsConfig.skipVerify,
			})
		}

		if err != nil {
			return nil, fmt.Errorf("failed to create TLS credentials: %w", err)
		}

		options.dialOptions = append(options.dialOptions, grpc.WithTransportCredentials(creds))
	} else {
		// Default to insecure if no TLS config provided
		options.dialOptions = append(options.dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Add token auth if provided
	if options.token != "" {
		options.dialOptions = append(options.dialOptions, grpc.WithPerRPCCredentials(tokenAuth{token: options.token}))
	}

	conn, err := grpc.NewClient(masterAddr, options.dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %w", err)
	}

	return &Client{
		conn:   conn,
		client: proto.NewMasterServiceClient(conn),
	}, nil
}

type tokenAuth struct {
	token string
}

func (t tokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + t.token,
	}, nil
}

func (t tokenAuth) RequireTransportSecurity() bool {
	return false // Allow insecure (handled by transport credentials)
}

// Close closes the connection to the master
func (c *Client) Close() error {
	return c.conn.Close()
}

// SubmitJob submits a new job to the scheduler
func (c *Client) SubmitJob(ctx context.Context, jobType string, payload []byte, priority, maxRetries int32) (string, error) {
	return c.SubmitJobWithTimeout(ctx, jobType, payload, priority, maxRetries, 0)
}

// SubmitJobWithTimeout submits a new job to the scheduler with a timeout
func (c *Client) SubmitJobWithTimeout(ctx context.Context, jobType string, payload []byte, priority, maxRetries int32, timeoutSeconds int64) (string, error) {
	resp, err := c.client.SubmitJob(ctx, &proto.SubmitJobRequest{
		Type:           jobType,
		Payload:        payload,
		Priority:       priority,
		MaxRetries:     maxRetries,
		TimeoutSeconds: timeoutSeconds,
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

// SubmitJobWithTemplate submits a job using a predefined template
func (c *Client) SubmitJobWithTemplate(ctx context.Context, template JobTemplate, payload []byte) (string, error) {
	return c.SubmitJob(ctx, template.Type, payload, template.Priority, template.MaxRetries)
}

// BatchJob represents a job in a batch submission
type BatchJob struct {
	Type       string
	Payload    []byte
	Priority   int32
	MaxRetries int32
}

// BatchResult contains the result of a batch job submission
type BatchResult struct {
	JobID string
	Error error
	Index int // Index in the original batch
}

// SubmitBatch submits multiple jobs in parallel
func (c *Client) SubmitBatch(ctx context.Context, jobs []BatchJob) []BatchResult {
	results := make([]BatchResult, len(jobs))
	resultsChan := make(chan BatchResult, len(jobs))

	// Submit all jobs concurrently
	for i, job := range jobs {
		go func(idx int, j BatchJob) {
			jobID, err := c.SubmitJob(ctx, j.Type, j.Payload, j.Priority, j.MaxRetries)
			resultsChan <- BatchResult{
				JobID: jobID,
				Error: err,
				Index: idx,
			}
		}(i, job)
	}

	// Collect results
	for i := 0; i < len(jobs); i++ {
		result := <-resultsChan
		results[result.Index] = result
	}

	close(resultsChan)
	return results
}

// SubmitBatchWithTemplate submits multiple jobs using the same template
func (c *Client) SubmitBatchWithTemplate(ctx context.Context, template JobTemplate, payloads [][]byte) []BatchResult {
	jobs := make([]BatchJob, len(payloads))
	for i, payload := range payloads {
		jobs[i] = BatchJob{
			Type:       template.Type,
			Payload:    payload,
			Priority:   template.Priority,
			MaxRetries: template.MaxRetries,
		}
	}
	return c.SubmitBatch(ctx, jobs)
}

// WaitForBatchCompletion waits for all jobs in a batch to complete
func (c *Client) WaitForBatchCompletion(ctx context.Context, jobIDs []string, pollInterval time.Duration) ([]*proto.Job, error) {
	results := make([]*proto.Job, len(jobIDs))
	errors := make([]error, len(jobIDs))
	done := make(chan struct{})

	// Wait for each job concurrently
	for i, jobID := range jobIDs {
		go func(idx int, id string) {
			job, err := c.WaitForJobCompletion(ctx, id, pollInterval)
			if err != nil {
				errors[idx] = err
			} else {
				results[idx] = job
			}
			done <- struct{}{}
		}(i, jobID)
	}

	// Wait for all jobs to complete
	for range jobIDs {
		<-done
	}
	close(done)

	// Check for errors
	for _, err := range errors {
		if err != nil {
			return results, fmt.Errorf("one or more jobs failed: %w", err)
		}
	}

	return results, nil
}

// FilterJobsByStatus filters jobs by their status
func (c *Client) FilterJobsByStatus(ctx context.Context, status string) ([]*proto.Job, error) {
	resp, err := c.client.ListJobs(ctx, &proto.ListJobsRequest{
		StatusFilter: status,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to filter jobs by status: %w", err)
	}

	return resp.Jobs, nil
}

// GetCompletedJobs retrieves all completed jobs
func (c *Client) GetCompletedJobs(ctx context.Context) ([]*proto.Job, error) {
	return c.FilterJobsByStatus(ctx, "completed")
}

// GetFailedJobs retrieves all failed jobs
func (c *Client) GetFailedJobs(ctx context.Context) ([]*proto.Job, error) {
	return c.FilterJobsByStatus(ctx, "failed")
}

// GetPendingJobs retrieves all pending jobs
func (c *Client) GetPendingJobs(ctx context.Context) ([]*proto.Job, error) {
	return c.FilterJobsByStatus(ctx, "pending")
}

// GetRunningJobs retrieves all running jobs
func (c *Client) GetRunningJobs(ctx context.Context) ([]*proto.Job, error) {
	return c.FilterJobsByStatus(ctx, "running")
}

// RetryFailedJob resubmits a failed job with the same parameters
func (c *Client) RetryFailedJob(ctx context.Context, jobID string) (string, error) {
	job, err := c.GetJobStatus(ctx, jobID)
	if err != nil {
		return "", fmt.Errorf("failed to get job for retry: %w", err)
	}

	if job.Status != "failed" {
		return "", fmt.Errorf("job %s is not in failed state (current: %s)", jobID, job.Status)
	}

	return c.SubmitJob(ctx, job.Type, job.Payload, job.Priority, job.MaxRetries)
}

// JobStats represents statistics about job execution
type JobStats struct {
	TotalJobs       int
	CompletedJobs   int
	FailedJobs      int
	PendingJobs     int
	RunningJobs     int
	CancelledJobs   int
	SuccessRate     float64
	AverageDuration time.Duration
}

// GetJobStats retrieves statistics about all jobs
func (c *Client) GetJobStats(ctx context.Context) (*JobStats, error) {
	jobs, _, err := c.ListJobs(ctx, 10000, 0) // Get large batch
	if err != nil {
		return nil, fmt.Errorf("failed to get jobs for stats: %w", err)
	}

	stats := &JobStats{}
	var totalDuration time.Duration
	completedCount := 0

	for _, job := range jobs {
		stats.TotalJobs++
		switch job.Status {
		case "completed":
			stats.CompletedJobs++
			completedCount++
			if job.StartedAt != nil && job.CompletedAt != nil {
				duration := job.CompletedAt.AsTime().Sub(job.StartedAt.AsTime())
				totalDuration += duration
			}
		case "failed":
			stats.FailedJobs++
		case "pending":
			stats.PendingJobs++
		case "running":
			stats.RunningJobs++
		case "cancelled":
			stats.CancelledJobs++
		}
	}

	if stats.TotalJobs > 0 {
		stats.SuccessRate = float64(stats.CompletedJobs) / float64(stats.TotalJobs) * 100
	}

	if completedCount > 0 {
		stats.AverageDuration = totalDuration / time.Duration(completedCount)
	}

	return stats, nil
}

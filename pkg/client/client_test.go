package client

import (
	"context"
	"net"
	"testing"
	"time"

	proto "github.com/francisco3ferraz/conductor/api/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MockMasterServer implements proto.MasterServiceServer for testing
type MockMasterServer struct {
	proto.UnimplementedMasterServiceServer
	submitJobFunc    func(context.Context, *proto.SubmitJobRequest) (*proto.SubmitJobResponse, error)
	getJobStatusFunc func(context.Context, *proto.GetJobStatusRequest) (*proto.GetJobStatusResponse, error)
	listJobsFunc     func(context.Context, *proto.ListJobsRequest) (*proto.ListJobsResponse, error)
	cancelJobFunc    func(context.Context, *proto.CancelJobRequest) (*proto.CancelJobResponse, error)
}

func (m *MockMasterServer) SubmitJob(ctx context.Context, req *proto.SubmitJobRequest) (*proto.SubmitJobResponse, error) {
	if m.submitJobFunc != nil {
		return m.submitJobFunc(ctx, req)
	}
	return &proto.SubmitJobResponse{
		JobId:   "job-test-123",
		Status:  "success",
		Message: "Job submitted successfully",
	}, nil
}

func (m *MockMasterServer) GetJobStatus(ctx context.Context, req *proto.GetJobStatusRequest) (*proto.GetJobStatusResponse, error) {
	if m.getJobStatusFunc != nil {
		return m.getJobStatusFunc(ctx, req)
	}
	return &proto.GetJobStatusResponse{
		Job: &proto.Job{
			Id:        req.JobId,
			Type:      "image_processing",
			Status:    "completed",
			Priority:  5,
			CreatedAt: timestamppb.Now(),
		},
	}, nil
}

func (m *MockMasterServer) ListJobs(ctx context.Context, req *proto.ListJobsRequest) (*proto.ListJobsResponse, error) {
	if m.listJobsFunc != nil {
		return m.listJobsFunc(ctx, req)
	}
	return &proto.ListJobsResponse{
		Jobs: []*proto.Job{
			{Id: "job-1", Type: "image_processing", Status: "completed"},
			{Id: "job-2", Type: "web_scraping", Status: "running"},
		},
		Total: 2,
	}, nil
}

func (m *MockMasterServer) CancelJob(ctx context.Context, req *proto.CancelJobRequest) (*proto.CancelJobResponse, error) {
	if m.cancelJobFunc != nil {
		return m.cancelJobFunc(ctx, req)
	}
	return &proto.CancelJobResponse{
		Success: true,
		Message: "Job cancelled successfully",
	}, nil
}

// startMockServer starts a mock gRPC server for testing
func startMockServer(t *testing.T, mock *MockMasterServer) string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	proto.RegisterMasterServiceServer(server, mock)

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	t.Cleanup(func() {
		server.Stop()
	})

	return lis.Addr().String()
}

func TestNewClient(t *testing.T) {
	mock := &MockMasterServer{}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NotNil(t, client.conn)
	require.NotNil(t, client.client)

	err = client.Close()
	assert.NoError(t, err)
}

func TestNewClient_ConnectionFailure(t *testing.T) {
	// grpc.NewClient doesn't fail immediately on bad address
	// It only fails when you try to make actual RPC calls
	client, err := NewClient("localhost:99999")
	// Connection succeeds but RPC will fail
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	// Try to use the client - this should fail
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = client.GetJobStatus(ctx, "test")
	assert.Error(t, err)
}

func TestClient_SubmitJob(t *testing.T) {
	mock := &MockMasterServer{}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	jobID, err := client.SubmitJob(ctx, "image_processing", []byte("test payload"), 5, 3)

	assert.NoError(t, err)
	assert.Equal(t, "job-test-123", jobID)
}

func TestClient_SubmitJob_Failure(t *testing.T) {
	mock := &MockMasterServer{
		submitJobFunc: func(ctx context.Context, req *proto.SubmitJobRequest) (*proto.SubmitJobResponse, error) {
			return &proto.SubmitJobResponse{
				JobId:   "",
				Status:  "error",
				Message: "Invalid job type",
			}, nil
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	jobID, err := client.SubmitJob(ctx, "invalid_type", []byte("test"), 5, 3)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "job submission failed")
	assert.Empty(t, jobID)
}

func TestClient_SubmitJob_RPCError(t *testing.T) {
	mock := &MockMasterServer{
		submitJobFunc: func(ctx context.Context, req *proto.SubmitJobRequest) (*proto.SubmitJobResponse, error) {
			return nil, status.Error(codes.Internal, "internal server error")
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	jobID, err := client.SubmitJob(ctx, "image_processing", []byte("test"), 5, 3)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to submit job")
	assert.Empty(t, jobID)
}

func TestClient_GetJobStatus(t *testing.T) {
	mock := &MockMasterServer{}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	job, err := client.GetJobStatus(ctx, "job-123")

	assert.NoError(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, "job-123", job.Id)
	assert.Equal(t, "image_processing", job.Type)
	assert.Equal(t, "completed", job.Status)
}

func TestClient_GetJobStatus_NotFound(t *testing.T) {
	mock := &MockMasterServer{
		getJobStatusFunc: func(ctx context.Context, req *proto.GetJobStatusRequest) (*proto.GetJobStatusResponse, error) {
			return nil, status.Error(codes.NotFound, "job not found")
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	job, err := client.GetJobStatus(ctx, "non-existent")

	assert.Error(t, err)
	assert.Nil(t, job)
	assert.Contains(t, err.Error(), "failed to get job status")
}

func TestClient_ListJobs(t *testing.T) {
	mock := &MockMasterServer{}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	jobs, total, err := client.ListJobs(ctx, 10, 0)

	assert.NoError(t, err)
	assert.Len(t, jobs, 2)
	assert.Equal(t, int32(2), total)
	assert.Equal(t, "job-1", jobs[0].Id)
	assert.Equal(t, "job-2", jobs[1].Id)
}

func TestClient_ListJobs_Error(t *testing.T) {
	mock := &MockMasterServer{
		listJobsFunc: func(ctx context.Context, req *proto.ListJobsRequest) (*proto.ListJobsResponse, error) {
			return nil, status.Error(codes.Internal, "database error")
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	jobs, total, err := client.ListJobs(ctx, 10, 0)

	assert.Error(t, err)
	assert.Nil(t, jobs)
	assert.Equal(t, int32(0), total)
}

func TestClient_CancelJob(t *testing.T) {
	mock := &MockMasterServer{}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	err = client.CancelJob(ctx, "job-123")

	assert.NoError(t, err)
}

func TestClient_CancelJob_Failure(t *testing.T) {
	mock := &MockMasterServer{
		cancelJobFunc: func(ctx context.Context, req *proto.CancelJobRequest) (*proto.CancelJobResponse, error) {
			return &proto.CancelJobResponse{
				Success: false,
				Message: "Job already completed",
			}, nil
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	err = client.CancelJob(ctx, "job-123")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cancel failed")
}

func TestClient_CancelJob_RPCError(t *testing.T) {
	mock := &MockMasterServer{
		cancelJobFunc: func(ctx context.Context, req *proto.CancelJobRequest) (*proto.CancelJobResponse, error) {
			return nil, status.Error(codes.NotFound, "job not found")
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	err = client.CancelJob(ctx, "non-existent")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to cancel job")
}

func TestClient_SubmitJobWithDefaults(t *testing.T) {
	mock := &MockMasterServer{
		submitJobFunc: func(ctx context.Context, req *proto.SubmitJobRequest) (*proto.SubmitJobResponse, error) {
			// Verify default values
			assert.Equal(t, int32(5), req.Priority)
			assert.Equal(t, int32(3), req.MaxRetries)
			return &proto.SubmitJobResponse{
				JobId:   "job-defaults",
				Status:  "success",
				Message: "OK",
			}, nil
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	jobID, err := client.SubmitJobWithDefaults(ctx, "data_analysis", []byte("data"))

	assert.NoError(t, err)
	assert.Equal(t, "job-defaults", jobID)
}

func TestClient_WaitForJobCompletion_Completed(t *testing.T) {
	callCount := 0
	mock := &MockMasterServer{
		getJobStatusFunc: func(ctx context.Context, req *proto.GetJobStatusRequest) (*proto.GetJobStatusResponse, error) {
			callCount++
			status := "running"
			if callCount >= 3 {
				status = "completed"
			}
			return &proto.GetJobStatusResponse{
				Job: &proto.Job{
					Id:     req.JobId,
					Status: status,
				},
			}, nil
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	job, err := client.WaitForJobCompletion(ctx, "job-123", 10*time.Millisecond)

	assert.NoError(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, "completed", job.Status)
	assert.GreaterOrEqual(t, callCount, 3)
}

func TestClient_WaitForJobCompletion_Failed(t *testing.T) {
	mock := &MockMasterServer{
		getJobStatusFunc: func(ctx context.Context, req *proto.GetJobStatusRequest) (*proto.GetJobStatusResponse, error) {
			return &proto.GetJobStatusResponse{
				Job: &proto.Job{
					Id:           req.JobId,
					Status:       "failed",
					ErrorMessage: "execution error",
				},
			}, nil
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	job, err := client.WaitForJobCompletion(ctx, "job-123", 10*time.Millisecond)

	assert.NoError(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, "failed", job.Status)
}

func TestClient_WaitForJobCompletion_Cancelled(t *testing.T) {
	mock := &MockMasterServer{
		getJobStatusFunc: func(ctx context.Context, req *proto.GetJobStatusRequest) (*proto.GetJobStatusResponse, error) {
			return &proto.GetJobStatusResponse{
				Job: &proto.Job{
					Id:     req.JobId,
					Status: "cancelled",
				},
			}, nil
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	job, err := client.WaitForJobCompletion(ctx, "job-123", 10*time.Millisecond)

	assert.NoError(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, "cancelled", job.Status)
}

func TestClient_WaitForJobCompletion_ContextCancelled(t *testing.T) {
	mock := &MockMasterServer{
		getJobStatusFunc: func(ctx context.Context, req *proto.GetJobStatusRequest) (*proto.GetJobStatusResponse, error) {
			return &proto.GetJobStatusResponse{
				Job: &proto.Job{
					Id:     req.JobId,
					Status: "running",
				},
			}, nil
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	job, err := client.WaitForJobCompletion(ctx, "job-123", 10*time.Millisecond)

	assert.Error(t, err)
	assert.Nil(t, job)
	// Error is wrapped, so just check it contains deadline exceeded
	assert.Contains(t, err.Error(), "deadline exceeded")
}

func TestClient_WaitForJobCompletion_GetStatusError(t *testing.T) {
	mock := &MockMasterServer{
		getJobStatusFunc: func(ctx context.Context, req *proto.GetJobStatusRequest) (*proto.GetJobStatusResponse, error) {
			return nil, status.Error(codes.Internal, "server error")
		},
	}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	job, err := client.WaitForJobCompletion(ctx, "job-123", 10*time.Millisecond)

	assert.Error(t, err)
	assert.Nil(t, job)
	assert.Contains(t, err.Error(), "failed to get job status")
}

func TestClient_Close(t *testing.T) {
	mock := &MockMasterServer{}
	addr := startMockServer(t, mock)

	client, err := NewClient(addr)
	require.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)

	// Subsequent operations should fail
	ctx := context.Background()
	_, err = client.GetJobStatus(ctx, "job-123")
	assert.Error(t, err)
}

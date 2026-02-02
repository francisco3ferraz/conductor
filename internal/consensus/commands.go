package consensus

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/storage"
)

// ApplyCommand is a helper to apply commands to the Raft cluster
type ApplyCommand struct {
	raft *RaftNode
}

// NewApplyCommand creates a new command applier
func NewApplyCommand(raftNode *RaftNode) *ApplyCommand {
	return &ApplyCommand{raft: raftNode}
}

// SubmitJob submits a new job to the cluster
func (a *ApplyCommand) SubmitJob(j *job.Job) error {
	payload, err := json.Marshal(SubmitJobPayload{Job: j})
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	cmd := Command{
		Type:    CommandSubmitJob,
		Payload: payload,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	return a.raft.Apply(cmdBytes, 10*time.Second)
}

// AssignJob assigns a job to a worker
func (a *ApplyCommand) AssignJob(jobID, workerID string) error {
	payload, err := json.Marshal(AssignJobPayload{
		JobID:    jobID,
		WorkerID: workerID,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	cmd := Command{
		Type:    CommandAssignJob,
		Payload: payload,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	return a.raft.Apply(cmdBytes, 10*time.Second)
}

// UnassignJob unassigns a job from a worker (rollback operation)
func (a *ApplyCommand) UnassignJob(jobID string) error {
	payload, err := json.Marshal(UnassignJobPayload{
		JobID: jobID,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	cmd := Command{
		Type:    CommandUnassignJob,
		Payload: payload,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	return a.raft.Apply(cmdBytes, 10*time.Second)
}

// CompleteJob marks a job as completed
func (a *ApplyCommand) CompleteJob(jobID string, result *job.Result) error {
	payload, err := json.Marshal(CompleteJobPayload{
		JobID:  jobID,
		Result: result,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	cmd := Command{
		Type:    CommandCompleteJob,
		Payload: payload,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	return a.raft.Apply(cmdBytes, 10*time.Second)
}

// FailJob marks a job as failed
func (a *ApplyCommand) FailJob(jobID, errorMsg string) error {
	payload, err := json.Marshal(FailJobPayload{
		JobID: jobID,
		Error: errorMsg,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	cmd := Command{
		Type:    CommandFailJob,
		Payload: payload,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	return a.raft.Apply(cmdBytes, 10*time.Second)
}

// RegisterWorker registers a new worker
func (a *ApplyCommand) RegisterWorker(worker *storage.WorkerInfo) error {
	payload, err := json.Marshal(RegisterWorkerPayload{Worker: worker})
	if err != nil {
		return fmt.Errorf("failed to marshal worker: %w", err)
	}

	cmd := Command{
		Type:    CommandRegisterWorker,
		Payload: payload,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	return a.raft.Apply(cmdBytes, 10*time.Second)
}

// RemoveWorker removes a worker from the cluster
func (a *ApplyCommand) RemoveWorker(workerID string) error {
	payload, err := json.Marshal(RemoveWorkerPayload{WorkerID: workerID})
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	cmd := Command{
		Type:    CommandRemoveWorker,
		Payload: payload,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	return a.raft.Apply(cmdBytes, 10*time.Second)
}

package worker

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/francisco3ferraz/conductor/internal/job"
	"github.com/francisco3ferraz/conductor/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Executor executes jobs
type Executor struct {
	workerID  string
	logger    *zap.Logger
	executing map[string]context.CancelFunc // job ID -> cancel func
}

// NewExecutor creates a new job executor
func NewExecutor(workerID string, logger *zap.Logger) *Executor {
	return &Executor{
		workerID:  workerID,
		logger:    logger,
		executing: make(map[string]context.CancelFunc),
	}
}

// Execute runs a job and returns the result
func (e *Executor) Execute(ctx context.Context, j *job.Job) *job.Result {
	// Start tracing span for job execution
	tracer := tracing.GetTracer("conductor.worker")
	ctx, span := tracing.StartSpan(ctx, tracer, "worker.execute_job",
		trace.WithAttributes(
			attribute.String("job.id", j.ID),
			attribute.String("job.type", j.Type.String()),
			attribute.String("worker.id", e.workerID),
		),
	)
	defer span.End()

	// Add job and worker attributes
	tracing.AddJobAttributes(span, j.ID, j.Type.String())
	tracing.AddWorkerAttributes(span, e.workerID)

	e.logger.Info("Executing job",
		zap.String("job_id", j.ID),
		zap.String("type", j.Type.String()),
		zap.String("worker_id", e.workerID),
		zap.String("trace_id", span.SpanContext().TraceID().String()),
	)

	startTime := time.Now()

	// Simulate job execution based on type
	var result *job.Result
	switch j.Type {
	case job.TypeImageProcessing:
		span.AddEvent("executing_image_processing")
		result = e.executeImageProcessing(ctx, j)
	case job.TypeWebScraping:
		span.AddEvent("executing_web_scraping")
		result = e.executeWebScraping(ctx, j)
	case job.TypeDataAnalysis:
		span.AddEvent("executing_data_analysis")
		result = e.executeDataAnalysis(ctx, j)
	default:
		span.AddEvent("unknown_job_type")
		result = job.NewErrorResult(fmt.Errorf("unknown job type: %s", j.Type), 0)
	}

	duration := time.Since(startTime)
	result.DurationMs = duration.Milliseconds()

	// Add result attributes to span
	span.SetAttributes(
		attribute.Bool("job.success", result.Success),
		attribute.Int64("job.duration_ms", result.DurationMs),
	)

	if !result.Success {
		span.RecordError(fmt.Errorf("job execution failed"))
		span.AddEvent("job_execution_failed")
	} else {
		span.AddEvent("job_execution_completed")
	}

	e.logger.Info("Job execution complete",
		zap.String("job_id", j.ID),
		zap.Bool("success", result.Success),
		zap.Duration("duration", duration),
		zap.String("trace_id", span.SpanContext().TraceID().String()),
	)

	return result
}

// executeImageProcessing simulates image processing
func (e *Executor) executeImageProcessing(ctx context.Context, j *job.Job) *job.Result {
	// Simulate processing time: 1-3 seconds
	processingTime := time.Duration(1000+rand.Intn(2000)) * time.Millisecond

	select {
	case <-time.After(processingTime):
		// Success
		output := fmt.Sprintf("Processed image: %s (worker: %s)", string(j.Payload), e.workerID)
		return job.NewResult([]byte(output), processingTime)
	case <-ctx.Done():
		// Cancelled
		return job.NewErrorResult(ctx.Err(), processingTime)
	}
}

// executeWebScraping simulates web scraping
func (e *Executor) executeWebScraping(ctx context.Context, j *job.Job) *job.Result {
	// Simulate scraping time: 2-4 seconds
	processingTime := time.Duration(2000+rand.Intn(2000)) * time.Millisecond

	select {
	case <-time.After(processingTime):
		// Random success/failure (90% success rate)
		if rand.Float32() < 0.9 {
			output := fmt.Sprintf("Scraped data from: %s (worker: %s)", string(j.Payload), e.workerID)
			return job.NewResult([]byte(output), processingTime)
		} else {
			return job.NewErrorResult(fmt.Errorf("scraping failed: timeout"), processingTime)
		}
	case <-ctx.Done():
		return job.NewErrorResult(ctx.Err(), processingTime)
	}
}

// executeDataAnalysis simulates data analysis
func (e *Executor) executeDataAnalysis(ctx context.Context, j *job.Job) *job.Result {
	// Simulate analysis time: 3-5 seconds
	processingTime := time.Duration(3000+rand.Intn(2000)) * time.Millisecond

	select {
	case <-time.After(processingTime):
		output := fmt.Sprintf("Analyzed data: %s (worker: %s)", string(j.Payload), e.workerID)
		return job.NewResult([]byte(output), processingTime)
	case <-ctx.Done():
		return job.NewErrorResult(ctx.Err(), processingTime)
	}
}

// Cancel cancels a running job
func (e *Executor) Cancel(jobID string) {
	if cancel, ok := e.executing[jobID]; ok {
		cancel()
		delete(e.executing, jobID)
		e.logger.Info("Cancelled job", zap.String("job_id", jobID))
	}
}

// IsExecuting returns true if the job is currently executing
func (e *Executor) IsExecuting(jobID string) bool {
	_, ok := e.executing[jobID]
	return ok
}

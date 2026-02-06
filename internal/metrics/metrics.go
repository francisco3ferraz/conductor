package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the system
type Metrics struct {
	// Job metrics
	JobsTotal    *prometheus.CounterVec
	JobDuration  *prometheus.HistogramVec
	JobsActive   prometheus.Gauge
	JobRetries   *prometheus.CounterVec
	JobQueueSize prometheus.Gauge
	JobsInDLQ    prometheus.Gauge

	// Worker metrics
	WorkersRegistered prometheus.Gauge
	WorkersActive     prometheus.Gauge
	WorkersFailed     prometheus.Gauge
	WorkerHeartbeats  *prometheus.CounterVec
	WorkerJobCapacity *prometheus.GaugeVec

	// Scheduler metrics
	SchedulingAttempts *prometheus.CounterVec
	SchedulingLatency  prometheus.Histogram
	AssignmentFailures *prometheus.CounterVec

	// Raft metrics
	RaftApplies      *prometheus.CounterVec
	RaftApplyLatency prometheus.Histogram
	RaftSnapshots    prometheus.Counter
	RaftIsLeader     prometheus.Gauge

	// RPC metrics
	RPCRequests *prometheus.CounterVec
	RPCLatency  *prometheus.HistogramVec
	RPCErrors   *prometheus.CounterVec

	// Recovery metrics
	RecoveryAttempts  *prometheus.CounterVec
	RecoverySuccesses prometheus.Counter
	RecoveryFailures  prometheus.Counter
	DLQMovements      prometheus.Counter

	// Security metrics
	AuthAttempts  *prometheus.CounterVec
	RateLimitHits *prometheus.CounterVec
}

// NewMetrics creates and registers all Prometheus metrics
func NewMetrics(namespace string) *Metrics {
	m := &Metrics{
		// Job metrics
		JobsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "jobs_total",
				Help:      "Total number of jobs by type and status",
			},
			[]string{"type", "status"},
		),
		JobDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "job_duration_seconds",
				Help:      "Job execution duration in seconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"type", "status"},
		),
		JobsActive: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "jobs_active",
				Help:      "Number of currently running jobs",
			},
		),
		JobRetries: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "job_retries_total",
				Help:      "Total number of job retries by type",
			},
			[]string{"type"},
		),
		JobQueueSize: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "job_queue_size",
				Help:      "Number of jobs waiting to be assigned",
			},
		),
		JobsInDLQ: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "jobs_dlq_total",
				Help:      "Number of jobs in dead letter queue",
			},
		),

		// Worker metrics
		WorkersRegistered: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "workers_registered",
				Help:      "Number of registered workers",
			},
		),
		WorkersActive: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "workers_active",
				Help:      "Number of active workers",
			},
		),
		WorkersFailed: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "workers_failed",
				Help:      "Number of failed workers",
			},
		),
		WorkerHeartbeats: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "worker_heartbeats_total",
				Help:      "Total number of worker heartbeats",
			},
			[]string{"worker_id", "status"},
		),
		WorkerJobCapacity: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "worker_job_capacity",
				Help:      "Worker job capacity and current load",
			},
			[]string{"worker_id", "metric"},
		),

		// Scheduler metrics
		SchedulingAttempts: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "scheduling_attempts_total",
				Help:      "Total number of scheduling attempts",
			},
			[]string{"policy", "result"},
		),
		SchedulingLatency: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "scheduling_latency_seconds",
				Help:      "Time taken to schedule a job",
				Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
			},
		),
		AssignmentFailures: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "assignment_failures_total",
				Help:      "Total number of job assignment failures",
			},
			[]string{"reason"},
		),

		// Raft metrics
		RaftApplies: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "raft_applies_total",
				Help:      "Total number of Raft apply operations",
			},
			[]string{"command", "status"},
		),
		RaftApplyLatency: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "raft_apply_latency_seconds",
				Help:      "Raft apply operation latency",
				Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5},
			},
		),
		RaftSnapshots: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "raft_snapshots_total",
				Help:      "Total number of Raft snapshots created",
			},
		),
		RaftIsLeader: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "raft_is_leader",
				Help:      "Whether this node is the Raft leader (1) or not (0)",
			},
		),

		// RPC metrics
		RPCRequests: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "rpc_requests_total",
				Help:      "Total number of RPC requests",
			},
			[]string{"method", "status"},
		),
		RPCLatency: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "rpc_latency_seconds",
				Help:      "RPC request latency",
				Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5},
			},
			[]string{"method"},
		),
		RPCErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "rpc_errors_total",
				Help:      "Total number of RPC errors",
			},
			[]string{"method", "error_type"},
		),

		// Recovery metrics
		RecoveryAttempts: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "recovery_attempts_total",
				Help:      "Total number of job recovery attempts",
			},
			[]string{"reason"},
		),
		RecoverySuccesses: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "recovery_successes_total",
				Help:      "Total number of successful job recoveries",
			},
		),
		RecoveryFailures: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "recovery_failures_total",
				Help:      "Total number of failed job recoveries",
			},
		),
		DLQMovements: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "dlq_movements_total",
				Help:      "Total number of jobs moved to dead letter queue",
			},
		),

		// Security metrics
		AuthAttempts: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "auth_attempts_total",
				Help:      "Total number of authentication attempts",
			},
			[]string{"result"},
		),
		RateLimitHits: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "rate_limit_hits_total",
				Help:      "Total number of rate limit hits",
			},
			[]string{"client"},
		),
	}

	return m
}

// UpdateJobMetrics updates job-related metrics
func (m *Metrics) UpdateJobMetrics(jobType, status string, duration float64) {
	m.JobsTotal.WithLabelValues(jobType, status).Inc()
	if duration > 0 {
		m.JobDuration.WithLabelValues(jobType, status).Observe(duration)
	}
}

// UpdateWorkerMetrics updates worker registry metrics
func (m *Metrics) UpdateWorkerMetrics(registered, active, failed int) {
	m.WorkersRegistered.Set(float64(registered))
	m.WorkersActive.Set(float64(active))
	m.WorkersFailed.Set(float64(failed))
}

// RecordSchedulingAttempt records a scheduling attempt
func (m *Metrics) RecordSchedulingAttempt(policy, result string, latency float64) {
	m.SchedulingAttempts.WithLabelValues(policy, result).Inc()
	m.SchedulingLatency.Observe(latency)
}

// RecordRaftApply records a Raft apply operation
func (m *Metrics) RecordRaftApply(command, status string, latency float64) {
	m.RaftApplies.WithLabelValues(command, status).Inc()
	m.RaftApplyLatency.Observe(latency)
}

// RecordRPCRequest records an RPC request
func (m *Metrics) RecordRPCRequest(method, status string, latency float64) {
	m.RPCRequests.WithLabelValues(method, status).Inc()
	m.RPCLatency.WithLabelValues(method).Observe(latency)
}

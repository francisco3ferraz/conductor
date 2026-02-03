package tracing

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// TracingConfig holds the configuration for OpenTelemetry tracing
type TracingConfig struct {
	ServiceName    string
	ServiceVersion string
	Environment    string
	OTLPEndpoint   string
	Enabled        bool
}

// DefaultConfig returns a default tracing configuration
func DefaultConfig() *TracingConfig {
	return &TracingConfig{
		ServiceName:    "conductor",
		ServiceVersion: "1.0.0",
		Environment:    getEnvOrDefault("CONDUCTOR_ENV", "development"),
		OTLPEndpoint:   getEnvOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
		Enabled:        getEnvOrDefault("OTEL_TRACE_ENABLED", "true") == "true",
	}
}

// Initialize sets up OpenTelemetry tracing for the given service
func Initialize(ctx context.Context, config *TracingConfig) (func(), error) {
	if !config.Enabled {
		log.Printf("OpenTelemetry tracing disabled")
		return func() {}, nil
	}

	// Create resource with service information
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(config.ServiceName),
			semconv.ServiceVersion(config.ServiceVersion),
			semconv.DeploymentEnvironment(config.Environment),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create OTLP HTTP trace exporter
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(config.OTLPEndpoint),
		otlptracehttp.WithInsecure(), // For local development
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create trace provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	// Set global trace provider
	otel.SetTracerProvider(tp)

	// Set global propagator for distributed tracing
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("OpenTelemetry tracing initialized: service=%s, endpoint=%s",
		config.ServiceName, config.OTLPEndpoint)

	// Return cleanup function
	return func() {
		if err := tp.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}, nil
}

// GetTracer returns a tracer for the given name
func GetTracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// StartSpan is a convenience function to start a span with common attributes
func StartSpan(ctx context.Context, tracer trace.Tracer, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, name, opts...)
}

// AddJobAttributes adds common job-related attributes to a span
func AddJobAttributes(span trace.Span, jobID, jobType string) {
	span.SetAttributes(
		semconv.ServiceNamespace("job"),
		attribute.String("job.id", jobID),
		attribute.String("job.type", jobType),
	)
}

// AddWorkerAttributes adds worker-related attributes to a span
func AddWorkerAttributes(span trace.Span, workerID string) {
	span.SetAttributes(
		semconv.ServiceNamespace("worker"),
		attribute.String("worker.id", workerID),
	)
}

// AddRaftAttributes adds Raft consensus attributes to a span
func AddRaftAttributes(span trace.Span, nodeID, command string) {
	span.SetAttributes(
		semconv.ServiceNamespace("raft"),
		attribute.String("raft.node_id", nodeID),
		attribute.String("raft.command", command),
	)
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

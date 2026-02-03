package rpc

import (
	"context"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// General-purpose gRPC interceptors.
// For authentication and authorization, see internal/security/

// LoggingInterceptor logs all incoming gRPC requests
func LoggingInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		// Extract trace information if available
		span := oteltrace.SpanFromContext(ctx)
		traceID := span.SpanContext().TraceID().String()

		// Call the handler
		resp, err := handler(ctx, req)

		// Log the request
		duration := time.Since(start)
		fields := []zap.Field{
			zap.String("method", info.FullMethod),
			zap.Duration("duration", duration),
		}

		// Add trace ID if available
		if span.SpanContext().IsValid() {
			fields = append(fields, zap.String("trace_id", traceID))
		}

		if err != nil {
			fields = append(fields, zap.Error(err))
			logger.Error("RPC call failed", fields...)
		} else {
			logger.Info("RPC call", fields...)
		}

		return resp, err
	}
}

// RecoveryInterceptor recovers from panics in gRPC handlers
func RecoveryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Panic recovered in gRPC handler",
					zap.String("method", info.FullMethod),
					zap.Any("panic", r),
					zap.Stack("stack"),
				)
				err = status.Errorf(grpccodes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}

// ChainInterceptors combines multiple interceptors into one
func ChainInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Build the handler chain in reverse order
		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chained
			chained = func(ctx context.Context, req interface{}) (interface{}, error) {
				return interceptor(ctx, req, info, next)
			}
		}
		return chained(ctx, req)
	}
}

// GetServerInterceptors returns the standard gRPC server options with tracing
func GetServerInterceptors(logger *zap.Logger) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()), // OpenTelemetry tracing via stats handler
		grpc.ChainUnaryInterceptor(
			LoggingInterceptor(logger),  // Logging with trace IDs
			RecoveryInterceptor(logger), // Panic recovery
		),
	}
}

// GetClientInterceptors returns the standard gRPC client options with tracing
func GetClientInterceptors() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}
}

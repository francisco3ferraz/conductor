package metrics

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor returns a gRPC interceptor that records RPC metrics
func UnaryServerInterceptor(metrics *Metrics) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		// Call the handler
		resp, err := handler(ctx, req)

		// Record metrics
		duration := time.Since(start).Seconds()
		statusLabel := "success"
		if err != nil {
			statusLabel = "error"
			st, _ := status.FromError(err)
			metrics.RPCErrors.WithLabelValues(info.FullMethod, st.Code().String()).Inc()
		}

		metrics.RecordRPCRequest(info.FullMethod, statusLabel, duration)

		return resp, err
	}
}

// StreamServerInterceptor returns a gRPC interceptor for streaming RPCs
func StreamServerInterceptor(metrics *Metrics) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()

		// Call the handler
		err := handler(srv, ss)

		// Record metrics
		duration := time.Since(start).Seconds()
		statusLabel := "success"
		if err != nil {
			statusLabel = "error"
			st, _ := status.FromError(err)
			metrics.RPCErrors.WithLabelValues(info.FullMethod, st.Code().String()).Inc()
		}

		metrics.RecordRPCRequest(info.FullMethod, statusLabel, duration)

		return err
	}
}

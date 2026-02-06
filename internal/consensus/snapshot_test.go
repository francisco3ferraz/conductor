package consensus_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"testing"

	"github.com/francisco3ferraz/conductor/internal/consensus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSnapshotCompression(t *testing.T) {
	logger := zap.NewNop()
	fsm := consensus.NewFSM(context.Background(), logger)

	// Create snapshot (even with empty state, it should be compressed)
	snapshot, err := fsm.CreateSnapshot()
	require.NoError(t, err)

	// Mock sink to capture snapshot data
	mockSink := &mockSnapshotSink{
		buf: &bytes.Buffer{},
	}

	// Persist snapshot (should be compressed)
	err = snapshot.Persist(mockSink)
	require.NoError(t, err)

	// Verify the data is gzip compressed by trying to decompress it
	gzReader, err := gzip.NewReader(bytes.NewReader(mockSink.buf.Bytes()))
	require.NoError(t, err, "Snapshot should be gzip compressed")
	defer gzReader.Close()

	// Read some data to verify it's valid gzip
	decompressed := make([]byte, 100)
	n, _ := gzReader.Read(decompressed)
	assert.Greater(t, n, 0, "Should be able to read decompressed data")

	t.Logf("Compressed snapshot size: %d bytes", mockSink.buf.Len())
}

func TestSnapshotRestoreCompressed(t *testing.T) {
	logger := zap.NewNop()

	// Create original FSM
	fsm1 := consensus.NewFSM(context.Background(), logger)

	// Create snapshot
	snapshot, err := fsm1.CreateSnapshot()
	require.NoError(t, err)

	// Persist to buffer
	mockSink := &mockSnapshotSink{buf: &bytes.Buffer{}}
	err = snapshot.Persist(mockSink)
	require.NoError(t, err)

	// Restore to new FSM
	fsm2 := consensus.NewFSM(context.Background(), logger)
	readCloser := &mockReadCloser{Reader: bytes.NewReader(mockSink.buf.Bytes())}
	err = fsm2.RestoreFromSnapshot(readCloser)
	require.NoError(t, err)

	// Verify restoration completed without error
	jobs := fsm2.ListJobs()
	workers := fsm2.ListWorkers()

	assert.NotNil(t, jobs, "Jobs list should not be nil after restore")
	assert.NotNil(t, workers, "Workers list should not be nil after restore")

	t.Logf("Successfully restored snapshot with %d jobs and %d workers", len(jobs), len(workers))
}

// Mock snapshot sink for testing
type mockSnapshotSink struct {
	buf    *bytes.Buffer
	closed bool
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	m.closed = true
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "test-snapshot"
}

func (m *mockSnapshotSink) Cancel() error {
	m.buf.Reset()
	return nil
}

// Mock ReadCloser for testing
type mockReadCloser struct {
	*bytes.Reader
}

func (m *mockReadCloser) Close() error {
	return nil
}

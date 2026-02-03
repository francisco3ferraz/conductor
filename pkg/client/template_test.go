package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJobTemplate_WithType(t *testing.T) {
	template := StandardTemplate.WithType("custom_job")
	assert.Equal(t, "custom_job", template.Type)
	assert.Equal(t, int32(5), template.Priority)
	assert.Equal(t, int32(3), template.MaxRetries)
}

func TestJobTemplate_WithPriority(t *testing.T) {
	template := StandardTemplate.WithPriority(8)
	assert.Equal(t, int32(8), template.Priority)
	assert.Equal(t, int32(3), template.MaxRetries)
}

func TestJobTemplate_WithMaxRetries(t *testing.T) {
	template := StandardTemplate.WithMaxRetries(5)
	assert.Equal(t, int32(5), template.MaxRetries)
	assert.Equal(t, int32(5), template.Priority)
}

func TestJobTemplate_Chaining(t *testing.T) {
	template := StandardTemplate.
		WithType("image_processing").
		WithPriority(10).
		WithMaxRetries(2)

	assert.Equal(t, "image_processing", template.Type)
	assert.Equal(t, int32(10), template.Priority)
	assert.Equal(t, int32(2), template.MaxRetries)
}

func TestImageProcessingTemplate(t *testing.T) {
	template := ImageProcessingTemplate(7)
	assert.Equal(t, "image_processing", template.Type)
	assert.Equal(t, int32(7), template.Priority)
	assert.Equal(t, int32(3), template.MaxRetries)
}

func TestImageProcessingTemplate_DefaultPriority(t *testing.T) {
	template := ImageProcessingTemplate(0)
	assert.Equal(t, "image_processing", template.Type)
	assert.Equal(t, int32(5), template.Priority)
}

func TestWebScrapingTemplate(t *testing.T) {
	template := WebScrapingTemplate(4)
	assert.Equal(t, "web_scraping", template.Type)
	assert.Equal(t, int32(4), template.Priority)
	assert.Equal(t, int32(5), template.MaxRetries) // More retries for network issues
}

func TestDataAnalysisTemplate(t *testing.T) {
	template := DataAnalysisTemplate(6)
	assert.Equal(t, "data_analysis", template.Type)
	assert.Equal(t, int32(6), template.Priority)
	assert.Equal(t, int32(2), template.MaxRetries)
}

func TestPredefinedTemplates(t *testing.T) {
	tests := []struct {
		name     string
		template JobTemplate
		priority int32
		retries  int32
	}{
		{"HighPriority", HighPriorityTemplate, 10, 5},
		{"Standard", StandardTemplate, 5, 3},
		{"LowPriority", LowPriorityTemplate, 1, 2},
		{"QuickRetry", QuickRetryTemplate, 7, 10},
		{"NoRetry", NoRetryTemplate, 5, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.priority, tt.template.Priority)
			assert.Equal(t, tt.retries, tt.template.MaxRetries)
		})
	}
}

func TestBatchJob_Creation(t *testing.T) {
	job := BatchJob{
		Type:       "test_job",
		Payload:    []byte("test payload"),
		Priority:   5,
		MaxRetries: 3,
	}

	assert.Equal(t, "test_job", job.Type)
	assert.Equal(t, []byte("test payload"), job.Payload)
	assert.Equal(t, int32(5), job.Priority)
	assert.Equal(t, int32(3), job.MaxRetries)
}

func TestBatchResult_Fields(t *testing.T) {
	result := BatchResult{
		JobID: "job-123",
		Error: nil,
		Index: 5,
	}

	assert.Equal(t, "job-123", result.JobID)
	assert.Nil(t, result.Error)
	assert.Equal(t, 5, result.Index)
}

package client

// JobTemplate defines a reusable job configuration
type JobTemplate struct {
	Type       string
	Priority   int32
	MaxRetries int32
}

// Common job templates for frequently used job types
var (
	// HighPriorityTemplate for urgent jobs
	HighPriorityTemplate = JobTemplate{
		Priority:   10,
		MaxRetries: 5,
	}

	// StandardTemplate for normal priority jobs
	StandardTemplate = JobTemplate{
		Priority:   5,
		MaxRetries: 3,
	}

	// LowPriorityTemplate for background jobs
	LowPriorityTemplate = JobTemplate{
		Priority:   1,
		MaxRetries: 2,
	}

	// QuickRetryTemplate for fast-failing jobs that need quick retries
	QuickRetryTemplate = JobTemplate{
		Priority:   7,
		MaxRetries: 10,
	}

	// NoRetryTemplate for jobs that should not be retried
	NoRetryTemplate = JobTemplate{
		Priority:   5,
		MaxRetries: 0,
	}
)

// ImageProcessingTemplate returns a template for image processing jobs
func ImageProcessingTemplate(priority int32) JobTemplate {
	if priority == 0 {
		priority = 5
	}
	return JobTemplate{
		Type:       "image_processing",
		Priority:   priority,
		MaxRetries: 3,
	}
}

// WebScrapingTemplate returns a template for web scraping jobs
func WebScrapingTemplate(priority int32) JobTemplate {
	if priority == 0 {
		priority = 3
	}
	return JobTemplate{
		Type:       "web_scraping",
		Priority:   priority,
		MaxRetries: 5, // Web scraping might need more retries due to network issues
	}
}

// DataAnalysisTemplate returns a template for data analysis jobs
func DataAnalysisTemplate(priority int32) JobTemplate {
	if priority == 0 {
		priority = 5
	}
	return JobTemplate{
		Type:       "data_analysis",
		Priority:   priority,
		MaxRetries: 2,
	}
}

// WithType sets the job type for a template
func (t JobTemplate) WithType(jobType string) JobTemplate {
	t.Type = jobType
	return t
}

// WithPriority sets the priority for a template
func (t JobTemplate) WithPriority(priority int32) JobTemplate {
	t.Priority = priority
	return t
}

// WithMaxRetries sets the max retries for a template
func (t JobTemplate) WithMaxRetries(maxRetries int32) JobTemplate {
	t.MaxRetries = maxRetries
	return t
}

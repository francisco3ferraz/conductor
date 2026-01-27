package job

import "fmt"

// Type represents the type of job to execute
type Type int

const (
	TypeImageProcessing Type = iota
	TypeWebScraping
	TypeDataAnalysis
)

// String returns the string representation of the job type
func (t Type) String() string {
	switch t {
	case TypeImageProcessing:
		return "image_processing"
	case TypeWebScraping:
		return "web_scraping"
	case TypeDataAnalysis:
		return "data_analysis"
	default:
		return "unknown"
	}
}

// ParseType parses a string into a Type
func ParseType(s string) (Type, error) {
	switch s {
	case "image_processing":
		return TypeImageProcessing, nil
	case "web_scraping":
		return TypeWebScraping, nil
	case "data_analysis":
		return TypeDataAnalysis, nil
	default:
		return 0, fmt.Errorf("unknown job type: %s", s)
	}
}

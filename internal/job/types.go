package job

import "fmt"

// Type represents the type of job to execute
type Type int

const (
	TypeImageProcessing Type = iota
	TypeWebScraping
	TypeDataAnalysis
	TypeTestSleep
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
	case TypeTestSleep:
		return "test_sleep"
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
	case "test_sleep":
		return TypeTestSleep, nil
	default:
		return 0, fmt.Errorf("unknown job type: %s", s)
	}
}

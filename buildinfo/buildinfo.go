package buildinfo

import "fmt"

var (
	Version     = "1.0.7"
	BuildNumber = "unknown"
	Commit      = "unknown"
	BuildDate   = "unknown"
)

func normalize(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

// VersionString returns the formatted version metadata for --version output.
func VersionString() string {
	return fmt.Sprintf(
		"Version: %s\nBuild Number: %s\nCommit: %s\nBuild Date: %s",
		normalize(Version),
		normalize(BuildNumber),
		normalize(Commit),
		normalize(BuildDate),
	)
}

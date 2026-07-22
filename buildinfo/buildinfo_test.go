package buildinfo

import "testing"

func TestVersionString(t *testing.T) {
	origVersion := Version
	origBuildNumber := BuildNumber
	origCommit := Commit
	origBuildDate := BuildDate
	t.Cleanup(func() {
		Version = origVersion
		BuildNumber = origBuildNumber
		Commit = origCommit
		BuildDate = origBuildDate
	})

	Version = "1.2.3"
	BuildNumber = "42"
	Commit = "abc123"
	BuildDate = "2026-07-22T08:00:00Z"

	got := VersionString()
	want := "Version: 1.2.3\nBuild Number: 42\nCommit: abc123\nBuild Date: 2026-07-22T08:00:00Z"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestVersionStringNormalizesEmptyValues(t *testing.T) {
	origVersion := Version
	origBuildNumber := BuildNumber
	origCommit := Commit
	origBuildDate := BuildDate
	t.Cleanup(func() {
		Version = origVersion
		BuildNumber = origBuildNumber
		Commit = origCommit
		BuildDate = origBuildDate
	})

	Version = ""
	BuildNumber = ""
	Commit = ""
	BuildDate = ""

	got := VersionString()
	want := "Version: unknown\nBuild Number: unknown\nCommit: unknown\nBuild Date: unknown"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

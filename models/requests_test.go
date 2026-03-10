package models

import (
	"errors"
	"strings"
	"testing"
)

func testServer(id int64, name string) Server {
	srv := Server{}
	srv.s.ID = ServerID(id)
	srv.s.Name = name
	return srv
}

func TestNormalizeServerNames(t *testing.T) {
	t.Parallel()

	got := normalizeServerNames([]string{" alpha ", "", "  ", "beta", " gamma"})
	want := []string{"alpha", "beta", "gamma"}

	if len(got) != len(want) {
		t.Fatalf("got %d names, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %q at index %d, want %q", got[i], i, want[i])
		}
	}
}

func TestValidateQueueRequestForm(t *testing.T) {
	ReplaceServerRegistry(
		map[string]Server{
			"1": testServer(1, "localhost"),
			"2": testServer(2, "target"),
			"3": testServer(3, "copy"),
		},
		map[string]Server{
			"localhost": testServer(1, "localhost"),
			"target":    testServer(2, "target"),
			"copy":      testServer(3, "copy"),
		},
	)

	tests := []struct {
		name    string
		form    RequestForm
		wantErr string
	}{
		{
			name: "valid form",
			form: RequestForm{
				Source:      "localhost",
				Destination: "target",
				Body:        `{"ok":true}`,
				CCServers:   []string{"copy"},
			},
		},
		{
			name: "missing source",
			form: RequestForm{
				Destination: "target",
				Body:        `{"ok":true}`,
			},
			wantErr: "source query parameter is required",
		},
		{
			name: "unknown destination",
			form: RequestForm{
				Source:      "localhost",
				Destination: "missing",
				Body:        `{"ok":true}`,
			},
			wantErr: `unknown destination server "missing"`,
		},
		{
			name: "unknown cc server",
			form: RequestForm{
				Source:      "localhost",
				Destination: "target",
				Body:        `{"ok":true}`,
				CCServers:   []string{"missing"},
			},
			wantErr: `unknown cc server "missing"`,
		},
		{
			name: "empty body",
			form: RequestForm{
				Source:      "localhost",
				Destination: "target",
				Body:        "   ",
			},
			wantErr: "request body must not be empty",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateQueueRequestForm(&tc.form)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error containing %q", tc.wantErr)
			}
			if !errors.Is(err, ErrInvalidQueueRequest) {
				t.Fatalf("expected ErrInvalidQueueRequest, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got error %q, want substring %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestBuildRequestFromForm(t *testing.T) {
	t.Parallel()

	ReplaceServerRegistry(
		map[string]Server{
			"1": testServer(1, "localhost"),
			"2": testServer(2, "target"),
		},
		map[string]Server{
			"localhost": testServer(1, "localhost"),
			"target":    testServer(2, "target"),
		},
	)

	req, err := buildRequestFromForm(&RequestForm{
		Source:       "localhost",
		Destination:  "target",
		ContentType:  "application/json",
		Body:         `{"ok":true}`,
		Period:       "202603",
		SubmissionID: "sub-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.Source() != 1 {
		t.Fatalf("got source %d, want 1", req.Source())
	}
	if req.Destination() != 2 {
		t.Fatalf("got destination %d, want 2", req.Destination())
	}
	if req.ContentType() != "application/json" {
		t.Fatalf("got content type %q", req.ContentType())
	}
	if req.Body() != `{"ok":true}` {
		t.Fatalf("got body %q", req.Body())
	}
	if req.UID() == "" {
		t.Fatal("expected generated UID")
	}
}

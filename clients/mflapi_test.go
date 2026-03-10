package clients

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/go-resty/resty/v2"
)

func TestFacilitiesQueryNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     FacilitiesQuery
		wantPage  int
		wantSize  int
		wantDate  string
		wantError string
	}{
		{
			name:     "defaults and alias",
			query:    FacilitiesQuery{UpdatedSince: "2026-03-10"},
			wantPage: DefaultPage,
			wantSize: DefaultPageSize,
			wantDate: "2026-03-10",
		},
		{
			name:      "invalid status",
			query:     FacilitiesQuery{Status: "Closed"},
			wantError: `invalid status "Closed"`,
		},
		{
			name:      "mismatched dates",
			query:     FacilitiesQuery{LastUpdated: "2026-03-10", UpdatedSince: "2026-03-09"},
			wantError: "both last_updated",
		},
		{
			name:      "invalid date",
			query:     FacilitiesQuery{LastUpdated: "10-03-2026"},
			wantError: "invalid last_updated",
		},
		{
			name:     "page size capped",
			query:    FacilitiesQuery{Page: -1, PageSize: MaxPageSize + 10},
			wantPage: DefaultPage,
			wantSize: MaxPageSize,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.query.normalize()
			if tc.wantError != "" {
				if err == nil {
					t.Fatalf("expected error containing %q", tc.wantError)
				}
				if !contains(err.Error(), tc.wantError) {
					t.Fatalf("got error %q, want substring %q", err.Error(), tc.wantError)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Page != tc.wantPage {
				t.Fatalf("got page %d, want %d", got.Page, tc.wantPage)
			}
			if got.PageSize != tc.wantSize {
				t.Fatalf("got page size %d, want %d", got.PageSize, tc.wantSize)
			}
			if got.LastUpdated != tc.wantDate {
				t.Fatalf("got lastUpdated %q, want %q", got.LastUpdated, tc.wantDate)
			}
		})
	}
}

func TestListFacilitiesBuildsExpectedRequest(t *testing.T) {
	t.Parallel()

	restyClient := resty.New()
	restyClient.SetTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/orgunits" {
			t.Fatalf("got path %q, want /orgunits", r.URL.Path)
		}
		if got := r.URL.Query().Get("page"); got != "2" {
			t.Fatalf("got page %q", got)
		}
		if got := r.URL.Query().Get("pageSize"); got != "75" {
			t.Fatalf("got pageSize %q", got)
		}
		if got := r.URL.Query().Get("status"); got != "Functional" {
			t.Fatalf("got status %q", got)
		}
		if got := r.URL.Query().Get("last_updated"); got != "2026-03-10" {
			t.Fatalf("got last_updated %q", got)
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(
				`{"pager":{"page":2,"pageSize":75,"pageCount":1,"total":0},"OrgUnits":null}`,
			)),
			Request: r,
		}, nil
	}))

	client := NewMFLApi("http://example.test", WithResty(restyClient))
	resp, err := client.ListFacilities(context.Background(), FacilitiesQuery{
		Page:        2,
		PageSize:    75,
		Status:      "Functional",
		LastUpdated: "2026-03-10",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.OrgUnits == nil {
		t.Fatal("expected OrgUnits to be normalized to an empty slice")
	}
	if len(resp.OrgUnits) != 0 {
		t.Fatalf("got %d org units, want 0", len(resp.OrgUnits))
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

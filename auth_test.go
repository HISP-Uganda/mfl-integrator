package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestBasicAuthFailures(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name          string
		authHeader    string
		wantChallenge bool
	}{
		{name: "missing header", authHeader: "", wantChallenge: true},
		{name: "wrong scheme", authHeader: "Bearer token", wantChallenge: true},
		{name: "invalid base64", authHeader: "Basic !!!", wantChallenge: true},
		{name: "missing colon", authHeader: "Basic dXNlcm9ubHk=", wantChallenge: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := gin.New()
			r.Use(BasicAuth())
			r.GET("/secure", func(c *gin.Context) {
				c.String(http.StatusOK, "ok")
			})

			req := httptest.NewRequest(http.MethodGet, "/secure", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()

			r.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("got status %d, want %d", rec.Code, http.StatusUnauthorized)
			}
			if got := rec.Header().Get("WWW-Authenticate"); tc.wantChallenge && got != `Basic realm="mfl-integrator"` {
				t.Fatalf("got challenge %q", got)
			}
		})
	}
}

package models

import (
	"testing"

	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
)

func TestGenerateMetadataPayloadGeometry(t *testing.T) {
	tests := []struct {
		name         string
		geometry     interface{}
		wantGeometry bool
	}{
		{
			name:         "valid object",
			geometry:     map[string]interface{}{"type": "Point", "coordinates": []interface{}{32.5, 0.3}},
			wantGeometry: true,
		},
		{
			name:         "string",
			geometry:     "{\"type\":\"Point\"}",
			wantGeometry: false,
		},
		{
			name:         "nil",
			geometry:     nil,
			wantGeometry: false,
		},
		{
			name:         "missing type",
			geometry:     map[string]interface{}{"coordinates": []interface{}{32.5, 0.3}},
			wantGeometry: false,
		},
		{
			name:         "non-string type",
			geometry:     map[string]interface{}{"type": 1},
			wantGeometry: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := GenerateMetadataPayload(dbutils.MapAnything{
				"geometry": tt.geometry,
				"name":     "Facility",
			})

			geometryFound := false
			nameFound := false
			for _, item := range payload {
				switch item.Path {
				case "/geometry":
					geometryFound = true
				case "/name":
					nameFound = true
				}
			}

			if geometryFound != tt.wantGeometry {
				t.Fatalf("geometry patch present = %v, want %v; payload = %#v", geometryFound, tt.wantGeometry, payload)
			}
			if !nameFound {
				t.Fatal("valid name patch was omitted")
			}
		})
	}
}

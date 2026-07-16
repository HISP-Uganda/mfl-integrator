package main

import (
	"testing"

	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
)

func TestCreateOrgUnitGroupPayloadIncludesAllDerivedGroups(t *testing.T) {
	facility := models.OrganisationUnit{
		UID: "test-ou-1",
		OrgUnitGroups: []models.OrgUnitGroup{
			{Name: "HC II", UID: "LEVEL123456", ID: "LEVEL123456"},
			{Name: "Private", UID: "OWNERSHIP01", ID: "OWNERSHIP01"},
			{Name: "MoH", UID: "AUTHORITY01", ID: "AUTHORITY01"},
		},
	}

	metadata := makeMetadataOu(facility)
	payloads := CreateOrgUnitGroupPayload(metadata)

	if len(payloads) != 3 {
		t.Fatalf("expected 3 org unit group payloads, got %d", len(payloads))
	}
	for _, groupID := range []string{"LEVEL123456", "OWNERSHIP01", "AUTHORITY01"} {
		if _, ok := payloads[groupID]; !ok {
			t.Fatalf("expected payload for group %q", groupID)
		}
	}
}

func TestCreateOrgUnitGroupPayloadFallsBackToUID(t *testing.T) {
	metadata := models.MetadataOu{
		UID: "test-ou-2",
		OrganisationUnitGroups: []dbutils.Map{
			newGroupMap("", "OWNERSHIP01", "Private"),
			newGroupMap("", "AUTHORITY01", "MoH"),
		},
	}

	payloads := CreateOrgUnitGroupPayload(metadata)

	if len(payloads) != 2 {
		t.Fatalf("expected 2 org unit group payloads, got %d", len(payloads))
	}
	for _, groupID := range []string{"OWNERSHIP01", "AUTHORITY01"} {
		if _, ok := payloads[groupID]; !ok {
			t.Fatalf("expected payload for fallback uid %q", groupID)
		}
	}
}

func newGroupMap(id, uid, name string) dbutils.Map {
	group := dbutils.Map{}
	if id != "" {
		group.Set("id", id)
	}
	if uid != "" {
		group.Set("uid", uid)
	}
	if name != "" {
		group.Set("name", name)
	}
	return group
}

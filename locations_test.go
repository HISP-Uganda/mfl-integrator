package main

import (
	"encoding/json"
	"testing"

	"github.com/HISP-Uganda/mfl-integrator/clients"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
)

func TestGetOrgUnitFromFacilityUsesIdentifierForAttributeValue(t *testing.T) {
	originalAttributeID := config.MFLIntegratorConf.API.DHIS2OUMFLIDAttributeID
	config.MFLIntegratorConf.API.DHIS2OUMFLIDAttributeID = "ATTR1234567"
	t.Cleanup(func() {
		config.MFLIntegratorConf.API.DHIS2OUMFLIDAttributeID = originalAttributeID
	})

	parentCache := map[string]models.OrganisationUnit{
		"parent-mfl-1": {
			ID:    "42",
			UID:   "PARENTUID01",
			Level: 4,
			Path:  "/ROOT/PARENT",
		},
	}

	facility := GetOrgUnitFromFacility(
		clients.Facility{
			Identifier:   "8038880",
			MFLUID:       "jZQNbneoe9S",
			Name:         "Test Facility",
			ShortName:    "Test Facility",
			HistoricalID: "FACILITY001",
			Parent:       &clients.RefEntity{MFLUID: "parent-mfl-1"},
		},
		parentCache,
		map[string]string{},
		map[string]string{},
	)

	var attributeValues []models.AttributeValue
	if err := json.Unmarshal(facility.AttributeValues, &attributeValues); err != nil {
		t.Fatalf("unmarshal attribute values: %v", err)
	}
	if len(attributeValues) != 1 {
		t.Fatalf("expected 1 attribute value, got %d", len(attributeValues))
	}
	if attributeValues[0].Value != "8038880" {
		t.Fatalf("expected attribute value %q, got %q", "8038880", attributeValues[0].Value)
	}
	if attributeValues[0].Value == "jZQNbneoe9S" {
		t.Fatalf("attribute value should not use mfl_uid")
	}
	if attributeValues[0].Attribute.ID != "ATTR1234567" {
		t.Fatalf("expected attribute id %q, got %q", "ATTR1234567", attributeValues[0].Attribute.ID)
	}
}

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

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/HISP-Uganda/mfl-integrator/clients"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/HISP-Uganda/mfl-integrator/utils"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	"github.com/buger/jsonparser"
	_ "github.com/lib/pq"
	"github.com/samber/lo"
	"github.com/samply/golang-fhir-models/fhir-models/fhir"
	log "github.com/sirupsen/logrus"
)

// LoadOuLevels populates organisation unit levels in our DB from base DHIS2
func LoadOuLevels() {
	var id int
	err := db.GetDB().Get(&id, "SELECT count(*) FROM orgunitlevel")
	if err != nil {
		log.WithError(err).Info("Error reading organisation unit levels:")
		return
	}
	if id != 0 {
		log.WithField("ouCount:", id).Info("Base Organisation units levels found!")
		return
	} else {
		log.WithField("ouCount:", id).Info("Base Organisation units levels not found!")

		apiURL := config.MFLIntegratorConf.API.DHIS2BaseURL + "/organisationUnitLevels.json"
		p := url.Values{}
		p.Add("fields", "id,name,level")
		p.Add("paging", "false")

		ouURL := apiURL + "?" + p.Encode()
		fmt.Println(ouURL)
		resp, err := utils.GetWithBasicAuth(ouURL, config.MFLIntegratorConf.API.DHIS2User,
			config.MFLIntegratorConf.API.DHIS2Password)
		if err != nil {
			log.WithError(err).Info("Failed to fetch organisation unit levels")
			return
		}
		// fmt.Println(string(resp))
		v, _, _, err := jsonparser.Get(resp, "organisationUnitLevels")
		if err != nil {
			log.WithError(err).Error("No organisationUnitLevels found by json parser")
			return
		}
		// fmt.Printf("Entries: %s", v)
		var ouLevels []models.OrgUnitLevel
		err = json.Unmarshal(v, &ouLevels)
		if err != nil {
			fmt.Println("Error unmarshaling orgunit level response body:", err)
			return
		}
		for i := range ouLevels {
			ouLevels[i].UID = ouLevels[i].ID
			log.WithFields(
				log.Fields{"uid": ouLevels[i].ID, "name": ouLevels[i].Name, "level": ouLevels[i].Level}).Info("Creating New Orgunit Level")
			ouLevels[i].NewOrgUnitLevel()
		}
	}
}

// LoadOuGroups populates organisation unit groups in our DB from base DHIS2
func LoadOuGroups() {
	var id int
	err := db.GetDB().Get(&id, "SELECT count(*) FROM orgunitgroup")
	if err != nil {
		log.WithError(err).Info("Error reading organisation unit groups:")
		return
	}
	if id != 0 {
		log.WithField("ouCount:", id).Info("Base Organisation unit groups found!")
		return
	} else {
		log.WithField("ouCount:", id).Info("Base Organisation unit groups not found!")

		apiURL := config.MFLIntegratorConf.API.DHIS2BaseURL + "/organisationUnitGroups.json"
		p := url.Values{}
		p.Add("fields", "id,name,shortName")
		p.Add("paging", "false")

		ouURL := apiURL + "?" + p.Encode()
		fmt.Println(ouURL)
		resp, err := utils.GetWithBasicAuth(ouURL, config.MFLIntegratorConf.API.DHIS2User,
			config.MFLIntegratorConf.API.DHIS2Password)
		if err != nil {
			log.WithError(err).Info("Failed to fetch organisation unit groups")
			return
		}
		v, _, _, err := jsonparser.Get(resp, "organisationUnitGroups")
		if err != nil {
			log.WithError(err).Error("json parser failed to get organisationUnitGroups key")
			return
		}
		var ouGroups []models.OrgUnitGroup
		err = json.Unmarshal(v, &ouGroups)
		if err != nil {
			fmt.Println("Error unmarshaling orgunit groups response body:", err)
			return
		}
		for i := range ouGroups {
			ouGroups[i].UID = ouGroups[i].ID
			if len(ouGroups[i].ShortName) == 0 {
				ouGroups[i].ShortName = ouGroups[i].Name
			}
			log.WithFields(
				log.Fields{"uid": ouGroups[i].ID, "name": ouGroups[i].Name, "level": ouGroups[i].ShortName}).Info("Creating New Orgunit Group")
			ouGroups[i].NewOrgUnitGroup()
		}
	}
}

// LoadAttributes fetches OU related attributes from DHIS2 to our DB
func LoadAttributes() {
	apiURL := config.MFLIntegratorConf.API.DHIS2BaseURL + "/attributes.json"
	p := url.Values{}
	fields := `id~rename(uid),name,displayName,code,shortName,valueType`
	p.Add("fields", fields)
	p.Add("paging", "false")
	p.Add("filter", "organisationUnitAttribute:eq:true")
	attrURL := apiURL + "?" + p.Encode()
	resp, err := utils.GetWithBasicAuth(attrURL, config.MFLIntegratorConf.API.DHIS2User,
		config.MFLIntegratorConf.API.DHIS2Password)
	if err != nil {
		log.WithError(err).Info("Failed to fetch organisation unit attributes")
		return
	}

	v, _, _, err := jsonparser.Get(resp, "attributes")
	if err != nil {
		log.WithError(err).Error("json parser failed to get attributes key")
		return
	}
	var attributes []models.Attribute
	err = json.Unmarshal(v, &attributes)
	if err != nil {
		log.WithError(err).Error("Error unmarshalling attributes to attribute list:")
		return
	}
	for _, attribute := range attributes {
		if attribute.ExistsInDB() {
			log.WithFields(log.Fields{
				"uid": attribute.UID, "name": attribute.Name}).Info("Attribute Exists in DB")
		} else {
			log.WithFields(log.Fields{
				"uid": attribute.UID, "name": attribute.Name}).Info("Creating OU attribute:")
			attribute.OrganisationUnitAttribute = true
			attribute.NewAttribute()
			if len(attribute.Code) > 0 {
				attribute.UpdateCode(attribute.Code)
			}
		}
	}

}

// LoadLocations populates organisation units in our DB from base DHIS2
func LoadLocations() {
	var id int
	err := db.GetDB().Get(&id, "SELECT count(*) FROM organisationunit WHERE hierarchylevel=1")
	if err != nil {
		log.WithError(err).Info("Error reading organisation units:")
		return
	}
	if id != 0 {
		log.WithField("ouCount:", id).Info("Base Organisation units hierarchy found!")
		return
	} else {
		log.WithField("ouCount:", id).Info("Base Organisation units hierarchy not found!")

		apiURL := config.MFLIntegratorConf.API.DHIS2BaseURL + "/organisationUnits.json"
		p := url.Values{}
		fields := `id,name,displayName,code,shortName,openingDate,phoneNumber,` +
			`path,level,description,geometry,organisationUnitGroups[id,name]`
		p.Add("fields", fields)
		p.Add("paging", "false")

		for i := 1; i < config.MFLIntegratorConf.API.DHIS2FacilityLevel; i++ {
			p.Add("level", fmt.Sprintf("%d", i))
			ouURL := apiURL + "?" + p.Encode()
			fmt.Println(ouURL)
			resp, err := utils.GetWithBasicAuth(ouURL, config.MFLIntegratorConf.API.DHIS2User,
				config.MFLIntegratorConf.API.DHIS2Password)
			if err != nil {
				log.WithError(err).Info("Failed to fetch organisation units")
				return
			}

			v, _, _, err := jsonparser.Get(resp, "organisationUnits")
			if err != nil {
				log.WithError(err).Error("json parser failed to get organisationUnit key")
				return
			}
			var ous []models.OrganisationUnit
			err = json.Unmarshal(v, &ous)
			if err != nil {
				log.WithError(err).Error("Error unmarshalling response body:")
				return
			}
			for i := range ous {
				ous[i].UID = ous[i].ID
				log.WithFields(
					log.Fields{"uid": ous[i].ID, "name": ous[i].Name, "level": ous[i].Level,
						"Geometry-Type": ous[i].Geometry.Type}).Info("Creating New Orgunit")
				_ = ous[i].NewOrgUnit()
			}

			p.Del("level")
		}
	}
}

// MatchLocationsWithMFL tries to create a match for our facilities based on DHIS2 uid, name and level
func MatchLocationsWithMFL() {
	dbConn := db.GetDB()
	var levels []models.OrgUnitLevel
	err := dbConn.Select(&levels,
		`SELECT id, name, level FROM orgunitlevel WHERE level < $1 AND name <> 'National' ORDER BY level`,
		config.MFLIntegratorConf.API.DHIS2FacilityLevel)
	if err != nil {
		log.WithError(err).Info("Error reading orgunit levels:")
		return
	}
	for _, l := range levels {
		apiURL := config.MFLIntegratorConf.API.MFLBaseURL
		params := url.Values{}
		_type := l.Name
		if _type == "District" || _type == "District/City" {
			_type = "Local Government"
		}
		if _type == "Sub County/Town Council/Division" || _type == "Sub County/Town Council/Div" {
			_type = "Sub county/Town Council/Division"
		}
		params.Add("resource", "Location")
		params.Add("type", _type)
		params.Add("_count", "20000") // set _count too high value and get everything

		apiURL += "?" + params.Encode()
		log.Info("Locations URL: ", apiURL)
		resp, err := utils.GetWithBasicAuth(apiURL, config.MFLIntegratorConf.API.MFLUser,
			config.MFLIntegratorConf.API.MFLPassword)
		if err != nil {
			log.WithError(err).Info("Matching Process 001: Failed to fetch locations from MFL")
		}
		if resp != nil {
			v, _, _, err := jsonparser.Get(resp, "entry")
			if err != nil {
				log.WithError(err).Error("json parser failed to get entry key")
				continue
			}
			var entries []LocationEntry
			err = json.Unmarshal(v, &entries)
			if err != nil {
				log.WithError(err).Info("Matching Process 002: Error unmarshaling response body")
				fmt.Println(string(resp))
				continue
			}
			for i := range entries {
				name := *entries[i].Resource.Name
				id := *entries[i].Resource.Id
				parent := ""
				if entries[i].Resource.PartOf != nil {
					parent = *entries[i].Resource.PartOf.Reference
				}
				var ous []models.OrganisationUnit
				err = dbConn.Select(&ous,
					`SELECT id, uid, name, COALESCE(mfluid, '') AS mfluid, mflparent, parentid 
					FROM organisationunit WHERE name=$1 AND hierarchylevel =$2`,
					name, l.Level)
				if err != nil {
					log.WithError(err).Info("Matching Process 003: Failed to query ous")
					continue
				}
				switch count := len(ous); count {
				case 0:
					log.WithFields(log.Fields{"Id": id, "Name": name, "Parent": parent, "Level": l.Level}).Info(
						"Matching Process: No matching Ou found")
					// No match
				case 1:
					// Exact Match
					log.WithFields(log.Fields{"Id": id, "Name": name, "Parent": parent, "UID": ous[0].UID}).Info(
						"Matching Process: Exact Ou match found")
					ous[0].UpdateMFLID(id)
					if len(parent) > 0 {
						// FHIR return parent location of the form "Location/1"
						ous[0].UpdateMFLParent(strings.ReplaceAll(parent, "Location/", ""))
					}
				default:
					// More than one
					log.WithFields(log.Fields{"Id": id, "Name": name, "Parent": parent}).Info(
						"Matching Process: Many matches found")
					parent = strings.ReplaceAll(parent, "Location/", "")
					for _, ou := range ous {
						if ou.ParentID == models.GetOUByMFLParentId(parent) {
							// This could be our match
							ou.UpdateMFLParent(parent)
							ou.UpdateMFLID(id)
							log.WithFields(log.Fields{"Id": id, "Name": name, "To": ou.ID, "Parent": parent}).Info("Matched to this one")
						}
					}

				}
			}
		}

	}
}

func MatchLocationsWithMFL2(ctx context.Context, c *clients.Client) {
	dbConn := db.GetDB()

	var levels []models.OrgUnitLevel
	err := dbConn.Select(&levels,
		`SELECT id, name, level
		 FROM orgunitlevel
		 WHERE level < $1 AND name <> 'National'
		 ORDER BY level`,
		config.MFLIntegratorConf.API.DHIS2FacilityLevel,
	)
	if err != nil {
		log.WithError(err).Info("Error reading orgunit levels")
		return
	}

	pageSize := 200 // tune

	for _, l := range levels {
		q := clients.FacilitiesQuery{
			Page:     1,
			PageSize: pageSize,
			Level:    fmt.Sprintf("%d", l.Level),
		}

		log.WithFields(log.Fields{"level": l.Level}).Info("Matching MFL organisation units for level")

		StreamErr := c.StreamFacilities(ctx, q, func(f clients.Facility) error {
			// respect cancellation early
			if err := ctx.Err(); err != nil {
				return err
			}

			name := strings.TrimSpace(f.Name)
			mflUID := strings.TrimSpace(f.MFLUID)
			identifier := strings.TrimSpace(f.Identifier)

			var parentMFLUID string
			if f.Parent != nil {
				parentMFLUID = strings.TrimSpace(f.Parent.MFLUID)
				//if parentMFLUID == "" && strings.TrimSpace(f.Parent.ID) != "" {
				//	parentMFLUID = strings.TrimSpace(f.Parent.ID)
				//}
			}

			var ous []models.OrganisationUnit
			if err := dbConn.Select(&ous,
				`SELECT id, uid, name, COALESCE(mfluid, '') AS mfluid, mflparent, parentid
				 FROM organisationunit
				 WHERE name=$1 AND hierarchylevel=$2`,
				name, l.Level,
			); err != nil {
				log.WithError(err).Info("Matching Process 003: Failed to query ous")
				return nil // keep going
			}

			switch len(ous) {
			case 0:
				log.WithFields(log.Fields{
					"MFLUID": mflUID, "Name": name, "Parent": parentMFLUID, "Level": l.Level,
				}).Info("Matching Process: No matching OU found")

			case 1:
				ou := ous[0]
				ou.UpdateMFLID(identifier)
				ou.UpdateMFLUID(mflUID)
				if parentMFLUID != "" {
					ou.UpdateMFLParent(parentMFLUID)
				}

			default:
				// disambiguate by parent
				if parentMFLUID == "" {
					return nil
				}
				parentLocalID := models.GetOUByMFLParentUID(parentMFLUID)
				if parentLocalID == 0 {
					// log cleanly, but with more detail
					log.WithFields(log.Fields{
						"MFLUID": mflUID, "Name": name, "Parent": parentMFLUID,
					}).Info("Matching Process: No matching parent OU found")
					return nil
				}
				for _, ou := range ous {
					if ou.ParentID == parentLocalID {
						ou.UpdateMFLID(identifier)
						ou.UpdateMFLUID(mflUID)
						ou.UpdateMFLParent(parentMFLUID)
					}
				}
			}

			return nil
		})

		if StreamErr != nil {
			// ctx cancellation should bubble up; other errors are real failures
			if errors.Is(StreamErr, context.Canceled) || errors.Is(StreamErr, context.DeadlineExceeded) {
				log.WithError(StreamErr).Info("MatchLocationsWithMFL2 cancelled")
				return
			}
			log.WithError(StreamErr).WithFields(log.Fields{"level": l.Level}).Info("StreamFacilities failed")
		}
		// log done matching for level
		log.WithFields(log.Fields{"level": l.Level}).Info("MatchLocations With MFL completed for level")
	}
	// log matching process completed
	log.Info("Match Locations With MFL completed for all levels")
}

// FetchFacilities pulls facilities from the MFL after loading and matching
//func FetchFacilities(mflId, batchId string) {
//	dbConn := db.GetDB()
//	var upperLevels int64
//	err := dbConn.Get(&upperLevels, "SELECT count(*) FROM organisationunit WHERE hierarchylevel = $1 - 1",
//		config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel)
//	if err != nil {
//		log.WithError(err).Info("Error reading records on level above facility")
//		return
//	}
//	if upperLevels == 0 {
//		log.Warn("Cannot Synchronize Facilities when hierarchy levels above facility are missing")
//		return
//	}
//	var facilityCount int64
//	err = dbConn.Get(&facilityCount, "SELECT count(*) FROM organisationunit WHERE hierarchylevel = $1 LIMIT 1",
//		config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel)
//	if err != nil {
//		log.WithError(err).Info("Error getting the count of health facilities")
//		return
//	}
//	apiURL := config.MFLIntegratorConf.API.MFLBaseURL
//	params := url.Values{}
//	params.Add("resource", "Location")
//	params.Add("type", "healthFacility")
//	if len(mflId) > 0 {
//		params.Add("facilityLocalGovernment", mflId)
//	}
//	params.Add("_count", "30000")
//	if facilityCount == 0 {
//		//None exist so far
//		apiURL += "?" + params.Encode()
//	} else {
//		// Some facilities exist
//		lastSyncDate := models.GetLastSyncDate(mflId)
//		if len(lastSyncDate) > 0 {
//			params.Add("_lastUpdated", fmt.Sprintf("gt%s", lastSyncDate))
//
//		} else {
//			params.Add("_lastUpdated", fmt.Sprintf("gt2023-01-01"))
//		}
//		apiURL += "?" + params.Encode()
//	}
//	log.WithField("URL", apiURL).Info("Facility URL")
//	startTime := time.Now() // .Format("2006-01-02 15:04:05")
//	log.WithField("StartTime", startTime.Format("2006-01-02 15:04:05")).Info("Starting to fetch facilities")
//	numberCreated := 0
//	numberUpdated := 0
//	numberIgnored := 0
//	// numberDeleted := 0
//
//	resp, err := utils.GetWithBasicAuth(apiURL, config.MFLIntegratorConf.API.MFLUser,
//		config.MFLIntegratorConf.API.MFLPassword)
//	if err != nil {
//		log.WithError(err).Info("Sync S001: Failed to fetch facilities from MFL")
//		return
//	}
//
//	if resp != nil {
//		v, _, _, err := jsonparser.Get(resp, "entry")
//		if err != nil {
//			log.WithError(err).Info("No entries found from MFL")
//			return
//		}
//		var entries []LocationEntry
//		err = json.Unmarshal(v, &entries)
//		if err != nil {
//			log.WithError(err).Info("Sync S002: Error unmarshaling response body")
//			fmt.Println(string(resp))
//			return
//		}
//		// Now we have our facilities
//		for i := range entries {
//			facility := GetOrgUnitFromFHIRLocation(entries[i])
//
//			facilityJSON, err := json.Marshal(facility)
//			if err != nil {
//				log.WithError(err).Info("Failed to marshal facility to JSON")
//				continue
//			}
//			if facility.Geometry.Type == "" {
//				facilityJSON = utils.PatchJSONObject(facilityJSON, []byte(`[
//				{"op": "remove", "path": "/geometry"}]`))
//			}
//			log.WithField("FacilityJson", string(facilityJSON)).Info("Facility Object")
//			if len(facility.Path) < 15 {
//				log.WithFields(log.Fields{"UID": facility.UID, "Name": facility.Name}).Warn("Found no Parent for facility")
//				var fj dbutils.MapAnything
//				_ = json.Unmarshal(facilityJSON, &fj)
//				ouFailure := models.OrgUnitFailure{UID: utils.GetUID(), FacilityUID: facility.UID, MFLUID: facility.MFLUID,
//					Reason: "Parent not found", Object: fj, Action: "MFL Fetch"}
//				ouFailure.NewOrgUnitFailure()
//				// log this one too
//				continue
//			}
//
//			if !facility.ExistsInDB() { // facility doesn't exist in our DB
//				facility.NewOrgUnit()
//
//				facility.UpdateMFLID(facility.MFLID)
//				facility.UpdateMFLParent(facility.MFLParent.String)
//				facility.UpdateMFLUID(facility.MFLUID)
//				facility.UpdateGeometry()
//				// Track the versions
//				var fj dbutils.MapAnything
//				_ = json.Unmarshal(facilityJSON, &fj)
//				fj["parent"] = facility.ParentByUID()
//				facilityRevision := models.OrgUnitRevision{
//					OrganisationUnitUID: dbutils.Int(facility.DBID()), Revision: 0, Definition: fj, UID: utils.GetUID(),
//				}
//				facilityRevision.NewOrgUnitRevision()
//				//facilityMetadata, err := GenerateOuMetadataByUID(facility.UID)
//
//				facilityMetadata := models.MetadataOu{}
//				facilityMetadata.Parent = facility.ParentByUID()
//
//				err := json.Unmarshal(facilityJSON, &facilityMetadata)
//				if err != nil {
//					log.WithError(err).Error("Failed to unmarshal facility into Metadata object")
//				}
//				numberCreated += 1
//				// Create Dispatcher2 request
//				// remove name and uid in organisationUnitGroups sent as part of metadata
//				facilityMetadata.OrganisationUnitGroups = lo.Map(
//					facilityMetadata.OrganisationUnitGroups, func(item dbutils.Map, _ int) dbutils.Map {
//						delete(item.Map(), "uid")
//						delete(item.Map(), "name")
//						return item
//					})
//
//				var ouM []models.MetadataOu
//				payload := make(map[string][]models.MetadataOu)
//				ouM = append(ouM, facilityMetadata)
//				payload["organisationUnits"] = ouM
//				reqBody, err := json.Marshal(payload)
//				if facilityMetadata.Geometry.Get("type", "") == "" {
//					reqBody = utils.PatchJSONObject(reqBody, []byte(
//						`[{"op": "remove", "path": "/organisationUnits/0/geometry"},
//						{"op": "remove", "path": "/organisationUnits/0/organisationUnitGroups"}]`))
//				}
//
//				if err != nil {
//					log.WithError(err).Error("Failed to marshal facility metadata")
//				}
//				year, week := time.Now().ISOWeek()
//				reqF := models.RequestForm{
//					Source: "localhost", Destination: "base_OU", ContentType: "application/json",
//					Year: fmt.Sprintf("%d", year), Week: fmt.Sprintf("%d", week),
//					Month: fmt.Sprintf("%d", int(time.Now().Month())), Period: "", Facility: facility.UID,
//					BatchID: batchId, SubmissionID: "", District: mflId,
//					CCServers: strings.Split(config.MFLIntegratorConf.API.MFLCCDHIS2CreateServers, ","),
//					Body:      string(reqBody), ObjectType: "ORGANISATION_UNIT", ReportType: "OU",
//				}
//				// Only queue facility if UID is valid
//				if facility.ValidateUID() && len(facility.Path) > 15 {
//					fRequest, err := reqF.Save(dbConn)
//					if err != nil {
//						log.WithError(err).Error("Failed to queue facility creation")
//					}
//					log.WithField("Facility", fRequest.UID()).Info("Queued Facility for addition")
//
//					// Now create requests to add orgunit to the right orgunit groups
//					ouGroupRequests := CreateOrgUnitGroupPayload(facilityMetadata)
//					requestForms := MakeOrgUnitGroupsAdditionRequests(
//						ouGroupRequests, dbutils.Int(fRequest.ID()), facilityMetadata.UID)
//					for _, rForm := range requestForms {
//						if rForm.Source == "" {
//							continue
//						}
//						rForm.BatchID = batchId // set the BatchID here
//						_, err := rForm.Save(dbConn)
//						if err != nil {
//							log.WithError(err).WithFields(log.Fields{"OU": rForm.UID, "Group": rForm.URLSuffix}).Error(
//								"Failed to queue request to add OU to OUGroup")
//							continue
//						}
//						log.WithFields(log.Fields{"OU": rForm.UID, "Group": rForm.URLSuffix}).Info(
//							"Queued Request to add facility to group!")
//					}
//				} else {
//					log.WithFields(log.Fields{"Facility": facility.Name, "UID": facility.UID, "MFLID": facility.MFLID}).Warn(
//						"Facility had invalid UID or has no parent thus not queued:")
//				}
//
//			} else {
//				// facility exists
//
//				var fj, latestRevision dbutils.MapAnything
//				_ = json.Unmarshal(facilityJSON, &fj)
//				_ = json.Unmarshal(facility.GetLatestRevision(), &latestRevision)
//				fj["parent"] = facility.Parent()
//				newMatchedOld, diffMap, err := models.CompareDefinition(fj, latestRevision)
//				if err != nil {
//					log.WithError(err).Info("Failed to make comparison between old and new facility JSON objects")
//				}
//				if newMatchedOld {
//					// new definition matched the old
//					log.WithFields(log.Fields{
//						"UID": facility.UID, "ValidUID": facility.ValidateUID(),
//						"Parent": facility.ParentID,
//						"Diff":   diffMap}).Info("========= Facility has no changes =======")
//					numberIgnored += 1
//					continue
//				} else {
//					// metadataPayload := models.GenerateMetadataPayload(fj)
//					old, _ := json.Marshal(latestRevision)
//					newDefinition, _ := json.Marshal(facility)
//					metadataPayloadFromDiff := models.GenerateMetadataPayload(diffMap)
//					log.WithFields(log.Fields{
//						"UID": facility.UID, "ValidUID": facility.ValidateUID(),
//						"New":  string(newDefinition),
//						"OLD":  string(old),
//						"Diff": diffMap, "parent": facility.ParentID,
//						"MetadataFromDiff": metadataPayloadFromDiff,
//						// "MetadataPalyload": metadataPayload,
//					}).Info(
//						"::::::::: Facility had some changes :::::::::")
//
//					// make a revision
//					facilityRevision := models.OrgUnitRevision{
//						OrganisationUnitUID: dbutils.Int(facility.DBID()), Revision: 0, Definition: fj, UID: utils.GetUID(),
//						District: mflId,
//					}
//					facilityRevision.NewOrgUnitRevision()
//
//					// Generate Metadata Update object - for facility with valid UID
//					if facility.ValidateUID() {
//						reqF := GenerateUpdateMetadataRequest(metadataPayloadFromDiff, facility.UID, mflId)
//						reqF.BatchID = batchId
//						_, err := reqF.Save(dbConn)
//						if err != nil {
//							log.WithError(err).WithFields(log.Fields{"OU": reqF.UID}).Error(
//								"Failed to queue - update request for OU")
//							continue
//						}
//					}
//
//					// We want to update OU Groups as well
//					facilityMetadata := models.MetadataOu{}
//
//					err := json.Unmarshal(facilityJSON, &facilityMetadata)
//					if err != nil {
//						log.WithError(err).Error("Updating OU: Failed to unmarshal facility into Metadata object")
//					}
//					// Now create requests to add orgunit to the right orgunit groups
//					ouGroupRequests := CreateOrgUnitGroupPayload(facilityMetadata)
//					requestForms := MakeOrgUnitGroupsAdditionRequests(
//						ouGroupRequests, dbutils.Int(0), facilityMetadata.UID)
//					for _, rForm := range requestForms {
//						if rForm.Source == "" {
//							continue
//						}
//						rForm.BatchID = batchId // set the BatchID here
//						_, err := rForm.Save(dbConn)
//						if err != nil {
//							log.WithError(err).WithFields(log.Fields{"OU": rForm.UID, "Group": rForm.URLSuffix}).Error(
//								"Updating OU: Failed to queue request to add OU to OUGroup")
//							continue
//						}
//						log.WithFields(log.Fields{"OU": rForm.UID, "Group": rForm.URLSuffix}).Info(
//							"Updating OU: Queued Request to add facility to group!")
//					}
//
//					numberUpdated += 1
//				}
//
//			}
//		}
//	}
//
//	endTime := time.Now() // .Format("2006-01-02 15:04:05")
//
//	syncLog := models.SyncLog{
//		UID:           utils.GetUID(),
//		Started:       startTime,
//		MFLID:         mflId,
//		BatchID:       batchId,
//		Stopped:       endTime,
//		NumberCreated: dbutils.Int(numberCreated),
//		NumberUpdated: dbutils.Int(numberUpdated),
//		NumberIgnored: dbutils.Int(numberIgnored),
//		// ServersSyncLog:
//	}
//	syncLog.LogSync()
//
//	log.WithFields(log.Fields{
//		"EndTime":       endTime,
//		"NumberCreated": numberCreated,
//		"NumberUpdated": numberUpdated}).Info("Stopped processing fetched facilities")
//}

func FetchFacilities2(mflId, batchId string) {
	dbConn := db.GetDB()

	// 1) Sanity check: ensure levels above facility exist
	var upperLevels int64
	err := dbConn.Get(&upperLevels,
		"SELECT count(*) FROM organisationunit WHERE hierarchylevel = $1 - 1",
		config.MFLIntegratorConf.API.DHIS2FacilityLevel,
	)
	if err != nil {
		log.WithError(err).Info("Error reading records on level above facility")
		return
	}
	if upperLevels == 0 {
		log.Warn("Cannot Synchronize Facilities when hierarchy levels above facility are missing")
		return
	}

	// 2) Check if we already have facilities to decide updatedSince
	var facilityCount int64
	err = dbConn.Get(&facilityCount,
		"SELECT count(*) FROM organisationunit WHERE hierarchylevel = $1 LIMIT 1",
		config.MFLIntegratorConf.API.DHIS2FacilityLevel,
	)
	if err != nil {
		log.WithError(err).Info("Error getting the count of health facilities")
		return
	}

	// 3) Build the new Resty client (Basic Auth)
	apiBaseURL := config.MFLIntegratorConf.API.MFLBaseURL
	cli := clients.NewMFLApi(
		apiBaseURL,
		clients.WithBasicAuth(
			config.MFLIntegratorConf.API.MFLUser,
			config.MFLIntegratorConf.API.MFLPassword,
		),
		clients.WithDebug(),
	)

	// 4) Build query params for the new API
	q := clients.FacilitiesQuery{
		Page:     1,
		PageSize: 200, // max supported
		Level:    fmt.Sprintf("%d", config.MFLIntegratorConf.API.DHIS2FacilityLevel),
		// IMPORTANT: your new API filters are name-based partial matches.
		// If mflId is a district name, this is correct.
		// If mflId is an ID/UID, you should map it to a name first.
	}
	// XXX
	if strings.TrimSpace(mflId) != "" {
		q.DistrictID = mflId
	}

	if facilityCount > 0 {
		lastSyncDate := models.GetLastSyncDate(mflId)
		if strings.TrimSpace(lastSyncDate) != "" {
			// the new API expects YYYY-MM-DD
			q.UpdatedSince = lastSyncDate
		} else {
			q.UpdatedSince = "2023-01-01"
		}
	}

	startTime := time.Now()
	log.WithField("StartTime", startTime.Format("2006-01-02 15:04:05")).Info("Starting to fetch facilities")

	numberCreated := 0
	numberUpdated := 0
	numberIgnored := 0
	parentOrgUnitCache := make(map[string]models.OrganisationUnit)
	usedUIDCache := make(map[string]string)
	orgUnitGroupUIDCache := make(map[string]string)

	// 5) Stream from the new API (no huge memory usage)
	ctx := context.Background()
	err = cli.StreamFacilities(ctx, q, func(f clients.Facility) error {
		// Convert API Facility -> your internal OrgUnit model
		//
		// You MUST implement this adapter based on your existing structures.
		// Example signature you should add somewhere:
		//   func GetOrgUnitFromFacility(f models.Facility) models.OrganisationUnit
		//
		// If you still need FHIR-style mapping, do it inside this function.

		facility := GetOrgUnitFromFacility(f, parentOrgUnitCache, usedUIDCache, orgUnitGroupUIDCache)

		facilityMap, facilityJSON, err := buildFacilityPayload(facility)
		if err != nil {
			log.WithError(err).Info("Failed to build facility payload")
			return nil
		}
		log.WithField("FacilityJson", string(facilityJSON)).Info("Facility Object")

		if len(facility.Path) < 15 {
			log.WithFields(log.Fields{
				"UID": facility.UID, "Name": facility.Name, "Path": facility.Path}).Warn("Found no Parent for facility")
			ouFailure := models.OrgUnitFailure{
				UID:         utils.GetUID(),
				FacilityUID: facility.UID,
				MFLUID:      facility.MFLUID,
				Reason:      "Parent not found",
				Object:      facilityMap,
				Action:      "MFL Fetch",
			}
			ouFailure.NewOrgUnitFailure()
			return nil
		}

		syncState, existsInDB, err := models.GetOrganisationUnitSyncState(facility.UID)
		if err != nil {
			log.WithError(err).WithField("UID", facility.UID).Error("Failed to load facility sync state")
			return nil
		}

		if !existsInDB {
			// facility doesn't exist in our DB
			orgUnitID := facility.NewOrgUnit()
			if orgUnitID == 0 {
				return nil
			}

			// Track the versions
			fj := cloneMapAnything(facilityMap)
			fj["parent"] = facility.ParentRef()

			facilityRevision := models.OrgUnitRevision{
				OrganisationUnitUID: dbutils.Int(orgUnitID),
				Revision:            0,
				Definition:          fj,
				UID:                 utils.GetUID(),
			}
			facilityRevision.NewOrgUnitRevision()

			facilityMetadata := makeMetadataOu(facility)
			facilityMetadata.Parent = facility.ParentRef()
			numberCreated++

			// remove name and uid in organisationUnitGroups sent as part of metadata
			facilityMetadata.OrganisationUnitGroups = lo.Map(
				facilityMetadata.OrganisationUnitGroups,
				func(item dbutils.Map, _ int) dbutils.Map {
					delete(item.Map(), "uid")
					delete(item.Map(), "name")
					return item
				},
			)

			reqBody, err := metadataRequestBody(facilityMetadata, true)
			if err != nil {
				log.WithError(err).Error("Failed to marshal facility metadata")
			}

			year, week := time.Now().ISOWeek()
			reqF := models.RequestForm{
				Source:       "localhost",
				Destination:  "base_OU",
				ContentType:  "application/json",
				Year:         fmt.Sprintf("%d", year),
				Week:         fmt.Sprintf("%d", week),
				Month:        fmt.Sprintf("%d", int(time.Now().Month())),
				Period:       "",
				Facility:     facility.UID,
				BatchID:      batchId,
				SubmissionID: "",
				District:     mflId,
				CCServers:    strings.Split(config.MFLIntegratorConf.API.CCDHIS2CreateServers, ","),
				Body:         string(reqBody),
				ObjectType:   "ORGANISATION_UNIT",
				ReportType:   "OU",
			}

			if facility.ValidateUID() && len(facility.Path) > 15 {
				fRequest, err := reqF.Save(dbConn)
				if err != nil {
					log.WithError(err).Error("Failed to queue facility creation")
				}
				log.WithField("Facility", fRequest.UID()).Info("Queued Facility for addition")

				ouGroupRequests := CreateOrgUnitGroupPayload(facilityMetadata)
				requestForms := MakeOrgUnitGroupsAdditionRequests(
					ouGroupRequests,
					dbutils.Int(fRequest.ID()),
					facilityMetadata.UID,
				)
				batchRequestForms := make([]models.RequestForm, 0, len(requestForms))
				for _, rForm := range requestForms {
					if rForm.Source == "" {
						continue
					}
					rForm.BatchID = batchId
					batchRequestForms = append(batchRequestForms, rForm)
				}
				err = models.SaveRequestFormsBatch(dbConn, batchRequestForms)
				if err != nil {
					log.WithError(err).WithField("OU", facilityMetadata.UID).Error(
						"Failed to batch queue requests to add OU to groups",
					)
				} else if len(batchRequestForms) > 0 {
					log.WithFields(log.Fields{"OU": facilityMetadata.UID, "Count": len(batchRequestForms)}).Info(
						"Queued requests to add facility to groups",
					)
				}
			} else {
				log.WithFields(log.Fields{"Facility": facility.Name, "UID": facility.UID, "MFLID": facility.MFLID}).Warn(
					"Facility had invalid UID or has no parent thus not queued:",
				)
			}

			return nil
		}

		// facility exists
		facility.ParentUID = syncState.ParentUID
		fj := cloneMapAnything(facilityMap)
		fj["parent"] = facility.ParentRef()
		newMatchedOld, diffMap, err := models.CompareDefinition(fj, syncState.LatestRevision)
		if err != nil {
			log.WithError(err).Info("Failed to make comparison between old and new facility JSON objects")
		}

		if newMatchedOld {
			log.WithFields(log.Fields{
				"UID":      facility.UID,
				"ValidUID": facility.ValidateUID(),
				"Parent":   facility.ParentID,
				"Diff":     diffMap,
			}).Info("========= Facility has no changes =======")
			numberIgnored++
			return nil
		}

		old, _ := json.Marshal(syncState.LatestRevision)
		newDefinition, _ := json.Marshal(fj)

		metadataPayloadFromDiff := models.GenerateMetadataPayload(diffMap)
		log.WithFields(log.Fields{
			"UID":              facility.UID,
			"ValidUID":         facility.ValidateUID(),
			"New":              string(newDefinition),
			"OLD":              string(old),
			"Diff":             diffMap,
			"parent":           facility.ParentID,
			"MetadataFromDiff": metadataPayloadFromDiff,
		}).Info("::::::::: Facility had some changes :::::::::")

		// revision
		facilityRevision := models.OrgUnitRevision{
			OrganisationUnitUID: dbutils.Int(syncState.DBID),
			Revision:            0,
			Definition:          fj,
			UID:                 utils.GetUID(),
			District:            mflId,
		}
		facilityRevision.NewOrgUnitRevision()

		// update request (only if UID is valid)
		if facility.ValidateUID() {
			reqF := GenerateUpdateMetadataRequest(metadataPayloadFromDiff, facility.UID, mflId)
			reqF.BatchID = batchId
			_, err := reqF.Save(dbConn)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{"OU": reqF.UID}).Error(
					"Failed to queue - update request for OU",
				)
				return nil
			}
		}

		// OU Groups update
		facilityMetadata := makeMetadataOu(facility)

		ouGroupRequests := CreateOrgUnitGroupPayload(facilityMetadata)
		requestForms := MakeOrgUnitGroupsAdditionRequests(
			ouGroupRequests,
			dbutils.Int(0),
			facilityMetadata.UID,
		)
		batchRequestForms := make([]models.RequestForm, 0, len(requestForms))
		for _, rForm := range requestForms {
			if rForm.Source == "" {
				continue
			}
			rForm.BatchID = batchId
			batchRequestForms = append(batchRequestForms, rForm)
		}
		err = models.SaveRequestFormsBatch(dbConn, batchRequestForms)
		if err != nil {
			log.WithError(err).WithField("OU", facilityMetadata.UID).Error(
				"Updating OU: Failed to batch queue requests to add OU to groups",
			)
		} else if len(batchRequestForms) > 0 {
			log.WithFields(log.Fields{"OU": facilityMetadata.UID, "Count": len(batchRequestForms)}).Info(
				"Updating OU: Queued requests to add facility to groups",
			)
		}

		numberUpdated++
		return nil
	})
	if err != nil {
		log.WithError(err).Info("Sync S001: Failed to fetch facilities from MFL (new client)")
		return
	}

	endTime := time.Now()

	syncLog := models.SyncLog{
		UID:           utils.GetUID(),
		Started:       startTime,
		MFLID:         mflId,
		BatchID:       batchId,
		Stopped:       endTime,
		NumberCreated: dbutils.Int(numberCreated),
		NumberUpdated: dbutils.Int(numberUpdated),
		NumberIgnored: dbutils.Int(numberIgnored),
	}
	syncLog.LogSync()

	log.WithFields(log.Fields{
		"EndTime":       endTime,
		"NumberCreated": numberCreated,
		"NumberUpdated": numberUpdated,
	}).Info("Stopped processing fetched facilities")
}

const districtSQL = `SELECT mfluid FROM organisationunit WHERE hierarchylevel = 
    (SELECT level from orgunitlevel where name = $1) ORDER BY name`

// FetchFacilitiesByDistrict fetches Facilities from MFL by district - if we don't get mflids then fetch everything
func FetchFacilitiesByDistrict() {
	log.Info("Going to Fetch Facilities By District")
	batchId := utils.GetUID()
	rows, err := db.GetDB().Queryx(districtSQL, config.MFLIntegratorConf.API.DHIS2DistrictLevelName)
	if err != nil {
		log.WithError(err).Info("Failed to get district mfluids from database")
		FetchFacilities2("", batchId)
		return
	}

	districts := make([]string, 0)
	for rows.Next() {
		var district string
		err := rows.Scan(&district)
		if err != nil {
			log.WithError(err).Error("Error reading district mflid from database:")
			continue
		}
		districts = append(districts, district)
	}
	_ = rows.Close()

	for _, district := range districts {
		log.WithField("District", district).Info("Syncing Facilities")
		FetchFacilities2(district, batchId)
	}
	log.Info("Finished Fetching Facilities By District")
}

// GetExtensions gets extensions within an entry in a FHIR bundle
func GetExtensions(extensions []fhir.Extension) map[string]any {
	resp := make(map[string]any)
	for i := range extensions {
		if extensions[i].ValueCode != nil {
			resp[extensions[i].Url] = *extensions[i].ValueCode
		}
		if extensions[i].ValueString != nil {
			resp[extensions[i].Url] = *extensions[i].ValueString

		}
		if extensions[i].ValueInteger != nil {
			resp[extensions[i].Url] = fmt.Sprintf(
				"%d", *extensions[i].ValueInteger)
		}

	}
	return resp
}

// getPhoneAndEmail retrieves phone and email from ContactPoint of a FHIR Location
func getPhoneAndEmail(telecom []fhir.ContactPoint) (string, string) {
	phone, email := "", ""
	for _, v := range telecom {
		if v.System.String() == "phone" {
			if v.Value != nil && *v.Value != "Unknown" {
				phone = *v.Value
			}
		}
		if v.System.String() == "email" {
			if v.Value != nil && *v.Value != "Unknown" {
				email = *v.Value
			}
		}
	}
	return phone, email
}

func SanitizeShortName(name string) string {
	if len(name) <= 50 {
		return name
	}
	newName := strings.ReplaceAll(name, "Health Centre", "HC")
	newName = strings.ReplaceAll(newName, "Hospital", "")
	if len(newName) <= 50 {
		return newName
	}
	return newName[:50]
}

// GetOrgUnitFromFHIRLocation is used to generate DHIS2 OrganisationUnit from FHIR Location
func GetOrgUnitFromFHIRLocation(location LocationEntry) models.OrganisationUnit {
	_url := location.FullURL
	name := *location.Resource.Name
	id := *location.Resource.Id
	parent := ""
	if location.Resource.PartOf != nil {
		if location.Resource.PartOf.Reference != nil {
			parent = *location.Resource.PartOf.Reference
		}
	}
	parent = strings.ReplaceAll(parent, "Location/", "")
	extensions := GetExtensions(location.Resource.Extension)
	address := ""
	if location.Resource.Address != nil {
		if location.Resource.Address.Text != nil {
			address = *location.Resource.Address.Text
		}
	}
	lat := location.Resource.Position.Latitude
	long := location.Resource.Position.Longitude
	// XXX https://docs.dhis2.org/en/use/user-guides/dhis-core-version-240/configuring-the-system/maps.html
	coordinates := []json.Number{long, lat} // XXX DHIS2 uses  EPSG:4326 which does longitude/latitude
	if lat.String() == "0.0" && long.String() == "0.0" {
		coordinates = []json.Number{}
	}
	// var point dbutils.JSON
	coordinateBytes, err := json.Marshal(coordinates)
	if err != nil {
		log.WithError(err).Info("Failed to marshal coordinates", coordinates)
		coordinateBytes, _ = json.Marshal([]json.Number{})
	}

	phone, email := getPhoneAndEmail(location.Resource.Telecom)
	tempOpening, _ := time.Parse("2006-01-02", "1970-01-01")
	levelOfCare := ""
	if level, ok := extensions["levelOfCare"]; ok {
		levelOfCare = level.(string)
	}
	parentOrgUnit := models.GetOrgUnitByMFLID(parent)
	facility := models.OrganisationUnit{}
	mflUniqueIdentifier := ""
	if mflUniqueId, ok := extensions["uniqueIdentifier"]; ok {
		mflUniqueIdentifier = mflUniqueId.(string)
	}
	historicalId := ""
	if histId, ok := extensions["historicalIdentifier"]; ok {
		historicalId = histId.(string)
	} else {
		// no historical identifier
		uid := models.GetUIDByMFLUID(mflUniqueIdentifier)
		if len(uid) == 11 {
			historicalId = uid
		} else {
			historicalId = utils.GetUID()
			ou := models.UsedUID{UID: historicalId, MFLUID: mflUniqueIdentifier}
			ou.NewUsedUID() // log it to DB
		}
		log.WithFields(log.Fields{"Facility": name, "UID": historicalId}).Debug("No Historical identifier found for facility")
	}

	facility.OpeningDate = tempOpening.Format("2006-01-02")
	facility.ID = historicalId
	facility.UID = historicalId
	facility.Name = name
	facility.ShortName = SanitizeShortName(name)
	facility.URL = _url
	facility.MFLID = id
	facility.MFLUID = mflUniqueIdentifier
	facility.MFLParent = sql.NullString{String: parent, Valid: true}
	facility.Level = parentOrgUnit.Level + 1
	facility.ParentID = parentOrgUnit.ParentID
	facility.Path = parentOrgUnit.Path + "/" + historicalId
	facility.Address = address
	facility.PhoneNumber = phone
	facility.Email = email
	facility.Extras = extensions

	attV := []models.AttributeValue{{Value: mflUniqueIdentifier, Attribute: models.Attribute{
		ID: config.MFLIntegratorConf.API.DHIS2OUMFLIDAttributeID,
		// Name: "",
	}}}
	// attributeList = append(attributeList, )
	attributeValues, err := json.Marshal(attV)
	if err == nil {
		facility.AttributeValues = attributeValues
	} else {
		log.WithError(err).Info("Failed to marshal attribute values")
	}

	if lat.String() != "0.0" && long.String() != "0.0" {
		facility.Geometry = models.Geometry{Type: "Point", Coordinates: coordinateBytes}
	}

	ouUID := models.GetOuGroupUIDByName(levelOfCare)
	facility.OrgUnitGroups = []models.OrgUnitGroup{
		{Name: levelOfCare, UID: ouUID, ID: ouUID},
	}

	return facility
}

// GetOrgUnitFromFacility converts the new Facilities API "Facility" payload into your DHIS2 OrganisationUnit model.
// This replaces GetOrgUnitFromFHIRLocation, where LocationEntry ~= Facility now.
func GetOrgUnitFromFacility(
	f clients.Facility,
	parentOrgUnitCache map[string]models.OrganisationUnit,
	usedUIDCache map[string]string,
	orgUnitGroupUIDCache map[string]string,
) models.OrganisationUnit {
	name := f.Name
	if strings.TrimSpace(name) == "" {
		name = f.ShortName
	}

	// In the new API payload:
	// - mfl_uid seems like the stable UID (DHIS2-ish uid length)
	// - historical_id is what your existing code used as OrgUnit UID/ID
	mflUID := strings.TrimSpace(f.MFLUID)
	historicalID := strings.TrimSpace(f.HistoricalID)

	// If historical_id is missing, fall back safely (keep behavior similar to your old code)
	if historicalID == "" {
		uid, ok := usedUIDCache[mflUID]
		if !ok {
			uid = models.GetUIDByMFLUID(mflUID)
			usedUIDCache[mflUID] = uid
		}
		if len(uid) == 11 {
			historicalID = uid
		} else {
			historicalID = utils.GetUID()
			ou := models.UsedUID{UID: historicalID, MFLUID: mflUID}
			ou.NewUsedUID()
			usedUIDCache[mflUID] = historicalID
		}
		log.WithFields(log.Fields{"Facility": name, "UID": historicalID}).Debug("No historical_id found for facility")
	}

	// Parent resolution:
	parentKey := ""
	if f.Parent != nil {
		parentKey = strings.TrimSpace(f.Parent.MFLUID)
	}
	parentOrgUnit := models.OrganisationUnit{}
	if parentKey != "" {
		if cachedParent, ok := parentOrgUnitCache[parentKey]; ok {
			parentOrgUnit = cachedParent
		} else {
			// let's use MFLUD
			parentOrgUnit = models.GetOrgUnitByMFLUID(parentKey)
			parentOrgUnitCache[parentKey] = parentOrgUnit
		}
	}

	// Address
	address := ""
	if f.Address != nil {
		address = strings.TrimSpace(*f.Address)
	}

	// phone/email from contact fields (new payload has these)
	phone := ""
	if f.ContactPersonMobile != nil {
		phone = strings.TrimSpace(*f.ContactPersonMobile)
	}
	email := ""
	if f.ContactPersonEmail != nil {
		email = strings.TrimSpace(*f.ContactPersonEmail)
	}

	// Opening date: old code defaulted to 1970-01-01
	tempOpening, _ := time.Parse("2006-01-02", "1970-01-01")

	// Geometry: DHIS2 uses lon/lat (EPSG:4326)
	var geometry models.Geometry
	if f.Latitude != nil && f.Longitude != nil {
		lat := *f.Latitude
		lon := *f.Longitude
		if lat != 0.0 && lon != 0.0 {
			coords, err := json.Marshal([]float64{lon, lat})
			if err != nil {
				log.WithError(err).Info("Failed to marshal coordinates")
			} else {
				geometry = models.Geometry{Type: "Point", Coordinates: coords}
			}
		}
	}

	// Extras: mimic the old extension-map behavior with a lightweight map
	extras := map[string]any{
		"historical_id": f.HistoricalID,
		"bed_capacity":  f.BedCapacity,
		"ownership":     refName(f.Ownership),
		"authority":     refName(f.Authority),
		"region":        refName(f.Region),
		"district":      refName(f.District),
		"subcounty":     refName(f.Subcounty),
	}

	// Level of care (OU group):
	levelOfCare := ""
	if f.FacilityLevel != nil {
		levelOfCare = strings.TrimSpace(f.FacilityLevel.Name)
	}

	// Build OrganisationUnit
	facility := models.OrganisationUnit{}
	facility.OpeningDate = tempOpening.Format("2006-01-02")
	facility.ID = historicalID
	facility.UID = historicalID
	facility.Name = name
	facility.ShortName = SanitizeShortName(name)

	// Old code stored FHIR fullUrl. New payload doesn't have it; keep blank or derive if you have a canonical URL.
	facility.URL = ""

	// Keep your old semantics:
	// - MFLID: use "identifier" (since old id came from FHIR resource.Id; now identifier is the concrete ID)
	// - MFLUID: use mfl_uid
	facility.MFLID = strings.TrimSpace(f.Identifier)
	facility.MFLUID = mflUID

	if parentKey != "" {
		facility.MFLParent = sql.NullString{String: parentKey, Valid: true}
	} else {
		facility.MFLParent = sql.NullString{Valid: false}
	}

	// Hierarchy (best-effort; depends on parentOrgUnit existence)
	facility.Level = parentOrgUnit.Level + 1
	facility.ParentUID = parentOrgUnit.UID
	if parentOrgUnit.ID != "" {
		parentID, err := strconv.ParseInt(parentOrgUnit.ID, 10, 64)
		if err != nil {
			log.WithError(err).WithField("ParentID", parentOrgUnit.ID).Debug("Failed to parse parent organisation unit id")
		} else {
			facility.ParentID = dbutils.Int(parentID)
		}
	}
	if strings.TrimSpace(parentOrgUnit.Path) != "" {
		facility.Path = parentOrgUnit.Path + "/" + historicalID
	}

	facility.Address = address
	facility.PhoneNumber = phone
	facility.Email = email
	facility.Extras = extras

	// Attribute values (same as before): store MFL UID into the configured attribute
	attV := []models.AttributeValue{{
		Value: mflUID,
		Attribute: models.Attribute{
			ID: config.MFLIntegratorConf.API.DHIS2OUMFLIDAttributeID,
		},
	}}
	if b, err := json.Marshal(attV); err == nil {
		facility.AttributeValues = b
	} else {
		log.WithError(err).Info("Failed to marshal attribute values")
	}

	// Geometry
	if geometry.Type != "" {
		facility.Geometry = geometry
	}

	// OrgUnitGroups from level-of-care
	if levelOfCare != "" {
		ouUID, ok := orgUnitGroupUIDCache[levelOfCare]
		if !ok {
			ouUID = models.GetOuGroupUIDByName(levelOfCare)
			orgUnitGroupUIDCache[levelOfCare] = ouUID
		}
		facility.OrgUnitGroups = []models.OrgUnitGroup{
			{Name: levelOfCare, UID: ouUID, ID: ouUID},
		}
	}

	return facility
}

func nilString(p *string) any {
	if p == nil {
		return nil
	}
	s := strings.TrimSpace(*p)
	if s == "" {
		return nil
	}
	return s
}

func buildFacilityPayload(facility models.OrganisationUnit) (dbutils.MapAnything, []byte, error) {
	facilityJSON, err := json.Marshal(facility)
	if err != nil {
		return nil, nil, err
	}

	var facilityMap dbutils.MapAnything
	if err := json.Unmarshal(facilityJSON, &facilityMap); err != nil {
		return nil, nil, err
	}
	if facility.Geometry.Type == "" {
		delete(facilityMap, "geometry")
	}

	normalizedJSON, err := json.Marshal(facilityMap)
	if err != nil {
		return nil, nil, err
	}
	return facilityMap, normalizedJSON, nil
}

func cloneMapAnything(src dbutils.MapAnything) dbutils.MapAnything {
	cloned := make(dbutils.MapAnything, len(src))
	for k, v := range src {
		cloned[k] = v
	}
	return cloned
}

func makeMetadataOu(facility models.OrganisationUnit) models.MetadataOu {
	metadata := models.MetadataOu{
		UID:         facility.UID,
		Level:       facility.Level,
		Name:        facility.Name,
		ShortName:   facility.ShortName,
		Description: facility.Description,
		Email:       facility.Email,
		Address:     facility.Address,
		Phone:       facility.PhoneNumber,
		OpeningDate: facility.OpeningDate,
	}

	if facility.ValidateUID() {
		metadata.Parent = facility.ParentRef()
	}
	if facility.Geometry.Type != "" {
		geometry := dbutils.Map{}
		geometry.Set("type", facility.Geometry.Type)
		var coords any
		if err := json.Unmarshal(facility.Geometry.Coordinates, &coords); err == nil {
			geometry.Set("coordinates", coords)
			metadata.Geometry = geometry
		}
	}
	if len(facility.AttributeValues) > 0 {
		var attributeValues []dbutils.Map
		if err := json.Unmarshal(facility.AttributeValues, &attributeValues); err == nil {
			metadata.AttributeValues = attributeValues
		}
	}
	if len(facility.OrgUnitGroups) > 0 {
		metadata.OrganisationUnitGroups = make([]dbutils.Map, 0, len(facility.OrgUnitGroups))
		for _, group := range facility.OrgUnitGroups {
			groupMap := dbutils.Map{}
			groupMap.Set("id", group.ID)
			metadata.OrganisationUnitGroups = append(metadata.OrganisationUnitGroups, groupMap)
		}
	}
	return metadata
}

func metadataRequestBody(metadata models.MetadataOu, omitGroups bool) ([]byte, error) {
	payloadOU := metadata
	if payloadOU.Geometry.Get("type", "") == "" {
		payloadOU.Geometry = dbutils.Map{}
	}
	if omitGroups {
		payloadOU.OrganisationUnitGroups = nil
	}

	payload := map[string][]models.MetadataOu{
		"organisationUnits": {payloadOU},
	}

	return json.Marshal(payload)
}

func refName(r *clients.RefEntity) any {
	if r == nil {
		return nil
	}
	s := strings.TrimSpace(r.Name)
	if s == "" {
		return nil
	}
	return s
}

// SyncLocationsToDHIS2Instances syncs the DHIS2 base hierarchy to subscribing DHIS2 instances
func SyncLocationsToDHIS2Instances() {
	for _, serverName := range strings.Split(config.MFLIntegratorConf.API.CCDHIS2HierarchyServers, ",") {
		models.SyncLocationsToServer(serverName)
	}
}

// CreateOrgUnitGroupPayload ...
func CreateOrgUnitGroupPayload(ou models.MetadataOu) map[string][]byte {
	ouGroupReqs := make(map[string][]byte)
	if len(ou.OrganisationUnitGroups) > 0 {
		for _, g := range ou.OrganisationUnitGroups {
			groupId := g.Get("id", "")
			ouGroupReqs[groupId.(string)] = []byte(fmt.Sprintf(
				`[{"op": "add", "path": "/organisationUnits/-", "value": {"id": "%s"}}]`, ou.UID))
		}

	}
	return ouGroupReqs
}

// MakeOrgUnitGroupsAdditionRequests ....
func MakeOrgUnitGroupsAdditionRequests(
	ouGroupPayloads map[string][]byte, dependency dbutils.Int, facilityUID string) []models.RequestForm {
	var requests []models.RequestForm
	for k, v := range ouGroupPayloads {
		if len(k) == 0 {
			continue
		}
		year, week := time.Now().ISOWeek()
		var reqF = models.RequestForm{
			// DependsOn: dependency,
			Source: "localhost", Destination: "base_OU_GroupAdd", ContentType: "application/json-patch+json",
			Year: fmt.Sprintf("%d", year), Week: fmt.Sprintf("%d", week),
			Month: fmt.Sprintf("%d", int(time.Now().Month())), Period: "", Facility: facilityUID, BatchID: "", SubmissionID: "",
			CCServers: strings.Split(config.MFLIntegratorConf.API.CCDHIS2OuGroupAddServers, ","),
			URLSuffix: fmt.Sprintf("/%s", k),
			Body:      string(v), ObjectType: "ORGUNIT_GROUP_ADD", ReportType: "OUGROUP_ADD",
		}
		if dependency > 0 {
			reqF.DependsOn = dependency
		}
		requests = append(requests, reqF)
	}
	return requests
}

func GenerateUpdateMetadataRequest(update []models.MetadataObject, facilityUID string, district string) models.RequestForm {
	req := models.RequestForm{}
	body, err := json.Marshal(update)
	if err != nil {
		log.WithError(err).Error("Failed to parse facility update metadata")
		return req
	}
	year, week := time.Now().ISOWeek()
	reqF := models.RequestForm{
		Source: "localhost", Destination: "base_OU_Update", ContentType: "application/json-patch+json",
		Year: fmt.Sprintf("%d", year), Week: fmt.Sprintf("%d", week),
		Month: fmt.Sprintf("%d", int(time.Now().Month())), Period: "", Facility: facilityUID, BatchID: "", SubmissionID: "",
		District:  district,
		CCServers: strings.Split(config.MFLIntegratorConf.API.CCDHIS2UpdateServers, ","),
		URLSuffix: fmt.Sprintf("/%s", facilityUID),
		Body:      string(body),
	}

	return reqF
}

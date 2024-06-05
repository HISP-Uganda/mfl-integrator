package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
	"net/url"
	"strings"
	"time"
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

		apiURL := config.MFLIntegratorConf.API.MFLDHIS2BaseURL + "/organisationUnitLevels.json"
		p := url.Values{}
		p.Add("fields", "id,name,level")
		p.Add("paging", "false")

		ouURL := apiURL + "?" + p.Encode()
		fmt.Println(ouURL)
		resp, err := utils.GetWithBasicAuth(ouURL, config.MFLIntegratorConf.API.MFLDHIS2User,
			config.MFLIntegratorConf.API.MFLDHIS2Password)
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

		apiURL := config.MFLIntegratorConf.API.MFLDHIS2BaseURL + "/organisationUnitGroups.json"
		p := url.Values{}
		p.Add("fields", "id,name,shortName")
		p.Add("paging", "false")

		ouURL := apiURL + "?" + p.Encode()
		fmt.Println(ouURL)
		resp, err := utils.GetWithBasicAuth(ouURL, config.MFLIntegratorConf.API.MFLDHIS2User,
			config.MFLIntegratorConf.API.MFLDHIS2Password)
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
	apiURL := config.MFLIntegratorConf.API.MFLDHIS2BaseURL + "/attributes.json"
	p := url.Values{}
	fields := `id~rename(uid),name,displayName,code,shortName,valueType`
	p.Add("fields", fields)
	p.Add("paging", "false")
	p.Add("filter", "organisationUnitAttribute:eq:true")
	attrURL := apiURL + "?" + p.Encode()
	resp, err := utils.GetWithBasicAuth(attrURL, config.MFLIntegratorConf.API.MFLDHIS2User,
		config.MFLIntegratorConf.API.MFLDHIS2Password)
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

		apiURL := config.MFLIntegratorConf.API.MFLDHIS2BaseURL + "/organisationUnits.json"
		p := url.Values{}
		fields := `id,name,displayName,code,shortName,openingDate,phoneNumber,` +
			`path,level,description,geometry,organisatoinUnitGroups[id,name]`
		p.Add("fields", fields)
		p.Add("paging", "false")

		for i := 1; i < config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel; i++ {
			p.Add("level", fmt.Sprintf("%d", i))
			ouURL := apiURL + "?" + p.Encode()
			fmt.Println(ouURL)
			resp, err := utils.GetWithBasicAuth(ouURL, config.MFLIntegratorConf.API.MFLDHIS2User,
				config.MFLIntegratorConf.API.MFLDHIS2Password)
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
				ous[i].NewOrgUnit()
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
		config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel)
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
					`SELECT id, uid, name, mflparent, parentid FROM organisationunit WHERE name=$1 AND hierarchylevel =$2`,
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

// FetchFacilities pulls facilities from the MFL after loading and matching
func FetchFacilities(mflId, batchId string) {
	dbConn := db.GetDB()
	var upperLevels int64
	err := dbConn.Get(&upperLevels, "SELECT count(*) FROM organisationunit WHERE hierarchylevel = $1 - 1",
		config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel)
	if err != nil {
		log.WithError(err).Info("Error reading records on level above facility")
		return
	}
	if upperLevels == 0 {
		log.Warn("Cannot Synchronize Facilities when hierarchy levels above facility are missing")
		return
	}
	var facilityCount int64
	err = dbConn.Get(&facilityCount, "SELECT count(*) FROM organisationunit WHERE hierarchylevel = $1 LIMIT 1",
		config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel)
	if err != nil {
		log.WithError(err).Info("Error getting the count of health facilities")
		return
	}
	apiURL := config.MFLIntegratorConf.API.MFLBaseURL
	params := url.Values{}
	params.Add("resource", "Location")
	params.Add("type", "healthFacility")
	if len(mflId) > 0 {
		params.Add("facilityLocalGovernment", mflId)
	}
	params.Add("_count", "30000")
	if facilityCount == 0 {
		//None exist so far
		apiURL += "?" + params.Encode()
	} else {
		// Some facilities exist
		lastSyncDate := models.GetLastSyncDate(mflId)
		if len(lastSyncDate) > 0 {
			params.Add("_lastUpdated", fmt.Sprintf("gt%s", lastSyncDate))

		} else {
			params.Add("_lastUpdated", fmt.Sprintf("gt2023-01-01"))
		}
		apiURL += "?" + params.Encode()
	}
	log.WithField("URL", apiURL).Info("Facility URL")
	startTime := time.Now() // .Format("2006-01-02 15:04:05")
	log.WithField("StartTime", startTime.Format("2006-01-02 15:04:05")).Info("Starting to fetch facilities")
	numberCreated := 0
	numberUpdated := 0
	numberIgnored := 0
	// numberDeleted := 0

	resp, err := utils.GetWithBasicAuth(apiURL, config.MFLIntegratorConf.API.MFLUser,
		config.MFLIntegratorConf.API.MFLPassword)
	if err != nil {
		log.WithError(err).Info("Sync S001: Failed to fetch facilities from MFL")
		return
	}

	if resp != nil {
		v, _, _, err := jsonparser.Get(resp, "entry")
		if err != nil {
			log.WithError(err).Info("No entries found from MFL")
			return
		}
		var entries []LocationEntry
		err = json.Unmarshal(v, &entries)
		if err != nil {
			log.WithError(err).Info("Sync S002: Error unmarshaling response body")
			fmt.Println(string(resp))
			return
		}
		// Now we have our facilities
		for i := range entries {
			facility := GetOrgUnitFromFHIRLocation(entries[i])

			facilityJSON, err := json.Marshal(facility)
			if err != nil {
				log.WithError(err).Info("Failed to marshal facility to JSON")
				continue
			}
			if facility.Geometry.Type == "" {
				facilityJSON = utils.PatchJSONObject(facilityJSON, []byte(`[
				{"op": "remove", "path": "/geometry"}]`))
			}
			log.WithField("FacilityJson", string(facilityJSON)).Info("Facility Object")
			if len(facility.Path) < 15 {
				log.WithFields(log.Fields{"UID": facility.UID, "Name": facility.Name}).Warn("Found no Parent for facility")
				var fj dbutils.MapAnything
				_ = json.Unmarshal(facilityJSON, &fj)
				ouFailure := models.OrgUnitFailure{UID: utils.GetUID(), FacilityUID: facility.UID, MFLUID: facility.MFLUID,
					Reason: "Parent not found", Object: fj, Action: "MFL Fetch"}
				ouFailure.NewOrgUnitFailure()
				// log this one too
				continue
			}

			if !facility.ExistsInDB() { // facility doesn't exist in our DB
				facility.NewOrgUnit()

				facility.UpdateMFLID(facility.MFLID)
				facility.UpdateMFLParent(facility.MFLParent.String)
				facility.UpdateMFLUID(facility.MFLUID)
				facility.UpdateGeometry()
				// Track the versions
				var fj dbutils.MapAnything
				_ = json.Unmarshal(facilityJSON, &fj)
				fj["parent"] = facility.ParentByUID()
				facilityRevision := models.OrgUnitRevision{
					OrganisationUnitUID: dbutils.Int(facility.DBID()), Revision: 0, Definition: fj, UID: utils.GetUID(),
				}
				facilityRevision.NewOrgUnitRevision()
				//facilityMetadata, err := GenerateOuMetadataByUID(facility.UID)

				facilityMetadata := models.MetadataOu{}
				facilityMetadata.Parent = facility.ParentByUID()

				err := json.Unmarshal(facilityJSON, &facilityMetadata)
				if err != nil {
					log.WithError(err).Error("Failed to unmarshal facility into Metadata object")
				}
				numberCreated += 1
				// Create Dispatcher2 request
				// remove name and uid in organisationUnitGroups sent as part of metadata
				facilityMetadata.OrganisationUnitGroups = lo.Map(
					facilityMetadata.OrganisationUnitGroups, func(item dbutils.Map, _ int) dbutils.Map {
						delete(item.Map(), "uid")
						delete(item.Map(), "name")
						return item
					})

				var ouM []models.MetadataOu
				payload := make(map[string][]models.MetadataOu)
				ouM = append(ouM, facilityMetadata)
				payload["organisationUnits"] = ouM
				reqBody, err := json.Marshal(payload)
				if facilityMetadata.Geometry.Get("type", "") == "" {
					reqBody = utils.PatchJSONObject(reqBody, []byte(
						`[{"op": "remove", "path": "/organisationUnits/0/geometry"},
						{"op": "remove", "path": "/organisationUnits/0/organisationUnitGroups"}]`))
				}

				if err != nil {
					log.WithError(err).Error("Failed to marshal facility metadata")
				}
				year, week := time.Now().ISOWeek()
				reqF := models.RequestForm{
					Source: "localhost", Destination: "base_OU", ContentType: "application/json",
					Year: fmt.Sprintf("%d", year), Week: fmt.Sprintf("%d", week),
					Month: fmt.Sprintf("%d", int(time.Now().Month())), Period: "", Facility: facility.UID,
					BatchID: batchId, SubmissionID: "", District: mflId,
					CCServers: strings.Split(config.MFLIntegratorConf.API.MFLCCDHIS2CreateServers, ","),
					Body:      string(reqBody), ObjectType: "ORGANISATION_UNIT", ReportType: "OU",
				}
				// Only queue facility if UID is valid
				if facility.ValidateUID() && len(facility.Path) > 15 {
					fRequest, err := reqF.Save(dbConn)
					if err != nil {
						log.WithError(err).Error("Failed to queue facility creation")
					}
					log.WithField("Facility", fRequest.UID()).Info("Queued Facility for addition")

					// Now create requests to add orgunit to the right orgunit groups
					ouGroupRequests := CreateOrgUnitGroupPayload(facilityMetadata)
					requestForms := MakeOrgunitGroupsAdditionRequests(
						ouGroupRequests, dbutils.Int(fRequest.ID()), facilityMetadata.UID)
					for _, rForm := range requestForms {
						if rForm.Source == "" {
							continue
						}
						rForm.BatchID = batchId // set the BatchID here
						_, err := rForm.Save(dbConn)
						if err != nil {
							log.WithError(err).WithFields(log.Fields{"OU": rForm.UID, "Group": rForm.URLSuffix}).Error(
								"Failed to queue request to add OU to OUGroup")
							continue
						}
						log.WithFields(log.Fields{"OU": rForm.UID, "Group": rForm.URLSuffix}).Info(
							"Queued Request to add facility to group!")
					}
				} else {
					log.WithFields(log.Fields{"Facility": facility.Name, "UID": facility.UID, "MFLID": facility.MFLID}).Warn(
						"Facility had invalid UID or has no parent thus not queued:")
				}

			} else {
				// facility exists

				var fj, lastestRevision dbutils.MapAnything
				_ = json.Unmarshal(facilityJSON, &fj)
				_ = json.Unmarshal(facility.GetLatestRevision(), &lastestRevision)
				fj["parent"] = facility.Parent()
				newMatchedOld, diffMap, err := models.CompareDefinition(fj, lastestRevision)
				if err != nil {
					log.WithError(err).Info("Failed to make comparison between old and new facility JSON objects")
				}
				if newMatchedOld {
					// new definition matched the old
					log.WithFields(log.Fields{
						"UID": facility.UID, "ValidUID": facility.ValidateUID(),
						"Parent": facility.ParentID,
						"Diff":   diffMap}).Info("========= Facility has no changes =======")
					numberIgnored += 1
					continue
				} else {
					// metadataPayload := models.GenerateMetadataPayload(fj)
					old, _ := json.Marshal(lastestRevision)
					new, _ := json.Marshal(facility)
					metadataPayloadFromDiff := models.GenerateMetadataPayload(diffMap)
					log.WithFields(log.Fields{
						"UID": facility.UID, "ValidUID": facility.ValidateUID(),
						"New":  string(new),
						"OLD":  string(old),
						"Diff": diffMap, "parent": facility.ParentID,
						"MetadataFromDiff": metadataPayloadFromDiff,
						// "MetadataPalyload": metadataPayload,
					}).Info(
						"::::::::: Facility had some changes :::::::::")

					// make a revision
					facilityRevision := models.OrgUnitRevision{
						OrganisationUnitUID: dbutils.Int(facility.DBID()), Revision: 0, Definition: fj, UID: utils.GetUID(),
					}
					facilityRevision.NewOrgUnitRevision()

					// Generate Metadata Update object - for facility with valid UID
					if facility.ValidateUID() {
						reqF := GenerateUpdateMetadataRequest(metadataPayloadFromDiff, facility.UID)
						reqF.BatchID = batchId
						_, err := reqF.Save(dbConn)
						if err != nil {
							log.WithError(err).WithFields(log.Fields{"OU": reqF.UID}).Error(
								"Failed to queue - update request for OU")
							continue
						}
					}

					// We want to update OU Groups as well
					facilityMetadata := models.MetadataOu{}

					err := json.Unmarshal(facilityJSON, &facilityMetadata)
					if err != nil {
						log.WithError(err).Error("Updating OU: Failed to unmarshal facility into Metadata object")
					}
					// Now create requests to add orgunit to the right orgunit groups
					ouGroupRequests := CreateOrgUnitGroupPayload(facilityMetadata)
					requestForms := MakeOrgunitGroupsAdditionRequests(
						ouGroupRequests, dbutils.Int(0), facilityMetadata.UID)
					for _, rForm := range requestForms {
						if rForm.Source == "" {
							continue
						}
						rForm.BatchID = batchId // set the BatchID here
						_, err := rForm.Save(dbConn)
						if err != nil {
							log.WithError(err).WithFields(log.Fields{"OU": rForm.UID, "Group": rForm.URLSuffix}).Error(
								"Updating OU: Failed to queue request to add OU to OUGroup")
							continue
						}
						log.WithFields(log.Fields{"OU": rForm.UID, "Group": rForm.URLSuffix}).Info(
							"Updating OU: Queued Request to add facility to group!")
					}

					numberUpdated += 1
				}

			}
		}
	}

	endTime := time.Now() // .Format("2006-01-02 15:04:05")

	syncLog := models.SyncLog{
		UID:           utils.GetUID(),
		Started:       startTime,
		MFLID:         mflId,
		BatchID:       batchId,
		Stopped:       endTime,
		NumberCreated: dbutils.Int(numberCreated),
		NumberUpdated: dbutils.Int(numberUpdated),
		NumberIgnored: dbutils.Int(numberIgnored),
		// ServersSyncLog:
	}
	syncLog.LogSync()

	log.WithFields(log.Fields{
		"EndTime":       endTime,
		"NumberCreated": numberCreated,
		"NumberUpdated": numberUpdated}).Info("Stopped processing fetched facilities")
}

const districtSQL = `SELECT mflid FROM organisationunit WHERE hierarchylevel = 
    (SELECT level from orgunitlevel where name = $1) ORDER BY name`

// FetchFacilitiesByDistrict fetches Facilities from MFL by district - if we don't get mflids then fetch everything
func FetchFacilitiesByDistrict() {
	log.Info("Going to Fetch Facilities By District")
	batchId := utils.GetUID()
	rows, err := db.GetDB().Queryx(districtSQL, config.MFLIntegratorConf.API.MFLDHIS2DistrictLevelName)
	if err != nil {
		log.WithError(err).Info("Failed to get district mflids from database")
		FetchFacilities("", batchId)
		return
	}
	for rows.Next() {
		var district string
		err := rows.Scan(&district)
		log.WithField("District", district).Info("Syncing Facilities")
		if err != nil {
			log.WithError(err).Error("Error reading district mflid from database:")
			continue
		}
		FetchFacilities(district, batchId)
	}
	_ = rows.Close()
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
	coordinates := []json.Number{lat, long}
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
		log.WithField("Facility", name).Debug("No Historical identifier found for facility %v", historicalId)
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
		ID: config.MFLIntegratorConf.API.MFLDHIS2OUMFLIDAttributeID,
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

// SyncLocationsToDHIS2Instances syncs the DHIS2 base hierarchy to subscribing DHIS2 instances
func SyncLocationsToDHIS2Instances() {
	for _, serverName := range strings.Split(config.MFLIntegratorConf.API.MFLCCDHIS2HierarchyServers, ",") {
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

// MakeOrgunitGroupsAdditionRequests ....
func MakeOrgunitGroupsAdditionRequests(
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
			CCServers: strings.Split(config.MFLIntegratorConf.API.MFLCCDHIS2OuGroupAddServers, ","),
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

func GenerateUpdateMetadataRequest(update []models.MetadataObject, facilityUID string) models.RequestForm {
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
		CCServers: strings.Split(config.MFLIntegratorConf.API.MFLCCDHIS2UpdateServers, ","),
		URLSuffix: fmt.Sprintf("/%s", facilityUID),
		Body:      string(body),
	}

	return reqF
}

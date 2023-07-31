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
		v, _, _, _ := jsonparser.Get(resp, "organisationUnitLevels")
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
		v, _, _, _ := jsonparser.Get(resp, "organisationUnitGroups")
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

			v, _, _, _ := jsonparser.Get(resp, "organisationUnits")
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
		if _type == "District" {
			_type = "Local Government"
		}
		if _type == "Sub County/Town Council/Div" {
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
			v, _, _, _ := jsonparser.Get(resp, "entry")
			var entries []LocationEntry
			err := json.Unmarshal(v, &entries)
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
							log.WithFields(log.Fields{"Id": id, "Name": name, "To": ou.ID, "Parent": parent}).Info("Matched to this one")
						}
					}

				}
			}
		}

	}
}

// FetchFacilities pulls facilities from the MFL after loading and matching
func FetchFacilities() {
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
	params.Add("_count", "30000")
	if facilityCount == 0 {
		//None exist so far
		apiURL += "?" + params.Encode()
	} else {
		// Some facilities exist
		params.Add("_lastUpdated", fmt.Sprintf("gt2023-07-01"))
		apiURL += "?" + params.Encode()
	}
	log.WithField("URL", apiURL).Info("Facility URL")
	startTime := time.Now().Format("2006-01-02 15:04:05")
	log.WithField("StartTime", startTime).Info("Starting to fetch facilities")
	numberCreated := 0
	numberUpdated := 0
	// numberDeleted := 0

	resp, err := utils.GetWithBasicAuth(apiURL, config.MFLIntegratorConf.API.MFLUser,
		config.MFLIntegratorConf.API.MFLPassword)
	if err != nil {
		log.WithError(err).Info("Sync S001: Failed to fetch facilities from MFL")
		return
	}

	if resp != nil {
		v, _, _, _ := jsonparser.Get(resp, "entry")
		var entries []LocationEntry
		err := json.Unmarshal(v, &entries)
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
			}
			log.WithField("FacilityJson", string(facilityJSON)).Info("Facility Object")

			if !facility.ExistsInDB() { // facility doesn't exist in our DB
				facility.NewOrgUnit()

				facility.UpdateMFLID(facility.MFLID)
				facility.UpdateMFLParent(facility.MFLParent.String)
				facility.UpdateMFLUID(facility.MFLUID)
				facility.UpdateGeometry()
				// Track the versions
				var fj dbutils.MapAnything
				_ = json.Unmarshal(facilityJSON, &fj)
				fj["parent"] = facility.Parent()
				facilityRevision := models.OrgUnitRevision{
					OrganisationUnitUID: dbutils.Int(facility.DBID()), Revision: 0, Definition: fj, UID: utils.GetUID(),
				}
				facilityRevision.NewOrgUnitRevision()
				//facilityMetadata, err := GenerateOuMetadataByUID(facility.UID)

				facilityMetadata := models.MetadataOu{}
				err := json.Unmarshal(facilityJSON, &facilityMetadata)
				if err != nil {
					log.WithError(err).Error("Failed to get unmarshal facility into Metadata object")
				}
				numberCreated += 1
			} else {
				// facility exists
				var fj dbutils.MapAnything
				_ = json.Unmarshal(facilityJSON, &fj)
				newMatchedOld, diffMap, err := facility.CompareDefinition(fj)
				fj["parent"] = facility.Parent()
				if err != nil {
					log.WithError(err).Info("Failed to make comparison between old and new facility JSON objects")
				}
				if newMatchedOld {
					// new definition matched the old
					log.WithFields(log.Fields{"UID": facility.UID, "ValidUID": facility.ValidateUID(), "Parent": fj["parent"]}).Info(
						"========= Facility has no changes =======")
					continue
				} else {
					metadataPayload := models.GenerateMetadataPayload(fj, diffMap)
					log.WithFields(log.Fields{"UID": facility.UID, "ValidUID": facility.ValidateUID(),
						"Diff": diffMap, "MetadataPalyload": metadataPayload}).Info(
						"::::::::: Facility had some changes :::::::::")

					// make a revision
					//facilityRevision := models.OrgUnitRevision{
					//	OrganisationUnitUID: dbutils.Int(facility.DBID()), Revision: 0, Definition: fj, UID: utils.GetUID(),
					//}

					// Generate Metadata Update object

					numberUpdated += 1

				}

			}
		}
	}

	endTime := time.Now().Format("2006-01-02 15:04:05")
	log.WithField("EndTime", endTime).Info("Stopped processing fetched facilities")
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

// GetOrgUnitFromFHIRLocation is used to generate DHIS2 OrganisationUnit from FHIR Location
func GetOrgUnitFromFHIRLocation(location LocationEntry) models.OrganisationUnit {
	_url := location.FullURL
	name := *location.Resource.Name
	id := *location.Resource.Id
	parent := ""
	if location.Resource.PartOf != nil {
		parent = *location.Resource.PartOf.Reference
	}
	parent = strings.ReplaceAll(parent, "Location/", "")
	extensions := GetExtensions(location.Resource.Extension)
	address := *location.Resource.Address.Text
	lat := location.Resource.Position.Latitude
	long := location.Resource.Position.Longitude
	coordinates := []json.Number{lat, long}
	// var point dbutils.JSON
	coordinateBytes, err := json.Marshal(coordinates)
	if err != nil {
		log.WithError(err).Info("Failed to marshal coordinates", coordinates)
		_, _ = json.Marshal([]json.Number{})
	}

	phone, email := getPhoneAndEmail(location.Resource.Telecom)
	tempOpening, _ := time.Parse("2006-01-02", "1970-01-01")
	levelOfCare := ""
	if level, ok := extensions["levelOfCare"]; ok {
		levelOfCare = level.(string)
	}
	parentOrgUnit := models.GetOrgUnitByMFLID(parent)
	facility := models.OrganisationUnit{}
	historicalId := ""
	if histId, ok := extensions["historicalIdentifier"]; ok {
		historicalId = histId.(string)
	}
	mflUniqueIdentifier := ""
	if mflUniqueId, ok := extensions["uniqueIdentifier"]; ok {
		mflUniqueIdentifier = mflUniqueId.(string)
	}
	facility.OpeningDate = tempOpening.Format("2006-01-02")
	facility.ID = historicalId
	facility.UID = historicalId
	facility.Name = name
	facility.ShortName = name
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
	facility.Geometry = models.Geometry{Type: "Point", Coordinates: coordinateBytes}
	ouUID := models.GetOuGroupUIDByName(levelOfCare)
	facility.OrgUnitGroups = []models.OrgUnitGroup{
		{Name: levelOfCare, UID: ouUID, ID: ouUID},
	}

	return facility
}

func SyncLocationsToDHIS2Instances() {
	for _, serverName := range strings.Split(config.MFLIntegratorConf.API.MFLCCDHIS2HierarchyServers, ",") {
		// Servers in this config can receive the base DHIS2 organisation unit hierarchy
		log.WithField("CCDHIS2", serverName).Info("CC Hierarchy Servers")
		server, err := models.GetServerByName(serverName)
		if err != nil {
			log.WithError(err).Info("Could not proceed to CC hierarchy to server")
		}
		// check if we already have ous in the instance and only create if none present
		ouURL := server.URL()
		baseDHIS2URL, err := utils.GetDHIS2BaseURL(ouURL)
		if err != nil {
			log.WithError(err).Info("CAUTION DHIS2 API URL should have /api/ part")
			continue
		}
		p := url.Values{}
		p.Add("fields", "id,name")
		p.Add("paging", "true")
		p.Add("pageSize", "1")
		p.Add("level", fmt.Sprintf("%d", config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel-1))
		chekOusURL := baseDHIS2URL + "/api/organisationUnits.json?"
		//if strings.LastIndex(ouURL, "?") == len(ouURL)-1 {
		//	ouURL += p.Encode()
		//} else {
		//	ouURL += "?" + p.Encode()
		//}
		chekOusURL += p.Encode()
		log.WithFields(log.Fields{"ServerURL": chekOusURL, "Name": server.Name()}).Info("Trying to send hierarchy to server")

		var respBody []byte
		switch server.AuthMethod() {
		case "Basic":
			resp, err := utils.GetWithBasicAuth(chekOusURL, server.Username(), server.Password())
			respBody = resp
			if err != nil {
				log.WithError(err).Error("Failed to get ous from server")
				continue
			}

		case "Token":
			resp, err := utils.GetWithToken(chekOusURL, server.AuthToken())
			respBody = resp
			if err != nil {
				log.WithError(err).Error("Failed to get ous from server")
				continue
			}
		default:
			// pass
		}
		if respBody != nil {
			v, _, _, _ := jsonparser.Get(respBody, "pager", "total")
			if string(v) != "0" && !*config.ForceSync {

				log.WithField("ForceSync", *config.ForceSync).Info("FORCE SYNC >>>>>>>>>>")
				continue // we have ous already
			}
			log.WithField("Server", serverName).Info("We can now sync the hierarchy!!")
			//Send Ou Levels
			syncOuLevels := GenerateOuLevelMetadata()
			levelsPayload := make(map[string][]models.MetadataOuLevel)
			levelsPayload["organisationUnitLevels"] = syncOuLevels
			sendMetadata(server, levelsPayload)

			// send Ou Groups
			syncOuGroups := GenerateOuGroupsMetadata()
			groupsPayload := make(map[string][]models.MetadataOuGroup)
			groupsPayload["organisationUnitGroups"] = syncOuGroups
			sendMetadata(server, groupsPayload)

			// Send Ous
			//for level := 1; level < 3; level++ {
			for level := 1; level < config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel; level++ {

				syncOus := GenerateOuMetadataByLevel(level)
				//for _, ou := range syncOus {
				//	pp, _ := json.Marshal(ou)
				//	log.WithFields(log.Fields{"Payload": string(pp), "Server": serverName, "UID": ou.UID, "ID": ou.ID}).Info("DEBUGGGGGGGG")
				//	rBody := sendMetadata(server, ou)
				//	if rBody != nil {
				//		log.WithFields(log.Fields{"Server": serverName, "Response": string(rBody)}).Info("Metadata Import")
				//	}
				//}
				ouChunks := lo.Chunk(syncOus, config.MFLIntegratorConf.API.MFLMetadataBatchSize)

				for _, chunck := range ouChunks {
					payload := make(map[string][]models.MetadataOu)
					payload["organisationUnits"] = chunck

					rBody := sendMetadata(server, payload)
					if rBody != nil {
						log.WithFields(log.Fields{"Server": serverName, "Response": string(rBody)}).Info("Metadata Import")
					}
					time.Sleep(3000)

				}

			}

		}

	}
}

const selectMetaDatOusSQL = `SELECT uid,name, code, shortname,description, path, hierarchylevel, 
    	to_char(openingdate, 'YYYY-mm-dd') AS openingdate, geometry_geojson(geometry, hierarchylevel) AS geometry, 
    	phonenumber, email, address, get_parent(id) parent 
		FROM organisationunit 
		 `

// WHERE hierarchylevel = $1 `

func GenerateOuMetadataByLevel(level int) []models.MetadataOu {
	var ous []models.MetadataOu
	dbConn := db.GetDB()
	ouSQL := selectMetaDatOusSQL
	if level == 1 {
		topLevelUIDs := strings.Split(config.MFLIntegratorConf.API.MFLDHIS2TreeIDs, ",")
		if len(topLevelUIDs) > 0 {
			for i, uid := range topLevelUIDs {
				topLevelUIDs[i] = fmt.Sprintf("'%s'", uid)
			}
			ouSQL += fmt.Sprintf(" WHERE uid IN(%s)  AND hierarchylevel = $1", strings.Join(topLevelUIDs, ","))
		}
	} else {
		ouSQL += " WHERE hierarchylevel = $1 "

	}
	err := dbConn.Select(&ous, ouSQL, level)
	if err != nil {
		log.WithError(err).WithField("SQL", ouSQL).Error("Failed to generate Org Units Metadata")
		return nil
	}
	ous = lo.Map(ous, func(item models.MetadataOu, index int) models.MetadataOu {
		item.ID = item.UID
		return item
	})

	return ous
}

func GenerateOuMetadataByUID(uid string) (models.MetadataOu, error) {
	var ou models.MetadataOu
	dbConn := db.GetDB()
	ouSQL := selectMetaDatOusSQL + " WHERE uid = $1"

	err := dbConn.Get(&ou, ouSQL, uid)
	if err != nil {
		log.WithError(err).WithField("SQL", ouSQL).Error("Failed to generate Org Unit Metadata")
		return ou, err
	}
	ou.ID = ou.UID

	return ou, nil
}

func GenerateOuLevelMetadata() []models.MetadataOuLevel {
	var ouLevels []models.MetadataOuLevel
	dbConn := db.GetDB()

	err := dbConn.Select(&ouLevels, `SELECT uid,name, 
       case when code is null then '' else code end, level  FROM orgunitlevel`)
	if err != nil {
		log.WithError(err).Error("Failed to generate Org Unit Levels Metadata")
		return nil
	}
	return ouLevels
}

func GenerateOuGroupsMetadata() []models.MetadataOuGroup {
	var ouGroups []models.MetadataOuGroup
	dbConn := db.GetDB()

	err := dbConn.Select(&ouGroups, `SELECT uid,name, case when code is null then '' else code end, shortname  FROM orgunitgroup`)
	if err != nil {
		log.WithError(err).Error("Failed to generate Org Unit Groups Metadata")
		return nil
	}
	return ouGroups
}

func sendMetadata(server models.Server, data interface{}) []byte {
	mURL := server.CompleteURL()
	log.WithField("URL", mURL).Info("MetaDataURL")
	serverName := server.Name()

	var rBody []byte
	switch server.AuthMethod() {
	case "Basic":
		resp, err := utils.PostWithBasicAuth(mURL, data, server.Username(), server.Password())
		rBody = resp
		if err != nil {
			ouList := data.(map[string][]models.MetadataOu)["organisationUnits"]
			troubleOus := lo.Map(ouList, func(item models.MetadataOu, index int) string {
				return item.ID
			})
			log.WithFields(log.Fields{"Server": serverName, "TroubleOus": troubleOus}).WithError(err).Error(
				"Failed to import metadata in server")
		}
	case "Token":
		resp, err := utils.PostWithToken(mURL, data, server.AuthToken())
		rBody = resp
		if err != nil {
			ouList := data.(map[string][]models.MetadataOu)["organisationUnits"]
			troubleOus := lo.Map(ouList, func(item models.MetadataOu, index int) string {
				return item.ID
			})
			log.WithFields(log.Fields{"Server": serverName, "TroubleOus": troubleOus}).WithError(err).Error(
				"Failed to import metadata in server")
		}
	default:
		//
	}
	if rBody != nil {
		log.WithFields(log.Fields{"Server": serverName, "Response": string(rBody)}).Info("Metadata Import")
	}
	return rBody
}

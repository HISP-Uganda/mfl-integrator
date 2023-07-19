package main

import (
	"encoding/json"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/HISP-Uganda/mfl-integrator/utils"
	"github.com/buger/jsonparser"
	log "github.com/sirupsen/logrus"
	"net/url"
)

func LoadOuLevels() {
	dbconn := db.GetDB()
	var id int
	err := dbconn.QueryRowx("SELECT count(*) FROM orgunitlevel").Scan(&id)
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
			ouLevels[i].NewOrgUnitLevel(dbconn)
		}

	}

}

func LoadOuGroups() {
	dbconn := db.GetDB()
	var id int
	err := dbconn.QueryRowx("SELECT count(*) FROM orgunitgroup").Scan(&id)
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
		}
		// fmt.Println(string(resp))
		v, _, _, _ := jsonparser.Get(resp, "organisationUnitGroups")
		// fmt.Printf("Entries: %s", v)
		var ouGroups []models.OrgUnitGroup
		err = json.Unmarshal(v, &ouGroups)
		if err != nil {
			fmt.Println("Error unmarshaling orgunit groups response body:", err)
			return
		}
		for i := range ouGroups {
			ouGroups[i].UID = ouGroups[i].ID
			log.WithFields(
				log.Fields{"uid": ouGroups[i].ID, "name": ouGroups[i].Name, "level": ouGroups[i].ShortName}).Info("Creating New Orgunit Group")
			ouGroups[i].NewOrgUnitGroup(dbconn)
		}

	}

}

func LoadLocations() {
	dbconn := db.GetDB()
	var id int
	err := dbconn.QueryRowx("SELECT count(*) FROM organisationunit WHERE hierarchylevel=1").Scan(&id)
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
		p.Add("fields", "id,name,displayName,code,shortName,openingDate,phoneNumber,path,level,description,geometry")
		p.Add("paging", "false")

		for i := 1; i < config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel; i++ {
			p.Add("level", fmt.Sprintf("%d", i))
			ouURL := apiURL + "?" + p.Encode()
			fmt.Println(ouURL)
			resp, err := utils.GetWithBasicAuth(ouURL, config.MFLIntegratorConf.API.MFLDHIS2User,
				config.MFLIntegratorConf.API.MFLDHIS2Password)
			if err != nil {
				log.WithError(err).Info("Failed to fetch organisation units")
			}
			// fmt.Println(string(resp))
			v, _, _, _ := jsonparser.Get(resp, "organisationUnits")
			// fmt.Printf("Entries: %s", v)
			var ous []models.OrganisationUnit
			err = json.Unmarshal(v, &ous)
			if err != nil {
				fmt.Println("Error unmarshaling response body:", err)
				return
			}
			for i := range ous {
				ous[i].UID = ous[i].ID
				log.WithFields(
					log.Fields{"uid": ous[i].ID, "name": ous[i].Name, "level": ous[i].Level,
						"Geometry-Type": ous[i].Geometry.Type}).Info("Creating New Orgunit")
				ous[i].NewOrgUnit(dbconn)
			}

			p.Del("level")
		}
	}

}

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
		p.Add("fields", "id,name,displayName,code,shortName,openingDate,phoneNumber,path,level,description")
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
			fmt.Println(string(resp))
			v, _, _, _ := jsonparser.Get(resp, "organisationUnits")
			fmt.Printf("Entries: %s", v)
			var ous []models.OrganisationUnit
			err = json.Unmarshal(v, &ous)
			if err != nil {
				fmt.Println("Error unmarshaling response body:", err)
				return
			}
			for i := range ous {
				ous[i].UID = ous[i].ID
				log.WithFields(
					log.Fields{"uid": ous[i].ID, "name": ous[i].Name, "level": ous[i].Level}).Info("Creating New Orgunit")
				ous[i].NewOrgUnit(dbconn)
			}

			p.Del("level")
		}
	}

}

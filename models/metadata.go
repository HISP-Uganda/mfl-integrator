package models

import (
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/utils"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"strings"
)

type MetadataOu struct {
	ID                     string        `db:"id" json:"-"`
	UID                    string        `db:"uid" json:"id"`
	ParentID               int64         `db:"parentid" json:"-"`
	Path                   string        `db:"path" json:"-"`
	Level                  int           `db:"hierarchylevel" json:"level,omitempty"`
	Name                   string        `db:"name" json:"name"`
	ShortName              string        `db:"shortname" json:"shortName"`
	Description            string        `db:"description" json:"description,omitempty"`
	Email                  string        `db:"email" json:"email,omitempty"`
	Address                string        `db:"address" json:"address,omitempty"`
	Phone                  string        `db:"phonenumber" json:"phone,omitempty"`
	Code                   string        `db:"code" json:"code,omitempty"`
	OpeningDate            string        `db:"openingdate" json:"openingDate"`
	Parent                 dbutils.Map   `db:"parent" json:"parent,omitempty"`
	Geometry               dbutils.Map   `db:"geometry" json:"geometry"`
	OrganisationUnitGroups []dbutils.Map `json:"organisationUnitGroups,omitempty"`
	AttributeValues        []dbutils.Map `json:"attributeValues,omitempty"`
}

type MetadataOuLevel struct {
	ID    string `db:"id" json:"-"`
	UID   string `db:"uid" json:"id"`
	Code  string `db:"code" json:"code,omitempty"`
	Name  string `db:"name" json:"name"`
	Level int    `db:"level" json:"level"`
}

type MetadataOuGroup struct {
	ID        string `db:"id" json:"-"`
	UID       string `db:"uid" json:"id"`
	Code      string `db:"code" json:"code,omitempty"`
	Name      string `db:"name" json:"name"`
	ShortName string `db:"shortname" json:"shortName"`
}

// GenerateOuLevelMetadata ...
func GenerateOuLevelMetadata() []MetadataOuLevel {
	var ouLevels []MetadataOuLevel
	dbConn := db.GetDB()

	err := dbConn.Select(&ouLevels, `SELECT uid,name, 
       case when code is null then '' else code end, level  FROM orgunitlevel`)
	if err != nil {
		log.WithError(err).Error("Failed to generate Org Unit Levels Metadata")
		return nil
	}
	return ouLevels
}

type AttributeMetadata struct {
	ID                        string `db:"id" json:"-"`
	UID                       string `db:"uid" json:"id"`
	Code                      string `db:"code" json:"code,omitempty"`
	Name                      string `db:"name" json:"name"`
	ShortName                 string `db:"shortname" json:"shortName"`
	ValueType                 string `db:"valuetype" json:"valueType,omitempty"`
	Mandatory                 bool   `db:"mandatory" json:"mandatory"`
	Unique                    bool   `db:"isunique" json:"unique"`
	OrganisationUnitAttribute bool   `db:"organisationunitattribute" json:"organisationUnitAttribute,omitempty"`
}

// GenerateOuGroupsMetadata ...
func GenerateOuGroupsMetadata() []MetadataOuGroup {
	var ouGroups []MetadataOuGroup
	dbConn := db.GetDB()

	err := dbConn.Select(&ouGroups, `SELECT uid,name, case when code is null then '' else code end, shortname  FROM orgunitgroup`)
	if err != nil {
		log.WithError(err).Error("Failed to generate Org Unit Groups Metadata")
		return nil
	}
	return ouGroups
}

// GenerateAttributeMetadata ...
func GenerateAttributeMetadata() []AttributeMetadata {
	var attributes []AttributeMetadata
	dbConn := db.GetDB()

	err := dbConn.Select(&attributes, `
	SELECT uid,name, case when code is null then '' else code end, shortname, valueType, organisationunitattribute,
	       mandatory, isunique
	FROM attribute`)
	if err != nil {
		log.WithError(err).Error("Failed to generate Organisation Units Attribute Metadata")
		return attributes
	}
	return attributes
}

// sendMetadata .....
func SendMetadata(server Server, data interface{}) []byte {
	mURL := server.CompleteURL()
	log.WithField("URL", mURL).Info("MetaDataURL")
	serverName := server.Name()

	var rBody []byte
	switch server.AuthMethod() {
	case "Basic":
		resp, err := utils.PostWithBasicAuth(mURL, data, server.Username(), server.Password())
		rBody = resp
		if err != nil {
			ouList := data.(map[string][]MetadataOu)["organisationUnits"]
			troubleOus := lo.Map(ouList, func(item MetadataOu, index int) string {
				return item.ID
			})
			log.WithFields(log.Fields{"Server": serverName, "TroubleOus": troubleOus}).WithError(err).Error(
				"Failed to import metadata in server")
		}
	case "Token":
		resp, err := utils.PostWithToken(mURL, data, server.AuthToken())
		rBody = resp
		if err != nil {
			ouList := data.(map[string][]MetadataOu)["organisationUnits"]
			troubleOus := lo.Map(ouList, func(item MetadataOu, index int) string {
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

const selectMetaDatOusSQL = `SELECT uid,name, code, shortname,description, path, hierarchylevel, 
    	to_char(openingdate, 'YYYY-mm-dd') AS openingdate, geometry_geojson(geometry, hierarchylevel) AS geometry, 
    	phonenumber, email, address, get_parent(id) parent 
		FROM organisationunit `

// GenerateOuMetadataByLevel ....
func GenerateOuMetadataByLevel(level int) []MetadataOu {
	var ous []MetadataOu
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
	ous = lo.Map(ous, func(item MetadataOu, index int) MetadataOu {
		item.ID = item.UID
		return item
	})

	return ous
}

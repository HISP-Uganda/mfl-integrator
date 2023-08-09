package models

import "github.com/HISP-Uganda/mfl-integrator/utils/dbutils"

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

package models

import (
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	"github.com/jmoiron/sqlx"
	"reflect"
)

type OrgUnitGroup struct {
	ID        string `db:"id" json:"-"`
	UID       string `db:"uid" json:"id"`
	Code      string `db:"code" json:"code,omitempty"`
	Name      string `db:"name" json:"name"`
	ShortName string `db:"shortname" json:"shortName,omitempty"`
	Created   string `json:"created,omitempty"`
	Updated   string `json:"updated,omitempty"`
}

type OrganisationUnit struct {
	ID              string              `db:"id" json:"id"`
	UID             string              `db:"uid" json:"uid"`
	Code            string              `db:"code" json:"code,omitempty"`
	Name            string              `db:"name" json:"name"`
	ShortName       string              `db:"shortname" json:"shortName,omitempty"`
	Email           string              `db:"email" json:"email,omitempty"`
	URL             string              `db:"url" json:"url,omitempty"`
	Address         string              `db:"address" json:"address,omitempty"`
	DisplayName     string              `db:"-" json:"displayName,omitempty"`
	Description     string              `db:"description" json:"description,omitempty"`
	PhoneNumber     string              `db:"phonenumber" json:"phoneNumber,omitempty"`
	Level           int                 `db:"hierarchylevel" json:"level"`
	ParentID        dbutils.Int         `db:"parentid" json:"parentid,omitempty"`
	Path            string              `db:"path" json:"path,omitempty"`
	MFLID           string              `db:"mflid" json:"mflId,omitempty"`
	OpeningDate     string              `db:"openingdate" json:"openingDate"`
	ClosedDate      string              `db:"closeddate" json:"closedDate,omitempty"`
	Deleted         bool                `db:"deleted" json:"deleted,omitempty"`
	Extras          dbutils.MapAnything `db:"extras" json:"extras,omitempty"`
	AttributeValues dbutils.MapAnything `db:"attributevalues" json:"attributeValues,omitempty"`
	LastSyncDate    string              `db:"lastsyncdate" json:"lastSyncDate,omitempty"`
	Created         string              `db:"created" json:"created,omitempty"`
	Updated         string              `db:"updated" json:"updated,omitempty"`
}

func (o *OrganisationUnit) OrganisationUnitDBFields() []string {
	e := reflect.ValueOf(&o).Elem()
	var ret []string
	for i := 0; i < e.NumField(); i++ {
		t := e.Type().Field(i).Tag.Get("db")
		if len(t) > 0 && t != "-" {
			ret = append(ret, t)
		}
	}
	ret = append(ret, "*")
	return ret
}

const insertOrgUnitSQL = `
INSERT INTO organisationunit (uid,name, shortname,path, parentid, hierarchylevel,address,
        email,phonenumber,url,mflid,extras,openingdate, created, updated)
VALUES (:uid, :name,  :shortname, :path, ou_paraent_from_path(:path, :hierarchylevel), 
        :hierarchylevel, :address, :email, :phonenumber, :url, :mflid, :extras, :openingdate, now(), now())
`

func (ou *OrganisationUnit) NewOrgUnit(db *sqlx.DB) {
	_, err := db.NamedExec(insertOrgUnitSQL, ou)
	if err != nil {
		fmt.Printf("ERROR INSERTING OrgUnit", err)
	}
}

package models

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
	"reflect"
)

type Geometry struct {
	Type        string       `json:"type"`
	Coordinates dbutils.JSON `json:"coordinates"`
}

//func (g *Geometry) UnmarshalJSON(data []byte) error {
//	type Alias Geometry
//	aux := &struct {
//		*Alias
//	}{
//		Alias: (*Alias)(g),
//	}
//	if err := json.Unmarshal(data, &aux); err != nil {
//		return err
//	}
//	switch g.Type {
//	case "Point", "Polygon":
//		return nil
//	default:
//		return fmt.Errorf("unsupported geometry type: %s", g.Type)
//	}
//}

type OrgUnitLevel struct {
	ID      string `db:"id" json:"id"`
	UID     string `db:"uid" json:"uid"`
	Code    string `db:"code" json:"code,omitempty"`
	Name    string `db:"name" json:"name"`
	Level   int    `db:"level" json:"level"`
	Created string `json:"created,omitempty"`
	Updated string `json:"updated,omitempty"`
}

const insertOrgunitLevelSQL = `
INSERT INTO orgunitlevel(uid, name, level, created, updated)
VALUES(:uid, :name, :level, NOW(), NOW())
`

func (ol *OrgUnitLevel) NewOrgUnitLevel(db *sqlx.DB) {
	_, err := db.NamedExec(insertOrgunitLevelSQL, ol)
	if err != nil {
		fmt.Printf("ERROR INSERTING OrgUnit Level", err)
	}
}

type OrgUnitGroup struct {
	ID        string `db:"id" json:"id"`
	UID       string `db:"uid" json:"uid"`
	Code      string `db:"code" json:"code,omitempty"`
	Name      string `db:"name" json:"name"`
	ShortName string `db:"shortname" json:"shortName,omitempty"`
	Created   string `json:"created,omitempty"`
	Updated   string `json:"updated,omitempty"`
}

const insertOrgunitGroupSQL = `
INSERT INTO orgunitgroup(uid, name, shortname, created, updated)
VALUES(:uid, :name, :shortname, NOW(), NOW())`

func (og *OrgUnitGroup) NewOrgUnitGroup(db *sqlx.DB) {
	_, err := db.NamedExec(insertOrgunitGroupSQL, og)
	if err != nil {
		fmt.Printf("ERROR INSERTING OrgUnit Level", err)
	}
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
	MFLUID          string              `db:"mfluid" json:"mflUID,omitempty"`
	OpeningDate     string              `db:"openingdate" json:"openingDate"`
	ClosedDate      string              `db:"closeddate" json:"closedDate,omitempty"`
	Deleted         bool                `db:"deleted" json:"deleted,omitempty"`
	Extras          dbutils.MapAnything `db:"extras" json:"extras,omitempty"`
	AttributeValues dbutils.MapAnything `db:"attributevalues" json:"attributeValues,omitempty"`
	LastSyncDate    string              `db:"lastsyncdate" json:"lastSyncDate,omitempty"`
	Geometry        Geometry            `db:"geometry" json:"geometry"`
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
RETURNING id
`

func (ou *OrganisationUnit) NewOrgUnit(db *sqlx.DB) {
	rows, err := db.NamedQuery(insertOrgUnitSQL, ou)
	if err != nil {
		fmt.Printf("ERROR INSERTING OrgUnit", err)
	}
	for rows.Next() {
		var id sql.NullInt64
		_ = rows.Scan(&id)
		log.WithFields(log.Fields{"ID": id.Int64, "UID": ou.UID, "OuByID": ou.ID}).Info("Created New OrgUnit")
		ou.UpdateGeometry(db)
	}
	_ = rows.Close()
}

func (ou *OrganisationUnit) UpdateGeometry(db *sqlx.DB) {
	if len(ou.Geometry.Type) == 0 {
		return
	}
	log.WithField("Geometry", ou.Geometry.Type).Info("Going to update Location Geometry")

	var geomObj geom.T
	switch ou.Geometry.Type {
	case "Point":
		var coordinates []float64
		if err := json.Unmarshal(ou.Geometry.Coordinates, &coordinates); err != nil {
			log.WithError(err).Error("Failed to unmarshal Point coordinates")
			return
		}
		pointGeom := geom.NewPoint(geom.XY).MustSetCoords([]float64{coordinates[0], coordinates[1]})
		geomObj = pointGeom
	case "Polygon":
		var coordinates [][][]float64
		if err := json.Unmarshal(ou.Geometry.Coordinates, &coordinates); err != nil {
			log.WithError(err).Error("Failed to unmarshal Polygon coordinates")
			return
		}
		geomObj = getPloygon(coordinates)
	case "MultiPolygon":
		var coordinates [][][][]float64
		if err := json.Unmarshal(ou.Geometry.Coordinates, &coordinates); err != nil {
			log.WithError(err).Error("Failed to unmarshal MultiPolygon coordinates")
			return
		}
		geomObj = getMultiPloygon(coordinates)
	default:
		log.WithField("Type", ou.Geometry.Type).Error("Unsupported geometry type:")
		return
	}
	geoJSONBytes, err := geojson.Marshal(geomObj)
	if err != nil {
		log.WithError(err).Error("Failed to Marshal Geometry Object")
		return
	}
	geoJSONString := string(geoJSONBytes)
	// log.WithField("geoJSONString", geoJSONString).Info("XXXXX Geo")
	args := dbutils.MapAnything{"geometry": geoJSONString, "uid": ou.UID}
	_, _ = db.NamedExec(`UPDATE organisationunit SET geometry = :geometry  WHERE uid = :uid`, args)
}

func getPloygon(coordinates [][][]float64) *geom.Polygon {
	ring := geom.NewLinearRing(geom.XY)
	var coords []geom.Coord
	for _, c := range coordinates[0] {
		coords = append(coords, geom.Coord{c[0], c[1]})

	}
	ring.MustSetCoords(coords)
	polygonGeom := geom.NewPolygon(geom.XY).MustSetCoords([][]geom.Coord{ring.Coords()})
	return polygonGeom
}

func getMultiPloygon(coordinates [][][][]float64) *geom.MultiPolygon {
	polygonGeoms := make([]*geom.Polygon, len(coordinates))
	multiPolygonGeom := geom.NewMultiPolygon(geom.XY)
	for i, c := range coordinates {
		polygonGeoms[i] = getPloygon(c)
		err := multiPolygonGeom.Push(polygonGeoms[i])
		if err != nil {
			log.WithError(err).Info("Failed to push polygon")
			continue
		}
	}
	return multiPolygonGeom
}

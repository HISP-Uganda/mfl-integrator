package models

import (
	"github.com/HISP-Uganda/mfl-integrator/db"
	log "github.com/sirupsen/logrus"
)

type UsedUID struct {
	ID      string `db:"id" json:"id"`
	UID     string `db:"uid" json:"uid"`
	MFLUID  string `db:"mfluid" json:"mfluid"`
	Created string `db:"created" json:"created,omitempty"`
	Updated string `db:"updated" json:"updated,omitempty"`
}

const insertUsedUIDSQL = `
INSERT INTO used_uids(uid, mfluid, created, updated)
VALUES(:uid, :mfluid, NOW(), NOW())`

func (u *UsedUID) NewUsedUID() {
	dbConn := db.GetDB()
	_, err := dbConn.NamedExec(insertUsedUIDSQL, u)
	if err != nil {
		log.WithError(err).Info("ERROR INSERTING Used MFLUID")
	}
}

func GetUIDByMFLUID(mfluid string) string {
	dbConn := db.GetDB()
	var uid string
	err := dbConn.Get(&uid, `SELECT uid FROM used_uids WHERE mfluid = $1`, mfluid)
	if err != nil {
		log.WithField("MFLUID", mfluid).WithError(err).Info("Failed to get UID for MFLUID")
		return ""
	}
	return uid
}

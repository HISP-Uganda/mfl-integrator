package models

import (
	db2 "github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	log "github.com/sirupsen/logrus"
	"time"
)

type SyncLog struct {
	ID             dbutils.Int  `db:"id" json:"-"`
	UID            string       `db:"uid" json:"uid"`
	MFLID          string       `db:"mflid" json:"MFLID"`
	BatchID        string       `db:"batchid" json:"BatchID"`
	Started        time.Time    `db:"started" json:"started"`
	Stopped        time.Time    `db:"stopped" json:"stopped"`
	NumberCreated  dbutils.Int  `db:"number_created" json:"numberCreated"`
	NumberDeleted  dbutils.Int  `db:"number_deleted" json:"numberDeleted"`
	NumberUpdated  dbutils.Int  `db:"number_updated" json:"numberUpdated"`
	NumberIgnored  dbutils.Int  `db:"number_ignored" json:"numberIgnored"`
	ServersSyncLog dbutils.JSON `db:"servers_sync_log" json:"serversSyncLog"`
	Created        time.Time    `db:"created" json:"created"`
	Updated        time.Time    `db:"updated" json:"updated"`
}

// LogSync helps to log the sync process
func (s *SyncLog) LogSync() {
	db := db2.GetDB()
	_, err := db.NamedExec(`INSERT INTO sync_log (uid, mflid, batchid, started, stopped, number_created, 
        number_deleted, number_ignored, number_updated) 
		VALUES (:uid, :mflid, :batchid, :started, :stopped, :number_created, :number_deleted, :number_ignored, :number_updated)`, s)
	if err != nil {
		log.WithError(err).Info("Failed to log synchronisation")
		return
	}
}

// GetLastSyncDate last successful sync date as a string
func GetLastSyncDate(mflId string) string {
	var date string
	err := db2.GetDB().Get(&date,
		"SELECT date(stopped) FROM sync_log  WHERE mflid = $1 ORDER BY created DESC LIMIT 1", mflId)
	if err != nil {
		return ""
	}
	return date
}

package models

import (
	db2 "github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	log "github.com/sirupsen/logrus"
	"time"
)

type SyncLog struct {
	s struct {
		ID             dbutils.Int  `db:"id" json:"-"`
		UID            string       `db:"uid" json:"uid"`
		Started        time.Time    `db:"started" json:"started"`
		Stopped        time.Time    `db:"stopped" json:"stopped"`
		NumberCreated  dbutils.Int  `db:"number_created" json:"numberCreated"`
		NumberDeleted  dbutils.Int  `db:"number_deleted" json:"numberDeleted"`
		NumberUpdated  dbutils.Int  `db:"number_updated" json:"numberUpdated"`
		ServersSyncLog dbutils.JSON `db:"servers_sync_log" json:"serversSyncLog"`
		Created        time.Time    `db:"created" json:"created"`
		Updated        time.Time    `db:"updated" json:"updated"`
	}
}

func (s *SyncLog) LogSync() {
	db := db2.GetDB()
	_, err := db.NamedExec(`INSERT INTO sync_log (uid, started, stopped, number_created, 
        number_deleted, number_updated) VALUES (:uid,:started, :stopped, :number_created, number_updated
                    )`, s)
	if err != nil {
		log.WithError(err).Info("Failed to log synchronisation")
		return
	}
}

package main

import (
	"encoding/json"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/models"
	log "github.com/sirupsen/logrus"
	"strconv"
)

// LoadServersFromConfigFiles saves the servers read from /etc/mflintegrator/conf.d
func LoadServersFromConfigFiles(serverConfMap map[string]config.ServerConf) {
	for k := range serverConfMap {
		// log.WithField("SERVER", serverConfMap[k]).Info("SERVER_CONFIG >>>")
		serverJSON, err := json.Marshal(serverConfMap[k])
		if err != nil {
			log.WithError(err).Error("Failed to marshal server configuration to []byte:")
			continue
		}
		dbConn := db.GetDB()
		srv, err := models.CreateServerFromJSON(dbConn, serverJSON)
		if err != nil {
			log.WithError(err).Error("Failed to create/update server")
		}
		models.ServerMap[strconv.Itoa(int(srv.ID()))] = srv
		models.ServerMapByName[srv.Name()] = srv
	}
}

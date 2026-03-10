package main

import (
	"encoding/json"
	"strconv"

	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/models"
	log "github.com/sirupsen/logrus"
)

func LoadServersFromConfigFiles(serverConfMap map[string]config.ServerConf) {
	dbConn := db.GetDB()

	nextByID := make(map[string]models.Server, len(serverConfMap))
	nextByName := make(map[string]models.Server, len(serverConfMap))

	for k := range serverConfMap {
		serverJSON, err := json.Marshal(serverConfMap[k])
		if err != nil {
			log.WithError(err).Error("Failed to marshal server configuration to []byte")
			continue
		}

		srv, err := models.CreateServerFromJSON(dbConn, serverJSON)
		if err != nil {
			log.WithError(err).Error("Failed to create/update server")
			continue
		}

		nextByID[strconv.Itoa(int(srv.ID()))] = srv
		nextByName[srv.Name()] = srv
	}

	// Atomic swap: readers always see a consistent snapshot.
	models.ReplaceServerRegistry(nextByID, nextByName)
}

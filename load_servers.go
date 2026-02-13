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
	// Ensure maps exist (prevents: panic: assignment to entry in nil map)
	if models.ServerMap == nil {
		models.ServerMap = make(map[string]models.Server)
	}
	if models.ServerMapByName == nil {
		models.ServerMapByName = make(map[string]models.Server)
	}

	for k := range serverConfMap {
		serverJSON, err := json.Marshal(serverConfMap[k])
		if err != nil {
			log.WithError(err).Error("Failed to marshal server configuration to []byte")
			continue
		}

		dbConn := db.GetDB()
		srv, err := models.CreateServerFromJSON(dbConn, serverJSON)
		if err != nil {
			log.WithError(err).Error("Failed to create/update server")
			continue
		}

		models.ServerMap[strconv.Itoa(int(srv.ID()))] = srv
		models.ServerMapByName[srv.Name()] = srv
	}
}

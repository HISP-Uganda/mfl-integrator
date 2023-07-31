package controllers

import (
	"encoding/json"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"net/http"
)

type ServerController struct{}

func (s *ServerController) CreateServer(c *gin.Context) {
	db := c.MustGet("dbConn").(*sqlx.DB)
	srv, err := models.NewServer(c, db)
	if err != nil {
		log.WithError(err).Error("Failed to create server")
		c.JSON(http.StatusConflict, gin.H{
			"message":  "Failed to create server",
			"conflict": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, srv.Self())
}

func (s *ServerController) ImportServers(c *gin.Context) {
	db := c.MustGet("dbConn").(*sqlx.DB)

	var servers []models.Server
	contentType := c.Request.Header.Get("Content-Type")
	switch contentType {
	case "application/json":
		if err := c.BindJSON(&servers); err != nil {
			log.WithError(err).Error("Error reading list of server object from POST body")
		}
		// log.WithField("New Server", s).Info("Going to create new server")
	default:
		//
		log.WithField("Content-Type", contentType).Error("Unsupported content-Type")
		return
	}
	importSummary, err := models.CreateServers(db, servers)
	if err != nil {
		log.WithError(err).Error("Failed to import servers servers")
		c.JSON(http.StatusConflict, gin.H{
			"message":  "Failed to import servers",
			"conflict": err.Error(),
		})
		return
	}
	summary, _ := json.Marshal(importSummary)
	c.JSON(http.StatusOK, gin.H{
		"status":       "SUCCCESS",
		"importSumary": summary,
	})
}

package controllers

import (
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

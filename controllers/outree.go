package controllers

import (
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/gin-gonic/gin"
	"net/http"
)

// OrgUnitTreeController defines the orgunitTree creation request controller methods
type OrgUnitTreeController struct{}

// CreateOrgUnitTree method handles the /outree request
func (o *OrgUnitTreeController) CreateOrgUnitTree(c *gin.Context) {
	serverName := c.Param("server")
	result := make(chan gin.H)
	go func() {
		models.SyncLocationsToServer(serverName)
		result <- gin.H{"message": "Location Syncing in background"}
	}()
	// c.IndentedJSON(http.StatusOK, gin.H{"message": "Location Syncing in background"})
	c.AbortWithStatusJSON(http.StatusOK, <-result)
}

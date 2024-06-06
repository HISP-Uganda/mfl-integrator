package controllers

import (
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/gin-gonic/gin"
	"net/http"
)

type AdminController struct{}

func (a *AdminController) ClearSyncLog(c *gin.Context) {
	districtMFLID := c.Param("district")
	result := make(chan gin.H)
	go func() {
		models.ClearLogs(districtMFLID)
		result <- gin.H{"message": "Clearing sync logs in background"}
	}()
	c.AbortWithStatusJSON(http.StatusOK, <-result)
}

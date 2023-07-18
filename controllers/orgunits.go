package controllers

import (
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	// log "github.com/sirupsen/logrus"
	// "net/http"
)

// OrgUnitController defines the orgunit request controller methods
type OrgUnitController struct{}

// OrgUnit method handles the /organisationUnit request
func (o *OrgUnitController) OrgUnit(c *gin.Context) {

	db := c.MustGet("dbConn").(*sqlx.DB)
	var ou models.OrganisationUnit
	if c.ShouldBind(&ou) == nil {
		fmt.Printf("%v\n", ou)
		ou.NewOrgUnit(db)
		//_, err := db.NamedExec(insertOrgUnitSQL, ou)
		//if err != nil {
		//	fmt.Printf("ERROR INSERTING OrgUnit", err)
		//}

	} else {
		fmt.Printf("Didn't bind \n")
	}
	c.JSON(200, gin.H{"message": "OrgUnit"})
	// source := c.PostForm("source")
	// destination := c.PostForm("destination")
	//contentType := c.Request.Header.Get("Content-Type")
	//req, err := models.NewRequest(c, db)
	//if err != nil {
	//	log.WithError(err).Error("Failed to add request to queue")
	//	c.String(http.StatusBadGateway, "Failed to add request to queue")
	//	return
	//}
	//
	//fmt.Printf("cType %s", contentType)
	//c.JSON(http.StatusOK, gin.H{
	//	"uid":         req.UID(),
	//	"source":      req.Source(),
	//	"destination": req.Destination(),
	//	"body":        req.Body(),
	//	"status":      req.Status(),
	//	"RawMsg":      req.RawMsg(),
	//	"period":      req.Period()})
	//return
}

//func NewOrgUnit(c *gin.Context) (models.OrganisationUnit, error) {
//	// ou := &models.OrganisationUnit{}
//	var ouBody models.OrganisationUnit
//	if c.ShouldBind(&ouBody) == nil {
//		fmt.Printf("%v\n", ouBody)
//	}
//	return c.
//
//}

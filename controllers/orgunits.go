package controllers

import (
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/HISP-Uganda/mfl-integrator/utils"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"net/http"
	// log "github.com/sirupsen/logrus"
	// "net/http"
)

// OrgUnitController defines the orgunit request controller methods
type OrgUnitController struct{}

// OrgUnit method handles the /organisationUnit request
func (o *OrgUnitController) OrgUnit(c *gin.Context) {
	// db := c.MustGet("dbConn").(*sqlx.DB)
	var ou models.OrganisationUnit
	if c.ShouldBind(&ou) == nil {
		fmt.Printf("%v\n", ou)
		ou.NewOrgUnit()
		//_, err := db.NamedExec(insertOrgUnitSQL, ou)
		//if err != nil {
		//	fmt.Printf("ERROR INSERTING OrgUnit", err)
		//}

	} else {
		fmt.Printf("Didn't bind \n")
	}
	c.JSON(200, gin.H{"message": "OrgUnit"})
	return
}

var organisationUnitDBFields = new(models.OrganisationUnit).OrganisationUnitDBFields()

func (o *OrgUnitController) GetOrganisationUnits(c *gin.Context) {

	page := c.DefaultQuery("page", "1")
	pageSize := c.DefaultQuery("pageSize", "50")
	paging := c.DefaultQuery("paging", "true")
	orderbys := c.QueryArray("order") // property:desc|asc|iasc|idesc
	filters := c.QueryArray("filter")
	qfields := c.DefaultQuery("fields", "*")

	/*Lets get the fields*/
	filtered, relationships := utils.GetFieldsAndRelationships(organisationUnitDBFields, qfields)
	requestsTable := dbutils.Table{Name: "organisationunit", Alias: "o"}

	qbuild := &dbutils.QueryBuilder{}
	qbuild.QueryTemplate = `SELECT %s
FROM %s
%s`
	qbuild.Table = requestsTable
	var fields []dbutils.Field
	for _, f := range filtered {
		fields = append(fields, dbutils.Field{Name: f, TablePrefix: "o", Alias: ""})
	}

	qbuild.Conditions = dbutils.QueryFiltersToConditions(filters, "o")
	qbuild.Fields = fields
	qbuild.OrderBy = dbutils.OrderListToOrderBy(orderbys, organisationUnitDBFields, "o")

	var whereClause string
	if len(qbuild.Conditions) == 0 {
		whereClause = " TRUE"
	} else {
		whereClause = fmt.Sprintf("%s", dbutils.QueryConditions(qbuild.Conditions))
	}
	countquery := fmt.Sprintf("SELECT COUNT(*) AS count FROM organisationunit o WHERE %s", whereClause)

	db := c.MustGet("dbConn").(*sqlx.DB)
	var count int64
	err := db.Get(&count, countquery)
	if err != nil {
		log.WithError(err).Info("Failed to get count from query")
		return
	}

	// get the Paginator
	shouldWePage := true
	if paging == "false" {
		shouldWePage = false
	}
	p := utils.GetPaginator(count, pageSize, page, shouldWePage)
	qbuild.Limit = p.PageSize
	qbuild.Offset = p.FirstItem() - 1

	jsonquery := fmt.Sprintf(`SELECT ROW_TO_JSON(s) FROM (%s) s;`, qbuild.ToSQL(shouldWePage))

	var organisationUnits []dbutils.MapAnything //map[string]interface{}

	err = db.Select(&organisationUnits, jsonquery)
	if err != nil {
		log.WithError(err).Error("Failed to query organisatiounits")
	}

	c.JSON(http.StatusOK, gin.H{
		"pager":         p,
		"customers":     organisationUnits,
		"order":         orderbys,
		"fields":        qfields,
		"filtered":      filtered,
		"filters":       filters,
		"relationships": relationships,
		"query":         jsonquery,
		"countQuery":    countquery,
		"count":         count})
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

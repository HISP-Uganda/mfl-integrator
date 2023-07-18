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
)

// QueueController defines the queue request controller methods
type QueueController struct{}

// Queue method handles the /queque request
func (q *QueueController) Queue(c *gin.Context) {

	db := c.MustGet("dbConn").(*sqlx.DB)

	// source := c.PostForm("source")
	// destination := c.PostForm("destination")
	contentType := c.Request.Header.Get("Content-Type")
	req, err := models.NewRequest(c, db)
	if err != nil {
		log.WithError(err).Error("Failed to add request to queue")
		c.String(http.StatusBadGateway, "Failed to add request to queue")
		return
	}

	fmt.Printf("cType %s", contentType)
	c.JSON(http.StatusOK, gin.H{
		"uid":         req.UID(),
		"source":      req.Source(),
		"destination": req.Destination(),
		"body":        req.Body(),
		"status":      req.Status(),
		"RawMsg":      req.RawMsg(),
		"period":      req.Period()})
	return
}

var requestFields = []string{
	"uid", "source", "destination", "ctype", "body", "response", "status", "statuscode",
	"retries", "errors", "frequency_type", "period", "day", "week", "month", "year",
	"msisdn", "raw_msg", "facility", "district", "report_type", "extras", "suspended",
	"body_is_query_param", "submissionid", "url_suffix", "created", "updated", "*"}

// Requests method handles the /queque GET request
func (q *QueueController) Requests(c *gin.Context) {
	log.Info("Gonna read requests in queue")
	log.Info(requestFields)

	page := c.DefaultQuery("page", "1")
	pageSize := c.DefaultQuery("pageSize", "50")
	paging := c.DefaultQuery("paging", "true")
	orderbys := c.QueryArray("order") // property:desc|asc|iasc|idesc
	filters := c.QueryArray("filter")
	qfields := c.DefaultQuery("fields", "*")

	/*Lets get the fields*/
	filtered, relationships := utils.GetFieldsAndRelationships(requestFields, qfields)
	requestsTable := dbutils.Table{Name: "requests", Alias: "r"}
	// serversTable := dbutil.Table{"servers", "s"}

	qbuild := &dbutils.QueryBuilder{}
	qbuild.QueryTemplate = `SELECT %s
FROM %s
%s`
	qbuild.Table = requestsTable
	var fields []dbutils.Field
	for _, f := range filtered {
		fields = append(fields, dbutils.Field{Name: f, TablePrefix: "r", Alias: ""})
	}

	qbuild.Conditions = dbutils.QueryFiltersToConditions(filters, "r")
	qbuild.Fields = fields
	qbuild.OrderBy = dbutils.OrderListToOrderBy(orderbys, requestFields, "r")

	var whereClause string
	if len(qbuild.Conditions) == 0 {
		whereClause = " TRUE"
	} else {
		whereClause = fmt.Sprintf("%s", dbutils.QueryConditions(qbuild.Conditions))
	}
	countquery := fmt.Sprintf("SELECT COUNT(*) AS count FROM requests r WHERE %s", whereClause)

	db := c.MustGet("dbConn").(*sqlx.DB)
	var count int64
	err := db.Get(&count, countquery)
	if err != nil {
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>", err)
		return
	}

	// get the Paginator
	shouldWePage := true
	if paging == "false" {
		shouldWePage = false
	}
	pager := dbutils.GetPaginator(count, pageSize, page, shouldWePage)
	qbuild.Limit = pager.PageSize
	qbuild.Offset = pager.FirstItem() - 1

	jsonquery := fmt.Sprintf("SELECT ROW_TO_JSON(s) FROM (%s) s;", qbuild.ToSQL(shouldWePage))

	var requests []dbutils.MapAnything

	err = db.Select(&requests, jsonquery)
	if err != nil {
		log.WithError(err).Error("Failed to query request")
	}

	c.JSON(http.StatusOK, gin.H{
		"pager":         pager,
		"requests":      requests,
		"order":         orderbys,
		"fields":        qfields,
		"filtered":      filtered,
		"filters":       filters,
		"relationships": relationships,
		"query":         jsonquery,
		"countQuery":    countquery,
		"count":         count})
	return
}

// GetRequest method handles the /queque/:id GET request
func (q *QueueController) GetRequest(c *gin.Context) {
	uid := c.Param("id")
	qfields := c.DefaultQuery("fields", "uid,source,destination,body,status")
	filters := []string{"uid:EQ:" + uid}

	requestsTable := dbutils.Table{Name: "requests", Alias: "r"}
	// change _ to relationships and handle them
	filtered, _ := utils.GetFieldsAndRelationships(requestFields, qfields)

	qbuild := &dbutils.QueryBuilder{}
	qbuild.QueryTemplate = `SELECT %s
FROM %s
%s`
	qbuild.Table = requestsTable
	var fields []dbutils.Field
	for _, f := range filtered {
		fields = append(fields, dbutils.Field{Name: f, TablePrefix: "r", Alias: ""})
	}
	qbuild.Conditions = dbutils.QueryFiltersToConditions(filters, "r")
	qbuild.Fields = fields

	jsonquery := fmt.Sprintf(`
SELECT ROW_TO_JSON(s) FROM (%s) s;`, qbuild.ToSQL(false))

	db := c.MustGet("dbConn").(*sqlx.DB)
	var request dbutils.MapAnything

	err := db.Get(&request, jsonquery)
	if err != nil {
		log.WithError(err).Error("Failed to query request:" + jsonquery)
	}
	c.JSON(http.StatusOK, request)
	return
}

// DeleteRequest method handles the /queque/:id DELETE request
func (q *QueueController) DeleteRequest(c *gin.Context) {
	uid := c.Param("id")
	db := c.MustGet("dbConn").(*sqlx.DB)

	query := fmt.Sprintf("DELETE FROM requests WHERE uid = '%s'", uid)
	_, err := db.Query(query)
	if err != nil {
		log.WithError(err).Error("Failed to delete request:")
		c.JSON(http.StatusConflict, gin.H{"status": "failed to delete"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status": "deleted"})
	return
}

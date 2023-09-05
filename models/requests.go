package models

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/utils"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	"github.com/lib/pq"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"reflect"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

// RequestID is the id for our request
type RequestID int64

// RequestStatus is the status for each request
type RequestStatus string

// constants for the status
const (
	RequestStatusReady = RequestStatus("ready")
	// RequestStatusPending   = RequestStatus("pending")
	RequestStatusExpired   = RequestStatus("expired")
	RequestStatusCompleted = RequestStatus("completed")
	RequestStatusFailed    = RequestStatus("failed")
	// RequestStatusError     = RequestStatus("error")
	// RequestStatusIgnored   = RequestStatus("ignored")
	RequestStatusCanceled = RequestStatus("canceled")
)

// Request represents our requests queue in the database
type Request struct {
	r struct {
		ID                     RequestID     `db:"id" json:"-"`
		UID                    string        `db:"uid" json:"uid"`
		BatchID                string        `db:"batchid" json:"batchId,omitempty"`
		DependsOn              dbutils.Int   `db:"depends_on" json:"dependsOn,omitempty"`
		Source                 int           `db:"source" json:"source" validate:"required"`
		Destination            int           `db:"destination" json:"destination" validate:"required"`
		CCServers              pq.Int64Array `db:"cc_servers" json:"CCServers,omitempty"`
		CCServersStatus        dbutils.JSON  `db:"cc_servers_status" json:"CCServersStatus,omitempty"`
		ContentType            string        `db:"ctype" json:"contentType,omitempty" validate:"required"`
		Body                   string        `db:"body" json:"body" validate:"required"`
		Response               string        `db:"response" json:"response,omitempty"`
		Status                 RequestStatus `db:"status" json:"status,omitempty"`
		StatusCode             string        `db:"statuscode" json:"statusCode,omitempty"`
		Retries                int           `db:"retries" json:"retries,omitempty"`
		Errors                 string        `db:"errors" json:"errors,omitempty"`
		InSubmissoinPeriodbool bool          `db:"in_submission_period" json:"-"`
		FrequencyType          string        `db:"frequency_type" json:"frequencyType,omitempty"`
		Period                 string        `db:"period" json:"period,omitempty"`
		Day                    string        `db:"day" json:"day,omitempty"`
		Week                   string        `db:"week" json:"week,omitempty"`
		Month                  string        `db:"month" json:"month,omitempty"`
		Year                   string        `db:"year" json:"year,omitempty"`
		MSISDN                 string        `db:"msisdn" json:"msisdn,omitempty"`
		RawMsg                 string        `db:"raw_msg" json:"rawMsg,omitempty"`
		Facility               string        `db:"facility" json:"facility,omitempty"`
		District               string        `db:"district" json:"district,omitempty"`
		ReportType             string        `db:"report_type" json:"reportType,omitempty" validate:"required"` // type of object eg event, enrollment, datavalues
		ObjectType             string        `db:"object_type" json:"objectType,omitempty"`                     // type of report as in source system
		Extras                 string        `db:"extras" json:"extras,omitempty"`
		Suspended              bool          `db:"suspended" json:"suspended,omitempty"`                   // whether request is suspended
		BodyIsQueryParams      bool          `db:"body_is_query_param" json:"bodyIsQueryParams,omitempty"` // whether body is to be used a query parameters
		SubmissionID           string        `db:"submissionid" json:"submissionId,omitempty"`             // a reference ID is source system
		URLSuffix              string        `db:"url_suffix" json:"urlSuffix,omitempty"`
		Created                time.Time     `db:"created" json:"created,omitempty"`
		Updated                time.Time     `db:"updated" json:"updated,omitempty"`
		// OrgID              OrgID         `db:"org_id" json:"org_id"` // Lets add these later
	}
}

// ID return the id of this request
func (r *Request) ID() RequestID { return r.r.ID }

// UID returns the uid of this request
func (r *Request) UID() string { return r.r.UID }

// Status returns the status of the request
func (r *Request) Status() RequestStatus { return r.r.Status }

// StatusCode reture the statuscode of the request
func (r *Request) StatusCode() string { return r.r.StatusCode }

// Period returns the period of the request
func (r *Request) Period() string { return r.r.Period }

// ContentType returns the contentType of the request
func (r *Request) ContentType() string { return r.r.ContentType }
func (r *Request) ObjectType() string  { return r.r.ObjectType }

// Errors return the errors after processing requests
func (r *Request) Errors() string { return r.r.Errors }

// BodyIsQueryParams returns whether request body is used as query params
func (r *Request) BodyIsQueryParams() bool { return r.r.BodyIsQueryParams }

// Body returns the body or the request
func (r *Request) Body() string { return r.r.Body }

// RawMsg returns the body or the request
func (r *Request) RawMsg() string { return r.r.RawMsg }

// URLSurffix returns the url surffix used when submitting request
func (r *Request) URLSurffix() string { return r.r.URLSuffix }

// Source return id of source app
func (r *Request) Source() int { return r.r.Source }

// Destination return id of destination app
func (r *Request) Destination() int { return r.r.Destination }

// CreatedOn return time when request was created
func (r *Request) CreatedOn() time.Time { return r.r.Created }

// UpdatedOn return time when request was updated
func (r *Request) UpdatedOn() time.Time { return r.r.Updated }

// NewRequest creates new request and saves it in DB
func NewRequest(c *gin.Context, db *sqlx.DB) (Request, error) {
	source := utils.GetServer(c.Query("source"))
	destination := utils.GetServer(c.Query("destination"))
	fmt.Printf("Source>: %v, Destination: %v", source, destination)

	req := &Request{}
	r := &req.r
	r.Source = source
	r.Destination = destination
	r.UID = utils.GetUID()
	r.ContentType = c.Request.Header.Get("Content-Type")
	r.SubmissionID = c.Query("msgid")
	r.BatchID = c.Query("batchid")
	r.Period = c.Query("period")
	r.Week = c.Query("week")
	r.Month = c.Query("month")
	r.Year = c.Query("year")
	r.MSISDN = c.Query("msisdn")
	r.Facility = c.Query("facility")
	r.RawMsg = c.Query("rawMsg")
	r.URLSuffix = c.DefaultQuery("urlSuffix", "")
	if c.Query("isQueryParams") == "true" {
		r.BodyIsQueryParams = true
	}
	r.ReportType = c.Query("reportType")
	r.ObjectType = c.Query("objectType")
	r.Errors = c.Query("extras")
	r.District = c.Query("district")
	ccList := c.DefaultQuery("cc", config.MFLIntegratorConf.API.MFLCCDHIS2Servers)
	serverIDs := lo.Map(strings.Split(ccList, ","), func(name string, _ int) int64 { // lodash stuff
		return GetServerIDByName(name)
	})
	validServerIDs := lo.Filter(serverIDs, func(item int64, _ int) bool {
		return item > 0
	})
	r.CCServers = validServerIDs

	r.Status = RequestStatusReady

	switch r.ContentType {
	case "application/json", "application/json-patch+json", "application/geo+json":
		var body map[string]interface{} // validate based on dest system endpoint
		if err := c.BindJSON(&body); err != nil {
			fmt.Printf("Error reading json body %v", err)
			log.WithError(err).Error("Error reading request body from POST body")
		}
		b, _ := json.Marshal(body)
		fmt.Println(string(b))
		r.Body = string(b)
	case "application/xml":
		// var xmlBody interface{}
		xmlBody, err := c.GetRawData()
		if err != nil {
			log.WithError(err).Error("Error reading request XML body from POST body")
		}
		r.Body = string(xmlBody)
	default:
		body, err := c.GetRawData()
		if err != nil {
			log.WithError(err).Error("Error reading request from POST body")
		}
		r.Body = string(body)
	}

	_, err := db.NamedExec(insertRequestSQL, r)
	if err != nil {
		log.WithError(err).Error("Error INSERTING Request")
	}

	return *req, nil
}

func NewRequestFromPOST(c *gin.Context, db *sqlx.DB) (Request, error) {
	reqForm := &RequestForm{}

	req := &Request{}
	r := &req.r

	contentType := c.Request.Header.Get("Content-Type")
	switch contentType {
	case "application/json", "application/json-patch+json", "application/geo+json":
		if err := c.BindJSON(reqForm); err != nil {
			log.WithError(err).Error("Error reading request object from POST body")
		}
		// log.WithField("New Server", s).Info("Going to create new server")
	default:
		//
		log.WithField("Content-Type", contentType).Error("Unsupported content-Type")
		return *req, errors.New(fmt.Sprintf("Unsupported Content-Type: %s", contentType))
	}

	// r.Source = int(GetServerIDByName(reqForm.Source))
	// r.Destination = int(GetServerIDByName(reqForm.Destination))
	source := ServerMapByName[reqForm.Source]
	r.Source = int(source.ID())
	destination := ServerMapByName[reqForm.Destination]
	r.Source = int(destination.ID())
	if r.Source == 0 {
		return *req, errors.New(fmt.Sprintf("Source server %s not found!", reqForm.Source))
	}
	if r.Destination == 0 {
		return *req, errors.New(fmt.Sprintf("Destination server %s not found!", reqForm.Destination))

	}
	ccServers := lo.Map(reqForm.CCServers, func(name string, _ int) int64 {
		srv := ServerMapByName[name]
		return int64(srv.ID())
	})
	r.CCServers = ccServers
	r.UID = utils.GetUID()
	r.ContentType = reqForm.ContentType
	r.SubmissionID = reqForm.SubmissionID
	r.BatchID = reqForm.BatchID
	r.Period = reqForm.Period
	r.Week = reqForm.Week
	r.Month = reqForm.Month
	r.Year = reqForm.Year
	r.MSISDN = reqForm.MSISDN
	r.Facility = reqForm.Facility
	r.RawMsg = reqForm.RawMsg
	r.URLSuffix = reqForm.URLSuffix
	if reqForm.BodyIsQueryParams {
		r.BodyIsQueryParams = true
	}
	r.ReportType = reqForm.ReportType
	r.ObjectType = reqForm.ObjectType
	r.Errors = reqForm.Extras
	r.District = reqForm.District
	r.Body = reqForm.Body

	rows, err := db.NamedQuery(insertRequestSQL, r)
	if err != nil {
		log.WithError(err).Error("Error INSERTING Request")
	}
	for rows.Next() {
		var reqId sql.NullInt64
		_ = rows.Scan(&reqId)
		r.ID = RequestID(reqId.Int64)
	}
	_ = rows.Close()
	return *req, nil
}

func (r *Request) RequestDBFields() []string {
	e := reflect.ValueOf(&r.r).Elem()
	var ret []string
	for i := 0; i < e.NumField(); i++ {
		t := e.Type().Field(i).Tag.Get("db")
		if len(t) > 0 {
			ret = append(ret, t)
		}
	}
	ret = append(ret, "*")
	return ret
}

const insertRequestSQL = `
INSERT INTO 
requests (source, destination, depends_on, uid, batchid, ctype, body, body_is_query_param, period, week, month, year,
			raw_msg, msisdn, facility, district, report_type, object_type, extras, url_suffix, cc_servers,
			created, updated) 
	VALUES(:source, :destination, :depends_on, :uid, :batchid, :ctype, :body, :body_is_query_param, :period,
			:week, :month, :year, :raw_msg, :msisdn, :facility, :district, :report_type, :object_type,
			:extras, :url_suffix, :cc_servers, now(), now()) RETURNING id`

type RequestForm struct {
	ID                RequestID   `db:"id" json:"-"`
	UID               string      `db:"uid" json:"uid"`
	BatchID           string      `db:"batchid" json:"batchId,omitempty"`
	Source            string      `db:"source" json:"source" validate:"required"`
	Destination       string      `db:"destination" json:"destination" validate:"required"`
	DependsOn         dbutils.Int `db:"depends_on" json:"dependsOn,omitempty"`
	CCServers         []string    `db:"cc_servers" json:"CCServers,omitempty"`
	ContentType       string      `db:"ctype" json:"contentType,omitempty" validate:"required"`
	Body              string      `db:"body" json:"body" validate:"required"`
	FrequencyType     string      `db:"frequency_type" json:"frequencyType,omitempty"`
	Period            string      `db:"period" json:"period,omitempty"`
	Day               string      `db:"day" json:"day,omitempty"`
	Week              string      `db:"week" json:"week,omitempty"`
	Month             string      `db:"month" json:"month,omitempty"`
	Year              string      `db:"year" json:"year,omitempty"`
	MSISDN            string      `db:"msisdn" json:"msisdn,omitempty"`
	RawMsg            string      `db:"raw_msg" json:"rawMsg,omitempty"`
	Facility          string      `db:"facility" json:"facility,omitempty"`
	District          string      `db:"district" json:"district,omitempty"`
	ReportType        string      `db:"report_type" json:"reportType,omitempty" validate:"required"` // type of report as in source system
	ObjectType        string      `db:"object_type" json:"objectType,omitempty"`                     // type of object eg event, enrollment, datavalues
	Extras            string      `db:"extras" json:"extras,omitempty"`
	Suspended         bool        `db:"suspended" json:"suspended,omitempty"`                   // whether request is suspended
	BodyIsQueryParams bool        `db:"body_is_query_param" json:"bodyIsQueryParams,omitempty"` // whether body is to be used a query parameters
	SubmissionID      string      `db:"submissionid" json:"submissionId,omitempty"`             // a reference ID is source system
	URLSuffix         string      `db:"url_suffix" json:"urlSuffix,omitempty"`
}

func (rq *RequestForm) Save(db *sqlx.DB) (Request, error) {
	req := &Request{}
	r := &req.r

	r.DependsOn = rq.DependsOn
	// r.Source = int(GetServerIDByName(rq.Source))
	// r.Destination = int(GetServerIDByName(rq.Destination))
	source := ServerMapByName[rq.Source]
	r.Source = int(source.ID())
	destination := ServerMapByName[rq.Destination]
	r.Destination = int(destination.ID())
	if r.Source == 0 {
		return *req, errors.New(fmt.Sprintf("Source server %s not found!", rq.Source))
	}
	if r.Destination == 0 {
		return *req, errors.New(fmt.Sprintf("Destination server %s not found!", rq.Destination))

	}
	ccServers := lo.Map(rq.CCServers, func(name string, _ int) int64 {
		return GetServerIDByName(name)
	})
	r.CCServers = ccServers
	r.UID = utils.GetUID()
	r.ContentType = rq.ContentType
	r.SubmissionID = rq.SubmissionID
	r.BatchID = rq.BatchID
	r.Period = rq.Period
	r.Week = rq.Week
	r.Month = rq.Month
	r.Year = rq.Year
	r.MSISDN = rq.MSISDN
	r.Facility = rq.Facility
	r.RawMsg = rq.RawMsg
	r.URLSuffix = rq.URLSuffix
	if rq.BodyIsQueryParams {
		r.BodyIsQueryParams = true
	}
	r.ReportType = rq.ReportType
	r.ObjectType = rq.ObjectType
	r.Errors = rq.Extras
	r.District = rq.District
	r.Body = rq.Body

	rows, err := db.NamedQuery(insertRequestSQL, r)
	if err != nil {
		log.WithError(err).Error("Error INSERTING Request")
	}

	for rows.Next() {
		var reqId sql.NullInt64
		_ = rows.Scan(&reqId)
		r.ID = RequestID(reqId.Int64)
	}
	_ = rows.Close()
	return *req, nil
}

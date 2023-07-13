package main

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/buger/jsonparser"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RequestObj is our object used by consumers
type RequestObj struct {
	ID                 models.RequestID     `db:"id"`
	Source             int                  `db:"source"`
	Destination        int                  `db:"destination"`
	Body               string               `db:"body"`
	Retries            int                  `db:"retries"`
	InSubmissionPeriod bool                 `db:"in_submission_period"`
	ContentType        string               `db:"ctype"`
	ObjectType         string               `db:"object_type"`
	BodyIsQueryParams  bool                 `db:"body_is_query_param"`
	SubmissionID       int64                `db:"submissionid"`
	URLSurffix         string               `db:"url_suffix"`
	Suspended          bool                 `db:"suspended"`
	Status             models.RequestStatus `db:"status"`
	StatusCode         string               `db:"statuscode"`
	Errors             string               `db:"errors"`
}

const updateRequestSQL = `
UPDATE requests SET (status, statuscode, errors, retries, updated)
	= (:status, :statuscode, :errors, :retries, timeofday()::::timestamp) WHERE id = :id
`
const updateStatusSQL = `
	UPDATE requests SET (status,  updated) = (:status, timeofday()::::timestamp)
	WHERE id = :id`

// updateRequest is used by consumers to update request in the db
func (r *RequestObj) updateRequest(tx *sqlx.Tx) {
	_, err := tx.NamedExec(updateRequestSQL, r)
	if err != nil {
		log.WithError(err).Error("Error updating request status")
	}
}

// updateRequestStatus
func (r *RequestObj) updateRequestStatus(tx *sqlx.Tx) {
	_, err := tx.NamedExec(updateStatusSQL, r)
	if err != nil {
		log.WithError(err).Error("Error updating request")
	}
}
func (r *RequestObj) withStatus(s models.RequestStatus) *RequestObj { r.Status = s; return r }

func (r *RequestObj) canSendRequest(tx *sqlx.Tx, server models.Server) bool {
	// check if we have exceeded retries
	if r.Retries > config.MFLIntegratorConf.Server.MaxRetries {
		r.Status = models.RequestStatusExpired
		r.updateRequestStatus(tx)
		return false
	}
	// check if we're  suspended
	if server.Suspended() {
		log.WithFields(log.Fields{
			"server": server.ID(),
			"name":   server.Name(),
		}).Info("Destination server is suspended")
		return false
	}
	// check if we're out of submission period
	if !r.InSubmissionPeriod {
		log.WithFields(log.Fields{
			"server": server.ID(),
			"name":   server.Name(),
		}).Info("Destination server out of submission period")
		return false
	}
	// check if this request is  blacklisted
	if r.Suspended {
		r.Errors = "Blacklisted"
		r.StatusCode = "ERROR7"
		r.Retries += 1
		r.Status = models.RequestStatusCanceled
		r.updateRequest(tx)
		log.WithFields(log.Fields{
			"request": r.ID,
		}).Info("Request blacklisted")
		return false
	}
	// check if body is empty
	if len(strings.TrimSpace(r.Body)) == 0 {
		r.Status = models.RequestStatusFailed
		r.StatusCode = "ERROR1"
		r.Errors = "Request has empty body"
		r.updateRequest(tx)
		log.WithFields(log.Fields{
			"request": r.ID,
		}).Info("Request has empty body")
		return false
	}
	return true
}

func (r *RequestObj) unMarshalBody() (interface{}, error) {
	var data interface{}
	switch r.ObjectType {
	case "ORGANISATION_UNITS":
		//data = models.DataValuesRequest{}
		//err := json.Unmarshal([]byte(r.Body), &data)
		//if err != nil {
		//	return nil, err
		//}
	default:
		data = map[string]interface{}{}
		err := json.Unmarshal([]byte(r.Body), &data)
		if err != nil {
			return nil, err
		}

	}
	return data, nil
}

// sendRequest sends request to destination server
func (r *RequestObj) sendRequest(destination models.Server) (*http.Response, error) {
	data, err := r.unMarshalBody()
	if err != nil {
		return nil, err
	}
	marshalled, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("Failed to marshal request body")
		return nil, err
	}
	req, err := http.NewRequest(destination.HTTPMethod(), destination.URL(), bytes.NewReader(marshalled))

	switch destination.AuthMethod() {
	case "Token":
		// Add API token
		tokenAuth := "ApiToken " + destination.AuthToken()
		req.Header.Set("Authorization", tokenAuth)
		log.WithField("AuthToken", tokenAuth).Info("The authentication token:")
	default: // Basic Auth
		// Add basic authentication
		auth := destination.Username() + ":" + destination.Password()
		basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		req.Header.Set("Authorization", basicAuth)

	}

	req.Header.Set("Content-Type", r.ContentType)
	// Create custom transport with TLS settings
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			// Set any necessary TLS settings here
			// For example, to disable certificate validation:
			InsecureSkipVerify: true,
		},
	}

	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func Produce(db *sqlx.DB, jobs chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println("Producer staring:!!!")
	for {
		log.Println("Going to read requests")
		rows, err := db.Queryx(`
                SELECT id FROM requests WHERE status = $1 ORDER BY created LIMIT 100000
                `, "ready")
		if err != nil {
			log.Fatalln(err)
		}

		for rows.Next() {
			var requestID int
			err := rows.Scan(&requestID)
			if err != nil {
				log.Fatalln("==>", err)
			}
			// log.Printf("Adding request [id: %v]\n", requestID)

			go func() {
				jobs <- requestID
			}()
			log.Printf("Added Request [id: %v]\n", requestID)
		}
		if err := rows.Err(); err != nil {
			log.Println(err)
		}
		rows.Close()

		log.Println("Fetch Requests")
		log.Printf("Going to sleep for: %v", config.MFLIntegratorConf.Server.RequestProcessInterval)
		// Not good enough but let's bare with the sleep this initial version
		time.Sleep(
			time.Duration(config.MFLIntegratorConf.Server.RequestProcessInterval) * time.Second)
	}
}

// consume is the consumer go routine
func Consume(db *sqlx.DB, worker int, jobs <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("Calling Consumer")

	for req := range jobs {
		fmt.Printf("Message %v is consumed by worker %v.\n", req, worker)

		reqObj := RequestObj{}
		tx := db.MustBegin()
		err := tx.QueryRowx(`
                SELECT
                        id, source, destination, body, retries, in_submission_period(destination),
                        ctype, object_type, body_is_query_param, submissionid, url_suffix,suspended,
                        statuscode, status, errors
                        
                FROM requests
                WHERE id = $1 FOR UPDATE NOWAIT`, req).StructScan(&reqObj)
		if err != nil {
			log.WithError(err).Error("Error reading request for processing")
		}
		log.WithFields(log.Fields{
			"worker":     worker,
			"request-ID": req}).Info("Handling Request")
		/* Work on the request */
		// dest = utils.GetServer(reqObj.Destination)
		log.WithFields(log.Fields{"servers": models.ServerMap}).Info("Servers")
		if server, ok := models.ServerMap[strconv.Itoa(reqObj.Destination)]; ok {
			fmt.Printf("Found Server Config: %v, URL: %s\n", server, server.URL())
			if reqObj.canSendRequest(tx, server) {
				log.WithFields(log.Fields{"request": reqObj.ID}).Info("Request can be processed")
				// send request
				resp, err := reqObj.sendRequest(server)
				if err != nil {
					log.WithError(err).WithField("RequestID", reqObj.ID).Error(
						"Failed to send request")
					reqObj.Status = models.RequestStatusFailed
					reqObj.StatusCode = "ERROR02"
					reqObj.Errors = "Server possibly unreachable"
					reqObj.Retries += 1
					reqObj.updateRequest(tx)
					return
				}

				if !server.UseAsync() {
					result := models.ImportSummary{}
					json.NewDecoder(resp.Body).Decode(&result)
					if resp.StatusCode/100 == 2 {
						reqObj.withStatus(models.RequestStatusCompleted).updateRequestStatus(tx)
						log.WithFields(log.Fields{
							"status":      result.Response.Status,
							"description": result.Response.Description,
							"importCount": result.Response.ImportCount,
							"conflicts":   result.Response.Conflicts,
						}).Info("Request completed successfully!")
					}
				} else {
					// var result map[string]interface{}
					// json.NewDecoder(resp.Body).Decode(&result)
					bodyBytes, err := io.ReadAll(resp.Body)
					if err != nil {
						reqObj.withStatus(models.RequestStatusFailed).updateRequestStatus(tx)
						log.WithError(err).Error("Could not read response")
						return
					}
					log.WithField("responseBytes", bodyBytes).Info("Response Payload")
					if resp.StatusCode/100 == 2 {
						v, _, _, _ := jsonparser.Get(bodyBytes, "status")
						fmt.Println(v)
					}

				}
				resp.Body.Close()
			}

		} else {
			log.WithFields(log.Fields{"server": reqObj.Destination}).Info(
				"Failed to load server configuration")
		}

		tx.Commit()
	}

}

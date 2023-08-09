package main

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	"github.com/buger/jsonparser"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ServerStatus is the status object within each request to
// track status for the CC servers
type ServerStatus struct {
	Retries    int                  `json:"retries"`
	Status     models.RequestStatus `json:"status,omitempty"`
	StatusCode string               `json:"statuscode,omitempty"`
	Response   string               `json:"response,omitempty"`
	Errors     string               `json:"errors"`
}

// Scan is the db driver scanner for ServerStatus
func (a *ServerStatus) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &a)
}

// RequestObj is our object used by consumers
type RequestObj struct {
	ID                 models.RequestID     `db:"id"`
	Source             int                  `db:"source"`
	Destination        int                  `db:"destination"`
	CCServers          pq.Int32Array        `db:"cc_servers" json:"CCServers"`
	CCServersStatus    dbutils.MapAnything  `db:"cc_servers_status" json:"CCServersStatus"`
	Body               string               `db:"body"`
	Retries            int                  `db:"retries"`
	InSubmissionPeriod bool                 `db:"in_submission_period"`
	ContentType        string               `db:"ctype"`
	ObjectType         string               `db:"object_type"`
	BodyIsQueryParams  bool                 `db:"body_is_query_param"`
	SubmissionID       string               `db:"submissionid"`
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
	// _ = db.Commit()
}

// updateCCServerStatus updates the status for CC servers on the request
func (r *RequestObj) updateCCServerStatus(tx *sqlx.Tx) {
	_, err := tx.NamedExec(`UPDATE requests SET cc_servers_status = :cc_servers_status WHERE id = :id`, r)
	if err != nil {
		log.WithError(err).Error("Error updating request CC Server Status!")
	}
}

// updateRequestStatus
func (r *RequestObj) updateRequestStatus(tx *sqlx.Tx) {
	_, err := tx.NamedExec(updateStatusSQL, r)
	if err != nil {
		log.WithError(err).Error("Error updating request")
	}
}

// withStatus updates the RequestObj status with passed value
func (r *RequestObj) withStatus(s models.RequestStatus) *RequestObj { r.Status = s; return r }

// canSendRequest checks if a queued request is eligible for sending
// based on constraints on request and the receiving servers
func (r *RequestObj) canSendRequest(tx *sqlx.Tx, server models.Server, serverInCC bool) bool {
	if !config.MFLIntegratorConf.Server.SyncOn {
		return false // helps to globally turn off sync and debug
	}
	if !serverInCC {
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
				"server": server.ID,
				"name":   server.Name,
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
	} else {
		// if ccServerStatus := r.CCServersStatus[server];
		// lo.Filter()
		ccServers := lo.Filter(r.CCServers, func(item int32, index int) bool {
			if item == int32(server.ID()) && item != int32(r.Destination) {
				// just make sure we don't sent to cc server same as destination on request
				return true
			}
			return false
		})
		if len(ccServers) > 0 {
			var ccServerStatus ServerStatus
			if ccServerObject, ok := models.ServerMap[fmt.Sprintf("%d", ccServers[0])]; ok {
				// Check if cc server is suspended
				if ccServerObject.Suspended() {
					return false
				}
				// get server status from request
				if ccstatusObj, ok := r.CCServersStatus[fmt.Sprintf("%d", ccServerObject.ID())]; ok {
					// err := json.Unmarshal(ccstatusObj.(dbutils.MapAnything), &ccServerStatus)
					//st, err := json.Marshal(ccstatusObj)
					//if err != nil {
					//	log.WithError(err).Info("Failed to marshal cc server JSON status object!")
					//	return false
					//}
					//err = json.Unmarshal(st, &ccServerStatus)
					//if err != nil {
					//	log.WithError(err).Info("Failed to unmarshal json into CC Status object")
					//	return false
					//}
					err := ccServerStatus.Scan(ccstatusObj)
					if err != nil {
						log.WithError(err).Info("Failed to scan []bytes of cc server status")
					}
				}
				// Now check with the ccServerStatus object for sending eligibility
				// check if we have exceeded the retries for this server
				if ccServerStatus.Retries > config.MFLIntegratorConf.Server.MaxRetries {
					ccServerStatus.Status = models.RequestStatusExpired
					var ccServerStatusJSON dbutils.MapAnything
					err := ccServerStatusJSON.Scan(ccServerStatus)
					if err != nil {
						log.WithError(err).Error("Failed to convert CC server status to required db type")
						return false
					}
					r.CCServersStatus = ccServerStatusJSON
					r.updateCCServerStatus(tx)
					return false
				}
				// check if we're out of submission period
				if !ccServerObject.InSubmissionPeriod(tx) {
					log.WithFields(log.Fields{
						"server": ccServerObject.ID,
						"name":   ccServerObject.Name,
					}).Info("Destination server out of submission period")
					return false
				}

				// check if we're  suspended
				if ccServerObject.Suspended() {
					ccServerStatus.Errors = "Blacklisted"
					ccServerStatus.StatusCode = "ERROR7"
					ccServerStatus.Retries += 1
					ccServerStatus.Status = models.RequestStatusCanceled
					var ccServerStatusJSON dbutils.MapAnything
					err := ccServerStatusJSON.Scan(ccServerStatus)
					if err != nil {
						log.WithError(err).Error("Failed to convert CC server status to required db type")
						return false
					}
					r.CCServersStatus = ccServerStatusJSON
					r.updateCCServerStatus(tx)
					log.WithFields(log.Fields{
						"server": ccServerObject.ID,
						"name":   ccServerObject.Name,
					}).Info("Destination server is suspended")
					return false
				}
				// check if this request is  blacklisted
				if r.Suspended {
					ccServerStatus.Errors = "Blacklisted"
					ccServerStatus.StatusCode = "ERROR7"
					ccServerStatus.Retries += 1
					ccServerStatus.Status = models.RequestStatusCanceled
					var ccServerStatusJSON dbutils.MapAnything
					err := ccServerStatusJSON.Scan(ccServerStatus)
					if err != nil {
						log.WithError(err).Error("Failed to convert CC server status to required db type")
						return false
					}
					r.CCServersStatus = ccServerStatusJSON
					r.updateCCServerStatus(tx)
					log.WithFields(log.Fields{
						"request": r.ID, "CCServer": ccServers[0],
					}).Info("Request blacklisted for CC Server")
					return false
				}

				// check if body is empty
				if len(strings.TrimSpace(r.Body)) == 0 {
					ccServerStatus.Status = models.RequestStatusFailed
					ccServerStatus.StatusCode = "ERROR1"
					ccServerStatus.Errors = "Request has empty body"
					var ccServerStatusJSON dbutils.MapAnything
					err := ccServerStatusJSON.Scan(ccServerStatus)
					if err != nil {
						log.WithError(err).Error("Failed to convert CC server status to required db type")
						return false
					}
					r.CCServersStatus = ccServerStatusJSON
					r.updateCCServerStatus(tx)

					log.WithFields(log.Fields{
						"request": r.ID, "CCServer": ccServerObject.ID,
					}).Info("Request has empty body")
					return false
				}

			}
		}

		return false
	}

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
	destURL := destination.URL()
	if len(r.URLSurffix) > 0 {
		destURL += r.URLSurffix
	}
	req, err := http.NewRequest(destination.HTTPMethod(), destURL, bytes.NewReader(marshalled))

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

//Produce gets all the ready requests in the queue
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
		_ = rows.Close()

		log.Println("Fetch Requests")
		log.Printf("Going to sleep for: %v", config.MFLIntegratorConf.Server.RequestProcessInterval)
		// Not good enough but let's bare with the sleep this initial version
		time.Sleep(
			time.Duration(config.MFLIntegratorConf.Server.RequestProcessInterval) * time.Second)
	}
}

// Consume is the consumer go routine
func Consume(db *sqlx.DB, worker int, jobs <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("Calling Consumer")

	for req := range jobs {
		fmt.Printf("Message %v is consumed by worker %v.\n", req, worker)

		reqObj := RequestObj{}
		tx := db.MustBegin()
		err := tx.QueryRowx(`
                SELECT
                        id, source, destination, cc_servers, cc_servers_status, body, retries, in_submission_period(destination),
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
		// log.WithFields(log.Fields{"servers": models.ServerMap}).Info("Servers")
		if reqDestination, ok := models.ServerMap[fmt.Sprintf("%d", reqObj.Destination)]; ok {
			_ = ProcessRequest(tx, reqObj, reqDestination, false)
		} else {
			// Using Go lodash to process
			lo.Map(reqObj.CCServers, func(item int32, index int) error {
				log.WithFields(log.Fields{"CCServerID": item, "ServerIndex": index}).Info("CC Server:")
				if ccServer, ok := models.ServerMap[fmt.Sprintf("%d", item)]; ok {
					return ProcessRequest(tx, reqObj, ccServer, true)
				}
				return nil
			})
		}

		err = tx.Commit()
		if err != nil {
			log.WithError(err).Error("Failed to Commit transaction after processing!")
		}
	}

}

// ProcessRequest handles a ready request
func ProcessRequest(tx *sqlx.Tx, reqObj RequestObj, destination models.Server, serverInCC bool) error {
	if reqObj.canSendRequest(tx, destination, serverInCC) {
		log.WithFields(log.Fields{"request": reqObj.ID}).Info("Request can be processed")
		// send request
		resp, err := reqObj.sendRequest(destination)
		if err != nil {
			log.WithError(err).WithField("RequestID", reqObj.ID).Error(
				"Failed to send request")
			reqObj.Status = models.RequestStatusFailed
			reqObj.StatusCode = "ERROR02"
			reqObj.Errors = "Server possibly unreachable"
			reqObj.Retries += 1
			reqObj.updateRequest(tx)
			return err
		}

		if !destination.UseAsync() {
			result := models.ImportSummary{}
			respBody, _ := io.ReadAll(resp.Body)
			err := json.Unmarshal(respBody, &result)
			// err := json.NewDecoder(resp.Body).Decode(&result)
			if err != nil {
				reqObj.Status = models.RequestStatusFailed
				reqObj.StatusCode = "ERROR03"
				reqObj.Errors = "Failed to decode import summary"
				reqObj.Retries += 1
				reqObj.updateRequest(tx)
				log.WithField("Resp", string(respBody)).WithError(err).Error("Failed to decode import summary")
				return err
			}
			if resp.StatusCode/100 == 2 {
				reqObj.withStatus(models.RequestStatusCompleted).updateRequestStatus(tx)
				log.WithFields(log.Fields{
					"status":      result.Response.Status,
					"description": result.Response.Description,
					"importCount": result.Response.ImportCount,
					"conflicts":   result.Response.Conflicts,
				}).Info("Request completed successfully!")
				return nil
			} else {
				reqObj.withStatus(models.RequestStatusFailed).updateRequestStatus(tx)
				log.WithFields(log.Fields{"Request": reqObj.Body, "Response": string(respBody)}).Warn("A non 200 response")

			}
		} else {
			// var result map[string]interface{}
			// json.NewDecoder(resp.Body).Decode(&result)
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				reqObj.withStatus(models.RequestStatusFailed).updateRequestStatus(tx)
				log.WithError(err).Error("Could not read response")
				return err
			}
			log.WithField("responseBytes", bodyBytes).Info("Response Payload")
			if resp.StatusCode/100 == 2 {
				v, _, _, _ := jsonparser.Get(bodyBytes, "status")
				fmt.Println(v)
			}

		}
		err = resp.Body.Close()
		if err != nil {
			log.WithError(err).Error("Failed to close response body")
		}
	}
	return nil
}

// StartConsumers starts the consumer go routines
func StartConsumers(jobs <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	dbURI := config.MFLIntegratorConf.Database.URI

	fmt.Printf("Going to create %d Consumers!!!!!\n", config.MFLIntegratorConf.Server.MaxConcurrent)
	for i := 1; i <= config.MFLIntegratorConf.Server.MaxConcurrent; i++ {

		newConn, err := sqlx.Connect("postgres", dbURI)
		if err != nil {
			log.Fatalln("Request processor failed to connect to database: %v", err)
		}
		fmt.Printf("Adding Consumer: %d\n", i)
		wg.Add(1)
		go Consume(newConn, i, jobs, wg)
	}
	log.WithFields(log.Fields{"MaxConsumers": config.MFLIntegratorConf.Server.MaxConcurrent}).Info("Created Consumers: ")
}

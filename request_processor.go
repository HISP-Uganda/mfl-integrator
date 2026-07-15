package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/HISP-Uganda/mfl-integrator/clients"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	"github.com/buger/jsonparser"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
)

var outboundHTTPClient = clients.NewStandardHTTPClient()

type producerCycleStats struct {
	fetched     int
	enqueued    int
	duplicates  int
	scanErrors  int
	queryErrors int
}

type retryRunStats struct {
	scanned            int
	destinationRetried int
	ccRetried          int
	expired            int
	scanErrors         int
	txErrors           int
	missingServers     int
	commitErrors       int
	queryErrors        int
	canceled           bool
}

func requestLogFields(r RequestObj, destination models.Server, serverInCC bool) log.Fields {
	fields := log.Fields{
		"request_id":          r.ID,
		"destination_id":      destination.ID(),
		"destination_name":    destination.Name(),
		"server_in_cc":        serverInCC,
		"request_retries":     r.Retries,
		"content_type":        r.ContentType,
		"object_type":         r.ObjectType,
		"body_is_query_param": r.BodyIsQueryParams,
	}
	if r.DependsOn > 0 {
		fields["depends_on"] = r.DependsOn
	}
	return fields
}

// ServerStatus is the status object within each request to
// track status for the CC servers
type ServerStatus struct {
	Retries    int                  `json:"retries"`
	Status     models.RequestStatus `json:"status,omitempty"`
	StatusCode string               `json:"statuscode,omitempty"`
	Response   string               `json:"response,omitempty"`
	Errors     string               `json:"errors"`
}

func getCCServerStatus(statuses dbutils.MapAnything, serverID int64) ServerStatus {
	status := ServerStatus{}
	if statuses == nil {
		return status
	}
	raw, ok := statuses[fmt.Sprintf("%d", serverID)]
	if !ok || raw == nil {
		return status
	}
	switch val := raw.(type) {
	case ServerStatus:
		return val
	case map[string]interface{}:
		if retries, ok := ccServerRetries(val); ok {
			status.Retries = retries
		}
		if statusCode, ok := val["statusCode"].(string); ok {
			status.StatusCode = statusCode
		}
		if response, ok := val["response"].(string); ok {
			status.Response = response
		}
		if errorsValue, ok := val["errors"].(string); ok {
			status.Errors = errorsValue
		}
		if requestStatus, ok := val["status"].(string); ok {
			status.Status = models.RequestStatus(requestStatus)
		}
	}
	return status
}

func setCCServerStatus(statuses dbutils.MapAnything, serverID int64, status ServerStatus) dbutils.MapAnything {
	if statuses == nil {
		statuses = dbutils.MapAnything{}
	}
	statuses[fmt.Sprintf("%d", serverID)] = map[string]interface{}{
		"retries":    status.Retries,
		"status":     status.Status,
		"statusCode": status.StatusCode,
		"response":   status.Response,
		"errors":     status.Errors,
	}
	return statuses
}

func ccServerRetries(status map[string]any) (int, bool) {
	if status == nil {
		return 0, false
	}
	switch val := status["retries"].(type) {
	case float64:
		return int(val), true
	case int:
		return val, true
	case int32:
		return int(val), true
	case int64:
		return int(val), true
	default:
		return 0, false
	}
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
	DependsOn          dbutils.Int          `db:"depends_on"`
	CCServers          pq.Int32Array        `db:"cc_servers" json:"CCServers"`
	CCServersStatus    dbutils.MapAnything  `db:"cc_servers_status" json:"CCServersStatus"`
	Body               string               `db:"body"`
	Response           string               `db:"response"`
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
UPDATE requests SET (status, statuscode, errors, retries, response, updated)
	= (:status, :statuscode, :errors, :retries, :response, current_timestamp) WHERE id = :id
`
const updateStatusSQL = `
	UPDATE requests SET (status,  updated) = (:status, current_timestamp)
	WHERE id = :id`

// HasDependency returns true if request has a request it depends on
func (r *RequestObj) HasDependency() bool {
	return r.DependsOn > 0
}

// DependencyCompleted returns true request's dependent request was completed
func (r *RequestObj) DependencyCompleted(tx *sqlx.Tx) bool {
	if r.HasDependency() {
		completed := false
		err := tx.Get(&completed, "SELECT status = 'completed' FROM requests WHERE id = $1", r.DependsOn)
		if err != nil {
			log.WithError(err).Info("Error reading dependent request status")
			return false
		}
		return completed
	}
	return false
}

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
	log.WithFields(log.Fields{"request_id": r.ID, "cc_server_status": r.CCServersStatus}).Debug("Updated CC server request status")
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
	reason := ""
	log.WithField("Reason", reason)
	if !config.MFLIntegratorConf.Server.SyncOn {
		reason = "Synchronisation turned off."
		log.Info("Synchronisation turned off. Set sync_on to true to activate")
		return false // helps to globally turn off sync and debug
	}

	if r.HasDependency() {
		if !r.DependencyCompleted(tx) {
			reason = "Dependency incomplete."
			log.WithFields(log.Fields{
				"requestID":  r.ID,
				"dependency": r.DependsOn,
			}).Info("Request dependency incomplete")
			return false
		}
	}
	if !serverInCC {
		// check if we have exceeded retries
		if r.Retries > config.MFLIntegratorConf.Server.MaxRetries {
			reason = "Max retries exceeded."
			r.Status = models.RequestStatusExpired
			r.updateRequestStatus(tx)
			log.WithFields(log.Fields{
				"requestID": r.ID,
				"retries":   r.Retries,
				"reason":    reason,
			}).Info("Cannot send request")
			return false
		}
		// check if we're  suspended
		if server.Suspended() {
			reason = "Destination server is suspended."
			log.WithFields(log.Fields{
				"server": server.ID(),
				"name":   server.Name(),
			}).Info("Destination server is suspended")
			return false
		}
		// check if we're out of submission period
		if !r.InSubmissionPeriod {
			reason = "Destination server out of submission period."
			log.WithFields(log.Fields{
				"server": server.ID,
				"name":   server.Name,
			}).Info("Destination server out of submission period")
			return false
		}
		// check if this request is  blacklisted
		if r.Suspended {
			reason = "Request blacklisted."
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
			reason = "Request has empty body."
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
			if ccServerObject, ok := models.GetServerFromRegistryByID(fmt.Sprintf("%d", ccServers[0])); ok {
				// get server status from request
				ccServerStatus = getCCServerStatus(r.CCServersStatus, int64(ccServerObject.ID()))
				// Now check with the ccServerStatus object for sending eligibility
				// check if we have exceeded the retries for this server
				if ccServerStatus.Retries > config.MFLIntegratorConf.Server.MaxRetries {
					ccServerStatus.Status = models.RequestStatusExpired
					r.CCServersStatus = setCCServerStatus(r.CCServersStatus, int64(ccServerObject.ID()), ccServerStatus)
					r.updateCCServerStatus(tx)
					log.WithFields(log.Fields{
						"server":   ccServerObject.Name(),
						"serverID": ccServerObject.ID(),
						"request":  r.ID,
					}).Info("Request to CC server exceeded max retries")
					return false
				}
				// check if we're out of submission period
				if !ccServerObject.InSubmissionPeriod(tx) {
					log.WithFields(log.Fields{
						"server": ccServerObject.ID(),
						"name":   ccServerObject.Name(),
					}).Info("Destination server out of submission period")
					return false
				}

				// check if we're  suspended
				if ccServerObject.Suspended() {
					ccServerStatus.Errors = "Blacklisted"
					ccServerStatus.StatusCode = "ERROR7"
					ccServerStatus.Retries += 1
					ccServerStatus.Status = models.RequestStatusCanceled
					r.CCServersStatus = setCCServerStatus(r.CCServersStatus, int64(ccServerObject.ID()), ccServerStatus)
					r.updateCCServerStatus(tx)
					log.WithFields(log.Fields{
						"server": ccServerObject.ID(),
						"name":   ccServerObject.Name(),
					}).Info("Destination server is suspended")
					return false
				}
				// check if this request is  blacklisted
				if r.Suspended {
					ccServerStatus.Errors = "Blacklisted"
					ccServerStatus.StatusCode = "ERROR7"
					ccServerStatus.Retries += 1
					ccServerStatus.Status = models.RequestStatusCanceled
					r.CCServersStatus = setCCServerStatus(r.CCServersStatus, int64(ccServerObject.ID()), ccServerStatus)
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
					r.CCServersStatus = setCCServerStatus(r.CCServersStatus, int64(ccServerObject.ID()), ccServerStatus)
					r.updateCCServerStatus(tx)

					log.WithFields(log.Fields{
						"request": r.ID, "CCServer": ccServerObject.ID,
					}).Info("Request has empty body")
					return false
				}
				return true

			}
		}

		return true
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
func (r *RequestObj) sendRequest(ctx context.Context, destination models.Server) (*http.Response, error) {
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
	if len(r.URLSurffix) > 1 {
		destURL += r.URLSurffix
	}
	req, err := http.NewRequestWithContext(ctx, destination.HTTPMethod(), destURL, bytes.NewReader(marshalled))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", r.ContentType)
	req.Header.Set("User-Agent", clients.DefaultUserAgent)
	switch destination.AuthMethod() {
	case "Token":
		clients.ApplyAuth(req, clients.AuthConfig{Token: destination.AuthToken(), TokenScheme: "ApiToken"})
	case "Basic", "Basic Auth":
		clients.ApplyAuth(req, clients.AuthConfig{Username: destination.Username(), Password: destination.Password()})
	}
	resp, err := outboundHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// var RequestsMap = make(map[string]int)

//// Produce gets all the ready requests in the queue
//func Produce(db *sqlx.DB, jobs chan<- int, wg *sync.WaitGroup, mutex *sync.Mutex, seenMap map[models.RequestID]bool) {
//	defer wg.Done()
//	log.Println("Producer staring:!!!")
//
//	// RequestsMap[""] = 6
//	for {
//
//		// log.Println("Going to read requests")
//		rows, err := db.Queryx(`
//                SELECT
//                    id FROM requests
//                WHERE status = $1  and status_of_dependence(id) IN ('completed', '')
//                ORDER BY depends_on desc, created LIMIT 100000
//                `, "ready")
//		if err != nil {
//			log.WithError(err).Error("ERROR READING READY REQUESTS!!!")
//		}
//		requestsCount := 0
//		for rows.Next() {
//
//			requestsCount += 1
//			var requestID int
//			err := rows.Scan(&requestID)
//			if err != nil {
//				// log.Fatalln("==>", err)
//				log.WithError(err).Error("Error reading request from queue:")
//			}
//			mutex.Lock()
//			if _, exists := seenMap[models.RequestID(requestID)]; exists {
//				log.WithField("requestID", requestID).Info("Request already in dynamic queue")
//				continue
//			}
//			mutex.Unlock()
//			go func(req int) {
//				// Let see if we can recover from panics XXX
//				defer func() {
//					if r := recover(); r != nil {
//						fmt.Println("Recovered in Produce", r)
//
//					}
//				}()
//				mutex.Lock()
//				defer mutex.Unlock()
//				//if _, exists := seenMap[models.RequestID(req)]; exists {
//				//	log.WithField("requestID", req).Info("Request already in dynamic queue")
//				//	return
//				//}
//				jobs <- req
//				seenMap[models.RequestID(req)] = true
//				log.Info(fmt.Sprintf("Added Request [id: %v]", req))
//			}(requestID)
//
//		}
//		if err := rows.Err(); err != nil {
//			log.WithError(err).Error("Error reading requests")
//		}
//		_ = rows.Close()
//		if requestsCount > 0 {
//			log.WithField("requestsAdded", requestsCount).Info("Fetched Requests")
//			log.Info(fmt.Sprintf("Going to sleep for: %v", config.MFLIntegratorConf.Server.RequestProcessInterval))
//		}
//		// Not good enough but let's bare with the sleep this initial version
//		time.Sleep(
//			time.Duration(config.MFLIntegratorConf.Server.RequestProcessInterval) * time.Second)
//	}
//}

// Produce gets all the ready requests in the queue
func Produce(ctx context.Context, db *sqlx.DB, jobs chan<- int, wg *sync.WaitGroup, mutex *sync.Mutex, seenMap map[models.RequestID]bool) {
	defer wg.Done()
	defer close(jobs) // signal downstream that no more jobs will be produced

	log.WithField("poll_interval_seconds", config.MFLIntegratorConf.Server.RequestProcessInterval).Info("Request producer started")
	sleepInterval := time.Duration(config.MFLIntegratorConf.Server.RequestProcessInterval) * time.Second

	for {
		cycleStart := time.Now()
		stats := producerCycleStats{}

		// Stop promptly on cancellation.
		select {
		case <-ctx.Done():
			log.WithError(ctx.Err()).Info("Producer stopping due to context cancellation")
			return
		default:
		}

		rows, err := db.Queryx(`
                    SELECT 
                        id FROM requests 
                    WHERE status = $1  and status_of_dependence(id) IN ('completed', '') 
                    ORDER BY depends_on desc, created LIMIT 100000
                    `, "ready")
		if err != nil {
			stats.queryErrors++
			log.WithError(err).Error("Failed to read ready requests")
			sleep := time.NewTimer(sleepInterval)
			select {
			case <-ctx.Done():
				sleep.Stop()
				log.WithError(ctx.Err()).Info("Producer stopping due to context cancellation")
				return
			case <-sleep.C:
			}
			continue
		}

		requestsCount := 0
		for rows.Next() {
			// Allow cancellation while iterating rows.
			select {
			case <-ctx.Done():
				_ = rows.Close()
				log.WithError(ctx.Err()).Info("Producer stopping due to context cancellation")
				return
			default:
			}

			requestsCount += 1
			stats.fetched++
			var requestID int
			err := rows.Scan(&requestID)
			if err != nil {
				stats.scanErrors++
				log.WithError(err).Error("Failed to scan ready request row")
				continue
			}

			mutex.Lock()
			if _, exists := seenMap[models.RequestID(requestID)]; exists {
				mutex.Unlock()
				stats.duplicates++
				log.WithField("request_id", requestID).Debug("Request already in in-memory queue")
				continue
			}
			mutex.Unlock()

			// Send directly (no per-request goroutine) so we can cancel gracefully.
			select {
			case <-ctx.Done():
				_ = rows.Close()
				log.WithError(ctx.Err()).Info("Producer stopping due to context cancellation")
				return
			case jobs <- requestID:
				mutex.Lock()
				seenMap[models.RequestID(requestID)] = true
				mutex.Unlock()
				stats.enqueued++
				log.WithField("request_id", requestID).Info("Enqueued request for processing")
			}
		}

		if err := rows.Err(); err != nil {
			log.WithError(err).Error("Error reading requests")
		}
		_ = rows.Close()

		if requestsCount > 0 || stats.duplicates > 0 || stats.scanErrors > 0 || stats.queryErrors > 0 {
			log.WithFields(log.Fields{
				"fetched":                stats.fetched,
				"enqueued":               stats.enqueued,
				"duplicates_skipped":     stats.duplicates,
				"scan_errors":            stats.scanErrors,
				"query_errors":           stats.queryErrors,
				"duration_ms":            time.Since(cycleStart).Milliseconds(),
				"sleep_interval_seconds": config.MFLIntegratorConf.Server.RequestProcessInterval,
			}).Info("Request producer cycle finished")
		}

		// Sleep, but wake early if ctx is canceled.
		sleep := time.NewTimer(sleepInterval)
		select {
		case <-ctx.Done():
			sleep.Stop()
			log.WithError(ctx.Err()).Info("Producer stopping due to context cancellation")
			return
		case <-sleep.C:
		}
	}
}

// Consume is the consumer go routine
func Consume(ctx context.Context, db *sqlx.DB, worker int, jobs <-chan int, wg *sync.WaitGroup, mutex *sync.RWMutex, seenMap map[models.RequestID]bool) {
	defer wg.Done()
	log.WithField("worker", worker).Info("Request consumer started")

	for req := range jobs {
		start := time.Now()
		select {
		case <-ctx.Done():
			log.WithError(ctx.Err()).WithField("worker", worker).Info("Consumer stopping due to context cancellation")
			return
		default:
		}

		reqObj := RequestObj{}
		tx, err := db.Beginx()
		if err != nil {
			log.WithError(err).WithField("worker", worker).Error("Failed to begin request transaction")
			continue
		}
		err = tx.QueryRowx(`
                SELECT
                        id, depends_on,source, destination, cc_servers, cc_servers_status, body, retries, in_submission_period(destination),
                        ctype, object_type, body_is_query_param, submissionid, url_suffix,suspended,
                        statuscode, status, errors
                        
                FROM requests
                WHERE id = $1 FOR UPDATE NOWAIT`, req).StructScan(&reqObj)
		if err != nil {
			log.WithError(err).Error("Error reading request for processing")
			_ = tx.Rollback()
			continue
		}
		log.WithFields(log.Fields{
			"worker":     worker,
			"request_id": req,
		}).Info("Handling request")
		/* Work on the request */
		// dest = utils.GetServer(reqObj.Destination)
		// log.WithFields(log.Fields{"servers": models.ServerMap}).Info("Servers")
		if reqDestination, ok := models.GetServerFromRegistryByID(fmt.Sprintf("%d", reqObj.Destination)); ok {
			if config.MFLIntegratorConf.Server.FakeSyncToBaseDHIS2 {
				reqObj.withStatus(models.RequestStatusCompleted).updateRequestStatus(tx)
				reqObj.StatusCode = "FAKED"
				reqObj.updateRequest(tx)
			} else {
				_ = ProcessRequest(ctx, tx, reqObj, reqDestination, false, false)
			}

			lo.Map(reqObj.CCServers, func(item int32, index int) error {
				if ccServer, ok := models.GetServerFromRegistryByID(fmt.Sprintf("%d", item)); ok {
					log.WithFields(log.Fields{"request_id": reqObj.ID, "cc_server_id": item, "cc_server_index": index}).Debug("Processing CC server request")
					return ProcessRequest(ctx, tx, reqObj, ccServer, true, false)
				} else {
					log.WithFields(log.Fields{"request_id": reqObj.ID, "server_id": item}).Warn("CC server not found in registry")
				}
				return nil
			})
		} else {
			// Using Go lodash to process
			lo.Map(reqObj.CCServers, func(item int32, index int) error {
				log.WithFields(log.Fields{"request_id": reqObj.ID, "cc_server_id": item, "cc_server_index": index}).Debug("Processing CC-only request")
				if ccServer, ok := models.GetServerFromRegistryByID(fmt.Sprintf("%d", item)); ok {
					return ProcessRequest(ctx, tx, reqObj, ccServer, true, false)
				} else {
					log.WithFields(log.Fields{"request_id": reqObj.ID, "server_id": item}).Warn("CC server not found in registry")

				}
				return nil
			})
		}

		err = tx.Commit()
		if err != nil {
			log.WithError(err).Error("Failed to Commit transaction after processing!")
		}
		mutex.Lock()
		delete(seenMap, models.RequestID(req))
		log.WithFields(log.Fields{
			"request_id":    req,
			"worker":        worker,
			"seenMapLength": len(seenMap),
			"duration_ms":   time.Since(start).Milliseconds(),
		}).Info("Finished handling request")
		mutex.Unlock()
		// delete(RequestsMap, fmt.Sprintf("Req-%d", req))
		time.Sleep(1 * time.Second)
	}

}

// ProcessRequest handles a ready request
func ProcessRequest(ctx context.Context, tx *sqlx.Tx, reqObj RequestObj, destination models.Server, serverInCC, skipCheck bool) error {
	if skipCheck || reqObj.canSendRequest(tx, destination, serverInCC) {
		start := time.Now()
		log.WithFields(requestLogFields(reqObj, destination, serverInCC)).Info("Processing outbound request")
		// send request
		resp, err := reqObj.sendRequest(ctx, destination)
		if err != nil {
			log.WithError(err).WithFields(requestLogFields(reqObj, destination, serverInCC)).Error(
				"Failed to send outbound request")
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
				if serverInCC {
					serverStatus := getCCServerStatus(reqObj.CCServersStatus, int64(destination.ID()))
					serverStatus.Errors = "Failed to decode import summary"
					serverStatus.Status = models.RequestStatusFailed
					serverStatus.StatusCode = "ERROR03"
					serverStatus.Retries += 1
					reqObj.CCServersStatus = setCCServerStatus(reqObj.CCServersStatus, int64(destination.ID()), serverStatus)
					reqObj.updateCCServerStatus(tx)
					// _, _ = tx.NamedExec(`UPDATE requests SET cc_servers_status = :cc_servers_status WHERE id = :id`, reqObj)
				} else {
					reqObj.Status = models.RequestStatusFailed
					reqObj.StatusCode = "ERROR03"
					reqObj.Errors = "Failed to decode import summary"
					reqObj.Retries += 1
					reqObj.updateRequest(tx)
					log.WithError(err).WithFields(requestLogFields(reqObj, destination, serverInCC)).WithField("response", string(respBody)).Error("Failed to decode import summary")
					return err
				}
			}
			if resp.StatusCode/100 == 2 {

				if serverInCC {
					serverStatus := getCCServerStatus(reqObj.CCServersStatus, int64(destination.ID()))
					serverStatus.Errors = fmt.Sprintf("Created: %d, Updated: %d", result.Response.Stats.Created, result.Response.Stats.Updated)
					serverStatus.Status = models.RequestStatusCompleted
					serverStatus.StatusCode = fmt.Sprintf("%d", resp.StatusCode)
					reqObj.CCServersStatus = setCCServerStatus(reqObj.CCServersStatus, int64(destination.ID()), serverStatus)
					// reqObj.updateCCServerStatus(tx)
					_, _ = tx.NamedExec(`UPDATE requests SET cc_servers_status = :cc_servers_status WHERE id = :id`, reqObj)

				} else {
					summary := fmt.Sprintf("Created: %d, Updated: %d", result.Response.Stats.Created, result.Response.Stats.Updated)
					reqObj.StatusCode = fmt.Sprintf("%d", resp.StatusCode)
					reqObj.Errors = summary
					reqObj.Retries += 1
					reqObj.Status = models.RequestStatusCompleted
					reqObj.updateRequest(tx)
					reqObj.withStatus(models.RequestStatusCompleted).updateRequestStatus(tx)
				}
				log.WithFields(log.Fields{
					"status":      result.Response.Status,
					"created":     result.Response.Stats.Created,
					"updated":     result.Response.Stats.Updated,
					"total":       result.Response.Stats.Total,
					"status_code": resp.StatusCode,
					"duration_ms": time.Since(start).Milliseconds(),
				}).WithFields(requestLogFields(reqObj, destination, serverInCC)).Info("Outbound request completed")
				// reqObj.CCServersStatus.Scan()
				return nil
			} else {
				log.WithFields(log.Fields{
					"response_status": resp.StatusCode,
					"duration_ms":     time.Since(start).Milliseconds(),
				}).WithFields(requestLogFields(reqObj, destination, serverInCC)).Warn("Outbound request returned non-success status")
				if serverInCC {
					serverStatus := getCCServerStatus(reqObj.CCServersStatus, int64(destination.ID()))
					serverStatus.Status = models.RequestStatusFailed
					serverStatus.StatusCode = fmt.Sprintf("%d", resp.StatusCode)
					serverStatus.Retries += 1
					serverStatus.Errors = "server possibly unreachable"
					reqObj.CCServersStatus = setCCServerStatus(reqObj.CCServersStatus, int64(destination.ID()), serverStatus)
					_, _ = tx.NamedExec(`UPDATE requests SET cc_servers_status = :cc_servers_status WHERE id = :id`, reqObj)

					// reqObj.updateCCServerStatus(tx)
				} else {
					reqObj.StatusCode = fmt.Sprintf("%d", resp.StatusCode)
					reqObj.Status = models.RequestStatusFailed
					reqObj.Errors = "request might have conflicts"
					reqObj.Retries += 1
					reqObj.Response = string(respBody)
					reqObj.updateRequest(tx)
					// reqObj.withStatus(models.RequestStatusFailed).updateRequestStatus(tx)
				}
			}
		} else {
			// var result map[string]interface{}
			// json.NewDecoder(resp.Body).Decode(&result)
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				reqObj.withStatus(models.RequestStatusFailed).updateRequestStatus(tx)
				log.WithError(err).WithFields(requestLogFields(reqObj, destination, serverInCC)).Error("Could not read async response")
				return err
			}
			log.WithFields(requestLogFields(reqObj, destination, serverInCC)).WithField("response_bytes", len(bodyBytes)).Debug("Received async response payload")
			if resp.StatusCode/100 == 2 {
				v, _, _, err := jsonparser.Get(bodyBytes, "status")
				if err != nil {
					log.WithError(err).WithFields(requestLogFields(reqObj, destination, serverInCC)).Error("No status field found in async response payload")
				}
				fmt.Println(v)
			}

		}
		err = resp.Body.Close()
		if err != nil {
			log.WithError(err).WithFields(requestLogFields(reqObj, destination, serverInCC)).Error("Failed to close response body")
		}
	} else {
		log.WithFields(log.Fields{
			"request_id": reqObj.ID,
			"skipCheck":  skipCheck,
		}).Info("Request not eligible for processing")
	}
	return nil
}

// StartConsumers starts the consumer go routines
func StartConsumers(ctx context.Context, jobs <-chan int, wg *sync.WaitGroup, mutex *sync.RWMutex, seedMap map[models.RequestID]bool) {
	defer wg.Done()

	// Internal channel we can close to stop workers gracefully.
	workerJobs := make(chan int)
	dbConn := db.GetDB()

	// We wait for all Consume goroutines here, so StartConsumers doesn't return immediately.
	var workersWG sync.WaitGroup

	// Start the consumer workers.
	for i := 1; i <= config.MFLIntegratorConf.Server.MaxConcurrent; i++ {
		workersWG.Add(1)
		wg.Add(1)
		go func(workerID int) {
			defer workersWG.Done()

			// Consume exits when workerJobs is closed.
			Consume(ctx, dbConn, workerID, workerJobs, wg, mutex, seedMap)
		}(i)
	}
	log.WithFields(log.Fields{"MaxConsumers": config.MFLIntegratorConf.Server.MaxConcurrent}).Info("Created Consumers: ")

	// Forward jobs to workerJobs until cancellation.
	// When ctx is canceled, close workerJobs so workers exit their `for req := range jobs` loop.
	forwardDone := make(chan struct{})
	go func() {
		defer close(forwardDone)
		defer func() {
			// Safe close: this goroutine is the only closer.
			close(workerJobs)
		}()

		for {
			select {
			case <-ctx.Done():
				log.Info("Cancel signal received. Stopping consumers...")
				return
			case req, ok := <-jobs:
				if !ok {
					// Upstream channel closed -> stop workers too.
					return
				}
				select {
				case <-ctx.Done():
					log.Info("Cancel signal received. Stopping consumers...")
					return
				case workerJobs <- req:
				}
			}
		}
	}()

	// Wait until the forwarder has closed workerJobs, then wait for workers to finish.
	<-forwardDone
	workersWG.Wait()

	log.Info("All consumers stopped gracefully")

}

const incompleteRequestsSQL = `
	SELECT id, destination, status, retries, failed_cc_servers(cc_servers, cc_servers_status) AS cc_servers, body,
	       url_suffix, cc_servers_status, object_type, ctype, body_is_query_param
	FROM requests 
	WHERE 
	    ((status IN ('completed', 'failed') AND failed_cc_servers(cc_servers, cc_servers_status) <> '{}')  
	   	OR status = 'failed') AND suspended = 0 AND status <> 'expired' ORDER by depends_on desc;
`

// RetryIncompleteRequests is intended to occasionally retry incomplete requests - there could be a success chance
// this could be scheduled to run every so often
func RetryIncompleteRequests(ctx context.Context) {
	start := time.Now()
	stats := retryRunStats{}
	log.Info("Starting incomplete request retry run")
	dbConn := db.GetDB()
	rows, err := dbConn.Queryx(incompleteRequestsSQL)
	if err != nil {
		stats.queryErrors++
		log.WithError(err).Error("Failed to read incomplete requests")
		return
	}
	defer func() {
		log.WithFields(log.Fields{
			"scanned":             stats.scanned,
			"destination_retried": stats.destinationRetried,
			"cc_retried":          stats.ccRetried,
			"expired":             stats.expired,
			"scan_errors":         stats.scanErrors,
			"tx_errors":           stats.txErrors,
			"missing_servers":     stats.missingServers,
			"commit_errors":       stats.commitErrors,
			"query_errors":        stats.queryErrors,
			"canceled":            stats.canceled,
			"duration_ms":         time.Since(start).Milliseconds(),
		}).Info("Finished incomplete request retry run")
	}()

	for rows.Next() {
		select {
		case <-ctx.Done():
			_ = rows.Close()
			stats.canceled = true
			log.WithError(ctx.Err()).Info("Stopping incomplete request retry due to context cancellation")
			return
		default:
		}

		reqObj := RequestObj{}
		stats.scanned++
		err := rows.StructScan(&reqObj)
		if err != nil {
			stats.scanErrors++
			log.WithError(err).Error("Error reading incomplete request for processing")
		}
		log.WithFields(log.Fields{
			"request_id": reqObj.ID,
		}).Info("Handling incomplete request")
		tx, err := dbConn.Beginx()
		if err != nil {
			stats.txErrors++
			log.WithError(err).WithField("requestID", reqObj.ID).Error("Failed to begin retry transaction")
			continue
		}

		if reqObj.Status == "failed" { // destination server request had failed
			if reqDestination, ok := models.GetServerFromRegistryByID(fmt.Sprintf("%d", reqObj.Destination)); ok {
				if reqObj.Retries <= config.MFLIntegratorConf.Server.MaxRetries {
					if config.MFLIntegratorConf.Server.FakeSyncToBaseDHIS2 {
						reqObj.StatusCode = "FAKED"
						reqObj.withStatus(models.RequestStatusCompleted).updateRequestStatus(tx)
						reqObj.updateRequest(tx)

					} else {
						stats.destinationRetried++
						_ = ProcessRequest(ctx, tx, reqObj, reqDestination, false, true)
					}
				} else {
					stats.expired++
					reqObj.withStatus(models.RequestStatusExpired).updateRequestStatus(tx)
				}

				lo.Map(reqObj.CCServers, func(item int32, index int) error {
					if ccServer, ok := models.GetServerFromRegistryByID(fmt.Sprintf("%d", item)); ok {
						stats.ccRetried++
						log.WithFields(log.Fields{"request_id": reqObj.ID, "cc_server_id": item, "cc_server_index": index}).Info("Retrying incomplete request for CC server")
						return ProcessRequest(ctx, tx, reqObj, ccServer, true, true)
					} else {
						stats.missingServers++
						log.WithField("server_id", item).Warn("Incomplete request retry: server not found in registry")
					}
					return nil
				})
			}
		} else {
			lo.Map(reqObj.CCServers, func(item int32, index int) error {
				if ccServer, ok := models.GetServerFromRegistryByID(fmt.Sprintf("%d", item)); ok {

					// get cc server's status
					ccServerStatus := getCCServerStatus(reqObj.CCServersStatus, int64(item))
					retries := ccServerStatus.Retries

					// only retry if max retries is not exceeded else expire request
					if retries <= config.MFLIntegratorConf.Server.MaxRetries {
						stats.ccRetried++
						log.WithFields(
							log.Fields{
								"cc_server_id":    item,
								"cc_server_index": index,
								"retries":         retries,
								"request_id":      reqObj.ID,
							}).Info("Retrying incomplete request for CC server")
						return ProcessRequest(ctx, tx, reqObj, ccServer, true, true)
					} else {
						stats.expired++
						log.WithFields(
							log.Fields{
								"cc_server_id":    item,
								"cc_server_index": index,
								"retries":         retries,
								"request_id":      reqObj.ID,
							}).Info("Skipping incomplete request retry because max retries was exceeded")
						reqObj.withStatus(models.RequestStatusExpired).updateRequestStatus(tx)
						return nil
					}
				} else {
					stats.missingServers++
					log.WithField("server_id", item).Warn("Incomplete request retry: server not found in registry")
				}
				return nil
			})
		}

		err = tx.Commit()
		if err != nil {
			stats.commitErrors++
			log.WithError(err).Error("Failed to Commit transaction after processing incomplete request!")
		}
	}
	_ = rows.Close()
}

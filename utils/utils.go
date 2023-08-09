package utils

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/db"
	jsonpatch "github.com/evanphx/json-patch"
	log "github.com/sirupsen/logrus"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func PatchJSONObject(originalJSON, patchJSON []byte) []byte {
	patch, err := jsonpatch.DecodePatch(patchJSON)
	if err != nil {
		return originalJSON
	}

	modified, err := patch.Apply(originalJSON)
	if err != nil {
		return originalJSON
	}
	return modified

}

// GetDefaultEnv Returns default value passed if env variable not defined
func GetDefaultEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func GetDHIS2BaseURL(url string) (string, error) {
	if strings.Contains(url, "/api/") {
		pos := strings.Index(url, "/api/")
		return url[:pos], nil
	}
	return url, errors.New("URL doesn't contain /api/ part")
}

const alphabet = `abcdefghijklmnopqrstuvwxyz`
const allowedCharacters = alphabet + "ABCDEFGHIJKLMOPQRSTUVWXYZ" + "0123456789"
const codeSize = 11

// GetServer returns the ID of a server/app
func GetServer(serverName string) int {
	var i int
	err := db.GetDB().QueryRowx(
		"SELECT id FROM servers WHERE name = $1", serverName).Scan(&i)
	if err != nil {
		fmt.Printf("Error:: getting server: [%v] %v", err, serverName)
		return 0
	}
	return i
}

type DHIS2UID interface {
	ValidateUID() bool
}

// GetUID return a Unique ID for our resources
func GetUID() string {
	rand.Seed(time.Now().UnixNano())
	numberOfCodePoinst := len(allowedCharacters)

	s := ""
	s += fmt.Sprintf("%s", strings.ToUpper(string(alphabet[rand.Intn(25)])))

	for i := 1; i < codeSize; i++ {
		s += fmt.Sprintf("%s", string(allowedCharacters[rand.Intn(numberOfCodePoinst-1)]))
	}

	return s
}

func GetWithToken(
	baseUrl string, authToken string) ([]byte, error) {

	req, err := http.NewRequest("GET", baseUrl, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add token authentication
	token := "ApiToken " + authToken
	req.Header.Set("Authorization", token)

	// Create custom transport with TLS settings
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil, err
	}
	_ = resp.Body.Close()
	return body, nil
}

func PostWithToken(
	baseUrl string, data interface{}, authToken string) ([]byte, error) {

	requestBody, err := json.Marshal(data)
	if err != nil {

		log.WithError(err).Info("XXXXX ERROR")
		return nil, err
	}
	req, err := http.NewRequest("POST", baseUrl, bytes.NewReader(requestBody))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add token authentication
	token := "ApiToken " + authToken
	req.Header.Set("Authorization", token)

	// Create custom transport with TLS settings
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil, err
	}
	_ = resp.Body.Close()
	return body, nil
}

func GetWithBasicAuth(
	baseUrl string, username, password string) ([]byte, error) {

	req, err := http.NewRequest("GET", baseUrl, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add basic authentication
	auth := username + ":" + password
	basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
	req.Header.Set("Authorization", basicAuth)

	// Create custom transport with TLS settings
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil, err
	}
	_ = resp.Body.Close()
	return body, nil
}

func PostWithBasicAuth(
	baseUrl string, data interface{}, username, password string) ([]byte, error) {

	requestBody, err := json.Marshal(data)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"Data": fmt.Sprintf("%v", data)}).Info("XXXXX ERROR")
		return nil, err
	}
	req, err := http.NewRequest("POST", baseUrl, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add basic authentication
	auth := username + ":" + password
	basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
	req.Header.Set("Authorization", basicAuth)

	// Create custom transport with TLS settings
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil, err
	}
	_ = resp.Body.Close()
	return body, nil
}

func GetRequest(
	baseUrl string) ([]byte, error) {

	req, err := http.NewRequest("GET", baseUrl, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	// Add basic authentication
	auth := config.MFLIntegratorConf.API.MFLUser + ":" + config.MFLIntegratorConf.API.MFLPassword
	basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
	req.Header.Set("Authorization", basicAuth)

	// Create custom transport with TLS settings
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			// Set any necessary TLS settings here
			// For example, to disable certificate validation:
			InsecureSkipVerify: true,
		},
	}

	client := &http.Client{
		Transport: tr,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil, err
	}

	_ = resp.Body.Close()
	return body, nil
}

// SliceContains checks if a string is present in a slice
func SliceContains(s []string, str string) bool {

	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

// GetFieldsAndRelationships returns the slice of fields in existingFields & the relationships as in passedFields
func GetFieldsAndRelationships(existingFields []string, passedFields string) ([]string, map[string][]string) {

	var filtered []string
	relationships := make(map[string][]string)
	fields := strings.Split(passedFields, "[")

	var prevLast string

	for idx, x := range fields {

		y := strings.Split(x, ",")

		if len(y) > 1 {
			relationships[y[len(y)-1]] = []string{}
		} else {
			if SliceContains(existingFields, y[0]) {
				filtered = append(filtered, y[0])
			}
			return filtered, relationships
		}

		rest := y[:len(y)-1] // ignore last element
		if idx == 0 {
			for _, v := range rest {
				if SliceContains(existingFields, v) {
					filtered = append(filtered, v)
				}
			}
		} else {
			restCombined := strings.Join(rest, ",")
			zz := strings.Split(restCombined, "]")
			if len(zz) > 1 {
				for _, m := range zz[1:] {
					for _, f := range strings.Split(m, ",") {
						if len(f) > 1 && SliceContains(existingFields, f) {
							filtered = append(filtered, f)
						}
					}
				}
			}
		}
		// fmt.Printf("<<<<<<<%#v>>>>>>>>>\n", rest)

		prevLast = y[len(y)-1]
		// fmt.Printf("First: %v  Last: %v  F: %v. Prev:%v\n", rest, last, filtered, prevLast)

		// LOOK AHEAD for fields that were enclosed in []
		if len(fields) > idx+1 {
			nextFields := fields[idx+1]

			m := strings.Split(nextFields, "]")

			rest := m[:len(m)-1]

			// fmt.Printf(">>>>>>>>> %v:%v\n", prevLast, rest)
			var appendFields []string
			if len(rest) > 0 {
				for _, p := range strings.Split(rest[0], ",") {
					if len(p) > 0 {
						appendFields = append(appendFields, p)
					}
				}
			}
			relationships[prevLast] = appendFields
		}

	}

	for k, v := range relationships {

		if len(v) <= 0 && SliceContains(existingFields, k) {
			filtered = append(filtered, k)
			delete(relationships, k)
		}
		if v == nil {
			delete(relationships, k)
		}
	}
	// fmt.Printf("%#v==> %#v, %v\n", relationships, filtered, len(relationships["z"]))
	return filtered, relationships
}

// addExtraParams adds some more params to URL
func addExtraParams(baseURL string, extraParams url.Values) (string, error) {
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("error parsing URL: %v", err)
	}

	queryParams := parsedURL.Query()
	for key, values := range extraParams {
		queryParams[key] = append(queryParams[key], values...)
	}

	parsedURL.RawQuery = queryParams.Encode()
	return parsedURL.String(), nil
}

// Paginator is the structure representing the paginator object
type Paginator struct {
	PageCount    int64
	PageSize     int64 // the limit
	Total        int64
	CurrentPage  int64
	NextPage     int64
	PreviousPage int64
	Offset       int64
	PageExists   bool
	Paging       bool
}

// HasNext returns true if there is a next page
func (p *Paginator) HasNext() bool {
	if p.HasPage(p.CurrentPage + 1) {
		return true
	}
	return false
}

// HasPrev returns true if there is a previous page
func (p *Paginator) HasPrev() bool {
	if p.HasPage(p.CurrentPage-1) && (p.CurrentPage-1) != 0 {
		return true
	}
	return false
}

// Pages returns the number of pages
func (p *Paginator) Pages() int64 {
	return int64(math.Ceil(
		(float64(p.Total) / float64(p.PageSize))))
}

// HasPages returns true if there are pages
func (p *Paginator) HasPages() bool {
	if p.Total < 1 {
		return false
	}
	return true
}

// HasPage returns true if @param page is available
func (p *Paginator) HasPage(page int64) bool {
	if p.HasPages() && page <= p.Pages() {
		return true
	}
	return false
}

// FirstItem returns the number for first item in the page - used as OFFSET
func (p *Paginator) FirstItem() int64 {
	if p.PageCount < p.CurrentPage {
		return p.Total + 1
	}
	return int64(math.Min(float64((p.CurrentPage-1)*p.PageSize+1), float64(p.Total)))
}

// LastItem returns the number for the last item in the page
func (p *Paginator) LastItem() int64 {
	return int64(math.Min(float64(p.FirstItem()+p.PageSize-1), float64(p.Total)))
}

// GetPaginator returns the a pointer to the Paginator structure
func GetPaginator(totalRecords int64, pageSize string, page string, paging bool) Paginator {
	p := Paginator{}
	p.Paging = true
	p.Total = totalRecords

	ps, err := strconv.ParseInt(pageSize, 10, 64)
	if err != nil {
		log.WithError(err).Info("Failed to convert pageSize to integer. Defaulting to 50")
		p.PageSize = 50
	} else {
		p.PageSize = ps
	}

	pc, err := strconv.ParseInt(page, 10, 64)
	if err != nil {
		log.WithError(err).Info("Failed to convert page to integer. Defaulting to 1")
		p.CurrentPage = 1
	} else {
		p.CurrentPage = pc
	}
	p.PageCount = p.Pages()
	p.Offset = p.FirstItem() - 1
	if p.HasPrev() {
		p.PreviousPage = p.CurrentPage - 1
	}
	if p.HasNext() {
		p.NextPage = p.CurrentPage + 1
	}
	p.PageExists = p.HasPage(pc)
	return p
}

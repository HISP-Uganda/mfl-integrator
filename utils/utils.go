package utils

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/db"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// GetDefaultEnv Returns default value passed if env variable not defined
func GetDefaultEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
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
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil, err
	}
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
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil, err
	}
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

package utils

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/db"
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

func GetRequest(
	baseUrl string,
	username string, password string) (*http.Response, error) {

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

	return resp, nil
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

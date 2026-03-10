package clients

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"strings"
)

func GetDHIS2BaseURL(url string) (string, error) {
	if strings.Contains(url, "/api/") {
		pos := strings.Index(url, "/api/")
		return url[:pos], nil
	}
	return url, errors.New("URL doesn't contain /api/ part")
}

func (s *Dhis2Server) NewDhis2Client() (*Client, error) {
	baseUrl, err := GetDHIS2BaseURL(s.BaseUrl)
	if err != nil {
		log.WithFields(log.Fields{
			"URL": s.BaseUrl, "Error": err}).Error("Failed to get base URL from URL")
		return nil, err
	}
	client := NewStandardRestyClient(baseUrl + "/api")
	switch s.AuthMethod {
	case "Basic":
		client.SetBasicAuth(s.Username, s.Password)
	case "Token":
		client.SetAuthScheme("Token")
		client.SetAuthToken(s.AuthToken)
	}
	return &Client{
		RestClient: client,
		BaseURL:    baseUrl + "/api",
	}, nil
}

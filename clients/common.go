package clients

import (
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

type Client struct {
	RestClient *resty.Client
	BaseURL    string
}

type Dhis2Server struct {
	BaseUrl    string `json:"base_url"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	AuthToken  string `json:"auth_token"`
	AuthMethod string `json:"auth_method"`
}

func (c *Client) GetResource(resourcePath string, params map[string]string) (*resty.Response, error) {
	request := c.RestClient.R()

	if params != nil {
		request.SetQueryParams(params)
	}

	resp, err := request.Get(resourcePath)
	if err != nil {
		log.WithError(err).Infof("Error when calling `GetResource`: %v", err)
	}
	return resp, err
}
func (c *Client) PostResource(resourcePath string, data interface{}) (*resty.Response, error) {
	resp, err := c.RestClient.R().
		SetHeader("Content-Type", "application/json").
		SetBody(data).
		Post(resourcePath)
	if err != nil {
		log.Errorf("Error when calling `PostResource: %v`", err)
	}
	return resp, err
}

func (c *Client) PutResource(resourcePath string, data interface{}) (*resty.Response, error) {
	resp, err := c.RestClient.R().
		SetHeader("Content-Type", "application/json").
		SetBody(data).
		Put(resourcePath)
	if err != nil {
		log.Errorf("Error when calling `PutResource`: %v", err)
	}
	return resp, err
}

func (c *Client) DeleteResource(resourcePath string) (*resty.Response, error) {
	resp, err := c.RestClient.R().
		Delete(resourcePath)
	if err != nil {
		log.Errorf("Error when calling `DeleteResource`: %v", err)
	}
	return resp, err
}

func (c *Client) PatchResource(resourcePath string, data interface{}) (*resty.Response, error) {
	resp, err := c.RestClient.R().
		// SetHeader("Content-Type", "application/json").
		SetBody(data).
		Patch(resourcePath)
	if err != nil {
		log.Errorf("Error when calling `PatchResource`: %v", err)
	}
	return resp, err
}

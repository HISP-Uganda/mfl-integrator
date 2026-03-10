package clients

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

const (
	DefaultTimeout      = 60 * time.Second
	DefaultRetryCount   = 2
	DefaultRetryWait    = 500 * time.Millisecond
	DefaultRetryMaxWait = 3 * time.Second
	DefaultUserAgent    = "MFL-Integrator"
)

type Client struct {
	RestClient *resty.Client
	BaseURL    string
}

type AuthConfig struct {
	Username    string
	Password    string
	Token       string
	TokenScheme string
}

type Dhis2Server struct {
	BaseUrl    string `json:"base_url"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	AuthToken  string `json:"auth_token"`
	AuthMethod string `json:"auth_method"`
}

func NewStandardTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	return transport
}

func NewStandardHTTPClient() *http.Client {
	return &http.Client{
		Transport: NewStandardTransport(),
		Timeout:   DefaultTimeout,
	}
}

func NewStandardRestyClient(baseURL string) *resty.Client {
	return resty.New().
		SetBaseURL(strings.TrimSpace(baseURL)).
		SetHeader("User-Agent", DefaultUserAgent).
		SetHeader("Accept", "application/json").
		SetDisableWarn(true).
		SetTransport(NewStandardTransport()).
		SetTimeout(DefaultTimeout).
		SetRetryCount(DefaultRetryCount).
		SetRetryWaitTime(DefaultRetryWait).
		SetRetryMaxWaitTime(DefaultRetryMaxWait).
		AddRetryCondition(func(resp *resty.Response, err error) bool {
			if err != nil {
				return true
			}
			code := resp.StatusCode()
			return code == http.StatusTooManyRequests || code >= http.StatusInternalServerError
		})
}

func ApplyAuth(req *http.Request, auth AuthConfig) {
	switch {
	case strings.TrimSpace(auth.Username) != "":
		req.SetBasicAuth(auth.Username, auth.Password)
	case strings.TrimSpace(auth.Token) != "":
		scheme := strings.TrimSpace(auth.TokenScheme)
		if scheme == "" {
			scheme = "Bearer"
		}
		req.Header.Set("Authorization", scheme+" "+auth.Token)
	}
}

func DoRequest(ctx context.Context, method, requestURL, contentType string, body io.Reader, auth AuthConfig) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(contentType) != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("User-Agent", DefaultUserAgent)
	ApplyAuth(req, auth)
	return NewStandardHTTPClient().Do(req)
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

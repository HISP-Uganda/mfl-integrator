package clients

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

const (
	DefaultPage     = 1
	DefaultPageSize = 50
	MaxPageSize     = 200
)

type Facility struct {
	ID                  int64      `json:"id,omitempty"`
	Identifier          string     `json:"identifier"`
	MFLUID              string     `json:"mfl_uid,omitempty"`
	Name                string     `json:"name"`
	ShortName           string     `json:"shortName,omitempty"`
	HistoricalID        string     `json:"historical_id,omitempty"`
	Status              *string    `json:"status,omitempty"`
	Reporting           bool       `json:"reporting,omitempty"`
	Licensed            bool       `json:"licensed,omitempty"`
	Address             *string    `json:"address,omitempty"`
	ContactPersonEmail  *string    `json:"contact_personemail,omitempty"`
	ContactPersonMobile *string    `json:"contact_personmobile,omitempty"`
	ContactPersonName   *string    `json:"contact_personname,omitempty"`
	ContactPersonTitle  *string    `json:"contact_persontitle,omitempty"`
	Longitude           *float64   `json:"longitude,omitempty"`
	Latitude            *float64   `json:"latitude,omitempty"`
	OpeningDate         *time.Time `json:"opening_date,omitempty"`
	ClosingDate         *time.Time `json:"closing_date,omitempty"`
	BedCapacity         *int       `json:"bed_capacity,omitempty"`
	Services            []string   `json:"services,omitempty"`
	CreatedAt           time.Time  `json:"created,omitempty"`
	UpdatedAt           time.Time  `json:"updatedAt,omitempty"`
	Parent              *RefEntity `json:"parent,omitempty"`
	Level               *int       `json:"level,omitempty"`
	National            *RefEntity `json:"national,omitempty"`
	Region              *RefEntity `json:"region,omitempty"`
	District            *RefEntity `json:"district,omitempty"`
	DLGMunicipality     *RefEntity `json:"dlg_municipality,omitempty"`
	Subcounty           *RefEntity `json:"subcounty,omitempty"`
	FacilityLevel       *RefEntity `json:"facility_level"`
	Ownership           *RefEntity `json:"ownership,omitempty"`
	Authority           *RefEntity `json:"authority,omitempty"`
	LastUpdated         time.Time  `json:"lastUpdated,omitempty"`
}

type RefEntity struct {
	ID     int64  `json:"id,omitempty"`
	MFLUID string `json:"mfl_uid,omitempty"`
	Name   string `json:"name,omitempty"`
}

type Pager struct {
	Page      int   `json:"page"`
	PageSize  int   `json:"pageSize"`
	PageCount int   `json:"pageCount"`
	Total     int64 `json:"total"`
}

type PageResponse[T any] struct {
	Pager    Pager `json:"pager"`
	OrgUnits []T   `json:"orgunits"`
}

// Client is a Resty-based client for the Facilities API.
//type Client struct {
//	baseURL string
//	resty   *resty.Client
//}

// Option configures the client.
type Option func(*Client)

// NewMFLApi creates a new Facilities API client.
//
// baseURL example: "https://example.org/api" (no trailing slash preferred)
func NewMFLApi(baseURL string, opts ...Option) *Client {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")

	c := &Client{
		BaseURL:    base,
		RestClient: NewStandardRestyClient(base),
	}

	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}

	// Ensure BaseURL is consistent if someone injected a resty client.
	c.RestClient.SetBaseURL(c.BaseURL)

	return c
}

// WithResty allows injecting a pre-configured Resty client (proxy, transport, hooks, etc.).
// Note: we will still override its BaseURL with the client's baseURL.
func WithResty(r *resty.Client) Option {
	return func(c *Client) {
		if r != nil {
			c.RestClient = r
		}
	}
}

// WithBasicAuth sets Authorization: Basic <base64(user:pass)> for all requests.
func WithBasicAuth(username, password string) Option {
	return func(c *Client) {
		if strings.TrimSpace(username) != "" {
			c.RestClient.SetBasicAuth(username, password)
		}
	}
}

// WithHeader sets a default header for all requests.
func WithHeader(k, v string) Option {
	return func(c *Client) {
		if strings.TrimSpace(k) != "" {
			c.RestClient.SetHeader(k, v)
		}
	}
}

// WithDebug enables Resty debug logging.
func WithDebug() Option {
	return func(c *Client) { c.RestClient.SetDebug(true) }
}

// WithUserAgent sets a User-Agent header.
func WithUserAgent(ua string) Option {
	return func(c *Client) {
		if strings.TrimSpace(ua) != "" {
			c.RestClient.SetHeader("User-Agent", ua)
		}
	}
}

// FacilitiesQuery models all supported query parameters for listing facilities.
type FacilitiesQuery struct {
	Page          int
	PageSize      int
	Paging        bool
	Q             string
	Name          string
	MflUID        string
	ParentID      string
	RegionID      string
	RegionName    string
	DistrictID    string
	DistrictName  string
	dlgID         string
	SubcountyID   string
	SubcountyName string
	Parish        string
	Village       string
	Level         string
	Ownership     string
	Authority     string
	Status        string // "Functional" or "Non-Functional"
	LastUpdated   string // YYYY-MM-DD
	UpdatedSince  string // alias for last_updated
	National      string
}

func (q FacilitiesQuery) normalize() (FacilitiesQuery, error) {
	out := q

	// defaults
	if out.Page <= 0 {
		out.Page = DefaultPage
	}
	if out.PageSize <= 0 {
		out.PageSize = DefaultPageSize
	}
	if out.PageSize > MaxPageSize {
		out.PageSize = MaxPageSize
	}

	// status validation (if provided)
	if s := strings.TrimSpace(out.Status); s != "" {
		if s != "Functional" && s != "Non-Functional" {
			return out, fmt.Errorf(`invalid status %q (expected "Functional" or "Non-Functional")`, s)
		}
		out.Status = s
	}

	// last_updated / updatedSince alias handling
	lu := strings.TrimSpace(out.LastUpdated)
	us := strings.TrimSpace(out.UpdatedSince)

	switch {
	case lu != "" && us != "":
		// If both provided, require them to match to avoid ambiguity.
		if lu != us {
			return out, fmt.Errorf("both last_updated (%s) and updatedSince (%s) provided but differ", lu, us)
		}
	case lu == "" && us != "":
		out.LastUpdated = us
	}

	// validate date format if present
	if strings.TrimSpace(out.LastUpdated) != "" {
		if _, err := time.Parse("2006-01-02", out.LastUpdated); err != nil {
			return out, fmt.Errorf("invalid last_updated %q (expected YYYY-MM-DD): %w", out.LastUpdated, err)
		}
	}

	return out, nil
}

// ListFacilities calls GET /facilities (adjust path if yours differs)
// and returns a paginated PageResponse[Facility].
func (c *Client) ListFacilities(
	ctx context.Context,
	query FacilitiesQuery,
) (*PageResponse[Facility], error) {

	q, err := query.normalize()
	if err != nil {
		return nil, err
	}

	params := map[string]string{
		"page":     fmt.Sprintf("%d", q.Page),
		"pageSize": fmt.Sprintf("%d", q.PageSize),
	}

	setIf := func(k, v string) {
		if s := strings.TrimSpace(v); s != "" {
			params[k] = s
		}
	}

	setIf("q", q.Q)
	setIf("name", q.Name)
	setIf("mflUid", q.MflUID)
	setIf("parentId", q.ParentID)
	setIf("regionId", q.RegionID)
	setIf("regionName", q.RegionName)
	setIf("districtName", q.DistrictName)
	setIf("districtId", q.DistrictID)
	setIf("dlgId", q.dlgID)
	setIf("subcountyId", q.SubcountyID)
	setIf("subcountyName", q.SubcountyName)
	setIf("parish", q.Parish)
	setIf("village", q.Village)
	setIf("level", q.Level)
	setIf("ownership", q.Ownership)
	setIf("authority", q.Authority)
	setIf("status", q.Status)
	setIf("last_updated", q.LastUpdated)
	setIf("national", q.National)

	var out PageResponse[Facility]

	resp, err := c.RestClient.R().
		SetContext(ctx).
		SetQueryParams(params).
		SetResult(&out).
		Get("/orgunits") // <-- change if your endpoint path differs

	log.WithFields(log.Fields{"query": q, "URL": c.RestClient.BaseURL, "statusCode": resp.StatusCode()}).Info("streaming facilities")

	if err != nil {
		return nil, fmt.Errorf("list facilities: %w", err)
	}

	if resp.IsError() {
		return nil, fmt.Errorf("list facilities: status=%d body=%s", resp.StatusCode(), string(resp.Body()))
	}

	// Normalize possible null rows into an empty slice
	if out.OrgUnits == nil {
		out.OrgUnits = []Facility{}
	}

	return &out, nil
}

// IterateAllFacilities fetches ALL matching facilities by paging through results
// until all rows are retrieved (or the context is canceled).
//
// It returns a slice containing all facilities. For large datasets, prefer StreamFacilities.
func (c *Client) IterateAllFacilities(
	ctx context.Context,
	query FacilitiesQuery,
) ([]Facility, error) {

	q, err := query.normalize()
	if err != nil {
		return nil, err
	}

	// Always start at the requested page (or default 1)
	page := q.Page
	pageSize := q.PageSize

	var all []Facility

	for {
		// respect cancellation
		if err := ctx.Err(); err != nil {
			return all, err
		}

		q.Page = page
		q.PageSize = pageSize

		resp, err := c.ListFacilities(ctx, q)
		if err != nil {
			return all, err
		}

		if len(resp.OrgUnits) > 0 {
			all = append(all, resp.OrgUnits...)
		}

		// Stop conditions
		// 1) We've already fetched all rows according to total
		if resp.Pager.Total > 0 && len(all) >= int(resp.Pager.Total) {
			break
		}
		// 2) API returned no rows (end or inconsistent total)
		if len(resp.OrgUnits) == 0 {
			break
		}
		// 3) If the API returns fewer than pageSize, we're likely at the last page
		if len(resp.OrgUnits) < pageSize {
			break
		}

		page++
	}

	return all, nil
}

// StreamFacilities streams facilities page-by-page and calls fn for each facility.
// It stops early if:
// - ctx is canceled
// - fn returns an error (that error is returned)
// - there are no more rows
//
// This is the safest option for huge datasets because it does not accumulate all rows in memory.
func (c *Client) StreamFacilities(
	ctx context.Context,
	query FacilitiesQuery,
	fn func(Facility) error,
) error {

	if fn == nil {
		return fmt.Errorf("stream facilities: fn is nil")
	}

	q, err := query.normalize()
	if err != nil {
		return err
	}

	page := q.Page
	pageSize := q.PageSize
	processed := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		q.Page = page
		q.PageSize = pageSize

		resp, err := c.ListFacilities(ctx, q)
		if err != nil {
			return err
		}

		// No rows => done
		if len(resp.OrgUnits) == 0 {

			return nil
		}

		for _, f := range resp.OrgUnits {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn(f); err != nil {
				return err
			}
			processed++
		}

		// Stop if we reached total
		if resp.Pager.Total > 0 && int64(processed) >= resp.Pager.Total {
			return nil
		}

		// If last page (short read), stop
		if len(resp.OrgUnits) < pageSize {
			return nil
		}

		page++
	}
}

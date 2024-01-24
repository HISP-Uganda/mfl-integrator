package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/utils"
	"github.com/HISP-Uganda/mfl-integrator/utils/dbutils"
	"github.com/buger/jsonparser"
	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

func init() {
	var migrationsDir string
	currentOS := runtime.GOOS
	switch currentOS {
	case "windows":
		migrationsDir = "file:///C:\\ProgramData\\MFLIntegrator"
	case "darwin", "linux":
		migrationsDir = "file:///usr/share/mflintegrator/db/migrations"
	default:
		migrationsDir = "file://db/migrations"
	}
	m, err := migrate.New(
		// "file:///usr/share/mflintegrator/db/migrations", // file://db/migrations
		migrationsDir,
		config.MFLIntegratorConf.Database.URI)
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatal("Error running migration:", err)
	}

	if err != nil {
		log.Fatalln(err)
	}
	CreateBaseDHIS2Server()
	rows, err := db.GetDB().Queryx("SELECT * FROM servers")

	if err != nil {
		log.WithError(err).Info("Failed to load servers")
	}
	ServerMap = make(map[string]Server)
	ServerMapByName = make(map[string]Server)
	for rows.Next() {
		srv := Server{}

		err := rows.StructScan(&srv.s)
		if err != nil {
			log.Fatalln("Server Loading ==>", err)
		}

		ServerMap[strconv.Itoa(int(srv.s.ID))] = srv
		ServerMapByName[srv.s.Name] = srv
		// log.WithField("SERVER", srv.s).Info("=====>")

	}
	// log.WithField("ServerMapByName", ServerMapByName).Info("========>")
	_ = rows.Close()
}

// ServerMap is the List of Servers
var ServerMap map[string]Server
var ServerMapByName map[string]Server

// ServerID is the id for the server
type ServerID int64

// Server is our user object
type Server struct {
	s struct {
		ID                      ServerID            `db:"id" json:"id"`
		UID                     string              `db:"uid" json:"uid,omitempty"`
		Name                    string              `db:"name" json:"name" validate:"required"`
		Username                string              `db:"username" json:"username"`
		Password                string              `db:"password" json:"password,omitempty"`
		IsProxyServer           bool                `db:"is_proxy_server" json:"isProxyServer,omitempty"` // whether response is received as is
		SystemType              string              `db:"system_type" json:"systemType,omitempty"`        // the type of system e.g DHIS2, Other is the default
		EndPointType            string              `db:"endpoint_type" json:"endPointType,omitempty"`    // e.g /dataValueSets,
		AuthToken               string              `db:"auth_token" json:"AuthToken"`
		IPAddress               string              `db:"ipaddress" json:"IPAddress"` // Usefull for setting Trusted Proxies
		URL                     string              `db:"url" json:"URL" validate:"required,url"`
		CCURLS                  pq.StringArray      `db:"cc_urls" json:"CCURLS,omitempty"`                   // just an additional URL to receive same request
		CallbackURL             string              `db:"callback_url" json:"callbackURL,omitempty"`         // receives response on success call to url
		HTTPMethod              string              `db:"http_method" json:"HTTPMethod" validate:"required"` // the HTTP Method used when calling the url
		AuthMethod              string              `db:"auth_method" json:"AuthMethod" validate:"required"` // the Authentication Method used
		AllowCallbacks          bool                `db:"allow_callbacks" json:"allowCallbacks,omitempty"`   // Whether to allow calling sending callbacks
		AllowCopies             bool                `db:"allow_copies" json:"allowCopies,omitempty"`         // Whether to allow copying similar request to CCURLs
		UseAsync                bool                `db:"use_async" json:"useAsync,omitempty"`
		UseSSL                  bool                `db:"use_ssl" json:"useSSL,omitempty"`
		ParseResponses          bool                `db:"parse_responses" json:"parseResponses,omitempty"`
		SSLClientCertKeyFile    string              `db:"ssl_client_certkey_file" json:"sslClientCertkeyFile"`
		StartOfSubmissionPeriod int                 `db:"start_submission_period" json:"startSubmissionPeriod"`
		EndOfSubmissionPeriod   int                 `db:"end_submission_period" json:"endSubmissionPeriod"`
		XMLResponseXPATH        string              `db:"xml_response_xpath"  json:"XMLResponseXPATH"`
		JSONResponseXPATH       string              `db:"json_response_xpath" json:"JSONResponseXPATH"`
		Suspended               bool                `db:"suspended" json:"suspended,omitempty"`
		URLParams               dbutils.MapAnything `db:"url_params" json:"URLParams,omitempty"`
		Created                 time.Time           `db:"created" json:"created,omitempty"`
		Updated                 time.Time           `db:"updated" json:"updated,omitempty"`
		AllowedSources          []string            `json:"allowedSources,omitempty"`
	}
}

// ServerAllowedApps hold servers and servers they allow to communicate with
type ServerAllowedApps struct {
	ID             int64         `db:"id" json:"id"`
	ServerID       int64         `db:"server_id" json:"server_id"`
	AllowedServers pq.Int64Array `db:"allowed_sources" json:"allowed_sources"`
}

func (sa *ServerAllowedApps) Save() {
	dbConn := db.GetDB()
	_, err := dbConn.NamedExec(`INSERT INTO server_allowed_sources (server_id, allowed_sources)
			VALUES(:server_id, :allowed_sources)`, sa)
	if err != nil {
		log.WithError(err).Error("Failed to save server allowed sources")
	}
}

// ID return the id of this server
func (s *Server) ID() ServerID { return s.s.ID }

// UID returns the uid of the server/app
func (s *Server) UID() string { return s.s.UID }

// Name ...
func (s *Server) Name() string { return s.s.Name }

// Username ...
func (s *Server) Username() string { return s.s.Username }

// Password ...
func (s *Server) Password() string { return s.s.Password }

// SystemType return the type of system/app it is
func (s *Server) SystemType() string { return s.s.SystemType }

// AuthToken return the Authentication token for this server
func (s *Server) AuthToken() string { return s.s.AuthToken }

// URL returns the URL for the server
func (s *Server) URL() string { return s.s.URL }

// HTTPMethod returns the method used when calling the URL
func (s *Server) HTTPMethod() string { return s.s.HTTPMethod }

// AuthMethod ...
func (s *Server) AuthMethod() string { return s.s.AuthMethod }

// AllowCallbacks returns whether server allows callbacks
func (s *Server) AllowCallbacks() bool { return s.s.AllowCallbacks }

// UseAsync ...
func (s *Server) UseAsync() bool { return s.s.UseAsync }

// CallbackURL return the server callback url
func (s *Server) CallbackURL() string { return s.s.CallbackURL }

// ParseResponses return whether we shold parse the server's responses
func (s *Server) ParseResponses() bool { return s.s.ParseResponses }

// EndOfSubmissionPeriod returns the end of the submission period for the server
func (s *Server) EndOfSubmissionPeriod() int { return s.s.EndOfSubmissionPeriod }

// StartOfSubmissionPeriod returns the start of the submission period for the server
func (s *Server) StartOfSubmissionPeriod() int { return s.s.StartOfSubmissionPeriod }

// Suspended returns whether the server is suspended
func (s *Server) Suspended() bool { return s.s.Suspended }

// CreatedOn return time when Server/App was created
func (s *Server) CreatedOn() time.Time { return s.s.Created }

// UpdatedOn return time when server/app was updated
func (s *Server) UpdatedOn() time.Time { return s.s.Updated }

// CompleteURL returns server URL plus its URLParams
func (s *Server) CompleteURL() string {
	p := url.Values{}
	for k, v := range s.s.URLParams {
		p.Add(k, fmt.Sprintf("%v", v))
	}
	sURL := s.s.URL
	if strings.LastIndex(sURL, "?") == len(sURL)-1 {
		sURL += p.Encode()
	} else {
		sURL += "?" + p.Encode()
	}
	return sURL
}

// GetServerByID returns server object using id
//func GetServerByID(id int64) Server {
//	srv := Server{}
//	err := db.GetDB().Get(&srv.s, "SELECT * FROM servers WHERE id = $1", id)
//
//	if err != nil {
//		fmt.Printf("Error geting server: [%v]", err)
//		return Server{}
//	}
//	return srv
//
//}

// GetServerByName returns server object using id
func GetServerByName(name string) (Server, error) {
	srv := Server{}
	err := db.GetDB().Get(&srv.s, "SELECT * FROM servers WHERE name = $1", name)

	if err != nil {
		fmt.Printf("Error geting server: [%v]", err)
		return Server{}, errors.New(fmt.Sprintf("Server with name '%s' Not found!", name))
	}
	return srv, nil

}

// Self returns server map
func (s *Server) Self() map[string]any {
	srvJSON, err := json.Marshal(s.s)
	if err != nil {
		log.WithError(err).Error("Could not marshal server struct to JSON")
	}
	var srv map[string]any
	_ = json.Unmarshal(srvJSON, &srv)
	return srv
}

func (s *Server) ExistsInDB() bool {
	var count int
	err := db.GetDB().Get(&count, "SELECT count(*)  FROM servers WHERE name = $1", s.s.Name)
	if err != nil {
		log.WithError(err).Info("Error checking server existence:")
		return false
	}
	return count > 0
}

// GetServerIDByName returns server object using id
func GetServerIDByName(name string) int64 {
	var id int64
	err := db.GetDB().Get(&id, "SELECT id FROM servers WHERE name = $1", name)

	if err != nil {
		fmt.Printf("Error geting server: Name: %v [%v]", name, err)
		return 0
	}
	return id

}

func GetServerUIDByName(name string) string {
	var uid string
	err := db.GetDB().Get(&uid, "SELECT uid FROM servers WHERE name = $1", name)

	if err != nil {
		fmt.Printf("Error geting server: [%v]", err)
		return ""
	}
	return uid

}

func (s *Server) InSubmissionPeriod(tx *sqlx.Tx) bool {
	inSubmissionPeriod := false
	err := tx.Get(&inSubmissionPeriod, `SELECT in_submission_period($1)`, s.s.ID)
	if err != nil {
		log.WithError(err).Info("Failed to get server submission period status!")
		return false
	}
	return inSubmissionPeriod
}

// ServerDBFields returns the fields in the servers table
func (s *Server) ServerDBFields() []string {
	e := reflect.ValueOf(s).Elem()
	var ret []string
	for i := 0; i < e.NumField(); i++ {
		t := e.Type().Field(i).Tag.Get("db")
		if len(t) > 0 {
			ret = append(ret, t)
		}
	}
	ret = append(ret, "*")
	return ret
}

func (s *Server) ValidateUID() bool {
	uidPattern := `^[a-zA-Z0-9]{11}$`
	re := regexp.MustCompile(uidPattern)
	return re.MatchString(s.s.UID)
}

func (s *Server) SetUID(uid string) {
	s.s.UID = uid
}

var serversFields = new(Server).ServerDBFields()

func GetServers(db *sqlx.DB, page string, pageSize string,
	orderBy []string, fields string, filters []string) []dbutils.MapAnything {

	filtered, _ := utils.GetFieldsAndRelationships(serversFields, fields)
	serversTable := dbutils.Table{Name: "servers", Alias: "s"}

	qbuild := &dbutils.QueryBuilder{}
	qbuild.QueryTemplate = `SELECT %s FROM %s %s`
	qbuild.Table = serversTable
	var qfields []dbutils.Field
	for _, f := range filtered {
		qfields = append(qfields, dbutils.Field{Name: f, TablePrefix: "s", Alias: ""})
	}

	qbuild.Conditions = dbutils.QueryFiltersToConditions(filters, "s")
	qbuild.Fields = qfields
	qbuild.OrderBy = dbutils.OrderListToOrderBy(orderBy, serversFields, "s")

	var whereClause string
	if len(qbuild.Conditions) == 0 {
		whereClause = " TRUE"
	} else {
		whereClause = fmt.Sprintf("%s", dbutils.QueryConditions(qbuild.Conditions))
	}
	countquery := fmt.Sprintf("SELECT COUNT(*) AS count FROM servers s WHERE %s", whereClause)
	var count int64
	err := db.Get(&count, countquery)
	if err != nil {
		return nil
	}
	pager := dbutils.GetPaginator(count, pageSize, page, true)
	qbuild.Limit = pager.PageSize
	qbuild.Offset = pager.FirstItem() - 1

	jsonquery := fmt.Sprintf("SELECT ROW_TO_JSON(s) FROM (%s) s;", qbuild.ToSQL(true))
	var results []dbutils.MapAnything

	err = db.Select(&results, jsonquery)
	if err != nil {
		log.WithError(err).Error("Failed to get query results")
	}
	return results
}

const insertServerSQL = `
INSERT INTO servers(uid, name, username, password, url, ipaddress, http_method, auth_method, auth_token,
       callback_url, allow_callbacks, cc_urls, allow_copies, start_submission_period, end_submission_period,
       parse_responses, use_ssl, suspended, ssl_client_certkey_file, json_response_xpath, xml_response_xpath, endpoint_type, url_params)
       VALUES (:uid,:name,:username,:password,:url,:ipaddress,:http_method,:auth_method,:auth_token, :callback_url,:allow_callbacks, 
               :cc_urls,:allow_copies,:start_submission_period,:end_submission_period,:parse_responses,:use_ssl,
               :suspended,:ssl_client_certkey_file,:json_response_xpath,:xml_response_xpath, :endpoint_type, :url_params)
	RETURNING id
`

// NewServer creates new server and saves it in DB
func NewServer(c *gin.Context, db *sqlx.DB) (Server, error) {
	srv := &Server{}

	contentType := c.Request.Header.Get("Content-Type")
	switch contentType {
	case "application/json":
		if err := c.BindJSON(&srv.s); err != nil {
			log.WithError(err).Error("Error reading server object from POST body")
		}
		// log.WithField("New Server", s).Info("Going to create new server")
	default:
		//
		log.WithField("Content-Type", contentType).Error("Unsupported content-Type")
		return *srv, errors.New(fmt.Sprintf("Unsupported Content-Type: %s", contentType))
	}
	if !srv.ValidateUID() {
		srv.SetUID(utils.GetUID())
	}
	if srv.ExistsInDB() {
		log.WithField("Server Name", srv.s.Name).Info("Server with same name already exists!")
		srv.s.UID = GetServerUIDByName(srv.Name())
		_, err := db.NamedExec(updateServerSQL, srv.s)
		if err != nil {
			log.WithError(err).Error("Failed to update server!")
			return *srv, err
		}
		return *srv, nil
	} else {
		rows, err := db.NamedQuery(insertServerSQL, srv.s)
		if err != nil {
			log.WithError(err).Error("Failed to save server to database")
			return Server{}, err
		}
		for rows.Next() {
			var serverId int64
			_ = rows.Scan(&serverId)
			if len(srv.s.AllowedSources) > 0 {
				servers := lo.Map(srv.s.AllowedSources, func(name string, _ int) int64 {
					iSrv := ServerMapByName[name]
					return int64(iSrv.ID())
				})
				allowedSources := ServerAllowedApps{ServerID: serverId, AllowedServers: servers}
				allowedSources.Save()

			}

		}
		_ = rows.Close()
	}

	return *srv, nil
}

func CreateServerFromJSON(db *sqlx.DB, serverJSON []byte) (Server, error) {
	srv := &Server{}
	err := json.Unmarshal(serverJSON, &srv.s)
	if err != nil {
		log.WithError(err).Error("Failed to Unmarshal serverJSON to Server object!")
		return Server{}, err
	}

	if srv.ExistsInDB() {
		log.WithField("Server Name", srv.s.Name).Info("Server with same name already exists!")
		// Update server
		srv.s.UID = GetServerUIDByName(srv.Name())
		_, err := db.NamedExec(updateServerSQL, srv.s)
		if err != nil {
			log.WithError(err).Error("Failed to update server!")
			return *srv, err
		}
		log.WithField("ServerUID", srv.s.UID).Info("Updating server!")
		return GetServerByName(srv.Name())
	} else {
		// create server
		srv.SetUID(utils.GetUID())
		rows, err := db.NamedQuery(insertServerSQL, srv.s)
		if err != nil {
			log.WithError(err).Error("Failed to save server to database")
			return Server{}, err
		}
		for rows.Next() {
			var serverId int64
			_ = rows.Scan(&serverId)
			if len(srv.s.AllowedSources) > 0 {
				servers := lo.Map(srv.s.AllowedSources, func(name string, _ int) int64 {
					return GetServerIDByName(name)
				})
				allowedSources := ServerAllowedApps{ServerID: serverId, AllowedServers: servers}
				allowedSources.Save()

			}

		}
		_ = rows.Close()
	}

	return *srv, nil
}

const updateServerSQL = `
UPDATE servers SET (name, username, password, url, ipaddress, http_method,auth_method, auth_token,
       callback_url, allow_callbacks, cc_urls, allow_copies, start_submission_period, end_submission_period,
       parse_responses, use_ssl, suspended, ssl_client_certkey_file, json_response_xpath, xml_response_xpath, endpoint_type, url_params)
	= (:name,:username,:password,:url,:ipaddress,:http_method,:auth_method,:auth_token, :callback_url,:allow_callbacks, 
               :cc_urls,:allow_copies,:start_submission_period,:end_submission_period,:parse_responses,:use_ssl,
               :suspended,:ssl_client_certkey_file,:json_response_xpath,:xml_response_xpath, :endpoint_type, :url_params)
	WHERE uid = :uid
`

func CreateServers(db *sqlx.DB, servers []Server) (dbutils.MapAnything, error) {
	importSummary := make(dbutils.MapAnything)
	importSummary["updated"] = 0
	importSummary["created"] = 0
	for _, server := range servers {
		if !server.ValidateUID() {
			server.SetUID(utils.GetUID())
		}
		if server.ExistsInDB() {
			log.WithField("Server Name", server.s.Name).Info("Server with same name already exists!")
			// return errors.New(fmt.Sprintf("Server with name %s already exists!", server.s.Name))
			server.s.UID = GetServerUIDByName(server.Name())
			_, err := db.NamedExec(updateServerSQL, server.s)
			if err != nil {
				log.WithError(err).Error("Failed to update server!")
				return importSummary, err
			}
			log.WithField("ServerUID", server.s.UID).Info("Updating server!")
			importSummary["updated"] = importSummary["updated"].(int) + 1
		} else {
			rows, err := db.NamedQuery(insertServerSQL, server.s)
			if err != nil {
				log.WithError(err).Error("Failed to save server to database")
				return importSummary, err
			}
			for rows.Next() {
				var serverId int64
				_ = rows.Scan(&serverId)
				if len(server.s.AllowedSources) > 0 {
					servers := lo.Map(server.s.AllowedSources, func(name string, _ int) int64 {
						iSrv := ServerMapByName[name]
						return int64(iSrv.ID())
					})
					allowedSources := ServerAllowedApps{ServerID: serverId, AllowedServers: servers}
					allowedSources.Save()

				}

			}
			importSummary["created"] = importSummary["created"].(int) + 1
			_ = rows.Close()
		}
	}
	return importSummary, nil
}

func CreateBaseDHIS2Server() {
	metadataServer := &Server{}
	metadataServer.s.Name = "base_OU"
	metadataServer.s.URL = config.MFLIntegratorConf.API.MFLDHIS2BaseURL + "/metadata.json"
	metadataServer.s.Username = config.MFLIntegratorConf.API.MFLDHIS2User
	metadataServer.s.Password = config.MFLIntegratorConf.API.MFLDHIS2Password
	metadataServer.s.AuthMethod = "Basic"
	metadataServer.s.AuthToken = config.MFLIntegratorConf.API.MFLDHIS2PAT
	metadataServer.s.EndPointType = "OUMetadata"

	metadataServer.s.IPAddress = "*"
	metadataServer.s.HTTPMethod = "POST"
	metadataServer.s.SystemType = "DHIS2"
	metadataServer.s.AllowedSources = []string{"localhost"}
	metadataServer.s.ParseResponses = true
	var urlParams dbutils.MapAnything
	if err := json.Unmarshal([]byte(`{"mergeMode":"REPLACE", "importStrategy": "CREATE_AND_UPDATE","async": true,
"importReportMode": "DEBUG"}`), &urlParams); err != nil {
		log.WithError(err).Error("Failed to unmarshal server URL params")
		return
	}
	metadataServer.s.URLParams = urlParams
	metadataServer.s.StartOfSubmissionPeriod = 0
	metadataServer.s.EndOfSubmissionPeriod = 23

	ouGroupAddServer := *metadataServer
	ouGroupAddServer.s.Name = "base_OU_GroupAdd"
	ouGroupAddServer.s.HTTPMethod = "PATCH"
	ouGroupAddServer.s.URL = config.MFLIntegratorConf.API.MFLDHIS2BaseURL + "/organisationUnitGroups"
	ouGroupAddServer.s.EndPointType = "OU_ORGUNIT_GROUP_ADD"
	ouGroupAddServer.s.URLParams = make(dbutils.MapAnything)

	serverList := []Server{*metadataServer, ouGroupAddServer}
	summary, err := CreateServers(db.GetDB(), serverList)
	if err != nil {
		log.WithError(err).Error("Failed to create base DHIS server in dispatcher")
	}
	log.WithFields(log.Fields{"Server": metadataServer.UID(), "Summary": summary}).Info("Server Creation Summary")
}

func SyncLocationsToServer(serverName string) {
	// Servers in this config can receive the base DHIS2 organisation unit hierarchy
	log.WithField("CCDHIS2", serverName).Info("CC Hierarchy Servers")
	server, err := GetServerByName(serverName)
	if err != nil {
		log.WithError(err).Info("Could not proceed to CC hierarchy to server")
	}
	// check if we already have ous in the instance and only create if none present
	ouURL := server.URL()
	baseDHIS2URL, err := utils.GetDHIS2BaseURL(ouURL)
	if err != nil {
		log.WithError(err).Info("CAUTION DHIS2 API URL should have /api/ part")
		return
	}
	p := url.Values{}
	p.Add("fields", "id,name")
	p.Add("paging", "true")
	p.Add("pageSize", "1")
	p.Add("level", fmt.Sprintf("%d", config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel-1))
	chekOusURL := baseDHIS2URL + "/api/organisationUnits.json?"
	//if strings.LastIndex(ouURL, "?") == len(ouURL)-1 {
	//	ouURL += p.Encode()
	//} else {
	//	ouURL += "?" + p.Encode()
	//}
	chekOusURL += p.Encode()
	log.WithFields(log.Fields{"ServerURL": chekOusURL, "Name": server.Name()}).Info("Trying to send hierarchy to server")

	var respBody []byte
	switch server.AuthMethod() {
	case "Basic":
		resp, err := utils.GetWithBasicAuth(chekOusURL, server.Username(), server.Password())
		respBody = resp
		if err != nil {
			log.WithError(err).Error("Failed to get ous from server")
			return
		}

	case "Token":
		resp, err := utils.GetWithToken(chekOusURL, server.AuthToken())
		respBody = resp
		if err != nil {
			log.WithError(err).Error("Failed to get ous from server")
			return
		}
	default:
		// pass
	}
	if respBody != nil {
		v, _, _, err := jsonparser.Get(respBody, "pager", "total")
		if err != nil {
			log.WithError(err).Error("json parser failed to get pager.total key")
		}
		if string(v) != "0" && !*config.ForceSync {

			log.WithField("ForceSync", *config.ForceSync).Info("FORCE SYNC >>>>>>>>>>")
			return // we have ous already
		}
		log.WithField("Server", serverName).Info("We can now sync the hierarchy!!")
		//Send Ou Levels
		syncOuLevels := GenerateOuLevelMetadata()
		levelsPayload := make(map[string][]MetadataOuLevel)
		levelsPayload["organisationUnitLevels"] = syncOuLevels
		SendMetadata(server, levelsPayload)

		// send Ou Groups
		syncOuGroups := GenerateOuGroupsMetadata()
		groupsPayload := make(map[string][]MetadataOuGroup)
		groupsPayload["organisationUnitGroups"] = syncOuGroups
		SendMetadata(server, groupsPayload)

		// send Attributes
		syncAttributes := GenerateAttributeMetadata()
		attributesPayload := make(map[string][]AttributeMetadata)
		attributesPayload["attributes"] = syncAttributes
		SendMetadata(server, attributesPayload)

		// Send Ous
		// for level := 1; level < 3; level++ {
		for level := 1; level < config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel; level++ {

			syncOus := GenerateOuMetadataByLevel(level)
			//for _, ou := range syncOus {
			//	pp, _ := json.Marshal(ou)
			//	log.WithFields(log.Fields{"Payload": string(pp), "Server": serverName, "UID": ou.UID, "ID": ou.ID}).Info("DEBUGGGGGGGG")
			//	rBody := sendMetadata(server, ou)
			//	if rBody != nil {
			//		log.WithFields(log.Fields{"Server": serverName, "Response": string(rBody)}).Info("Metadata Import")
			//	}
			//}
			ouChunks := lo.Chunk(syncOus, config.MFLIntegratorConf.API.MFLMetadataBatchSize)
			// ouChunks := lo.Chunk(syncOus, 1)

			for _, chunck := range ouChunks {
				payload := make(map[string][]MetadataOu)
				payload["organisationUnits"] = chunck
				log.WithFields(log.Fields{"Parant": chunck[0].Parent, "Name": chunck[0].Name, "Level": chunck[0].Level}).Info("OUDATA =>>>>>>>>")

				rBody := SendMetadata(server, payload)
				if rBody != nil {
					log.WithFields(log.Fields{"Server": serverName, "Response": string(rBody)}).Info("Metadata Import")
				}
				if level < config.MFLIntegratorConf.API.MFLDHIS2FacilityLevel-1 {
					time.Sleep(5 * time.Second) // give a longer time to ensure creation on the other side
				} else {
					time.Sleep(3 * time.Second)
				}

			}

		}

	}
}

func SyncAttributesToServer(serverName string) {
	server, err := GetServerByName(serverName)
	if err != nil {
		log.WithError(err).Info("Could not proceed to send attributes to server")
	}

	// send Attributes
	syncAttributes := GenerateAttributeMetadata()
	attributesPayload := make(map[string][]AttributeMetadata)
	attributesPayload["attributes"] = syncAttributes
	SendMetadata(server, attributesPayload)
}

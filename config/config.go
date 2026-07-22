package config

import (
	"context"
	goflag "flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/HISP-Uganda/mfl-integrator/buildinfo"
	"github.com/lib/pq"

	"github.com/fsnotify/fsnotify"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// MFLIntegratorConf is the global conf
var (
	MFLIntegratorConf        Config
	ForceSync                *bool
	SkipOUSync               *bool
	MFLSyncOFF               *bool
	MFLSyncScheduleOFF       *bool
	DisableHTTPServer        *bool
	SkipRequestProcessing    *bool // used to ignore the attempt to send request. Don't produce or consume requests
	SkipAdminUnitsMatching   *bool
	SyncToDHIS2Instances     *bool
	ConfDDir                 string
	mflDHIS2ServersMu        sync.RWMutex
	MFLDHIS2ServersConfigMap = make(map[string]ServerConf)
	ShowVersion              *bool
)

// RedactedSummary returns a small, non-sensitive config view suitable for logs.
func RedactedSummary() map[string]any {
	return map[string]any{
		"database": map[string]any{
			"max_open_conns":     MFLIntegratorConf.Database.MaxOpenConns,
			"max_idle_conns":     MFLIntegratorConf.Database.MaxIdleConns,
			"conn_max_lifetime":  MFLIntegratorConf.Database.ConnMaxLifetime,
			"conn_max_idle_time": MFLIntegratorConf.Database.ConnMaxIdleTime,
		},
		"server": map[string]any{
			"host":                      MFLIntegratorConf.Server.Host,
			"port":                      MFLIntegratorConf.Server.Port,
			"max_retries":               MFLIntegratorConf.Server.MaxRetries,
			"max_concurrent":            MFLIntegratorConf.Server.MaxConcurrent,
			"request_process_interval":  MFLIntegratorConf.Server.RequestProcessInterval,
			"sync_on":                   MFLIntegratorConf.Server.SyncOn,
			"skip_request_processing":   MFLIntegratorConf.Server.SkipRequestProcessing,
			"skip_admin_units_matching": MFLIntegratorConf.Server.SkipAdminUnitsMatching,
			"skip_ou_sync":              MFLIntegratorConf.Server.SkipOUSync,
			"mfl_sync_off":              MFLIntegratorConf.Server.MFLSyncOFF,
			"sync_schedule_off":         MFLIntegratorConf.Server.SyncScheduleOFF,
			"sync_to_dhis2_instances":   MFLIntegratorConf.Server.SyncToDHIS2Instances,
		},
		"api": map[string]any{
			"mfl_base_url":                MFLIntegratorConf.API.MFLBaseURL,
			"mfl_page_size":               MFLIntegratorConf.API.MFLPageSize,
			"dhis2_base_url":              MFLIntegratorConf.API.DHIS2BaseURL,
			"dhis2_facility_level":        MFLIntegratorConf.API.DHIS2FacilityLevel,
			"dhis2_district_oulevel_name": MFLIntegratorConf.API.DHIS2DistrictLevelName,
			"metadata_batch_size":         MFLIntegratorConf.API.MetadataBatchSize,
			"sync_cron_expression":        MFLIntegratorConf.API.SyncCronExpression,
			"retry_cron_expression":       MFLIntegratorConf.API.RetryCronExpression,
		},
	}
}

// Load reads configuration (defaults < file < env < flags), unmarshals into MFLIntegratorConf,
// and loads per-server configs from conf.d.
func Load() error {
	configFilePath, configDir, confDDir, err := defaultPaths(runtime.GOOS)
	if err != nil {
		return err
	}
	ConfDDir = confDDir

	configFile := flag.String("config-file", configFilePath,
		"The path to the configuration file of the application")

	// Flags (one-off overrides). Prefer dotted names to match viper keys.
	ForceSync = flag.Bool("server.force_sync", false, "Whether to forcefully sync organisation unit hierarchy")
	SkipOUSync = flag.Bool("server.skip_ou_sync", false, "Whether to skip ou and facility sync. But process requests")
	DisableHTTPServer = flag.Bool("server.disable_http_server", false, "Whether to disable HTTP Server")
	SkipRequestProcessing = flag.Bool("server.skip_request_processing", false, "Whether to skip requests processing")
	SkipAdminUnitsMatching = flag.Bool("server.skip_admin_units_matching", false, "Whether to skip admin units matching")
	SyncToDHIS2Instances = flag.Bool("server.sync_to_dhis2_instances", false, "Whether to sync to DHIS2 instances")
	MFLSyncOFF = flag.Bool("server.mfl_sync_off", false, "Whether to skip MFL sync, i.e ignore Fetch Facilities from MFL")
	MFLSyncScheduleOFF = flag.Bool("server.sync_schedule_off", false, "Whether to skip MFL sync schedule")
	ShowVersion = flag.Bool("version", false, "Display version of MFL Integrator")

	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	flag.Parse()

	if *ShowVersion {
		fmt.Println(buildinfo.VersionString())
		os.Exit(0)
	}

	// ----- Viper setup (defaults < file < env < flags) -----

	setDefaults()

	viper.SetConfigName("mfld")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(configDir)

	viper.SetEnvPrefix("MFLINTEGRATOR")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// Flags override everything else.
	_ = viper.BindPFlags(flag.CommandLine)

	if strings.TrimSpace(*configFile) != "" {
		viper.SetConfigFile(*configFile)
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			return fmt.Errorf("configuration file not found: %w", err)
		}
		return fmt.Errorf("error reading config: %w", err)
	}

	if err := viper.Unmarshal(&MFLIntegratorConf); err != nil {
		return fmt.Errorf("unable to decode config into struct: %w", err)
	}

	// Hot reload main config (single file).
	viper.OnConfigChange(func(e fsnotify.Event) {
		log.WithField("file", e.Name).Info("Config file changed; reloading")
		if err := viper.ReadInConfig(); err != nil {
			log.WithError(err).Error("unable to reread configuration")
			return
		}
		if err := viper.Unmarshal(&MFLIntegratorConf); err != nil {
			log.WithError(err).Error("unable to re-unmarshal configuration")
			return
		}
	})
	viper.WatchConfig()

	// Initial conf.d load (top-level *.json only)
	if err := loadServerConfsFromDir(confDDir); err != nil {
		log.WithError(err).Warn("Failed to load conf.d server configs")
	}

	return nil
}

func defaultPaths(goos string) (configFilePath, configDir, confDDir string, err error) {
	switch goos {
	case "windows":
		configDir = "C:\\ProgramData\\MFLIntegrator"
		configFilePath = "C:\\ProgramData\\MFLIntegrator\\mfld.yml"
		confDDir = "C:\\ProgramData\\MFLIntegrator\\conf.d"
		return configFilePath, configDir, confDDir, nil
	case "darwin", "linux":
		configFilePath = "/etc/mflintegrator/mfld.yml"
		configDir = "/etc/mflintegrator/"
		confDDir = "/etc/mflintegrator/conf.d"
		return configFilePath, configDir, confDDir, nil
	default:
		return "", "", "", fmt.Errorf("unsupported operating system: %s", goos)
	}
}

func setDefaults() {
	// Keep defaults aligned with mapstructure tags in Config.

	viper.SetDefault("database.uri", "postgres://postgres:postgres@localhost/mfldb_new?sslmode=disable")
	viper.SetDefault("database.max_open_conns", 20)
	viper.SetDefault("database.max_idle_conns", 10)
	viper.SetDefault("database.conn_max_lifetime", "30m")
	viper.SetDefault("database.conn_max_idle_time", "5m")
	// server.*
	viper.SetDefault("server.host", "localhost")
	viper.SetDefault("server.http_port", "9090")
	viper.SetDefault("server.proxy_port", "9191")
	viper.SetDefault("server.max_retries", 3)
	viper.SetDefault("server.max_concurrent", 5)
	viper.SetDefault("server.request_process_interval", 4)

	viper.SetDefault("server.skip_request_processing", false)
	viper.SetDefault("server.skip_admin_units_matching", false)
	viper.SetDefault("server.skip_ou_sync", false)
	viper.SetDefault("server.sync_schedule_off", false)
	viper.SetDefault("server.force_sync", false)
	viper.SetDefault("server.sync_on", true)

	// Safer default: do real work unless explicitly faked.
	viper.SetDefault("server.fake_sync_to_base_dhis2", false)
	viper.SetDefault("server.sync_to_dhis2_instances", false)

	// api.*
	viper.SetDefault("api.dhis2_top_level_tree_ids", "")
	viper.SetDefault("api.mfl_page_size", 200)
	viper.SetDefault("api.dhis2_facility_level", 5)
	viper.SetDefault("api.dhis2_district_oulevel_name", "District/City")
	viper.SetDefault("api.metadata_batch_size", 50)
	viper.SetDefault("api.sync_cron_expression", "0 0-23/6 * * *")
	viper.SetDefault("api.retry_cron_expression", "*/5 * * * *")
}

// WatchConfD watches confDDir for changes and reloads conf.d JSON configs when any top-level *.json file changes.
// If onReload is non-nil, it is called after a successful reload with a snapshot of the new config map.
func WatchConfD(ctx context.Context, confDDir string, onReload func(map[string]ServerConf)) error {
	if strings.TrimSpace(confDDir) == "" {
		return fmt.Errorf("watch conf.d: empty directory")
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("watch conf.d: create watcher: %w", err)
	}

	if err := w.Add(confDDir); err != nil {
		_ = w.Close()
		return fmt.Errorf("watch conf.d: add directory %q: %w", confDDir, err)
	}

	const debounce = 300 * time.Millisecond

	go func() {
		defer func() { _ = w.Close() }()

		var (
			timer  *time.Timer
			timerC <-chan time.Time
		)

		scheduleReload := func() {
			if timer == nil {
				timer = time.NewTimer(debounce)
				timerC = timer.C
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(debounce)
			timerC = timer.C
		}

		reload := func() {
			if err := loadServerConfsFromDir(confDDir); err != nil {
				log.WithError(err).WithField("dir", confDDir).Warn("conf.d reload failed")
				return
			}
			log.WithField("dir", confDDir).Info("conf.d reloaded")

			if onReload != nil {
				onReload(GetDHIS2ServersConfigMap())
			}
		}

		for {
			select {
			case <-ctx.Done():
				log.WithError(ctx.Err()).WithField("dir", confDDir).Info("Stopping conf.d watcher")
				return

			case err := <-w.Errors:
				if err != nil {
					log.WithError(err).WithField("dir", confDDir).Warn("conf.d watcher error")
				}

			case e := <-w.Events:
				if e.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) == 0 {
					continue
				}
				if strings.HasSuffix(strings.ToLower(e.Name), ".json") || e.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
					scheduleReload()
				}

			case <-timerC:
				timerC = nil
				reload()
			}
		}
	}()

	return nil
}

func loadServerConfsFromDir(confDDir string) error {
	v := viper.New()
	v.SetConfigType("json")

	files, err := listTopLevelJSONFiles(confDDir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		// Clear map if directory becomes empty.
		mflDHIS2ServersMu.Lock()
		MFLDHIS2ServersConfigMap = make(map[string]ServerConf)
		mflDHIS2ServersMu.Unlock()
		return nil
	}

	sort.Strings(files)

	next := make(map[string]ServerConf, len(files))

	for _, file := range files {
		v.SetConfigFile(file)

		if err := v.ReadInConfig(); err != nil {
			log.WithError(err).WithField("file", file).Error("Error reading server config file")
			continue
		}

		var sc ServerConf
		if err := v.Unmarshal(&sc); err != nil {
			log.WithError(err).WithField("file", file).Error("Error unmarshaling server config file")
			continue
		}

		if strings.TrimSpace(sc.Name) == "" {
			log.WithField("file", file).Warn("Skipping server config with empty name")
			continue
		}

		next[sc.Name] = sc
	}

	// Swap map atomically under lock to avoid concurrent map read/write panics.
	mflDHIS2ServersMu.Lock()
	MFLDHIS2ServersConfigMap = next
	mflDHIS2ServersMu.Unlock()

	return nil
}

func listTopLevelJSONFiles(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		// If conf.d doesn't exist, treat as "no extra server configs"
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(strings.ToLower(name), ".json") {
			files = append(files, filepath.Join(directory, name))
		}
	}

	return files, nil
}

func listJSONFilesRecursive(directory string) ([]string, error) {
	var files []string

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".json") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return files, nil
}

// var FakeSyncToBaseDHIS2 *bool

// Config is the top level cofiguration object
type Config struct {
	Database struct {
		URI             string `mapstructure:"uri" env:"MFLINTEGRATOR_DB" env-default:"postgres://postgres:postgres@localhost/mfldb?sslmode=disable"`
		DBHost          string `mapstructure:"db_host" env:"MFLINTEGRATOR_DB_HOST" env-default:"localhost"`
		DBPort          string `mapstructure:"db_port" env:"MFLINTEGRATOR_DB_PORT" env-default:"5432"`
		DBUsername      string `mapstructure:"db_username" env:"MFLINTEGRATOR_DB_USER" env-description:"API user name"`
		DBPassword      string `mapstructure:"db_password" env:"MFLINTEGRATOR_DB_PASSWORD" env-description:"API user password"`
		MaxOpenConns    int    `mapstructure:"max_open_conns" env:"MFLINTEGRATOR_DB_MAX_OPEN_CONNS" env-default:"20"`
		MaxIdleConns    int    `mapstructure:"max_idle_conns" env:"MFLINTEGRATOR_DB_MAX_IDLE_CONNS" env-default:"10"`
		ConnMaxLifetime string `mapstructure:"conn_max_lifetime" env:"MFLINTEGRATOR_DB_CONN_MAX_LIFETIME" env-default:"30m"`
		ConnMaxIdleTime string `mapstructure:"conn_max_idle_time" env:"MFLINTEGRATOR_DB_CONN_MAX_IDLE_TIME" env-default:"5m"`
	} `yaml:"database"`

	Server struct {
		Host                    string `mapstructure:"host" env:"MFLINTEGRATOR_HOST" env-default:"localhost"`
		Port                    string `mapstructure:"http_port" env:"MFLINTEGRATOR_SERVER_PORT" env-description:"Server port" env-default:"9090"`
		ProxyPort               string `mapstructure:"proxy_port" env:"MFLINTEGRATOR_PROXY_PORT" env-description:"Server port" env-default:"9191"`
		MaxRetries              int    `mapstructure:"max_retries" env:"MFLINTEGRATOR_MAX_RETRIES" env-default:"3"`
		StartOfSubmissionPeriod string `mapstructure:"start_submission_period" env:"MFLINTEGRATOR_START_SUBMISSION_PERIOD" env-default:"18"`
		EndOfSubmissionPeriod   string `mapstructure:"end_submission_period" env:"MFLINTEGRATOR_END_SUBMISSION_PERIOD" env-default:"24"`
		MaxConcurrent           int    `mapstructure:"max_concurrent" env:"MFLINTEGRATOR_MAX_CONCURRENT" env-default:"5"`
		SkipRequestProcessing   bool   `mapstructure:"skip_request_processing" env:"MFLINTEGRATOR_SKIP_REQUEST_PROCESSING" env-default:"false"`
		SkipAdminUnitsMatching  bool   `mapstructure:"skip_admin_units_matching" env:"MFLINTEGRATOR_SKIP_ADMIN_UNITS_MATCHING" env-default:"true"`
		SkipOUSync              bool   `mapstructure:"skip_ou_sync" env:"MFLINTEGRATOR_SKIP_OU_SYNC" env-default:"false"`
		SyncScheduleOFF         bool   `mapstructure:"sync_schedule_off" env:"MFLINTEGRATOR_SYNC_SCHEDULE_OFF" env-default:"false"`
		MFLSyncOFF              bool   `mapstructure:"mfl_sync_off" env-default:"false"`
		ForceSync               bool   `mapstructure:"force_sync" env:"MFLINTEGRATOR_FORCE_SYNC" env-default:"false"` // Assume OU hierarchy already there
		SyncOn                  bool   `mapstructure:"sync_on" env:"MFLINTEGRATOR_SYNC_ON" env-default:"true"`
		FakeSyncToBaseDHIS2     bool   `mapstructure:"fake_sync_to_base_dhis2" env:"MFLINTEGRATOR_FAKE_SYNC_TO_BASE_DHIS2" env-default:"false"`
		SyncToDHIS2Instances    bool   `mapstructure:"sync_to_dhis2_instances" env:"MFLINTEGRATOR_SYNC_TO_DHIS2_INSTANCES" env-default:"false"`
		RequestProcessInterval  int    `mapstructure:"request_process_interval" env:"MFLINTEGRATOR_REQUEST_PROCESS_INTERVAL" env-default:"4"`
		LogDirectory            string `mapstructure:"log_dir" env:"MFLINTEGRATOR_LOG_DIRECTORY" env-default:"/var/log/mflintegrator"`
		UseSSL                  string `mapstructure:"use_ssl" env:"MFLINTEGRATOR_USE_SSL" env-default:"true"`
		SSLClientCertKeyFile    string `mapstructure:"ssl_client_certkey_file" env:"SSL_CLIENT_CERTKEY_FILE" env-default:""`
		SSLServerCertKeyFile    string `mapstructure:"ssl_server_certkey_file" env:"SSL_SERVER_CERTKEY_FILE" env-default:""`
		SSLTrustedCAFile        string `mapstructure:"ssl_trusted_cafile" env:"SSL_TRUSTED_CA_FILE" env-default:""`
	} `yaml:"server"`

	API struct {
		MFLBaseURL               string `mapstructure:"mfl_base_url" env:"MFLINTEGRATOR_BASE_URL" env-description:"The MFL base API URL"`
		MFLPageSize              int    `mapstructure:"mfl_page_size" env:"MFLINTEGRATOR_MFL_PAGE_SIZE" env-description:"The page size used when fetching facilities from MFL" env-default:"200"`
		MFLUser                  string `mapstructure:"mfl_user"  env:"MFLINTEGRATOR_USER" env-description:"The MFL API username"`
		MFLPassword              string `mapstructure:"mfl_password"  env:"MFLINTEGRATOR_PASSWORD" env-description:"The MFL API user password"`
		DHIS2BaseURL             string `mapstructure:"dhis2_base_url" env:"MFLINTEGRATOR_DHIS2_BASE_URL" env-description:"The base DHIS2 instance base API URL"`
		DHIS2User                string `mapstructure:"dhis2_user" env:"MFLINTEGRATOR_DHIS2_USER" env-description:"The base DHIS2 username"`
		DHIS2Password            string `mapstructure:"dhis2_password" env:"MFLINTEGRATOR_DHIS2_PASSWORD" env-description:"The MFL base DHIS2  user password"`
		DHIS2PAT                 string `mapstructure:"dhis2_pat" env:"MFLINTEGRATOR_DHIS2_PAT" env-description:"The base DHIS2  Personal Access Token"`
		DHIS2TopLevelTreeIDs     string `mapstructure:"dhis2_top_level_tree_ids" env:"MFLINTEGRATOR_DHIS2_TOP_LEVEL_TREE_IDS" env-description:"The MFL base DHIS2  orgunits top level tree ids"`
		DHIS2FacilityLevel       int    `mapstructure:"dhis2_facility_level" env:"MFLINTEGRATOR_DHIS2_FACILITY_LEVEL" env-description:"The base DHIS2  Orgunit Level for health facilities" env-default:"5"`
		DHIS2DistrictLevelName   string `mapstructure:"dhis2_district_oulevel_name"  env:"MFLINTEGRATOR_DHIS2_DISTRICT_OULEVEL_NAME" env-description:"The MFL base DHIS2 OU Level name for districts" env-default:"District/City"`
		DHIS2OUMFLIDAttributeID  string `mapstructure:"dhis2_ou_mflid_attribute_id" env:"MFLINTEGRATOR_DHIS2_OU_MFLID_ATTRIBUTE_ID" env-description:"The DHIS2 OU MFLID Attribute ID"`
		CCDHIS2Servers           string `mapstructure:"cc_dhis2_servers"  env:"MFLINTEGRATOR_CC_DHIS2_SERVERS" env-description:"The MFL CC DHIS2 instances to receive copy of facilities"`
		CCDHIS2HierarchyServers  string `mapstructure:"cc_dhis2_hierarchy_servers"  env:"MFLINTEGRATOR_CC_DHIS2_HIERARCHY_SERVERS" env-description:"The MFL CC DHIS2 instances to receive copy of OU hierarchy"`
		CCDHIS2CreateServers     string `mapstructure:"cc_dhis2_create_servers"  env:"MFLINTEGRATOR_CC_DHIS2_CREATE_SERVERS" env-description:"The MFL CC DHIS2 instances to receive copy of OU creations"`
		CCDHIS2UpdateServers     string `mapstructure:"cc_dhis2_update_servers"  env:"MFLINTEGRATOR_CC_DHIS2_UPDATE_SERVERS" env-description:"The MFL CC DHIS2 instances to receive copy of OU updates"`
		CCDHIS2OuGroupAddServers string `mapstructure:"cc_dhis2_ougroup_add_servers"  env:"MFLINTEGRATOR_CC_DHIS2_OUGROUP_ADD_SERVERS" env-description:"The MFL CC DHIS2 instances APIs used to add ous to groups"`
		MetadataBatchSize        int    `mapstructure:"metadata_batch_size" env:"MFLINTEGRATOR_METADATA_BATCH_SIZE" env-description:"The MFL Metadata items to chunk in a metadata request" env-default:"50"`
		SyncCronExpression       string `mapstructure:"sync_cron_expression"  env:"MFLINTEGRATOR_SYNC_CRON_EXPRESSION" env-description:"The MFL Health Facility Syncronisation Cron Expression" env-default:"0 0-23/6 * * *"`
		RetryCronExpression      string `mapstructure:"retry_cron_expression"  env:"MFLINTEGRATOR_RETRY_CRON_EXPRESSION" env-description:"The MFL request retry Cron Expression" env-default:"*/5 * * * *"`
		Email                    string `mapstructure:"email" env:"MFLINTEGRATOR_EMAIL" env-description:"API user email address"`
		AuthToken                string `mapstructure:"auth_token" env:"RAPIDPRO_AUTH_TOKEN" env-description:"API JWT authorization token"`
		SmsURL                   string `mapstructure:"sms_url" env:"MFLINTEGRATOR_SMS_URL" env-description:"API SMS endpoint"`
	} `yaml:"api"`
}

type ServerConf struct {
	ID                      int64          `mapstructure:"id" json:"-"`
	UID                     string         `mapstructure:"uid" json:"uid,omitempty"`
	Name                    string         `mapstructure:"name" json:"name" validate:"required"`
	Username                string         `mapstructure:"username" json:"username"`
	Password                string         `mapstructure:"password" json:"password,omitempty"`
	IsProxyServer           bool           `mapstructure:"isProxyserver" json:"isProxyServer,omitempty"`
	SystemType              string         `mapstructure:"systemType" json:"systemType,omitempty"`
	EndPointType            string         `mapstructure:"endpointType" json:"endPointType,omitempty"`
	AuthToken               string         `mapstructure:"authToken" db:"auth_token" json:"AuthToken"`
	IPAddress               string         `mapstructure:"IPAddress"  json:"IPAddress"`
	URL                     string         `mapstructure:"URL" json:"URL" validate:"required,url"`
	CCURLS                  pq.StringArray `mapstructure:"CCURLS" json:"CCURLS,omitempty"`
	CallbackURL             string         `mapstructure:"callbackURL" json:"callbackURL,omitempty"`
	HTTPMethod              string         `mapstructure:"HTTPMethod" json:"HTTPMethod" validate:"required"`
	AuthMethod              string         `mapstructure:"AuthMethod" json:"AuthMethod" validate:"required"`
	AllowCallbacks          bool           `mapstructure:"allowCallbacks" json:"allowCallbacks,omitempty"`
	AllowCopies             bool           `mapstructure:"allowCopies" json:"allowCopies,omitempty"`
	UseAsync                bool           `mapstructure:"useAsync" json:"useAsync,omitempty"`
	UseSSL                  bool           `mapstructure:"useSSL" json:"useSSL,omitempty"`
	ParseResponses          bool           `mapstructure:"parseResponses" json:"parseResponses,omitempty"`
	SSLClientCertKeyFile    string         `mapstructure:"sslClientCertkeyFile" json:"sslClientCertkeyFile"`
	StartOfSubmissionPeriod int            `mapstructure:"startSubmissionPeriod" json:"startSubmissionPeriod"`
	EndOfSubmissionPeriod   int            `mapstructure:"endSubmissionPeriod" json:"endSubmissionPeriod"`
	XMLResponseXPATH        string         `mapstructure:"XMLResponseXPATH"  json:"XMLResponseXPATH"`
	JSONResponseXPATH       string         `mapstructure:"JSONResponseXPATH" json:"JSONResponseXPATH"`
	Suspended               bool           `mapstructure:"suspended" json:"suspended,omitempty"`
	URLParams               map[string]any `mapstructure:"URLParams" json:"URLParams,omitempty"`
	Created                 time.Time      `mapstructure:"created" json:"created,omitempty"`
	Updated                 time.Time      `mapstructure:"updated" json:"updated,omitempty"`
	AllowedSources          []string       `mapstructure:"allowedSources" json:"allowedSources,omitempty"`
}

// GetDHIS2ServersConfigMap returns a snapshot copy of the current conf.d server configs.
// Use this instead of reading MFLDHIS2ServersConfigMap directly to avoid concurrent map access panics.
func GetDHIS2ServersConfigMap() map[string]ServerConf {
	mflDHIS2ServersMu.RLock()
	defer mflDHIS2ServersMu.RUnlock()

	out := make(map[string]ServerConf, len(MFLDHIS2ServersConfigMap))
	for k, v := range MFLDHIS2ServersConfigMap {
		out[k] = v
	}
	return out
}

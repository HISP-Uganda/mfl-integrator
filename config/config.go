package config

import (
	goflag "flag"
	"fmt"
	"github.com/lib/pq"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// MFLIntegratorConf is the global conf
var MFLIntegratorConf Config
var ForceSync *bool
var SkipOUSync *bool
var SkipRequestProcessing *bool
var MFLDHIS2ServersConfigMap = make(map[string]ServerConf)

func init() {
	// ./mfl-integrator --config-file /etc/mflintegrator/mfld.yml
	var configFilePath, configDir, conf_dDir string
	curentOS := runtime.GOOS
	switch curentOS {
	case "windows":
		configDir = "C:\\ProgramData\\MFLIntegrator"
		configFilePath = "C:\\ProgramData\\MFLIntegrator\\mfld.yml"
		conf_dDir = "C:\\ProgramData\\MFLIntegrator\\conf.d"
	case "darwin", "linux":
		configFilePath = "/etc/mflintegrator/mfld.yml"
		configDir = "/etc/mflintegrator/"
		conf_dDir = "/etc/mflintegrator/conf.d" // for the conf.d directory where to dump server confs
	default:
		fmt.Println("Unsupported operating system")
		return
	}

	configFile := flag.String("config-file", configFilePath,
		"The path to the configuration file of the application")

	ForceSync = flag.Bool("force-sync", false, "Whether to forcefully sync organisation unit hierarchy")
	SkipOUSync = flag.Bool("skip-ousync", false, "Whether to skip ou and facility sync. But process requests")
	SkipRequestProcessing = flag.Bool("skip-request-processing", false, "Whether to skip requests processing")

	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	flag.Parse()

	viper.SetConfigName("mfld")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(configDir)

	if len(*configFile) > 0 {
		viper.SetConfigFile(*configFile)
		log.Printf("Config File %v", *configFile)
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// log.Fatalf("Configuration File: %v Not Found", *configFile, err)
			panic(fmt.Errorf("Fatal Error %w \n", err))

		} else {
			log.Fatalf("Error Reading Config: %v", err)

		}
	}

	err := viper.Unmarshal(&MFLIntegratorConf)
	if err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}

	viper.OnConfigChange(func(e fsnotify.Event) {
		fmt.Println("Config file changed:", e.Name)
		err = viper.ReadInConfig()
		if err != nil {
			log.Fatalf("unable to reread configuration into global conf: %v", err)
		}
		_ = viper.Unmarshal(&MFLIntegratorConf)
	})
	viper.WatchConfig()

	v := viper.New()
	v.SetConfigType("json")

	fileList, err := getFilesInDirectory(conf_dDir)
	if err != nil {
		log.WithError(err).Info("Error reading directory")
	}
	// Loop through the files and read each one
	for _, file := range fileList {
		v.SetConfigFile(file)

		if err := v.ReadInConfig(); err != nil {
			log.WithError(err).WithField("File", file).Error("Error reading config file:")
			continue
		}

		// Unmarshal the config data into your structure
		var config ServerConf
		if err := v.Unmarshal(&config); err != nil {
			log.WithError(err).WithField("File", file).Error("Error unmarshaling config file:")
			continue
		}
		MFLDHIS2ServersConfigMap[config.Name] = config

		// Now you can use the config structure as needed
		// fmt.Printf("Configuration from %s: %+v\n", file, config)
	}
	v.OnConfigChange(func(e fsnotify.Event) {
		if err := v.ReadInConfig(); err != nil {
			log.WithError(err).WithField("File", e.Name).Error("Error reading config file:")
		}

		// Unmarshal the config data into your structure
		var config ServerConf
		if err := v.Unmarshal(&config); err != nil {
			log.WithError(err).WithField("File", e.Name).Fatalf("Error unmarshaling config file:")
		}
		MFLDHIS2ServersConfigMap[config.Name] = config
	})
	v.WatchConfig()
}

// Config is the top level cofiguration object
type Config struct {
	Database struct {
		URI        string `mapstructure:"uri" env:"MFLINTEGRATOR_DB" env-default:"postgres://postgres:postgres@localhost/mfldb?sslmode=disable"`
		DBHost     string `mapstructure:"db_host" env:"MFLINTEGRATOR_DB_HOST" env-default:"localhost"`
		DBPort     string `mapstructure:"db_port" env:"MFLINTEGRATOR_DB_PORT" env-default:"5432"`
		DBUsername string `mapstructure:"db_username" env:"MFLINTEGRATOR_DB_USER" env-description:"API user name"`
		DBPassword string `mapstructure:"db_password" env:"MFLINTEGRATOR_DB_PASSWORD" env-description:"API user password"`
	} `yaml:"database"`

	Server struct {
		Host                    string `mapstructure:"host" env:"MFLINTEGRATOR_HOST" env-default:"localhost"`
		Port                    string `mapstructure:"http_port" env:"MFLINTEGRATOR_SERVER_PORT" env-description:"Server port" env-default:"9090"`
		ProxyPort               string `mapstructure:"proxy_port" env:"MFLINTEGRATOR_PROXY_PORT" env-description:"Server port" env-default:"9191"`
		MaxRetries              int    `mapstructure:"max_retries" env:"MFLINTEGRATOR_MAX_RETRIES" env-default:"3"`
		StartOfSubmissionPeriod string `mapstructure:"start_submission_period" env:"MFLINTEGRATOR_START_SUBMISSION_PERIOD" env-default:"18"`
		EndOfSubmissionPeriod   string `mapstructure:"end_submission_period" env:"MFLINTEGRATOR_END_SUBMISSION_PERIOD" env-default:"24"`
		MaxConcurrent           int    `mapstructure:"max_concurrent" env:"MFLINTEGRATOR_MAX_CONCURRENT" env-default:"5"`
		SyncOn                  bool   `mapstructure:"sync_on" env:"MFLINTEGRATOR_SYNC_ON" env-default:"true"`
		RequestProcessInterval  int    `mapstructure:"request_process_interval" env:"MFLINTEGRATOR_REQUEST_PROCESS_INTERVAL" env-default:"4"`
		LogDirectory            string `mapstructure:"logdir" env:"MFLINTEGRATOR_LOGDIR" env-default:"/var/log/mflintegrator"`
		UseSSL                  string `mapstructure:"use_ssl" env:"MFLINTEGRATOR_USE_SSL" env-default:"true"`
		SSLClientCertKeyFile    string `mapstructure:"ssl_client_certkey_file" env:"SSL_CLIENT_CERTKEY_FILE" env-default:""`
		SSLServerCertKeyFile    string `mapstructure:"ssl_server_certkey_file" env:"SSL_SERVER_CERTKEY_FILE" env-default:""`
		SSLTrustedCAFile        string `mapstructure:"ssl_trusted_cafile" env:"SSL_TRUSTED_CA_FILE" env-default:""`
	} `yaml:"server"`

	API struct {
		MFLBaseURL                  string `mapstructure:"mfl_base_url" env:"MFLINTEGRATOR_BASE_URL" env-description:"The MFL base API URL"`
		MFLUser                     string `mapstructure:"mfl_user"  env:"MFLINTEGRATOR_USER" env-description:"The MFL API username"`
		MFLPassword                 string `mapstructure:"mfl_password"  env:"MFLINTEGRATOR_PASSWORD" env-description:"The MFL API user password"`
		MFLDHIS2BaseURL             string `mapstructure:"mfl_dhis2_base_url" env:"MFLINTEGRATOR_DHIS2_BASE_URL" env-description:"The MFL base DHIS2 instance base API URL"`
		MFLDHIS2User                string `mapstructure:"mfl_dhis2_user"  env:"MFLINTEGRATOR_DHIS2_USER" env-description:"The MFL base DHIS2 username"`
		MFLDHIS2Password            string `mapstructure:"mfl_dhis2_password"  env:"MFLINTEGRATOR_DHIS2_PASSWORD" env-description:"The MFL base DHIS2  user password"`
		MFLDHIS2PAT                 string `mapstructure:"mfl_dhis2_pat"  env:"MFLINTEGRATOR_DHIS2_PAT" env-description:"The MFL base DHIS2  Personal Access Token"`
		MFLDHIS2TreeIDs             string `mapstructure:"mfl_dhis2_tree_ids"  env:"MFLINTEGRATOR_DHIS2_TREE_IDS" env-description:"The MFL base DHIS2  orgunits top level ids"`
		MFLDHIS2FacilityLevel       int    `mapstructure:"mfl_dhis2_facility_level"  env:"MFLINTEGRATOR_DHIS2_FACILITY_LEVEL" env-description:"The MFL base DHIS2  Orgunit Level for health facilities" env-default:"5"`
		MFLDHIS2OUMFLIDAttributeID  string `mapstructure:"mfl_dhis2_ou_mflid_attribute_id" env:"MFLINTEGRATOR_DHIS2_OU_MFLID_ATTRIBUTE_ID" env-description:"The DHIS2 OU MFLID Attribute ID"`
		MFLCCDHIS2Servers           string `mapstructure:"mfl_cc_dhis2_servers"  env:"MFLINTEGRATOR_CC_DHIS2_SERVERS" env-description:"The MFL CC DHIS2 instances to receive copy of facilities"`
		MFLCCDHIS2HierarchyServers  string `mapstructure:"mfl_cc_dhis2_hierarchy_servers"  env:"MFLINTEGRATOR_CC_DHIS2_HIERARCHY_SERVERS" env-description:"The MFL CC DHIS2 instances to receive copy of OU hierarchy"`
		MFLCCDHIS2CreateServers     string `mapstructure:"mfl_cc_dhis2_create_servers"  env:"MFLINTEGRATOR_CC_DHIS2_CREATE_SERVERS" env-description:"The MFL CC DHIS2 instances to receive copy of OU creations"`
		MFLCCDHIS2OuGroupAddServers string `mapstructure:"mfl_cc_dhis2_ougroup_add_servers"  env:"MFLINTEGRATOR_CC_DHIS2_OUGROUP_ADD_SERVERS" env-description:"The MFL CC DHIS2 instances APIs used to add ous to groups"`
		MFLMetadataBatchSize        int    `mapstructure:"mfl_metadata_batch_size"  env:"MFLINTEGRATOR_METADATA_BATCH_SIZE" env-description:"The MFL Metadata items to chunk in a metadata request" env-default:"50"`
		MFLSyncCronExpression       string `mapstructure:"mfl_sync_cron_expression"  env:"MFLINTEGRATOR_SYNC_CRON_EXPRESSION" env-description:"The MFL Health Facility Syncronisation Cron Expression" env-default:"0 0-23/6 * * *"`
		MFLRetryCronExpression      string `mapstructure:"mfl_retry_cron_expression"  env:"MFLINTEGRATOR_RETRY_CRON_EXPRESSION" env-description:"The MFL request retry Cron Expression" env-default:"*/5 * * * *"`
		Email                       string `mapstructure:"email" env:"MFLINTEGRATOR_EMAIL" env-description:"API user email address"`
		AuthToken                   string `mapstructure:"authtoken" env:"RAPIDPRO_AUTH_TOKEN" env-description:"API JWT authorization token"`
		SmsURL                      string `mapstructure:"smsurl" env:"MFLINTEGRATOR_SMS_URL" env-description:"API SMS endpoint"`
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

func getFilesInDirectory(directory string) ([]string, error) {
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

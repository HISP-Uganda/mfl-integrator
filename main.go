package main

import (
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/controllers"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/gin-gonic/gin"
	"github.com/go-co-op/gocron"
	"github.com/jmoiron/sqlx"
	"github.com/samply/golang-fhir-models/fhir-models/fhir"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"time"
)

func init() {
	formatter := new(log.TextFormatter)
	formatter.TimestampFormat = time.RFC3339
	formatter.FullTimestamp = true
	log.SetFormatter(formatter)
	log.SetOutput(os.Stdout)
}

type LocationEntry struct {
	FullURL  string        `json:"fullUrl"`
	Resource fhir.Location `json:"resource"`
}

var splash = `
┏┳┓┏━╸╻     ╺┳╸┏━┓   ╺┳┓╻ ╻╻┏━┓┏━┓
┃┃┃┣╸ ┃      ┃ ┃ ┃    ┃┃┣━┫┃┗━┓┏━┛
╹ ╹╹  ┗━╸    ╹ ┗━┛   ╺┻┛╹ ╹╹┗━┛┗━
`

func main() {
	fmt.Printf(splash)
	dbConn, err := sqlx.Connect("postgres", config.MFLIntegratorConf.Database.URI)
	if err != nil {
		log.Fatalln(err)
	}
	// log.WithField("DHIS2_SERVER_CONFIGS", config.MFLDHIS2ServersConfigMap).Info("SERVER: =======>")
	LoadServersFromConfigFiles(config.MFLDHIS2ServersConfigMap)
	// log.WithFields(log.Fields{"Servers": models.ServerMapByName["localhost"]}).Info("SERVERS==>>")
	// os.Exit(1)

	go func() {
		// Create a new scheduler
		s := gocron.NewScheduler(time.UTC)
		// Schedule the task to run "30 minutes after midn, 4am, 8am, 12pm..., everyday"
		// if --skip-ousync flag is on we ignore
		if !*config.SkipOUSync {
			log.WithFields(log.Fields{"SyncCronExpression": config.MFLIntegratorConf.API.MFLSyncCronExpression}).Info(
				"Facility Synchronisation Cron Expression")
			_, err := s.Cron(config.MFLIntegratorConf.API.MFLSyncCronExpression).Do(FetchFacilitiesByDistrict)
			// _, err := s.CronWithSeconds("* * * * * *").Do(task)
			if err != nil {
				log.WithError(err).Error("Error scheduling facility sync task:")
				return
			}
		}

		// retrying incomplete requests runs every 5 minutes
		log.WithFields(log.Fields{"RetryCronExpression": config.MFLIntegratorConf.API.MFLRetryCronExpression}).Info(
			"Request Retry Cron Expression")
		_, err = s.Cron(config.MFLIntegratorConf.API.MFLRetryCronExpression).Do(RetryIncompleteRequests)
		if err != nil {
			log.WithError(err).Error("Error scheduling incomplete request retry task:")
		}
		s.StartAsync()
	}()

	go func() {
		if !*config.SkipOUSync {
			LoadOuLevels()
			LoadOuGroups()
			LoadLocations() // Load organisation units - before facility in base DHIS2 instance
			MatchLocationsWithMFL()
			SyncLocationsToDHIS2Instances()
			// fetch facilities after initial run, just use a scheduled job.
			FetchFacilitiesByDistrict()
		}

	}()

	jobs := make(chan int)
	var wg sync.WaitGroup

	seenMap := make(map[models.RequestID]bool)
	mutex := &sync.Mutex{}
	rWMutex := &sync.RWMutex{}

	// Start the producer goroutine
	wg.Add(1)
	go Produce(dbConn, jobs, &wg, mutex, seenMap)

	// Start the consumer goroutine
	wg.Add(1)
	go StartConsumers(jobs, &wg, rWMutex, seenMap)

	// Start the backend API gin server
	wg.Add(1)
	go startAPIServer(&wg)

	wg.Wait()
}

func startAPIServer(wg *sync.WaitGroup) {
	defer wg.Done()
	router := gin.Default()
	v2 := router.Group("/api", BasicAuth())
	{
		v2.GET("/test2", func(c *gin.Context) {
			c.String(200, "Authorized")
		})

		q := new(controllers.QueueController)
		v2.POST("/queue", q.Queue)
		v2.GET("/queue", q.Requests)
		v2.GET("/queue/:id", q.GetRequest)
		v2.DELETE("/queue/:id", q.DeleteRequest)

		ou := new(controllers.OrgUnitController)
		v2.POST("/organisationUnits", ou.OrgUnit)
		v2.GET("/organisationUnits", ou.GetOrganisationUnits)

		s := new(controllers.ServerController)
		v2.POST("/servers", s.CreateServer)
		v2.POST("/importServers", s.ImportServers)

		ot := new(controllers.OrgUnitTreeController)
		v2.GET("/outree/:server", ot.CreateOrgUnitTree)

	}
	// Handle error response when a route is not defined
	router.NoRoute(func(c *gin.Context) {
		c.String(404, "Page Not Found!")
	})

	_ = router.Run(":" + fmt.Sprintf("%s", config.MFLIntegratorConf.Server.Port))
}

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/HISP-Uganda/mfl-integrator/clients"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/controllers"
	"github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/HISP-Uganda/mfl-integrator/utils"
	"github.com/gin-gonic/gin"
	"github.com/go-co-op/gocron"
	"github.com/samply/golang-fhir-models/fhir-models/fhir"
	log "github.com/sirupsen/logrus"
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

func runObservedJob(jobName string, fn func()) {
	runID := utils.GetUID()
	start := time.Now()
	entry := log.WithFields(log.Fields{
		"job":    jobName,
		"run_id": runID,
	})
	entry.Info("Job started")
	defer func() {
		entry.WithField("duration_ms", time.Since(start).Milliseconds()).Info("Job finished")
	}()
	fn()
}

func main() {
	fmt.Printf(splash)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Load configuration first (before you read config.MFLIntegratorConf anywhere).
	if err := config.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}
	log.WithField("config", config.RedactedSummary()).Info("Loaded config")

	if err := db.Init(); err != nil {
		log.Fatalf("Main: failed to init database: %v\n", err)
	}

	// log.WithField("DHIS2_SERVER_CONFIGS", config.MFLDHIS2ServersConfigMap).Info("SERVER: =======>")
	err := db.RunMigrations()
	if err != nil {
		log.Fatalf("Failed to run database migrations %v\n", err)
	}

	// Source of truth: conf.d -> upsert into DB + refresh in-memory server maps.
	LoadServersFromConfigFiles(config.GetDHIS2ServersConfigMap())

	// Watch conf.d and re-apply on every change.
	if err := config.WatchConfD(ctx, config.ConfDDir, func(m map[string]config.ServerConf) {
		LoadServersFromConfigFiles(m)
	}); err != nil {
		log.WithError(err).Warn("conf.d watcher not started")
	}

	models.ReloadServersFromDB()

	mflClient := clients.NewMFLApi(
		config.MFLIntegratorConf.API.MFLBaseURL,
		clients.WithBasicAuth(
			config.MFLIntegratorConf.API.MFLUser,
			config.MFLIntegratorConf.API.MFLPassword,
		),
	)

	go func() {
		// Create a new scheduler
		s := gocron.NewScheduler(time.UTC)
		s.SetMaxConcurrentJobs(1, gocron.RescheduleMode)
		// Schedule the task to run "30 minutes after midn, 4am, 8am, 12pm..., everyday"
		// if the --mfl-sync-schedule-off flag is present, we ignore
		if !*config.MFLSyncScheduleOFF {
			log.WithFields(log.Fields{"SyncCronExpression": config.MFLIntegratorConf.API.SyncCronExpression}).Info(
				"Facility Synchronisation Cron Expression")
			_, err := s.Cron(config.MFLIntegratorConf.API.SyncCronExpression).SingletonMode().Do(func() {
				runObservedJob("facility_sync", FetchFacilitiesByDistrict)
			})
			// _, err := s.CronWithSeconds("* * * * * *").Do(task)
			if err != nil {
				log.WithError(err).Error("Error scheduling facility sync task:")
				return
			}
		}

		// retrying incomplete requests runs every 5 minutes
		log.WithFields(log.Fields{"RetryCronExpression": config.MFLIntegratorConf.API.RetryCronExpression}).Info(
			"Request Retry Cron Expression")
		if !*config.SkipRequestProcessing {
			_, err = s.Cron(config.MFLIntegratorConf.API.RetryCronExpression).SingletonMode().Do(func() {
				runObservedJob("retry_incomplete_requests", func() {
					RetryIncompleteRequests(ctx)
				})
			})
			if err != nil {
				log.WithError(err).Error("Error scheduling incomplete request retry task:")
			}
		}
		s.StartAsync()
	}()

	go func() {
		if !*config.SkipOUSync {
			log.Info("Starting OrgUnits sync from base DHIS2")
			LoadOuLevels()
			LoadOuGroups()
			LoadOuGroupSets()
			LoadAttributes()
			LoadLocations() // Load organisation units - before facility in base DHIS2 instance
			if config.MFLIntegratorConf.Server.SyncToDHIS2Instances {
				SyncLocationsToDHIS2Instances()
			} else {
				log.Info("Skipping OrgUnits sync to DHIS2 instances: --skip-dhis2-sync flag added")
			}
		} else {
			log.Warn("Skipping OrgUnits sync from base DHIS2: --skip-ou-sync flag added")
		}
		if !*config.SkipAdminUnitsMatching {
			log.Info("Performing admin unit matching!!!")
			MatchLocationsWithMFL2(ctx, mflClient)
			log.Info("Admin unit matching completed!!!")
		} else {
			log.Warn("Skipping admin unit matching! --skip-admin-units-matching flag added")
		}

		// fetch facilities after initial run, just use a scheduled job.
		if !*config.MFLSyncOFF {
			runObservedJob("initial_facility_sync", FetchFacilitiesByDistrict)
		} else {
			// log fetching facilities from MFL disabled
			log.Warn(".::::..Fetching facilities from MFL disabled..::::..")
		}

		//if config.MFLIntegratorConf.Server.SyncToDHIS2Instances {
		//	SyncLocationsToDHIS2Instances()
		//}

	}()

	jobs := make(chan int)
	var wg sync.WaitGroup

	seenMap := make(map[models.RequestID]bool)
	mutex := &sync.Mutex{}
	rWMutex := &sync.RWMutex{}

	// Add this: capture server errors and convert them into a graceful shutdown.
	serverErrCh := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			return
		case err := <-serverErrCh:
			if err != nil {
				log.WithError(err).Error("API server error; initiating shutdown")
				stop() // cancels ctx => producer/consumers/server all start shutting down
			}
		}
	}()

	if !*config.SkipRequestProcessing {
		// don't produce anything if skip processing is enabled

		// Start the producer goroutine
		wg.Add(1)
		go Produce(ctx, db.GetDB(), jobs, &wg, mutex, seenMap)

		// Start the consumer goroutine
		wg.Add(1)
		go StartConsumers(ctx, jobs, &wg, rWMutex, seenMap)
	}

	// Start the backend API gin server
	if !*config.DisableHTTPServer {
		wg.Add(1)
		go startAPIServer(ctx, &wg, serverErrCh)
	}

	wg.Wait()
	if err := db.Close(); err != nil {
		log.WithError(err).Warn("db close failed")
	}
	log.Info("shutdown complete!")
}

func startAPIServer(ctx context.Context, wg *sync.WaitGroup, serverErrCh chan<- error) {
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

		at := new(controllers.AttributeController)
		v2.GET("/syncAttributes/:server", at.SyncAttributes)

		ad := new(controllers.AdminController)
		v2.GET("/clearSyncLog/:district", ad.ClearSyncLog)
		v2.GET("/clearDistrictRequests/:district", ad.ClearRequestsByDistrict)
		v2.GET("/clearBatchRequests/:batch", ad.ClearRequestsByBatch)

	}
	// Handle error response when a route is not defined
	router.NoRoute(func(c *gin.Context) {
		c.String(404, "Page Not Found!")
	})

	httpServer := &http.Server{
		Addr:    ":" + fmt.Sprintf("%s", config.MFLIntegratorConf.Server.Port),
		Handler: router,
	}

	// Start the server in a goroutine
	errCh := make(chan error, 1)
	go func() {
		log.Infof("Starting API server on :%s...", config.MFLIntegratorConf.Server.Port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("Shutdown signal received. Shutting down MFL-Integrator server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Errorf("Error shutting down API server: %v", err)
		} else {
			log.Info("API server shut down gracefully.")
		}
	case err := <-errCh:
		// Was log.Fatalf(...) before; now: report and return so shutdown is graceful.
		select {
		case serverErrCh <- err:
		default:
			// don't block if nobody is listening
		}
		return
	}

	// _ = router.Run(":" + fmt.Sprintf("%s", config.MFLIntegratorConf.Server.Port))
}

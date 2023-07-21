package main

import (
	"encoding/json"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/controllers"
	"github.com/HISP-Uganda/mfl-integrator/utils"
	"github.com/buger/jsonparser"
	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	"github.com/samply/golang-fhir-models/fhir-models/fhir"
	log "github.com/sirupsen/logrus"
	"net/url"
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

	LoadOuLevels()
	LoadOuGroups()
	LoadLocations() // Load organisation units - before facility in base DHIS2 instance
	MatchLocationsWithMFL()

	FetchFacilities()

	jobs := make(chan int)
	var wg sync.WaitGroup

	// Start the producer goroutine
	wg.Add(1)
	go Produce(dbConn, jobs, &wg)

	// Start the consumer goroutine
	wg.Add(1)
	go StartConsumers(jobs, &wg)

	// Start the backend API gin server
	wg.Add(1)
	go startAPIServer(&wg)

	//fmt.Println("MFL Integrator v1")
	//baseURL := config.MFLIntegratorConf.API.MFLBaseURL
	//parameters := url.Values{}
	//parameters.Add("resource", "Location")
	//parameters.Add("type", "healthFacility")
	//parameters.Add("_count", "1")
	//parameters.Add("facilityLevelOfCare", "HC IV")
	//baseURL += "?" + parameters.Encode()
	//
	//body, _ := utils.GetRequest(baseURL)
	//
	//if body != nil {
	//	v, _, _, _ := jsonparser.Get(body, "entry")
	//	fmt.Printf("Entries: %s", v)
	//	var entries []LocationEntry
	//	err := json.Unmarshal(v, &entries)
	//	if err != nil {
	//		fmt.Println("Error unmarshaling response body:", err)
	//		return
	//	}
	//
	//	// fmt.Printf("Our Bundle: %v\n", *bundle.Meta.LastUpdated)
	//	fmt.Printf("Records Found: %v\n", len(entries))
	//	// :w
	//	extensions := make(map[string]any)
	//	for i := range entries {
	//		extensions = GetExtensions(entries[i].Resource.Extension)
	//
	//	}
	//}
	// fmt.Printf("Districts: %v\n", getDistricts())
	//districts := getDistricts()
	//for i := range districts {
	//	fmt.Println(districts[i]["name"])
	//}
	wg.Wait()
}

func getDistricts() []map[string]interface{} {
	baseURL := config.MFLIntegratorConf.API.MFLBaseURL
	params := url.Values{}
	params.Add("resource", "Location")
	params.Add("type", "Local Government")
	params.Add("_count", "200") // We have less than 200 districts
	baseURL += "?" + params.Encode()

	body, _ := utils.GetRequest(baseURL)
	// fmt.Printf("BODY:%v\n", string(body))
	// Read the response body
	var districtList []map[string]interface{}
	if body != nil {
		v, _, _, _ := jsonparser.Get(body, "entry")
		var entries []LocationEntry
		err := json.Unmarshal(v, &entries)
		if err != nil {
			fmt.Println("Error unmarshaling response body:", err)
			return nil
		}

		for i := range entries {
			district := make(map[string]interface{})
			district["id"] = *entries[i].Resource.Id
			district["name"] = *entries[i].Resource.Name
			district["parent"] = *entries[i].Resource.PartOf.Reference

			districtList = append(districtList, district)
		}
		return districtList
	}

	return nil
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
		v2.POST("/organisationUnit", ou.OrgUnit)

	}
	// Handle error response when a route is not defined
	router.NoRoute(func(c *gin.Context) {
		c.String(404, "Page Not Found!")
	})

	_ = router.Run(":" + fmt.Sprintf("%s", config.MFLIntegratorConf.Server.Port))
}

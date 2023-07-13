package main

import (
	"encoding/json"
	"fmt"
	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/HISP-Uganda/mfl-integrator/utils"
	"github.com/buger/jsonparser"
	"github.com/gcinnovate/integrator/controllers"
	"github.com/gin-gonic/gin"
	"github.com/samply/golang-fhir-models/fhir-models/fhir"
	log "github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
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

func main() {
	fmt.Println("MFL Integrator v1")
	baseURL := config.MFLIntegratorConf.API.MFLBaseURL
	parameters := url.Values{}
	parameters.Add("count", "1")
	parameters.Add("levelOfCare", "HC IV")
	baseURL += "/search?" + parameters.Encode()

	resp, _ := utils.GetRequest(baseURL, "postman", "password")

	// fmt.Printf("%v", resp)
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return
	}
	if resp.StatusCode/100 == 2 {
		v, _, _, _ := jsonparser.Get(body, "data", "entry")
		fmt.Printf("DATA: %s", v)
		var entries []LocationEntry
		err = json.Unmarshal(v, &entries)
		if err != nil {
			fmt.Println("Error unmarshaling response body:", err)
			return
		}
		// fmt.Printf("Our Bundle: %v\n", *bundle.Meta.LastUpdated)
		fmt.Printf("Records Found: %v\n", len(entries))
		// :w
		extensions := make(map[string]interface{})
		for i := range entries {
			for e := range entries[i].Resource.Extension {
				if entries[i].Resource.Extension[e].ValueCode != nil {
					fmt.Printf("%v\n", *entries[i].Resource.Extension[e].ValueCode)
					extensions[entries[i].Resource.Extension[e].Url] = *entries[i].Resource.Extension[e].ValueCode
				}
				if entries[i].Resource.Extension[e].ValueString != nil {
					fmt.Printf("%v\n", *entries[i].Resource.Extension[e].ValueString)
					extensions[entries[i].Resource.Extension[e].Url] = *entries[i].Resource.Extension[e].ValueString

				}
				if entries[i].Resource.Extension[e].ValueInteger != nil {
					fmt.Printf("%v\n", *entries[i].Resource.Extension[e].ValueInteger)
					extensions[entries[i].Resource.Extension[e].Url] = fmt.Sprintf(
						"%d", *entries[i].Resource.Extension[e].ValueInteger)

				}

			}

			fmt.Printf("Entry: %s\n", extensions)
			fmt.Printf("Entry: %s\n", *entries[i].Resource.Name)
			fmt.Printf("Parent Reference: %s\n", *entries[i].Resource.PartOf.Reference)
			fmt.Printf("Parent DisplayName: %s\n", *entries[i].Resource.PartOf.Display)
		}
	}
	// fmt.Printf("Body: %s\n", body)
}

func startAPIServer() {
	// defer wg.Done()
	router := gin.Default()
	// done := make(chan bool)
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

	}
	// Handle error response when a route is not defined
	router.NoRoute(func(c *gin.Context) {
		c.String(404, "Page Not Found!")
	})

	router.Run(":" + fmt.Sprintf("%d", config.MFLIntegratorConf.Server.Port))
}

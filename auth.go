package main

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/gin-gonic/gin"
)

func BasicAuth() gin.HandlerFunc {

	return func(c *gin.Context) {
		c.Set("dbConn", db.GetDB())
		auth := strings.SplitN(c.Request.Header.Get("Authorization"), " ", 2)

		if len(auth) != 2 || !strings.EqualFold(auth[0], "Basic") {
			respondWithBasicAuthChallenge(c)
			return
		}
		payload, err := base64.StdEncoding.DecodeString(auth[1])
		if err != nil {
			respondWithBasicAuthChallenge(c)
			return
		}
		pair := strings.SplitN(string(payload), ":", 2)

		if len(pair) != 2 || !AuthenticateUser(pair[0], pair[1]) {
			respondWithBasicAuthChallenge(c)
			return
		}

		c.Next()
	}
}

func respondWithBasicAuthChallenge(c *gin.Context) {
	c.Header("WWW-Authenticate", `Basic realm="mfl-integrator"`)
	RespondWithError(http.StatusUnauthorized, "Unauthorized", c)
}

func AuthenticateUser(username, password string) bool {
	// log.Printf("Username:%s, password:%s", username, password)
	userObj := models.User{}
	err := db.GetDB().QueryRowx(
		`SELECT
                        id, username, firstname, lastname , telephone, email
                FROM users
                WHERE
                        username = $1 AND password = crypt($2, password)`,
		username, password).StructScan(&userObj)
	if err != nil {
		// fmt.Printf("User:[%v]", err)
		return false
	}
	// fmt.Printf("User:[%v]", userObj)
	return true
}

func RespondWithError(code int, message string, c *gin.Context) {
	resp := map[string]string{"error": message}

	c.JSON(code, resp)
	c.Abort()
}

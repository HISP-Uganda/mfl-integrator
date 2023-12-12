package main

import (
	"encoding/base64"
	"github.com/HISP-Uganda/mfl-integrator/db"
	"github.com/HISP-Uganda/mfl-integrator/models"
	"github.com/gin-gonic/gin"
	"strings"
)

func BasicAuth() gin.HandlerFunc {

	return func(c *gin.Context) {
		c.Set("dbConn", db.GetDB())
		auth := strings.SplitN(c.Request.Header.Get("Authorization"), " ", 2)

		if len(auth) != 2 || auth[0] != "Basic" {
			RespondWithError(401, "Unauthorized", c)
			return
		}
		payload, _ := base64.StdEncoding.DecodeString(auth[1])
		pair := strings.SplitN(string(payload), ":", 2)

		if len(pair) != 2 || !AuthenticateUser(pair[0], pair[1]) {
			RespondWithError(401, "Unauthorized", c)
			// c.Writer.Header().Set("WWW-Authenticate", "Basic realm=Restricted")
			return
		}

		c.Next()
	}
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

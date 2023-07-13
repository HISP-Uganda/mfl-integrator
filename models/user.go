package models

import (
	"time"
)

// User is our user object
type User struct {
	ID           int64     `db:"id"`
	UID          string    `db:"uid" json:"uid"`
	Username     string    `db:"username"`
	Password     string    `db:"password"`
	FirstName    string    `json:"firstname" db:"firstname"`
	LastName     string    `json:"lastname" db:"lastname"`
	Email        string    `json:"email" db:"email"`
	Phone        string    `json:"telephone" db:"telephone"`
	IsActive     bool      `json:"is_active" db:"is_active"`
	IsSystemUser bool      `json:"is_system_user" db:"is_system_user"`
	Created      time.Time `json:"created" db:"created"`
	Updated      time.Time `json:"updated" db:"updated"`
}

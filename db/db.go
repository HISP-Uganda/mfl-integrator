package db

import (
	"embed"
	"errors"
	"fmt"

	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	log "github.com/sirupsen/logrus"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //import postgres
)

var db *sqlx.DB

func init() {
	psqlInfo := config.MFLIntegratorConf.Database.URI
	//
	var err error
	db, err = ConnectDB(psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
}

// ConnectDB ...
func ConnectDB(dataSourceName string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", dataSourceName)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}
	return db, nil
}

// GetDB ...
func GetDB() *sqlx.DB {
	return db
}

//go:embed migrations/*.sql
var migrationsFS embed.FS

func RunMigrations() error {
	log.Infof("Running embedded migrations")

	// Base path inside the embedded FS must match the directory prefix used in go:embed
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("iofs source: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, config.MFLIntegratorConf.Database.URI)
	if err != nil {
		return fmt.Errorf("new migrate: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}

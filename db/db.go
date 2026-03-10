package db

import (
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/HISP-Uganda/mfl-integrator/config"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	log "github.com/sirupsen/logrus"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //import postgres
)

var DB *sqlx.DB

// Init initializes the global DB after config is loaded.
func Init() error {
	if config.MFLIntegratorConf.Database.URI == "" {
		return fmt.Errorf("database uri is empty (config not loaded?)")
	}

	dsn := config.MFLIntegratorConf.Database.URI

	conn, err := ConnectDB(dsn)
	if err != nil {
		return err
	}

	DB = conn
	return nil
}

// ConnectDB ...
func ConnectDB(dataSourceName string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(config.MFLIntegratorConf.Database.MaxOpenConns)
	db.SetMaxIdleConns(config.MFLIntegratorConf.Database.MaxIdleConns)

	if lifetime := config.MFLIntegratorConf.Database.ConnMaxLifetime; lifetime != "" {
		d, err := time.ParseDuration(lifetime)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("parse database.conn_max_lifetime: %w", err)
		}
		db.SetConnMaxLifetime(d)
	}
	if idleTime := config.MFLIntegratorConf.Database.ConnMaxIdleTime; idleTime != "" {
		d, err := time.ParseDuration(idleTime)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("parse database.conn_max_idle_time: %w", err)
		}
		db.SetConnMaxIdleTime(d)
	}

	return db, nil
}

func Close() error {
	if DB == nil {
		return nil
	}
	return DB.Close()
}

// GetDB ...
func GetDB() *sqlx.DB {
	return DB
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

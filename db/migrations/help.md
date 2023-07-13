# Migrations
## Create and configure database
```bash

export POSTGRESQL_URL='postgres://postgres:password@localhost:5432/mfldb?sslmode=disable'
    
psql -h localhost -U postgres -w -c "create database mfldb;"
```
## Create migrations

```bash
migrate create -ext sql -dir db/migrations -seq initial
```

If there were no errors, we should have two files available under db/migrations folder:

- 000001_initial.down.sql

- 000001_initial.up.sql

## Run migrations
```bash 
migrate -database ${POSTGRESQL_URL} -path db/migrations up
```
### reverse a single migration
```bash 
migrate -database ${POSTGRESQL_URL} -path db/migrations down 1
```

For more see [PostgreSQL Migration Tutorial](https://github.com/golang-migrate/migrate/blob/master/database/postgres/TUTORIAL.md)
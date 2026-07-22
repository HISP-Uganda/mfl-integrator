VERSION ?= 1.0.7
BUILD_NUMBER ?= local
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS = -X github.com/HISP-Uganda/mfl-integrator/buildinfo.Version=$(VERSION) -X github.com/HISP-Uganda/mfl-integrator/buildinfo.BuildNumber=$(BUILD_NUMBER) -X github.com/HISP-Uganda/mfl-integrator/buildinfo.Commit=$(COMMIT) -X github.com/HISP-Uganda/mfl-integrator/buildinfo.BuildDate=$(BUILD_DATE)

all:
	go build -ldflags "$(LDFLAGS)" && ./mfl-integrator --config-file /etc/mflintegrator/mfld.yml

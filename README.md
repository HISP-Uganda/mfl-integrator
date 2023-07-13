# The DHIS2 - MoH Master Facility List Integrator

# Introduction
As the MoH Uganda switches from using DHIS2 as its Facility Registry, a Master Facility List (**MFL**) was created for the same purpose.
Health systems working with public health information are thus required to pick health facilities from the MFL.

The purpose of this application is help with the synchronisation of health facilities from the MFL to various DHIS2 instances.
The `mfl-integrator` application  compiles to a binary and that binary along with the config file `mfld.yml` is all
you need to run it on your server.

# Configuration
mfl-integrator uses a tiered configuration system, each option takes precendence over the ones above it:
1. The configuration file. The default path is `/etc/mflintegrator/mfld.yml`
2. Environment variables starting with `MFLINTEGRATOR_`
3. Command line parameters

We recommend running mfl-integrator with no changes to the configuration and no parameters, using only
environment variables to configure it. You can use `% mfl-integrator --help` to see a list of the
environment variables and parameters and for more details on each option.

# Deployment
Install mfl-integrator source in your workspace with:
```
go get github.com/HISP-Uganda/mfl-integrator
```

Build mfl-integrator with:

```
go install github.com/HISP-Uganda/mfl-integrator
```

This will create a new executable in $GOPATH/bin called `mfl-integrator`. 
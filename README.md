# The DHIS2 - MoH Master Facility List Integrator

# Introduction
As the MoH Uganda switches from using DHIS2 as its Facility Registry, a Master Facility List (**MFL**) was created for the same purpose.
Health systems working with public health information are thus required to pick health facilities from the MFL.

The purpose of this application is help with the synchronisation of health facilities from the MFL to various DHIS2 instances.
The `mfl-integrator` application  compiles to a binary and that binary along with the config file `mfld.yml` is all
you need to run it on your server.

For more detailed documentation, please visit [https://wiki.hispuganda.org/en/mfl/index](https://wiki.hispuganda.org/en/internship/index) 

# Stable Versions
- **Linux:** [mfl-integrator_1.0.2_linux_amd64.tar.gz](https://github.com/HISP-Uganda/mfl-integrator/releases/download/v1.0.2/mfl-integrator_1.0.2_linux_amd64.tar.gz)
- **Debian Package:** [mflintegrator_1.0.2_amd64.deb](https://github.com/HISP-Uganda/mfl-integrator/releases/download/v1.0.2/mflintegrator_1.0.2_amd64.deb)
- **Mac-OS Darwin:** [mfl-integrator_1.0.2_darwin_amd64.tar.gz](https://github.com/HISP-Uganda/mfl-integrator/releases/download/v1.0.2/mfl-integrator_1.0.2_darwin_amd64.tar.gz)
- **Windows:** [mfl-integrator_1.0.2_windows_amd64.tar.gz](https://github.com/HISP-Uganda/mfl-integrator/releases/download/v1.0.2/mfl-integrator_1.0.2_windows_amd64.tar.gz)

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
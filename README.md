# art-tools

Collection of Release tools for managing OCP releases

- [Doozer](./doozer) - cli tool for managing builds (and more)
- [Elliott](./elliott) - cli tool for managing release advisories and bugs (and more)
- [pyartcd](./pyartcd) - code for release pipelines
- [ocp-build-data-validator](./ocp-build-data-validator) - schema validator for [ocp-build-data](https://github.com/openshift/ocp-build-data)
- [artcommon](./artcommon) - common package used by Doozer, Elliott, pyartcd

## Setup

- Requires Python >= 3.11
- `git clone https://github.com/openshift-eng/art-tools.git`
- You will need to install local package dependencies, redhat certificates and configure kerberos for authentication before we get to installing python dependencies. See [Containerfile](./.devcontainer/Containerfile) for details
- `make venv`

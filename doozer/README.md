## Doozer

[![PyPI version](https://badge.fury.io/py/rh-doozer.svg)](https://badge.fury.io/py/rh-doozer)
[![codecov](https://codecov.io/gh/openshift/doozer/branch/master/graph/badge.svg)](https://codecov.io/gh/openshift/doozer)

Doozer is a build management utility that currently has the capability to build RPMs and Container Images via OSBS/Brew

## Deployment

_**Note**: This is for running the full, local python `doozer` client, not the containerized version. This requires other dependencies discussed in the [Installation doc](Installation.md)._

For local development pull the code and run:

`pip3 install --user -e .`

For new releases, Jenkins is already setup and deployment to PyPi is easy:

- Bump the version in `./doozerlib/VERSION`
- Push the change to `master`
- Create a new GitHub release: https://github.com/openshift-eng/doozer/releases/new

That's it. Jenkins will do the rest automatically.


## Installation

If you just want to use `doozer` checkout the [container usage doc](Container.md).

If you need to develop for doozer checkout the full [installation doc](Installation.md).

## License

Most of doozer is released under [Apache License 2.0][], except [doozerlib/dotconfig.py][] and
[doozerlib/gitdata.py][], which are embedded copies of [dotconfig][] and [gitdata][] projects
respectively, therefore those two files are released under [LGPL v3][].

## Usage

Checkout the [Usage doc](Usage.md)

### Build-time Konflux verification eligibility

An image's ocp-build-data YAML can override whether build-time Enterprise
Contract (EC) verification and configured custom IntegrationTestScenarios run.
For example, this enables them without making the image eligible for a release:

```yaml
for_release: false
konflux:
  its:
    enabled: true
```

When `konflux.its.enabled` is absent, `for_release` determines eligibility.
Setting it to `true` enables both checks even with `for_release: false`; setting
it to `false` disables both checks even with `for_release: true`. This is an
image-level setting and is not inherited from group configuration. Both checks
still require a successful build and a definitive digest pullspec. EC still
requires a policy for the current lifecycle
and a non-OKD variant, and uses the existing policy and effective time. Custom ITS
still require configured scenarios. `--skip-ec-verify` and `--skip-custom-its`
take precedence independently. EC or blocking custom ITS failures still fail the
build with `ITS_ERROR`; optional custom scenarios retain their existing behavior.

This option does not affect Elliott
release/advisory discovery, payload selection, assembly selection, base-only
invariants, delivery configuration requirements, or task-bundle rebuild scans.

[Apache License 2.0]: https://github.com/openshift-eng/doozer/blob/master/LICENSE
[doozerlib/dotconfig.py]: https://github.com/openshift-eng/doozer/blob/master/doozerlib/dotconfig.py
[doozerlib/gitdata.py]: https://github.com/openshift-eng/doozer/blob/master/doozerlib/gitdata.py
[dotconfig]: https://github.com/adammhaile/dotconfig
[gitdata]: https://github.com/adammhaile/gitdata
[LGPL v3]: https://www.gnu.org/licenses/lgpl-3.0.en.html

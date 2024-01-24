
# Using our validator image

## Building manually

You can build this locally in two steps:

1. The first step builds a base image with all the dependencies (takes several minutes).

        podman build . -f deploy/Dockerfile.base -t validator:base

2.  The second step is very brief and simply updates to the latest validator code.

        podman build . -f deploy/Dockerfile.update -t validator:latest

## Deploying

Builds are deployed in our cluster under the art-tools project as follows:

    oc create -f deploy/secrets.yaml   # but you will want to update/recreate these secrets with real values
    oc create -f deploy/build.base.yaml
    oc create -f deploy/build.update.yaml

This is currently configured to push to the
[image repo](https://quay.io/repository/openshift-art/art-ci-toolkit)
that's used in our validator automation.

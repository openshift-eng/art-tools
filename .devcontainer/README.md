# Art Tools Development Container Support for VSCode

This directory contains the `Containerfile` and `devcontainer.json` file
that allows you to develop and debug `art-tools` inside a development container
using Visual Studio Code. See [vs code dev containers](https://code.visualstudio.com/docs/devcontainers/containers) for more information.

## Quick Start

1. `art-tools` needs to access internal resources, make sure you are connected to the redhat vpn.
2. `art-tools` needs access resources from quay.io and registry.ci.openshift.org, it achieves this by using your local docker configuration.
Ensure you are [logged in to quay.io](https://answers.redhat.com/cloud-services/discussion/346/how-do-i-access-quay-io-openshift-release-dev-ocp-v4-0-art-dev) and registry.ci.openshift.org before proceeding to start up the container.
3. Install the [Remote Development Extension Pack](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack) on Visual Studio Code.
4. Open `art-tools` project locally.
5. If you are using Linux, make sure the `USER_UID` `USER_GID` arguments in `Containerfile` match your actual UID and GID. Ignore this step if you are using macOS or Windows.
6. Change `user` in doozer.settings.yaml and in elliott.settings.yaml to your kerberos user.
7. Click the green icon on the bottom left of the VSCode window or press <kbd>F1</kbd>, then choose `Remote-Containers: Reopen in Container`.

## Development container use with podman

The same `Containerfile` can be used independently to provide an art-tools environment container.
A build with podman may look like:

```bash
    podman build --build-arg USERNAME=yours --build-arg USER_UID=1234 \
                 -f .devcontainer/Containerfile -t local/art-tools .
```

Then a script similar to the following (you will certainly want your own modifications)
will run the container, mounting in relevant things from your own user directory to be
accessible to the same user inside the container.

```bash
    #!/bin/bash

    USER=yours
    # location of art-tools checkout
    ART_TOOLS="$HOME/art-tools"
    CONTAINER="$ART_TOOLS/.devcontainer"

    # mounting in your .ssh dir changes selinux labels, preventing sshd from logging
    # your user in remotely; make a copy and mount that instead if needed.
    rm -rf $HOME/.ssh_art_tools
    cp -a $HOME/.ssh{,_art_tools}

    # you'll likely have to modify uidmap according to your own user's uid range.
    # for 1234 below of course substitute your own UID.
    podman run -it --rm \
        --uidmap 0:10000:1000 --uidmap=1234:0:1 \
        -v $ART_TOOLS:/workspaces/art-tools:cached,z \
        -v $HOME/.ssh_art_tools:/home/$USER/.ssh:ro,cached,z \
        -v $HOME/.docker:/home/$USER/.docker:ro,cached,z \
        -v $HOME/.config/python-bugzilla:/home/$USER/.config/python-bugzilla:ro,cached,z \
        -v $CONTAINER/doozer.settings.yaml:/home/$USER/.config/doozer/settings.yaml:ro,cached,z \
        -v $CONTAINER/elliott.settings.yaml:/home/$USER/.config/elliott/settings.yaml:ro,cached,z \
        -v $CONTAINER/brewkoji.conf:/etc/koji.conf.d/brewkoji.conf:ro,cached,z \
        -v $ART_TOOLS/artcommon/configs/krb5-redhat.conf:/etc/krb5.conf.d/krb5-redhat.conf:ro,cached,z \
        local/art-tools
```

# art-tools Development Container Support

This directory contains the `Containerfile` and supporting files to enable you
to develop and debug `art-tools` inside a development container.

A build with podman may look like:

    podman build --build-arg USERNAME=yours --build-arg USER_UID=1234 \
                 -f .devcontainer/Containerfile -t local/art-tools .

Then a script similar to the following (you will certainly want your own modifications)
will run the container, mounting in relevant things from your own user directory to be
accessible to the same user inside the container.

#!/bin/bash

    USER=yours
    USER_ID=1234 (yours)
    # location of art-tools checkout
    TOOLS="$HOME/openshift/art-tools"
    CONTAINER="$TOOLS/.devcontainer"

    # if you've configured kerberos on your host to write a file keytab, you
    # can mount it in to use; otherwise you'll need to kinit in each new container.
    # make a copy of your kerberos credentials to mount in (if you mount in the
    # original from your host, the selinux labels are changed and host kerberos
    # stops updating it).
    cp -a "${KRB5CCNAME#FILE:}"{,_arttools}

    # mounting in your .ssh dir changes selinux labels, preventing sshd from logging
    # your user in remotely; make a copy and mount that instead if needed.
    rm -rf $HOME/.ssh_arttools
    cp -a $HOME/.ssh{,_arttools}

    # to use elliott with Jira you'll want to configure a JIRA_TOKEN env var
    source ~/.config/jira/token.sh

    # you'll likely have to modify uidmap according to your own user's uid range.
    podman run -it --rm \
        --uidmap 0:10000:1000 --uidmap=$USER_ID:0:1 \
        -v "${KRB5CCNAME#FILE:}_arttools":/tmp/krb5cc_$USER_ID:ro,z \
        -v $TOOLS:/workspaces/art-tools:cached,z \
        -v $HOME/.ssh_arttools:/home/$USER/.ssh:ro,cached,z \
        -v $HOME/.gitconfig:/home/$USER/.gitconfig:ro,cached,z \
        -v $HOME/.config/python-bugzilla:/home/$USER/.config/python-bugzilla:ro,cached,z \
        -e JIRA_TOKEN=$JIRA_TOKEN \
        -v $CONTAINER/doozer.settings.yaml:/home/$USER/.config/doozer/settings.yaml:ro,cached,z \
        -v $CONTAINER/elliott.settings.yaml:/home/$USER/.config/elliott/settings.yaml:ro,cached,z \
        -v $CONTAINER/krb5-redhat.conf:/etc/krb5.conf.d/krb5-redhat.conf:ro,cached,z \
        -v $CONTAINER/brewkoji.conf:/etc/koji.conf.d/brewkoji.conf:ro,cached,z \
        art-tools:latest

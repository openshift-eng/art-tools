#!/bin/bash

EL_VERSION=$(grep -E '^PLATFORM_ID=' /etc/os-release | sed 's/[^0-9]*//g')
case $EL_VERSION in
    7)
        EL_SUFFIX=""
        ;;
    *)
        EL_SUFFIX="-rhel${EL_VERSION}"
        ;;
esac

ARCH=$(rpm --eval '%{_arch}')
case $ARCH in
    x86_64)
        ARCH_SUFFIX=""
        ;;
    s390x)
        ARCH_SUFFIX="-s390x"
        ;;
    ppc64le)
        ARCH_SUFFIX="-ppc64le"
        ;;
    aarch64)
        ARCH_SUFFIX="-arm64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

if curl "http://base-${MAJOR}-${MINOR}-${EL_SUFFIX}.ocp${ARCH_SUFFIX}.svc" > /etc/yum.repos.d/art.repo ; then
	echo "Injected ART repos"
	cat /etc/yum.repos.d/art.repo
else
	echo "Unable to inject ART CI repo mirror yum configuration. This is expected if you are building locally. If" \
	"this is a CI triggered build, contact @release-artists in #forum-ocp-art ."
fi

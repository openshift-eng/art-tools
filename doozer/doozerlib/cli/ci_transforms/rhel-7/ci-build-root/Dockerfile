FROM replaced-by-buildconfig
# Serves as a standard build environment for OpenShift builds. It is ultimately based on the
# ART golang builder and has packages layered on top of support CI only use cases
# (e.g. compiling test cases) that simply don't occur downstream.
# Used as a template for 'images:streams gen-buildconfigs'

ENV OPENSHIFT_CI=true

# Install common CI tools and epel for packages like tito.
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && \
    INSTALL_PKGS="bc bind-utils bsdtar bzr ceph-common createrepo device-mapper device-mapper-persistent-data e2fsprogs ethtool file findutils gcc git glibc-static glib2-devel gpgme gpgme-devel hostname iptables jq krb5-devel libassuan libassuan-devel libseccomp-devel libvirt-devel lsof make mercurial nmap-ncat openssl protobuf-compiler protobuf-devel rsync socat systemd-devel sysvinit-tools tar tito tree util-linux wget which xfsprogs zip goversioninfo gettext" && \
    yum install -y $INSTALL_PKGS && \
    rpm -V $INSTALL_PKGS && \
    yum clean all && \
    touch /os-build-image && \
    git config --system user.name origin-release-container && \
    git config --system user.email origin-release@redhat.com

# Install common go tools upstream devs are expecting in CI.
# Versions are pinned to the last release whose go.mod minimum is <= 1.21
# to stay compatible with Go 1.21+ (OCP 4.16).
# Removed deprecated/archived tools: glide, godep, golint, dep.
# Note: `go tool cover` is built into Go since 1.2; the x/tools/cmd/cover
# package was removed from x/tools long ago, so no explicit install needed.
RUN GOFLAGS='' go install golang.org/x/tools/cmd/goimports@v0.24.0 && \
    GOFLAGS='' go install gotest.tools/gotestsum@v1.12.2 && \
    GOFLAGS='' go install github.com/openshift/imagebuilder/cmd/imagebuilder@v1.2.15 && \
    git clone --depth=1 --filter=blob:none --sparse https://github.com/openshift/release.git /tmp/openshift-release && \
    cd /tmp/openshift-release && git sparse-checkout set tools/gotest2junit && \
    cd /tmp/openshift-release/tools/gotest2junit && \
    go mod init github.com/openshift/release/tools/gotest2junit && \
    go build -o $GOPATH/bin/gotest2junit . && \
    cd / && rm -rf /tmp/openshift-release && \
    mv $GOPATH/bin/* /usr/bin/ && \
    rm -rf $GOPATH/* $GOPATH/.cache && \
    mkdir $GOPATH/bin && \
    mkdir -p /go/src/github.com/openshift/origin && \
    ln -s /usr/bin/imagebuilder $GOPATH/bin/imagebuilder && \
    ln -s /usr/bin/goimports $GOPATH/bin/goimports

RUN chmod g+xw -R $GOPATH && \
    chmod g+xw -R $(go env GOROOT)

# Some image building tools don't create a missing WORKDIR
RUN mkdir -p /go/src/github.com/openshift/origin
WORKDIR /go/src/github.com/openshift/origin

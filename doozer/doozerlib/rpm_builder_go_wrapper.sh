export GOEXPERIMENT=strictfipsruntime
export GOTOOLCHAIN=local

LOG_PREFIX="Go compliance shim [${__doozer_group}][${__doozer_key}]:"
echoerr() {
  echo -n "${LOG_PREFIX} " 1>&2
  cat <<< "$@" 1>&2
}

# Neutralise the no_openssl build tag, which would otherwise disable the
# openssl crypto backend that FORCE_OPENSSL exists to mandate. Operates in
# place on the caller's "arg" variable rather than returning the new value,
# so that both tag spellings can share it.
scrub_no_openssl() {
  pre_arg="${arg}"
  arg=$(echo "${arg}" | sed 's/no_openssl/shim_prevented_no_openssl/g')
  if [[ "${pre_arg}" != "${arg}" ]]; then
    echoerr "non-compliant: eliminated no_openssl"
  fi
}

set -o xtrace

echo 1>&2
echo 1>&2
echo -n "${LOG_PREFIX} incoming command line arguments: " 1>&2
for arg in "$@"; do
  echo -n "\"${arg}\" " 1>&2
done
echo 1>&2
echo 1>&2

echoerr "----"
echoerr "incoming environment: "
echoerr "---------------------"
env 1>&2
echoerr "---------------------"
echo 1>&2

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd ) # Get the directory in which this script is executing

# Work around the golang-1.26.7-2 toolchain bug where GOFIPS140 defaults to
# 'certified' rather than being unset, so that the version probe below runs
# against a toolchain that has not been asked for FIPS mode by accident.
# GOFIPS140 is set for real at the end of the script, once the version is
# known, so this only governs the probe.
export GOFIPS140="${GOFIPS140:-off}"

# Go 1.26 ships a built-in, FIPS 140-3 validated cryptographic module,
# selected with GOFIPS140. It is pure Go, so none of the workarounds needed
# to steer earlier toolchains onto the openssl backend apply. OCP 5.0 and 5.1
# both get a golang 1.26 in their buildroot, but the built-in module is only
# wanted from 5.1 onwards, so on 1.26 specifically it takes corroboration
# from __doozer_group to qualify. golang 1.27+ always qualifies on its own.
#
# This mirrors the same check in golang_builder_FIPS_wrapper.sh, which does
# the equivalent job for container builds; the two are expected to agree.
GO_BUILTIN_FIPS="0"
FOUND_GO_VERSION="$(${SCRIPT_DIR}/go.real env GOVERSION)"
GO_VER="${FOUND_GO_VERSION#go}"   # "go1.26.0" -> "1.26.0"
GO_MAJOR="${GO_VER%%.*}"          #            -> "1"
GO_REST="${GO_VER#*.}"            #            -> "26.0"
GO_MINOR="${GO_REST%%.*}"         #            -> "26"
# GOVERSION can carry a suffix (e.g. "go1.24.6 X:strictfipsruntime") which the
# parsing above tolerates, or be a devel string with no version at all. Only
# trust the result when both fields came out as numbers; anything else falls
# back to the openssl behaviour, which is the safe default.
if [[ "${GO_MAJOR}" =~ ^[0-9]+$ && "${GO_MINOR}" =~ ^[0-9]+$ ]]; then
  if [[ "${GO_MAJOR}" -gt 1 ]] || [[ "${GO_MAJOR}" -eq 1 && "${GO_MINOR}" -ge 27 ]]; then
    GO_BUILTIN_FIPS="1"
  elif [[ "${GO_MAJOR}" -eq 1 && "${GO_MINOR}" -eq 26 ]]; then
    # __doozer_group is baked into this script by doozer, and holds the OCP
    # group being built, e.g. "openshift-5.1". Any group that doesn't match
    # that shape is treated conservatively as pre-5.1.
    if [[ "${__doozer_group}" =~ ^openshift-([0-9]+)\.([0-9]+)$ ]]; then
      OCP_MAJOR="${BASH_REMATCH[1]}"
      OCP_MINOR="${BASH_REMATCH[2]}"
      if [[ "${OCP_MAJOR}" -gt 5 ]] || [[ "${OCP_MAJOR}" -eq 5 && "${OCP_MINOR}" -ge 1 ]]; then
        GO_BUILTIN_FIPS="1"
      fi
    fi
  fi
fi
echoerr "GOVERSION is ${FOUND_GO_VERSION}, GO_BUILTIN_FIPS is ${GO_BUILTIN_FIPS}"

if [[ "${CGO_ENABLED}" == "0" && "${GO_BUILTIN_FIPS}" == "0" ]]; then
    echoerr "non-compliant RPM attempted to use CGO_ENABLED=${CGO_ENABLED}"
fi

# The built-in module is pure Go, so cgo is not needed to reach a compliant
# backend and the RPM's own choice stands.
if [[ "${GO_BUILTIN_FIPS}" == "1" ]]; then
    echoerr "permitting CGO_ENABLED=${CGO_ENABLED} because the built-in FIPS module needs no cgo"
elif [[ "${GOOS}" == darwin || "${GOOS}" == windows ]]; then
    echoerr "permitting CGO_ENABLED=${CGO_ENABLED} because of GOOS=${GOOS}"
else
    export CGO_ENABLED="1"
fi
echoerr "GOOS is $GOOS, CGO_ENABLED is ${CGO_ENABLED}"

HAS_TAGS=0
if cat <<< "$@" | grep "\-tags" > /dev/null; then
  HAS_TAGS="1"
fi

FORCE_FOD_MODE=1
FORCE_OPENSSL=1
# Forcing cgo on is what makes static linking fail, so with cgo left alone
# there is nothing to justify rewriting the RPM's -extldflags either.
if [[ "${GO_BUILTIN_FIPS}" == "1" ]]; then
  FORCE_DYNAMIC=0
else
  FORCE_DYNAMIC=1
fi

# Build tags the shim injects, as a single comma-delimited list so that the
# three places which splice tags into the command line do not each have to
# know what the list is made of. These are orthogonal concerns:
# strictfipsruntime makes a binary die at startup on a FIPS host if it was
# not built against a compliant backend, whatever that backend is;
# no_openssl selects which backend.
SHIM_TAGS=""
if [[ "${FORCE_FOD_MODE}" == "1" ]]; then
  SHIM_TAGS="strictfipsruntime"
fi
if [[ "${GO_BUILTIN_FIPS}" == "1" ]]; then
  # Companion of GOFIPS140 below; together they select the built-in module.
  SHIM_TAGS="${SHIM_TAGS:+${SHIM_TAGS},}no_openssl"
fi

IN_RUN=0
IN_BUILD=0
IN_TAGS=0
COVERAGE_ADDED=0  # Track whether coverage_server.go has been added to the args
ARGS=()  # We need to rebuild the argument list.
# Compilation with -extldflags "-static" is problematic with
# CGO_ENABLED=1 because compilation tries to link against
# static libraries which don't exist. Remove -static flag
# when detected. This is tricky because extldflags can be simple
# or something like -ldflags '-X $(REPO_PATH)/pkg/version.Raw=$(VERSION) -extldflags "-lm -lstdc++ -static"'
ARGS=()  # We need to rebuild the argument list.
for arg in "$@"; do
    if [[ "${arg}" == "run" ]]; then
      IN_RUN="1"
    fi

    # prior to detecting 'IN_RUN', grafana failed because it ran
    # 'go run build.go build' which caused the script to exec 'go run build.go build -tags strictfipsruntime'
    # 'go install ...' can also trigger builds (e.g. Kubernetes uses `go install` instead of `go build`).
    if [[ ( "${arg}" == "build" || "${arg}" == "install" ) && "${IN_RUN}" == "0" ]]; then
      # -tags apparently cannot come after a build path
      # e.g. "build ./cmd/cluster-openshift-apiserver-operator -tags strictfipsruntime" is invalid.
      # So, if we see "build" and no "-tags" ahead, then go ahead and force FOD tag.

      IN_BUILD="1"  # This is a go build invocation
      ARGS+=("${arg}") # Add "build" or "install"

      if [[ "${GO_COMPLIANCE_COVER:-}" == "1" ]]; then
        echoerr "adding -cover for build or install operation"
        ARGS+=("-cover")
        ARGS+=("-covermode=atomic")
      fi

      if [[ -n "${SHIM_TAGS}" && "${HAS_TAGS}" == "0" ]]; then
        ARGS+=("-tags")
        ARGS+=("${SHIM_TAGS}")
      fi
      continue  # We've already added 'build' or 'install', so don't reach the bottom of the loop where it would be added again.
    fi

    if [[ ( "${arg}" == "-tags="* || "${arg}" == "--tags="* ) && -n "${SHIM_TAGS}" ]]; then
      echoerr "adding ${SHIM_TAGS} tags to \"${arg}\""
      arg=$(echo "${arg}" | tr -d "'" | tr -d "\"")  # Delete any quotes which get passed in literally. grafana managed this.
      if [[ "${arg}" == *" "* ]]; then  # If the tags parameter is space delimited
        arg="${arg} ${SHIM_TAGS//,/ }"
      else
        # Assume comma delimited
        arg="${arg},${SHIM_TAGS}"
      fi
    fi

    # A tag list reaches us in one of two spellings: attached to the flag as
    # "-tags=<list>" (handled here), or as the argument following a bare
    # "-tags" (handled by the IN_TAGS block below). Both have to be scrubbed,
    # and the scrub is independent of whether we are also adding tags of our
    # own.
    if [[ ( "${arg}" == "-tags="* || "${arg}" == "--tags="* ) && "${FORCE_OPENSSL}" == "1" && "${GO_BUILTIN_FIPS}" == "0" ]]; then
      scrub_no_openssl
    fi

    if [[ "${IN_TAGS}" == "1" ]]; then
      if [[ -n "${SHIM_TAGS}" ]]; then
        echoerr "adding ${SHIM_TAGS} tags to ${arg} (IN_TAGS=${IN_TAGS})"
        arg=$(echo "${arg}" | tr -d "'" | tr -d "\"")  # Delete any quotes which get passed in literally. prom-label-proxy managed this.
        if [[ "${arg}" == *" "* ]]; then  # If the tags parameter is space delimited
          arg="${arg} ${SHIM_TAGS//,/ }"
        else
          # Assume comma delimited
          arg="${arg},${SHIM_TAGS}"
        fi
      fi
      if [[ "${FORCE_OPENSSL}" == "1" && "${GO_BUILTIN_FIPS}" == "0" ]]; then
        scrub_no_openssl
      fi
      IN_TAGS=0
    fi

    if [[ "${arg}" == "-tags" || "${arg}" == "--tags" ]]; then
      IN_TAGS=1
    fi

    # Compilation with -extldflags "-static" is problematic with
    # CGO_ENABLED=1 because compilation tries to link against
    # static libraries which don't exist. Remove -static flag
    # when detected. This is tricky because extldflags can be simple
    # or something like -ldflags '-X $(REPO_PATH)/pkg/version.Raw=$(VERSION) -extldflags "-lm -lstdc++ -static"'
    # Note that extldflags is a flag embedded within the value of the
    # -ldflags argument. From our script's perspective, it will be part of a single
    # argument, but this argument might look like '-X $(REPO_PATH)/pkg/version.Raw=$(VERSION) -extldflags "-lm -lstdc++ -static"'.
    if [[ "${arg}" == *"-extldflags"* && "${FORCE_DYNAMIC}" == "1" ]]; then
      # We replace -static with -lc because '-lc' implies to link against stdlib. This is a default
      # and should therefore be benign for virtually any compilation (unless -nostdlib or -nodefaultlibs
      # is specified -- and we don't account for this).
      # Why replace instead of remove? Consider the complex possible scenarios:
      # -ldflags '-extldflags "-static"'   # Removing -extldflags would mean we also need to remove ldflags.
      # -ldflags '-X $(REPO_PATH)/pkg/version.Raw=$(VERSION) -extldflags "-static"'  # Would remove extldflags but keep ldflags
      # -ldflags '-X $(REPO_PATH)/pkg/version.Raw=$(VERSION) -extldflags "-static -lm"'  # Would need to remove -static but keep extldflags
      # In any scenario, replacing "-static" with something benign should work without the need for complex logic.
      pre_arg="${arg}"
      arg=$(echo "${arg}" | sed "s/-static/-lc/g")
      if [[ "${pre_arg}" != "${arg}" ]]; then
        echoerr "non-compliant: eliminated static"
      fi
    fi
    ARGS+=("${arg}")

    # When coverage is enabled and explicit .go files are being compiled,
    # ensure coverage_server.go is included if it exists alongside them.
    # Package-path builds (e.g. ./cmd/...) don't need this since Go
    # automatically includes all .go files in the package.
    if [[ "${GO_COMPLIANCE_COVER:-}" == "1" && "${IN_BUILD}" == "1" && "${COVERAGE_ADDED}" == "0" && "${arg}" == *.go ]]; then
      if [[ "$(basename "${arg}")" == "coverage_server.go" ]]; then
        COVERAGE_ADDED=1
      else
        coverage_path="$(dirname "${arg}")/coverage_server.go"
        if [[ -f "${coverage_path}" ]]; then
          echoerr "adding ${coverage_path} to build for coverage instrumentation"
          ARGS+=("${coverage_path}")
          COVERAGE_ADDED=1
        fi
      fi
    fi
done

echo 1>&2
echo 1>&2
echo -n "${LOG_PREFIX} final command line arguments: " 1>&2
for arg in "${ARGS[@]}"; do
  echo -n "\"${arg}\" " 1>&2
done
echo 1>&2
echo 1>&2

# Companion of the no_openssl tag: together they select the built-in module.
# The value is the module version, and must be kept in step with the one in
# golang_builder_FIPS_wrapper.sh.
if [[ "${GO_BUILTIN_FIPS}" == "1" ]]; then
  export GOFIPS140="v1.26.0"
else
  export GOFIPS140="off"
fi

echoerr "Invoking actual go binary"

echo -n "${LOG_PREFIX} go environment variable" 1>&2
{ "${SCRIPT_DIR}/go.real" env | grep -e CGO_ENABLED -e GOEXPERIMENT -e GOFIPS140 -e GOOS -e GOARCH -e GOAMD64; } 1>&2

echoerr "FORCE_FOD_MODE=$FORCE_FOD_MODE"
echoerr "FORCE_OPENSSL=$FORCE_OPENSSL"
echoerr "FORCE_DYNAMIC=$FORCE_DYNAMIC"
echoerr "GO_BUILTIN_FIPS=$GO_BUILTIN_FIPS"
echoerr "GOFIPS140=$GOFIPS140"

${SCRIPT_DIR}/go.real "${ARGS[@]}"

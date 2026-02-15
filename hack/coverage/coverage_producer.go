package main

// Coverage Producer — injected into specific binaries (currently the
// kubelet) by doozer's coverage instrumentation.  Injection is controlled
// by doozer at build time (see inject_coverage_producer in util.py), so
// there is no runtime executable-name check.
//
// On startup, this file's init() starts a background goroutine that:
//   1. Reads S3 credentials from a fake registry auth entry in
//      /var/lib/kubelet/config.json (server: "cluster-code-coverage.openshift.io").
//   2. Creates a Kubernetes client using the kubelet's own kubeconfig.
//   3. Discovers the node name via a pod list.
//   4. Watches pods on the node and periodically scans for coverage-
//      instrumented containers using crictl and /proc.
//   5. Collects coverage data from each container's coverage HTTP server
//      and uploads it to S3.
//
// NOTE ON PID NAMESPACE: This code reads /proc/<pid>/net/tcp and
// /proc/<pid>/environ for container processes.  This requires the kubelet
// to have host PID namespace access (the default for standard OCP
// kubelets).  If the kubelet itself is containerized without hostPID: true
// (e.g. some MicroShift configurations), the /proc-based discovery will
// not work.
//
// All imports use a _prod prefix to avoid collisions with the host package
// (same rationale as coverage_server.go's _cov prefix).

import (
	_prodBufio "bufio"
	_prodBytes "bytes"
	_prodHMAC "crypto/hmac"
	_prodSHA256 "crypto/sha256"
	_prodBase64 "encoding/base64"
	_prodHex "encoding/hex"
	_prodJSON "encoding/json"
	_prodFmt "fmt"
	_prodIO "io"
	_prodLog "log"
	_prodHTTP "net/http"
	_prodOS "os"
	_prodExec "os/exec"
	_prodStrconv "strconv"
	_prodStrings "strings"
	_prodSync "sync"
	_prodTime "time"

	_prodClientcmd "k8s.io/client-go/tools/clientcmd"
	_prodKubernetes "k8s.io/client-go/kubernetes"
	_prodAuthV1 "k8s.io/api/authentication/v1"
	_prodMetaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	_prodWatch "k8s.io/apimachinery/pkg/watch"
	_prodContext "context"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	_prodCoverageAuthServer  = "cluster-code-coverage.openshift.io"
	_prodDefaultCoveragePort = 53700
	_prodPortScanRange       = 100
	_prodScanInterval        = 10 * _prodTime.Second
)

// _prodRegistryAuthPaths is the list of registry auth config files to try,
// in order.  The first file that exists, is readable, AND contains the
// coverage auth entry wins.  On a normal node the kubelet's config.json is
// first; on a bootstrap node the pull secret may be in a different location.
var _prodRegistryAuthPaths = []string{
	"/var/lib/kubelet/config.json",
	"/root/.docker/config.json",
	"/run/containers/0/auth.json",
	"/root/.config/containers/auth.json",
}

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

// _prodS3Config holds S3 upload configuration extracted from the fake
// registry auth entry.
type _prodS3Config struct {
	AccessKey  string `json:"accessKey"`
	SecretKey  string `json:"secretKey"`
	Bucket     string `json:"bucket"`
	Region     string `json:"region"`
	S3BasePath string `json:"s3BasePath"`
}

// _prodCoverageConfig is the top-level structure stored (base64-encoded) as
// the password in the registry auth entry for cluster-code-coverage.io.
type _prodCoverageConfig struct {
	Service string         `json:"service"`
	S3      _prodS3Config  `json:"s3"`
}

// _prodDockerConfig mirrors the Docker/CRI-O auth config.json format.
type _prodDockerConfig struct {
	Auths map[string]_prodDockerAuth `json:"auths"`
}

type _prodDockerAuth struct {
	Auth string `json:"auth"` // base64(user:password)
}

// ---------------------------------------------------------------------------
// CRI types (crictl JSON output)
// ---------------------------------------------------------------------------

type _prodCrictlPSOutput struct {
	Containers []_prodCrictlContainer `json:"containers"`
}

type _prodCrictlContainer struct {
	ID        string                   `json:"id"`
	Metadata  _prodCrictlContainerMeta `json:"metadata"`
	PodSandboxID string               `json:"podSandboxId"`
}

type _prodCrictlContainerMeta struct {
	Name string `json:"name"`
}

type _prodCrictlInspectOutput struct {
	Info   _prodCrictlInspectInfo   `json:"info"`
	Status _prodCrictlInspectStatus `json:"status"`
}

type _prodCrictlInspectInfo struct {
	Pid int `json:"pid"`
}

type _prodCrictlInspectStatus struct {
	ImageRef string `json:"imageRef"`
}

type _prodCrictlSandboxInspect struct {
	Status _prodCrictlSandboxStatus `json:"status"`
}

type _prodCrictlSandboxStatus struct {
	Metadata _prodCrictlSandboxMeta   `json:"metadata"`
	Network  _prodCrictlSandboxNet    `json:"network"`
	Linux    _prodCrictlSandboxLinux  `json:"linux"`
}

type _prodCrictlSandboxMeta struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type _prodCrictlSandboxNet struct {
	IP string `json:"ip"`
}

// _prodCrictlSandboxLinux captures status.linux.namespaces.options which
// tells us the namespace mode for the sandbox.
type _prodCrictlSandboxLinux struct {
	Namespaces _prodCrictlSandboxNS `json:"namespaces"`
}

type _prodCrictlSandboxNS struct {
	Options _prodCrictlNSOptions `json:"options"`
}

// _prodCrictlNSOptions holds the namespace mode strings.
// Network is "NODE" for hostNetwork pods and "POD" for isolated pods.
type _prodCrictlNSOptions struct {
	Network string `json:"network"`
}

// ---------------------------------------------------------------------------
// Coverage server response (matches coverage_server.go's _covResponse)
// ---------------------------------------------------------------------------

type _prodCovResponse struct {
	MetaFilename     string `json:"meta_filename"`
	MetaData         string `json:"meta_data"`
	CountersFilename string `json:"counters_filename"`
	CountersData     string `json:"counters_data"`
	Timestamp        int64  `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// Per-container tracking
// ---------------------------------------------------------------------------

type _prodContainerInfo struct {
	ContainerID   string
	ContainerName string
	PodName       string
	Namespace     string
	PodIP         string // empty for hostNetwork pods
	HostNetwork   bool   // true if the pod uses the host network namespace
	ImageRef      string
	Pid           int
	CoveragePort  int
	DiscoveryTime string // UTC formatted as 20060102T150405Z
}

// _prodConnectAddr returns the address to use when connecting to a coverage
// server in this container.  For hostNetwork containers, this is 127.0.0.1.
// For containers with their own network namespace, this is the pod IP.
func (ci *_prodContainerInfo) _prodConnectAddr() string {
	if ci.HostNetwork || ci.PodIP == "" {
		return "127.0.0.1"
	}
	return ci.PodIP
}

// ---------------------------------------------------------------------------
// init — starts the coverage producer in the background
// ---------------------------------------------------------------------------

func init() {
	go _prodRun()
}

// ---------------------------------------------------------------------------
// Main producer loop
// ---------------------------------------------------------------------------

func _prodRun() {
	_prodLog.Println("[COVERAGE-PRODUCER] Starting coverage producer")

	// 1. Parse S3 config from registry auth (tries multiple paths)
	s3Cfg, err := _prodLoadS3Config()
	if err != nil {
		_prodLog.Printf("[COVERAGE-PRODUCER] ERROR: %v — producer shutting down", err)
		return
	}
	_prodLog.Printf("[COVERAGE-PRODUCER] S3 config loaded: bucket=%s region=%s basePath=%s",
		s3Cfg.Bucket, s3Cfg.Region, s3Cfg.S3BasePath)

	// 2. Get hostname for S3 path
	hostname, _ := _prodOS.Hostname()
	if hostname == "" {
		hostname = "unknown-host"
	}

	// 3. Determine mode: if the kubelet has a kubeconfig, run in full mode
	// (container discovery + host scanning).  If not, run in bootstrap mode
	// (host-only scanning).
	//
	// The kubeconfig may not exist yet when the kubelet first starts (it is
	// created asynchronously during node bootstrap).  Poll for it before
	// falling back to bootstrap mode.  Start the host-network scanner
	// immediately so we collect coverage while waiting.
	kubeconfigPath := _prodGetKubeconfigPath()

	go _prodScanHostLoop(s3Cfg, hostname)

	kubeClient := _prodWaitForKubeClient(kubeconfigPath)
	if kubeClient == nil {
		_prodLog.Println("[COVERAGE-PRODUCER] Running in bootstrap node mode — only host-network coverage will be collected")
		select {} // block forever; host scanner is already running
	}
	_prodLog.Println("[COVERAGE-PRODUCER] Kubernetes client created — running in full mode")

	// 4. Discover node name via SelfSubjectReview (retry indefinitely since
	// the API server may not be available during early boot).
	// The kubelet authenticates as "system:node:<name>", so we strip that
	// prefix to get the node name.
	nodeName := _prodDiscoverNodeName(kubeClient)
	_prodLog.Printf("[COVERAGE-PRODUCER] Discovered node name: %s", nodeName)

	// Shared state: tracked container IDs
	var trackedMu _prodSync.Mutex
	tracked := make(map[string]struct{})

	// Channel to trigger scans
	scanCh := make(chan struct{}, 1)
	triggerScan := func() {
		select {
		case scanCh <- struct{}{}:
		default:
		}
	}

	// 5. Start pod watcher in background
	go _prodWatchPods(kubeClient, nodeName, triggerScan)

	// 6. Host-network scanner already started above (step 3).

	// 7. Main scan loop
	ticker := _prodTime.NewTicker(_prodScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-scanCh:
		case <-ticker.C:
		}

		containers := _prodDiscoverContainers()
		for _, ci := range containers {
			trackedMu.Lock()
			_, already := tracked[ci.ContainerID]
			if !already {
				tracked[ci.ContainerID] = struct{}{}
			}
			trackedMu.Unlock()

			if !already {
				_prodLog.Printf("[COVERAGE-PRODUCER] New container: %s/%s/%s (pid=%d, podIP=%s, hostNet=%v)",
					ci.Namespace, ci.PodName, ci.ContainerName, ci.Pid, ci.PodIP, ci.HostNetwork)
				go _prodMonitorContainer(ci, s3Cfg, hostname, func() {
					trackedMu.Lock()
					delete(tracked, ci.ContainerID)
					trackedMu.Unlock()
				})
			}
		}
	}
}

// ---------------------------------------------------------------------------
// S3 config loading
// ---------------------------------------------------------------------------

// _prodLoadS3Config tries each path in _prodRegistryAuthPaths looking for
// a registry auth config that contains the cluster-code-coverage.openshift.io entry.
// Files that don't exist, can't be read, or don't contain the entry are
// silently skipped.  Returns an error only if NO file provides the secret.
func _prodLoadS3Config() (*_prodS3Config, error) {
	for _, path := range _prodRegistryAuthPaths {
		cfg, err := _prodTryLoadS3ConfigFrom(path)
		if err != nil {
			_prodLog.Printf("[COVERAGE-PRODUCER] Tried %s: %v", path, err)
			continue
		}
		_prodLog.Printf("[COVERAGE-PRODUCER] Loaded coverage config from %s", path)
		return cfg, nil
	}
	return nil, _prodFmt.Errorf("no registry auth file contains %q (tried %v)", _prodCoverageAuthServer, _prodRegistryAuthPaths)
}

// _prodTryLoadS3ConfigFrom attempts to read and parse the coverage S3
// config from a single registry auth file.
func _prodTryLoadS3ConfigFrom(path string) (*_prodS3Config, error) {
	data, err := _prodOS.ReadFile(path)
	if err != nil {
		return nil, _prodFmt.Errorf("cannot read: %w", err)
	}

	var dockerCfg _prodDockerConfig
	if err := _prodJSON.Unmarshal(data, &dockerCfg); err != nil {
		return nil, _prodFmt.Errorf("cannot parse JSON: %w", err)
	}

	authEntry, ok := dockerCfg.Auths[_prodCoverageAuthServer]
	if !ok {
		return nil, _prodFmt.Errorf("no %q entry", _prodCoverageAuthServer)
	}

	// The auth field is base64(username:password).  The password is a
	// base64-encoded JSON config.
	decoded, err := _prodBase64.StdEncoding.DecodeString(authEntry.Auth)
	if err != nil {
		return nil, _prodFmt.Errorf("cannot base64-decode auth: %w", err)
	}

	parts := _prodStrings.SplitN(string(decoded), ":", 2)
	if len(parts) < 2 {
		return nil, _prodFmt.Errorf("auth is not in user:password format")
	}
	password := parts[1]

	// The password itself is base64-encoded JSON
	cfgJSON, err := _prodBase64.StdEncoding.DecodeString(password)
	if err != nil {
		return nil, _prodFmt.Errorf("cannot base64-decode password: %w", err)
	}

	var covCfg _prodCoverageConfig
	if err := _prodJSON.Unmarshal(cfgJSON, &covCfg); err != nil {
		return nil, _prodFmt.Errorf("cannot parse coverage config: %w", err)
	}

	if covCfg.Service != "s3" {
		return nil, _prodFmt.Errorf("unsupported service %q", covCfg.Service)
	}
	if covCfg.S3.Bucket == "" || covCfg.S3.Region == "" || covCfg.S3.AccessKey == "" || covCfg.S3.SecretKey == "" {
		return nil, _prodFmt.Errorf("incomplete S3 configuration")
	}

	return &covCfg.S3, nil
}

// ---------------------------------------------------------------------------
// Kubernetes client
// ---------------------------------------------------------------------------

func _prodGetKubeconfigPath() string {
	// Discover the --kubeconfig flag from the kubelet's own command line
	// (works for both the real kubelet and fake-kubelet since they share
	// the same process and /proc/self/cmdline).
	kubeconfigPath := _prodFindKubeconfigFlag()
	if kubeconfigPath == "" {
		// Fall back to the standard OCP kubelet kubeconfig path
		kubeconfigPath = "/var/lib/kubelet/kubeconfig"
	}
	return kubeconfigPath
}

func _prodBuildKubeClient(kubeconfigPath string) (*_prodKubernetes.Clientset, error) {
	config, err := _prodClientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, _prodFmt.Errorf("cannot build rest config from %s: %w", kubeconfigPath, err)
	}

	return _prodKubernetes.NewForConfig(config)
}

// _prodWaitForKubeClient polls for the kubeconfig file to appear (it may
// be created asynchronously during node bootstrap) and builds a client.
// Returns nil after 5 minutes if the file never appears — the caller
// should fall back to bootstrap mode.
func _prodWaitForKubeClient(kubeconfigPath string) *_prodKubernetes.Clientset {
	deadline := _prodTime.Now().Add(5 * _prodTime.Minute)
	logged := false
	for {
		client, err := _prodBuildKubeClient(kubeconfigPath)
		if err == nil {
			return client
		}
		if _prodTime.Now().After(deadline) {
			_prodLog.Printf("[COVERAGE-PRODUCER] WARNING: kubeconfig %s still not available after 5 minutes: %v", kubeconfigPath, err)
			return nil
		}
		if !logged {
			_prodLog.Printf("[COVERAGE-PRODUCER] Waiting for kubeconfig %s to appear (polling 1/s): %v", kubeconfigPath, err)
			logged = true
		}
		_prodTime.Sleep(1 * _prodTime.Second)
	}
}

// _prodDiscoverNodeName queries the API server's SelfSubjectReview to find
// the authenticated username, then strips the "system:node:" prefix to get
// the node name.  Retries indefinitely since the API server may not be
// available during early boot.
func _prodDiscoverNodeName(client *_prodKubernetes.Clientset) string {
	const prefix = "system:node:"
	for {
		review, err := client.AuthenticationV1().SelfSubjectReviews().Create(
			_prodContext.Background(),
			&_prodAuthV1.SelfSubjectReview{},
			_prodMetaV1.CreateOptions{},
		)
		if err != nil {
			_prodLog.Printf("[COVERAGE-PRODUCER] SelfSubjectReview failed (will retry): %v", err)
			_prodTime.Sleep(5 * _prodTime.Second)
			continue
		}
		username := review.Status.UserInfo.Username
		_prodLog.Printf("[COVERAGE-PRODUCER] Authenticated as: %s", username)
		if _prodStrings.HasPrefix(username, prefix) {
			return _prodStrings.TrimPrefix(username, prefix)
		}
		_prodLog.Printf("[COVERAGE-PRODUCER] WARNING: username %q does not start with %q; using full username as node name", username, prefix)
		return username
	}
}

// _prodFindKubeconfigFlag parses /proc/self/cmdline to find --kubeconfig=<path>
// or --kubeconfig <path>.
func _prodFindKubeconfigFlag() string {
	data, err := _prodOS.ReadFile("/proc/self/cmdline")
	if err != nil {
		return ""
	}
	args := _prodStrings.Split(string(data), "\x00")
	for i, arg := range args {
		if arg == "--kubeconfig" && i+1 < len(args) {
			return args[i+1]
		}
		if _prodStrings.HasPrefix(arg, "--kubeconfig=") {
			return _prodStrings.TrimPrefix(arg, "--kubeconfig=")
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Pod watcher (robust, auto-reconnects)
// ---------------------------------------------------------------------------

func _prodWatchPods(client *_prodKubernetes.Clientset, nodeName string, triggerScan func()) {
	for {
		_prodLog.Printf("[COVERAGE-PRODUCER] Starting pod watch for node %s", nodeName)
		watcher, err := client.CoreV1().Pods("").Watch(
			_prodContext.Background(),
			_prodMetaV1.ListOptions{
				FieldSelector: "spec.nodeName=" + nodeName,
			},
		)
		if err != nil {
			_prodLog.Printf("[COVERAGE-PRODUCER] Watch failed (will retry): %v", err)
			_prodTime.Sleep(5 * _prodTime.Second)
			continue
		}

		for event := range watcher.ResultChan() {
			switch event.Type {
			case _prodWatch.Added, _prodWatch.Modified, _prodWatch.Deleted:
				triggerScan()
			case _prodWatch.Error:
				_prodLog.Println("[COVERAGE-PRODUCER] Watch error event received")
			}
		}

		_prodLog.Println("[COVERAGE-PRODUCER] Watch channel closed; reconnecting")
		_prodTime.Sleep(2 * _prodTime.Second)
	}
}

// ---------------------------------------------------------------------------
// Container discovery via crictl
// ---------------------------------------------------------------------------

func _prodDiscoverContainers() []_prodContainerInfo {
	out, err := _prodExec.Command("crictl", "ps", "-o", "json").Output()
	if err != nil {
		_prodLog.Printf("[COVERAGE-PRODUCER] crictl ps failed: %v", err)
		return nil
	}

	var psOut _prodCrictlPSOutput
	if err := _prodJSON.Unmarshal(out, &psOut); err != nil {
		_prodLog.Printf("[COVERAGE-PRODUCER] crictl ps JSON parse failed: %v", err)
		return nil
	}

	var results []_prodContainerInfo
	// Cache sandbox inspections to avoid redundant calls
	sandboxCache := make(map[string]*_prodCrictlSandboxInspect)

	for _, c := range psOut.Containers {
		ci := _prodInspectContainer(c, sandboxCache)
		if ci != nil {
			results = append(results, *ci)
		}
	}
	return results
}

func _prodInspectContainer(c _prodCrictlContainer, sandboxCache map[string]*_prodCrictlSandboxInspect) *_prodContainerInfo {
	// Inspect the container for pid and imageRef
	out, err := _prodExec.Command("crictl", "inspect", c.ID).Output()
	if err != nil {
		return nil // Container may have been removed
	}

	var inspect _prodCrictlInspectOutput
	if err := _prodJSON.Unmarshal(out, &inspect); err != nil {
		return nil
	}
	if inspect.Info.Pid == 0 {
		return nil
	}

	// Inspect the sandbox for pod metadata and IP
	sandbox, ok := sandboxCache[c.PodSandboxID]
	if !ok {
		sOut, err := _prodExec.Command("crictl", "inspectp", c.PodSandboxID).Output()
		if err != nil {
			return nil
		}
		sandbox = &_prodCrictlSandboxInspect{}
		if err := _prodJSON.Unmarshal(sOut, sandbox); err != nil {
			return nil
		}
		sandboxCache[c.PodSandboxID] = sandbox
	}

	// Read COVERAGE_PORT from the container's environment
	coveragePort := _prodDefaultCoveragePort
	envData, err := _prodOS.ReadFile(_prodFmt.Sprintf("/proc/%d/environ", inspect.Info.Pid))
	if err == nil {
		for _, entry := range _prodStrings.Split(string(envData), "\x00") {
			if _prodStrings.HasPrefix(entry, "COVERAGE_PORT=") {
				if p, err := _prodStrconv.Atoi(_prodStrings.TrimPrefix(entry, "COVERAGE_PORT=")); err == nil && p > 0 {
					coveragePort = p
				}
			}
		}
	}

	// Detect host networking from status.linux.namespaces.options.network.
	// "NODE" means the pod shares the host's network namespace (hostNetwork).
	// "POD" means the pod has its own network namespace with a pod IP.
	// Also treat an empty pod IP as a fallback indicator for host networking.
	hostNet := sandbox.Status.Linux.Namespaces.Options.Network == "NODE"
	podIP := sandbox.Status.Network.IP
	if podIP == "" && !hostNet {
		// Safety: if the IP is empty but the namespace mode says POD,
		// treat it as hostNetwork to avoid connecting to an empty address.
		hostNet = true
	}

	return &_prodContainerInfo{
		ContainerID:   c.ID,
		ContainerName: c.Metadata.Name,
		PodName:       sandbox.Status.Metadata.Name,
		Namespace:     sandbox.Status.Metadata.Namespace,
		PodIP:         podIP,
		HostNetwork:   hostNet,
		ImageRef:      inspect.Status.ImageRef,
		Pid:           inspect.Info.Pid,
		CoveragePort:  coveragePort,
		DiscoveryTime: _prodTime.Now().UTC().Format("20060102T150405Z"),
	}
}

// ---------------------------------------------------------------------------
// Per-container monitoring goroutine
// ---------------------------------------------------------------------------

func _prodMonitorContainer(ci _prodContainerInfo, s3Cfg *_prodS3Config, hostname string, onDone func()) {
	defer onDone()

	// Track which ports we have already started collectors for
	collectorPorts := make(map[int]struct{})

	for {
		// Check if the process is still alive
		if !_prodPidAlive(ci.Pid) {
			_prodLog.Printf("[COVERAGE-PRODUCER] Container %s pid %d gone; stopping monitor",
				ci.ContainerName, ci.Pid)
			return
		}

		// Parse /proc/<pid>/net/tcp{,6} for listening ports in the coverage range.
		// For non-hostNetwork containers, /proc/<pid>/net/ only shows the pod's
		// namespace, so all ports belong to this pod.
		// For hostNetwork containers, /proc/<pid>/net/ shows all host sockets,
		// so we filter by socket inodes owned by this specific PID.
		ports := _prodFindListeningPorts(ci.Pid, ci.CoveragePort, ci.CoveragePort+_prodPortScanRange, ci.HostNetwork)
		connectHost := ci._prodConnectAddr()

		for _, port := range ports {
			if _, already := collectorPorts[port]; already {
				continue
			}

			// Verify it is a coverage server via HEAD.
			// For non-hostNetwork containers, we connect via the pod IP which
			// routes into the pod's network namespace from the host.
			// For hostNetwork containers, we connect to 127.0.0.1 since the
			// port is on the host network.
			binary := _prodCheckCoverageServer(connectHost, port)
			if binary == "" {
				continue
			}

			_prodLog.Printf("[COVERAGE-PRODUCER] Confirmed coverage server for %s (pid %d) at %s:%d (binary=%s, hostNet=%v)",
				ci.ContainerName, ci.Pid, connectHost, port, binary, ci.HostNetwork)
			collectorPorts[port] = struct{}{}

			sanitizedBinary := _prodSanitizePath(binary)
			sanitizedImage := _prodSanitizePath(ci.ImageRef)
			s3Prefix := _prodFmt.Sprintf("%s/%s/%s/%s/%s/%s/%s/%s",
				s3Cfg.S3BasePath, hostname, ci.Namespace, ci.PodName,
				ci.ContainerName, ci.DiscoveryTime, sanitizedImage, sanitizedBinary)

			go _prodCollectCoverage(ci.Pid, connectHost, port, s3Prefix, s3Cfg)
		}

		_prodTime.Sleep(2 * _prodTime.Second)
	}
}

// ---------------------------------------------------------------------------
// Host-network scanner (kubelet itself + hostNetwork containers)
// ---------------------------------------------------------------------------

func _prodScanHostLoop(s3Cfg *_prodS3Config, hostname string) {
	_prodLog.Println("[COVERAGE-PRODUCER] Host-network scanner started")

	// Track which ports we have already started collectors for
	collectorPorts := make(map[int]struct{})
	coveragePort := _prodDefaultCoveragePort
	if envPort := _prodOS.Getenv("COVERAGE_PORT"); envPort != "" {
		if p, err := _prodStrconv.Atoi(envPort); err == nil && p > 0 {
			coveragePort = p
		}
	}

	scanCount := 0
	for {
		scanCount++
		// Parse /proc/net/tcp and /proc/net/tcp6 for listening ports in the
		// coverage range (host network namespace).  Go may bind to IPv6, so
		// we must check both files.  No inode filter — we want ALL host ports.
		ports := _prodFindListeningPortsFromFile("/proc/net/tcp", coveragePort, coveragePort+_prodPortScanRange, nil)
		ports = append(ports, _prodFindListeningPortsFromFile("/proc/net/tcp6", coveragePort, coveragePort+_prodPortScanRange, nil)...)
		ports = _prodDedup(ports)

		if scanCount <= 3 || scanCount%30 == 0 {
			_prodLog.Printf("[COVERAGE-PRODUCER] Host scan #%d: %d candidate port(s) in range %d-%d",
				scanCount, len(ports), coveragePort, coveragePort+_prodPortScanRange)
		}

		for _, port := range ports {
			if _, already := collectorPorts[port]; already {
				continue
			}

			_prodLog.Printf("[COVERAGE-PRODUCER] Host scan: checking port %d", port)
			binary := _prodCheckCoverageServer("127.0.0.1", port)
			if binary == "" {
				_prodLog.Printf("[COVERAGE-PRODUCER] Host scan: port %d is not a coverage server", port)
				continue
			}

			_prodLog.Printf("[COVERAGE-PRODUCER] Confirmed host-network coverage server at :%d (binary=%s)", port, binary)
			collectorPorts[port] = struct{}{}

			sanitizedBinary := _prodSanitizePath(binary)
			discoveryTime := _prodTime.Now().UTC().Format("20060102T150405Z")
			s3Prefix := _prodFmt.Sprintf("%s/%s/_host_/%s/%s",
				s3Cfg.S3BasePath, hostname, discoveryTime, sanitizedBinary)

			_prodLog.Printf("[COVERAGE-PRODUCER] S3 prefix for host server: %s", s3Prefix)

			// pid=0 for host-network: collector won't check /proc/<pid>
			go _prodCollectCoverage(0, "127.0.0.1", port, s3Prefix, s3Cfg)
		}

		_prodTime.Sleep(_prodScanInterval)
	}
}

// ---------------------------------------------------------------------------
// Coverage collection goroutine (adaptive polling)
// ---------------------------------------------------------------------------

func _prodCollectCoverage(pid int, host string, port int, s3Prefix string, s3Cfg *_prodS3Config) {
	_prodLog.Printf("[COVERAGE-PRODUCER] Starting coverage collection from %s:%d (pid=%d, prefix=%s)", host, port, pid, s3Prefix)
	baseURL := _prodFmt.Sprintf("http://%s:%d/coverage", host, port)
	metaUploaded := false
	toggleBit := 1       // toggles between 1 and 2
	cachedHash := ""     // hash from the first full (with metadata) response

	// Adaptive polling schedule
	startTime := _prodTime.Now()
	getPollInterval := func() _prodTime.Duration {
		elapsed := _prodTime.Since(startTime)
		switch {
		case elapsed < 10*_prodTime.Second:
			return 1 * _prodTime.Second
		case elapsed < 5*_prodTime.Minute+10*_prodTime.Second:
			return 5 * _prodTime.Second
		default:
			return 10 * _prodTime.Second
		}
	}

	for {
		// If we are tracking a specific PID, check it is alive
		if pid > 0 && !_prodPidAlive(pid) {
			_prodLog.Printf("[COVERAGE-PRODUCER] PID %d gone; stopping coverage collection for %s:%d", pid, host, port)
			return
		}

		url := baseURL
		if metaUploaded {
			url = baseURL + "?nometa=1"
		}

		resp, err := _prodHTTPGet(url)
		if err != nil {
			_prodLog.Printf("[COVERAGE-PRODUCER] GET %s failed: %v", url, err)
			// If the server is gone (host network, pid=0), stop
			if pid == 0 {
				return
			}
			_prodTime.Sleep(getPollInterval())
			continue
		}

		var covResp _prodCovResponse
		if err := _prodJSON.Unmarshal(resp, &covResp); err != nil {
			_prodLog.Printf("[COVERAGE-PRODUCER] JSON parse failed: %v", err)
			_prodTime.Sleep(getPollInterval())
			continue
		}

		// Upload metadata (only on the first successful response)
		if !metaUploaded && covResp.MetaFilename != "" && covResp.MetaData != "" {
			// Cache the hash from the meta filename (e.g. "covmeta.<hash>")
			if parts := _prodStrings.SplitN(covResp.MetaFilename, ".", 2); len(parts) == 2 && parts[1] != "unknown" {
				cachedHash = parts[1]
			}
			metaBytes, err := _prodBase64.StdEncoding.DecodeString(covResp.MetaData)
			if err == nil {
				key := s3Prefix + "/" + covResp.MetaFilename
				if err := _prodS3Put(s3Cfg, key, metaBytes); err != nil {
					_prodLog.Printf("[COVERAGE-PRODUCER] S3 PUT meta failed: %v", err)
				} else {
					_prodLog.Printf("[COVERAGE-PRODUCER] Uploaded metadata: %s", key)
					metaUploaded = true
				}
			}
		}

		// Upload counters (toggling between .1 and .2)
		if covResp.CountersData != "" {
			counterBytes, err := _prodBase64.StdEncoding.DecodeString(covResp.CountersData)
			if err == nil {
				counterFilename := covResp.CountersFilename
				// If the server returned "unknown" as the hash (old server
				// without hash caching, or first request was nometa=1),
				// substitute with our cached hash from the metadata response.
				if cachedHash != "" && _prodStrings.Contains(counterFilename, ".unknown.") {
					counterFilename = _prodStrings.Replace(counterFilename, ".unknown.", "."+cachedHash+".", 1)
				}
				// Build the toggling filename from the response filename.
				// Response filename: covcounters.<hash>.<pid>.<timestamp>
				// We want:          covcounters.<hash>.<pid>.<toggle>
				counterName := _prodBuildToggledCounterName(counterFilename, toggleBit)
				key := s3Prefix + "/" + counterName
				if err := _prodS3Put(s3Cfg, key, counterBytes); err != nil {
					_prodLog.Printf("[COVERAGE-PRODUCER] S3 PUT counters failed: %v", err)
				}
				// Toggle for next cycle
				if toggleBit == 1 {
					toggleBit = 2
				} else {
					toggleBit = 1
				}
			}
		}

		_prodTime.Sleep(getPollInterval())
	}
}

// _prodBuildToggledCounterName takes a filename like
// "covcounters.<hash>.<pid>.<timestamp>" and replaces the timestamp
// portion with the toggle value (1 or 2).
func _prodBuildToggledCounterName(filename string, toggle int) string {
	parts := _prodStrings.Split(filename, ".")
	if len(parts) >= 4 {
		// Replace the last part (timestamp) with the toggle
		parts[len(parts)-1] = _prodStrconv.Itoa(toggle)
		return _prodStrings.Join(parts, ".")
	}
	// Fallback: just append
	return _prodFmt.Sprintf("%s.%d", filename, toggle)
}

// ---------------------------------------------------------------------------
// /proc helpers
// ---------------------------------------------------------------------------

func _prodPidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	_, err := _prodOS.Stat(_prodFmt.Sprintf("/proc/%d", pid))
	return err == nil
}

// _prodFindListeningPorts reads /proc/<pid>/net/tcp and /proc/<pid>/net/tcp6
// and returns ports in [minPort, maxPort) that are in LISTEN state (0A).
// Both files are checked because Go's net.Listen("tcp", ":port") may bind
// to IPv6 on modern kernels, which only appears in tcp6.
//
// If hostNetwork is true, the process shares the host network namespace and
// /proc/<pid>/net/tcp6 shows ALL host sockets — not just this process's.
// In that case we filter by matching socket inodes from /proc/<pid>/fd/
// to only return ports owned by this specific PID.
func _prodFindListeningPorts(pid int, minPort int, maxPort int, hostNetwork bool) []int {
	tcp4 := _prodFmt.Sprintf("/proc/%d/net/tcp", pid)
	tcp6 := _prodFmt.Sprintf("/proc/%d/net/tcp6", pid)

	var inodeFilter map[string]struct{}
	if hostNetwork {
		// Build the set of socket inodes owned by this PID so we can
		// filter the global host socket table to just this process's sockets.
		inodeFilter = _prodSocketInodesForPid(pid)
	}

	ports := _prodFindListeningPortsFromFile(tcp4, minPort, maxPort, inodeFilter)
	ports = append(ports, _prodFindListeningPortsFromFile(tcp6, minPort, maxPort, inodeFilter)...)
	return _prodDedup(ports)
}

// _prodSocketInodesForPid returns the set of socket inode numbers referenced
// by /proc/<pid>/fd/*.  Each fd that is a socket has a symlink target like
// "socket:[12345]" where 12345 is the inode.
func _prodSocketInodesForPid(pid int) map[string]struct{} {
	fdDir := _prodFmt.Sprintf("/proc/%d/fd", pid)
	entries, err := _prodOS.ReadDir(fdDir)
	if err != nil {
		return nil
	}
	inodes := make(map[string]struct{})
	for _, entry := range entries {
		link, err := _prodOS.Readlink(fdDir + "/" + entry.Name())
		if err != nil {
			continue
		}
		// link looks like "socket:[12345]"
		if _prodStrings.HasPrefix(link, "socket:[") && _prodStrings.HasSuffix(link, "]") {
			inode := link[8 : len(link)-1] // extract "12345"
			inodes[inode] = struct{}{}
		}
	}
	return inodes
}

// _prodDedup removes duplicate integers from a slice.
func _prodDedup(ports []int) []int {
	seen := make(map[int]struct{}, len(ports))
	out := make([]int, 0, len(ports))
	for _, p := range ports {
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			out = append(out, p)
		}
	}
	return out
}

// _prodFindListeningPortsFromFile parses a /proc/net/tcp-format file and
// returns local ports in [minPort, maxPort) that are in LISTEN state (0A).
// This works for both tcp and tcp6 files since they share the same format
// (tcp6 just has wider hex address fields, but the port is in the same position).
//
// If inodeFilter is non-nil, only sockets whose inode (field index 9) is in
// the filter set are included.  This is used to restrict results to sockets
// owned by a specific PID when scanning from a shared (host) network namespace.
func _prodFindListeningPortsFromFile(path string, minPort int, maxPort int, inodeFilter map[string]struct{}) []int {
	f, err := _prodOS.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var ports []int
	scanner := _prodBufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		if lineNo == 1 {
			continue // skip header
		}
		line := _prodStrings.TrimSpace(scanner.Text())
		fields := _prodStrings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		// fields[1] = local_address (hex IP:port), fields[3] = state, fields[9] = inode
		if fields[3] != "0A" {
			continue // not LISTEN
		}

		// If we have an inode filter, check that this socket belongs to our PID
		if inodeFilter != nil {
			if _, ok := inodeFilter[fields[9]]; !ok {
				continue
			}
		}

		localAddr := fields[1]
		colonIdx := _prodStrings.Index(localAddr, ":")
		if colonIdx < 0 {
			continue
		}
		portHex := localAddr[colonIdx+1:]
		port64, err := _prodStrconv.ParseInt(portHex, 16, 32)
		if err != nil {
			continue
		}
		port := int(port64)
		if port >= minPort && port < maxPort {
			ports = append(ports, port)
		}
	}
	return ports
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

// _prodCheckCoverageServer sends a HEAD request to a coverage server's
// health endpoint and returns the binary name if it is a coverage server.
func _prodCheckCoverageServer(host string, port int) string {
	client := &_prodHTTP.Client{Timeout: 5 * _prodTime.Second}
	url := _prodFmt.Sprintf("http://%s:%d/health", host, port)
	req, err := _prodHTTP.NewRequest("HEAD", url, nil)
	if err != nil {
		return ""
	}
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	resp.Body.Close()
	if resp.Header.Get("X-Art-Coverage-Server") != "1" {
		return ""
	}
	return resp.Header.Get("X-Art-Coverage-Binary")
}

// _prodHTTPGet fetches a URL and returns the body.
func _prodHTTPGet(url string) ([]byte, error) {
	client := &_prodHTTP.Client{Timeout: 30 * _prodTime.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, _prodFmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return _prodIO.ReadAll(resp.Body)
}

// ---------------------------------------------------------------------------
// AWS S3 upload — Signature V4, stdlib only
// ---------------------------------------------------------------------------

func _prodS3Put(cfg *_prodS3Config, objectKey string, data []byte) error {
	now := _prodTime.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	host := _prodFmt.Sprintf("%s.s3.%s.amazonaws.com", cfg.Bucket, cfg.Region)
	url := _prodFmt.Sprintf("https://%s/%s", host, objectKey)

	payloadHash := _prodSHA256Hex(data)

	// Canonical request
	canonicalHeaders := _prodFmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		host, payloadHash, amzDate)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonicalRequest := _prodStrings.Join([]string{
		"PUT",
		"/" + objectKey,
		"", // query string
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	// String to sign
	credentialScope := _prodFmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, cfg.Region)
	stringToSign := _prodStrings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		_prodSHA256Hex([]byte(canonicalRequest)),
	}, "\n")

	// Signing key
	kDate := _prodHMACSHA256([]byte("AWS4"+cfg.SecretKey), []byte(dateStamp))
	kRegion := _prodHMACSHA256(kDate, []byte(cfg.Region))
	kService := _prodHMACSHA256(kRegion, []byte("s3"))
	kSigning := _prodHMACSHA256(kService, []byte("aws4_request"))

	signature := _prodHex.EncodeToString(_prodHMACSHA256(kSigning, []byte(stringToSign)))

	authorization := _prodFmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		cfg.AccessKey, credentialScope, signedHeaders, signature)

	req, err := _prodHTTP.NewRequest("PUT", url, _prodBytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Host", host)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("Authorization", authorization)
	req.Header.Set("Content-Length", _prodStrconv.Itoa(len(data)))

	client := &_prodHTTP.Client{Timeout: 60 * _prodTime.Second}

	// Retry on transient errors
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			_prodTime.Sleep(_prodTime.Duration(attempt) * 2 * _prodTime.Second)
			// Reset body for retry
			req.Body = _prodIO.NopCloser(_prodBytes.NewReader(data))
		}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode >= 500 {
			lastErr = _prodFmt.Errorf("S3 returned %d", resp.StatusCode)
			continue
		}
		return _prodFmt.Errorf("S3 PUT %s returned %d", objectKey, resp.StatusCode)
	}
	return _prodFmt.Errorf("S3 PUT %s failed after retries: %w", objectKey, lastErr)
}

// ---------------------------------------------------------------------------
// Crypto helpers (stdlib only)
// ---------------------------------------------------------------------------

func _prodSHA256Hex(data []byte) string {
	h := _prodSHA256.Sum256(data)
	return _prodHex.EncodeToString(h[:])
}

func _prodHMACSHA256(key []byte, data []byte) []byte {
	h := _prodHMAC.New(_prodSHA256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// ---------------------------------------------------------------------------
// Path sanitization
// ---------------------------------------------------------------------------

func _prodSanitizePath(s string) string {
	s = _prodStrings.ReplaceAll(s, "/", "-")
	s = _prodStrings.ReplaceAll(s, ":", "-")
	s = _prodStrings.ReplaceAll(s, "@", "-")
	// Remove leading dashes
	s = _prodStrings.TrimLeft(s, "-")
	if s == "" {
		s = "unknown"
	}
	return s
}

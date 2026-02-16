package main

// fake-kubelet — a minimal test binary that exercises both
// coverage_server.go and coverage_producer.go exactly as they would
// run inside the real kubelet.
//
// Build:
//   cd hack/coverage/fake-kubelet
//   go build -cover -covermode=atomic -o fake-kubelet .
//
// The binary MUST be named "kubelet" (or end with "kubelet") when
// copied to the node so that the producer can be activated by
// convention in future versions if a name check is re-added.
// However, the current producer starts unconditionally.
//
// Usage (on a node):
//   cp fake-kubelet /usr/local/bin/fake-kubelet
//   /usr/local/bin/fake-kubelet
//   /usr/local/bin/fake-kubelet --kubeconfig=/path/to/other/kubeconfig
//
// --kubeconfig defaults to /var/lib/kubelet/kubeconfig (same as the real
// kubelet on OCP).  The producer discovers the flag by parsing
// /proc/self/cmdline, so passing it on the command line is all that is
// needed.
//
// The binary will:
//   - Start the coverage HTTP server (port 53700+)
//   - Start the coverage producer (reads /var/lib/kubelet/config.json,
//     watches pods, scans containers, uploads to S3)
//   - Block forever (SIGINT/SIGTERM to stop)

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("[FAKE-KUBELET] Starting — coverage server and producer are running via init()")

	// Determine the effective --kubeconfig (mirrors the producer's logic)
	kubeconfigPath := "/var/lib/kubelet/kubeconfig"
	for i, arg := range os.Args[1:] {
		if arg == "--kubeconfig" && i+1 < len(os.Args[1:]) {
			kubeconfigPath = os.Args[i+2]
		}
		if strings.HasPrefix(arg, "--kubeconfig=") {
			kubeconfigPath = strings.TrimPrefix(arg, "--kubeconfig=")
		}
	}
	log.Printf("[FAKE-KUBELET] --kubeconfig = %s", kubeconfigPath)

	hostname, _ := os.Hostname()
	log.Printf("[FAKE-KUBELET] Hostname: %s  PID: %d", hostname, os.Getpid())
	fmt.Println()
	fmt.Println("The coverage server and producer are running in the background.")
	fmt.Println("Press Ctrl-C to stop.")
	fmt.Println()

	// Block until signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	log.Printf("[FAKE-KUBELET] Received %v — shutting down", s)
}

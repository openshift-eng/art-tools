#!/bin/bash
set -euo pipefail

usage() {
    cat <<EOF
Deploy fake-kubelet to cluster nodes via SSH bastion.

Prerequisites:
  An SSH bastion must be deployed in the cluster:
    curl https://raw.githubusercontent.com/eparis/ssh-bastion/8c73d4ec1872983a9ba41442bb38853387589c59/deploy/deploy.sh | bash

Usage: $(basename "$0") [OPTIONS] <binary-path>

Arguments:
  binary-path    Path to the fake-kubelet binary to deploy

Options:
  -k, --kubeconfig PATH   Path to kubeconfig (default: \$KUBECONFIG or ~/.kube/config)
  -n, --node NODE         Target a specific node by name (can be repeated; default: all nodes)
  --kill-only             Kill running fake-kubelet processes without deploying
  --logs NODE             Tail logs from a running fake-kubelet on the specified node
  -h, --help              Show this help

Examples:
  # Deploy to all nodes
  $(basename "$0") ./fake-kubelet

  # Deploy to specific nodes
  $(basename "$0") -n master-0 -n worker-a ./fake-kubelet

  # Kill fake-kubelet on all nodes
  $(basename "$0") --kill-only

  # Tail logs from a node
  $(basename "$0") --logs master-0
EOF
    exit "${1:-0}"
}

KUBECONFIG_PATH="${KUBECONFIG:-${HOME}/.kube/config}"
TARGET_NODES=()
KILL_ONLY=false
LOGS_NODE=""
BINARY_PATH=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -k|--kubeconfig) KUBECONFIG_PATH="$2"; shift 2 ;;
        -n|--node)       TARGET_NODES+=("$2"); shift 2 ;;
        --kill-only)     KILL_ONLY=true; shift ;;
        --logs)          LOGS_NODE="$2"; shift 2 ;;
        -h|--help)       usage 0 ;;
        -*)              echo "Unknown option: $1" >&2; usage 1 ;;
        *)               BINARY_PATH="$1"; shift ;;
    esac
done

if [[ -z "$BINARY_PATH" && "$KILL_ONLY" == false && -z "$LOGS_NODE" ]]; then
    echo "Error: binary-path is required unless --kill-only or --logs is used" >&2
    usage 1
fi

if [[ -n "$BINARY_PATH" && ! -f "$BINARY_PATH" ]]; then
    echo "Error: binary not found: $BINARY_PATH" >&2
    exit 1
fi

export KUBECONFIG="$KUBECONFIG_PATH"

SSH_OPTS="-o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o LogLevel=ERROR"

# Discover bastion IP
echo "Discovering SSH bastion..."
BASTION_IP=$(oc get service --all-namespaces -l run=ssh-bastion \
    -o go-template='{{ with (index (index .items 0).status.loadBalancer.ingress 0) }}{{ or .hostname .ip }}{{end}}' 2>/dev/null) || true

if [[ -z "$BASTION_IP" ]]; then
    echo "Error: SSH bastion not found. Deploy it first:" >&2
    echo "" >&2
    echo "  curl https://raw.githubusercontent.com/eparis/ssh-bastion/8c73d4ec1872983a9ba41442bb38853387589c59/deploy/deploy.sh | bash" >&2
    echo "" >&2
    exit 1
fi
echo "  Bastion: $BASTION_IP"

PROXY_CMD="ssh $SSH_OPTS -W %h:%p core@${BASTION_IP}"

ssh_node() {
    local ip="$1"; shift
    ssh $SSH_OPTS -o ProxyCommand="$PROXY_CMD" "core@${ip}" "$@"
}

scp_node() {
    local src="$1" ip="$2" dst="$3"
    scp $SSH_OPTS -o ProxyCommand="$PROXY_CMD" "$src" "core@${ip}:${dst}"
}

# Build node list: name -> internal IP
declare -A NODE_IPS
while IFS=$'\t' read -r name ip; do
    NODE_IPS["$name"]="$ip"
done < <(oc get nodes -o go-template='{{range .items}}{{.metadata.name}}{{"\t"}}{{range .status.addresses}}{{if eq .type "InternalIP"}}{{.address}}{{end}}{{end}}{{"\n"}}{{end}}')

if [[ ${#NODE_IPS[@]} -eq 0 ]]; then
    echo "Error: no nodes found in cluster" >&2
    exit 1
fi

# Resolve target nodes
RESOLVED_NODES=()
if [[ ${#TARGET_NODES[@]} -gt 0 ]]; then
    for pattern in "${TARGET_NODES[@]}"; do
        matched=false
        for name in "${!NODE_IPS[@]}"; do
            if [[ "$name" == *"$pattern"* ]]; then
                RESOLVED_NODES+=("$name")
                matched=true
            fi
        done
        if [[ "$matched" == false ]]; then
            echo "Error: no node matching '$pattern'" >&2
            echo "Available nodes:" >&2
            printf "  %s\n" "${!NODE_IPS[@]}" >&2
            exit 1
        fi
    done
else
    RESOLVED_NODES=("${!NODE_IPS[@]}")
fi

echo "  Target nodes: ${RESOLVED_NODES[*]}"
echo ""

# Handle --logs mode
if [[ -n "$LOGS_NODE" ]]; then
    matched_name=""
    for name in "${!NODE_IPS[@]}"; do
        if [[ "$name" == *"$LOGS_NODE"* ]]; then
            matched_name="$name"
            break
        fi
    done
    if [[ -z "$matched_name" ]]; then
        echo "Error: no node matching '$LOGS_NODE'" >&2
        exit 1
    fi
    echo "Tailing fake-kubelet logs on $matched_name..."
    ssh_node "${NODE_IPS[$matched_name]}" "sudo journalctl -f -u fake-kubelet 2>/dev/null || sudo tail -f /tmp/fake-kubelet.log 2>/dev/null || echo 'No logs found'"
    exit 0
fi

# Process each node
for node_name in "${RESOLVED_NODES[@]}"; do
    node_ip="${NODE_IPS[$node_name]}"
    echo "=== $node_name ($node_ip) ==="

    # Kill existing fake-kubelet
    echo "  Stopping existing fake-kubelet..."
    ssh_node "$node_ip" "sudo pkill -9 -f fake-kubelet 2>/dev/null; sleep 1; pgrep -f fake-kubelet >/dev/null 2>&1 && echo '  WARNING: still running' || echo '  Stopped'" 2>&1 || true

    if [[ "$KILL_ONLY" == true ]]; then
        echo ""
        continue
    fi

    # Remove old binary and copy new one
    echo "  Copying binary..."
    ssh_node "$node_ip" "rm -f ~/fake-kubelet" 2>&1
    scp_node "$BINARY_PATH" "$node_ip" "~/fake-kubelet"
    ssh_node "$node_ip" "chmod +x ~/fake-kubelet"

    # Run in background, logging to /tmp/fake-kubelet.log
    echo "  Starting fake-kubelet..."
    ssh_node "$node_ip" "sudo bash -c 'nohup /var/home/core/fake-kubelet > /tmp/fake-kubelet.log 2>&1 &'; sleep 2; pgrep -f fake-kubelet >/dev/null 2>&1 && echo '  Running (PID: \$(pgrep -f fake-kubelet | head -1))' || echo '  ERROR: failed to start'" 2>&1

    # Show first few lines of output
    echo "  Initial log output:"
    ssh_node "$node_ip" "head -5 /tmp/fake-kubelet.log 2>/dev/null" 2>&1 | sed 's/^/    /'
    echo ""
done

echo "Deployment complete."
echo ""
echo "Useful commands:"
echo "  View logs:  $(basename "$0") --logs <node-pattern>"
echo "  Kill all:   $(basename "$0") --kill-only"

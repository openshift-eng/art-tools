#!/usr/bin/env bash
#
# This script runs unit tests for all packages in parallel and displays a clean summary.
# It captures output to temporary files and shows results at the end.
#
# Usage:
#     ./run-tests-parallel.sh
#

set -euo pipefail

# Create a temporary directory for test outputs
tmpdir=$(mktemp -d)
trap "rm -rf $tmpdir" EXIT

echo "Running tests in parallel..."
echo ""

# Run tests in parallel, capturing output
packages=("artcommon" "doozer" "elliott" "pyartcd" "ocp-build-data-validator")
pids=()

for pkg in "${packages[@]}"; do
    echo "  Starting: $pkg"
    uv run pytest --verbose --color=yes "$pkg/tests/" > "$tmpdir/$pkg.out" 2>&1 &
    pids+=($!)
done

echo ""
echo "Waiting for tests to complete..."

# Wait for all background jobs and track which failed
failed=0
failed_packages=()
for i in "${!pids[@]}"; do
    pkg="${packages[$i]}"
    if ! wait "${pids[$i]}"; then
        failed=1
        failed_packages+=("$pkg")
    fi
done

# Display results
echo ""
echo "=========================================="
echo "Test Results Summary"
echo "=========================================="
echo ""

for pkg in "${packages[@]}"; do
    if grep -q "FAILED" "$tmpdir/$pkg.out" || grep -q "ERROR" "$tmpdir/$pkg.out"; then
        echo "❌ $pkg: FAILED"
    else
        passed=$(grep -oP '\d+(?= passed)' "$tmpdir/$pkg.out" | tail -1 || echo "0")
        echo "✅ $pkg: $passed tests passed"
    fi
done

echo ""

# If any tests failed, show full output for failed packages
if [ $failed -ne 0 ]; then
    echo "=========================================="
    echo "Failed Test Details"
    echo "=========================================="
    echo ""

    for pkg in "${failed_packages[@]}"; do
        echo "==================== $pkg ===================="
        cat "$tmpdir/$pkg.out"
        echo ""
    done

    exit 1
fi

echo "All tests passed! ✨"

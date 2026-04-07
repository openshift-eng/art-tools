#!/bin/bash
#
# Clean up old plashet revision directories, keeping the N most recent.
#
# Finds YYYY-MM/revision directories at depth 2, explicitly excludes latest/,
# protects whichever directory the latest symlink points to, and removes
# empty YYYY-MM parent directories afterwards.
#
# Usage: cleanup_plashets.sh BASE_DIR [KEEP] [--dry-run]
#   BASE_DIR  - the plashet base directory to clean
#   KEEP      - number of most recent revision dirs to keep (default: 3)
#   --dry-run - list what would be removed without deleting

set -euo pipefail

base_dir="${1:?Usage: cleanup_plashets.sh BASE_DIR [KEEP] [--dry-run]}"
keep="${2:-3}"
dry_run=false

for arg in "${@:3}"; do
    case "$arg" in
        --dry-run) dry_run=true ;;
    esac
done

if [[ ! -d "$base_dir" ]]; then
    echo "Base directory does not exist: $base_dir" >&2
    exit 0
fi

# Resolve what latest points to so we never delete it
protected=""
if [[ -L "$base_dir/latest" ]]; then
    protected=$(readlink "$base_dir/latest")
fi

# Match only YYYY-MM/revision directories (depth 2), excluding latest/.
# -regex matches paths like <base>/1234-56/789012... (digits-digits/digits).
# Sort lexicographically (works because timestamps are zero-padded).
# head -n -N is a no-op when there are N or fewer entries.
all_dirs=$(find "$base_dir" -maxdepth 2 -mindepth 2 -type d \
    -regextype posix-extended \
    -regex ".*/[0-9]+-[0-9]+/[0-9]+" \
    -not -path "$base_dir/latest/*" -printf "%P\n" \
    | sort)

total=$(echo "$all_dirs" | grep -c . || true)
to_remove=$(echo "$all_dirs" | head -n "-${keep}")
to_keep=$(echo "$all_dirs" | tail -n "${keep}")
removed=$(echo "$to_remove" | grep -c . || true)
echo "Found $total revision directories in $base_dir: removing $removed, keeping $((total - removed))"
[[ -n "$to_keep" ]] && echo "Keeping: $to_keep"

echo "$to_remove" | while read -r d; do
    [[ -z "$d" ]] && continue

    if [[ "$d" = "$protected" ]]; then
        echo "Skipping $d (target of latest symlink)"
        continue
    fi

    if $dry_run; then
        echo "Would remove $base_dir/$d"
    else
        echo "Removing $d"
        rm -rf -- "${base_dir:?}/$d"
    fi
done

# Remove empty YYYY-MM parent directories (but never latest)
if ! $dry_run; then
    find "$base_dir" -maxdepth 1 -mindepth 1 -type d \
        -not -name latest -empty -delete
fi

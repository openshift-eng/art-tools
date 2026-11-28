#!/bin/bash
#
# Clean up old plashet revision entries, keeping the N most recent.
#
# Finds YYYY-MM/revision entries at depth 2 -- both real directories AND the
# per-revision directory SYMLINKS that the embargoed-sibling optimization
# creates -- counting them together toward the keep window, explicitly excludes
# latest/, protects whichever directory the latest symlink points to, and
# removes empty YYYY-MM parent directories afterwards. Expired revision SYMLINKS
# are removed by unlinking the link only (never the public target they point
# to); expired revision DIRECTORIES are removed recursively.
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

# Also protect every revision in THIS repo that ANY live *sibling* symlink resolves into. An
# embargoed flavor may be published as a symlink to a public revision (see create_plashet_symlink
# in plashets.py): not only its `latest` symlink but also its per-revision directory symlinks
# (<sibling_base>/YYYY-MM/<rev>) can resolve, across directories, into one of this repo's
# revisions. The embargoed `latest` can advance to a newer revision while older embargoed
# revision symlinks (still inside the embargoed repo's keep window) keep pointing at public
# revisions here. Never delete such a revision, even when it falls outside this repo's keep
# window, or those sibling symlinks would dangle.
declare -A protected_rel
base_real=$(realpath -- "$base_dir" 2>/dev/null || echo "$base_dir")
parent_dir=$(dirname -- "$base_dir")

# Given a sibling symlink ($1) and the slug directory that owns it ($2), protect the revision it
# resolves to when that revision lives inside this repo's base dir.
protect_sibling_symlink() {
    local cand="$1" slug_dir="$2" target_real rel
    [[ -L "$cand" ]] || return 0
    # Skip symlinks that belong to our own repo; our own `latest` is handled via $protected above.
    if [[ "$slug_dir" -ef "$base_dir" ]]; then
        return 0
    fi
    target_real=$(realpath -- "$cand" 2>/dev/null) || return 0
    # Only protect targets that resolve inside this repo's base dir.
    if [[ "$target_real" == "$base_real/"* ]]; then
        rel="${target_real#"$base_real"/}"
        protected_rel["$rel"]=1
        echo "Protecting $rel (resolved target of sibling symlink $cand)"
    fi
}

shopt -s nullglob
# Sibling `latest` symlinks: <parent>/<slug>/latest
for sib_latest in "$parent_dir"/*/latest; do
    protect_sibling_symlink "$sib_latest" "$(dirname -- "$sib_latest")"
done
# Sibling per-revision symlinks: <parent>/<slug>/YYYY-MM/<rev>
for sib_rev in "$parent_dir"/*/*/*; do
    ym=$(basename -- "$(dirname -- "$sib_rev")")
    rev=$(basename -- "$sib_rev")
    # Only consider paths shaped like a revision directory (YYYY-MM/<rev>), skipping e.g.
    # latest/<arch> entries that the glob would otherwise reach.
    [[ "$ym" =~ ^[0-9]+-[0-9]+$ && "$rev" =~ ^[0-9]+$ ]] || continue
    protect_sibling_symlink "$sib_rev" "$(dirname -- "$(dirname -- "$sib_rev")")"
done
shopt -u nullglob

# Match only YYYY-MM/revision entries (depth 2), excluding latest/. Both real directories AND
# per-revision directory symlinks (created by the embargoed-sibling optimization) count toward the
# keep window -- without `-type l` the symlinks would never be counted or pruned, so their public
# targets would stay pinned forever and disk usage would grow without bound. find does NOT follow a
# matched symlink (no -L), so it lists the link itself as a leaf and never descends through it.
# -regex matches paths like <base>/1234-56/789012... (digits-digits/digits).
# Sort lexicographically (works because timestamps are zero-padded).
# head -n -N is a no-op when there are N or fewer entries.
all_dirs=$(find "$base_dir" -maxdepth 2 -mindepth 2 \( -type d -o -type l \) \
    -regextype posix-extended \
    -regex ".*/[0-9]+-[0-9]+/[0-9]+" \
    -not -path "$base_dir/latest/*" -printf "%P\n" \
    | sort)

total=$(echo "$all_dirs" | grep -c . || true)
to_remove=$(echo "$all_dirs" | head -n "-${keep}")
to_keep=$(echo "$all_dirs" | tail -n "${keep}")
removed=$(echo "$to_remove" | grep -c . || true)
echo "Found $total revision entries in $base_dir: removing $removed, keeping $((total - removed))"
[[ -n "$to_keep" ]] && echo "Keeping: $to_keep"

echo "$to_remove" | while read -r d; do
    [[ -z "$d" ]] && continue

    if [[ "$d" = "$protected" ]]; then
        echo "Skipping $d (target of latest symlink)"
        continue
    fi

    if [[ -n "${protected_rel[$d]:-}" ]]; then
        echo "Skipping $d (target of a live embargoed latest symlink)"
        continue
    fi

    # Remove a revision SYMLINK by unlinking the link ONLY -- never follow it, so the public target
    # it points to is left completely untouched. Remove a revision DIRECTORY recursively as before.
    if [[ -L "${base_dir}/$d" ]]; then
        if $dry_run; then
            echo "Would unlink $base_dir/$d (revision symlink)"
        else
            echo "Unlinking $d (revision symlink)"
            rm -f -- "${base_dir:?}/$d"
        fi
    else
        if $dry_run; then
            echo "Would remove $base_dir/$d"
        else
            echo "Removing $d"
            rm -rf -- "${base_dir:?}/$d"
        fi
    fi
done

# Remove empty YYYY-MM parent directories (but never latest). `-type d` only matches REAL
# directories, so a `latest` (or any other) symlink at depth 1 is neither followed nor deleted here;
# a YYYY-MM dir left empty after its revision symlinks were unlinked above is cleaned up normally.
if ! $dry_run; then
    find "$base_dir" -maxdepth 1 -mindepth 1 -type d \
        -not -name latest -empty -delete
fi

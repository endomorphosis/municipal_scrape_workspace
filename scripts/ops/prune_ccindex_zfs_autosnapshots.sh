#!/usr/bin/env bash
set -euo pipefail

APPLY=0
DATASETS=("storage/ccindex_parquet" "storage/ccindex_duckdb" "storage/ccindex")

usage() {
  cat <<'EOF'
Usage:
  prune_ccindex_zfs_autosnapshots.sh [--apply] [--dataset DATASET]...

Lists zfs-auto-snap_* snapshots for the given datasets. With --apply, destroys them.

Examples:
  ./prune_ccindex_zfs_autosnapshots.sh
  sudo ./prune_ccindex_zfs_autosnapshots.sh --apply --dataset storage/ccindex_parquet
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      APPLY=1
      shift
      ;;
    --dataset)
      DATASETS+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

# Unique datasets
readarray -t DATASETS < <(printf "%s\n" "${DATASETS[@]}" | awk 'NF' | sort -u)

snap_list() {
  local ds="$1"
  zfs list -H -t snapshot -o name,used,refer -r "$ds" 2>/dev/null | awk '$1 ~ /@zfs-auto-snap_/ {print}' || true
}

total=0
for ds in "${DATASETS[@]}"; do
  echo "== $ds"
  out=$(snap_list "$ds")
  if [[ -z "$out" ]]; then
    echo "(no zfs-auto-snap snapshots found)"
    continue
  fi
  echo "$out"
  count=$(echo "$out" | wc -l | tr -d ' ')
  echo "count=$count"
  total=$((total + count))

done

echo "total_snapshots=$total"

if [[ $APPLY -ne 1 ]]; then
  echo "dry_run=1 (pass --apply to destroy)"
  exit 0
fi

if [[ $total -eq 0 ]]; then
  echo "nothing to destroy"
  exit 0
fi

echo "DESTROYING zfs-auto-snap snapshots..."
for ds in "${DATASETS[@]}"; do
  snap_list "$ds" | awk '{print $1}' | while read -r snap; do
    [[ -n "$snap" ]] || continue
    echo "zfs destroy $snap"
    zfs destroy "$snap"
  done
done

echo "done"

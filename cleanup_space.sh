#!/bin/bash
# Free up space before building index

set -euo pipefail

echo "===================================================================================="
echo "SPACE CLEANUP"
echo "===================================================================================="
echo ""

# Check current disk usage
echo "Current disk usage:"
df -h /storage/ccindex /storage/ccindex_parquet /storage/ccindex_duckdb | tail -n +2
echo ""

FREED_SPACE=0

# 1. Delete ZFS snapshots for ccindex_duckdb
if zfs list -t snapshot | grep -q ccindex_duckdb; then
    echo "Deleting ZFS snapshots for ccindex_duckdb..."
    for snap in $(zfs list -t snapshot -o name | grep ccindex_duckdb); do
        SIZE=$(zfs list -t snapshot -o used -Hp "${snap}" 2>/dev/null || echo "0")
        echo "  Deleting: ${snap} ($(numfmt --to=iec ${SIZE} 2>/dev/null || echo ${SIZE}))"
        zfs destroy "${snap}" 2>/dev/null || echo "    Failed (may need sudo)"
    done
    echo ""
fi

# 2. Delete old backup index if it exists (AUTO-DELETE in overnight mode)
OLD_BACKUP="/storage/ccindex_duckdb/cc_pointers_by_year/cc_pointers_2024.duckdb.url_mode_backup_"*
if ls ${OLD_BACKUP} 2>/dev/null; then
    echo "Found old URL-mode backup indexes (166 GB each):"
    ls -lh ${OLD_BACKUP}
    echo ""
    echo "Auto-deleting backups to free space..."
    rm -f ${OLD_BACKUP}
    echo "  ✅ Deleted old backups"
    FREED_SPACE=$((FREED_SPACE + 166))
    echo ""
fi

# 3. Clean temporary files
echo "Cleaning temporary files..."
find /storage/ccindex_parquet -name "*.tmp" -type f -delete 2>/dev/null || true
find /storage/ccindex_parquet -name "*.sorted.tmp" -type f -delete 2>/dev/null || true
find /storage/ccindex_duckdb -name "*.tmp" -type f -delete 2>/dev/null || true
echo "  ✅ Cleaned temporary files"
echo ""

# 4. Remove old test/dev databases
echo "Found test/dev databases:"
ls -lh /storage/ccindex_duckdb/*.duckdb 2>/dev/null | grep -E "(test|dev)" || echo "  None"
echo ""

# Final disk usage
echo "Final disk usage:"
df -h /storage/ccindex /storage/ccindex_parquet /storage/ccindex_duckdb | tail -n +2
echo ""

if [ ${FREED_SPACE} -gt 0 ]; then
    echo "✅ Freed approximately ${FREED_SPACE} GB"
else
    echo "ℹ️  No significant space freed (may need manual cleanup)"
fi
echo ""
echo "===================================================================================="

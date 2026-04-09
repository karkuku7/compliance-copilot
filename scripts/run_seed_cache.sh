#!/bin/bash
# Cron wrapper for daily cache seeding.
#
# Features:
# - Pre-flight credential validation
# - Log rotation (14-day retention)
# - Per-owner execution with independent failure handling
# - Health check marker on success
#
# Crontab entry:
#   0 6 * * * /path/to/run_seed_cache.sh >> /path/to/logs/cron.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/../logs"
SEED_SCRIPT="${SCRIPT_DIR}/seed_cache.py"

mkdir -p "$LOG_DIR"

echo "=== Cache seed started at $(date -u +"%Y-%m-%dT%H:%M:%SZ") ==="

# --- Pre-flight: Validate credentials ---
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo "ERROR: AWS credentials are invalid or expired."
    echo "Run 'aws configure' or refresh your credential helper."
    exit 2
fi

# --- Log rotation: Delete logs older than 14 days ---
find "$LOG_DIR" -name "*.log" -mtime +14 -delete 2>/dev/null || true

# --- Run seed for each owner ---
OWNERS=("owner1" "owner2" "owner3")
EXIT_CODE=0
START_TIME=$(date +%s)

for owner in "${OWNERS[@]}"; do
    echo "--- Seeding owner: $owner ---"
    LOG_FILE="${LOG_DIR}/seed_${owner}_$(date +%Y%m%d).log"

    if python3 "$SEED_SCRIPT" \
        --owners "$owner" \
        --per-table \
        --chunk-size 0 \
        --timeout 1200 \
        --verbose \
        > "$LOG_FILE" 2>&1; then
        echo "  $owner: SUCCESS"
    else
        echo "  $owner: FAILED (see $LOG_FILE)"
        EXIT_CODE=1
    fi
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# --- Health check marker ---
if [ $EXIT_CODE -eq 0 ]; then
    date -u +"%Y-%m-%dT%H:%M:%SZ" > "${LOG_DIR}/last_success"
    echo "=== Cache seed completed successfully in ${DURATION}s ==="
else
    echo "=== Cache seed completed with errors in ${DURATION}s ==="
fi

exit $EXIT_CODE

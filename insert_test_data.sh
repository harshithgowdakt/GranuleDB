#!/usr/bin/env bash
# Insert multi-month test data into GranuleDB for testing partition/primary key pruning.
# Usage:
#   ./insert_test_data.sh              # insert data, keep existing table
#   RECREATE=1 ./insert_test_data.sh   # drop + recreate table first
#
# After inserting, try these queries to see pruning logs:
#
#   === Partition pruning (by month) ===
#
#   # Only scans January partition — other months pruned
#   curl -sS "$HOST/" -d "SELECT count(*) FROM events WHERE event_time >= 1704067200 AND event_time < 1706745600"
#
#   # Only scans February partition
#   curl -sS "$HOST/" -d "SELECT count(*) FROM events WHERE event_time >= 1706745600 AND event_time < 1709251200"
#
#   # Scans all partitions (no partition condition)
#   curl -sS "$HOST/" -d "SELECT count(*) FROM events"
#
#   === Primary key pruning (by workspace_id, user_id) ===
#
#   # Narrows granules via workspace_id (first ORDER BY column)
#   curl -sS "$HOST/" -d "SELECT * FROM events WHERE workspace_id = 1"
#
#   # Range on workspace_id
#   curl -sS "$HOST/" -d "SELECT * FROM events WHERE workspace_id >= 10 AND workspace_id <= 12"
#
#   # OR on workspace_id (new capability!)
#   curl -sS "$HOST/" -d "SELECT * FROM events WHERE workspace_id = 1 OR workspace_id = 25"
#
#   # Combined: partition + primary key pruning
#   curl -sS "$HOST/" -d "SELECT * FROM events WHERE event_time >= 1704067200 AND event_time < 1706745600 AND workspace_id = 5"
#
#   # Equality on user_id (second key column)
#   curl -sS "$HOST/" -d "SELECT * FROM events WHERE user_id = 100"
#
#   === Aggregation queries ===
#
#   # Per-workspace count with partition pruning (March only)
#   curl -sS "$HOST/" -d "SELECT workspace_id, count(*) FROM events WHERE event_time >= 1709251200 AND event_time < 1711929600 GROUP BY workspace_id ORDER BY workspace_id"
#
#   # Per-month row count (scans all)
#   curl -sS "$HOST/" -d "SELECT count(*) FROM events GROUP BY event_time"
#
#   === Column selection ===
#
#   # Only reads 2 columns from disk
#   curl -sS "$HOST/" -d "SELECT workspace_id, user_id FROM events WHERE workspace_id = 1 LIMIT 10"
#
#   # Reads all columns
#   curl -sS "$HOST/" -d "SELECT * FROM events LIMIT 5"

set -euo pipefail

HOST="${HOST:-http://127.0.0.1:8123}"
TABLE="events"
RECREATE="${RECREATE:-0}"

query() {
  curl -sS -X POST "$HOST/" --data-binary "$1"
}

if [[ "$RECREATE" == "1" ]]; then
  echo "Dropping table $TABLE..."
  query "DROP TABLE IF EXISTS $TABLE" || true
fi

echo "Creating table $TABLE (if not exists)..."
query "CREATE TABLE $TABLE (
  event_time DateTime,
  workspace_id UInt64,
  workspace_name String,
  user_id UInt64,
  user_name String,
  state_key String,
  state_value String
) ENGINE = MergeTree()
  ORDER BY (workspace_id, user_id, event_time)
  PARTITION BY toYYYYMM(event_time)" || true

# ---- Timestamps for each month (unix seconds) ----
# Jan 2024: 1704067200 (2024-01-01) to 1706745599 (2024-01-31)
# Feb 2024: 1706745600 (2024-02-01) to 1709251199 (2024-02-29)
# Mar 2024: 1709251200 (2024-03-01) to 1711929599 (2024-03-31)
# Apr 2024: 1711929600 (2024-04-01) to 1714521599 (2024-04-30)
# May 2024: 1714521600 (2024-05-01) to 1717199999 (2024-05-31)
# Jun 2024: 1717200000 (2024-06-01) to 1719791999 (2024-06-30)

MONTHS=(
  "1704067200:1706745599:Jan"
  "1706745600:1709251199:Feb"
  "1709251200:1711929599:Mar"
  "1711929600:1714521599:Apr"
  "1714521600:1717199999:May"
  "1717200000:1719791999:Jun"
)

STATE_KEYS=("login_status" "engaged_status" "provisioned_status" "app_enabled_status")
STATE_VALUES_0=("logged in" "logged out")
STATE_VALUES_1=("engaged" "not engaged")
STATE_VALUES_2=("enabled" "disabled")
STATE_VALUES_3=("enabled" "disabled")

ROWS_PER_MONTH=20000
BATCH=1000

echo ""
echo "Inserting $ROWS_PER_MONTH rows x ${#MONTHS[@]} months = $(( ROWS_PER_MONTH * ${#MONTHS[@]} )) total rows..."
echo ""

for month_spec in "${MONTHS[@]}"; do
  IFS=':' read -r ts_start ts_end month_name <<< "$month_spec"
  ts_range=$((ts_end - ts_start))

  inserted=0
  while (( inserted < ROWS_PER_MONTH )); do
    remaining=$((ROWS_PER_MONTH - inserted))
    batch_size=$((remaining < BATCH ? remaining : BATCH))

    values=""
    for ((n=0; n<batch_size; n++)); do
      # Random timestamp within this month
      ts=$(( ts_start + RANDOM % ts_range ))

      # Workspace 1-25, user 1-5000 (matching insert.sh distribution)
      workspace_id=$(( 1 + RANDOM % 25 ))
      workspace_name="workspace_${workspace_id}"
      user_id=$(( 1 + RANDOM % 5000 ))
      user_name="user_${user_id}"

      # Random state
      sk_idx=$(( RANDOM % 4 ))
      state_key="${STATE_KEYS[$sk_idx]}"
      sv_idx=$(( RANDOM % 2 ))
      case $sk_idx in
        0) state_value="${STATE_VALUES_0[$sv_idx]}" ;;
        1) state_value="${STATE_VALUES_1[$sv_idx]}" ;;
        2) state_value="${STATE_VALUES_2[$sv_idx]}" ;;
        *) state_value="${STATE_VALUES_3[$sv_idx]}" ;;
      esac

      row="($ts, $workspace_id, '$workspace_name', $user_id, '$user_name', '$state_key', '$state_value')"
      if [[ -z "$values" ]]; then
        values="$row"
      else
        values="$values, $row"
      fi
    done

    sql="INSERT INTO $TABLE (event_time, workspace_id, workspace_name, user_id, user_name, state_key, state_value) VALUES $values"
    resp=$(query "$sql" || true)
    inserted=$((inserted + batch_size))
    echo "  [$month_name 2024] inserted $inserted/$ROWS_PER_MONTH rows -> $resp"
  done
done

echo ""
echo "=== Done! Total: $(( ROWS_PER_MONTH * ${#MONTHS[@]} )) rows across ${#MONTHS[@]} months ==="
echo ""
echo "Try these queries (server logs show pruning decisions):"
echo ""
echo "--- Partition pruning ---"
echo "curl -sS '$HOST/' -d 'SELECT count(*) FROM events WHERE event_time >= 1704067200 AND event_time < 1706745600'"
echo "  ^ Only scans January partition"
echo ""
echo "curl -sS '$HOST/' -d 'SELECT count(*) FROM events WHERE event_time >= 1709251200 AND event_time < 1711929600'"
echo "  ^ Only scans March partition"
echo ""
echo "--- Primary key pruning ---"
echo "curl -sS '$HOST/' -d \"SELECT * FROM events WHERE workspace_id = 1 LIMIT 10\""
echo "  ^ Narrows granules by first ORDER BY column"
echo ""
echo "curl -sS '$HOST/' -d \"SELECT * FROM events WHERE workspace_id >= 10 AND workspace_id <= 12 LIMIT 10\""
echo "  ^ Range scan on workspace_id"
echo ""
echo "curl -sS '$HOST/' -d \"SELECT * FROM events WHERE workspace_id = 1 OR workspace_id = 25 LIMIT 10\""
echo "  ^ OR pruning (new!)"
echo ""
echo "--- Combined partition + primary key ---"
echo "curl -sS '$HOST/' -d \"SELECT * FROM events WHERE event_time >= 1704067200 AND event_time < 1706745600 AND workspace_id = 5 LIMIT 10\""
echo "  ^ Jan partition only + workspace_id=5 granule pruning"
echo ""
echo "--- Column selection ---"
echo "curl -sS '$HOST/' -d 'SELECT workspace_id, user_id FROM events WHERE workspace_id = 1 LIMIT 10'"
echo "  ^ Only reads 2 columns from disk"

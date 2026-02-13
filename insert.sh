#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-http://127.0.0.1:8123}"
TABLE="${TABLE:-events}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
SLEEP_SECS="${SLEEP_SECS:-0.1}"
RECREATE_TABLE="${RECREATE_TABLE:-0}"

if [[ "$RECREATE_TABLE" == "1" ]]; then
  curl -sS -X POST "$HOST/" --data-binary "DROP TABLE IF EXISTS $TABLE" >/dev/null || true
fi

# Create table once (ignore errors if it already exists).
curl -sS -X POST "$HOST/" --data-binary \
"CREATE TABLE $TABLE (event_time DateTime, workspace_id UInt64, workspace_name String, user_id UInt64, user_name String, state_key String, state_value String) ENGINE = MergeTree() ORDER BY (workspace_id, user_id, event_time)" >/dev/null || true

while true; do
  ts=$(date +%s)
  values=""

  for ((n=0; n<BATCH_SIZE; n++)); do
    workspace_id=$((1 + RANDOM % 25))
    workspace_name="workspace_${workspace_id}"
    user_id=$((1 + RANDOM % 5000))
    user_name="user_${user_id}"

    case $((RANDOM % 4)) in
      0)
        state_key="login_status"
        case $((RANDOM % 2)) in
          0) state_value="logged in" ;;
          *) state_value="logged out" ;;
        esac
        ;;
      1)
        state_key="engaged_status"
        case $((RANDOM % 2)) in
          0) state_value="engaged" ;;
          *) state_value="not engaged" ;;
        esac
        ;;
      2)
        state_key="provisioned_status"
        case $((RANDOM % 2)) in
          0) state_value="enabled" ;;
          *) state_value="disabled" ;;
        esac
        ;;
      *)
        state_key="app_enabled_status"
        case $((RANDOM % 2)) in
          0) state_value="enabled" ;;
          *) state_value="disabled" ;;
        esac
        ;;
    esac

    row="($ts, $workspace_id, '$workspace_name', $user_id, '$user_name', '$state_key', '$state_value')"

    if [[ -z "$values" ]]; then
      values="$row"
    else
      values="$values, $row"
    fi
  done

  sql="INSERT INTO $TABLE (event_time, workspace_id, workspace_name, user_id, user_name, state_key, state_value) VALUES $values"
  resp=$(curl -sS -X POST "$HOST/" --data-binary "$sql" || true)
  echo "[$(date '+%F %T')] inserted $BATCH_SIZE rows -> $resp"
  sleep "$SLEEP_SECS"
done

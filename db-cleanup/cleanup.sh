#!/bin/sh
MAX_ROWS=${MAX_ROWS:-10000000} #Deletes oldest data when there are more than this number of rows in temperature_averages
FRACTION=${DELETE_FRACTION:-0.2} #Deletes this fraction of oldest data
INTERVAL=${CHECK_INTERVAL:-3600} #How often this check is executed in seconds

while true; do
  rows_amount=$(psql -tAc "SELECT count(*) FROM temperature_averages")
  disk_usage=$(df -P /data | awk 'NR==2 {print $5}')
  echo "$(date -u '+%F %T') rows ${rows_amount} (max ${MAX_ROWS}), disk usage ${disk_usage}"

  if [ -n "$rows_amount" ] && [ "$rows_amount" -ge "$MAX_ROWS" ]; then
    if [ "${DRY_RUN:-1}" = "1" ]; then
      echo "DRY RUN: would delete rows older than:"
      psql -c "SELECT percentile_disc(${FRACTION}) WITHIN GROUP (ORDER BY timestamp) AS cutoff FROM temperature_averages;"
    else
      echo "Deleting oldest ${FRACTION} of data..."
      psql -v ON_ERROR_STOP=1 -v frac="${FRACTION}" <<'SQL'
SELECT percentile_disc(:frac) WITHIN GROUP (ORDER BY timestamp) AS cutoff FROM temperature_averages \gset
DELETE FROM temperature_averages WHERE timestamp < :'cutoff';
DELETE FROM events WHERE timestamp < :'cutoff';
VACUUM temperature_averages;
VACUUM events;
SQL
    fi
  fi
  sleep "$INTERVAL"
done
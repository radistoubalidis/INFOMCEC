import logging
import os
import signal
import time

import redis
import requests


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("notifier")

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
NOTIFICATIONS_URL = os.environ.get('NOTIFICATIONS_URL', "http://notifications-api:3000/notify")
TOKEN_FILE = os.environ.get("TOKEN_FILE", "/usr/src/auth/token")

AVERAGES_STREAM = os.environ.get("AVERAGES_STREAM", "averages")
DELIVERY_STREAM = "notifications:outbox"

DETECTION_GROUP = "notification-detection"
DELIVERY_GROUP = "notification-delivery"

WORKER = os.environ.get("HOSTNAME", "notifier")
RETRY_IDLE_MS = 10_000

running = True

redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5,
)

with open(TOKEN_FILE) as token_file:
    token = token_file.read().strip()


# Atomically:
# 1. Evaluate an average and update detection state.
# 2. Queue a notification when required.
# 3. Acknowledge the input average.
#
# KEYS: state hash, averages stream, notification outbox stream
# ARGV: group, message ID, timestamp, in_range, phase,
#       researcher, experiment_id, measurement_id, cipher_data
DETECT_LUA = """
local ts = tonumber(ARGV[3])
local last = redis.call('HGET', KEYS[1], 'last_ts')

if last and ts <= tonumber(last) then
    redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
    return 'duplicate_or_late'
end

local kind = ''

if ARGV[5] == 'stab' then
    local stabilized = redis.call('HGET', KEYS[1], 'stabilized')

    if ARGV[4] == '1' and stabilized ~= '1' then
        redis.call('HSET', KEYS[1], 'stabilized', '1')
        kind = 'Stabilized'
    end
else
    local previous = redis.call('HGET', KEYS[1], 'in_range')
    redis.call('HSET', KEYS[1], 'in_range', ARGV[4])

    if ARGV[4] == '0' and (not previous or previous == '1') then
        kind = 'OutOfRange'
    end
end

redis.call('HSET', KEYS[1], 'last_ts', ARGV[3])

if kind ~= '' then
    redis.call(
        'XADD', KEYS[3], '*',
        'notification_type', kind,
        'researcher', ARGV[6],
        'experiment_id', ARGV[7],
        'measurement_id', ARGV[8],
        'cipher_data', ARGV[9]
    )
end

redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
return kind
"""

detect_script = redis_client.register_script(DETECT_LUA)


def stop(signum, frame):
    global running
    running = False
    log.info("Shutdown requested")


def ensure_group(stream, group):
    try:
        redis_client.xgroup_create(
            stream,
            group,
            id="0-0",
            mkstream=True,
        )
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


class StreamReader:
    """Read new entries and reclaim abandoned pending entries."""

    def __init__(self, stream, group):
        self.stream = stream
        self.group = group
        self.claim_cursor = "0-0"

    def read(self, count=50, block=None):
        claimed = redis_client.xautoclaim(
            self.stream,
            self.group,
            WORKER,
            min_idle_time=RETRY_IDLE_MS,
            start_id=self.claim_cursor,
            count=count,
        )

        self.claim_cursor = claimed[0]
        entries = claimed[1]

        if entries:
            return entries

        batches = redis_client.xreadgroup(
            groupname=self.group,
            consumername=WORKER,
            streams={self.stream: ">"},
            count=count,
            block=block,
        )

        return [
            entry
            for _, batch in batches
            for entry in batch
        ]


def process_average(message_id, fields):
    required = {
        "experiment_id",
        "researcher",
        "measurement_id",
        "hash",
        "timestamp",
        "in_range",
        "phase",
    }

    missing = required - fields.keys()
    if missing:
        raise ValueError(
            f"Average {message_id} missing fields: {sorted(missing)}"
        )

    if fields["phase"] not in {"stab", "run"}:
        raise ValueError(f"Invalid phase in average {message_id}")

    if fields["in_range"] not in {"0", "1"}:
        raise ValueError(f"Invalid in_range in average {message_id}")

    # Validate before invoking the Lua script.
    float(fields["timestamp"])

    result = detect_script(
        keys=[
            f"notification-state:{fields['experiment_id']}",
            AVERAGES_STREAM,
            DELIVERY_STREAM,
        ],
        args=[
            DETECTION_GROUP,
            message_id,
            fields["timestamp"],
            fields["in_range"],
            fields["phase"],
            fields["researcher"],
            fields["experiment_id"],
            fields["measurement_id"],
            fields["hash"],
        ],
    )

    if result == "duplicate_or_late":
        log.debug("Skipped duplicate/late average %s", message_id)
    elif result:
        log.info(
            "Queued %s for experiment %s",
            result,
            fields["experiment_id"],
        )


def deliver(http, message_id, payload):
    try:
        debug = os.environ.get('DEBUG', False)
        ntf_url = f"{NOTIFICATIONS_URL}" if debug == True else f"{NOTIFICATIONS_URL}?token={token}"
        response = http.post(
            url=ntf_url,
            json=payload,
            timeout=(3, 5),
            allow_redirects=False,
        )

        if not 200 <= response.status_code < 300:
            # Leave pending so it can be retried.
            # Avoid logging the request URL because it contains the token.
            print(response.content)
            log.warning(
                "Delivery %s returned HTTP %s; will retry",
                message_id,
                response.status_code,
            )
            return

    except requests.RequestException as exc:
        log.warning(
            "Delivery %s failed (%s); will retry",
            message_id,
            type(exc).__name__,
        )
        return

    redis_client.xack(
        DELIVERY_STREAM,
        DELIVERY_GROUP,
        message_id,
    )

    log.info(
        "Delivered %s for experiment %s",
        payload["notification_type"],
        payload["experiment_id"],
    )


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    redis_client.ping()

    ensure_group(AVERAGES_STREAM, DETECTION_GROUP)
    ensure_group(DELIVERY_STREAM, DELIVERY_GROUP)

    averages = StreamReader(AVERAGES_STREAM, DETECTION_GROUP)
    deliveries = StreamReader(DELIVERY_STREAM, DELIVERY_GROUP)

    with requests.Session() as http:
        while running:
            try:
                for message_id, fields in averages.read(block=500):
                    if not running:
                        break

                    try:
                        process_average(message_id, fields)
                    except ValueError as exc:
                        # Keep invalid entries pending for investigation.
                        log.error("%s", exc)

                # Send sequentially; no premature acknowledgement.
                for message_id, payload in deliveries.read(count=10):
                    if not running:
                        break
                    deliver(http, message_id, payload)

            except redis.RedisError:
                log.exception("Redis operation failed")
                time.sleep(2)

    redis_client.close()


if __name__ == "__main__":
    main()
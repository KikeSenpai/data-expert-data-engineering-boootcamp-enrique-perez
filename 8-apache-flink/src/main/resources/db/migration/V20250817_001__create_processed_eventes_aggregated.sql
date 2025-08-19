CREATE TABLE IF NOT EXISTS processed_events_aggregated_host (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT,
    PRIMARY KEY (event_hour, host)
);

CREATE TABLE IF NOT EXISTS processed_events_aggregated_host_referrer (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    referrer VARCHAR,
    num_hits BIGINT,
    PRIMARY KEY (event_hour, host, referrer)
);
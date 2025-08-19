-- Creates the target table for sessionized web events produced by SessionizationPipeline
CREATE TABLE IF NOT EXISTS processed_events_sessions (
    ip TEXT,
    host TEXT,
    session_start TIMESTAMP(3) NOT NULL,
    session_end TIMESTAMP(3) NOT NULL,
    events_in_session BIGINT NOT NULL,
    CONSTRAINT processed_events_sessions_pk PRIMARY KEY (ip, host, session_start)
);
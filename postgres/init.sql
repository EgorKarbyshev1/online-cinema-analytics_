CREATE TABLE IF NOT EXISTS aggregates (
    date date NOT NULL,
    metric_name text NOT NULL,
    dimension text NOT NULL,
    value double precision,
    calculated_at timestamptz NOT NULL,
    PRIMARY KEY (date, metric_name, dimension)
);

CREATE TABLE IF NOT EXISTS daily_exports (
    date date PRIMARY KEY,
    scheduled boolean NOT NULL DEFAULT false,
    exported_at timestamptz
);

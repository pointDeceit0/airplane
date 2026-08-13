CREATE TABLE IF NOT EXISTS raw.air_flights (
	icao24 String,
	first_seen Nullable(Int64),
	last_seen Nullable(Int64),t
	est_departure_airport String,
	est_arrival_airport String,
	callsign String,
	est_departure_airport_horiz_distance Nullable(Int64),
	est_departure_airport_vert_distance Nullable(Int64),
	est_arrival_airport_horiz_distance Nullable(Int64),
	est_arrival_airport_vert_distance Nullable(Int64),
	departure_airport_candidates_count Nullable(Int64),
	arrival_airport_candidates_count Nullable(Int64)
) ENGINE = Kafka
SETTINGS
	kafka_broker_list = 'kafka:9092',
	kafka_topic_list = 'raw_opensky_telemetry',
	kafka_group_name = 'clickhouse',
	kafka_format = 'JSONEachRow',
	kafka_skip_broken_messages = 100,
	kafka_max_block_size = 500;

CREATE TABLE dds.air_flights (
    icao24 String,
	first_seen Int64,
	last_seen Int64,
	est_departure_airport String,
	est_arrival_airport String,
	callsign String,
	est_departure_airport_horiz_distance Int64,
	est_departure_airport_vert_distance Int64,
	est_arrival_airport_horiz_distance Int64,
	est_arrival_airport_vert_distance Int64,
	departure_airport_candidates_count Int64,
	arrival_airport_candidates_count Int64
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE MATERIALIZED VIEW dds.air_flights_mv
TO dds.air_flights AS
SELECT lower(icao24) AS icao24,
	   first_seen,
	   last_seen,
	   upper(est_departure_airport) AS est_departure_airport,
	   upper(est_arrival_airport) AS est_arrival_airport,
	   upper(callsign) AS callsign,
	   COALESCE(est_departure_airport_horiz_distance, -1) AS est_departure_airport_horiz_distance,
	   COALESCE(est_departure_airport_vert_distance, -1) AS est_departure_airport_vert_distance,
	   COALESCE(est_arrival_airport_horiz_distance, -1) AS est_arrival_airport_horiz_distance,
	   COALESCE(est_arrival_airport_vert_distance, -1) AS est_arrival_airport_vert_distance,
	   COALESCE(departure_airport_candidates_count, -1) AS departure_airport_candidates_count,
	   COALESCE(arrival_airport_candidates_count, -1) AS arrival_airport_candidates_count
FROM raw.air_flights
WHERE first_seen IS NOT NULL AND last_seen IS NOT NULL;

CREATE TABLE ref.dict_air_airports (
    iata FixedString(3),
    icao FixedString(4),
    name String,
    country LowCardinality(FixedString(2)), -- matches int to String
    city String,
    sub_division String,
    elevation Float32,
    latitude Float32,
    longitude Float32,
    tz LowCardinality(String),
    lid String
)
ENGINE = MergeTree()
ORDER BY iata;
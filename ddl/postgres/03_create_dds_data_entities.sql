CREATE TABLE dds_data.air_interval_flights_parsed (
	icao24 varchar(6) NOT NULL,
	first_seen int8 NULL,
	last_seen int8 NOT NULL,
	est_departure_airport varchar NULL,
	est_arrival_airport varchar NULL,
	callsign varchar(8) NULL,
	est_departure_airport_horiz_distance int8 NULL,
	est_departure_airport_vert_distance int8 NULL,
	est_arrival_airport_horiz_distance int8 NULL,
	est_arrival_airport_vert_distance int8 NULL,
	departure_airport_candidates_count int8 NULL,
	arrival_airport_candidates_count int8 NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT air_interval_flights_pkey PRIMARY KEY (icao24, last_seen)
);
CREATE TRIGGER air_put_icao24_to_path_queue AFTER
INSERT
    ON
    dds_data.air_interval_flights_parsed FOR EACH ROW EXECUTE FUNCTION dds_data.air_put_icao24_to_path_queue();

CREATE OR REPLACE FUNCTION dds_data.air_put_icao24_to_path_queue()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN

	IF (SELECT is_processed FROM tech_data.air_path_getting_queue WHERE icao24 = NEW.icao24) IS NULL THEN
		INSERT INTO tech_data.air_path_getting_queue (icao24, last_seen)
		VALUES (NEW.icao24, NEW.last_seen);
	ELSEIF NOT (SELECT is_processed FROM tech_data.air_path_getting_queue WHERE icao24 = NEW.icao24) THEN
		UPDATE tech_data.air_path_getting_queue
		SET last_seen = NEW.last_seen,
			updated_at = NEW.created_at
		WHERE icao24 = NEW.icao24;
	END IF;

	RETURN NEW;
END;
$function$
;

CREATE TABLE dds_data.air_aircraft_paths_parsed (
	icao24 varchar(6) NOT NULL,
	start_time int8 NOT NULL,
	end_time int8 NOT NULL,
	callsign varchar(8) NULL,
	"time" int8 NOT NULL,
	latitude float8 NULL,
	longitude float8 NULL,
	baro_altitude float8 NULL,
	true_track float8 NULL,
	is_onground bool NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT air_aircrafts_paths_pkey PRIMARY KEY (icao24, "time")
);
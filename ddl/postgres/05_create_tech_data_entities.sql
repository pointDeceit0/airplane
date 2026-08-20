CREATE TABLE tech_data.air_path_getting_queue (
	icao24 varchar(6) NOT NULL,
	last_seen float8 NOT NULL,
	is_processed bool DEFAULT false NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT air_path_getting_queue_pkey PRIMARY KEY (icao24)
);
CREATE INDEX idx_air_path_getting_queue ON tech_data.air_path_getting_queue USING btree (last_seen) WHERE (is_processed IS FALSE);
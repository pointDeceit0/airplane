CREATE TABLE raw_data.air_aircraft_paths (
	id int4 DEFAULT nextval('raw_data.air_aircrafts_paths_id_seq'::regclass) NOT NULL,
	raw_data jsonb NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT air_aircrafts_paths_pkey PRIMARY KEY (id)
);

CREATE TABLE raw_data.air_interval_flights (
	id serial4 NOT NULL,
	raw_data jsonb NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT air_interval_flights_pkey PRIMARY KEY (id)
);
CREATE TABLE tech_data.air_path_getting_queue (
	icao24 varchar(6) NOT NULL,
	last_seen float8 NOT NULL,
	is_processed bool DEFAULT false NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT air_path_getting_queue_pkey PRIMARY KEY (icao24)
);
CREATE INDEX idx_air_path_getting_queue ON tech_data.air_path_getting_queue USING btree (last_seen) WHERE (is_processed IS FALSE);

CREATE TABLE tech_data.tech_raw_to_dds_procedures (
	id bigserial NOT NULL,
	is_enabled bool NOT NULL,
	table_from varchar(126) NOT NULL,
	table_to varchar(126) NOT NULL,
	procedure_name varchar(126) NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
	CONSTRAINT tech_dds_to_raw_procedures_pkey PRIMARY KEY (id),
	CONSTRAINT unq_tech_dds_to_raw_procedures_table_from_to_proc UNIQUE (table_from, table_to)
);
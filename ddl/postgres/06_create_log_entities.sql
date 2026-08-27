CREATE TABLE log.log_air_raw_to_dds (
	id bigserial NOT NULL,
	process_id int8 NOT NULL,
	procedure_name varchar(126) NOT NULL,
	table_from varchar(126) NOT NULL,
	table_to varchar(126) NOT NULL,
	data_date_from timestamp NOT NULL,
	data_date_to timestamp NOT NULL,
	status varchar(20) NOT NULL,
	error_message varchar NULL,
	started_at timestamp DEFAULT now() NOT NULL,
	finished_at timestamp NULL,
	rows_read int8 NULL,
	rows_inserted int8 NULL,
	updated_by varchar(50) DEFAULT CURRENT_USER NOT NULL,
	CONSTRAINT log_air_raw_to_dds_pkey PRIMARY KEY (id),
	CONSTRAINT log_air_raw_to_dds_status_check CHECK (((status)::text = ANY ((ARRAY['RUNNING'::character varying, 'SUCCESS'::character varying, 'FAILED'::character varying])::text[]))),
	CONSTRAINT log_air_raw_to_dds_process_id_fkey FOREIGN KEY (process_id) REFERENCES tech_data.tech_raw_to_dds_procedures(id)
);
CREATE INDEX idx_air_raw_to_dds_proc_started_at ON log.log_air_raw_to_dds USING btree (procedure_name, started_at DESC);
CREATE INDEX idx_air_raw_to_dds_status ON log.log_air_raw_to_dds USING btree (status) WHERE ((status)::text = 'FAILED'::text);
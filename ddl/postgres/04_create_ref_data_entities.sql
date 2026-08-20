CREATE TABLE ref_data.dict_air_airports (
	id bigserial NOT NULL,
	iata varchar(3) NOT NULL,
	icao varchar(4) NOT NULL,
	"name" varchar NOT NULL,
	country varchar(2) NOT NULL,
	city varchar NULL,
	sub_division varchar NULL,
	elevation float8 NOT NULL,
	latitude float8 NOT NULL,
	longitude float8 NOT NULL,
	tz varchar NOT NULL,
	lid varchar NULL,
	CONSTRAINT dict_air_airports_iata_key UNIQUE (iata),
	CONSTRAINT dict_air_airports_icao_key UNIQUE (icao),
	CONSTRAINT dict_air_airports_pkey PRIMARY KEY (id)
);
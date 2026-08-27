-- DROP PROCEDURE raw_data.air_raw_to_dds_flights(timestamp, timestamp, int8);

CREATE OR REPLACE PROCEDURE raw_data.air_raw_to_dds_flights(IN p_date_start timestamp without time zone, IN p_date_end timestamp without time zone, IN p_id bigint)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	v_log_id int8;

	v_raw_rows int8;
	v_inserted_count int8;

	v_is_error boolean := FALSE;
	v_err_msg text;
BEGIN

	INSERT INTO log.log_air_raw_to_dds (process_id, procedure_name, table_from, table_to, data_date_from, data_date_to, status)
	SELECT p_id, t.procedure_name, t.table_from, t.table_to, p_date_start, p_date_end, 'RUNNING'::varchar
	FROM tech_data.tech_raw_to_dds_procedures t
	WHERE t.id = p_id
	RETURNING id INTO v_log_id;

	SELECT count(1) INTO v_raw_rows
	FROM raw_data.air_interval_flights
	WHERE created_at BETWEEN p_date_end - INTERVAL '2 days' AND p_date_end;

	RAISE NOTICE 'Number of inserting rows by plan: %', v_raw_rows;
	UPDATE log.log_air_raw_to_dds SET rows_read = v_raw_rows WHERE id = v_log_id;

COMMIT;

BEGIN

	INSERT INTO dds_data.air_interval_flights_parsed (icao24, first_seen, last_seen, est_departure_airport, est_arrival_airport, callsign, est_departure_airport_horiz_distance, est_departure_airport_vert_distance, est_arrival_airport_horiz_distance, est_arrival_airport_vert_distance, departure_airport_candidates_count, arrival_airport_candidates_count, created_at)
	SELECT lower(btrim(raw_data->>'icao24'))::varchar(6) AS icao24,
		   (raw_data->>'first_seen')::int8 AS first_seen,
		   (raw_data->>'last_seen')::int8 AS last_seen,
		   upper(raw_data->>'est_departure_airport')::varchar AS est_departure_airport,
		   upper(raw_data->>'est_arrival_airport')::varchar AS est_arrival_airport,
		   upper(raw_data->>'callsign') AS callsign,
		   (raw_data->>'est_departure_airport_horiz_distance')::int8 AS est_departure_airport_horiz_distance,
		   (raw_data->>'est_departure_airport_vert_distance')::int8 AS est_departure_airport_vert_distance,
		   (raw_data->>'est_arrival_airport_horiz_distance')::int8 AS est_arrival_airport_horiz_distance,
		   (raw_data->>'est_arrival_airport_vert_distance')::int8 AS est_arrival_airport_vert_distance,
		   (raw_data->>'departure_airport_candidates_count')::int8 AS departure_airport_candidates_count,
		   (raw_data->>'arrival_airport_candidates_count')::int8 AS arrival_airport_candidates_count,
		   now()
	FROM raw_data.air_interval_flights
	WHERE raw_data->>'first_seen' IS NOT NULL AND raw_data->>'last_seen' IS NOT NULL
		  AND created_at BETWEEN p_date_end - INTERVAL '2 days' AND p_date_end
	ON CONFLICT DO NOTHING;

	GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
	RAISE NOTICE 'Rows inserted: %', v_inserted_count;

EXCEPTION WHEN OTHERS THEN
	v_is_error = true;
	GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
END;

	IF v_is_error THEN

		UPDATE log.log_air_raw_to_dds
        SET status = 'FAILED',
            error_message = v_err_msg,
            finished_at = now()
        WHERE id = v_log_id;

		COMMIT;

		RAISE EXCEPTION 'Procedure failed: %', v_err_msg;
	ELSE

		UPDATE log.log_air_raw_to_dds
		SET rows_inserted = v_inserted_count,
			status = 'SUCCESS',
			finished_at = now()
		WHERE id = v_log_id;

	END IF;

END;
$procedure$
;

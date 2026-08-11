import json
from pathlib import Path
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.hooks.base import BaseHook
from airflow.providers.http.sensors.http import HttpSensor
from opensky_api import OpenSkyApi

from utils.token_manager import TokenManager
from utils.kafka_send import AirKafkaProducer


AIRFLOW_RAW_DATA_PATH = r'/opt/airflow/raw_data'
TARGET_TOPIC = 'raw_opensky_telemetry'
DAY_LAG = 2


default_args = {
    'owner': 'admin',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}


@dag(dag_id='dag_get_flights_from_interval_api',
     default_args=default_args,
     start_date=datetime(year=2026, month=7, day=31, hour=0),
     # every 15 minutes
     schedule=CronDataIntervalTimetable('*/15 * * * *', timezone='UTC'),
     catchup=False,
     max_active_runs=1)
def flights():
    """
    1. Checks connection
    2. Gets data from api and saves it
    3. Reads recieved data and writes it to kafka
    """

    check_api_ready = HttpSensor(
        task_id='check_api_ready',
        http_conn_id='opensky_api',
        endpoint='',
        method='GET',
    )

    @task()
    def get_write_flights_from_api(**context) -> str:
        # Get interval
        start_interval = context['data_interval_start']
        end_interval = context['data_interval_end']
        print(f'Interval start:\t{start_interval.strftime("%Y-%m-%d %H:%M:%S")}')
        print(f'Interval end:\t{end_interval.strftime("%Y-%m-%d %H:%M:%S")}')

        # Get data through api
        token_manager = TokenManager()
        with OpenSkyApi(token_manager=token_manager) as api:
            states = api.get_flights_from_interval(int(start_interval.timestamp()), int(end_interval.timestamp()))

        # Check if data is not None
        if states is None:
            print("No records recieved")
            return None
        print(f"Records recieved:\t{len(states)}")

        # Transform OpenSkyAPI vector into default list of dictionaries
        result = []
        for s in states:
            result.append({
                "icao24": s.icao24,
                "first_seen": s.firstSeen,
                "est_departure_airport": s.estDepartureAirport,
                "last_seen": s.lastSeen,
                "est_arrival_airport": s.estArrivalAirport,
                "callsign": s.callsign,
                "est_departure_airport_horiz_distance": s.estDepartureAirportHorizDistance,
                "est_departure_airport_vert_distance": s.estDepartureAirportVertDistance,
                "est_arrival_airport_horiz_distance": s.estArrivalAirportHorizDistance,
                "est_arrival_airport_vert_distance": s.estArrivalAirportVertDistance,
                "departure_airport_candidates_count": s.departureAirportCandidatesCount,
                "arrival_airport_candidates_count": s.arrivalAirportCandidatesCount,
            })

        # Create new folder if not exists
        new_folder = Path(f"{AIRFLOW_RAW_DATA_PATH}/{start_interval.strftime("%Y_%m_%d")}")
        if not new_folder.exists():
            new_folder.mkdir(parents=False, exist_ok=False)
            print(f"Created path:\t{new_folder}")
        else:
            print(f"Path {new_folder} already exists")

        # Open new file
        new_json = new_folder / f"{start_interval.strftime("%H-%M")}_{end_interval.strftime("%H-%M")}.json"
        with open(new_json, 'w', encoding="utf-8") as f:
            json.dump(result, f, indent=2)

        return str(new_json)

    @task
    def send_data_to_kafka(path: str) -> None:
        with open(path, 'r', encoding='utf-8') as f:
            d = json.load(f)

        conn = BaseHook.get_connection('kafka_conn')

        producer = AirKafkaProducer(topic=TARGET_TOPIC, network=conn.host)

        for i, rec in enumerate(d):
            producer.produce(json.dumps(rec))

            if i % 1000 == 0:
                # Clears kafka pipe if there any errors, callbacks delivery_report,
                #   if you don't do that queue may fullfill
                producer.poll()
        # waits till buffer will be empty (all messages sent)
        producer.flush()

    # check_api_ready >> send_data_to_kafka(get_write_flights_from_api())

    generated_file_path = get_write_flights_from_api()
    kafka_task = send_data_to_kafka(generated_file_path)

    check_api_ready >> generated_file_path >> kafka_task


flights()

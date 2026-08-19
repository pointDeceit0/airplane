import json
from pathlib import Path
from time import sleep
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from opensky_api import OpenSkyApi

from utils.token_manager import TokenManager
from utils.kafka_send import AirKafkaProducer


AIRFLOW_RAW_DATA_PATH = r'/opt/airflow/raw_data/air/air_flight_paths'
TARGET_TOPIC = 'air_aircraft_paths'
LIMIT_API_POOLS = 2


default_args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


@dag(dag_id='AIR_flight_paths_1h_dag',
     default_args=default_args,
     start_date=datetime(year=2026, month=7, day=31, hour=0),
     # every 15 minutes
     schedule=CronDataIntervalTimetable('30 */1 * * *', timezone='UTC'),
     catchup=False,
     max_active_runs=1)
def flight_paths_1h_dag():
    """
    """

    @task()
    def get_write_paths_from_api(**context) -> dict:
        # +================== Getting icao24 codes ==================+ #
        conn = BaseHook.get_connection('postgres_conn')
        engine = create_engine(
            f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/data'
        )
        icao_codes = pd.read_sql(
            """SELECT icao24
                FROM tech_data.air_path_getting_queue
                WHERE NOT is_processed
                ORDER BY last_seen;""",
            engine
        )
        print(f"Expect processing:\t{len(icao_codes)}")

        # +================== Getting data from api ==================+ #
        result, to_update = [], []
        print("Getting data from api...")
        token_manager = TokenManager()
        with OpenSkyApi(token_manager=token_manager) as api:
            for code in icao_codes['icao24']:
                paths = api.get_track_by_aircraft(code)

                to_update.append(code)

                if paths is None:
                    continue

                result.append(
                    {"icao24": paths.icao24,
                     "start_time": paths.startTime,
                     "end_time": paths.endTime,
                     "callsign": paths.callsign,
                     "data": [[v.time, v.latitude, v.longitude, v.baro_altitude, v.true_track, v.on_ground]
                              for v in paths.path]}
                )

                if len(result) == LIMIT_API_POOLS:
                    break

                # API restriction
                sleep(5.1)

        print(f'Record recieved:\t{len(result)}.')
        print(f'Number of missed codes:\t{len(to_update) - len(result)}.')

        # +================== Writing into file ==================+ #
        min_time = datetime.fromtimestamp(min(times := [v['end_time'] for v in result]))
        max_time = datetime.fromtimestamp(max(times))
        new_folder = Path(f"{AIRFLOW_RAW_DATA_PATH}/{min_time.strftime("%Y_%m_%d")}")
        if not new_folder.exists():
            new_folder.mkdir(parents=False, exist_ok=False)
            print(f"Created path:\t{new_folder}")
        else:
            print(f"Path {new_folder} already exists")

        # Open new file
        new_json = new_folder / f"{min_time.strftime("%H-%M")}_{max_time.strftime("%H-%M")}.json"
        with open(new_json, 'w', encoding="utf-8") as f:
            json.dump(result, f, indent=2)

        return {"path": str(new_json), "to_update": "'" + "', '".join(to_update) + "'"}

    update_statuses = SQLExecuteQueryOperator(
        task_id="update_queue_statuses",
        conn_id='postgres_conn',
        sql="""UPDATE tech_data.air_path_getting_queue
               SET is_processed = TRUE,
                   updated_at   = now()
               WHERE icao24 IN ({{ ti.xcom_pull(task_ids="get_write_paths_from_api")['to_update'] }});"""
    )

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

    api_task = get_write_paths_from_api()
    kafka_task = send_data_to_kafka(api_task['path'])

    api_task >> update_statuses
    api_task >> kafka_task


flight_paths_1h_dag()

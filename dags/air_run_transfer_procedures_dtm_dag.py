from datetime import datetime
from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    "owner": "admin",
    "retries": 0
}


@dag(dag_id='AIR_run_transfer_procedures_1h_dag',
     default_args=default_args,
     start_date=datetime(year=2026, month=1, day=1),
     schedule=CronDataIntervalTimetable('15 * * * *', timezone='UTC'),
     catchup=False,
     max_active_runs=1)
def run_transfer_procedures():
    """
    """

    get_procedures = SQLExecuteQueryOperator(
        task_id='get_procedures',
        conn_id='postgres_conn',
        sql="""SELECT id, procedure_name
               FROM tech_data.tech_raw_to_dds_procedures
               WHERE is_enabled;""",
        return_last=True,
    )

    @task
    def generate_sql_queries(procedure_list: list[tuple], data_interval_start=None, data_interval_end=None) -> list:
        """Generates sql queries for launching with dtm

        Args:
            procedure_list (list): procedure list

        Returns:
            list: queries list
        """
        if len(procedure_list) == 0:
            raise AirflowSkipException("No procedures to execute.")
        print(f"Plan number procedures to execute:\t{len(procedure_list)}")
        print(f"Period: {data_interval_start}--{data_interval_end}")
        return [f"CALL {v[1]}('{data_interval_start.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp without time zone, "
                f"'{data_interval_end.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp without time zone, {v[0]}::bigint);"
                for v in procedure_list]

    generate_queries = generate_sql_queries(get_procedures.output)

    call_procedures = SQLExecuteQueryOperator.partial(
        task_id='call_procedure',
        conn_id='postgres_conn',
        autocommit=True
    ).expand(sql=generate_queries)

    get_procedures >> generate_queries >> call_procedures


run_transfer_procedures()

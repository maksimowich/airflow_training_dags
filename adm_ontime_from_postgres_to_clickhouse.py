"""
Deletion, creation and insertion monthly data into adm_ontime
"""


from datetime import datetime
from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator


postgres_connection_param = {
    'host_port': 'db.mpkazantsev.ru:5432',
    'database': 'postgres',
    'schema': 'public',
    'table': 'ontime',
    'user': 'postgres',
    'password': '5555',
}

begin_and_end_dates = {
    'begin_date': '2018-05-01',
    'end_date': '2018-05-31',
}


dag = DAG('adm_ontime_from_postgres_to_clickhouse',
          description='Deletion, creation and insertion monthly data into adm.ontime',
          schedule_interval=None,
          start_date=datetime(2023, 5, 11),
          catchup=False)

drop_table = ClickHouseOperator(
    task_id="drop_table",
    clickhouse_conn_id="maksimovich_clickhouse",
    sql="sql_scripts/adm_ontime_from_postgres_to_clickhouse/drop_query.sql",
    dag=dag
)

create_table = ClickHouseOperator(
    task_id="create_table",
    clickhouse_conn_id="maksimovich_clickhouse",
    sql="sql_scripts/adm_ontime_from_postgres_to_clickhouse/create_query.sql",
    parameters=postgres_connection_param,
    dag=dag
)

insert_rows = ClickHouseOperator(
    task_id="insert_rows",
    clickhouse_conn_id="maksimovich_clickhouse",
    sql="sql_scripts/adm_ontime_from_postgres_to_clickhouse/insert_query.sql",
    parameters={**begin_and_end_dates, **postgres_connection_param},
    dag=dag
)

drop_table >> create_table >> insert_rows

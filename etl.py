from sqlite3 import paramstyle

import json

from datetime import datetime, timedelta
from airflow import DAG

from airflow.models.connection import Connection
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

with DAG(
    'etl',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='A simple etl DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False, 
    tags=['etl','bhhc','architecture','poc'],
) as dag:

    conn = Connection(
        conn_id="ARCH_AZURE_MSSQL",
        conn_type="mysql",
        description="ARCH_AZURE_MSSQL",
        host="tcp:archpocsqlserver.database.windows.net",
        login="archadmin",
        password="tintin85!",
        extra=json.dumps(dict(
            port="1433", 
            initial_catalog="archpocdb", 
            persist_security_info="False",
            MultipleActiveResultSets="False",
            Encrypt="True",
            TrustServerCertificate="False",
            Connection_Timeout="30")),
    )

    t1_get_all_customers = MsSqlOperator(
        task_id = "get_all_customers",
        mssql_conn_id = "ARCH_AZURE_MSSQL",
        sql = "Select * from Customer",
        _hook = MsSqlHook(mssql_conn_id='ARCH_AZURE_MSSQL', schema='archpocdb'),
    )

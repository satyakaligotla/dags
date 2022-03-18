from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.azure.hooks.azure_cosmos import AzureCosmosInsertDocumentOperator

with DAG(
    dag_id='cosmos',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['cosmos'],
) as dag:

    t1 = AzureCosmosInsertDocumentOperator(
        task_id='insert_cosmos_document',
        database_name="ArchAirflowPoc",
        collection_name='SalesLTCustomers',
        document={"CustomerID": "1", "param1": "value1", "param2": "value2"},
        azure_cosmos_conn_id="ARCH_AZURE_COSMOS",
    )

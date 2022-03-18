from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.azure.operators.cosmos import AzureCosmosInsertDocumentOperator

with DAG(
    dag_id='cosmos',
    default_args={'database_name': 'ArchAirflowPoc'},
    start_date=datetime(2021, 1, 1),
    catchup=False,
    doc_md=__doc__,
    tags=['cosmos'],
) as dag:

    t1 = AzureCosmosInsertDocumentOperator(
        task_id='insert_cosmos_document',
        collection_name='SalesLTCustomers',
        document={"CustomerID": "1", "param1": "value1", "param2": "value2"},
    )

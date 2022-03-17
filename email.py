
from datetime import datetime, timedelta
from typing import List
from airflow import DAG
from airflow.operators.email import EmailOperator

with DAG(
    'send_email',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple email DAG',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 3, 17),
    catchup=False, 
    tags=['email','bhhc','architecture','poc'],
) as dag:
    
    t1_send_email = EmailOperator(
        task_id='send_email',
        to = list('skaligotla@bhhc.com'),
        subject = 'This is a test email sent from a DAG.',
        html_content = '<h1>You just got dagged</h1>',
        files = [],
        cc = list('skaligotla@bhhc.com'),
        bcc = list('skaligotla@bhhc.com'),
        mime_subtype = 'mixed',
        mime_charset = 'utf-8'
    )

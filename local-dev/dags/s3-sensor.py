from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dummy import DummyOperator


with DAG(
    "s3_sensor_v1.0",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule_interval=None,
    description="DAG that waits for a file in S3",

) as dag:
    
    task_1 = S3KeySensor(
        task_id = "wait_for_file",
        aws_conn_id="aws_s3",
        bucket_key="s3://astro-bucket-1/*.txt",
        wildcard_match=True,
        timeout= 1 * 3600 ,
        poke_interval = 60
    )

    task_2 = DummyOperator(
        task_id = "Processing"
    )
    
    task_3 = DummyOperator(
        task_id = "END"
    )

    task_1 >> task_2 >> task_3

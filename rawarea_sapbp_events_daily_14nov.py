from __future__ import annotations
from asyncio.subprocess import DEVNULL
from datetime import datetime
import datetime
import time
from google.cloud import bigquery
from google.cloud import storage
from airflow import DAG
from airflow import models
from airflow import XComArg
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubPullOperator,
)
from airflow.operators import python
from airflow.decorators import dag, task
from airflow import models, AirflowException, DAG
from google.cloud.exceptions import NotFound
from string import Template
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import pubsub_v1
from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor
from dags.rdp_cdwh_integration.dag_config_functions import final_load,default_args,publish_dataset_completion
from airflow.models import Variable
from dags.jobnet.utils import constants
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
 
 
 
#pass all the hardcoded variables
COMPOSER_BUCKET_NAME=Variable.get("main_composer_bucket")
PROJECT_NAME=Variable.get("project_id")
RDP_PROJECT=Variable.get("rdp_project_rawarea")
DATASET_NAME='rawarea_sapbp'
RDP_DATASET= RDP_PROJECT+'.'+DATASET_NAME
PREFIX='dags/dags/rdp_cdwh_integration/rawarea_sapbp/'
DAG_NAME= 'rawarea_sapbp_events_daily_manual'
 
#variables for pubsub
topic_id = "rawarea_change_events_topic"
cdwh_topic_id=topic=constants.jobnet_topic_name
 
subscription_id = "rawarea_change_events_sapbp_sub"
filter_subs = 'attributes.eventType="DataProductChanged" AND (attributes.changedDataProduct="rawarea_sapbp")'
 
 
#initalise all the service clients required
bq_client = bigquery.Client()
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
 
 
#Initialise paths for topic and subscription
topic_path = publisher.topic_path(RDP_PROJECT, topic_id)
cdwh_topic_path = publisher.topic_path(PROJECT_NAME, cdwh_topic_id)
subscription_path = subscriber.subscription_path(PROJECT_NAME, subscription_id)
 
#list the blobs for reading sql from composer bucket
composer_bucket = storage_client.get_bucket(COMPOSER_BUCKET_NAME)
blobs=composer_bucket.list_blobs(prefix=PREFIX)
 
var_table_list = Variable.get("table_list",deserialize_json=True)
table_list=var_table_list['table_list']
 
 
@dag(
    DAG_NAME, 
    schedule_interval=None, 
    default_args=default_args)  
 
def taskflow():
 
    @task
    def fetch_tables_from_pubsub(table_list):
        return table_list
 
    @task
    def select_insert_query_job(blobs,PROJECT_NAME,RDP_DATASET,rdp_table_name,bq_client,publisher,topic_path):
 
        final_load(blobs,PROJECT_NAME,RDP_DATASET,rdp_table_name,bq_client,publisher,topic_path)
 
    @task
    def publish_pub_sub_msg(values,publisher,cdwh_topic_path,DATASET_NAME):
    
        print(f"Returned value was {values}")
        publish_dataset_completion(publisher,cdwh_topic_path,DATASET_NAME)
 
 
        
    rdp_table_list=fetch_tables_from_pubsub(table_list)
 
    st_list=select_insert_query_job.partial(blobs=blobs,PROJECT_NAME=PROJECT_NAME,RDP_DATASET=RDP_DATASET,bq_client=bq_client,publisher=publisher,topic_path=cdwh_topic_path).expand(rdp_table_name=rdp_table_list)
 
    publish_pub_sub_msg(st_list,publisher,cdwh_topic_path,DATASET_NAME)
    
 
 
dag = taskflow()
 


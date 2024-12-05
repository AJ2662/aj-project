from google.cloud.exceptions import NotFound
from string import Template
from airflow import models, AirflowException, DAG
from google.cloud import bigquery
import datetime
from airflow.models import TaskInstance
import json
from dags.bigquery_sync.utils.logging_config import get_bigquery_sync_logger
 
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
 
default_args = {
    'owner': 'RDP SA Loading',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'start_date': YESTERDAY,
}
 
#function to create subscription
def subscription_creation(subscriber,subscription_path,topic_path,filter_subs,subscription_id):
    with subscriber:
        subscription = subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path, "filter": filter_subs}
        )
    print(f"Subscription {subscription_id} created with filter: {filter_subs}")
   

 
# Function to create a subscription if it doesn't exist
def create_subscription_if_not_exists(topic_path,subscription_path,subscriber,subscription_id,filter_subs):
    
    try:
        # Check if the subscription already exists
        print(f"Subscription Path : {subscription_path}")
        subscriber.get_subscription(subscription=subscription_path)
        print(f"Subscription {subscription_id} already exists.")
    except Exception as e:
        print(f"Subscription {subscription_id} does not exist. Creating it.")
        subscription_creation(subscriber,subscription_path,topic_path,filter_subs,subscription_id)
 
#Function to acknowledge msgs received from pub sub via raw area topic
def acknowledge_message(messages, context):
 
    ti = context['ti']
    processed_messages = [message.message.data.decode('utf-8') for message in messages]
    print(processed_messages)
 
    ti.xcom_push(key="messages", value=processed_messages)
 
#Function to extract list of table names from the msg received
def fetch_tables_from_msg(ti: TaskInstance):
 
    messages = ti.xcom_pull(key="messages", task_ids="pull_pubsub_messages")
    table_name = []
 
    for message in messages:
        data_json = json.loads(message)
        print(f"JSON Data: {data_json}")
        try:
            table_name.append(data_json["data"]["changedObjects"][0]["objectName"])
        except Exception as e:
            print(f'Error is: {e}')
 
    return table_name
 
 
#check if staging table exists or not in the mm staging area
def check_table_exists(tbl_name,bq_client):
	 
    try:
        bq_client.get_table(tbl_name)
        #print("Table already exists.")
        return True
    except NotFound:
        print("Not Found table, Creating...")
        return False
 
#reading the sqls stored in the composer bucket for the tables received from pub sub
def read_sql(blobs,PROJECT_NAME,rdp_table_name,table_name,RDP_DATASET,bq_client):
 
    for blob in blobs:
        if(table_name+'.sql') in blob.name:
 
    
                rdp_table_id='`'+ RDP_DATASET + '.'+rdp_table_name +'`'
                print(rdp_table_id)
 
                table_id=PROJECT_NAME+'.mm_staging_area.'+table_name
                DELTA_TABLE_ID='`'+ PROJECT_NAME +'.mm_config_schema.t_st_rtb_delta`'
 
 
                sql_query = (f"select max(MD_LOAD_DTS) from {rdp_table_id};")
                print(sql_query)
 
                results = bq_client.query(sql_query) 
 
                print(results)
 
                for row in results:
                    latest_timestamp=row[0]
            
                print(latest_timestamp)
                                
                                    
                with blob.open("r") as f:
                        sql_qry = f.read()
 
                        query = Template(sql_qry).substitute(        #pass the variables here to the sql query
                                rdp_table_id =rdp_table_id,
                                delta_table_id = DELTA_TABLE_ID,
                                tble_name= table_name,
                                table_id= table_id,
                                max_timestamp= (latest_timestamp)
                            )
                
    return query,table_id,latest_timestamp
 
#find out staging table name from the map table by input of rdp table name
def map_tbl(PROJECT_NAME,bq_client,rdp_table_name):
 
    print(rdp_table_name)
 
    try:
        
        qry=f'SELECT TARGET_TABLE_NAME,WRITE_DISPOSITION FROM `{PROJECT_NAME}.mm_config_schema.t_st_source_sa_cdwh_map` where SOURCE_TABLE_NAME="{rdp_table_name}"'
        print(qry)
 
        results = bq_client.query(qry)
 
        for row in results:
            st_tbl_name=row[0]
            write_disposition=row[1]
            
            print(st_tbl_name)
            print(write_disposition)
 
        return st_tbl_name,write_disposition
 
    except Exception as err:
            print(f'ERROR:{err}')
            raise AirflowException(f"ERROR: NO matching table found in CDWH.")
 
def update_rtb_delta_table(PROJECT_NAME,latest_timestamp,end_time,table_name,bq_client):
 
       
    UPDATE_T_ST_RTB_DELTA_QUERY = (f"Update `{PROJECT_NAME}.mm_config_schema.t_st_rtb_delta` set LAST_RUN_TIMESTAMP = cast('{latest_timestamp}' as timestamp) , LAST_RUN_DATE = cast('{end_time}' as timestamp) where job_name = '{table_name}'")
    print(UPDATE_T_ST_RTB_DELTA_QUERY)
 
    update_job = bq_client.query(UPDATE_T_ST_RTB_DELTA_QUERY)   #update delta table with the latest timestamp
    update_job.result()
            
 
def insert_log_table(PROJECT_NAME,latest_timestamp,total_rows,start_time,end_time,table_name,bq_client):
 
    INSERT_T_ST_RTB_LOGGING_QUERY = (f"INSERT INTO `{PROJECT_NAME}.mm_config_schema.t_st_rtb_logging` (TABLE_NAME,START_TIMESTAMP,MD_LOAD_DTS,RECORDS_COUNT,LADE_PROZESS_TSTAMP) values ( '{table_name}' ,cast('{start_time}' as timestamp), cast('{latest_timestamp}' as timestamp) , {total_rows},cast('{end_time}' as timestamp));")
    print(INSERT_T_ST_RTB_LOGGING_QUERY)
 
    insert_job = bq_client.query(INSERT_T_ST_RTB_LOGGING_QUERY)  #update the logs table with all the details
    insert_job.result()    
 
    """
 
    insert_job = bq_client.query(INSERT_T_ST_RTB_LOGGING_QUERY)  #update the logs table with all the details
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger_completed = get_bigquery_sync_logger(
        extra_labels={"sync_table_name": table_name, "sync_receiveTimestamp": f"{current_time}", "status": "completed", "rows_inserted": f"{total_rows}"})
 
    logger_completed.info(
        f"data sync completed on {current_time}, {total_rows} rows is inserted as part of data sync to table {table_name}")
 
    insert_job.result()    
    """
 
def publish_table_completion(publisher,topic_path,table_name):
 
 
    message={"attributes": {  "table_id": table_name}}
    print(message)
    data_str = json.dumps(message)
    data = data_str.encode("utf-8")
 
    future = publisher.publish(
        topic_path, data
    )
 
    print(future.result())
 
def publish_dataset_completion(publisher,topic_path,dataset_name):
 
 
    message={"attributes": {  "dataset_id": dataset_name}}
    print(message)
    data_str = json.dumps(message)
    data = data_str.encode("utf-8")
 
    future = publisher.publish(
        topic_path, data
    )
 
    print(future.result())
 
#final load function to bigquery staging table
def final_load(blobs,PROJECT_NAME,RDP_DATASET,rdp_table_name,bq_client,publisher,topic_path):
 
    print("Mapping rdp table to staging table name:")
 
    table_name,write_disposition= map_tbl(PROJECT_NAME,bq_client,rdp_table_name)
 
    print(table_name)
 
    #read the sqls for the tables read along with full name
    print("Loading sql query to insert into sa table:")
    sql_qry,table_id,latest_timestamp=read_sql(blobs,PROJECT_NAME,rdp_table_name,table_name,RDP_DATASET,bq_client)
 
    try:
    
        #can be removed later printing for testing purpose
        write_disposition='WRITE_'+ write_disposition
        print(write_disposition)
        print(sql_qry)
        print(table_id)
 
        if check_table_exists(table_id,bq_client):                        #checks if staging table exists or else creates it
                    print("Table exists")
                    
        else:
                    table = bigquery.Table(table_id)
                    table = bq_client.create_table(table)
 
        
        start_time=datetime.datetime.now()                       #checking start time of the load
        print(start_time)
 
        table = bq_client.get_table(table_id)
        num_rows_begin = table.num_rows
        print(num_rows_begin)
 
        if write_disposition.upper() == 'WRITE_UPSERT':
                print("Running Load from bigquery to bigquery")                
 
                upsert_job = bq_client.query(sql_qry)                 
                upsert_job.result()
                output=upsert_job.result()
        else:
                job_config = bigquery.QueryJobConfig(
                                destination=table_id,
                                write_disposition=write_disposition)
 
                print("Running Load from bigquery to bigquery")
                query_job = bq_client.query(sql_qry, job_config=job_config)
                output=query_job.result()
 
        total_rows=output.total_rows
        print(total_rows)
 
        new_rows_added=total_rows-num_rows_begin
 
        end_time=datetime.datetime.now()                      #calculate end time of the load
        print(end_time) 
 
        print("Updating Delta table:")
        update_rtb_delta_table(PROJECT_NAME,latest_timestamp,end_time,table_name,bq_client)
 
        print("Inserting into Log table:")
        insert_log_table(PROJECT_NAME,latest_timestamp,new_rows_added,start_time,end_time,table_name,bq_client)
 
 
        if(new_rows_added!=0):
            print("Publish messages for the table:")
            publish_table_completion(publisher,topic_path,"mm_staging_area."+table_name)
        else:
            f"No data received for {table_name}, hence no msgs published!"
 
        return 0
        
 
    except Exception as err:
            print(f'ERROR:{err}')
            raise AirflowException(f"ERROR: LOADING DATA FAILED")
 
 
 
 





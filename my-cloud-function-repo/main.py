from google.cloud import pubsub_v1, bigquery
from datetime import datetime
import json
import os

PROJECT_ID = os.environ.get("alpine-effort-459902-c3")
TOPIC_ID = "rawarea-change-events-topic"
DATASET_ID = "rawarea_sapbp"
TABLE_ID = "t_sa_gp_rollen"

def publish_if_table_refreshed(request):
    bq_client = bigquery.Client()
    pub_client = pubsub_v1.PublisherClient()
    topic_path = pub_client.topic_path(PROJECT_ID, TOPIC_ID)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    table = bq_client.get_table(table_ref)
    changetimestamp = table.modified.isoformat()

    if table.modified.date() == datetime.utcnow().date():
        message = {
            "table_name": TABLE_ID,
            "changetimestamp": changetimestamp
        }
        future = pub_client.publish(topic_path, json.dumps(message).encode("utf-8"))
        return f"Published: {future.result()}"
    else:
        return "Table not refreshed today. Skipping publish."

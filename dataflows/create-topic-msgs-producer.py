from google.cloud import pubsub_v1, bigquery
import json

topic_id = 'vehicle_data'
project_id = 'cloud-final-418807'
dataset_id = 'highway_analysis_db'
table_id = 'vehicle_data_averages'

publisher = pubsub_v1.PublisherClient()
bigquery_client = bigquery.Client(project=project_id)
topic = publisher.topic_path(project_id, topic_id)

# Load challenging scenarios from BigQuery
table_ref = bigquery_client.dataset(dataset_id).table(table_id)
table = bigquery_client.get_table(table_ref)

# Define schema
schema = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("average_velocity", "FLOAT"),
    bigquery.SchemaField("average_acceleration", "FLOAT"),
    bigquery.SchemaField("min_ttc", "FLOAT"),
    bigquery.SchemaField("max_ttc", "FLOAT"),
]

for row in bigquery_client.list_rows(table, selected_fields=schema):
    vehicle = {
        "id": row.id,
        "average_velocity": row.average_velocity,
        "average_acceleration": row.average_acceleration,
        "min_ttc": row.min_ttc,
        "max_ttc": row.max_ttc,
    }
    message_data = json.dumps(vehicle).encode("utf-8")
    future = publisher.publish(topic, data=message_data)
    print(f"publishing vehicle {vehicle['id']} to {topic}: {future.result()}")

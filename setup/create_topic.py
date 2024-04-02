from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

# Replace 'your-project-id' with your actual project ID
project_id = 'cloud-final-418807'

topic_name = 'vehicle_data'
subscription_name = 'vehicle_data_sub'

topic_name2 = 'risk_analysis'
subscription_name2 = 'risk_analysis_sub'

def create_topic_and_subscription(project_id, topic_name, subscription_name):
    # Initialize the Publisher and Subscriber clients
    publisher_client = pubsub_v1.PublisherClient()
    subscriber_client = pubsub_v1.SubscriberClient()

    # Create topic
    topic_path = publisher_client.topic_path(project_id, topic_name)
    try:
        topic = publisher_client.create_topic(request={"name": topic_path})
        print(f"Topic created: {topic.name}")
    except AlreadyExists:
        print(f"Topic '{topic_name}' already exists.")

    # Create subscription
    subscription_path = subscriber_client.subscription_path(project_id, subscription_name)
    try:
        subscription = pubsub_v1.types.Subscription(name=subscription_path, topic=topic_path)
        subscription = subscriber_client.create_subscription(request={"name": subscription_path, "topic": topic_path})
        print(f"Subscription created: {subscription.name}")
    except AlreadyExists:
        print(f"Subscription '{subscription_name}' already exists.")

create_topic_and_subscription(project_id, topic_name, subscription_name)

# Create a second topic and subscription
create_topic_and_subscription(project_id, topic_name2, subscription_name2)

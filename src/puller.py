import json
import os

from google.cloud import pubsub_v1
from google.oauth2 import service_account


def synchronous_pull(project_id, subscription_name, credentials_json):
    """Pulling messages synchronously."""
    info = json.loads(credentials_json)
    credentials = service_account.Credentials.from_service_account_info(info)

    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    NUM_MESSAGES = 1
    TIMEOUT_SEC = 30

    # The subscriber pulls a specific number of messages. The actual
    # number of messages pulled may be smaller than max_messages.
    print(f"Waiting for a message for {TIMEOUT_SEC} seconds")
    response = subscriber.pull(
        subscription=subscription_path,
        max_messages=NUM_MESSAGES,
        timeout=TIMEOUT_SEC,
    )

    ack_ids = []
    for received_message in response.received_messages:
        print(f"Received: {received_message.message.data}.")
        ack_ids.append(received_message.ack_id)

    if len(ack_ids) == 0:
        print("No messages received. Aborting.")

    # Acknowledges the received messages so they will not be sent again.
    subscriber.acknowledge(subscription=subscription_path, ack_ids=ack_ids)

    print(
        f"Received and acknowledged {len(response.received_messages)} messages from {subscription_path}."
    )


if __name__ == "__main__":
    project_id = os.getenv('PROJECT_ID')
    subscription_name = os.getenv("SUBSCRIPTION_NAME")
    credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")

    synchronous_pull(project_id=project_id, subscription_name=subscription_name, credentials_json=credentials_json)

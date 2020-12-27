# Code based on - https://github.com/googleapis/python-pubsub/blob/master/samples/snippets/subscriber.py
import os

from google.cloud import pubsub_v1


def synchronous_pull(project_id, subscription_name, credentials_json):
    """Pulling messages synchronously."""

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    NUM_MESSAGES = 1
    TIMEOUT_SEC = 30

    # The subscriber pulls a specific number of messages. The actual
    # number of messages pulled may be smaller than max_messages.
    response = subscriber.pull(
        subscription=subscription_path,
        max_messages=NUM_MESSAGES,
        timeout=TIMEOUT_SEC,
    )

    ack_ids = []
    for received_message in response.received_messages:
        print(f"Received: {received_message.message.data}.")
        ack_ids.append(received_message.ack_id)

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

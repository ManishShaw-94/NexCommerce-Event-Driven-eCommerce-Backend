import json
import os

from confluent_kafka import Consumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_ORDER_TOPIC = os.getenv(
    "KAFKA_ORDER_TOPIC",
    "order-events"
)


consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

    "group.id": "notification-service",

    "auto.offset.reset": "earliest"
})


consumer.subscribe([
    KAFKA_ORDER_TOPIC
])


print("Notification consumer started...")
print(f"Listening to topic: {KAFKA_ORDER_TOPIC}")


try:

    while True:

        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(
                f"Kafka consumer error: "
                f"{message.error()}"
            )
            continue

        event = json.loads(
            message.value().decode("utf-8")
        )

        event_type = event.get(
            "event_type"
        )

        if event_type == "ORDER_CREATED":

            print()
            print("=" * 50)

            print("NEW ORDER RECEIVED")

            print(
                f"Order ID: "
                f"{event['order_id']}"
            )

            print(
                f"Customer: "
                f"{event['customer_name']}"
            )

            print(
                f"Total: "
                f"${event['total_amount']}"
            )

            print(
                "Sending order confirmation..."
            )

            print(
                f"Order #{event['order_id']} "
                f"confirmation sent."
            )

            print("=" * 50)
            print()


finally:

    consumer.close()
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_ORDER_TOPIC = os.getenv(
    "KAFKA_ORDER_TOPIC",
    "order-events"
)


producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "sbns-api"
})

def delivery_report(error, message):

    if error is not None:

        print(
            f"Kafka delivery failed: {error}"
        )

    else:

        print(
            f"Kafka message delivered "
            f"to {message.topic()} "
            f"partition {message.partition()} "
            f"offset {message.offset()}"
        )

def publish_order_created(
    order,
    customer,
    items
):

    event = {
        "event_id": str(uuid4()),
        "event_type": "ORDER_CREATED",
        "event_version": 1,

        "occurred_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "order_id": order["order_id"],

        "customer_id": order["customer_id"],

        "customer_name": customer["name"],

        "total_amount": str(
            order["total_amount"]
        ),

        "items": [
            {
                "product_id":
                    item["product_id"],

                "product_name":
                    item["product_name"],

                "quantity":
                    item["quantity"],

                "unit_price":
                    str(item["unit_price"]),

                "line_total":
                    str(item["line_total"])
            }
            for item in items
        ]
    }

    delivery_state = {
        "error": None
    }

    def callback(error, message):

        if error is not None:

            delivery_state["error"] = str(error)

        delivery_report(
            error,
            message
        )

    producer.produce(
        topic=KAFKA_ORDER_TOPIC,

        key=str(
            order["order_id"]
        ),

        value=json.dumps(event),

        callback=callback
    )

    remaining_messages = producer.flush(
        timeout=10
    )

    if remaining_messages > 0:

        raise RuntimeError(
            "Kafka message delivery timed out"
        )

    if delivery_state["error"] is not None:

        raise RuntimeError(
            "Kafka message delivery failed: "
            + delivery_state["error"]
        )

    return event
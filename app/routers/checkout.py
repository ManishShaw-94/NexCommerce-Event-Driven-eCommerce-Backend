from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from psycopg import Connection
from redis import Redis

from app.db import get_db
from app.redis_client import get_redis
from app.schemas import CheckoutRequest

from app.kafka_producer import publish_order_created

router = APIRouter(
    prefix="/checkout",
    tags=["Checkout"]
)

# @router.post("", status_code=201)
# def checkout(
#     checkout_request: CheckoutRequest,
#     db: Connection = Depends(get_db)
# ):
    
#     # Validate duplicate products
#     product_ids = [
#         item.product_id
#         for item in checkout_request.items
#     ]

#     if len(product_ids) != len(set(product_ids)):
#         raise HTTPException(
#             status_code=400,
#             detail="Duplicate products are not allowed in checkout"
#         )

#     with db.cursor() as cursor:

#         # -----------------------------------
#         # 1. Validate customer
#         # -----------------------------------

#         cursor.execute("""
#             SELECT
#                 customer_id,
#                 name
#             FROM customers
#             WHERE customer_id = %s;
#         """, (
#             checkout_request.customer_id,
#         ))

#         customer = cursor.fetchone()

#         if customer is None:
#             raise HTTPException(
#                 status_code=404,
#                 detail="Customer not found"
#             )

#         # -----------------------------------
#         # 2. Validate and lock products
#         # -----------------------------------

#         products = {}

#         sorted_items = sorted(
#             checkout_request.items,
#             key=lambda item: item.product_id
#         )

#         for item in sorted_items:

#             cursor.execute("""
#                 SELECT
#                     product_id,
#                     name,
#                     price,
#                     stock_quantity
#                 FROM products
#                 WHERE product_id = %s
#                 FOR UPDATE;
#             """, (
#                 item.product_id,
#             ))

#             product = cursor.fetchone()

#             if product is None:
#                 raise HTTPException(
#                     status_code=404,
#                     detail=f"Product {item.product_id} not found"
#                 )

#             if product["stock_quantity"] < item.quantity:

#                 raise HTTPException(
#                     status_code=409,
#                     detail=(
#                         f"Insufficient stock for "
#                         f"{product['name']}. "
#                         f"Requested: {item.quantity}, "
#                         f"Available: {product['stock_quantity']}"
#                     )
#                 )

#             products[item.product_id] = product

#         # -----------------------------------
#         # 3. Calculate total
#         # -----------------------------------

#         total_amount = Decimal("0.00")
#         response_items = []

#         for item in checkout_request.items:

#             product = products[item.product_id]

#             line_total = (
#                 product["price"] * item.quantity
#             )

#             total_amount += line_total

#             response_items.append({
#                 "product_id": product["product_id"],
#                 "product_name": product["name"],
#                 "quantity": item.quantity,
#                 "unit_price": product["price"],
#                 "line_total": line_total
#             })

#         # -----------------------------------
#         # 4. Create order
#         # -----------------------------------

#         cursor.execute("""
#             INSERT INTO orders (
#                 customer_id,
#                 status,
#                 total_amount
#             )
#             VALUES (%s, %s, %s)
#             RETURNING
#                 order_id,
#                 customer_id,
#                 status,
#                 total_amount,
#                 created_at;
#         """, (
#             checkout_request.customer_id,
#             "CREATED",
#             total_amount
#         ))

#         order = cursor.fetchone()

#         # -----------------------------------
#         # 5. Create items + reduce stock
#         # -----------------------------------

#         for item in checkout_request.items:

#             product = products[item.product_id]

#             cursor.execute("""
#                 INSERT INTO order_items (
#                     order_id,
#                     product_id,
#                     quantity,
#                     unit_price
#                 )
#                 VALUES (%s, %s, %s, %s);
#             """, (
#                 order["order_id"],
#                 product["product_id"],
#                 item.quantity,
#                 product["price"]
#             ))

#             # Reduce inventory
#             cursor.execute("""
#                 UPDATE products
#                 SET stock_quantity = stock_quantity - %s
#                 WHERE product_id = %s;
#             """, (
#                 item.quantity,
#                 item.product_id
#             ))

#     # -----------------------------------
#     # Transaction commits when the
#     # database dependency exits normally
#     # -----------------------------------

#     return {
#         "order_id": order["order_id"],
#         "customer_id": order["customer_id"],
#         "customer_name": customer["name"],
#         "status": order["status"],
#         "total_amount": order["total_amount"],
#         "created_at": order["created_at"],
#         "items": response_items
#     }

@router.post("/{customer_id}", status_code=201)
def checkout(
    customer_id: int,
    db: Connection = Depends(get_db),
    redis_db: Redis = Depends(get_redis)
):

    # ----------------------------------
    # 1. Read cart from Redis
    # ----------------------------------

    key = f"cart:{customer_id}"

    cart_data = redis_db.hgetall(key)

    if not cart_data:
        raise HTTPException(
            status_code=400,
            detail="Cart is empty"
        )

    cart_items = [
        (int(product_id), int(quantity))
        for product_id, quantity in cart_data.items()
    ]

    cart_items.sort(key=lambda item: item[0])

    with db.cursor() as cursor:

        # ----------------------------------
        # 2. Validate customer
        # ----------------------------------

        cursor.execute("""
            SELECT
                customer_id,
                name
            FROM customers
            WHERE customer_id = %s;
        """, (
            customer_id,
        ))

        customer = cursor.fetchone()

        if customer is None:
            raise HTTPException(
                status_code=404,
                detail="Customer not found"
            )

        # ----------------------------------
        # 3. Lock + validate products
        # ----------------------------------

        products = {}

        for product_id, quantity in cart_items:

            cursor.execute("""
                SELECT
                    product_id,
                    name,
                    price,
                    stock_quantity
                FROM products
                WHERE product_id = %s
                FOR UPDATE;
            """, (
                product_id,
            ))

            product = cursor.fetchone()

            if product is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"Product {product_id} "
                        f"no longer exists"
                    )
                )

            if product["stock_quantity"] < quantity:

                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Insufficient stock for "
                        f"{product['name']}. "
                        f"Requested: {quantity}, "
                        f"Available: "
                        f"{product['stock_quantity']}"
                    )
                )

            products[product_id] = product

        # ----------------------------------
        # 4. Calculate total
        # ----------------------------------

        total_amount = Decimal("0.00")
        response_items = []

        for product_id, quantity in cart_items:

            product = products[product_id]

            line_total = (
                product["price"] * quantity
            )

            total_amount += line_total

            response_items.append({
                "product_id":
                    product["product_id"],

                "product_name":
                    product["name"],

                "quantity":
                    quantity,

                "unit_price":
                    product["price"],

                "line_total":
                    line_total
            })

        # ----------------------------------
        # 5. Create order
        # ----------------------------------

        cursor.execute("""
            INSERT INTO orders (
                customer_id,
                status,
                total_amount
            )
            VALUES (%s, %s, %s)
            RETURNING
                order_id,
                customer_id,
                status,
                total_amount,
                created_at;
        """, (
            customer_id,
            "CREATED",
            total_amount
        ))

        order = cursor.fetchone()

        # ----------------------------------
        # 6. Create items + reduce stock
        # ----------------------------------

        for product_id, quantity in cart_items:

            product = products[product_id]

            cursor.execute("""
                INSERT INTO order_items (
                    order_id,
                    product_id,
                    quantity,
                    unit_price
                )
                VALUES (%s, %s, %s, %s);
            """, (
                order["order_id"],
                product_id,
                quantity,
                product["price"]
            ))

            cursor.execute("""
                UPDATE products
                SET stock_quantity =
                    stock_quantity - %s
                WHERE product_id = %s;
            """, (
                quantity,
                product_id
            ))

    # ----------------------------------
    # 7. Commit PostgreSQL FIRST
    # ----------------------------------

    db.commit()

    # ----------------------------------
    # 8. Create Kafka event for ORDER_CREATED
    # ----------------------------------
    kafka_event = None
    kafka_published = False


    try:

        kafka_event = publish_order_created(
            order=order,
            customer=customer,
            items=response_items
        )

        kafka_published = True

    except Exception as error:

        print(
            f"Failed to publish "
            f"ORDER_CREATED event: {error}"
        )

    # ----------------------------------
    # 9. Product stock changed, so cached products may now be stale.
    # ----------------------------------

    for product_id, quantity in cart_items:

        redis_db.delete(
            f"product:{product_id}"
        )

    # ----------------------------------
    # 10. Then clear Redis cart
    # ----------------------------------

    redis_db.delete(key)

    # ----------------------------------
    # 11. Return completed order
    # ----------------------------------

    return {
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "customer_name": customer["name"],
        "status": order["status"],
        "total_amount": order["total_amount"],
        "created_at": order["created_at"],
        "items": response_items,

        "event_published": kafka_published,

        "event_id": (
            kafka_event["event_id"]
            if kafka_event
            else None
        )
    }
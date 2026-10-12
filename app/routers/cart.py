import os

from fastapi import APIRouter, Depends, HTTPException
from psycopg import Connection
from redis import Redis

from app.db import get_db
from app.redis_client import get_redis
from app.schemas import CartItemAdd, CartItemUpdate


router = APIRouter(
    prefix="/cart",
    tags=["Cart"]
)


CART_TTL_SECONDS = int(
    os.getenv("CART_TTL_SECONDS", 604800)
)

def cart_key(customer_id: int) -> str:
    return f"cart:{customer_id}"

@router.post("/{customer_id}/items", status_code=201)
def add_cart_item(
    customer_id: int,
    item: CartItemAdd,
    db: Connection = Depends(get_db),
    redis_db: Redis = Depends(get_redis)
):

    # Validate customer
    with db.cursor() as cursor:

        cursor.execute("""
            SELECT customer_id
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

        # Validate product
        cursor.execute("""
            SELECT
                product_id,
                name,
                price,
                stock_quantity
            FROM products
            WHERE product_id = %s;
        """, (
            item.product_id,
        ))

        product = cursor.fetchone()

        if product is None:
            raise HTTPException(
                status_code=404,
                detail="Product not found"
            )

    key = cart_key(customer_id)

    new_quantity = redis_db.hincrby(
        key,
        str(item.product_id),
        item.quantity
    )

    redis_db.expire(
        key,
        CART_TTL_SECONDS
    )

    return {
        "customer_id": customer_id,
        "product_id": item.product_id,
        "product_name": product["name"],
        "quantity": new_quantity
    }

@router.get("/{customer_id}")
def get_cart(
    customer_id: int,
    db: Connection = Depends(get_db),
    redis_db: Redis = Depends(get_redis)
):

    key = cart_key(customer_id)

    cart_data = redis_db.hgetall(key)

    if not cart_data:

        return {
            "customer_id": customer_id,
            "items": [],
            "total_amount": 0
        }

    items = []
    total_amount = 0

    with db.cursor() as cursor:

        for product_id, quantity in cart_data.items():

            cursor.execute("""
                SELECT
                    product_id,
                    name,
                    price,
                    stock_quantity
                FROM products
                WHERE product_id = %s;
            """, (
                int(product_id),
            ))

            product = cursor.fetchone()

            if product is None:
                continue

            quantity = int(quantity)

            line_total = (
                product["price"] * quantity
            )

            total_amount += line_total

            items.append({
                "product_id": product["product_id"],
                "product_name": product["name"],
                "quantity": quantity,
                "unit_price": product["price"],
                "line_total": line_total,
                "available_stock": product["stock_quantity"]
            })

    return {
        "customer_id": customer_id,
        "items": items,
        "total_amount": total_amount
    }

@router.put("/{customer_id}/items/{product_id}")
def update_cart_item(
    customer_id: int,
    product_id: int,
    item: CartItemUpdate,
    db: Connection = Depends(get_db),
    redis_db: Redis = Depends(get_redis)
):

    with db.cursor() as cursor:

        cursor.execute("""
            SELECT
                product_id,
                name
            FROM products
            WHERE product_id = %s;
        """, (
            product_id,
        ))

        product = cursor.fetchone()

        if product is None:
            raise HTTPException(
                status_code=404,
                detail="Product not found"
            )

    key = cart_key(customer_id)

    existing_quantity = redis_db.hget(
        key,
        str(product_id)
    )

    if existing_quantity is None:
        raise HTTPException(
            status_code=404,
            detail="Product is not in the cart"
        )

    redis_db.hset(
        key,
        str(product_id),
        item.quantity
    )

    redis_db.expire(
        key,
        CART_TTL_SECONDS
    )

    return {
        "customer_id": customer_id,
        "product_id": product_id,
        "product_name": product["name"],
        "quantity": item.quantity
    }

@router.delete(
    "/{customer_id}/items/{product_id}",
    status_code=204
)
def remove_cart_item(
    customer_id: int,
    product_id: int,
    redis_db: Redis = Depends(get_redis)
):

    key = cart_key(customer_id)

    deleted = redis_db.hdel(
        key,
        str(product_id)
    )

    if deleted == 0:
        raise HTTPException(
            status_code=404,
            detail="Product is not in the cart"
        )

@router.delete(
    "/{customer_id}",
    status_code=204
)
def clear_cart(
    customer_id: int,
    redis_db: Redis = Depends(get_redis)
):

    key = cart_key(customer_id)

    redis_db.delete(key)
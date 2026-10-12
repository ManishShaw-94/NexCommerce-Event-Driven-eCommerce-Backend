from fastapi import APIRouter, Depends, HTTPException
from psycopg import Connection

from app.db import get_db


router = APIRouter(
    prefix="/orders",
    tags=["Orders"]
)

@router.get("/{order_id}")
def get_order(
    order_id: int,
    db: Connection = Depends(get_db)
):
    with db.cursor() as cursor:

        cursor.execute("""
            SELECT
                o.order_id,
                o.customer_id,
                c.name AS customer_name,
                o.status,
                o.total_amount,
                o.created_at
            FROM orders o
            JOIN customers c
                ON o.customer_id = c.customer_id
            WHERE o.order_id = %s;
        """, (order_id,))

        order = cursor.fetchone()

        if order is None:
            raise HTTPException(
                status_code=404,
                detail="Order not found"
            )

        cursor.execute("""
            SELECT
                oi.order_item_id,
                oi.product_id,
                p.name AS product_name,
                oi.quantity,
                oi.unit_price,
                oi.quantity * oi.unit_price AS line_total
            FROM order_items oi
            JOIN products p
                ON oi.product_id = p.product_id
            WHERE oi.order_id = %s
            ORDER BY oi.order_item_id;
        """, (order_id,))

        items = cursor.fetchall()

    return {
        **order,
        "items": items
    }
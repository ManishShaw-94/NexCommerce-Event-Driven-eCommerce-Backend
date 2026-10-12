from fastapi import APIRouter, Depends, HTTPException
from psycopg import Connection
from psycopg.errors import UniqueViolation

from app.db import get_db
from app.schemas import CustomerCreate


router = APIRouter(
    prefix="/customers",
    tags=["Customers"]
)

@router.get("")
def get_customers(
    db: Connection = Depends(get_db)
):
    with db.cursor() as cursor:

        cursor.execute("""
            SELECT
                customer_id,
                name,
                email,
                created_at
            FROM customers
            ORDER BY customer_id;
        """)

        customers = cursor.fetchall()

    return customers

@router.get("/{customer_id}")
def get_customer(
    customer_id: int,
    db: Connection = Depends(get_db)
):
    with db.cursor() as cursor:

        cursor.execute("""
            SELECT
                customer_id,
                name,
                email,
                created_at
            FROM customers
            WHERE customer_id = %s;
        """, (customer_id,))

        customer = cursor.fetchone()

    if customer is None:
        raise HTTPException(
            status_code=404,
            detail="Customer not found"
        )

    return customer

@router.post("", status_code=201)
def create_customer(
    customer: CustomerCreate,
    db: Connection = Depends(get_db)
):
    try:

        with db.cursor() as cursor:

            cursor.execute("""
                INSERT INTO customers (
                    name,
                    email
                )
                VALUES (%s, %s)
                RETURNING
                    customer_id,
                    name,
                    email,
                    created_at;
            """, (
                customer.name,
                customer.email
            ))

            new_customer = cursor.fetchone()

        return new_customer

    except UniqueViolation:

        raise HTTPException(
            status_code=409,
            detail="A customer with this email already exists"
        )

@router.get("/{customer_id}/orders")
def get_customer_orders(
    customer_id: int,
    db: Connection = Depends(get_db)
):
    with db.cursor() as cursor:

        cursor.execute("""
            SELECT customer_id
            FROM customers
            WHERE customer_id = %s;
        """, (customer_id,))

        customer = cursor.fetchone()

        if customer is None:
            raise HTTPException(
                status_code=404,
                detail="Customer not found"
            )

        cursor.execute("""
            SELECT
                order_id,
                status,
                total_amount,
                created_at
            FROM orders
            WHERE customer_id = %s
            ORDER BY created_at DESC;
        """, (customer_id,))

        orders = cursor.fetchall()

    return orders
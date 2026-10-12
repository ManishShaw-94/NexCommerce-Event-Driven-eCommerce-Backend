import json
import os

from fastapi import FastAPI, APIRouter, Depends, HTTPException, Response
from fastapi.encoders import jsonable_encoder
from psycopg import Connection
from redis import Redis

from app.db import get_db
from app.redis_client import get_redis
from app.schemas import ProductCreate

router = APIRouter(
    prefix="/products",
    tags=["Products"]
)


#@app.get("/products")
@router.get("")
def get_products(
    db: Connection = Depends(get_db)
):

    with db.cursor() as cursor:

        cursor.execute("""
            SELECT
                product_id,
                name,
                price,
                stock_quantity,
                created_at
            FROM products
            ORDER BY product_id;
        """)

        products = cursor.fetchall()

    return products

# @router.get("/{product_id}")
# def get_product(
#     product_id: int,
#     db: Connection = Depends(get_db)
# ):

#     with db.cursor() as cursor:

#         cursor.execute("""
#             SELECT
#                 product_id,
#                 name,
#                 price,
#                 stock_quantity,
#                 created_at
#             FROM products
#             WHERE product_id = %s;
#         """, (product_id,))

#         product = cursor.fetchone()

#     if product is None:
#         raise HTTPException(
#             status_code=404,
#             detail="Product not found"
#         )

#     return product

PRODUCT_CACHE_TTL_SECONDS = int(
    os.getenv(
        "PRODUCT_CACHE_TTL_SECONDS",
        60
    )
)

def product_cache_key(product_id: int) -> str:
    return f"product:{product_id}"

@router.get("/{product_id}")
def get_product(
    product_id: int,
    response: Response,
    db: Connection = Depends(get_db),
    redis_db: Redis = Depends(get_redis)
):

    key = product_cache_key(product_id)

    # -----------------------------------
    # 1. Check Redis first
    # -----------------------------------

    cached_product = redis_db.get(key)

    if cached_product is not None:

        response.headers["X-Cache"] = "HIT"

        return json.loads(cached_product)

    # -----------------------------------
    # 2. Redis MISS → query PostgreSQL
    # -----------------------------------

    with db.cursor() as cursor:

        cursor.execute("""
            SELECT
                product_id,
                name,
                price,
                stock_quantity,
                created_at
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

    # -----------------------------------
    # 3. Store a copy in Redis
    # -----------------------------------

    cache_value = json.dumps(
        jsonable_encoder(product)
    )

    redis_db.set(
        key,
        cache_value,
        ex=PRODUCT_CACHE_TTL_SECONDS
    )

    response.headers["X-Cache"] = "MISS"

    return product

#@app.post("/products", status_code=201)
@router.post("", status_code=201)
def create_product(
    product: ProductCreate,
    db: Connection = Depends(get_db)
):

    with db.cursor() as cursor:

        cursor.execute("""
            INSERT INTO products (
                name,
                price,
                stock_quantity
            )
            VALUES (%s, %s, %s)
            RETURNING
                product_id,
                name,
                price,
                stock_quantity,
                created_at;
        """, (
            product.name,
            product.price,
            product.stock_quantity
        ))

        new_product = cursor.fetchone()

    return new_product
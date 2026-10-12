from fastapi import FastAPI, Depends, HTTPException
from psycopg import Connection

from app.db import get_db
from app.routers import products, customers, orders, checkout, cart

app = FastAPI(
    title="SBNS API",
    version="1.0.0"
)

app.include_router(products.router)
app.include_router(customers.router)
app.include_router(orders.router)
app.include_router(checkout.router)
app.include_router(cart.router)

@app.get("/")
def root():
    return {
        "message": "SBNS API is running"
    }

@app.get("/health")
def health_check(
    db: Connection = Depends(get_db)
):

    with db.cursor() as cursor:

        cursor.execute(
            "SELECT current_database(), current_user;"
        )

        result = cursor.fetchone()

    return {
        "api": "healthy",
        "database": "healthy",
        "database_name": result["current_database"],
        "database_user": result["current_user"]
    }
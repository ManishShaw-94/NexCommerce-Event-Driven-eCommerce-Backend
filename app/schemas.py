from decimal import Decimal
from pydantic import BaseModel, Field, EmailStr

class ProductCreate(BaseModel):

    name: str = Field(
        min_length=1,
        max_length=150
    )

    price: Decimal = Field(
        gt=0
    )

    stock_quantity: int = Field(
        ge=0
    )

class CustomerCreate(BaseModel):
    name: str = Field(
        min_length=1,
        max_length=100
    )

    email: EmailStr

class CheckoutItem(BaseModel):
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0)

class CheckoutRequest(BaseModel):
    customer_id: int = Field(gt=0)

    items: list[CheckoutItem] = Field(
        min_length=1
    )

class CartItemAdd(BaseModel):

    product_id: int = Field(
        gt=0
    )

    quantity: int = Field(
        gt=0
    )


class CartItemUpdate(BaseModel):

    quantity: int = Field(
        gt=0
    )
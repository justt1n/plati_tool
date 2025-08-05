from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class AuthToken(BaseModel):
    token: str
    valid_thru: datetime


class Product(BaseModel):
    id: int = Field(..., alias='id_goods')
    name: str = Field(..., alias='name_goods')
    price: float


class GoodsListResponse(BaseModel):
    return_value: int = Field(..., alias='retval')
    return_description: str = Field(..., alias='retval_desc')
    products: List[Product] = Field(default_factory=list, alias='rows')


class Category(BaseModel):
    id: str
    name: str
    count: str = Field(..., alias='cnt')

    subcategories: Optional[List['Category']] = Field(None, alias='sub')


class CategoryListResponse(BaseModel):
    return_value: int = Field(..., alias='retval')
    return_description: str = Field(..., alias='retdesc')
    categories: List[Category] = Field(default_factory=list, alias='category')

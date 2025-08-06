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


class SaleInfo(BaseModel):
    common_base_price: Optional[float] = None
    common_price_usd: Optional[float] = None
    common_price_rur: Optional[float] = None
    common_price_eur: Optional[float] = None
    common_price_uah: Optional[float] = None
    sale_end: Optional[datetime] = None
    sale_percent: Optional[float] = None


class SellerItem(BaseModel):
    id: int = Field(..., alias='id_goods')
    name: str = Field(..., alias='name_goods')
    info: Optional[str] = Field(None, alias='info_goods')
    additional_info: Optional[str] = Field(None, alias='add_info')
    price: float
    currency: str
    sales_count: int = Field(..., alias='cnt_sell')
    returns_count: int = Field(..., alias='cnt_return')
    good_responses_count: int = Field(..., alias='cnt_goodresponses')
    bad_responses_count: int = Field(..., alias='cnt_badresponses')
    price_usd: float
    price_rur: float
    price_eur: float
    price_uah: float
    in_stock: int
    num_in_stock: int
    visible: int
    agent_commission: float = Field(..., alias='commiss_agent')
    has_discount: int
    sale_info: SaleInfo
    owner_id: int


class SellerItemsResponse(BaseModel):
    return_value: int = Field(..., alias='retval')
    return_description: Optional[str] = Field(None, alias='retdesc')
    seller_id: int = Field(..., alias='id_seller')
    seller_name: str = Field(..., alias='name_seller')
    total_goods: int = Field(..., alias='cnt_goods')
    total_pages: int = Field(..., alias='pages')
    current_page: int = Field(..., alias='page')
    order_column: str = Field(..., alias='order_col')
    order_direction: str = Field(..., alias='order_dir')
    items: List[SellerItem] = Field(..., alias='rows')

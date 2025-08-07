from datetime import datetime
from typing import List, Optional, Literal

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
    common_price_usd: Optional[str] = None
    common_price_rur: Optional[str] = None
    common_price_eur: Optional[str] = None
    common_price_uah: Optional[str] = None
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
    agent_commission: float = Field(..., alias='commiss_agent')
    visible: int
    has_discount: int
    num_options: int
    sale_info: SaleInfo
    owner_id: int

    num_in_stock: Optional[int] = None

    sales_count_hidden: int = Field(..., alias='cnt_sell_hidden')
    returns_count_hidden: int = Field(..., alias='cnt_return_hidden')
    good_responses_hidden: int = Field(..., alias='cnt_goodresponses_hidden')
    bad_responses_hidden: int = Field(..., alias='cnt_badresponses_hidden')
    release_date: Optional[str] = None


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

    seller_rating: float = Field(..., alias='rating_seller')
    show_hidden_mode: int = Field(..., alias='show_hidden')


class ProductPriceVariantUpdate(BaseModel):
    variant_id: int
    rate: float
    type: Optional[Literal['percentplus', 'percentminus', 'priceplus', 'priceminus']] = None


class ProductPriceUpdate(BaseModel):
    product_id: int
    price: float
    variants: Optional[List[ProductPriceVariantUpdate]] = None


class BulkPriceUpdateResponse(BaseModel):
    taskId: str = Field(..., alias='taskId')


class BsProduct(BaseModel):
    seller_name: Optional[str] = None
    name: str
    price: Optional[float] = None
    outside_price: str
    sold_count: Optional[str] = None
    link: str
    image_link: str

    def get_price(self) -> Optional[float]:
        if self.price != -1:
            return self.price
        if self.outside_price:
            try:
                price_value = float(self.outside_price.replace(',', '').replace(' ', ''))
                return price_value
            except ValueError:
                return None
        return None


class InsideInfo(BaseModel):
    seller_name: str
    price: float
    order_sold_count: int


class InsideProduct(BaseModel):
    price_text: str
    request_url: str

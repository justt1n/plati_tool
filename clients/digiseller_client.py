import asyncio
import hashlib
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Literal

from models.digiseller_models import AuthToken, GoodsListResponse, CategoryListResponse, SellerItemsResponse
from utils.config import settings
from .base_client import BaseAPIClient

logger = logging.getLogger(__name__)


class DigisellerClient(BaseAPIClient):
    def __init__(self):
        super().__init__(base_url=settings.DIGISELLER_API_URL)
        self.seller_id = settings.SELLER_ID
        self.api_key = settings.DIGI_API_KEY
        self._token: Optional[str] = None
        self._token_valid_thru: Optional[datetime] = None
        self._auth_lock = asyncio.Lock()

    async def _authenticate(self) -> None:
        logger.info("Getting new token...")
        timestamp = int(time.time())
        string_to_sign = self.api_key.get_secret_value() + str(timestamp)
        sign = hashlib.sha256(string_to_sign.encode('utf-8')).hexdigest()
        auth_payload = {"seller_id": self.seller_id, "timestamp": timestamp, "sign": sign}
        response = await self._make_request(method='POST', endpoint="/apilogin", json_data=auth_payload)
        token_data = AuthToken.model_validate(response.json())
        self._token = token_data.token
        self._token_valid_thru = token_data.valid_thru
        logger.info(f"Get token: {self._token}, valid until {self._token_valid_thru}")

    async def _get_valid_token(self) -> str:
        if self._token and self._token_valid_thru and self._token_valid_thru > (
            datetime.now(timezone.utc) + timedelta(minutes=1)):
            return self._token
        async with self._auth_lock:
            if self._token and self._token_valid_thru and self._token_valid_thru > (
                datetime.now(timezone.utc) + timedelta(minutes=1)):
                return self._token
            await self._authenticate()
            return self._token

    async def _prepare_payload(self, auth_required: bool, **kwargs: Any) -> Dict[str, Any]:
        if auth_required:
            valid_token = await self._get_valid_token()
            payload = {
                "token": valid_token,
                "seller_id": self.seller_id
            }
            payload.update(kwargs)
            return payload
        else:
            payload = {
                "seller_id": self.seller_id,
                **kwargs
            }
            return payload

    async def get_all_goods(self) -> GoodsListResponse:
        return await self.get(
            endpoint="/getallgoods",
            response_model=GoodsListResponse,
            auth_required=True
        )

    async def get_all_categories(self, lang: str = "en-US", category_id: int = 0) -> CategoryListResponse:
        return await self.get(
            endpoint="categories",
            response_model=CategoryListResponse,
            auth_required=False,
            lang=lang,
            category_id=category_id
        )

    async def get_seller_items(
        self,
        page: int = 1,
        rows: int = 100,
        order_col: Literal["name", "price", "cntsell", "cntreturn", "cntgoodresponses", "cntbadresponses"] = "cntsell",
        order_dir: Literal["asc", "desc"] = "desc",
        currency: Literal["USD", "RUR", "EUR", "UAH"] = "RUR",
        lang: str = "en-US",
        show_hidden: Literal[0, 1, 2] = 1,
        owner_id: Optional[int] = None
    ) -> SellerItemsResponse:
        """
        Fetches a paginated list of the seller's items with sorting and filtering.

        This method uses the POST endpoint as specified by the API documentation
        to allow for more complex filtering options.

        Args:
            page (int): The page number to retrieve. Defaults to 1.
            rows (int): The number of items to retrieve per page (max 1000). Defaults to 1000.
            order_col (Literal): The field to sort items by. Defaults to "cntsell".
            order_dir (Literal): The sorting direction, "asc" or "desc". Defaults to "desc".
            currency (Literal): The currency for displaying item prices. Defaults to "RUR".
            lang (str): The language for item information. Defaults to "en-US".
            show_hidden (Literal): Mode for displaying hidden items.
                                 0 = no hidden goods, 1 = with hidden goods, 2 = only hidden goods.
                                 Defaults to 1.
            owner_id (Optional[int]): The marketplace identifier. Defaults to None.

        Returns:
            An instance of `SellerItemsResponse` containing the list of items
            and pagination details.

        Example:
            To get the top 10 most expensive items in USD:

            async with DigisellerClient() as client:
                items_response = await client.get_seller_items(
                    rows=10,
                    order_col="price",
                    order_dir="desc",
                    currency="USD"
                )
                for item in items_response.items:
                    print(f"{item.name}: ${item.price_usd}")
        """
        request_params = {
            "order_col": order_col,
            "order_dir": order_dir,
            "rows": rows,
            "page": page,
            "currency": currency,
            "lang": lang,
            "show_hidden": show_hidden,
        }
        if owner_id is not None:
            request_params["owner_id"] = owner_id

        return await self.post(
            endpoint="seller-goods",
            response_model=SellerItemsResponse,
            auth_required=True,
            **request_params
        )

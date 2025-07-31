import asyncio
import hashlib
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from models.digiseller_models import AuthToken, GoodsListResponse
from utils.config import settings
from .base_client import BaseAPIClient


class DigisellerClient(BaseAPIClient):
    def __init__(self):
        super().__init__(base_url=settings.DIGISELLER_API_URL)
        self.seller_id = settings.SELLER_ID
        self.api_key = settings.DIGI_API_KEY
        self._token: Optional[str] = None
        self._token_valid_thru: Optional[datetime] = None
        self._auth_lock = asyncio.Lock()

    async def _authenticate(self) -> None:
        print("Getting new token...")
        timestamp = int(time.time())
        string_to_sign = self.api_key.get_secret_value() + str(timestamp)
        sign = hashlib.sha256(string_to_sign.encode('utf-8')).hexdigest()
        auth_payload = {"seller_id": self.seller_id, "timestamp": timestamp, "sign": sign}
        response = await self._make_request(method='POST', endpoint="/apilogin", json_data=auth_payload)
        token_data = AuthToken.model_validate(response.json())
        self._token = token_data.token
        self._token_valid_thru = token_data.valid_thru
        print(f"Get token: {self._token}, valid until {self._token_valid_thru}")

    async def _get_valid_token(self) -> str:
        if self._token and self._token_valid_thru and self._token_valid_thru > (datetime.now(timezone.utc) + timedelta(minutes=1)):
            return self._token
        async with self._auth_lock:
            if self._token and self._token_valid_thru and self._token_valid_thru > (datetime.now(timezone.utc) + timedelta(minutes=1)):
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

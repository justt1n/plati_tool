# api_clients/base_client.py
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import httpx

import constants

logger = logging.getLogger(__name__)


class BaseAPIClient(ABC):
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None):
        self._base_url = base_url
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers=headers or constants.DEFAULT_HEADER,
            timeout=constants.DEFAULT_API_TIMEOUT,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self._client.aclose()

    @abstractmethod
    def _prepare_request_payload(self, **kwargs: Any) -> Dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def _parse_response(self, response: httpx.Response) -> Any:
        raise NotImplementedError

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None
    ) -> httpx.Response:
        try:
            response = await self._client.request(method, endpoint, params=params, json=json_data)
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Error on {e.request.url!r}: {e.response.status_code}", exc_info=True)
            raise
        except httpx.RequestError as e:
            logger.error(f"Connection Error for {e.request.url!r}", exc_info=True)
            raise

    async def get(self, endpoint: str, **kwargs: Any) -> Any:
        prepared_params = self._prepare_request_payload(**kwargs)
        response = await self._make_request(
            method='GET',
            endpoint=endpoint,
            params=prepared_params
        )
        return self._parse_response(response)

    async def post(self, endpoint: str, **kwargs: Any) -> Any:
        json_payload = self._prepare_request_payload(**kwargs)
        response = await self._make_request(
            method='POST',
            endpoint=endpoint,
            json_data=json_payload
        )
        return self._parse_response(response)

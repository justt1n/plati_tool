# api_clients/base_client.py
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import httpx


class BaseAPIClient(ABC):
    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None):
        self._base_url = base_url
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers=headers or {},
            timeout=15.0
        )

    async def close(self):
        await self._client.aclose()

    @abstractmethod
    def _prepare_request_payload(self, **kwargs) -> Dict[str, Any]:
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
            response = await self._client.request(
                method,
                endpoint,
                params=params,
                json=json_data
            )
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            print(f"Lỗi HTTP: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            print(f"Lỗi kết nối đến {e.request.url}")
            raise

    async def post(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None
    ) -> Any:
        response = await self._make_request(
            method='POST',
            endpoint=endpoint,
            params=params,
            json_data=json_data
        )
        return self._parse_response(response)

    async def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Any:
        response = await self._make_request(
            method='GET',
            endpoint=endpoint,
            params=params
        )
        return self._parse_response(response)

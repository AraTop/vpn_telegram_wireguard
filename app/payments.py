import base64
import uuid
import aiohttp
from typing import Any, Dict, Optional

class YooKassaError(Exception):
    pass

class YooKassaClient:
    API_URL = "https://api.yookassa.ru/v3"

    def __init__(self, shop_id: str, secret_key: str):
        self.shop_id = shop_id
        self.secret_key = secret_key
        token = f"{shop_id}:{secret_key}".encode()
        self._basic = base64.b64encode(token).decode()

    async def _request(self, method: str, path: str, *, idempotence_key: str | None = None, **kwargs):
        url = f"{self.API_URL}{path}"
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Basic {self._basic}"
        if idempotence_key:
            headers["Idempotence-Key"] = idempotence_key
        headers.setdefault("Content-Type", "application/json")
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                data = await resp.json(content_type=None)
                if resp.status >= 400:
                    raise YooKassaError(f"{resp.status}: {data}")
                return data

    async def create_payment(self, amount: float, currency: str, description: str, return_url: str, metadata: Optional[Dict[str, Any]] = None ) -> Dict[str, Any]:
        idem = str(uuid.uuid4())

        payload = {
            "amount": {
                "value": f"{amount:.2f}",
                "currency": currency
            },
            "confirmation": {
                "type": "redirect",
                "return_url": return_url
            },
            "capture": True,
            "description": description,
            "metadata": metadata or {},
            "receipt": {
                "customer": {
                    # Подставляем tg_id если есть, иначе "anonymous"
                    "email": f"user{metadata.get('tg_id', 'anonymous')}@vpn.local"
                },
                "items": [
                    {
                        "description": description[:128],  # max 128 символов
                        "quantity": "1.00",
                        "amount": {
                            "value": f"{amount:.2f}",
                            "currency": currency
                        },
                        "vat_code": 1  # 1 = без НДС
                    }
                ]
            }
        }

        data = await self._request("POST", "/payments", json=payload, idempotence_key=idem)
        return data

    async def get_payment(self, payment_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"/payments/{payment_id}")

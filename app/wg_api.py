from __future__ import annotations

import aiohttp
from aiohttp import ClientSession, ClientTimeout
from typing import Any, Dict, Optional, Iterable, Tuple


class WGEasyError(Exception):
    pass


async def _safe_text(resp: aiohttp.ClientResponse) -> str:
    try:
        return await resp.text()
    except Exception:
        return ""


class WGEasyClient:
    """
    Универсальный клиент для разных версий WG-Easy.

    Требования:
      - В контейнере wg-easy используем PASSWORD_HASH (НЕ PASSWORD).
      - В .env бота обычный пароль: WGEASY_PASSWORD=... (НЕ хэш).
      - URL изнутри docker-сети: http://wg-easy:51821
        (в браузере с хоста: http://localhost:51821)
    """

    def __init__(self, base_url: str, password: str, *, timeout: int = 20):
        self.base_url = base_url.rstrip("/")
        self.password = password
        self._session: Optional[ClientSession] = None
        self._logged_in = False
        self._timeout = ClientTimeout(total=timeout)

    # ---------- auth/session ----------

    async def _ensure_raw_session(self) -> ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def _login(self) -> None:
        # Cookie-сессия через /api/session
        sess = await self._ensure_raw_session()
        url = f"{self.base_url}/api/session"
        async with sess.post(url, json={"password": self.password}) as resp:
            if resp.status not in (200, 204):
                detail = await _safe_text(resp)
                raise WGEasyError(f"Login failed: {resp.status} {detail}")
        self._logged_in = True

    async def _ensure_session(self) -> ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        if not self._logged_in:
            await self._login()
        return self._session

    # ---------- low-level request helpers ----------

    async def _request(self, method: str, path: str, **kwargs) -> aiohttp.ClientResponse:
        sess = await self._ensure_session()
        url = f"{self.base_url}{path}"
        resp = await sess.request(method, url, **kwargs)

        # если cookie протухла — перелогинимся и повторим 1 раз
        if resp.status in (401, 403):
            await resp.release()
            self._logged_in = False
            await self._login()
            sess = await self._ensure_session()
            resp = await sess.request(method, url, **kwargs)

        if resp.status >= 400:
            detail = await _safe_text(resp)
            await resp.release()
            raise WGEasyError(f"{method} {path} -> {resp.status}: {detail}")

        return resp

    async def _try_variants(
        self,
        variants: Iterable[Tuple[str, str, dict]],
    ) -> aiohttp.ClientResponse:
        """
        Перебирает (method, path, kwargs) до первого 2xx.
        400/401/403/404/405 — пробуем следующий вариант.
        5xx — пробрасываем ошибку.
        """
        last_detail = ""
        for method, path, kwargs in variants:
            try:
                resp = await self._request(method, path, **kwargs)
                return resp
            except WGEasyError as e:
                msg = str(e)
                last_detail = msg
                if any(code in msg for code in [" 400:", " 401:", " 403:", " 404:", " 405:"]):
                    continue
                raise
        raise WGEasyError(f"All endpoint variants failed. Last error: {last_detail}")

    # ---------- утилиты по клиентам ----------

    @staticmethod
    def _extract_client_id(obj: dict) -> str | None:
        """Пытается достать id из разных полей/вложенностей."""
        if not isinstance(obj, dict):
            return None
        for key in ("id", "_id", "clientId", "client_id"):
            val = obj.get(key)
            if isinstance(val, (str, int)):
                return str(val)
        client = obj.get("client")
        if isinstance(client, dict):
            for key in ("id", "_id", "clientId", "client_id"):
                val = client.get(key)
                if isinstance(val, (str, int)):
                    return str(val)
        return None

    async def list_clients(self) -> list[dict]:
        variants = [
            ("GET", "/api/wireguard/client", {}),
            ("GET", "/api/wireguard/clients", {}),
            ("GET", "/api/client", {}),
            ("GET", "/api/clients", {}),
        ]
        resp = await self._try_variants(variants)
        data = await resp.json(content_type=None)
        await resp.release()
        if isinstance(data, dict) and "clients" in data and isinstance(data["clients"], list):
            return data["clients"]
        if isinstance(data, list):
            return data
        return []

    # ---------- High-level API ----------

    async def create_client(self, name: str) -> Dict[str, Any]:
        """
        Создать peer.

        Самый типичный актуальный путь: POST /api/wireguard/client
        тело: {"name": "..."}
        На всякий — оставляем и другие варианты.
        """
        json_payload = {"json": {"name": name}}
        form_payload = {"data": {"name": name}}

        variants = [
            ("POST", "/api/wireguard/client", json_payload),
            ("POST", "/api/wireguard/clients", json_payload),
            ("PUT",  "/api/wireguard/client", json_payload),
            ("PUT",  "/api/wireguard/clients", json_payload),

            ("POST", "/api/client", json_payload),
            ("POST", "/api/clients", json_payload),
            ("PUT",  "/api/client", json_payload),
            ("PUT",  "/api/clients", json_payload),

            ("POST", "/api/client", form_payload),
            ("POST", "/api/clients", form_payload),
            ("PUT",  "/api/client", form_payload),
            ("PUT",  "/api/clients", form_payload),
        ]

        resp = await self._try_variants(variants)
        data = await resp.json(content_type=None)
        await resp.release()

        # Подстрахуемся: если id не вернули — найдём по имени
        cid = self._extract_client_id(data)
        if not cid:
            clients = await self.list_clients()
            matches = [c for c in clients if str(c.get("name", "")) == name]
            if matches:
                cid2 = self._extract_client_id(matches[-1])
                if cid2:
                    data.setdefault("id", cid2)

        return data

    async def get_config(self, client_id: str) -> str:
        """
        Получить конфиг клиента.

        Актуально для новых версий: GET /api/wireguard/client/{id}/configuration
        (раньше многие примеры писали /config — из-за этого у тебя и был 404)
        """
        if not client_id:
            raise WGEasyError("get_config: empty client_id")
        variants = [
            ("GET", f"/api/wireguard/client/{client_id}/configuration", {}),
            ("GET", f"/api/wireguard/clients/{client_id}/configuration", {}),
            ("GET", f"/api/client/{client_id}/configuration", {}),
            ("GET", f"/api/clients/{client_id}/configuration", {}),
            # устаревшие/кастомные сборки могли использовать /config
            ("GET", f"/api/wireguard/client/{client_id}/config", {}),
            ("GET", f"/api/wireguard/clients/{client_id}/config", {}),
            ("GET", f"/api/client/{client_id}/config", {}),
            ("GET", f"/api/clients/{client_id}/config", {}),
        ]
        resp = await self._try_variants(variants)
        text = await resp.text()
        await resp.release()
        return text

    async def delete_client(self, client_id: str) -> None:
        variants = [
            ("DELETE", f"/api/wireguard/client/{client_id}", {}),
            ("DELETE", f"/api/wireguard/clients/{client_id}", {}),
            ("DELETE", f"/api/client/{client_id}", {}),
            ("DELETE", f"/api/clients/{client_id}", {}),
            ("POST",   f"/api/clients/{client_id}/remove", {}),
            ("POST",   f"/api/wireguard/clients/{client_id}/remove", {}),
        ]
        resp = await self._try_variants(variants)
        await resp.release()

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

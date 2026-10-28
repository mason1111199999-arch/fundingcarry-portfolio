from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.request import Request, urlopen


def request_json(url: str, *, method: str = "GET", headers=None, body=None, timeout=15):
    data = None
    merged = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
        **(headers or {}),
    }
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode()
        merged["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=merged, method=method)
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode())


async def async_request_json(*args: Any, **kwargs: Any):
    return await asyncio.to_thread(request_json, *args, **kwargs)

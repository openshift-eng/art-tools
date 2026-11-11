"""GET-only access to the configured shipment project; never forward browser tokens."""

import hashlib
import json
import os
import ssl
import time
from collections import OrderedDict
from pathlib import Path
from urllib.parse import quote, urlsplit

import httpx

_cache = OrderedDict()
MAX_CACHE_ENTRIES = 128


class GitLabReadError(Exception):
    pass


def service_token():
    path = os.getenv("RELEASES_GITLAB_TOKEN_FILE")
    if path:
        try:
            return Path(path).read_text().strip()
        except OSError:
            return ""
    return os.getenv("RELEASES_GITLAB_TOKEN", "").strip()


class GitLabReader:
    def __init__(self, token, *, transport=None):
        self.base = os.getenv("RELEASES_GITLAB_URL", "https://gitlab.cee.redhat.com").rstrip("/")
        if urlsplit(self.base).scheme != "https" or urlsplit(self.base).path:
            raise ValueError("RELEASES_GITLAB_URL must be an HTTPS origin")
        self.project = os.getenv("RELEASES_SHIPMENT_PROJECT", "hybrid-platforms/art/ocp-shipment-data")
        self.project_path = f"/projects/{quote(self.project, safe='')}"
        context = ssl.create_default_context()
        if os.getenv("RELEASES_GITLAB_CA_FILE"):
            context.load_verify_locations(os.environ["RELEASES_GITLAB_CA_FILE"])
        self.client = httpx.AsyncClient(
            base_url=f"{self.base}/api/v4/", headers={"PRIVATE-TOKEN": token},
            verify=context, timeout=25, follow_redirects=False, transport=transport,
        )
        self.cache_key = (self.base, self.project, hashlib.sha256(token.encode()).hexdigest())

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        await self.client.aclose()

    async def get(self, path, params=None, *, fresh=False):
        # Paths come from this module and verified numeric IDs, never response URLs.
        key = (*self.cache_key, path, json.dumps(params or {}, sort_keys=True))
        cached = _cache.get(key)
        if not fresh and cached and time.monotonic() - cached[0] < 20:
            return cached[1], cached[2]
        try:
            response = await self.client.get(path.lstrip("/"), params=params)
        except httpx.RequestError as error:
            raise GitLabReadError("GitLab is temporarily unreachable") from error
        if response.status_code in (401, 403):
            raise GitLabReadError("GitLab service credential cannot read this release information")
        if not response.is_success:
            raise GitLabReadError(f"GitLab returned HTTP {response.status_code}")
        if len(response.content) > 4_000_000:
            raise GitLabReadError("GitLab response exceeds the dashboard size limit")
        try:
            data = response.json()
        except ValueError as error:
            raise GitLabReadError("GitLab returned an invalid response") from error
        next_page = response.headers.get("x-next-page", "")
        if next_page and not next_page.isdecimal():
            raise GitLabReadError("GitLab returned an invalid pagination cursor")
        if len(response.content) < 256_000:
            _cache[key] = (time.monotonic(), data, next_page)
            _cache.move_to_end(key)
            while len(_cache) > MAX_CACHE_ENTRIES:
                _cache.popitem(last=False)
        return data, next_page

    async def pages(self, path, params=None, *, max_pages=10):
        values = []
        page = "1"
        for _ in range(max_pages):
            data, page = await self.get(path, {**(params or {}), "per_page": 100, "page": page})
            if not isinstance(data, list):
                raise GitLabReadError("GitLab returned an invalid list")
            values.extend(data)
            if not page:
                return values, False
        return values, True

    async def all(self, path, params=None):
        data, truncated = await self.pages(path, params)
        if truncated:
            raise GitLabReadError("GitLab history exceeds the dashboard limit; evidence is incomplete")
        return data

    async def object(self, path, *, fresh=False):
        data, _ = await self.get(path, fresh=fresh)
        if not isinstance(data, dict):
            raise GitLabReadError("GitLab returned an invalid object")
        return data

    def safe_url(self, value):
        parsed = urlsplit(value or "")
        if parsed.scheme == "https" and parsed.netloc == urlsplit(self.base).netloc and not parsed.username:
            return value
        return None

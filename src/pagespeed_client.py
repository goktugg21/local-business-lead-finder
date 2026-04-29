from __future__ import annotations

from pathlib import Path
from typing import Any

import requests

from src.utils import cache_key, normalize_url_for_key, read_json, retry_request, write_json


PAGESPEED_URL = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
CATEGORIES = ["performance", "seo", "accessibility", "best-practices"]


class PageSpeedClient:
    def __init__(self, api_key: str, cache_dir: str | Path = "cache/pagespeed") -> None:
        self.api_key = api_key
        self.cache_dir = Path(cache_dir)

    def analyze(self, url: str, strategy: str = "mobile") -> dict[str, Any]:
        empty = {
            "performance_score": None,
            "seo_score": None,
            "accessibility_score": None,
            "best_practices_score": None,
            "lcp": "",
            "cls": "",
            "error": "",
        }
        if not url:
            return empty

        cache_value = f"{normalize_url_for_key(url)}:{strategy}:{','.join(CATEGORIES)}"
        cache_path = self.cache_dir / f"{cache_key(cache_value)}.json"
        cached = read_json(cache_path)
        if cached is not None:
            return self._extract_scores(cached)

        params: list[tuple[str, str]] = [
            ("url", url),
            ("key", self.api_key),
            ("strategy", strategy),
        ]
        # Repeated category params are the format expected by PageSpeed Insights.
        params.extend(("category", category) for category in CATEGORIES)

        try:
            data = retry_request(lambda: self._request(params), retries=2, delay_seconds=1.0)
            write_json(cache_path, data)
            return self._extract_scores(data)
        except requests.RequestException as exc:
            failed = empty.copy()
            failed["error"] = str(exc)
            return failed

    def _request(self, params: list[tuple[str, str]]) -> dict[str, Any]:
        response = requests.get(PAGESPEED_URL, params=params, timeout=40)
        response.raise_for_status()
        return response.json()

    def _extract_scores(self, data: dict[str, Any]) -> dict[str, Any]:
        lighthouse = data.get("lighthouseResult", {})
        categories = lighthouse.get("categories", {})
        audits = lighthouse.get("audits", {})

        return {
            "performance_score": self._category_score(categories, "performance"),
            "seo_score": self._category_score(categories, "seo"),
            "accessibility_score": self._category_score(categories, "accessibility"),
            "best_practices_score": self._category_score(categories, "best-practices"),
            "lcp": self._audit_display_value(audits, "largest-contentful-paint"),
            "cls": self._audit_display_value(audits, "cumulative-layout-shift"),
            "error": "",
        }

    def _category_score(self, categories: dict[str, Any], key: str) -> int | None:
        score = categories.get(key, {}).get("score")
        if score is None:
            return None
        return round(float(score) * 100)

    def _audit_display_value(self, audits: dict[str, Any], key: str) -> str:
        audit = audits.get(key, {})
        return str(audit.get("displayValue", ""))

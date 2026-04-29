from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import requests

from src.utils import cache_key, normalize_text, read_json, retry_request, write_json


PLACES_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

FIELD_MASK = ",".join(
    [
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.nationalPhoneNumber",
        "places.rating",
        "places.userRatingCount",
        "places.googleMapsUri",
        "places.websiteUri",
    ]
)


class PlacesClient:
    def __init__(
        self,
        api_key: str,
        cache_dir: str | Path = "cache/places",
        request_delay_seconds: float = 1.0,
    ) -> None:
        self.api_key = api_key
        self.cache_dir = Path(cache_dir)
        self.request_delay_seconds = request_delay_seconds
        self.total_discovered_count = 0

    def search_text(self, query: str, max_results: int = 20) -> dict[str, Any]:
        cache_path = self.cache_dir / f"{cache_key(query + str(max_results))}.json"
        cached = read_json(cache_path)
        if cached is not None:
            return cached

        # Text Search New requires an explicit field mask; avoid wildcard fields.
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": FIELD_MASK,
        }
        payload = {
            "textQuery": query,
            "pageSize": min(max_results, 20),
        }

        def send_request() -> dict[str, Any]:
            response = requests.post(
                PLACES_SEARCH_URL,
                headers=headers,
                json=payload,
                timeout=20,
            )
            response.raise_for_status()
            return response.json()

        data = retry_request(send_request, retries=2, delay_seconds=1.0)
        write_json(cache_path, data)
        time.sleep(self.request_delay_seconds)
        return data

    def discover(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        leads: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        self.total_discovered_count = 0

        for country in config.get("countries", []):
            for city in config.get("cities", []):
                for sector in config.get("sectors", []):
                    query = f"{sector} {city}"
                    try:
                        response = self.search_text(
                            query=query,
                            max_results=int(config.get("max_results_per_query", 20)),
                        )
                    except requests.RequestException as exc:
                        print(f"Places search failed for '{query}': {exc}")
                        continue

                    places = response.get("places", [])
                    self.total_discovered_count += len(places)
                    for place in places:
                        lead = self._normalize_place(place, query, country, city, sector)
                        if not self._passes_filters(lead, config):
                            continue

                        # Prefer stable Google place IDs, then fall back to name + address.
                        dedupe_key = self._dedupe_key(lead)
                        if dedupe_key in seen_keys:
                            continue

                        seen_keys.add(dedupe_key)
                        leads.append(lead)

        return leads

    def _normalize_place(
        self,
        place: dict[str, Any],
        query: str,
        country: str,
        city: str,
        sector: str,
    ) -> dict[str, Any]:
        display_name = place.get("displayName") or {}
        return {
            "place_id": place.get("id", ""),
            "business_name": display_name.get("text", ""),
            "address": place.get("formattedAddress", ""),
            "google_phone": place.get("nationalPhoneNumber", ""),
            "rating": place.get("rating"),
            "review_count": place.get("userRatingCount", 0),
            "google_maps_url": place.get("googleMapsUri", ""),
            "website_url": place.get("websiteUri", ""),
            "country": country,
            "city": city,
            "sector": sector,
            "query": query,
        }

    def _passes_filters(self, lead: dict[str, Any], config: dict[str, Any]) -> bool:
        rating = lead.get("rating") or 0
        review_count = lead.get("review_count") or 0
        min_reviews = config.get("preferred_min_reviews", config.get("min_review_count", 0))
        return (
            float(rating) >= float(config.get("min_rating", 0))
            and int(review_count) >= int(min_reviews)
        )

    def _dedupe_key(self, lead: dict[str, Any]) -> str:
        place_id = lead.get("place_id")
        if place_id:
            return f"id:{place_id}"

        name = normalize_text(lead.get("business_name"))
        address = normalize_text(lead.get("address"))
        return f"name_address:{name}:{address}"

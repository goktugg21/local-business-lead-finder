from __future__ import annotations

import re
from typing import Any

from src.utils import normalize_text


SERVICE_STEMS = (
    "loodgieter",
    "riool",
    "ontstop",
    "lekkage",
    "ketel",
    "installatie",
    "dakdek",
    "dakwerk",
    "dakrenovatie",
    "dakinspectie",
    "dakgarantie",
    "schilder",
    "stukadoor",
    "stucad",
    "aannem",
    "klus",
    "verhuiz",
    "schoonmaak",
    "tegelzet",
    "timmer",
    "glazen",
    "renovatie",
    "onderhoud",
    "badkamer",
    "keuken",
    "montage",
    "bouwbedrijf",
    "bouwgroep",
    "bouw",
    "nagel",
    "schoonheid",
    "barber",
    "kapper",
    "beauty",
    "massage",
    "wellness",
    "spoed",
)

LOCATION_KEYWORDS = {
    "amsterdam",
    "rotterdam",
    "utrecht",
    "den",
    "haag",
    "haarlem",
    "leiden",
    "amstelveen",
    "diemen",
    "zaandam",
    "almere",
    "nederland",
    "holland",
}

CITY_NEIGHBORHOOD_TERMS = {
    "centrum",
    "noord",
    "zuid",
    "oost",
    "west",
    "binnen",
    "buiten",
    "stad",
    "wijk",
    "district",
    "regio",
    "omgeving",
}

GENERIC_NAME_PARTS = {
    "service",
    "services",
    "company",
    "b.v.",
    "bv",
    "v.o.f.",
    "vof",
    "nv",
    "n.v.",
    "the",
    "de",
    "het",
    "een",
    "and",
    "en",
    "or",
    "of",
    "&",
    "-",
    "|",
    "/",
}

_SPLIT_RE = re.compile(r"\s+|[/,;|]")


def evaluate_data_quality(
    lead: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_name = str(lead.get("business_name") or "").strip()
    normalized = normalize_text(raw_name)
    city = normalize_text(lead.get("city"))

    if not normalized or len(normalized) < 3:
        return _result(normalized, "noise", "business name too short or empty")

    if city and normalized == city:
        return _result(normalized, "noise", "business name equals city")

    if city and normalized in {f"{city} centrum", f"centrum {city}"}:
        return _result(normalized, "noise", "business name is just a city neighborhood")

    parts = [p for p in _SPLIT_RE.split(normalized) if p]

    if len(parts) == 1 and parts[0] in LOCATION_KEYWORDS:
        return _result(normalized, "noise", "business name is only a location")

    if parts and all(part in LOCATION_KEYWORDS or part in CITY_NEIGHBORHOOD_TERMS for part in parts):
        return _result(normalized, "noise", "business name is only city/neighborhood words")

    significant_parts = [p for p in parts if p not in GENERIC_NAME_PARTS]
    if not significant_parts:
        return _result(normalized, "noise", "business name has no significant words")

    if "|" in raw_name:
        return _result(
            normalized,
            "review",
            "name contains pipe separator (likely SEO stuffing)",
        )

    service_hits = sum(1 for part in parts if any(stem in part for stem in SERVICE_STEMS))
    location_hits = sum(1 for part in parts if part in LOCATION_KEYWORDS)

    long_name = len(raw_name) > 50
    if long_name and service_hits >= 3:
        return _result(
            normalized,
            "review",
            f"keyword stuffing ({service_hits} service terms in long name)",
        )
    if service_hits >= 4:
        return _result(
            normalized,
            "review",
            f"name contains {service_hits} service keywords",
        )
    if service_hits >= 3 and location_hits >= 1:
        return _result(
            normalized,
            "review",
            f"name contains {service_hits} service keywords and a location",
        )

    return _result(normalized, "clean", "")


def _result(normalized: str, status: str, reason: str) -> dict[str, Any]:
    return {
        "data_quality_status": status,
        "data_quality_reason": reason,
        "normalized_business_name": normalized,
    }

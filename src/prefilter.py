from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from src.scorer import HIGH_VALUE_SECTORS
from src.utils import normalize_text


SOCIAL_MEDIA_DOMAINS = ["instagram.com", "facebook.com", "linktr.ee"]
BOOKING_PLATFORM_DOMAINS = [
    "treatwell.nl",
    "booksy.com",
    "fresha.com",
    "salonized.com",
    "barberbooking",
    "planity",
    "wixsite",
    "jimdosite",
    "webnode",
    "wordpress.com",
    "business.site",
]
DIRECTORY_DOMAINS = [
    "yelp",
    "tripadvisor",
    "indebuurt",
    "openingstijden",
    "telefoonboek",
    "google.com/maps",
    "maps.google",
    "g.page",
]
MARKETPLACE_DOMAINS = ["marktplaats", "werkspot", "trustoo", "offerteadviseur"]

DEFAULT_EXCLUDE_TERMS = [
    "doctor",
    "dentist",
    "tandarts",
    "huisarts",
    "hospital",
    "ziekenhuis",
    "clinic",
    "kliniek",
    "advocaat",
    "lawyer",
    "makelaar",
    "real estate",
    "bank",
    "insurance",
    "verzekeringen",
    "hotel",
    "school",
    "university",
    "government",
    "gemeente",
    "supermarket",
    "albert heijn",
    "jumbo",
    "lidl",
    "hema",
    "kruidvat",
    "action",
    "franchise",
    "chain",
]

FALLBACK_HIGH_VALUE_SECTORS = [
    "klusbedrijf",
    "aannemer",
    "schilder",
    "loodgieter",
    "schoonmaakbedrijf",
    "stukadoor",
    "dakdekker",
    "tegelzetter",
    "timmerman",
    "verhuisbedrijf",
]

BEAUTY_TERMS = [
    "beauty",
    "salon",
    "nagel",
    "nagels",
    "kapper",
    "barber",
    "massage",
    "wellness",
]
RESTAURANT_TERMS = ["restaurant", "cafe", "coffee", "eetcafe", "lunchroom"]


def prefilter_leads(leads: list[dict], config: dict) -> list[dict]:
    filtered: list[dict] = []
    exclude_terms = _exclude_terms(config)
    strong_threshold = int(config.get("strong_threshold", 65))
    candidate_threshold = int(config.get("candidate_threshold", 45))
    weak_threshold = 25

    for lead in leads:
        score = 0
        reasons: list[str] = []

        rating = _float_value(lead.get("rating"))
        review_count = _int_value(lead.get("review_count"))
        website_url = str(lead.get("website_url") or "").strip()
        website_type = classify_website_url(website_url)
        sector = normalize_text(lead.get("sector"))
        name = normalize_text(lead.get("business_name"))
        search_text = " ".join([name, sector, normalize_text(website_url)])
        has_phone = bool(lead.get("google_phone") or lead.get("phone") or lead.get("phone_found"))

        rejected = _reject_reason(
            search_text=search_text,
            exclude_terms=exclude_terms,
            rating=rating,
            review_count=review_count,
            has_phone=has_phone,
            website_type=website_type,
            config=config,
        )
        if rejected:
            score = -50
            status = "skip"
            candidate_type = "skip"
            reasons.append(rejected)
        else:
            score += _rating_score(rating, reasons)
            score += _review_score(review_count, config, reasons)
            score += _website_score(website_url, website_type, reasons)
            score += _contact_score(has_phone, website_url, reasons)
            score += _sector_score(sector, reasons)

            status = _status(score, strong_threshold, candidate_threshold, weak_threshold)
            candidate_type = _candidate_type(status, website_type)

        lead["business_fit_score"] = max(0, min(score, 100))
        lead["business_fit_status"] = status
        lead["business_fit_reasons"] = ", ".join(reasons)
        lead["reject_reason"] = rejected
        # Backward-compatible aliases for older sheets or cached workflow habits.
        lead["prefilter_score"] = lead["business_fit_score"]
        lead["prefilter_status"] = lead["business_fit_status"]
        lead["prefilter_reasons"] = lead["business_fit_reasons"]
        lead["candidate_type"] = candidate_type
        lead["website_type"] = website_type
        filtered.append(lead)

    return filtered


def classify_website_url(website_url: str | None) -> str:
    if not website_url:
        return "missing"

    normalized = normalize_text(website_url)
    parsed = urlparse(website_url if "://" in website_url else f"https://{website_url}")
    host = normalize_text(parsed.netloc)
    haystack = f"{host} {normalized}"

    if any(domain in haystack for domain in SOCIAL_MEDIA_DOMAINS):
        return "social_media"
    if any(domain in haystack for domain in BOOKING_PLATFORM_DOMAINS):
        return "booking_platform"
    if any(domain in haystack for domain in DIRECTORY_DOMAINS):
        return "directory"
    if any(domain in haystack for domain in MARKETPLACE_DOMAINS):
        return "marketplace"
    if "." in host and host not in {"", "www"}:
        return "custom_website"
    return "unknown"


def _exclude_terms(config: dict) -> list[str]:
    configured_terms = config.get("exclude_terms", [])
    return sorted(
        {
            normalize_text(term)
            for term in [*DEFAULT_EXCLUDE_TERMS, *configured_terms]
            if normalize_text(term)
        }
    )


def _reject_reason(
    search_text: str,
    exclude_terms: list[str],
    rating: float,
    review_count: int,
    has_phone: bool,
    website_type: str,
    config: dict,
) -> str:
    for term in exclude_terms:
        if term in search_text:
            return f"rejected term: {term}"
    if any(term in search_text for term in RESTAURANT_TERMS):
        return "restaurant/cafe excluded for now"
    if rating < float(config.get("min_rating", 4.0)):
        return "rating below minimum"
    if review_count > int(config.get("max_reviews_hard", 800)):
        return "too many reviews"
    if website_type in {"directory", "marketplace"}:
        return f"{website_type} page only"
    if website_type == "missing" and not has_phone:
        return "no website and no phone/contact path"
    return ""


def _rating_score(rating: float, reasons: list[str]) -> int:
    if rating >= 4.8:
        reasons.append("excellent rating")
        return 18
    if rating >= 4.6:
        reasons.append("very strong rating")
        return 14
    if rating >= 4.4:
        reasons.append("strong rating")
        return 10
    if rating >= 4.0:
        reasons.append("acceptable rating")
        return 4
    return -20


def _review_score(review_count: int, config: dict, reasons: list[str]) -> int:
    preferred_min = int(config.get("preferred_min_reviews", 15))
    preferred_max = int(config.get("preferred_max_reviews", 250))
    soft_max = int(config.get("max_reviews_soft", 350))

    if preferred_min <= review_count <= preferred_max:
        reasons.append("preferred review volume")
        return 16
    if 10 <= review_count < preferred_min:
        reasons.append("some review history")
        return 5
    if preferred_max < review_count <= soft_max:
        reasons.append("slightly established")
        return 6
    if soft_max < review_count <= int(config.get("max_reviews_hard", 800)):
        reasons.append("too established")
        return -12
    return -12


def _website_score(website_url: str, website_type: str, reasons: list[str]) -> int:
    if website_type == "missing":
        reasons.append("missing website")
        return 0
    if website_type in {"social_media", "booking_platform"}:
        reasons.append(f"{website_type} instead of website")
        return 0
    if website_type == "custom_website":
        reasons.append("custom website to audit")
        return 5
    if website_type == "unknown":
        reasons.append("unclear website type")
        return 0
    return -20


def _contact_score(has_phone: bool, website_url: str, reasons: list[str]) -> int:
    score = 0
    if has_phone:
        score += 10
        reasons.append("phone available")
    else:
        score -= 10
        reasons.append("no phone found")

    lowered = website_url.casefold()
    if "contact" in lowered or "mailto:" in lowered:
        score += 6
        reasons.append("contact path visible")
    return score


def _sector_score(sector: str, reasons: list[str]) -> int:
    high_value_terms = {*HIGH_VALUE_SECTORS, *FALLBACK_HIGH_VALUE_SECTORS}
    if sector in high_value_terms or any(
        term in sector or (len(sector) >= 4 and sector in term) for term in high_value_terms
    ):
        reasons.append("high value sector")
        return 18
    if any(term in sector for term in BEAUTY_TERMS):
        reasons.append("beauty sector")
        return 12
    if any(term in sector for term in RESTAURANT_TERMS):
        reasons.append("restaurant/cafe sector")
        return -30
    return 0


def _status(
    score: int,
    strong_threshold: int,
    candidate_threshold: int,
    weak_threshold: int,
) -> str:
    if score < weak_threshold:
        return "skip"
    if score >= strong_threshold:
        return "strong_candidate"
    if score >= candidate_threshold:
        return "candidate"
    return "weak"


def _candidate_type(status: str, website_type: str) -> str:
    if status == "skip":
        return "skip"
    if status == "weak":
        return "weak_fit"
    if website_type == "missing":
        return "no_website_candidate"
    if website_type in {"social_media", "booking_platform"}:
        return "platform_candidate"
    if website_type == "custom_website":
        return "redesign_candidate"
    return "weak_fit"


def _float_value(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0

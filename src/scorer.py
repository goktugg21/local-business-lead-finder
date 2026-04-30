from __future__ import annotations

from datetime import datetime
from typing import Any


SCORING_VERSION = "pain_gate_v1_2026_04_28"
CONFIRMED_DEAD = "confirmed_dead"
CONFIRMED_LOADED = "confirmed_loaded"
BLOCKED_OR_UNCERTAIN = "blocked_or_uncertain"
OUTREACH_STATUSES = {"", "contacted", "replied", "won", "lost"}


def _normalize_outreach_status(value: Any) -> str:
    status = str(value or "").strip().casefold()
    if status in OUTREACH_STATUSES:
        return status
    return ""
UNCERTAIN_ERROR_TYPES = {
    "timeout",
    "blocked",
    "unknown",
    "ssl_error",
    "connection_error",
    "too_many_redirects",
    "request_exception",
}


HIGH_VALUE_SECTORS = {
    "klusbedrijf": 24,
    "aannemer": 25,
    "schilder": 22,
    "loodgieter": 24,
    "schoonmaakbedrijf": 22,
    "dakdekker": 25,
    "stukadoor": 23,
}

BEAUTY_SECTORS = {
    "nagelstudio": 16,
    "schoonheidssalon": 19,
    "barbershop": 17,
}


def score_lead(lead: dict[str, Any]) -> dict[str, Any]:
    normalize_audit_for_scoring(lead)
    _ensure_data_quality(lead)
    audit = lead.get("website_audit", {}) or {}
    pagespeed = lead.get("pagespeed", {}) or {}
    audited = is_audited(lead)

    business_fit = _clamp(_number(lead.get("business_fit_score", lead.get("prefilter_score"))), 0, 100)
    website_pain = _website_pain_score(lead, audit, pagespeed)
    visual = lead.get("visual_audit", {}) or {}
    visual_pain = _number(visual.get("visual_pain_score"))
    if visual_pain >= 40 and lead.get("data_quality_status") == "clean":
        website_pain = _clamp(website_pain + 15, 0, 100)
    reachability = _reachability_score(lead, audit)
    final_score = round((business_fit * 0.35) + (website_pain * 0.45) + (reachability * 0.20))
    pain_gate_pass, pain_gate_reason = _pain_gate(lead, audit, pagespeed, website_pain)
    priority = _priority(_clamp(final_score, 0, 100))
    if not pain_gate_pass and priority in {"A - Write First", "B - Good Lead"}:
        priority = "C - Maybe Later"

    lead["business_fit_score"] = business_fit
    lead["website_pain_score"] = website_pain
    lead["reachability_score"] = reachability
    lead["final_opportunity_score"] = _clamp(final_score, 0, 100)
    lead["pain_gate_pass"] = pain_gate_pass
    lead["pain_gate_reason"] = pain_gate_reason
    lead["score_version"] = SCORING_VERSION
    lead["scored_at"] = datetime.now().isoformat(timespec="seconds")

    # Backward-compatible fields used by existing exports and old habits.
    lead["website_weakness_score"] = round(website_pain * 0.4)
    lead["business_value_score"] = _business_value_score(str(lead.get("sector", "")).casefold())
    lead["trust_mismatch_score"] = _trust_mismatch_score(lead, lead["website_weakness_score"])
    lead["opportunity_score"] = lead["final_opportunity_score"]
    lead["priority"] = priority
    lead["outreach_decision"] = _outreach_decision(lead, audit, audited, pain_gate_pass, priority)
    lead["outreach_status"] = _normalize_outreach_status(lead.get("outreach_status"))
    lead["reason"] = _reason(lead, audit, pagespeed, website_pain)
    lead["suggested_outreach_angle"] = _outreach_angle(lead, audit, website_pain)
    return lead


def _website_pain_score(
    lead: dict[str, Any],
    audit: dict[str, Any],
    pagespeed: dict[str, Any],
) -> int:
    score = 0
    website_type = lead.get("website_type")
    load_confidence = audit.get("load_confidence")

    if website_type == "missing" or audit.get("website_exists") is False:
        return 95
    if audit.get("website_loads") is False and load_confidence == CONFIRMED_DEAD:
        return 90
    if website_type in {"social_media", "booking_platform"}:
        score = max(score, 85)
    if audit.get("uses_https") is False:
        score += 20
    if audit.get("cta_found") is False:
        score += 15
    if audit.get("contact_form_found") is False:
        score += 15
    contact_values = [
        audit.get("email_found"),
        audit.get("phone_found"),
        audit.get("contact_form_found"),
    ]
    if all(value is False for value in contact_values) and not lead.get("google_phone"):
        score += 10
    if audit.get("meta_description") == "" and load_confidence == CONFIRMED_LOADED:
        score += 5
    text_length = audit.get("text_length", audit.get("visible_text_length"))
    if text_length is not None and _number(text_length) < 500:
        score += 10
    if audit.get("old_website_signals") and load_confidence != BLOCKED_OR_UNCERTAIN:
        score += 10

    performance = pagespeed.get("performance_score")
    if performance is not None and performance < 50:
        score += 15

    return _clamp(score, 0, 100)


def _business_value_score(sector: str) -> int:
    if sector in HIGH_VALUE_SECTORS:
        return HIGH_VALUE_SECTORS[sector]
    if sector in BEAUTY_SECTORS:
        return BEAUTY_SECTORS[sector]
    return 15


def _trust_mismatch_score(lead: dict[str, Any], website_weakness_score: int) -> int:
    score = 0
    rating = lead.get("rating") or 0
    review_count = lead.get("review_count") or 0

    if rating >= 4.6 and review_count >= 50:
        score += 10
    elif rating >= 4.3 and review_count >= 25:
        score += 6

    if website_weakness_score >= 20:
        score += 4

    return min(score, 20)


def _reachability_score(lead: dict[str, Any], audit: dict[str, Any]) -> int:
    score = 0
    if audit.get("email_found"):
        score += 35
    if lead.get("google_phone") or audit.get("phone_found"):
        score += 35
    if audit.get("contact_form_found"):
        score += 25
    if audit.get("final_url") and "contact" in str(audit.get("final_url")).casefold():
        score += 5
    return _clamp(score, 0, 100)


def _priority(score: int) -> str:
    if score >= 70:
        return "A - Write First"
    if score >= 58:
        return "B - Good Lead"
    if score >= 48:
        return "C - Maybe Later"
    return "D - Skip"


def _pain_gate(
    lead: dict[str, Any],
    audit: dict[str, Any],
    pagespeed: dict[str, Any],
    website_pain_score: int,
) -> tuple[bool, str]:
    reasons: list[str] = []
    website_type = lead.get("website_type")
    load_confidence = audit.get("load_confidence")
    performance = pagespeed.get("performance_score")
    text_length = _number(audit.get("text_length", audit.get("visible_text_length")))

    if website_type == "missing":
        reasons.append("missing website")
    if website_type == "social_media":
        reasons.append("social media instead of website")
    if website_type == "booking_platform":
        reasons.append("booking platform instead of website")
    uncertain_load = load_confidence == BLOCKED_OR_UNCERTAIN
    if audit.get("website_loads") is False and load_confidence == CONFIRMED_DEAD:
        reasons.append("website does not load")
    if uncertain_load:
        uncertain_reason = "audit uncertain / needs manual browser check"
    else:
        uncertain_reason = ""
    if audit.get("uses_https") is False:
        reasons.append("not HTTPS")
    if performance is not None and performance < 55:
        reasons.append("PageSpeed below 55")
    if audit.get("text_length", audit.get("visible_text_length")) is not None and text_length < 500:
        reasons.append("thin homepage text")
    if audit.get("contact_form_found") is False and audit.get("cta_found") is False:
        reasons.append("no form and no clear CTA")
    if website_pain_score >= 55:
        reasons.append("website pain score >= 55")
    visual = lead.get("visual_audit", {}) or {}
    if _number(visual.get("visual_pain_score")) >= 25 and lead.get("data_quality_status") == "clean":
        reasons.append("visual website pain")

    if reasons:
        if uncertain_reason:
            reasons.append(uncertain_reason)
        return True, ", ".join(reasons)
    if uncertain_reason:
        return False, uncertain_reason
    return False, "No clear website pain detected."


def _outreach_decision(
    lead: dict[str, Any],
    audit: dict[str, Any],
    audited: bool,
    pain_gate_pass: bool,
    priority: str,
) -> str:
    if lead.get("data_quality_status") in {"review", "noise"}:
        return "skip"
    reachable = _is_reachable(lead, audit)
    if audited and audit.get("load_confidence") == BLOCKED_OR_UNCERTAIN:
        return "manual_review"
    if audited and reachable and pain_gate_pass and priority in {"A - Write First", "B - Good Lead"}:
        return "send_now"
    if audited and pain_gate_pass and priority == "C - Maybe Later":
        return "manual_review"
    if audited and not pain_gate_pass:
        return "looks_fine"
    return "skip"


def is_audited(lead: dict[str, Any]) -> bool:
    audit = lead.get("website_audit", {}) or {}

    if lead.get("audit_status") == "audited":
        return True
    if audit.get("audit_status") == "audited":
        return True
    if lead.get("audited_at"):
        return True
    if audit.get("audited_at"):
        return True

    load_confidence = audit.get("load_confidence")
    if (
        load_confidence in {CONFIRMED_LOADED, CONFIRMED_DEAD, BLOCKED_OR_UNCERTAIN}
        and audit.get("http_status_code") is not None
    ):
        return True

    if audit.get("website_loads") is True and audit.get("final_url"):
        return True

    if (
        load_confidence == BLOCKED_OR_UNCERTAIN
        and audit.get("audit_error_type") in {
            "timeout",
            "ssl_error",
            "connection_error",
            "too_many_redirects",
            "request_exception",
        }
    ):
        return True

    error_message = audit.get("audit_error_message") or audit.get("error") or ""
    if load_confidence == BLOCKED_OR_UNCERTAIN and error_message:
        return True

    return False


def _is_reachable(lead: dict[str, Any], audit: dict[str, Any]) -> bool:
    return bool(
        audit.get("email_found")
        or audit.get("email_address")
        or lead.get("google_phone")
        or audit.get("phone_found")
        or audit.get("contact_form_found")
    )


def _has_contact_path(lead: dict[str, Any], audit: dict[str, Any]) -> bool:
    return bool(
        audit.get("email_found")
        or audit.get("phone_found")
        or lead.get("google_phone")
        or audit.get("contact_form_found")
    )


def _reason(
    lead: dict[str, Any],
    audit: dict[str, Any],
    pagespeed: dict[str, Any],
    website_pain_score: int,
) -> str:
    reasons: list[str] = []
    rating = lead.get("rating") or 0
    review_count = lead.get("review_count") or 0

    if rating >= 4.4 and review_count >= 25:
        reasons.append("Strong Google reputation")
    if lead.get("website_type") in {"social_media", "booking_platform"}:
        reasons.append("uses platform instead of custom website")
    if lead.get("website_type") == "missing" or audit.get("website_exists") is False:
        reasons.append("no website")
    elif audit.get("website_loads") is False and audit.get("load_confidence") == CONFIRMED_DEAD:
        reasons.append("website does not load")
    if pagespeed.get("performance_score") is not None and pagespeed["performance_score"] < 50:
        reasons.append("weak mobile performance")
    if audit.get("cta_found") is False:
        reasons.append("no clear CTA")
    if audit.get("contact_form_found") is False:
        reasons.append("no contact form")
    if audit.get("email_found"):
        reasons.append("reachable by email")
    elif lead.get("google_phone") or audit.get("phone_found"):
        reasons.append("reachable by phone")
    if website_pain_score >= 45:
        reasons.append("high website pain")

    if not reasons:
        return "Good business fit with limited website pain detected."
    return _sentence(reasons)


def _outreach_angle(lead: dict[str, Any], audit: dict[str, Any], website_pain_score: int) -> str:
    sector = str(lead.get("sector", "")).casefold()

    if lead.get("website_type") == "missing" or audit.get("website_exists") is False:
        return "Offer simple 1-page mobile website"
    if lead.get("website_type") in {"social_media", "booking_platform"}:
        return "Move from platform-only presence to a conversion-focused website"
    if audit.get("cta_found") is False or audit.get("contact_form_found") is False:
        return "Improve quote/contact flow"
    if sector in BEAUTY_SECTORS:
        return "Improve premium visual presentation and booking flow"
    if sector in HIGH_VALUE_SECTORS:
        return "Improve trust, quote requests, and mobile calls"
    if website_pain_score >= 45:
        return "Improve website conversion and mobile experience"
    return "Improve local website conversion and contact flow"


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _clamp(value: float | int, minimum: int, maximum: int) -> int:
    return max(minimum, min(round(value), maximum))


def _sentence(parts: list[str]) -> str:
    text = ", ".join(parts)
    return text[:1].upper() + text[1:] + "."


def _ensure_data_quality(lead: dict[str, Any]) -> None:
    from src.data_quality import evaluate_data_quality
    quality = evaluate_data_quality(lead)
    lead["data_quality_status"] = quality["data_quality_status"]
    lead["data_quality_reason"] = quality["data_quality_reason"]
    lead["normalized_business_name"] = quality["normalized_business_name"]
    if quality["data_quality_status"] == "noise":
        if lead.get("business_fit_status") != "skip":
            lead["business_fit_status"] = "skip"
        lead["prefilter_status"] = "skip"
        lead["candidate_type"] = "skip"
        if not lead.get("reject_reason"):
            lead["reject_reason"] = quality["data_quality_reason"] or "data quality noise"

    if quality["data_quality_status"] in {"review", "noise"}:
        existing = lead.get("visual_audit")
        if existing and existing.get("visual_audit_status") != "skipped":
            lead["visual_audit"] = {
                "visual_audit_status": "skipped",
                "browser_error_type": "data_quality_not_clean",
                "browser_error_message": "",
                "browser_loads": None,
                "desktop_screenshot_path": "",
                "mobile_screenshot_path": "",
                "mobile_has_horizontal_scroll": None,
                "above_fold_text_length": None,
                "above_fold_cta_visible": None,
                "above_fold_phone_visible": None,
                "above_fold_form_visible": None,
                "visual_pain_score": None,
                "visual_pain_reasons": ["visual audit skipped: data_quality_not_clean"],
                "visual_audited_at": existing.get("visual_audited_at") or "",
            }


def normalize_audit_for_scoring(lead: dict[str, Any]) -> None:
    audit = lead.get("website_audit")
    if not isinstance(audit, dict):
        return

    pagespeed = lead.get("pagespeed", {}) or {}
    performance = pagespeed.get("performance_score", lead.get("pagespeed_performance_score"))
    confidence = _normalized_confidence(audit, lead)
    audit["load_confidence"] = confidence

    status_code_raw = audit.get("http_status_code")
    try:
        status_code_int = int(status_code_raw) if status_code_raw not in (None, "") else None
    except (TypeError, ValueError):
        status_code_int = None
    if confidence == CONFIRMED_DEAD and status_code_int not in {404, 410}:
        if performance is not None:
            confidence = CONFIRMED_LOADED
            audit["website_loads"] = True
        else:
            confidence = BLOCKED_OR_UNCERTAIN
        audit["load_confidence"] = confidence

    if confidence == BLOCKED_OR_UNCERTAIN and not audit.get("audit_error_type"):
        existing_message = audit.get("audit_error_message") or audit.get("error") or ""
        if existing_message:
            audit["audit_error_type"] = _infer_uncertain_error_type(existing_message)

    website_loads = audit.get("website_loads")
    audit_error_type = str(audit.get("audit_error_type") or "").casefold()
    uncertain_error = (
        audit_error_type in UNCERTAIN_ERROR_TYPES
        or audit_error_type.startswith("http_403")
        or audit_error_type.startswith("http_408")
        or audit_error_type.startswith("http_429")
        or audit_error_type.startswith("http_5")
    )

    if website_loads is False and (
        performance is not None
        or confidence in {"", "unknown", BLOCKED_OR_UNCERTAIN}
        or uncertain_error
    ):
        audit["load_confidence"] = BLOCKED_OR_UNCERTAIN
        _clear_uncertain_audit_fields(audit)


def _normalized_confidence(audit: dict[str, Any], lead: dict[str, Any]) -> str:
    confidence = str(audit.get("load_confidence") or "").strip().casefold()
    status_code = audit.get("http_status_code")
    try:
        status_code = int(status_code) if status_code not in (None, "") else None
    except (TypeError, ValueError):
        status_code = None

    if confidence == "confirmed_missing_or_dead":
        if status_code in {404, 410} or lead.get("website_type") == "missing" or audit.get("website_exists") is False:
            return CONFIRMED_DEAD
        return BLOCKED_OR_UNCERTAIN
    if confidence in {CONFIRMED_DEAD, CONFIRMED_LOADED, BLOCKED_OR_UNCERTAIN}:
        return confidence
    if confidence in {"blocked", "timeout", "uncertain"}:
        return BLOCKED_OR_UNCERTAIN
    if status_code in {404, 410}:
        return CONFIRMED_DEAD
    if status_code in {401, 403, 406, 408, 429, 500, 502, 503, 504}:
        return BLOCKED_OR_UNCERTAIN
    if status_code is not None and 200 <= status_code <= 399:
        return CONFIRMED_LOADED
    return "unknown"


def _infer_uncertain_error_type(message: str) -> str:
    text = str(message or "").casefold()
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if "ssl" in text or "certificate" in text:
        return "ssl_error"
    if (
        "nameresolutionerror" in text
        or "failed to resolve" in text
        or "getaddrinfo" in text
        or "connection" in text
    ):
        return "connection_error"
    if "redirect" in text:
        return "too_many_redirects"
    return "request_exception"


def _clear_uncertain_audit_fields(audit: dict[str, Any]) -> None:
    for field in (
        "website_loads",
        "uses_https",
        "contact_form_found",
        "cta_found",
        "text_length",
        "visible_text_length",
    ):
        audit[field] = None
    audit["old_website_signals"] = []


def _has_real_audit_observation(
    audit: dict[str, Any],
    pagespeed: dict[str, Any],
    lead: dict[str, Any],
) -> bool:
    if audit.get("load_confidence") in {CONFIRMED_LOADED, CONFIRMED_DEAD, BLOCKED_OR_UNCERTAIN}:
        return True
    return _has_historical_audit_observation(audit, pagespeed, lead)


def _has_historical_audit_observation(
    audit: dict[str, Any],
    pagespeed: dict[str, Any],
    lead: dict[str, Any],
) -> bool:
    if audit.get("http_status_code") not in (None, ""):
        return True
    if audit.get("audit_error_type"):
        return True
    if audit.get("final_url") and audit.get("website_loads") is True:
        return True
    if audit.get("text_length") not in (None, "") and _number(audit.get("text_length")) > 0:
        return True
    if any(
        audit.get(field) is True
        for field in ("website_loads", "uses_https", "email_found", "phone_found", "contact_form_found", "cta_found")
    ):
        return True
    if pagespeed.get("performance_score") is not None or lead.get("pagespeed_performance_score") is not None:
        return True
    return False

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.scorer import SCORING_VERSION, score_lead
from src.prefilter import prefilter_leads
from src.storage import (
    imported_file_exists,
    mark_imported_file,
    normalize_domain,
    upsert_leads,
)


MISSING_STRINGS = {"", "-", "unknown", "nan", "none", "null"}
BOOLEAN_FIELDS = {
    "website_exists",
    "website_loads",
    "uses_https",
    "email_found",
    "phone_found",
    "contact_form_found",
    "cta_found",
}
NUMBER_FIELDS = {
    "rating",
    "review_count",
    "text_length",
    "pagespeed_performance_score",
    "pagespeed_performance",
}
IGNORED_SCORE_FIELDS = {
    "business_fit_score",
    "business_fit_status",
    "business_fit_reasons",
    "website_pain_score",
    "reachability_score",
    "final_opportunity_score",
    "opportunity_score",
    "prefilter_score",
    "prefilter_status",
    "prefilter_reasons",
    "website_weakness_score",
    "business_value_score",
    "trust_mismatch_score",
    "priority",
    "outreach_decision",
    "pain_gate_pass",
    "pain_gate_reason",
    "reason",
    "suggested_outreach_angle",
    "audit_queue",
    "final_review",
}


def import_history(source: str | None = None) -> tuple[int, int]:
    imported_rows = 0
    imported_files = 0
    for path in _history_files(source):
        file_hash = _file_hash(path)
        if imported_file_exists(file_hash):
            continue
        leads = _read_workbook(path)
        if leads:
            upsert_leads(leads)
            imported_rows += len(leads)
        mark_imported_file(file_hash, str(path))
        imported_files += 1
    return imported_rows, imported_files


def _read_workbook(path: Path) -> list[dict[str, Any]]:
    leads: list[dict[str, Any]] = []
    sheets = pd.read_excel(path, sheet_name=None)
    for dataframe in sheets.values():
        for row in dataframe.to_dict(orient="records"):
            lead = normalize_imported_row(row, path)
            if lead:
                leads.append(lead)
    return leads


def normalize_imported_row(row: dict[str, Any], source_file: Path) -> dict[str, Any]:
    lead: dict[str, Any] = {
        "legacy_import_json": json.dumps(_json_safe(row), ensure_ascii=False),
        "legacy_source_file": str(source_file),
        "score_version": SCORING_VERSION,
    }

    for raw_key, raw_value in row.items():
        key = str(raw_key).strip()
        if not key or key.startswith("Unnamed:") or key in IGNORED_SCORE_FIELDS:
            continue
        value = _missing_to_none(raw_value)
        if key in BOOLEAN_FIELDS:
            lead[key] = _to_optional_bool(value)
        elif key in NUMBER_FIELDS:
            lead[key] = _to_number(value)
        else:
            lead[key] = "" if value is None else value

    lead["website_domain"] = normalize_domain(lead.get("website_url"))
    audit = _audit_from_observed_fields(lead)
    if audit:
        lead["website_audit"] = audit
    pagespeed = _pagespeed_from_observed_fields(lead)
    if pagespeed:
        lead["pagespeed"] = pagespeed
    prefilter_leads([lead], {})
    score_lead(lead)
    return lead


def _audit_from_observed_fields(lead: dict[str, Any]) -> dict[str, Any] | None:
    observed = {
        "website_exists",
        "website_loads",
        "uses_https",
        "email_found",
        "email_address",
        "website_email",
        "phone_found",
        "phone_text",
        "website_phone",
        "contact_form_found",
        "cta_found",
        "meta_description",
        "text_length",
        "old_website_signals",
        "homepage_title",
        "title",
        "final_url",
        "load_confidence",
        "audit_error_type",
        "audit_error_message",
        "http_status_code",
        "audit_status",
        "audited_at",
    }
    if not any(lead.get(field) not in (None, "", [], {}) for field in observed):
        return None
    return {
        "website_exists": _to_optional_bool(lead.get("website_exists")),
        "website_loads": _to_optional_bool(lead.get("website_loads")),
        "final_url": str(lead.get("final_url") or ""),
        "load_confidence": str(lead.get("load_confidence") or "unknown"),
        "audit_error_type": str(lead.get("audit_error_type") or ""),
        "audit_error_message": str(lead.get("audit_error_message") or ""),
        "http_status_code": _to_int_or_none(lead.get("http_status_code")),
        "uses_https": _to_optional_bool(lead.get("uses_https")),
        "email_found": _observed_contact_bool(
            lead.get("email_found"),
            lead.get("email_address") or lead.get("website_email"),
        ),
        "email_address": str(lead.get("email_address") or lead.get("website_email") or ""),
        "phone_found": _observed_contact_bool(
            lead.get("phone_found"),
            lead.get("website_phone"),
        ),
        "phone_text": str(lead.get("phone_text") or lead.get("website_phone") or ""),
        "contact_form_found": _to_optional_bool(lead.get("contact_form_found")),
        "cta_found": _to_optional_bool(lead.get("cta_found")),
        "meta_description": str(lead.get("meta_description") or ""),
        "text_length": _to_int_or_none(lead.get("text_length")),
        "visible_text_length": _to_int_or_none(lead.get("text_length")),
        "old_website_signals": _signals_from_value(lead.get("old_website_signals")),
        "homepage_title": str(lead.get("homepage_title") or lead.get("title") or ""),
        "title": str(lead.get("title") or lead.get("homepage_title") or ""),
        "audit_status": str(lead.get("audit_status") or ""),
        "audited_at": str(lead.get("audited_at") or ""),
        "error": "",
    }


def _pagespeed_from_observed_fields(lead: dict[str, Any]) -> dict[str, Any] | None:
    performance = _to_number(lead.get("pagespeed_performance_score"))
    if performance is None:
        performance = _to_number(lead.get("pagespeed_performance"))
    if performance is None:
        return None
    return {
        "performance_score": performance,
        "seo_score": _to_number(lead.get("pagespeed_seo")),
        "accessibility_score": _to_number(lead.get("pagespeed_accessibility")),
        "best_practices_score": _to_number(lead.get("pagespeed_best_practices")),
        "lcp": str(lead.get("lcp") or ""),
        "cls": str(lead.get("cls") or ""),
        "error": "",
    }


def _history_files(source: str | None) -> list[Path]:
    if source:
        path = Path(source)
        if path.is_file():
            return [path]
        if path.is_dir():
            return _excel_files(path)
    return _excel_files(Path("output"))


def _excel_files(directory: Path) -> list[Path]:
    return [
        path
        for path in sorted(directory.rglob("*.xlsx"))
        if not path.name.startswith("~$")
        and path.resolve() != Path("output/latest.xlsx").resolve()
    ]


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _missing_to_none(value: Any) -> Any | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str) and value.strip().casefold() in MISSING_STRINGS:
        return None
    return value


def _to_optional_bool(value: Any) -> bool | None:
    value = _missing_to_none(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().casefold()
    if text in {"yes", "y", "true", "t", "1"}:
        return True
    if text in {"no", "n", "false", "f", "0"}:
        return False
    return bool(text)


def _to_bool(value: Any) -> bool:
    return bool(_to_optional_bool(value))


def _observed_contact_bool(value: Any, fallback_text: Any) -> bool | None:
    explicit = _to_optional_bool(value)
    if explicit is not None:
        return explicit
    fallback_text = _missing_to_none(fallback_text)
    if fallback_text:
        return True
    return None


def _to_number(value: Any) -> int | float | None:
    value = _missing_to_none(value)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value) if float(value).is_integer() else float(value)
    text = str(value).strip().replace(",", ".")
    try:
        number = float(text)
    except ValueError:
        return None
    return int(number) if number.is_integer() else number


def _to_int_or_none(value: Any) -> int | None:
    number = _to_number(value)
    if number is None:
        return None
    return int(number)


def _signals_from_value(value: Any) -> list[str]:
    value = _missing_to_none(value)
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _json_safe(row: dict[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in row.items():
        value = _missing_to_none(value)
        if value is None:
            safe[str(key)] = None
            continue
        try:
            json.dumps(value)
            safe[str(key)] = value
        except TypeError:
            safe[str(key)] = str(value)
    return safe

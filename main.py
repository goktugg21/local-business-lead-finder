from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

# Reconfigure stdout/stderr to UTF-8 with replacement so the pipeline never
# crashes on non-Latin business names (Greek, Turkish, etc.) when the host's
# default codec (cp1252/cp1254 on Windows) cannot encode them.
for _stream_name in ("stdout", "stderr"):
    _stream = getattr(sys, _stream_name, None)
    if _stream is not None and hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

from src.exporter import audited_leads, export_leads, send_candidate_leads
from src.importer import import_history, normalize_imported_row
from src.storage import (
    SCORING_VERSION,
    archive_old_exports,
    clear_current_run_flags,
    export_latest,
    load_leads,
    replace_leads,
    upsert_leads,
)
from src.pagespeed_client import PageSpeedClient
from src.places_client import PlacesClient
from src.prefilter import prefilter_leads
from src.scorer import score_lead
from src.scorer import is_audited
from src.utils import cache_key, normalize_url_for_key, read_json, write_json
from src.visual_auditor import BrowserVisualAuditor
from src.website_auditor import WebsiteAuditor


CONFIG_PATH = Path("config.yaml")
AUDIT_CACHE_DIR = Path("cache/audits")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Find local business website redesign leads.")
    parser.add_argument(
        "--mode",
        choices=["discover", "fast", "full", "pipeline"],
        default="fast",
        help="pipeline: staged end-to-end run, discover: no audit, fast: audit only, full: audit + PageSpeed",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Override max_candidates_to_audit.",
    )
    parser.add_argument(
        "--pagespeed",
        action="store_true",
        help="Force PageSpeed for selected candidates.",
    )
    parser.add_argument("--city", default=None, help="Override config cities with one city.")
    parser.add_argument("--cities", default=None, help="Override config cities with comma-separated cities.")
    parser.add_argument("--sector", default=None, help="Override config sectors with one sector.")
    parser.add_argument("--preset", default=None, help="Use a configured query preset, such as home_services or beauty.")
    parser.add_argument(
        "--max-audit-queue",
        type=int,
        default=None,
        help="Override max_audit_queue.",
    )
    parser.add_argument(
        "--audit-limit",
        type=int,
        default=None,
        help="Override max_audit_queue for this run.",
    )
    parser.add_argument(
        "--final-limit",
        type=int,
        default=None,
        help="Override final_top_n for the Final Review sheet.",
    )
    parser.add_argument(
        "--import-history",
        nargs="?",
        const="output",
        default=None,
        help="Import historical Excel rows from a file or directory, export output/latest.xlsx, then exit.",
    )
    parser.add_argument(
        "--rescore-all",
        action="store_true",
        help="Rescore the local database, export output/latest.xlsx, then exit.",
    )
    parser.add_argument(
        "--archive-old-exports",
        action="store_true",
        help="Archive existing output/*.xlsx files except latest.xlsx, then exit.",
    )
    parser.add_argument(
        "--archive-export",
        action="store_true",
        help="Also create a timestamped Excel export after writing output/latest.xlsx.",
    )
    parser.add_argument(
        "--reaudit",
        action="store_true",
        help="Re-audit leads that are already audited or marked needs_browser_check.",
    )
    parser.add_argument(
        "--audit-global-backlog",
        action="store_true",
        help="Audit historical-DB leads matching the current scope, not just current_run discoveries.",
    )
    parser.add_argument(
        "--visual-audit",
        action="store_true",
        help="Run a headless-browser visual audit on top current-run custom websites that have load_confidence=confirmed_loaded.",
    )
    parser.add_argument(
        "--visual-limit",
        type=int,
        default=10,
        help="Maximum number of visual audits per run (default: 10).",
    )
    return parser.parse_args()


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def require_api_key() -> str:
    load_dotenv()
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    if not api_key:
        print("Missing GOOGLE_API_KEY. Create .env from .env.example and add your key.")
        sys.exit(1)
    return api_key


def main() -> None:
    args = parse_args()
    if _run_database_command(args):
        return

    config = load_config()
    _apply_cli_overrides(config, args)
    api_key = require_api_key()
    clear_current_run_flags()

    print("Starting local business lead discovery...")
    places_client = PlacesClient(
        api_key=api_key,
        request_delay_seconds=float(config.get("request_delay_seconds", 1.0)),
    )

    leads = places_client.discover(config)
    for lead in leads:
        lead["current_run"] = True
    print(f"Raw Places results: {places_client.total_discovered_count}")
    print(f"Unique leads after dedupe: {len(leads)}")

    leads = prefilter_leads(leads, config)
    leads = sorted(leads, key=lambda lead: lead.get("business_fit_score", 0), reverse=True)
    upsert_leads(leads)

    audit_limit = _audit_limit(config, args)
    master_leads = load_leads()
    candidates_available, scope_stats = _select_audit_candidates_from_master(
        master_leads, config, args
    )
    audit_candidates = candidates_available[: max(audit_limit, 0)]
    _mark_audit_queue_master(master_leads, audit_candidates)
    counts = Counter(lead.get("business_fit_status", "weak") for lead in leads)
    print(f"Rejected by hard rules: {sum(1 for lead in leads if lead.get('reject_reason'))}")
    print(
        "Business fit candidates: "
        f"{counts.get('strong_candidate', 0) + counts.get('candidate', 0)}"
    )
    print(f"Audit limit requested: {audit_limit}")
    print(f"Candidates available for audit: {len(candidates_available)}")
    print(f"Audit Queue: {len(audit_candidates)} leads.")
    print(f"Skipped (already audited): {scope_stats['skipped_already_audited']}")
    print(f"Skipped (needs browser check): {scope_stats['skipped_browser_check']}")
    print(f"Skipped (candidate type): {scope_stats['skipped_candidate_type']}")
    print(f"Skipped (business fit): {scope_stats['skipped_business_fit']}")
    print(f"Skipped (out of scope): {scope_stats['skipped_scope']}")
    print(f"Skipped (outreach status): {scope_stats['skipped_outreach_status']}")
    print(f"Skipped (data quality not clean): {scope_stats['skipped_data_quality']}")
    print(f"Skipped (not current run): {scope_stats['skipped_not_current_run']}")

    if args.mode == "discover":
        upsert_leads(master_leads)
        print("Discover mode selected: no website audit or PageSpeed calls.")
        output_path = export_latest()
        if args.archive_export:
            export_leads(load_leads(), filename_prefix="leads_output")
        print(f"Exported: {output_path}")
        return

    pagespeed_enabled = _pagespeed_enabled(args)
    pagespeed_limit = _pagespeed_limit(args, config, len(audit_candidates))
    estimated_pagespeed_calls = pagespeed_limit if pagespeed_enabled else 0

    print(f"Auditing only {len(audit_candidates)} leads.")
    if len(audit_candidates) < audit_limit:
        print(f"Only {len(audit_candidates)} candidates are available; audit limit was {audit_limit}.")
    print(f"PageSpeed enabled: {'yes' if pagespeed_enabled else 'no'}.")
    print(f"Estimated Google PageSpeed calls: {estimated_pagespeed_calls}.")

    website_auditor = WebsiteAuditor()
    pagespeed_client = PageSpeedClient(api_key=api_key)
    pagespeed_run_count = 0

    pagespeed_place_ids = {
        lead.get("place_id")
        for lead in audit_candidates[:pagespeed_limit]
        if lead.get("place_id")
    }
    pagespeed_urls = {
        normalize_url_for_key(lead.get("website_url", ""))
        for lead in audit_candidates[:pagespeed_limit]
        if lead.get("website_url")
    }

    for index, lead in enumerate(audit_candidates, start=1):
        name = lead.get("business_name") or "Unknown business"
        print(f"[{index}/{len(audit_candidates)}] Auditing {name}")

        audit = _audit_lead_cached(lead, website_auditor)
        lead["website_audit"] = audit

        if pagespeed_enabled and _should_run_pagespeed(lead, pagespeed_place_ids, pagespeed_urls):
            pagespeed_url = audit.get("final_url") or lead.get("website_url", "")
            if audit.get("website_loads"):
                lead["pagespeed"] = pagespeed_client.analyze(
                    pagespeed_url,
                    strategy=str(config.get("pagespeed_strategy", "mobile")),
                )
                pagespeed_run_count += 1
            else:
                lead["pagespeed"] = {}
        else:
            lead["pagespeed"] = {}

        score_lead(lead)
        lead["score_version"] = SCORING_VERSION

    if args.visual_audit:
        _run_visual_audit(audit_candidates, args.visual_limit)

    final_limit = args.final_limit or int(config.get("final_top_n", 25))
    _mark_final_review(master_leads, final_limit)
    upsert_leads(master_leads)
    final_review_count = _rescore_database(final_limit)
    print(f"Actually audited: {len(audit_candidates)}")
    print(f"Final review limit: {final_limit}")
    print(f"Final review leads: {final_review_count}")
    output_path = export_latest()
    if args.archive_export:
        export_leads(load_leads(), filename_prefix="leads_output")
    _print_summary(load_leads(), output_path, pagespeed_run_count, current_run_audited=len(audit_candidates))


def _audit_eligible(leads: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    allowed_statuses = {"strong_candidate"}
    if not bool(config.get("audit_only_strong_candidates", False)):
        allowed_statuses.add("candidate")

    max_business_candidates = int(config.get("max_business_candidates", 250))
    return [
        lead
        for lead in leads
        if lead.get("business_fit_status", lead.get("prefilter_status")) in allowed_statuses
    ][: max(max_business_candidates, 0)]


def _audit_limit(config: dict[str, Any], args: argparse.Namespace) -> int:
    if args.audit_limit is not None:
        return args.audit_limit
    if args.limit is not None:
        return args.limit
    if args.max_audit_queue is not None:
        return args.max_audit_queue
    return int(config.get("max_audit_queue", config.get("max_candidates_to_audit", 80)))


def _apply_cli_overrides(config: dict[str, Any], args: argparse.Namespace) -> None:
    sectors: list[str] = []
    if args.preset:
        preset = config.get("query_presets", {}).get(args.preset, {})
        sectors.extend(preset.get("sectors", []))
    if args.sector:
        sectors.extend(_split_csv(args.sector))
    if sectors:
        config["sectors"] = _dedupe_keep_order(sectors)

    cities: list[str] = []
    if args.city:
        cities.append(args.city)
    if args.cities:
        cities.extend(_split_csv(args.cities))
    if cities:
        config["cities"] = _dedupe_keep_order(cities)

    if args.max_audit_queue is not None:
        config["max_audit_queue"] = args.max_audit_queue
        config["max_candidates_to_audit"] = args.max_audit_queue
    if args.audit_limit is not None:
        config["max_audit_queue"] = args.audit_limit
        config["max_candidates_to_audit"] = args.audit_limit
    if args.final_limit is not None:
        config["final_top_n"] = args.final_limit


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _mark_audit_queue(
    leads: list[dict[str, Any]],
    audit_candidates: list[dict[str, Any]],
) -> None:
    queued_ids = {id(lead) for lead in audit_candidates}
    for lead in leads:
        lead["audit_queue"] = id(lead) in queued_ids


def _mark_audit_queue_master(
    master_leads: list[dict[str, Any]],
    audit_candidates: list[dict[str, Any]],
) -> None:
    candidate_ids = {id(lead) for lead in audit_candidates}
    for lead in master_leads:
        lead["audit_queue"] = id(lead) in candidate_ids


def _select_visual_candidates(
    audited_this_run: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for lead in audited_this_run:
        if lead.get("candidate_type") != "redesign_candidate":
            continue
        if lead.get("data_quality_status") != "clean":
            continue
        audit = lead.get("website_audit", {}) or {}
        if audit.get("load_confidence") != "confirmed_loaded":
            continue
        visual = lead.get("visual_audit", {}) or {}
        if visual.get("visual_audit_status") == "audited":
            continue
        candidates.append(lead)
    candidates.sort(key=lambda lead: lead.get("business_fit_score", 0), reverse=True)
    return candidates[: max(limit, 0)]


def _run_visual_audit(audited_this_run: list[dict[str, Any]], visual_limit: int) -> None:
    candidates = _select_visual_candidates(audited_this_run, visual_limit)
    auditor = BrowserVisualAuditor()
    if not auditor.playwright_available:
        print(
            "Visual audit skipped: Playwright is not installed. "
            "Install with `pip install playwright` and `playwright install chromium`."
        )
        for lead in candidates:
            lead["visual_audit"] = auditor.audit(lead, lead.get("website_audit", {}) or {})
        print(f"Visual audit candidates marked as skipped: {len(candidates)}")
        return

    print(f"Running visual audit on {len(candidates)} custom-website leads.")
    for index, lead in enumerate(candidates, start=1):
        name = lead.get("business_name") or "Unknown business"
        print(f"  [visual {index}/{len(candidates)}] {name}")
        audit = lead.get("website_audit", {}) or {}
        lead["visual_audit"] = auditor.audit(lead, audit)
        score_lead(lead)


def _select_audit_candidates_from_master(
    master_leads: list[dict[str, Any]],
    config: dict[str, Any],
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    eligible_statuses = {"strong_candidate"}
    if not bool(config.get("audit_only_strong_candidates", False)):
        eligible_statuses.add("candidate")

    excluded_candidate_types = {
        "no_website_candidate",
        "platform_candidate",
        "weak_fit",
        "skip",
    }

    contacted_statuses = {"contacted", "replied", "won", "lost"}

    cities = {str(c).casefold() for c in config.get("cities", []) if c}
    sectors = {str(s).casefold() for s in config.get("sectors", []) if s}

    stats = {
        "skipped_already_audited": 0,
        "skipped_browser_check": 0,
        "skipped_candidate_type": 0,
        "skipped_business_fit": 0,
        "skipped_scope": 0,
        "skipped_outreach_status": 0,
        "skipped_data_quality": 0,
        "skipped_not_current_run": 0,
    }

    audit_global_backlog = bool(getattr(args, "audit_global_backlog", False))

    filtered: list[dict[str, Any]] = []
    for lead in master_leads:
        if cities:
            lead_city = str(lead.get("city") or "").casefold()
            if lead_city not in cities:
                stats["skipped_scope"] += 1
                continue
        if sectors:
            lead_sector = str(lead.get("sector") or "").casefold()
            if lead_sector not in sectors:
                stats["skipped_scope"] += 1
                continue

        if not audit_global_backlog and not lead.get("current_run"):
            stats["skipped_not_current_run"] += 1
            continue

        if lead.get("data_quality_status") != "clean":
            stats["skipped_data_quality"] += 1
            continue

        candidate_type = lead.get("candidate_type")
        if candidate_type in excluded_candidate_types:
            stats["skipped_candidate_type"] += 1
            continue

        status = lead.get("business_fit_status", lead.get("prefilter_status"))
        if status not in eligible_statuses:
            stats["skipped_business_fit"] += 1
            continue

        outreach_status = str(lead.get("outreach_status") or "").strip().casefold()
        if not args.reaudit and outreach_status in contacted_statuses:
            stats["skipped_outreach_status"] += 1
            continue

        if not args.reaudit:
            if is_audited(lead):
                stats["skipped_already_audited"] += 1
                continue
            audit = lead.get("website_audit", {}) or {}
            if audit.get("audit_status") == "needs_browser_check":
                stats["skipped_browser_check"] += 1
                continue

        filtered.append(lead)

    filtered.sort(
        key=lambda lead: lead.get("business_fit_score", 0),
        reverse=True,
    )

    max_business_candidates = int(config.get("max_business_candidates", 250))
    return filtered[: max(max_business_candidates, 0)], stats


def _pagespeed_enabled(args: argparse.Namespace) -> bool:
    return bool(args.pagespeed and args.mode != "discover")


def _pagespeed_limit(args: argparse.Namespace, config: dict[str, Any], audit_count: int) -> int:
    return min(int(config.get("pagespeed_top_n", 30)), audit_count)


def _mark_final_review(leads: list[dict[str, Any]], final_limit: int) -> int:
    audited = [
        lead
        for lead in leads
        if is_audited(lead)
        and lead.get("final_opportunity_score") is not None
    ]
    top = sorted(
        audited,
        key=lambda lead: lead.get("final_opportunity_score", 0),
        reverse=True,
    )[: max(final_limit, 0)]
    final_ids = {id(lead) for lead in top}
    for lead in leads:
        lead["final_review"] = id(lead) in final_ids
    return len(top)


def _should_run_pagespeed(
    lead: dict[str, Any],
    pagespeed_place_ids: set[str],
    pagespeed_urls: set[str],
) -> bool:
    place_id = lead.get("place_id")
    if place_id and place_id in pagespeed_place_ids:
        return True
    website_url = normalize_url_for_key(lead.get("website_url", ""))
    return bool(website_url and website_url in pagespeed_urls)


def _audit_lead_cached(
    lead: dict[str, Any],
    website_auditor: WebsiteAuditor,
) -> dict[str, Any]:
    cache_paths = _audit_cache_paths(lead)
    for cache_path in cache_paths:
        cached = read_json(cache_path)
        if cached is not None:
            normalized = _normalize_audit_result(cached)
            for path in cache_paths:
                write_json(path, normalized)
            return normalized

    audit = website_auditor.audit(lead.get("website_url"))
    audit = _normalize_audit_result(audit)
    for cache_path in cache_paths:
        write_json(cache_path, audit)
    return audit


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


def _normalize_audit_result(audit: dict[str, Any]) -> dict[str, Any]:
    email_value = audit.get("email_found")
    phone_value = audit.get("phone_found")
    load_confidence = audit.get("load_confidence") or "unknown"
    website_loads = _optional_bool(audit.get("website_loads"))
    status_code = _optional_int(audit.get("http_status_code"))
    if load_confidence == "confirmed_missing_or_dead":
        if status_code in {404, 410} or audit.get("website_exists") is False:
            load_confidence = "confirmed_dead"
        else:
            load_confidence = "blocked_or_uncertain"
    if load_confidence == "confirmed_dead" and status_code not in {404, 410}:
        if audit.get("pagespeed_performance_score") is not None:
            # PageSpeed data means the site loaded for someone; trust that signal.
            load_confidence = "confirmed_loaded"
            website_loads = True
        else:
            load_confidence = "blocked_or_uncertain"
    if status_code in {404, 410} and website_loads is False:
        load_confidence = "confirmed_dead"
    if status_code in {401, 403, 406, 408, 429, 500, 502, 503, 504}:
        load_confidence = "blocked_or_uncertain"
    if load_confidence == "unknown" and website_loads is False:
        load_confidence = "blocked_or_uncertain"
    blocked_or_uncertain = load_confidence == "blocked_or_uncertain"
    audit_error_message = str(
        audit.get("audit_error_message") or audit.get("error") or ""
    )
    audit_error_type = str(audit.get("audit_error_type") or "")
    if (
        load_confidence == "blocked_or_uncertain"
        and not audit_error_type
        and audit_error_message
    ):
        audit_error_type = _infer_uncertain_error_type(audit_error_message)
    normalized = {
        "website_exists": _optional_bool(audit.get("website_exists")),
        "website_loads": None if blocked_or_uncertain else website_loads,
        "final_url": str(audit.get("final_url") or ""),
        "uses_https": _optional_bool(audit.get("uses_https")),
        "audit_error_type": audit_error_type,
        "audit_error_message": audit_error_message,
        "http_status_code": status_code,
        "load_confidence": load_confidence,
        "email_found": None if blocked_or_uncertain else _optional_bool(email_value),
        "email_address": email_value if isinstance(email_value, str) else str(audit.get("email_address") or ""),
        "phone_found": None if blocked_or_uncertain else _optional_bool(phone_value),
        "phone_text": phone_value if isinstance(phone_value, str) else str(audit.get("phone_text") or ""),
        "contact_form_found": None if blocked_or_uncertain else _optional_bool(audit.get("contact_form_found")),
        "cta_found": None if blocked_or_uncertain else _optional_bool(audit.get("cta_found")),
        "meta_description": str(audit.get("meta_description") or ""),
        "text_length": None if blocked_or_uncertain else _optional_int(audit.get("text_length", audit.get("visible_text_length"))),
        "visible_text_length": None if blocked_or_uncertain else _optional_int(audit.get("visible_text_length", audit.get("text_length"))),
        "old_website_signals": [] if blocked_or_uncertain else _normalize_signals(audit.get("old_website_signals")),
        "homepage_title": str(audit.get("homepage_title") or audit.get("title") or ""),
        "title": str(audit.get("title") or audit.get("homepage_title") or ""),
        "error": str(audit.get("error") or ""),
        "audit_status": str(audit.get("audit_status") or ""),
        "audited_at": str(audit.get("audited_at") or ""),
    }
    return normalized


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().casefold()
    if not text or text in {"-", "unknown", "nan", "none", "null"}:
        return None
    if text in {"yes", "y", "true", "t", "1"}:
        return True
    if text in {"no", "n", "false", "f", "0"}:
        return False
    return bool(text)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalize_signals(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _audit_cache_paths(lead: dict[str, Any]) -> list[Path]:
    paths: list[Path] = []
    place_id = str(lead.get("place_id") or "").strip()
    website_url = str(lead.get("website_url") or "").strip()

    if place_id:
        paths.append(AUDIT_CACHE_DIR / f"place_{cache_key(place_id)}.json")
    if website_url:
        normalized_url = normalize_url_for_key(website_url)
        paths.append(AUDIT_CACHE_DIR / f"url_{cache_key(normalized_url)}.json")
    if not paths:
        fallback = f"{lead.get('business_name', '')}:{lead.get('address', '')}"
        paths.append(AUDIT_CACHE_DIR / f"lead_{cache_key(fallback)}.json")
    return paths


def _print_summary(
    leads: list[dict[str, Any]],
    output_path: Path,
    pagespeed_run_count: int,
    current_run_audited: int | None = None,
) -> None:
    from src.exporter import _decision_bucket

    exported_audited_leads = audited_leads(leads)
    current_run_leads = [lead for lead in leads if lead.get("current_run")]
    audited_this_run_clean = [
        lead
        for lead in leads
        if lead.get("audit_queue")
        and lead.get("data_quality_status") == "clean"
        and is_audited(lead)
    ]
    bucket_counts = Counter(_decision_bucket(lead) for lead in current_run_leads)

    print("")
    print("Done.")
    print(f"Exported: {output_path}")

    print("")
    print("Current run summary:")
    print(f"  Current run leads: {len(current_run_leads)}")
    if current_run_audited is not None:
        print(f"  Actually audited this run: {current_run_audited}")
    print(f"  Audited this run (clean): {len(audited_this_run_clean)}")
    for bucket in (
        "send_now",
        "no_website_offer",
        "platform_offer",
        "manual_review",
        "needs_browser_check",
        "looks_fine",
        "hard_skip",
        "data_quality_review",
    ):
        print(f"  {bucket}: {bucket_counts.get(bucket, 0)}")
    current_priority_counts = Counter(
        lead.get("priority", "Unknown") for lead in audited_this_run_clean
    )
    for priority, count in sorted(current_priority_counts.items()):
        print(f"  audited this run priority {priority}: {count}")

    print("")
    print("All audited database:")
    print(f"  Total DB leads: {len(leads)}")
    print(f"  Truly audited (DB-wide): {len(exported_audited_leads)}")
    print(f"  PageSpeed calls made: {pagespeed_run_count}")
    print(f"  Email-ready (DB-wide): {len(send_candidate_leads(leads))}")
    db_priority_counts = Counter(
        lead.get("priority", "Unknown") for lead in exported_audited_leads
    )
    for priority, count in sorted(db_priority_counts.items()):
        print(f"  DB-wide audited priority {priority}: {count}")

    top = sorted(
        audited_this_run_clean,
        key=lambda lead: lead.get("opportunity_score", 0),
        reverse=True,
    )[:5]
    print("")
    if top:
        print("Top audited this run:")
        for lead in top:
            print(
                f"- {lead.get('opportunity_score')} | {lead.get('priority')} | "
                f"{lead.get('business_name')} | {lead.get('city')} | {lead.get('sector')}"
            )
    else:
        print("No leads audited this run.")


def _run_database_command(args: argparse.Namespace) -> bool:
    if args.import_history is not None:
        imported_count, imported_files = import_history(args.import_history)
        output_path = export_latest()
        print(f"Imported historical rows: {imported_count}")
        print(f"Imported files: {imported_files}")
        print(f"Exported: {output_path}")
        return True

    if args.rescore_all:
        total_count = len(load_leads())
        _rescore_database(25)
        output_path = export_latest()
        print(f"Rescored database leads: {total_count}")
        print(f"Exported: {output_path}")
        return True

    if args.archive_old_exports:
        archived_count = archive_old_exports()
        print(f"Archived old exports: {archived_count}")
        return True

    return False


def _rebuild_legacy_import_if_available(lead: dict[str, Any]) -> dict[str, Any]:
    if lead.get("current_run"):
        return lead
    audit = lead.get("website_audit", {}) or {}
    if (
        lead.get("audit_status") == "audited"
        or lead.get("audited_at")
        or audit.get("audit_status") == "audited"
        or audit.get("audited_at")
    ):
        return lead
    legacy_json = lead.get("legacy_import_json")
    if not legacy_json:
        return lead
    try:
        row = json.loads(legacy_json)
    except (TypeError, json.JSONDecodeError):
        return lead
    source_file = Path(str(lead.get("legacy_source_file") or "legacy_import.xlsx"))
    rebuilt = normalize_imported_row(row, source_file)
    for key in ("place_id", "website_domain", "outreach_status"):
        if lead.get(key) and not rebuilt.get(key):
            rebuilt[key] = lead[key]
    return rebuilt


def _rescore_database(final_limit: int) -> int:
    leads = load_leads()
    rescored: list[dict[str, Any]] = []
    for lead in leads:
        rebuilt = _rebuild_legacy_import_if_available(lead)
        # Refresh website_type/candidate_type/business_fit so classification
        # changes (e.g., new third-party directory domains) propagate during
        # rescore-all without requiring re-discovery.
        prefilter_leads([rebuilt], {})
        score_lead(rebuilt)
        rebuilt["score_version"] = SCORING_VERSION
        rescored.append(rebuilt)
    replace_leads(rescored)
    return _refresh_final_review_from_database(final_limit)


def _refresh_final_review_from_database(final_limit: int) -> int:
    leads = load_leads()
    for lead in leads:
        lead["final_review"] = False
    audited = [
        lead
        for lead in leads
        if is_audited(lead)
        and lead.get("final_opportunity_score") is not None
    ]
    top = sorted(
        audited,
        key=lambda lead: lead.get("final_opportunity_score", 0),
        reverse=True,
    )[: max(final_limit, 0)]
    top_ids = {id(lead) for lead in top}
    for lead in leads:
        lead["final_review"] = id(lead) in top_ids
    replace_leads(leads)
    return len(top)


if __name__ == "__main__":
    main()

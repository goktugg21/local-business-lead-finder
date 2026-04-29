from __future__ import annotations

from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl.utils import get_column_letter

from src.scorer import is_audited
from src.utils import ensure_dir


LEAD_COLUMNS = [
    "business_name",
    "sector",
    "city",
    "rating",
    "review_count",
    "website_url",
    "website_type",
    "final_opportunity_score",
    "priority",
    "outreach_decision",
    "pain_gate_pass",
    "pain_gate_reason",
    "business_fit_score",
    "website_pain_score",
    "reachability_score",
    "load_confidence",
    "audit_error_type",
    "http_status_code",
    "audit_error_message",
    "final_url",
    "email_address",
    "google_phone",
    "contact_form_found",
    "cta_found",
    "text_length",
    "pagespeed_performance_score",
    "country",
    "email_found",
    "phone_found",
    "website_email",
    "website_phone",
    "google_maps_url",
    "website_loads",
    "uses_https",
    "meta_description",
    "homepage_title",
    "title",
    "business_fit_status",
    "business_fit_reasons",
    "reject_reason",
    "prefilter_score",
    "prefilter_status",
    "candidate_type",
    "audit_queue",
    "final_review",
    "prefilter_reasons",
    "reason",
    "suggested_outreach_angle",
    "opportunity_score",
    "website_weakness_score",
    "business_value_score",
    "trust_mismatch_score",
    "pagespeed_performance",
    "pagespeed_seo",
    "pagespeed_accessibility",
    "pagespeed_best_practices",
    "lcp",
    "cls",
    "old_website_signals",
    "address",
    "place_id",
    "query",
]

RAW_COLUMNS = [
    "prefilter_score",
    "business_fit_score",
    "business_fit_status",
    "business_fit_reasons",
    "reject_reason",
    "prefilter_status",
    "candidate_type",
    "website_type",
    "audit_queue",
    "prefilter_reasons",
    "business_name",
    "sector",
    "city",
    "country",
    "rating",
    "review_count",
    "website_url",
    "google_phone",
    "google_maps_url",
    "address",
    "place_id",
    "query",
]


def export_leads(
    leads: list[dict[str, Any]],
    output_dir: str | Path = "output",
    total_discovered: int | None = None,
    filename_prefix: str = "leads_output",
    output_path: str | Path | None = None,
) -> Path:
    ensure_dir(output_dir)
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_path = Path(output_dir) / f"{filename_prefix}_{timestamp}.xlsx"
    else:
        output_path = Path(output_path)
        ensure_dir(output_path.parent)

    summary_df = _summary_dataframe(leads, total_discovered=total_discovered)
    raw_df = _raw_dataframe(leads)
    business_fit_df = _lead_dataframe(_business_fit(leads), sort_by="business_fit_score")
    no_website_df = _lead_dataframe(no_website_leads(leads), sort_by="business_fit_score")
    platform_df = _lead_dataframe(platform_leads(leads), sort_by="business_fit_score")
    audit_queue_df = _lead_dataframe(_audit_queue(leads))
    audited_df = _lead_dataframe(_audited(leads), sort_by="final_opportunity_score")
    final_review_df = _lead_dataframe(_final_review(leads), sort_by="final_opportunity_score")
    send_candidates_df = _lead_dataframe(_send_candidates(leads), sort_by="final_opportunity_score")
    manual_review_df = _lead_dataframe(_manual_review(leads), sort_by="final_opportunity_score")
    looks_fine_df = _lead_dataframe(_looks_fine(leads), sort_by="final_opportunity_score")
    needs_browser_check_df = _lead_dataframe(_needs_browser_check(leads), sort_by="final_opportunity_score")
    skip_df = _lead_dataframe(_skip(leads))

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="Summary")
        raw_df.to_excel(writer, index=False, sheet_name="Raw Discovered")
        business_fit_df.to_excel(writer, index=False, sheet_name="Business Fit")
        no_website_df.to_excel(writer, index=False, sheet_name="No Website Leads")
        platform_df.to_excel(writer, index=False, sheet_name="Platform Leads")
        audit_queue_df.to_excel(writer, index=False, sheet_name="Audit Queue")
        audited_df.to_excel(writer, index=False, sheet_name="Audited Websites")
        final_review_df.to_excel(writer, index=False, sheet_name="Final Review")
        send_candidates_df.to_excel(writer, index=False, sheet_name="Send Candidates")
        manual_review_df.to_excel(writer, index=False, sheet_name="Manual Review")
        looks_fine_df.to_excel(writer, index=False, sheet_name="Looks Fine")
        needs_browser_check_df.to_excel(writer, index=False, sheet_name="Needs Browser Check")
        skip_df.to_excel(writer, index=False, sheet_name="Skip")

        for sheet_name in writer.book.sheetnames:
            _format_sheet(writer.book[sheet_name])

    return output_path


def _lead_dataframe(
    leads: list[dict[str, Any]],
    sort_by: str = "opportunity_score",
) -> pd.DataFrame:
    rows = [_flatten_lead(lead) for lead in leads]
    df = pd.DataFrame(rows).reindex(columns=LEAD_COLUMNS)
    if not df.empty:
        primary_sort = sort_by if sort_by in df.columns else "opportunity_score"
        df = df.sort_values(
            by=[primary_sort, "business_fit_score"],
            ascending=[False, False],
            na_position="last",
        )
    return df


def _raw_dataframe(leads: list[dict[str, Any]]) -> pd.DataFrame:
    rows = [_flatten_lead(lead) for lead in leads]
    df = pd.DataFrame(rows).reindex(columns=RAW_COLUMNS)
    if not df.empty:
        df = df.sort_values("business_fit_score", ascending=False, na_position="last")
    return df


def _business_fit(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        lead
        for lead in leads
        if lead.get("business_fit_status", lead.get("prefilter_status")) != "skip"
    ]


def _audit_queue(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [lead for lead in leads if lead.get("audit_queue")]


def audited_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [lead for lead in leads if is_audited(lead)]


def _audited(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return audited_leads(leads)


def _final_review(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = [lead for lead in leads if lead.get("final_review")]
    if selected:
        return selected
    return _audited(leads)


def _send_candidates(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return send_candidate_leads(leads)


def send_candidate_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        lead
        for lead in leads
        if is_audited(lead)
        and lead.get("outreach_decision") == "send_now"
    ]


def _manual_review(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return manual_review_leads(leads)


def manual_review_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        lead
        for lead in leads
        if is_audited(lead)
        and lead.get("outreach_decision") == "manual_review"
    ]


def _looks_fine(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return looks_fine_leads(leads)


def looks_fine_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        lead
        for lead in leads
        if is_audited(lead)
        and lead.get("outreach_decision") == "looks_fine"
    ]


def no_website_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for lead in leads:
        candidate_type = lead.get("candidate_type")
        if candidate_type in {"weak_fit", "skip"}:
            continue
        status = lead.get("business_fit_status", lead.get("prefilter_status"))
        if status == "skip":
            continue
        if candidate_type == "no_website_candidate" or lead.get("website_type") == "missing":
            selected.append(lead)
    return selected


def platform_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for lead in leads:
        candidate_type = lead.get("candidate_type")
        if candidate_type in {"weak_fit", "skip"}:
            continue
        status = lead.get("business_fit_status", lead.get("prefilter_status"))
        if status == "skip":
            continue
        if candidate_type == "platform_candidate" or lead.get("website_type") in {
            "social_media",
            "booking_platform",
        }:
            selected.append(lead)
    return selected


def _needs_browser_check(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return needs_browser_check_leads(leads)


def needs_browser_check_leads(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        lead
        for lead in leads
        if is_audited(lead)
        and (lead.get("website_audit", {}) or {}).get("load_confidence") == "blocked_or_uncertain"
    ]


def _skip(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        lead
        for lead in leads
        if lead.get("business_fit_status", lead.get("prefilter_status")) == "skip"
        or lead.get("candidate_type") in {"skip", "weak_fit"}
        or lead.get("priority") == "D - Skip"
        or lead.get("outreach_decision") == "skip"
    ]


def _flatten_lead(lead: dict[str, Any]) -> dict[str, Any]:
    audit = lead.get("website_audit", {}) or {}
    pagespeed = lead.get("pagespeed", {}) or {}
    return {
        "opportunity_score": lead.get("opportunity_score"),
        "final_opportunity_score": lead.get("final_opportunity_score"),
        "priority": lead.get("priority"),
        "outreach_decision": lead.get("outreach_decision"),
        "pain_gate_pass": lead.get("pain_gate_pass"),
        "pain_gate_reason": lead.get("pain_gate_reason"),
        "business_fit_score": lead.get("business_fit_score"),
        "business_fit_status": lead.get("business_fit_status"),
        "business_fit_reasons": lead.get("business_fit_reasons"),
        "reject_reason": lead.get("reject_reason"),
        "website_pain_score": lead.get("website_pain_score"),
        "prefilter_score": lead.get("prefilter_score"),
        "prefilter_status": lead.get("prefilter_status"),
        "candidate_type": lead.get("candidate_type"),
        "website_type": lead.get("website_type"),
        "audit_queue": lead.get("audit_queue"),
        "final_review": lead.get("final_review"),
        "prefilter_reasons": lead.get("prefilter_reasons"),
        "reason": lead.get("reason"),
        "suggested_outreach_angle": lead.get("suggested_outreach_angle"),
        "business_name": lead.get("business_name"),
        "sector": lead.get("sector"),
        "city": lead.get("city"),
        "country": lead.get("country"),
        "rating": lead.get("rating"),
        "review_count": lead.get("review_count"),
        "website_url": lead.get("website_url"),
        "final_url": audit.get("final_url"),
        "load_confidence": audit.get("load_confidence"),
        "audit_error_type": audit.get("audit_error_type"),
        "http_status_code": audit.get("http_status_code"),
        "audit_error_message": audit.get("audit_error_message"),
        "google_phone": lead.get("google_phone"),
        "email_address": audit.get("email_address"),
        "email_found": audit.get("email_found"),
        "phone_found": audit.get("phone_found"),
        "website_email": audit.get("email_address"),
        "website_phone": audit.get("phone_text"),
        "google_maps_url": lead.get("google_maps_url"),
        "website_weakness_score": lead.get("website_weakness_score"),
        "business_value_score": lead.get("business_value_score"),
        "trust_mismatch_score": lead.get("trust_mismatch_score"),
        "reachability_score": lead.get("reachability_score"),
        "uses_https": audit.get("uses_https"),
        "website_loads": audit.get("website_loads"),
        "contact_form_found": audit.get("contact_form_found"),
        "cta_found": audit.get("cta_found"),
        "meta_description": audit.get("meta_description"),
        "text_length": audit.get("text_length"),
        "pagespeed_performance_score": pagespeed.get("performance_score"),
        "homepage_title": audit.get("homepage_title"),
        "title": audit.get("title"),
        "pagespeed_performance": pagespeed.get("performance_score"),
        "pagespeed_seo": pagespeed.get("seo_score"),
        "pagespeed_accessibility": pagespeed.get("accessibility_score"),
        "pagespeed_best_practices": pagespeed.get("best_practices_score"),
        "lcp": pagespeed.get("lcp"),
        "cls": pagespeed.get("cls"),
        "old_website_signals": _format_signals(audit.get("old_website_signals", [])),
        "address": lead.get("address"),
        "place_id": lead.get("place_id"),
        "query": lead.get("query"),
    }


def _summary_dataframe(
    leads: list[dict[str, Any]],
    total_discovered: int | None = None,
) -> pd.DataFrame:
    business_fit_counts = Counter(
        lead.get("business_fit_status", lead.get("prefilter_status", "unknown"))
        for lead in leads
    )
    priority_counts = Counter(lead.get("priority", "Unaudited") for lead in leads)
    decision_counts = Counter(
        {
            "send_now": len(send_candidate_leads(leads)),
            "manual_review": len(manual_review_leads(leads)),
            "looks_fine": len(looks_fine_leads(leads)),
            "skip": sum(
                1
                for lead in leads
                if not is_audited(lead) or lead.get("outreach_decision") == "skip"
            ),
        }
    )
    rows: list[dict[str, Any]] = [
        {
            "metric": "total_db_leads",
            "value": total_discovered if total_discovered is not None else len(leads),
        },
        {"metric": "current_run_leads", "value": sum(1 for lead in leads if lead.get("current_run"))},
        {"metric": "truly_audited_leads", "value": len(audited_leads(leads))},
        {"metric": "send_candidates", "value": len(send_candidate_leads(leads))},
        {"metric": "audit queue", "value": sum(1 for lead in leads if lead.get("audit_queue"))},
        {"metric": "no_website_leads", "value": len(no_website_leads(leads))},
        {"metric": "platform_leads", "value": len(platform_leads(leads))},
    ]

    rows.extend({"metric": f"business fit: {status}", "value": count} for status, count in business_fit_counts.items())
    rows.extend(
        {"metric": f"priority: {priority}", "value": count}
        for priority, count in priority_counts.items()
    )
    rows.extend(
        {"metric": f"outreach decision: {decision}", "value": count}
        for decision, count in decision_counts.items()
    )
    rows.extend(
        {"metric": f"candidate type: {candidate_type}", "value": count}
        for candidate_type, count in Counter(
            lead.get("candidate_type", "unknown") for lead in leads
        ).items()
    )
    rows.extend(
        {"metric": f"top city: {city}", "value": count}
        for city, count in Counter(lead.get("city", "Unknown") for lead in leads).most_common(10)
    )
    rows.extend(
        {"metric": f"top sector: {sector}", "value": count}
        for sector, count in Counter(lead.get("sector", "Unknown") for lead in leads).most_common(10)
    )
    return pd.DataFrame(rows)


def _format_signals(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value or "")


def _format_sheet(worksheet: Any) -> None:
    worksheet.freeze_panes = "A2"
    for column_cells in worksheet.columns:
        max_length = 0
        column_letter = get_column_letter(column_cells[0].column)
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, min(len(value), 60))
        worksheet.column_dimensions[column_letter].width = max(max_length + 2, 12)

"""Deterministic tests for the exporter's bucket routing and sheet helpers."""
from __future__ import annotations

from src.exporter import (
    _data_quality_review_leads,
    _decision_bucket,
    _visual_review_leads,
)


def _audited_lead(**fields) -> dict:
    base = {
        "business_name": fields.get("business_name", "Sample BV"),
        "city": fields.get("city", "Amsterdam"),
        "current_run": True,
        "data_quality_status": "clean",
        "candidate_type": "redesign_candidate",
        "website_type": "custom_website",
        "business_fit_status": "strong_candidate",
        "audit_queue": True,
        "outreach_decision": "looks_fine",
        "website_audit": {
            "load_confidence": "confirmed_loaded",
            "audit_status": "audited",
            "http_status_code": 200,
            "final_url": "https://example.nl/",
            "website_loads": True,
        },
    }
    base.update(fields)
    return base


def test_directory_lead_routes_to_platform_offer():
    lead = _audited_lead(
        candidate_type="platform_candidate",
        website_type="directory",
        business_fit_status="candidate",
    )
    assert _decision_bucket(lead) == "platform_offer"


def test_no_website_lead_routes_to_no_website_offer():
    lead = _audited_lead(
        candidate_type="no_website_candidate",
        website_type="missing",
        business_fit_status="candidate",
        outreach_decision="no_website_offer",
    )
    assert _decision_bucket(lead) == "no_website_offer"


def test_review_data_quality_routes_to_data_quality_review():
    lead = _audited_lead(data_quality_status="review", outreach_decision="skip")
    assert _decision_bucket(lead) == "data_quality_review"


def test_noise_data_quality_routes_to_data_quality_review():
    lead = _audited_lead(data_quality_status="noise", business_fit_status="skip", outreach_decision="skip")
    assert _decision_bucket(lead) == "data_quality_review"


def test_visual_review_intercepts_looks_fine_with_visual_pain():
    lead = _audited_lead(
        outreach_decision="looks_fine",
        visual_audit={
            "visual_audit_status": "audited",
            "visual_pain_score": 25,
            "visual_pain_reasons": ["mobile horizontal scroll"],
        },
    )
    assert _decision_bucket(lead) == "visual_review"


def test_looks_fine_without_visual_pain_stays_looks_fine():
    lead = _audited_lead(
        outreach_decision="looks_fine",
        visual_audit={
            "visual_audit_status": "audited",
            "visual_pain_score": 0,
            "visual_pain_reasons": [],
        },
    )
    assert _decision_bucket(lead) == "looks_fine"


def test_send_now_with_visual_pain_stays_send_now():
    lead = _audited_lead(
        outreach_decision="send_now",
        visual_audit={
            "visual_audit_status": "audited",
            "visual_pain_score": 30,
        },
    )
    # Send Now retains priority over Visual Review.
    assert _decision_bucket(lead) == "send_now"


def test_visual_review_is_exclusive_of_other_action_buckets():
    """A lead routed to visual_review must not also be classifiable to any
    other action bucket via _decision_bucket. The function returns one bucket
    per lead, so this is a structural guarantee verified by direct equality."""
    lead = _audited_lead(
        outreach_decision="looks_fine",
        visual_audit={
            "visual_audit_status": "audited",
            "visual_pain_score": 30,
        },
    )
    bucket = _decision_bucket(lead)
    assert bucket == "visual_review"
    assert bucket not in {
        "send_now",
        "no_website_offer",
        "platform_offer",
        "manual_review",
        "needs_browser_check",
        "looks_fine",
        "hard_skip",
        "data_quality_review",
    }


def test_data_quality_review_helper_filters_review_and_noise():
    leads = [
        _audited_lead(business_name="A", data_quality_status="clean"),
        _audited_lead(business_name="B", data_quality_status="review"),
        _audited_lead(business_name="C", data_quality_status="noise", business_fit_status="skip"),
        _audited_lead(business_name="D", data_quality_status="clean", current_run=False),
    ]
    selected = _data_quality_review_leads(leads)
    names = {lead["business_name"] for lead in selected}
    assert names == {"B", "C"}


def test_visual_review_helper_filters_only_visual_review_bucket():
    leads = [
        _audited_lead(
            business_name="HasVisualPain",
            outreach_decision="looks_fine",
            visual_audit={
                "visual_audit_status": "audited",
                "visual_pain_score": 25,
            },
        ),
        _audited_lead(
            business_name="LooksFineNoPain",
            outreach_decision="looks_fine",
            visual_audit={
                "visual_audit_status": "audited",
                "visual_pain_score": 0,
            },
        ),
        _audited_lead(
            business_name="SendNowWithVisualPain",
            outreach_decision="send_now",
            visual_audit={
                "visual_audit_status": "audited",
                "visual_pain_score": 40,
            },
        ),
    ]
    selected = _visual_review_leads(leads)
    names = {lead["business_name"] for lead in selected}
    assert names == {"HasVisualPain"}

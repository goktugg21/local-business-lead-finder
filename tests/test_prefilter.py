"""Deterministic tests for website classification and candidate routing."""
from __future__ import annotations

from src.prefilter import classify_website_url, prefilter_leads


def _lead(**fields) -> dict:
    base = {
        "business_name": fields.get("business_name", "Test Company B.V."),
        "city": fields.get("city", "Amsterdam"),
        "sector": fields.get("sector", "klusbedrijf"),
        "rating": fields.get("rating", 4.7),
        "review_count": fields.get("review_count", 50),
        "google_phone": fields.get("google_phone", "020 1234567"),
        "website_url": fields.get("website_url", ""),
    }
    base.update(fields)
    return base


def test_nlcompanies_is_directory():
    assert classify_website_url("https://nlcompanies.org/details/foo") == "directory"


def test_other_third_party_directory_domains():
    for url in (
        "https://cylex.nl/foo",
        "https://drimble.nl/foo",
        "https://oozo.nl/foo",
        "https://bedrijvenpagina.nl/foo",
        "https://allebedrijvenin.nl/foo",
    ):
        assert classify_website_url(url) == "directory", url


def test_marketplace_domains_remain_marketplace():
    for url in (
        "https://werkspot.nl/foo",
        "https://homedeal.nl/foo",
        "https://solvari.nl/foo",
        "https://marktplaats.nl/foo",
    ):
        assert classify_website_url(url) == "marketplace", url


def test_custom_website_classification():
    assert classify_website_url("https://hakbouw.nl/") == "custom_website"
    assert classify_website_url("https://amg-schilders.nl/") == "custom_website"


def test_directory_lead_becomes_platform_candidate():
    lead = _lead(
        business_name="Klusbedrijf Patrick de Vos",
        website_url="https://nlcompanies.org/details/patrick",
    )
    [scored] = prefilter_leads([lead], {})
    assert scored["website_type"] == "directory"
    assert scored["candidate_type"] == "platform_candidate"
    assert scored["business_fit_status"] != "skip"


def test_marketplace_lead_is_hard_skip():
    lead = _lead(
        business_name="Some Werkspot Listing",
        website_url="https://werkspot.nl/listing/foo",
    )
    [scored] = prefilter_leads([lead], {})
    assert scored["website_type"] == "marketplace"
    assert scored["business_fit_status"] == "skip"
    assert "marketplace" in (scored.get("reject_reason") or "").casefold()


def test_normal_custom_website_is_redesign_candidate():
    lead = _lead(
        business_name="HAK Bouwgroep",
        sector="aannemer",
        website_url="https://hakbouw.nl/",
    )
    [scored] = prefilter_leads([lead], {})
    assert scored["website_type"] == "custom_website"
    assert scored["candidate_type"] == "redesign_candidate"
    assert scored["business_fit_status"] in {"strong_candidate", "candidate"}

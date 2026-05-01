"""Deterministic tests for src.data_quality.evaluate_data_quality."""
from __future__ import annotations

from src.data_quality import evaluate_data_quality


def _eval(name: str, city: str = "Haarlem") -> dict:
    return evaluate_data_quality({"business_name": name, "city": city})


def test_accented_latin_stays_clean():
    for name in (
        "Oostbürg Tegelwerk & Afbouw",
        "İstanbul Tadilat",
        "Café Schilders",
    ):
        result = _eval(name)
        assert result["data_quality_status"] == "clean", (
            f"{name!r} should be clean, got {result}"
        )


def test_non_latin_letters_become_review():
    result = _eval("ΔΛΘ ΩΣΧΦΗ B.V.")
    assert result["data_quality_status"] == "review"
    assert "non-Latin" in result["data_quality_reason"]


def test_emoji_symbol_becomes_review():
    result = _eval("Delta Loodgieter Utrecht\U0001F527", city="Utrecht")
    assert result["data_quality_status"] == "review"
    assert "emoji" in result["data_quality_reason"]


def test_pipe_seo_stuffed_becomes_review():
    result = _eval("Loodgieter Haarlem | riool ontstoppen")
    assert result["data_quality_status"] == "review"
    assert "pipe" in result["data_quality_reason"]


def test_business_name_equals_city_is_noise():
    result = _eval("Amsterdam", city="Amsterdam")
    assert result["data_quality_status"] == "noise"
    assert "city" in result["data_quality_reason"].casefold()


def test_normal_brand_name_clean():
    result = _eval("HAK Bouwgroep")
    assert result["data_quality_status"] == "clean"

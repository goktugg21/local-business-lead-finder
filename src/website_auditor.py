from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(
    r"(?:(?:\+|00)\d{1,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?){2,5}\d{2,4}"
)
COPYRIGHT_YEAR_RE = re.compile(
    r"(?:copyright|\u00a9|\(c\)).{0,40}?((?:19|20)\d{2})",
    re.IGNORECASE,
)

CTA_KEYWORDS = [
    "contact",
    "book",
    "quote",
    "call",
    "appointment",
    "get started",
    "offerte",
    "aanvragen",
    "afspraak",
    "bellen",
    "bel ons",
]


class WebsiteAuditor:
    def __init__(self, timeout_seconds: int = 15) -> None:
        self.timeout_seconds = timeout_seconds

    def audit(self, website_url: str | None) -> dict[str, Any]:
        result: dict[str, Any] = {
            "website_exists": bool(website_url),
            "website_loads": None,
            "final_url": "",
            "uses_https": None,
            "audit_error_type": "",
            "audit_error_message": "",
            "http_status_code": None,
            "load_confidence": "unknown",
            "homepage_title": "",
            "title": "",
            "meta_description": "",
            "email_found": False,
            "email_address": "",
            "phone_found": False,
            "phone_text": "",
            "contact_form_found": False,
            "cta_found": False,
            "text_length": 0,
            "visible_text_length": 0,
            "old_copyright_year": None,
            "old_website_signals": [],
            "error": "",
            "audit_status": "audited",
            "audited_at": datetime.now().isoformat(timespec="seconds"),
        }

        if not website_url:
            result["website_loads"] = False
            result["uses_https"] = False
            result["load_confidence"] = "confirmed_dead"
            result["old_website_signals"].append("no website")
            return result

        normalized_url = self._with_scheme(website_url)
        result["uses_https"] = urlparse(normalized_url).scheme == "https"

        try:
            response = requests.get(
                normalized_url,
                timeout=self.timeout_seconds,
                allow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            )
            result["http_status_code"] = response.status_code
            result["final_url"] = response.url
            result["uses_https"] = urlparse(response.url).scheme == "https"
            if 200 <= response.status_code <= 399:
                result["website_loads"] = True
                result["load_confidence"] = "confirmed_loaded"
            elif response.status_code in {404, 410}:
                result["website_loads"] = False
                result["load_confidence"] = "confirmed_dead"
                result["audit_error_type"] = f"http_{response.status_code}"
                result["audit_error_message"] = f"HTTP status {response.status_code}"
                result["old_website_signals"].append("website does not load")
                return result
            elif response.status_code in {401, 403, 406, 408, 429, 500, 502, 503, 504}:
                result["website_loads"] = None
                result["load_confidence"] = "blocked_or_uncertain"
                result["audit_error_type"] = f"http_{response.status_code}"
                result["audit_error_message"] = f"HTTP status {response.status_code}"
                self._clear_uncertain_observations(result)
                return result
            else:
                result["website_loads"] = None
                result["load_confidence"] = "blocked_or_uncertain"
                result["audit_error_type"] = f"http_{response.status_code}"
                result["audit_error_message"] = f"Unexpected HTTP status {response.status_code}"
                self._clear_uncertain_observations(result)
                return result
        except requests.exceptions.Timeout as exc:
            return self._uncertain_failure(result, "timeout", exc)
        except requests.exceptions.SSLError as exc:
            return self._uncertain_failure(result, "ssl_error", exc)
        except requests.exceptions.ConnectionError as exc:
            return self._uncertain_failure(result, "connection_error", exc)
        except requests.exceptions.TooManyRedirects as exc:
            return self._uncertain_failure(result, "too_many_redirects", exc)
        except requests.exceptions.RequestException as exc:
            return self._uncertain_failure(result, "request_exception", exc)

        soup = BeautifulSoup(response.text, "html.parser")
        result["homepage_title"] = self._extract_title(soup)
        result["title"] = result["homepage_title"]
        result["meta_description"] = self._extract_meta_description(soup)
        visible_text = self._visible_text(soup)
        html_text = soup.get_text(" ", strip=True)
        result["text_length"] = len(visible_text)
        result["visible_text_length"] = len(visible_text)

        email_match = EMAIL_RE.search(response.text)
        phone_match = PHONE_RE.search(html_text)
        result["email_found"] = email_match is not None
        result["email_address"] = email_match.group(0) if email_match else ""
        result["phone_found"] = phone_match is not None
        result["phone_text"] = phone_match.group(0) if phone_match else ""
        result["contact_form_found"] = soup.find("form") is not None
        result["cta_found"] = self._has_cta(visible_text)

        copyright_year = self._old_copyright_year(visible_text)
        result["old_copyright_year"] = copyright_year

        result["old_website_signals"] = self._old_website_signals(result)
        return result

    def _uncertain_failure(
        self,
        result: dict[str, Any],
        error_type: str,
        exc: Exception,
    ) -> dict[str, Any]:
        result["website_loads"] = None
        result["load_confidence"] = "blocked_or_uncertain"
        result["audit_error_type"] = error_type
        result["audit_error_message"] = str(exc)
        result["error"] = str(exc)
        self._clear_uncertain_observations(result)
        return result

    def _clear_uncertain_observations(self, result: dict[str, Any]) -> None:
        result["email_found"] = None
        result["phone_found"] = None
        result["contact_form_found"] = None
        result["cta_found"] = None
        result["text_length"] = None
        result["visible_text_length"] = None

    def _with_scheme(self, url: str) -> str:
        parsed = urlparse(url)
        if parsed.scheme:
            return url
        return f"https://{url}"

    def _extract_title(self, soup: BeautifulSoup) -> str:
        if soup.title and soup.title.string:
            return soup.title.string.strip()
        return ""

    def _extract_meta_description(self, soup: BeautifulSoup) -> str:
        tag = soup.find("meta", attrs={"name": re.compile("^description$", re.IGNORECASE)})
        if tag and tag.get("content"):
            return str(tag["content"]).strip()
        return ""

    def _visible_text(self, soup: BeautifulSoup) -> str:
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        return soup.get_text(" ", strip=True)

    def _has_cta(self, text: str) -> bool:
        lowered = text.casefold()
        return any(keyword in lowered for keyword in CTA_KEYWORDS)

    def _old_copyright_year(self, text: str) -> int | None:
        current_year = datetime.now().year
        years = [int(match.group(1)) for match in COPYRIGHT_YEAR_RE.finditer(text)]
        old_years = [year for year in years if year < current_year - 4]
        return min(old_years) if old_years else None

    def _old_website_signals(self, audit: dict[str, Any]) -> list[str]:
        signals: list[str] = []
        if audit.get("old_copyright_year"):
            signals.append(f"old copyright year {audit['old_copyright_year']}")
        if not audit.get("meta_description"):
            signals.append("missing meta description")
        if not audit.get("cta_found"):
            signals.append("no clear CTA")
        if not audit.get("contact_form_found"):
            signals.append("no contact form")
        if not audit.get("email_found") and not audit.get("phone_found"):
            signals.append("no email or phone found on website")
        if audit.get("uses_https") is False:
            signals.append("http only")
        return signals

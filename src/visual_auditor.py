from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from src.utils import cache_key, normalize_url_for_key


SCREENSHOT_DIR = Path("cache/visual_audits/screenshots")

DESKTOP_VIEWPORT = {"width": 1366, "height": 768}
MOBILE_VIEWPORT = {"width": 390, "height": 844}

DESKTOP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
MOBILE_USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1"
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


class BrowserVisualAuditor:
    def __init__(
        self,
        timeout_ms: int = 20_000,
        screenshot_dir: Path | None = None,
    ) -> None:
        self.timeout_ms = timeout_ms
        self.screenshot_dir = Path(screenshot_dir) if screenshot_dir else SCREENSHOT_DIR
        self._playwright_available = self._detect_playwright()

    @staticmethod
    def _detect_playwright() -> bool:
        try:
            import playwright.sync_api  # noqa: F401
            return True
        except ImportError:
            return False

    @property
    def playwright_available(self) -> bool:
        return self._playwright_available

    def audit(self, lead: dict[str, Any], audit: dict[str, Any]) -> dict[str, Any]:
        result = self._empty_result()

        if not self._playwright_available:
            result["visual_audit_status"] = "skipped"
            result["browser_error_type"] = "playwright_not_installed"
            result["browser_error_message"] = (
                "playwright is not installed. Install with `pip install playwright` "
                "and `playwright install chromium`."
            )
            result["visual_pain_reasons"] = ["visual audit skipped: playwright_not_installed"]
            return result

        url = (audit.get("final_url") or lead.get("website_url") or "").strip()
        if not url:
            result["visual_audit_status"] = "skipped"
            result["browser_error_type"] = "no_url"
            result["visual_pain_reasons"] = ["visual audit skipped: no_url"]
            return result

        screenshot_key = self._screenshot_key(lead, url)
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        desktop_path = self.screenshot_dir / f"{screenshot_key}_desktop.png"
        mobile_path = self.screenshot_dir / f"{screenshot_key}_mobile.png"

        try:
            from playwright.sync_api import sync_playwright
            from playwright.sync_api import TimeoutError as PWTimeoutError
        except ImportError:
            result["visual_audit_status"] = "skipped"
            result["browser_error_type"] = "playwright_not_installed"
            result["visual_pain_reasons"] = ["visual audit skipped: playwright_not_installed"]
            return result

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                try:
                    desktop_data = self._capture_desktop(browser, url, desktop_path)
                    mobile_data = self._capture_mobile(browser, url, mobile_path)
                finally:
                    browser.close()

            result["visual_audit_status"] = "audited"
            result["browser_loads"] = True
            result["desktop_screenshot_path"] = str(desktop_path)
            result["mobile_screenshot_path"] = str(mobile_path)
            result["above_fold_text_length"] = int(mobile_data.get("above_fold_text_length", 0))
            result["above_fold_cta_visible"] = bool(
                mobile_data.get("cta_visible") or desktop_data.get("cta_visible")
            )
            result["above_fold_phone_visible"] = bool(
                mobile_data.get("phone_visible") or desktop_data.get("phone_visible")
            )
            result["above_fold_form_visible"] = bool(
                mobile_data.get("form_visible") or desktop_data.get("form_visible")
            )
            result["mobile_has_horizontal_scroll"] = bool(mobile_data.get("horizontal_scroll"))

            score, reasons = self._compute_visual_pain(result)
            result["visual_pain_score"] = score
            result["visual_pain_reasons"] = reasons
        except PWTimeoutError as exc:
            result["visual_audit_status"] = "failed"
            result["browser_loads"] = False
            result["browser_error_type"] = "browser_timeout"
            result["browser_error_message"] = str(exc)
            result["visual_pain_reasons"] = ["visual audit failed: browser_timeout"]
        except Exception as exc:
            result["visual_audit_status"] = "failed"
            result["browser_loads"] = False
            result["browser_error_type"] = "browser_error"
            result["browser_error_message"] = str(exc)
            result["visual_pain_reasons"] = ["visual audit failed: browser_error"]

        result["visual_audited_at"] = datetime.now().isoformat(timespec="seconds")
        return result

    @staticmethod
    def _empty_result() -> dict[str, Any]:
        return {
            "visual_audit_status": "skipped",
            "browser_loads": None,
            "browser_error_type": "",
            "browser_error_message": "",
            "desktop_screenshot_path": "",
            "mobile_screenshot_path": "",
            "mobile_has_horizontal_scroll": None,
            "above_fold_text_length": None,
            "above_fold_cta_visible": None,
            "above_fold_phone_visible": None,
            "above_fold_form_visible": None,
            "visual_pain_score": None,
            "visual_pain_reasons": [],
            "visual_audited_at": datetime.now().isoformat(timespec="seconds"),
        }

    def _screenshot_key(self, lead: dict[str, Any], url: str) -> str:
        place_id = str(lead.get("place_id") or "").strip()
        if place_id:
            return f"place_{cache_key(place_id)}"
        return f"url_{cache_key(normalize_url_for_key(url))}"

    def _capture_desktop(self, browser: Any, url: str, path: Path) -> dict[str, Any]:
        context = browser.new_context(
            viewport=DESKTOP_VIEWPORT,
            user_agent=DESKTOP_USER_AGENT,
        )
        page = context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            page.wait_for_timeout(1500)
            page.screenshot(path=str(path), full_page=False)
            return self._extract_above_fold(page, DESKTOP_VIEWPORT["height"])
        finally:
            context.close()

    def _capture_mobile(self, browser: Any, url: str, path: Path) -> dict[str, Any]:
        context = browser.new_context(
            viewport=MOBILE_VIEWPORT,
            user_agent=MOBILE_USER_AGENT,
            device_scale_factor=2,
            is_mobile=True,
            has_touch=True,
        )
        page = context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
            page.wait_for_timeout(1500)
            page.screenshot(path=str(path), full_page=False)
            data = self._extract_above_fold(page, MOBILE_VIEWPORT["height"])
            data["horizontal_scroll"] = self._check_horizontal_scroll(page)
            return data
        finally:
            context.close()

    def _extract_above_fold(self, page: Any, fold_height: int) -> dict[str, Any]:
        try:
            text = page.evaluate(
                """
                (fold) => {
                    let text = '';
                    const walk = document.createTreeWalker(
                        document.body, NodeFilter.SHOW_TEXT, null
                    );
                    let node;
                    while ((node = walk.nextNode())) {
                        const parent = node.parentElement;
                        if (!parent) continue;
                        const rect = parent.getBoundingClientRect();
                        if (rect.top < fold && rect.top + rect.height > 0) {
                            const t = (node.textContent || '').trim();
                            if (t) text += t + ' ';
                        }
                    }
                    return text.trim();
                }
                """,
                fold_height,
            )
        except Exception:
            text = ""

        text = str(text or "")
        lower = text.casefold()
        cta_visible = any(keyword in lower for keyword in CTA_KEYWORDS)

        try:
            phone_visible = page.evaluate(
                """
                (fold) => {
                    let found = false;
                    document.querySelectorAll('a[href^="tel:"]').forEach(el => {
                        const r = el.getBoundingClientRect();
                        if (r.top < fold && r.top + r.height > 0) found = true;
                    });
                    return found;
                }
                """,
                fold_height,
            )
        except Exception:
            phone_visible = False

        try:
            form_visible = page.evaluate(
                """
                (fold) => {
                    let found = false;
                    const sel = 'form, input[type="email"], input[type="tel"], textarea, button[type="submit"]';
                    document.querySelectorAll(sel).forEach(el => {
                        const r = el.getBoundingClientRect();
                        if (r.top < fold && r.top + r.height > 0) found = true;
                    });
                    return found;
                }
                """,
                fold_height,
            )
        except Exception:
            form_visible = False

        return {
            "above_fold_text_length": len(text),
            "cta_visible": cta_visible,
            "phone_visible": bool(phone_visible),
            "form_visible": bool(form_visible),
        }

    def _check_horizontal_scroll(self, page: Any) -> bool:
        try:
            return bool(
                page.evaluate(
                    "() => document.documentElement.scrollWidth > "
                    "document.documentElement.clientWidth + 4"
                )
            )
        except Exception:
            return False

    def _compute_visual_pain(self, result: dict[str, Any]) -> tuple[int, list[str]]:
        score = 0
        reasons: list[str] = []

        if result.get("mobile_has_horizontal_scroll"):
            score += 25
            reasons.append("mobile has horizontal scroll")

        if result.get("above_fold_cta_visible") is False:
            score += 15
            reasons.append("no CTA above the fold")

        if (
            result.get("above_fold_phone_visible") is False
            and result.get("above_fold_form_visible") is False
        ):
            score += 15
            reasons.append("no phone or form above the fold")

        text_len = int(result.get("above_fold_text_length") or 0)
        if text_len < 80:
            score += 15
            reasons.append("very little text above the fold")

        if score > 100:
            score = 100
        return score, reasons

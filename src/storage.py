from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.exporter import export_leads
from src.scorer import SCORING_VERSION


DB_PATH = Path("data/leads.sqlite")
LATEST_EXPORT_PATH = Path("output/latest.xlsx")


def init_db(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leads (
                lead_key TEXT PRIMARY KEY,
                place_id TEXT,
                website_domain TEXT,
                business_name TEXT,
                city TEXT,
                sector TEXT,
                data_json TEXT NOT NULL,
                scoring_version TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS imported_files (
                file_hash TEXT PRIMARY KEY,
                source_file TEXT NOT NULL,
                imported_at TEXT NOT NULL
            )
            """
        )


def upsert_leads(leads: list[dict[str, Any]], db_path: Path = DB_PATH) -> int:
    init_db(db_path)
    count = 0
    now = _now()
    with sqlite3.connect(db_path) as conn:
        for lead in leads:
            key = lead_key(lead)
            if not key:
                continue
            existing = conn.execute(
                "SELECT data_json, created_at FROM leads WHERE lead_key = ?",
                (key,),
            ).fetchone()
            data = lead
            created_at = now
            if existing:
                previous = json.loads(existing[0])
                data = _merge_lead(previous, lead)
                created_at = existing[1]
            data["website_domain"] = normalize_domain(data.get("website_url"))
            conn.execute(
                """
                INSERT OR REPLACE INTO leads (
                    lead_key, place_id, website_domain, business_name, city, sector,
                    data_json, scoring_version, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    data.get("place_id") or "",
                    data.get("website_domain") or "",
                    data.get("business_name") or "",
                    data.get("city") or "",
                    data.get("sector") or "",
                    json.dumps(data, ensure_ascii=False),
                    data.get("score_version") or data.get("scoring_version") or SCORING_VERSION,
                    created_at,
                    now,
                ),
            )
            count += 1
    return count


def load_leads(db_path: Path = DB_PATH) -> list[dict[str, Any]]:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT data_json FROM leads").fetchall()
    return [json.loads(row[0]) for row in rows]


def replace_leads(leads: list[dict[str, Any]], db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM leads")
    upsert_leads(leads, db_path=db_path)


def clear_current_run_flags(db_path: Path = DB_PATH) -> None:
    leads = load_leads(db_path)
    if not leads:
        return
    for lead in leads:
        lead["current_run"] = False
        lead["audit_queue"] = False
        lead["final_review"] = False
    replace_leads(leads, db_path=db_path)


def export_latest(db_path: Path = DB_PATH) -> Path:
    leads = load_leads(db_path)
    return export_leads(leads, output_path=LATEST_EXPORT_PATH)


def archive_old_exports() -> int:
    archive_dir = Path("output/archive")
    archive_dir.mkdir(parents=True, exist_ok=True)
    archived = 0
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for path in Path("output").glob("*.xlsx"):
        if path.name == LATEST_EXPORT_PATH.name:
            continue
        target = archive_dir / f"{path.stem}_{timestamp}{path.suffix}"
        shutil.move(str(path), str(target))
        archived += 1
    return archived


def imported_file_exists(file_hash: str, db_path: Path = DB_PATH) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM imported_files WHERE file_hash = ?",
            (file_hash,),
        ).fetchone()
    return row is not None


def mark_imported_file(file_hash: str, source_file: str, db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO imported_files (file_hash, source_file, imported_at)
            VALUES (?, ?, ?)
            """,
            (file_hash, source_file, _now()),
        )


def lead_key(lead: dict[str, Any]) -> str:
    if lead.get("place_id"):
        return f"place:{lead['place_id']}"
    domain = lead.get("website_domain") or normalize_domain(lead.get("website_url"))
    if domain:
        return f"domain:{domain}"
    name = str(lead.get("business_name") or "").casefold().strip()
    address = str(lead.get("address") or "").casefold().strip()
    if name or address:
        return f"name_address:{name}:{address}"
    return ""


def normalize_domain(website_url: Any) -> str:
    value = str(website_url or "").strip()
    if not value or value.casefold() in {"-", "unknown", "nan", "none", "null"}:
        return ""
    parsed = urlparse(value if "://" in value else f"https://{value}")
    host = parsed.netloc.casefold()
    if host.startswith("www."):
        host = host[4:]
    return host.rstrip("/")


def _merge_lead(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    merged = previous.copy()
    for key, value in current.items():
        if value not in (None, "", [], {}):
            merged[key] = value
    return merged


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")

"""Verify output/latest.xlsx workbook consistency with the SQLite DB.

Run this after a pipeline run to sanity-check the export. Exits non-zero on
any failure so it can be wired into CI later.

Checks:
- Sheet order matches the expected list.
- Actionable sheets (Audited This Run, Send Now, ...) contain only data_quality_status == "clean".
- Data Quality Review contains only review/noise.
- Audited This Run rows == DB rows where audit_queue=True AND data_quality_status="clean".
- Visual-audited current-run leads are a subset of audit_queue (i.e. of Audited This Run).
- Hard Skip and Data Quality Review have no A/B priority rows.
- No Website Offer rows do not show outreach_decision == "skip".
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

import pandas as pd


EXPECTED_SHEET_ORDER = [
    "README",
    "Current Run Summary",
    "Audited This Run",
    "Send Now",
    "No Website Offer",
    "Platform Website Offer",
    "Data Quality Review",
    "Manual Review",
    "Needs Browser Check",
    "Visual Review",
    "Looks Fine",
    "Hard Skip",
    "Current Run - Raw",
    "Current Run - Candidates",
    "All Database",
]

ACTIONABLE_SHEETS = (
    "Audited This Run",
    "Send Now",
    "No Website Offer",
    "Platform Website Offer",
    "Manual Review",
    "Needs Browser Check",
    "Visual Review",
    "Looks Fine",
)

A_B_PRIORITIES = {"A - Write First", "B - Good Lead"}


FAILURE_INDICATORS = (
    "Traceback",
    "UnicodeEncodeError",
    "NativeCommandError",
    "error: unrecognized arguments",
    "main.py: error:",
    "Exited with",
    "exit=1",
)

THIRD_PARTY_DIRECTORY_DOMAINS = (
    "nlcompanies.org",
    "telefoonboek.nl",
    "openingstijden.nl",
    "cylex.nl",
    "drimble.nl",
    "oozo.nl",
    "bedrijvenpagina.nl",
    "allebedrijvenin.nl",
    "solvari.nl",
    "werkspot.nl",
    "homedeal.nl",
)


def _check(condition: bool, ok_msg: str, fail_msg: str) -> int:
    if condition:
        print(f"  OK    {ok_msg}")
        return 0
    print(f"  FAIL  {fail_msg}")
    return 1


def _read_text_flexible(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16-le", errors="replace").lstrip("﻿").replace("\r\n", "\n")
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be", errors="replace").lstrip("﻿").replace("\r\n", "\n")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig", errors="replace").replace("\r\n", "\n")
    for encoding in ("utf-8", "cp1254", "cp1252", "latin-1"):
        try:
            text = raw.decode(encoding)
            return text.replace("\r\n", "\n")
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace").replace("\r\n", "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify output/latest.xlsx consistency.")
    parser.add_argument(
        "--run-log",
        type=Path,
        default=None,
        help="Pipeline run log (e.g. utrecht_run.log). If provided, check that every '[x/N] Auditing ...' name is in Audited This Run.",
    )
    parser.add_argument(
        "--expected-city",
        default=None,
        help="If provided, Current Run - Raw and (non-empty) Audited This Run city columns must contain only this city.",
    )
    args = parser.parse_args()

    workbook = Path("output/latest.xlsx")
    db = Path("data/leads.sqlite")
    if not workbook.exists():
        print(f"Workbook not found: {workbook}")
        return 2
    if not db.exists():
        print(f"DB not found: {db}")
        return 2

    failures = 0
    xl = pd.ExcelFile(workbook)

    print("Sheet order:")
    failures += _check(
        xl.sheet_names == EXPECTED_SHEET_ORDER,
        "matches expected order",
        f"order mismatch:\n    expected={EXPECTED_SHEET_ORDER}\n    actual={xl.sheet_names}",
    )

    print()
    print("Actionable sheets are clean-only:")
    for sheet in ACTIONABLE_SHEETS:
        df = pd.read_excel(xl, sheet_name=sheet)
        if df.empty:
            print(f"  ----  {sheet}: empty")
            continue
        non_clean = df[df["data_quality_status"] != "clean"]
        failures += _check(
            non_clean.empty,
            f"{sheet} all clean ({len(df)} rows)",
            f"{sheet} has {len(non_clean)} non-clean rows",
        )

    print()
    print("Data Quality Review only review/noise:")
    dqr = pd.read_excel(xl, sheet_name="Data Quality Review")
    if dqr.empty:
        print("  ----  empty")
    else:
        invalid = dqr[~dqr["data_quality_status"].isin(["review", "noise"])]
        failures += _check(
            invalid.empty,
            f"only review/noise ({len(dqr)} rows)",
            f"{len(invalid)} rows are not review/noise",
        )

    print()
    print("Sheet priority constraints:")
    for sheet, banned, label in (
        ("Hard Skip", A_B_PRIORITIES, "no A/B priority"),
        ("Data Quality Review", A_B_PRIORITIES, "no A/B priority"),
        ("Send Now", {"D - Skip", "Q - Data Quality Review"}, "no D/Q priority"),
        ("Manual Review", {"Q - Data Quality Review"}, "no Q priority"),
    ):
        df = pd.read_excel(xl, sheet_name=sheet)
        if df.empty:
            print(f"  ----  {sheet}: empty")
            continue
        bad = df[df["priority"].isin(banned)]
        failures += _check(
            bad.empty,
            f"{sheet} {label} ({len(df)} rows)",
            f"{sheet} has {len(bad)} rows with banned priority {sorted(banned)}",
        )

    print()
    print("Send Now third-party-directory check:")

    def _scan_third_party(frame: pd.DataFrame) -> list[tuple[str, int]]:
        urls = frame["website_url"].astype(str).str.casefold()
        offenders: list[tuple[str, int]] = []
        for domain in THIRD_PARTY_DIRECTORY_DOMAINS:
            mask = urls.str.contains(domain, regex=False)
            if mask.any():
                offenders.append((domain, int(mask.sum())))
        return offenders

    send_now_df = pd.read_excel(xl, sheet_name="Send Now")
    if send_now_df.empty:
        print("  ----  Send Now sheet: empty")
    else:
        offenders = _scan_third_party(send_now_df)
        failures += _check(
            not offenders,
            f"no third-party directory domains in Send Now sheet ({len(send_now_df)} rows)",
            f"Send Now sheet contains third-party directory domains: {offenders}",
        )

    all_db_df = pd.read_excel(xl, sheet_name="All Database")
    if all_db_df.empty:
        print("  ----  All Database: empty")
    else:
        send_now_mask = (
            (all_db_df["outreach_decision"].astype(str) == "send_now")
            | (all_db_df["decision_bucket"].astype(str) == "send_now")
        )
        send_now_db = all_db_df[send_now_mask]
        if send_now_db.empty:
            print("  ----  All Database send_now: empty")
        else:
            db_offenders = _scan_third_party(send_now_db)
            failures += _check(
                not db_offenders,
                f"no third-party directory domains in DB send_now rows ({len(send_now_db)} rows)",
                f"DB send_now rows contain third-party directory domains: {db_offenders}",
            )

    print()
    print("All Database decision_bucket <-> outreach_decision consistency:")
    all_db = pd.read_excel(xl, sheet_name="All Database")
    bucket_to_decision = {
        "no_website_offer": "no_website_offer",
        "platform_offer": "platform_website_offer",
        "send_now": "send_now",
    }
    for bucket, expected_decision in bucket_to_decision.items():
        subset = all_db[all_db["decision_bucket"] == bucket]
        if subset.empty:
            print(f"  SKIP  bucket={bucket}: no rows in All Database")
            continue
        mismatched = subset[subset["outreach_decision"] != expected_decision]
        failures += _check(
            mismatched.empty,
            f"bucket {bucket!r} -> outreach_decision {expected_decision!r} ({len(subset)} rows)",
            f"bucket {bucket!r}: {len(mismatched)} rows have outreach_decision != {expected_decision!r}",
        )

    print()
    print("DB consistency:")
    conn = sqlite3.connect(db)
    rows = conn.execute("SELECT data_json FROM leads").fetchall()
    conn.close()
    leads = [json.loads(row[0]) for row in rows]

    audit_queue_clean = {
        lead.get("business_name")
        for lead in leads
        if lead.get("audit_queue") and lead.get("data_quality_status") == "clean"
    }
    ath = pd.read_excel(xl, sheet_name="Audited This Run")
    ath_names = set(ath["business_name"].astype(str)) if not ath.empty else set()
    failures += _check(
        ath_names == audit_queue_clean,
        f"Audited This Run == audit_queue=True clean ({len(ath_names)} rows)",
        f"sheet has {len(ath_names)} leads, DB audit_queue+clean has {len(audit_queue_clean)}",
    )

    # The structural invariant ("visual audit only consumes audit_candidates,
    # so this-run visual audits are always in audit_queue") is enforced by
    # main._select_visual_candidates. We can only verify it from DB state with
    # a known run boundary, which the --run-log option provides below. Without
    # a log we'd false-positive on stale visual audits from earlier sessions
    # that linger on rediscovered current_run leads.

    if args.run_log is not None:
        print()
        print(f"Run log audit-attempt check ({args.run_log}):")
        if not args.run_log.exists():
            print(f"  FAIL  log not found: {args.run_log}")
            failures += 1
        else:
            log_text = _read_text_flexible(args.run_log)

            crash_indicators = [ind for ind in FAILURE_INDICATORS if ind in log_text]
            failures += _check(
                not crash_indicators,
                "log contains no crash/error indicators",
                f"log contains failure indicator(s): {crash_indicators}",
            )

            failures += _check(
                "Done." in log_text,
                "log contains 'Done.'",
                "log is missing 'Done.' marker (pipeline did not finish)",
            )

            only_match = re.search(r"^Auditing only (\d+) leads\.\s*$", log_text, re.MULTILINE)
            actually_match = re.search(r"^Actually audited:\s*(\d+)\s*$", log_text, re.MULTILINE)
            attempts = re.findall(r"^\[\d+/\d+\] Auditing (.+)$", log_text, re.MULTILINE)
            visual_attempts = re.findall(r"^\s*\[visual \d+/\d+\] (.+)$", log_text, re.MULTILINE)

            n_only = int(only_match.group(1)) if only_match else None
            n_actually = int(actually_match.group(1)) if actually_match else None

            failures += _check(
                n_only is not None,
                f"parsed 'Auditing only N leads.' -> N={n_only}",
                "log is missing 'Auditing only N leads.' line",
            )
            failures += _check(
                n_actually is not None,
                f"parsed 'Actually audited: M' -> M={n_actually}",
                "log is missing 'Actually audited: M' line",
            )

            if n_only is not None:
                failures += _check(
                    len(attempts) == n_only,
                    f"parsed [x/N] Auditing lines == N ({len(attempts)} == {n_only})",
                    f"parsed {len(attempts)} [x/N] Auditing lines but log says N={n_only}",
                )

            if n_only is not None and n_actually is not None:
                failures += _check(
                    n_only == n_actually,
                    f"'Auditing only' N == 'Actually audited' M ({n_only})",
                    f"'Auditing only' N={n_only} != 'Actually audited' M={n_actually}",
                )

            ath_df = pd.read_excel(xl, sheet_name="Audited This Run")
            ath_names = set(ath_df["business_name"].astype(str)) if not ath_df.empty else set()

            if n_only is not None:
                failures += _check(
                    len(ath_df) == n_only,
                    f"Audited This Run sheet rows == N ({len(ath_df)} == {n_only})",
                    f"Audited This Run sheet has {len(ath_df)} rows but log says N={n_only}",
                )

            missing = [name for name in attempts if name not in ath_names]
            failures += _check(
                not missing,
                f"all {len(attempts)} attempted names in Audited This Run",
                f"{len(missing)} attempted name(s) missing from Audited This Run",
            )
            visual_missing = [name for name in visual_attempts if name not in ath_names]
            failures += _check(
                not visual_missing,
                f"all {len(visual_attempts)} this-run visual-audit names in Audited This Run",
                f"{len(visual_missing)} visual-audit name(s) missing from Audited This Run",
            )
            if missing:
                print("  Missing names (with DB diagnostic):")
                conn_log = sqlite3.connect(db)
                rows_log = conn_log.execute("SELECT data_json FROM leads").fetchall()
                conn_log.close()
                by_name = {}
                for row in rows_log:
                    lead = json.loads(row[0])
                    by_name.setdefault(lead.get("business_name") or "", []).append(lead)
                for name in missing:
                    matches = by_name.get(name, [])
                    if not matches:
                        print(f"    {name}: NOT FOUND IN DB")
                        continue
                    lead = matches[0]
                    audit = lead.get("website_audit") or {}
                    print(
                        f"    {name}: "
                        f"audit_queue={lead.get('audit_queue')}  "
                        f"data_quality={lead.get('data_quality_status')}  "
                        f"load_conf={audit.get('load_confidence')}  "
                        f"audit_status={audit.get('audit_status')}"
                    )

    if args.expected_city is not None:
        print()
        print(f"Expected-city check ({args.expected_city!r}):")
        cr_raw = pd.read_excel(xl, sheet_name="Current Run - Raw")
        if cr_raw.empty:
            print("  ----  Current Run - Raw: empty")
        else:
            other = cr_raw[cr_raw["city"].astype(str) != args.expected_city]
            failures += _check(
                other.empty,
                f"Current Run - Raw all city == {args.expected_city!r} ({len(cr_raw)} rows)",
                f"Current Run - Raw has {len(other)} rows with city != {args.expected_city!r}: {sorted(set(other['city'].astype(str)))}",
            )
        ath_check = pd.read_excel(xl, sheet_name="Audited This Run")
        if ath_check.empty:
            print("  ----  Audited This Run: empty (skipping city check)")
        else:
            other_ath = ath_check[ath_check["city"].astype(str) != args.expected_city]
            failures += _check(
                other_ath.empty,
                f"Audited This Run all city == {args.expected_city!r} ({len(ath_check)} rows)",
                f"Audited This Run has {len(other_ath)} rows with city != {args.expected_city!r}",
            )

    print()
    if failures == 0:
        print("OK: all checks passed.")
        return 0
    print(f"FAIL: {failures} check(s) failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())

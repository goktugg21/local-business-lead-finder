# Local Business Lead Finder — PowerShell User Guide

This file is a practical operating guide so you can use the system without opening an LLM every time.

## 1) The most important logic

### What is `output\latest.xlsx`?

`latest.xlsx` is the **latest result file**. It is rewritten after every run.

That means:

- If you run Amsterdam, `latest.xlsx` becomes Amsterdam.
- If you then run Eindhoven, the same `latest.xlsx` becomes Eindhoven.
- The old Amsterdam result does **not** stay inside `latest.xlsx`.

So, after every successful run, copy the file if you want to keep it for sales:

```powershell
$stamp = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item output\latest.xlsx "output\sales_amsterdam_$stamp.xlsx"
```

### What is `data\leads.sqlite`?

This is the main database. All leads discovered from all cities accumulate here.

Important:

- Leads from older city runs stay in this database.
- `latest.xlsx` is only the Excel export from the most recent run.
- The `All Database` sheet can show data from the full database.
- `Current Run - Raw` and the sales/action sheets are filtered for the last city run.

### Main sheets to use for sales

Use these sheets first, in this order:

1. `Send Now`
2. `No Website Offer`
3. `Platform Website Offer`
4. `Manual Review`
5. `Visual Review`

Other sheets are for support, checking, or debugging:

- `Audited This Run`: leads audited in this run.
- `Looks Fine`: low sales priority for now.
- `Hard Skip`: do not use for sales.
- `Data Quality Review`: spam/suspicious/data-quality check.
- `Current Run - Raw`: raw discovery results.
- `Current Run - Candidates`: leads that passed business-fit filtering.
- `All Database`: full DB view.

---

## 2) Standard city run command

For a one-word city:

```powershell
python main.py --mode pipeline --preset home_services --city Amsterdam --audit-limit 10 --final-limit 10 --visual-audit --visual-limit 5 > amsterdam_run.log 2>&1
```

For a city with spaces, always use quotes:

```powershell
python main.py --mode pipeline --preset home_services --city "Den Haag" --audit-limit 10 --final-limit 10 --visual-audit --visual-limit 5 > denhaag_run.log 2>&1
```

Examples:

```powershell
python main.py --mode pipeline --preset home_services --city Rotterdam --audit-limit 10 --final-limit 10 --visual-audit --visual-limit 5 > rotterdam_run.log 2>&1
python main.py --mode pipeline --preset home_services --city Utrecht --audit-limit 10 --final-limit 10 --visual-audit --visual-limit 5 > utrecht_run.log 2>&1
python main.py --mode pipeline --preset home_services --city Eindhoven --audit-limit 10 --final-limit 10 --visual-audit --visual-limit 5 > eindhoven_run.log 2>&1
python main.py --mode pipeline --preset home_services --city "Den Bosch" --audit-limit 10 --final-limit 10 --visual-audit --visual-limit 5 > denbosch_run.log 2>&1
```

---

## 3) Always verify after each run

After every run:

```powershell
python scripts/verify_workbook.py --run-log amsterdam_run.log --expected-city "Amsterdam"
```

City examples:

```powershell
python scripts/verify_workbook.py --run-log rotterdam_run.log --expected-city "Rotterdam"
python scripts/verify_workbook.py --run-log denhaag_run.log --expected-city "Den Haag"
python scripts/verify_workbook.py --run-log eindhoven_run.log --expected-city "Eindhoven"
```

At the bottom, a successful result must say:

```text
OK: all checks passed.
```

If it does not say this, do not start sales outreach yet. Check the error first.

---

## 4) Save the successful result as a sales file

If verification passed:

```powershell
$stamp = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item output\latest.xlsx "output\sales_amsterdam_$stamp.xlsx"
```

Filename examples by city:

```powershell
$stamp = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item output\latest.xlsx "output\sales_rotterdam_$stamp.xlsx"

$stamp = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item output\latest.xlsx "output\sales_den_haag_$stamp.xlsx"

$stamp = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item output\latest.xlsx "output\sales_eindhoven_$stamp.xlsx"
```

Optional zip copy:

```powershell
Compress-Archive -Force "output\sales_amsterdam_$stamp.xlsx" "sales_amsterdam_$stamp.zip"
```

---

## 5) Ideal daily workflow

### Step 1 — Run a city

```powershell
python main.py --mode pipeline --preset home_services --city Amsterdam --audit-limit 10 --final-limit 10 --visual-audit --visual-limit 5 > amsterdam_run.log 2>&1
```

### Step 2 — Check the end of the log

```powershell
Get-Content amsterdam_run.log -Tail 100
```

You want to see:

```text
Done.
Exported: output\latest.xlsx
```

### Step 3 — Verify the workbook

```powershell
python scripts/verify_workbook.py --run-log amsterdam_run.log --expected-city "Amsterdam"
```

### Step 4 — Save a sales copy

```powershell
$stamp = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item output\latest.xlsx "output\sales_amsterdam_$stamp.xlsx"
```

### Step 5 — Open the sales sheets in Excel

Use this order:

1. `Send Now`
2. `No Website Offer`
3. `Platform Website Offer`
4. `Manual Review`
5. `Visual Review`

---

## 6) Sector-specific runs

The `home_services` preset includes sectors like:

- aannemer
- klusbedrijf
- loodgieter
- schilder
- dakdekker
- stukadoor
- tegelzetter
- schoonmaakbedrijf
- verhuisbedrijf
- badkamer renovatie
- keuken renovatie
- renovatiebedrijf
- timmerman
- glazenwasser

To run one specific sector:

```powershell
python main.py --mode pipeline --preset home_services --city Amsterdam --sector loodgieter --audit-limit 10 --final-limit 10 --visual-audit --visual-limit 5 > amsterdam_loodgieter_run.log 2>&1
```

Then verify:

```powershell
python scripts/verify_workbook.py --run-log amsterdam_loodgieter_run.log --expected-city "Amsterdam"
```

Then save a sales copy:

```powershell
$stamp = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item output\latest.xlsx "output\sales_amsterdam_loodgieter_$stamp.xlsx"
```

---

## 7) What does audit limit mean?

```powershell
--audit-limit 10
```

This means the system will website-audit up to 10 leads in that run.

Recommended values:

- Safe testing: `--audit-limit 10`
- More leads: `--audit-limit 25`
- Larger city scan: `--audit-limit 50`

Example:

```powershell
python main.py --mode pipeline --preset home_services --city Amsterdam --audit-limit 25 --final-limit 25 --visual-audit --visual-limit 10 > amsterdam_run.log 2>&1
```

---

## 8) What is visual audit?

```powershell
--visual-audit --visual-limit 5
```

This checks some custom websites visually with Playwright/headless browser.

- `visual-limit 5`: visual-audit up to 5 sites.
- Visual-audit results can appear in the `Visual Review` sheet.
- `Visual Review` is now exclusive and should not overlap with other action sheets.

---

## 9) When should you use PageSpeed?

The default command does not use PageSpeed. This is good for now because PageSpeed:

- is slower,
- can use API quota,
- is not required for the first sales list.

If you want to use it:

```powershell
python main.py --mode pipeline --preset home_services --city Amsterdam --audit-limit 10 --final-limit 10 --visual-audit --visual-limit 5 --pagespeed > amsterdam_run.log 2>&1
```

---

## 10) When should you run `--rescore-all`?

Use it when the code logic changed.

Examples:

- directory domain list changed,
- data-quality rules changed,
- priority/outreach logic changed.

Command:

```powershell
python main.py --rescore-all
```

You do not need to run it before every normal daily sales run. It is mainly useful after code changes.

---

## 11) Keep the output folder simple

`output\latest.xlsx` is created automatically and is the latest run.

Files you should keep for sales:

- `output\sales_city_timestamp.xlsx`

Before deleting old test files, list them first:

```powershell
Get-ChildItem output -Filter "latest_*.xlsx"
```

If you are sure, delete old `latest_*` test files:

```powershell
Remove-Item output\latest_* -Force
```

Log files and zip files are ignored by Git through `.gitignore`.

---

## 12) Common PowerShell mistake: city quotes

Wrong:

```powershell
python main.py --mode pipeline --preset home_services --city Den Haag --audit-limit 10
```

This fails because PowerShell sees `Den` and `Haag` as two separate arguments.

Correct:

```powershell
python main.py --mode pipeline --preset home_services --city "Den Haag" --audit-limit 10
```

---

## 13) Bash heredoc does not work in PowerShell

This Bash-style command does not work in PowerShell:

```powershell
python - <<'PY'
```

Use this PowerShell format instead:

```powershell
@'
import pandas as pd
xl = pd.ExcelFile("output/latest.xlsx")
print(xl.sheet_names)
'@ | python -
```

---

## 14) Quick Excel sheet row count

```powershell
@'
import pandas as pd

xl = pd.ExcelFile("output/latest.xlsx")
for s in xl.sheet_names:
    df = pd.read_excel(xl, sheet_name=s)
    print(f"{s}: {len(df)}")
'@ | python -
```

---

## 15) Quick sales sheet preview

```powershell
@'
import pandas as pd

xl = pd.ExcelFile("output/latest.xlsx")
for s in ["Send Now", "No Website Offer", "Platform Website Offer", "Manual Review", "Visual Review"]:
    df = pd.read_excel(xl, sheet_name=s)
    print()
    print(s, "rows:", len(df))
    if "business_name" in df.columns and len(df):
        print(df[["business_name", "city", "sector", "priority", "outreach_decision"]].head(20).to_string(index=False))
'@ | python -
```

---

## 16) Amsterdam starter command set

```powershell
python main.py --mode pipeline --preset home_services --city Amsterdam --audit-limit 10 --final-limit 10 --visual-audit --visual-limit 5 > amsterdam_run.log 2>&1

Get-Content amsterdam_run.log -Tail 100

python scripts/verify_workbook.py --run-log amsterdam_run.log --expected-city "Amsterdam"

$stamp = Get-Date -Format "yyyyMMdd_HHmm"
Copy-Item output\latest.xlsx "output\sales_amsterdam_$stamp.xlsx"
```

---

## 17) What to check after a successful Amsterdam result

In Excel, start with:

1. `Send Now`
2. `No Website Offer`
3. `Platform Website Offer`
4. `Manual Review`
5. `Visual Review`

In your last Amsterdam run, the example state was:

- `Send Now`: 0
- `No Website Offer`: 1
- `Platform Website Offer`: 1
- `Manual Review`: 4
- `Visual Review`: 7
- `Looks Fine`: 36
- `Data Quality Review`: 18
- `Current Run - Raw`: 149

So the first Amsterdam sales actions are:

- the 1 lead in `No Website Offer`,
- the 1 lead in `Platform Website Offer`,
- the 4 leads in `Manual Review`,
- the 7 leads in `Visual Review`.

---

## 18) If a run fails

Check the log:

```powershell
Get-Content city_run.log -Tail 120
```

If verification fails, pay attention to these lines:

- `log is missing 'Done.' marker`
- `log contains failure indicator`
- `Current Run - Raw has rows with city != ...`
- `Audited This Run sheet rows == N`

After a failed run, `latest.xlsx` may still be from the previous successful city. Do not copy it for sales unless verification passes.

---

## 19) Standard pre-commit check

If you changed code:

```powershell
python -m compileall .
python scripts/verify_workbook.py --run-log amsterdam_run.log --expected-city "Amsterdam"
git status -s
```

If everything is correct:

```powershell
git add .
git commit -m "your message"
git push
```

For a normal sales run, you do not need to commit anything.

---

## 20) Short decision summary

- `latest.xlsx` = latest run, temporary.
- `sales_city_timestamp.xlsx` = file to keep and use for sales.
- Do not start sales before verification passes.
- Put quotes around city names with spaces.
- Sales order: `Send Now`, `No Website Offer`, `Platform Website Offer`, `Manual Review`, `Visual Review`.
- Do not use `Hard Skip` or `Data Quality Review` for sales.

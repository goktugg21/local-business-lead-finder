# Local Business Lead Finder

MVP command-line lead discovery tool for finding local businesses that may need website redesigns.

It discovers businesses through the Google Places API Text Search New endpoint, lightly audits their websites, optionally runs PageSpeed Insights, scores each lead, and exports an Excel workbook sorted by opportunity.

## 1. Create your `.env`

Copy the example file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Edit `.env` and add your Google API key:

```env
GOOGLE_API_KEY=your_google_api_key_here
```

Never commit `.env`.

## 2. Install dependencies

```bash
pip install -r requirements.txt
```

The optional `--visual-audit` flag drives a Playwright headless-browser audit. Playwright is in `requirements.txt`, but `pip` does not install the Chromium binary. After the pip install, run this once:

```bash
python -m playwright install chromium
```

If Playwright or Chromium is missing, `--visual-audit` will gracefully mark each visual audit as `skipped` with `browser_error_type=playwright_not_installed` and the rest of the pipeline still runs.

## 3. Run

```bash
python main.py
```

Useful staged runs:

```bash
python main.py --mode pipeline
python main.py --mode pipeline --preset home_services --city Amsterdam --audit-limit 80 --final-limit 20
python main.py --mode pipeline --preset home_services --city Amsterdam --audit-limit 80 --final-limit 20 --pagespeed
python main.py --mode pipeline --preset home_services --cities Amsterdam,Rotterdam,Utrecht,Den Haag,Haarlem,Leiden --audit-limit 150 --final-limit 30
python main.py --mode pipeline --preset beauty --cities Amsterdam,Rotterdam,Utrecht --audit-limit 100 --final-limit 25
python main.py --mode pipeline --preset home_services --cities Amsterdam,Rotterdam,Utrecht --audit-limit 100 --final-limit 25 --pagespeed
python main.py --mode pipeline --pagespeed
python main.py --mode discover
python main.py --mode fast --limit 80
python main.py --mode full --limit 50 --pagespeed
```

Modes:

- `discover`: Google Places discovery, prefiltering, and Excel export only. No website audit or PageSpeed.
- `fast`: discovery, prefiltering, and website audit for top candidates. PageSpeed stays off unless `--pagespeed` is passed.
- `full`: discovery, prefiltering, website audit, and PageSpeed for the top `pagespeed_top_n` audited candidates.
- `pipeline`: end-to-end staged run: discovery, business-fit filter, audit queue, website audit, optional PageSpeed for top N, final scoring, and Excel export.

CLI overrides:

- `--city Amsterdam`: run one city instead of the configured city list.
- `--cities Amsterdam,Rotterdam,Utrecht`: run a comma-separated city list.
- `--preset home_services`: use a configured sector preset. Available presets include `home_services` and `beauty`.
- `--sector klusbedrijf,schilder`: add one or more comma-separated sectors. If combined with `--preset`, sectors are merged.
- `--max-audit-queue 80`: cap the future audit queue size.
- `--audit-limit 80`: cap how many queued leads are audited in this run.
- `--final-limit 25`: cap the final review shortlist.
- `--limit 50`: cap the current audit run size.
- `--pagespeed`: force PageSpeed for selected audited candidates.
- `--reaudit`: re-audit leads that are already audited or marked needs_browser_check.
- `--audit-global-backlog`: also consider historical-DB leads (not just current_run discoveries) that match the current city/sector scope. Default behavior audits only the current run's discoveries.
- `--visual-audit`: run a Playwright headless-browser audit on top current-run custom websites where the HTTP audit confirmed loaded. Requires Playwright + Chromium (see step 2).
- `--visual-limit 10`: cap the number of visual audits per run (default 10).

Maintenance commands:

- `python main.py --import-history`: import historical Excel exports from `output/`, export `output/latest.xlsx`, then exit.
- `python main.py --import-history path/to/file.xlsx`: import one workbook, export `output/latest.xlsx`, then exit.
- `python main.py --rescore-all`: rescore the local database, export `output/latest.xlsx`, then exit.
- `python main.py --archive-old-exports`: move old Excel exports into `output/archive/`, then exit.

Maintenance commands do not call Google Places, PageSpeed, or website audit.

The business-fit filter is intentionally selective. It classifies leads as `strong_candidate`, `candidate`, `weak`, or `skip`, and assigns a `candidate_type` such as `redesign_candidate`, `no_website_candidate`, or `platform_candidate`. Website pain is scored only after the website audit runs.

Excel exports include `Summary`, `Raw Discovered`, `Business Fit`, `Audit Queue`, `Audited Websites`, `Final Review`, `Send Candidates`, `Manual Review`, `Looks Fine`, and `Skip`.

`Final Review` is a scored shortlist for inspection. Only `Send Candidates` should be treated as outreach-ready.

The Excel export will be written to `output/leads_output_YYYYMMDD_HHMM.xlsx`. Discover-only exports use `output/raw_discovery_YYYYMMDD_HHMM.xlsx`.

## 4. Edit `config.yaml`

Use `config.yaml` to control countries, cities, sectors, result limits, prefilter thresholds, candidate audit queue size, PageSpeed behavior, and request delay.

Important prefilter knobs:

- `max_audit_queue`
- `max_business_candidates`
- `final_top_n`
- `strong_threshold`
- `candidate_threshold`
- `min_rating`
- `preferred_min_reviews`
- `preferred_max_reviews`
- `max_reviews_soft`
- `max_reviews_hard`

Start small while testing, for example one city and one sector, then expand once the workflow looks good.

## 5. API cost warning

Google Places API and PageSpeed Insights API usage may cost money or count against quotas. Start with small city/sector lists, enable billing alerts, and set quotas in Google Cloud.

## 6. Cache behavior

Places responses are cached by query in `cache/places/`.

Website audit responses are cached by place ID and normalized website URL in `cache/audits/`.

PageSpeed responses are cached by URL, strategy, and categories in `cache/pagespeed/`.

Delete cached files if you want to force fresh API calls.

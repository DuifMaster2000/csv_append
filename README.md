# CSV Appender

A small, browser-based tool for stitching multiple [TradingView](https://www.tradingview.com/)
historical CSV exports into **one continuous CSV file**.

TradingView only lets you pull a limited window of history at a time. This tool lets you
upload several overlapping exports and combines them into a single, de-duplicated,
chronologically-ordered file — and flags any suspicious gaps so you know whether your
combined series is truly continuous.

Everything runs **entirely in your browser**. Files are never uploaded anywhere.

## Features

- **Merge many files** — drag in as many exports as you like.
- **Automatic de-duplication** — overlapping rows (same timestamp) are collapsed to one.
- **Smart gap detection** — the largest gap *inside* your individual exports is treated
  as your normal weekend/holiday break. Any gap in the merged series bigger than that is
  flagged as a possible missing-data gap.
- **Overlap checking** — warns when two consecutive exports don't overlap, since overlap
  is what lets the tool verify continuity instead of guessing.
- **Conflict detection** — if the same timestamp appears in two files with different
  values (e.g. a revised candle), it's flagged and the first occurrence is kept.
- **Memory-friendly** — files are read with the streaming Web Streams API and stored
  compactly, so exports with tens of thousands of rows each merge without trouble.

## Why overlap matters

Rather than hard-coding what a "normal" gap is (weekends and holidays vary by market),
the tool learns it from your data: within a single continuous export, the biggest gap is
your longest normal break. So for the most reliable results, **make sure consecutive
exports overlap by at least a few bars.** The tool will warn you when they don't.

## Expected format

The standard TradingView export, e.g.:

```
time,open,high,low,close,SMA #1,SMA #2,SMA #3,Volume
2025-01-02T01:00:00+02:00,21094.2,21153.8,21087.8,21131.4,,,,2734
...
```

The first column is detected as the time column (`time`/`date`/`datetime`/`timestamp`,
or column 1 by default). ISO-8601 timestamps with timezone offsets, `YYYY-MM-DD HH:MM:SS`,
and unix seconds/millis are all understood. The original row text is preserved exactly in
the output.

## Usage

1. Open the site.
2. Drop in your CSV exports.
3. Click **Merge & Check**.
4. Review the summary and any warnings.
5. Click **Download combined CSV**.

## Development / deployment

It's a static site (`index.html`, `styles.css`, `app.js`) — no build step. To run locally:

```bash
python3 -m http.server 8000
# then open http://localhost:8000
```

Deployment to GitHub Pages is automated via `.github/workflows/deploy.yml` on every push
to the deploy branch.

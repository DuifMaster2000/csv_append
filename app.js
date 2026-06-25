"use strict";

/*
 * CSV Appender — merges multiple TradingView CSV exports into one continuous file.
 *
 * Design notes:
 *  - Files are read with the streaming Web Streams API (file.stream()), decoded and
 *    split into lines incrementally so we never hold the whole file text in memory
 *    twice. For each data row we keep only two things: a numeric timestamp and the
 *    original line string. Storing the raw line (instead of a parsed object per
 *    column) keeps per-row overhead low and preserves the exact original formatting.
 *  - Merging sorts by timestamp, removes duplicate timestamps (overlap between
 *    exports), and scans for gaps that are larger than any gap seen *inside* the
 *    individual exports — those are the suspicious ones worth flagging.
 */

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/** @type {Array<{id:number,file:File}>} */
let queue = [];
let nextId = 1;
let lastResult = null; // { headerLine, lines, tsList } for download

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

const $ = (sel) => document.querySelector(sel);

const dropZone       = $("#drop-zone");
const fileInput      = $("#file-input");
const fileListSection= $("#file-list-section");
const fileListEl     = $("#file-list");
const fileCountEl     = $("#file-count");
const processBtn     = $("#process-btn");
const clearBtn       = $("#clear-btn");
const progressSection= $("#progress-section");
const progressFill   = $("#progress-fill");
const progressText   = $("#progress-text");
const resultsSection = $("#results-section");
const resultsSummary = $("#results-summary");
const resultsWarnings= $("#results-warnings");
const downloadBtn    = $("#download-btn");

// ---------------------------------------------------------------------------
// File intake
// ---------------------------------------------------------------------------

dropZone.addEventListener("click", () => fileInput.click());
dropZone.addEventListener("keydown", (e) => {
  if (e.key === "Enter" || e.key === " ") { e.preventDefault(); fileInput.click(); }
});

["dragenter", "dragover"].forEach((evt) =>
  dropZone.addEventListener(evt, (e) => {
    e.preventDefault();
    dropZone.classList.add("dragover");
  })
);
["dragleave", "drop"].forEach((evt) =>
  dropZone.addEventListener(evt, (e) => {
    e.preventDefault();
    dropZone.classList.remove("dragover");
  })
);
dropZone.addEventListener("drop", (e) => addFiles(e.dataTransfer.files));
fileInput.addEventListener("change", () => {
  addFiles(fileInput.files);
  fileInput.value = ""; // allow re-selecting the same file
});

function addFiles(fileList) {
  for (const file of fileList) {
    if (!/\.csv$/i.test(file.name) && file.type !== "text/csv") continue;
    // Skip exact duplicates already in the queue (same name + size).
    if (queue.some((q) => q.file.name === file.name && q.file.size === file.size)) continue;
    queue.push({ id: nextId++, file });
  }
  renderQueue();
}

function removeFile(id) {
  queue = queue.filter((q) => q.id !== id);
  renderQueue();
}

clearBtn.addEventListener("click", () => {
  queue = [];
  renderQueue();
  resultsSection.classList.add("hidden");
});

function renderQueue() {
  if (queue.length === 0) {
    fileListSection.classList.add("hidden");
    return;
  }
  fileListSection.classList.remove("hidden");
  fileCountEl.textContent = queue.length;
  fileListEl.innerHTML = "";
  for (const { id, file } of queue) {
    const li = document.createElement("li");
    const info = document.createElement("div");
    info.className = "grow";
    const name = document.createElement("div");
    name.className = "fname";
    name.textContent = file.name;
    const meta = document.createElement("div");
    meta.className = "fmeta";
    meta.textContent = formatBytes(file.size);
    info.append(name, meta);

    const remove = document.createElement("button");
    remove.className = "remove";
    remove.title = "Remove";
    remove.textContent = "×";
    remove.addEventListener("click", () => removeFile(id));

    li.append(info, remove);
    fileListEl.append(li);
  }
}

// ---------------------------------------------------------------------------
// Streaming line reader
// ---------------------------------------------------------------------------

/**
 * Yields the file's text content one line at a time without buffering the
 * whole file. Handles both \n and \r\n line endings.
 */
async function* readLines(file) {
  const reader = file.stream().pipeThrough(new TextDecoderStream()).getReader();
  let buf = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += value;
      let idx;
      while ((idx = buf.indexOf("\n")) >= 0) {
        let line = buf.slice(0, idx);
        if (line.endsWith("\r")) line = line.slice(0, -1);
        yield line;
        buf = buf.slice(idx + 1);
      }
    }
    if (buf.length) {
      if (buf.endsWith("\r")) buf = buf.slice(0, -1);
      if (buf.length) yield buf;
    }
  } finally {
    reader.releaseLock();
  }
}

// ---------------------------------------------------------------------------
// Parsing a single file
// ---------------------------------------------------------------------------

/**
 * Parses one CSV export.
 * Returns { name, header, timeIndex, tsList, lines, badRows, internalMaxGap,
 *           minInterval, start, end }.
 * tsList[i] corresponds to lines[i].
 */
async function parseFile(file, onProgress) {
  let header = null;
  let timeIndex = 0;
  const tsList = [];
  const lines = [];
  let badRows = 0;
  let prevTs = -Infinity;
  let internalMaxGap = 0;
  let minInterval = Infinity;
  let read = 0;

  for await (const rawLine of readLines(file)) {
    read += rawLine.length + 1;
    if (rawLine.trim() === "") continue;

    if (header === null) {
      header = rawLine;
      timeIndex = detectTimeColumn(header);
      continue;
    }

    const ts = parseTimestamp(rawLine, timeIndex);
    if (ts === null) { badRows++; continue; }

    // Track natural gaps / bar interval within this single (trusted) export.
    if (prevTs !== -Infinity) {
      const delta = ts - prevTs;
      if (delta > 0) {
        if (delta < minInterval) minInterval = delta;
        if (delta > internalMaxGap) internalMaxGap = delta;
      }
    }
    prevTs = ts;

    tsList.push(ts);
    lines.push(rawLine);

    if ((tsList.length & 8191) === 0 && onProgress) onProgress(read);
  }

  if (header === null) throw new Error(`"${file.name}" appears to be empty.`);
  if (tsList.length === 0) throw new Error(`"${file.name}" has a header but no valid data rows.`);

  // Each export from TradingView is chronological, but sort defensively so the
  // start/end and internal-gap stats are correct even if rows are out of order.
  ensureSorted(tsList, lines);

  return {
    name: file.name,
    header,
    timeIndex,
    tsList,
    lines,
    badRows,
    internalMaxGap,
    minInterval: minInterval === Infinity ? 0 : minInterval,
    start: tsList[0],
    end: tsList[tsList.length - 1],
  };
}

function detectTimeColumn(header) {
  const cols = header.split(",").map((c) => c.trim().toLowerCase());
  const idx = cols.findIndex((c) => c === "time" || c === "date" || c === "datetime" || c === "timestamp");
  return idx === -1 ? 0 : idx;
}

/** Extracts the time field from a raw line and converts it to epoch ms. */
function parseTimestamp(line, timeIndex) {
  let field;
  if (timeIndex === 0) {
    const c = line.indexOf(",");
    field = c === -1 ? line : line.slice(0, c);
  } else {
    field = line.split(",")[timeIndex];
  }
  if (field == null) return null;
  field = field.trim();
  // ISO-8601 (with offset) parses directly. Also accept "YYYY-MM-DD HH:MM:SS"
  // and plain unix seconds/millis.
  let ms = Date.parse(field);
  if (Number.isNaN(ms)) {
    if (/^\d+$/.test(field)) {
      const n = Number(field);
      ms = field.length > 11 ? n : n * 1000; // 13-digit ms vs 10-digit s
    } else if (field.includes(" ")) {
      ms = Date.parse(field.replace(" ", "T"));
    }
  }
  return Number.isNaN(ms) ? null : ms;
}

/** In-place stable-ish sort of parallel arrays by timestamp (only if needed). */
function ensureSorted(tsList, lines) {
  let sorted = true;
  for (let i = 1; i < tsList.length; i++) {
    if (tsList[i] < tsList[i - 1]) { sorted = false; break; }
  }
  if (sorted) return;
  const idx = tsList.map((_, i) => i);
  idx.sort((a, b) => tsList[a] - tsList[b] || a - b);
  const ts2 = new Array(tsList.length);
  const ln2 = new Array(lines.length);
  for (let i = 0; i < idx.length; i++) { ts2[i] = tsList[idx[i]]; ln2[i] = lines[idx[i]]; }
  for (let i = 0; i < idx.length; i++) { tsList[i] = ts2[i]; lines[i] = ln2[i]; }
}

// ---------------------------------------------------------------------------
// Merge + analysis
// ---------------------------------------------------------------------------

processBtn.addEventListener("click", run);

async function run() {
  if (queue.length === 0) return;
  setBusy(true);
  resultsSection.classList.add("hidden");
  progressSection.classList.remove("hidden");

  try {
    const parsed = [];
    const totalBytes = queue.reduce((s, q) => s + q.file.size, 0);
    let doneBytes = 0;

    for (const { file } of queue) {
      setProgress(doneBytes / totalBytes, `Reading ${file.name}…`);
      const p = await parseFile(file, (read) => {
        setProgress((doneBytes + read) / totalBytes, `Reading ${file.name}…`);
      });
      doneBytes += file.size;
      parsed.push(p);
    }

    setProgress(1, "Merging…");
    await tick();

    const result = mergeAndAnalyze(parsed);
    lastResult = result;
    renderResults(result, parsed);
  } catch (err) {
    progressSection.classList.add("hidden");
    showError(err.message || String(err));
  } finally {
    setBusy(false);
    progressSection.classList.add("hidden");
  }
}

/**
 * Merges parsed files: validates headers, sorts globally, de-duplicates
 * overlapping timestamps, and detects gaps + overlaps.
 */
function mergeAndAnalyze(parsed) {
  const warnings = [];

  // 1) Header consistency.
  const canonicalHeader = parsed[0].header;
  const headerMismatches = parsed
    .filter((p) => normalizeHeader(p.header) !== normalizeHeader(canonicalHeader))
    .map((p) => p.name);

  // 2) Gap threshold: the largest gap observed *inside* any single export is
  //    by definition a "natural" gap (weekend / holiday) because each export is
  //    pulled continuously. Anything larger in the merged series is suspicious.
  let gapThreshold = 0;
  let minInterval = Infinity;
  for (const p of parsed) {
    if (p.internalMaxGap > gapThreshold) gapThreshold = p.internalMaxGap;
    if (p.minInterval > 0 && p.minInterval < minInterval) minInterval = p.minInterval;
  }
  if (minInterval === Infinity) minInterval = 0;

  // 3) Build global sorted index across all files (k-way via flat sort).
  //    We keep references (fileIdx,rowIdx) instead of copying lines.
  const refs = [];
  for (let f = 0; f < parsed.length; f++) {
    const ts = parsed[f].tsList;
    for (let r = 0; r < ts.length; r++) refs.push([ts[r], f, r]);
  }
  refs.sort((a, b) => a[0] - b[0] || a[1] - b[1] || a[2] - b[2]);

  // 4) De-duplicate identical timestamps; detect conflicting duplicates.
  const outLines = [];
  const outTs = [];
  let duplicates = 0;
  let conflicts = 0;
  const conflictSamples = [];
  let prevTs = null;
  let prevLine = null;
  for (const [ts, f, r] of refs) {
    const line = parsed[f].lines[r];
    if (ts === prevTs) {
      duplicates++;
      if (line !== prevLine) {
        conflicts++;
        if (conflictSamples.length < 5) {
          conflictSamples.push({ ts, a: prevLine, b: line });
        }
      }
      continue; // keep the first occurrence
    }
    outLines.push(line);
    outTs.push(ts);
    prevTs = ts;
    prevLine = line;
  }

  // 5) Scan merged series for suspicious gaps.
  const gaps = [];
  if (gapThreshold > 0) {
    // tolerance so an exactly-equal gap isn't flagged
    const limit = gapThreshold + Math.max(1, minInterval) * 0.5;
    for (let i = 1; i < outTs.length; i++) {
      const delta = outTs[i] - outTs[i - 1];
      if (delta > limit) {
        gaps.push({ from: outTs[i - 1], to: outTs[i], delta });
      }
    }
  }

  // 6) Overlap report between chronologically adjacent files.
  const order = parsed.map((_, i) => i).sort((a, b) => parsed[a].start - parsed[b].start);
  const adjacency = [];
  for (let i = 1; i < order.length; i++) {
    const prev = parsed[order[i - 1]];
    const cur = parsed[order[i]];
    if (cur.start <= prev.end) {
      adjacency.push({ a: prev.name, b: cur.name, type: "overlap", amount: prev.end - cur.start });
    } else {
      adjacency.push({ a: prev.name, b: cur.name, type: "gap", amount: cur.start - prev.end });
    }
  }

  return {
    headerLine: canonicalHeader,
    lines: outLines,
    tsList: outTs,
    inputRows: refs.length,
    outputRows: outLines.length,
    duplicates,
    conflicts,
    conflictSamples,
    badRows: parsed.reduce((s, p) => s + p.badRows, 0),
    gapThreshold,
    minInterval,
    gaps,
    adjacency,
    headerMismatches,
    fileOrder: order.map((i) => parsed[i].name),
    warnings,
  };
}

function normalizeHeader(h) {
  return h.split(",").map((c) => c.trim().toLowerCase()).join(",");
}

// ---------------------------------------------------------------------------
// Rendering results
// ---------------------------------------------------------------------------

function renderResults(res, parsed) {
  resultsSection.classList.remove("hidden");

  // Summary card.
  const span = res.tsList.length
    ? `${fmtDate(res.tsList[0])} → ${fmtDate(res.tsList[res.tsList.length - 1])}`
    : "—";
  resultsSummary.innerHTML = `
    <div class="card">
      <div class="stat-grid">
        ${stat(res.outputRows.toLocaleString(), "rows in output")}
        ${stat(res.duplicates.toLocaleString(), "overlapping rows removed")}
        ${stat(parsed.length.toLocaleString(), "files merged")}
        ${stat(res.minInterval ? humanDur(res.minInterval) : "—", "detected bar interval")}
      </div>
      <p class="fmeta" style="margin:.9rem 0 0;color:var(--muted)">
        Continuous range: <span class="mono">${span}</span><br/>
        Chronological order: ${res.fileOrder.map((n) => `<span class="mono">${escapeHtml(n)}</span>`).join(" → ")}
      </p>
    </div>`;

  // Warnings.
  const blocks = [];

  if (res.headerMismatches.length) {
    blocks.push(alertBlock("bad", "⚠", "Column headers don't match", `
      These files have a different header than the first file, so columns may not line up:
      <ul>${res.headerMismatches.map((n) => `<li class="mono">${escapeHtml(n)}</li>`).join("")}</ul>
      The merged file uses the first file's header. Double-check before using.`));
  }

  if (res.gaps.length) {
    const items = res.gaps.slice(0, 50).map((g) =>
      `<li><span class="mono">${fmtDate(g.from)}</span> &rarr; <span class="mono">${fmtDate(g.to)}</span>
       &nbsp;(<strong>${humanDur(g.delta)}</strong> gap)</li>`).join("");
    const more = res.gaps.length > 50 ? `<li>… and ${res.gaps.length - 50} more</li>` : "";
    blocks.push(alertBlock("warn", "⚠", `${res.gaps.length} suspicious data gap${res.gaps.length > 1 ? "s" : ""} found`, `
      These gaps are larger than the biggest gap seen inside your individual exports
      (the threshold is <strong>${humanDur(res.gapThreshold)}</strong>, i.e. your longest
      normal weekend/holiday break). They may mean missing data between downloads:
      <ul>${items}${more}</ul>`));
  }

  if (res.conflicts) {
    const items = res.conflictSamples.map((c) =>
      `<li><span class="mono">${fmtDate(c.ts)}</span><br/>
        kept: <span class="mono">${escapeHtml(trimLine(c.a))}</span><br/>
        dropped: <span class="mono">${escapeHtml(trimLine(c.b))}</span></li>`).join("");
    blocks.push(alertBlock("warn", "⚠", `${res.conflicts.toLocaleString()} overlapping rows had conflicting values`, `
      The same timestamp appeared in more than one file with <em>different</em> values
      (e.g. revised candles). The first occurrence was kept. Examples:
      <ul>${items}</ul>`));
  }

  if (res.badRows) {
    blocks.push(alertBlock("warn", "⚠", `${res.badRows.toLocaleString()} rows skipped`,
      "Some rows had an unparseable timestamp and were left out of the merge."));
  }

  // Overlap confirmation (informational, helps the "ensure overlap" workflow).
  const overlaps = res.adjacency.filter((a) => a.type === "overlap");
  const junctionGaps = res.adjacency.filter((a) => a.type === "gap");
  if (junctionGaps.length) {
    const items = junctionGaps.map((j) =>
      `<li><span class="mono">${escapeHtml(j.a)}</span> and <span class="mono">${escapeHtml(j.b)}</span>
       don't overlap (${humanDur(j.amount)} between them)</li>`).join("");
    blocks.push(alertBlock("warn", "⚠", "Some files don't overlap", `
      For the safest merge, consecutive exports should overlap a little so gaps can be
      verified rather than guessed. These neighbours have no overlap:
      <ul>${items}</ul>`));
  }

  if (!res.gaps.length && !res.headerMismatches.length && !res.conflicts && !junctionGaps.length) {
    const msg = overlaps.length
      ? `Files overlap and stitch together cleanly with no gaps beyond your normal weekend/holiday breaks.`
      : `No gaps beyond your normal weekend/holiday breaks were detected.`;
    blocks.push(alertBlock("good", "✓", "Looks continuous", msg));
  }

  resultsWarnings.innerHTML = blocks.join("");
  downloadBtn.disabled = res.outputRows === 0;
}

function stat(num, label) {
  return `<div class="stat"><div class="num">${num}</div><div class="label">${label}</div></div>`;
}
function alertBlock(kind, icon, title, bodyHtml) {
  return `<div class="alert ${kind}"><h3><span class="icon">${icon}</span>${title}</h3><div>${bodyHtml}</div></div>`;
}
function showError(msg) {
  resultsSection.classList.remove("hidden");
  resultsSummary.innerHTML = "";
  resultsWarnings.innerHTML = alertBlock("bad", "⚠", "Couldn't merge", escapeHtml(msg));
  downloadBtn.disabled = true;
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

downloadBtn.addEventListener("click", () => {
  if (!lastResult) return;
  // Assemble the output. Joining is the simplest reliable path across browsers;
  // we build one big string then hand it to a Blob (freed after download).
  const parts = [lastResult.headerLine];
  for (const line of lastResult.lines) parts.push(line);
  const blob = new Blob([parts.join("\n") + "\n"], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = buildOutputName();
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});

function buildOutputName() {
  // Derive a sensible name from the first file: strip a trailing date/range-ish
  // suffix and tag with "combined".
  const base = (queue[0]?.file.name || "combined.csv").replace(/\.csv$/i, "");
  return `${base}_combined.csv`;
}

// ---------------------------------------------------------------------------
// UI utility
// ---------------------------------------------------------------------------

function setBusy(busy) {
  processBtn.disabled = busy;
  clearBtn.disabled = busy;
  processBtn.textContent = busy ? "Working…" : "Merge & Check";
}
function setProgress(frac, text) {
  progressFill.style.width = `${Math.min(100, Math.round(frac * 100))}%`;
  if (text) progressText.textContent = text;
}
const tick = () => new Promise((r) => setTimeout(r, 0));

function formatBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}
function fmtDate(ms) {
  // Show in UTC with a clear suffix so it's unambiguous regardless of the
  // viewer's locale; the original timezone offset is preserved in the data.
  const d = new Date(ms);
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())} ` +
         `${p(d.getUTCHours())}:${p(d.getUTCMinutes())} UTC`;
}
function humanDur(ms) {
  const m = Math.round(ms / 60000);
  if (m < 60) return `${m} min`;
  const h = m / 60;
  if (h < 24) return `${trimNum(h)} h`;
  const d = h / 24;
  return `${trimNum(d)} day${d >= 2 ? "s" : ""}`;
}
function trimNum(n) { return Number.isInteger(n) ? String(n) : n.toFixed(1); }
function trimLine(line, max = 80) { return line.length > max ? line.slice(0, max) + "…" : line; }
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

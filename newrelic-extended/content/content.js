(() => {
  "use strict";

  // ─── Copy JSON Button ────────────────────────────────────────────────

  const COPY_ICON =
    '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';
  const CHECK_ICON =
    '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>';

  function addCopyButtons() {
    // Target New Relic's JSON chart visualization containers
    // The actual DOM uses: .-vz--chart-json > .-vz--viz-json > .-vz--chart-container
    const jsonCharts = document.querySelectorAll(".-vz--chart-json");

    jsonCharts.forEach((chart) => {
      if (chart.querySelector(".nrx-copy-btn")) return;

      // Make the chart container position:relative so we can absolutely position the button
      const vizJson = chart.querySelector(".-vz--viz-json") || chart;
      vizJson.style.position = "relative";

      const btn = document.createElement("button");
      btn.className = "nrx-copy-btn nrx-copy-btn--visible";
      btn.innerHTML = `${COPY_ICON} Copy JSON`;
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        e.preventDefault();

        // Get text from the chart container which holds the rendered JSON
        const container = chart.querySelector(".-vz--chart-container") || chart;
        const rawText = container.textContent;

        // Try to parse and re-format as clean JSON
        let output = rawText;
        try {
          output = JSON.stringify(JSON.parse(rawText), null, 2);
        } catch {
          // use raw text as-is
        }

        navigator.clipboard.writeText(output).then(() => {
          btn.innerHTML = `${CHECK_ICON} Copied!`;
          btn.classList.add("nrx-copied");
          setTimeout(() => {
            btn.innerHTML = `${COPY_ICON} Copy JSON`;
            btn.classList.remove("nrx-copied");
          }, 2000);
        });
      });

      vizJson.appendChild(btn);
    });

    // Also handle any generic <pre> blocks with JSON content (e.g. other NR pages)
    const preTags = document.querySelectorAll("pre");
    preTags.forEach((pre) => {
      if (pre.closest(".nrx-copy-wrapper")) return;
      if (pre.closest(".-vz--chart-json")) return; // already handled above

      const text = pre.textContent.trim();
      if (
        (text.startsWith("{") || text.startsWith("[")) &&
        text.length > 20 &&
        text.includes('"')
      ) {
        const wrapper = document.createElement("div");
        wrapper.className = "nrx-copy-wrapper";
        pre.parentNode.insertBefore(wrapper, pre);
        wrapper.appendChild(pre);

        const btn = document.createElement("button");
        btn.className = "nrx-copy-btn";
        btn.innerHTML = `${COPY_ICON} Copy`;
        btn.addEventListener("click", (e) => {
          e.stopPropagation();
          e.preventDefault();
          navigator.clipboard.writeText(pre.textContent).then(() => {
            btn.innerHTML = `${CHECK_ICON} Copied!`;
            btn.classList.add("nrx-copied");
            setTimeout(() => {
              btn.innerHTML = `${COPY_ICON} Copy`;
              btn.classList.remove("nrx-copied");
            }, 2000);
          });
        });
        wrapper.appendChild(btn);
      }
    });
  }

  // ─── Download Logs Panel ─────────────────────────────────────────────

  function getApiKeyFromStorage() {
    return new Promise((resolve) => {
      chrome.storage.local.get(["nrx_api_key", "nrx_account_id", "nrx_region"], (data) => {
        resolve({
          apiKey: data.nrx_api_key || "",
          accountId: data.nrx_account_id || "",
          region: data.nrx_region || "US",
        });
      });
    });
  }

  function saveSettings(apiKey, accountId, region) {
    chrome.storage.local.set({
      nrx_api_key: apiKey,
      nrx_account_id: accountId,
      nrx_region: region,
    });
  }

  function getSavedQueries() {
    return new Promise((resolve) => {
      chrome.storage.local.get(["nrx_saved_queries"], (data) => {
        const list = Array.isArray(data.nrx_saved_queries) ? data.nrx_saved_queries : [];
        resolve([...list].sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0)));
      });
    });
  }

  async function saveQueryToStorage(name, queryText) {
    const trimmedName = (name || "").trim();
    if (!trimmedName) {
      return { ok: false, error: "Name is required." };
    }
    if (!queryText || !queryText.trim()) {
      return { ok: false, error: "Query is empty." };
    }
    const existing = await getSavedQueries();
    if (existing.some((q) => q.name === trimmedName)) {
      return { ok: false, error: `A query named "${trimmedName}" already exists.` };
    }
    const entry = {
      id: (crypto.randomUUID && crypto.randomUUID()) || `q_${Date.now()}_${Math.random().toString(36).slice(2)}`,
      name: trimmedName,
      query: queryText,
      createdAt: Date.now(),
    };
    const next = [entry, ...existing];
    await new Promise((resolve) => chrome.storage.local.set({ nrx_saved_queries: next }, resolve));
    return { ok: true, entry };
  }

  async function deleteSavedQuery(id) {
    const existing = await getSavedQueries();
    const next = existing.filter((q) => q.id !== id);
    await new Promise((resolve) => chrome.storage.local.set({ nrx_saved_queries: next }, resolve));
  }

  function getNerdGraphUrl(region) {
    return region === "EU"
      ? "https://api.eu.newrelic.com/graphql"
      : "https://api.newrelic.com/graphql";
  }

  async function runNrqlQuery(apiKey, accountId, region, nrql) {
    const url = getNerdGraphUrl(region);
    const query = `{
      actor {
        account(id: ${accountId}) {
          nrql(query: "${nrql.replace(/"/g, '\\"')}", timeout: 120) {
            results
          }
        }
      }
    }`;

    const resp = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "API-Key": apiKey,
      },
      body: JSON.stringify({ query }),
    });

    if (!resp.ok) {
      throw new Error(`API error: ${resp.status} ${resp.statusText}`);
    }

    const json = await resp.json();
    if (json.errors && json.errors.length > 0) {
      throw new Error(json.errors.map((e) => e.message).join("; "));
    }

    return json.data.actor.account.nrql;
  }

  /**
   * Convert a Lucene log search query to NRQL WHERE clause.
   *
   * Handles common Lucene patterns used in New Relic Logs:
   *   key=value           → key = 'value'
   *   key:"value"         → key = 'value'
   *   key:"*wildcard*"    → key LIKE '%wildcard%'
   *   key:value           → key = 'value'
   *   bare term           → message LIKE '%term%'
   *   NOT / AND / OR      → preserved as NRQL boolean operators
   *   -key:value          → key != 'value'
   */
  function luceneToNrql(lucene) {
    if (!lucene || !lucene.trim()) return "SELECT * FROM Log";

    const tokens = [];
    const raw = lucene.trim();
    // Tokenise: respects quoted strings
    const regex = /(-?\w[\w.]*)\s*[:=]\s*("(?:[^"\\]|\\.)*"|\S+)|AND|OR|NOT|\S+/g;
    let match;

    while ((match = regex.exec(raw)) !== null) {
      const full = match[0];

      if (/^(AND|OR|NOT)$/i.test(full)) {
        tokens.push(full.toUpperCase());
        continue;
      }

      const key = match[1];
      let value = match[2];

      if (key && value !== undefined) {
        const negated = key.startsWith("-");
        const cleanKey = negated ? key.slice(1) : key;

        // Strip surrounding quotes
        if (value.startsWith('"') && value.endsWith('"')) {
          value = value.slice(1, -1);
        }

        // Handle wildcards
        if (value.includes("*")) {
          const likeVal = value.replace(/\*/g, "%");
          tokens.push(`${cleanKey} ${negated ? "NOT " : ""}LIKE '${likeVal}'`);
        } else {
          tokens.push(`${cleanKey} ${negated ? "!" : ""}= '${value}'`);
        }
      } else {
        // Bare term → search in message
        tokens.push(`message LIKE '%${full}%'`);
      }
    }

    // Join tokens: insert AND between conditions that aren't separated by boolean operators
    const parts = [];
    for (let i = 0; i < tokens.length; i++) {
      parts.push(tokens[i]);
      if (
        i < tokens.length - 1 &&
        !/^(AND|OR|NOT)$/i.test(tokens[i]) &&
        !/^(AND|OR|NOT)$/i.test(tokens[i + 1])
      ) {
        parts.push("AND");
      }
    }

    const whereClause = parts.join(" ");
    const nrql = `SELECT * FROM Log WHERE ${whereClause}`;

    // Append time range from the logs view time picker / URL
    const sinceClause = extractLogsViewTimeClause();
    return sinceClause ? `${nrql} ${sinceClause}` : nrql;
  }

  /**
   * Extract the time range from the Logs view and return a SINCE clause.
   * Checks URL params (begin/end/duration) which New Relic sets from the time picker.
   */
  function extractLogsViewTimeClause() {
    try {
      const hashParams = new URLSearchParams(window.location.hash.replace(/^#/, ""));
      const urlParams = new URLSearchParams(window.location.search);
      const begin = hashParams.get("begin") || urlParams.get("begin");
      const end = hashParams.get("end") || urlParams.get("end");
      const duration = hashParams.get("duration") || urlParams.get("duration");

      if (begin && end) {
        return `SINCE ${begin} UNTIL ${end}`;
      }
      if (duration) {
        const durationMs = parseInt(duration, 10);
        if (durationMs > 0) {
          const units = [
            { ms: 604800000, label: "week" },
            { ms: 86400000, label: "day" },
            { ms: 3600000, label: "hour" },
            { ms: 60000, label: "minute" },
          ];
          for (const { ms, label } of units) {
            if (durationMs >= ms && durationMs % ms === 0) {
              const count = durationMs / ms;
              return `SINCE ${count} ${label}${count > 1 ? "s" : ""} ago`;
            }
          }
          // Fallback to minutes (rounded up)
          const mins = Math.ceil(durationMs / 60000);
          return `SINCE ${mins} minute${mins > 1 ? "s" : ""} ago`;
        }
      }
    } catch {
      // ignore
    }

    // Try reading the time picker button text from the page
    const timePickerBtn = document.querySelector('[class*="TimePicker"] button, [aria-label*="time"] button');
    if (timePickerBtn) {
      const text = timePickerBtn.textContent.trim();
      // e.g. "30 minutes", "1 hour", "7 days"
      const relMatch = text.match(/(\d+)\s*(minute|hour|day|week)s?/i);
      if (relMatch) {
        const count = relMatch[1];
        const unit = relMatch[2].toLowerCase();
        return `SINCE ${count} ${unit}${count > 1 ? "s" : ""} ago`;
      }
    }

    return "";
  }

  function extractLuceneFromPage() {
    const input = document.querySelector(".logs-searchbar-input");
    if (input && input.value && input.value.trim()) {
      return input.value.trim();
    }
    return "";
  }

  function isLogsView() {
    return !!document.querySelector(".logs-searchbar");
  }

  function setNrqlOnPage(query) {
    const aceContainer = document.querySelector(".ace-nrql");
    if (!aceContainer) return false;
    const script = document.createElement("script");
    script.textContent = `
      (function() {
        try {
          var el = document.querySelector(".ace-nrql");
          if (el && window.ace && typeof window.ace.edit === "function") {
            var editor = window.ace.edit(el);
            editor.setValue(${JSON.stringify(query)}, 1);
            editor.focus();
          }
        } catch (e) { console.warn("nrx setNrqlOnPage failed:", e); }
      })();
    `;
    (document.head || document.documentElement).appendChild(script);
    script.remove();
    return true;
  }

  function extractNrqlFromPage() {
    // New Relic uses Ace Editor with class "ace-nrql"
    // The query text is rendered in .ace_text-layer .ace_line elements
    const aceEditor = document.querySelector(".ace-nrql .ace_text-layer");
    if (aceEditor) {
      const lines = aceEditor.querySelectorAll(".ace_line");
      const queryText = Array.from(lines)
        .map((line) => line.textContent)
        .join(" ")
        .replace(/\s+/g, " ")
        .trim();
      if (queryText && /^\s*(SELECT|FROM|SHOW)\s/i.test(queryText)) {
        return queryText;
      }
    }

    // Check for Logs view with Lucene search bar
    const luceneQuery = extractLuceneFromPage();
    if (luceneQuery) {
      return luceneToNrql(luceneQuery);
    }

    // Fallback: check URL for query parameter
    const urlParams = new URLSearchParams(window.location.search);
    const queryParam = urlParams.get("query") || urlParams.get("nrql");
    if (queryParam) return queryParam;

    return "";
  }

  function extractAccountIdFromPage() {
    // Account selector shows "Account: 4350830 - Account 4350830"
    const trigger = document.querySelector(".wnd-SearchSelectTrigger-title");
    if (trigger) {
      const match = trigger.textContent.match(/Account:\s*(\d+)/);
      if (match) return match[1];
    }
    return "";
  }

  // Strip LIMIT, OFFSET, SINCE, UNTIL, ORDER BY clauses so we can inject our own
  function stripPaginationClauses(query) {
    return query
      .replace(/\s+LIMIT\s+\d+/gi, "")
      .replace(/\s+OFFSET\s+\d+/gi, "")
      .replace(/\s+ORDER\s+BY\s+\S+\s*(ASC|DESC)?/gi, "");
  }

  function stripTimeClauses(query) {
    return query
      .replace(/\s+SINCE\s+[^A-Z]*?(UNTIL|LIMIT|OFFSET|ORDER|$)/gi, " $1")
      .replace(/\s+UNTIL\s+[^A-Z]*?(SINCE|LIMIT|OFFSET|ORDER|$)/gi, " $1")
      .replace(/\s+SINCE\s+.+$/gi, "")
      .trim();
  }

  // Extract SINCE/UNTIL from the original query to determine the time range
  function extractTimeRange(query) {
    // Try to get time range from the JSON chart metadata on the page
    const vizJson = document.querySelector(".-vz--chart-container");
    if (vizJson) {
      try {
        const parsed = JSON.parse(vizJson.textContent);
        if (parsed[0]?.metadata?.beginTimeMillis && parsed[0]?.metadata?.endTimeMillis) {
          return {
            startMs: parsed[0].metadata.beginTimeMillis,
            endMs: parsed[0].metadata.endTimeMillis,
          };
        }
      } catch {
        // not valid JSON or different structure
      }
    }

    // Parse relative time from query (e.g. "SINCE 30 minutes ago", "SINCE 1 hour ago")
    const sinceRelMatch = query.match(/SINCE\s+(\d+)\s+(minute|hour|day|week)s?\s+ago/i);
    if (sinceRelMatch) {
      const num = parseInt(sinceRelMatch[1], 10);
      const unit = sinceRelMatch[2].toLowerCase();
      const multipliers = { minute: 60000, hour: 3600000, day: 86400000, week: 604800000 };
      const now = Date.now();
      return {
        startMs: now - num * (multipliers[unit] || 3600000),
        endMs: now,
      };
    }

    // Try URL time params (New Relic logs view uses begin/end or duration in the URL hash/params)
    try {
      const hashParams = new URLSearchParams(window.location.hash.replace(/^#/, ""));
      const urlParams = new URLSearchParams(window.location.search);
      const begin = hashParams.get("begin") || urlParams.get("begin");
      const end = hashParams.get("end") || urlParams.get("end");
      const duration = hashParams.get("duration") || urlParams.get("duration");

      if (begin && end) {
        return { startMs: parseInt(begin, 10), endMs: parseInt(end, 10) };
      }
      if (duration) {
        const durationMs = parseInt(duration, 10);
        if (durationMs > 0) {
          const now = Date.now();
          return { startMs: now - durationMs, endMs: now };
        }
      }
    } catch {
      // ignore URL parsing errors
    }

    // Default: last 1 hour
    const now = Date.now();
    return { startMs: now - 3600000, endMs: now };
  }

  function downloadFile(content, filename, type) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  function jsonToDelimited(results, delimiter = ",") {
    if (!results.length) return "";
    // Collect all keys, skip empty/blank keys
    const headerSet = new Set();
    results.forEach((row) => Object.keys(row).forEach((k) => {
      if (k && k.trim()) headerSet.add(k);
    }));
    // Drop columns where every row has null/undefined/empty value
    const headers = Array.from(headerSet).filter((h) =>
      results.some((row) => {
        const val = row[h];
        return val !== null && val !== undefined && String(val).trim() !== "";
      })
    );
    const rows = results.map((row) =>
      headers
        .map((h) => {
          const val = row[h];
          if (val === null || val === undefined) return "";
          const str = typeof val === "object" ? JSON.stringify(val) : String(val);
          return `"${str.replace(/"/g, '""')}"`;
        })
        .join(delimiter)
    );
    return [headers.map((h) => `"${h}"`).join(delimiter), ...rows].join("\n");
  }

  // Flatten nested event arrays from NRQL results
  // NRQL SELECT * returns: [{events: [{...}, {...}]}]
  // NRQL SELECT field returns: [{field: val, timestamp: ...}, ...]
  function flattenResults(results) {
    if (!results || !results.length) return [];
    // Check if results contain nested "events" arrays
    if (results[0] && Array.isArray(results[0].events)) {
      return results.flatMap((r) => r.events || []);
    }
    return results;
  }

  /**
   * Download all logs using time-based cursor pagination.
   *
   * Strategy: NRQL caps OFFSET+LIMIT at 5000. To get more, we:
   * 1. Query with LIMIT 5000 ORDER BY timestamp ASC SINCE <start> UNTIL <end>
   * 2. Take the last timestamp from the batch
   * 3. Use that as the new SINCE for the next batch
   * 4. Repeat until a batch returns fewer than LIMIT rows
   * 5. Deduplicate by timestamp (multiple events can share the same ms)
   */
  /**
   * Fetch all logs using time-based cursor pagination.
   * Returns the array of results (does NOT download a file).
   */
  async function fetchAllLogs(apiKey, accountId, region, baseQuery, onProgress) {
    const BATCH_SIZE = 5000;
    const MAX_RECORDS = 500000;
    let allResults = [];

    const cleanQuery = stripPaginationClauses(stripTimeClauses(baseQuery));
    const { startMs, endMs } = extractTimeRange(baseQuery);

    let cursorMs = startMs;
    let hasMore = true;
    let batchNum = 0;

    // Track seen timestamps+messages to deduplicate overlap
    const seenKeys = new Set();

    function dedupeKey(row) {
      const ts = row.timestamp || row.beginTimeSeconds || "";
      const msg = (row.message || row.name || JSON.stringify(row)).slice(0, 100);
      return `${ts}:${msg}`;
    }

    while (hasMore) {
      batchNum++;
      const paginatedQuery = `${cleanQuery} SINCE ${cursorMs} UNTIL ${endMs} ORDER BY timestamp ASC LIMIT ${BATCH_SIZE}`;

      onProgress({
        status: "fetching",
        fetched: allResults.length,
        batch: batchNum,
      });

      try {
        const result = await runNrqlQuery(apiKey, accountId, region, paginatedQuery);
        let batch = flattenResults(result.results || []);

        if (batch.length === 0) {
          hasMore = false;
          break;
        }

        const newRows = [];
        for (const row of batch) {
          const key = dedupeKey(row);
          if (!seenKeys.has(key)) {
            seenKeys.add(key);
            newRows.push(row);
          }
        }

        allResults = allResults.concat(newRows);

        if (batch.length < BATCH_SIZE) {
          hasMore = false;
        } else {
          const lastTimestamp = batch[batch.length - 1].timestamp;
          if (!lastTimestamp || lastTimestamp <= cursorMs) {
            hasMore = false;
          } else {
            cursorMs = lastTimestamp;
          }
        }

        if (allResults.length >= MAX_RECORDS) {
          hasMore = false;
        }
      } catch (err) {
        onProgress({ status: "error", error: err.message, fetched: allResults.length });
        return null;
      }
    }

    if (allResults.length === 0) {
      onProgress({ status: "error", error: "No results returned.", fetched: 0 });
      return null;
    }

    onProgress({ status: "complete", fetched: allResults.length });
    return allResults;
  }

  async function downloadAllLogs(apiKey, accountId, region, baseQuery, format, onProgress) {
    const allResults = await fetchAllLogs(apiKey, accountId, region, baseQuery, onProgress);
    if (!allResults) return;

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

    if (format === "json") {
      downloadFile(JSON.stringify(allResults, null, 2), `newrelic-logs-${timestamp}.json`, "application/json");
    } else if (format === "tsv") {
      downloadFile(jsonToDelimited(allResults, "\t"), `newrelic-logs-${timestamp}.tsv`, "text/tab-separated-values");
    } else {
      downloadFile(jsonToDelimited(allResults, ","), `newrelic-logs-${timestamp}.csv`, "text/csv");
    }
  }

  // ─── UI Creation ─────────────────────────────────────────────────────

  const DOWNLOAD_ICON = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>`;

  function createFab() {
    if (document.querySelector(".nrx-fab")) return;

    const fab = document.createElement("button");
    fab.className = "nrx-fab";
    fab.title = "NewRelic Extended: Download Logs";
    fab.innerHTML = DOWNLOAD_ICON;
    fab.addEventListener("click", () => toggleDownloadPanel());
    document.body.appendChild(fab);
  }

  let panelEl = null;

  async function toggleDownloadPanel() {
    if (panelEl) {
      panelEl.remove();
      panelEl = null;
      return;
    }

    const settings = await getApiKeyFromStorage();
    const detectedQuery = extractNrqlFromPage();
    const detectedAccountId = extractAccountIdFromPage() || settings.accountId;
    const fromLucene = isLogsView() && !!extractLuceneFromPage();

    const hasApiKey = !!settings.apiKey;
    let currentAccountId = detectedAccountId;

    panelEl = document.createElement("div");
    panelEl.className = "nrx-download-panel";
    panelEl.innerHTML = `
      <div class="nrx-download-panel-header">
        <h3>Download Logs</h3>
        <div style="display:flex;align-items:center;gap:8px;">
          <button class="nrx-download-panel-settings" title="Settings">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
          </button>
          <button class="nrx-download-panel-close">&times;</button>
        </div>
      </div>
      <div class="nrx-download-panel-body">
        ${!hasApiKey ? '<div class="nrx-warning-text">API Key not configured. Click the gear icon or open extension settings.</div>' : ""}
        ${fromLucene ? '<div class="nrx-info-text">Lucene query auto-converted to NRQL. Review and adjust if needed.</div>' : ""}

        <label for="nrx-saved-queries">Saved queries</label>
        <div class="nrx-saved-row">
          <select id="nrx-saved-queries">
            <option value="" disabled selected>— Load a saved query —</option>
          </select>
          <button class="nrx-icon-btn" id="nrx-delete-saved" title="Delete selected saved query" disabled>
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v2"/></svg>
          </button>
        </div>

        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;">
          <label for="nrx-query" style="margin-bottom:0;">NRQL Query</label>
          <div style="display:flex;gap:6px;">
            <button class="nrx-reload-btn" id="nrx-save-query" title="Save current query">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>
              Save
            </button>
            <button class="nrx-reload-btn" id="nrx-reload-query" title="Reload query from page">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
              Reload
            </button>
          </div>
        </div>
        <textarea id="nrx-query" rows="3" placeholder="SELECT * FROM Log WHERE ...">${detectedQuery.replace(/</g, "&lt;").replace(/>/g, "&gt;")}</textarea>

        <label for="nrx-format">Format</label>
        <select id="nrx-format">
          <option value="json">JSON</option>
          <option value="csv">CSV</option>
          <option value="tsv">TSV</option>
        </select>

        <div class="nrx-progress-bar nrx-hidden" id="nrx-progress-bar">
          <div class="nrx-progress-bar-fill" id="nrx-progress-fill"></div>
        </div>
        <div class="nrx-progress-text nrx-hidden" id="nrx-progress-text"></div>
        <div class="nrx-error-text nrx-hidden" id="nrx-error-text"></div>

        <div class="nrx-btn-row">
          <button class="nrx-download-btn" id="nrx-start-download" ${!hasApiKey ? "disabled" : ""}>Download All Logs</button>
          <button class="nrx-copy-results-btn" id="nrx-copy-results" title="Copy results to clipboard" ${!hasApiKey ? "disabled" : ""}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
          </button>
        </div>
      </div>
    `;

    document.body.appendChild(panelEl);

    panelEl.querySelector(".nrx-download-panel-close").addEventListener("click", () => {
      panelEl.remove();
      panelEl = null;
    });

    panelEl.querySelector(".nrx-download-panel-settings").addEventListener("click", () => {
      chrome.runtime.sendMessage({ type: "OPEN_OPTIONS" });
    });

    panelEl.querySelector("#nrx-reload-query").addEventListener("click", () => {
      const freshQuery = extractNrqlFromPage();
      const freshAccountId = extractAccountIdFromPage();
      if (freshQuery) {
        panelEl.querySelector("#nrx-query").value = freshQuery;
      }
      if (freshAccountId) {
        currentAccountId = freshAccountId;
      }
      // Brief visual feedback
      const btn = panelEl.querySelector("#nrx-reload-query");
      btn.classList.add("nrx-reloaded");
      setTimeout(() => btn.classList.remove("nrx-reloaded"), 1000);
    });

    let savedQueriesCache = [];

    async function refreshSavedQueries(selectId) {
      savedQueriesCache = await getSavedQueries();
      const select = panelEl.querySelector("#nrx-saved-queries");
      const deleteBtn = panelEl.querySelector("#nrx-delete-saved");
      if (!select) return;
      const opts = ['<option value="" disabled' + (selectId ? "" : " selected") + '>— Load a saved query —</option>'];
      for (const q of savedQueriesCache) {
        const label = q.name.replace(/</g, "&lt;").replace(/>/g, "&gt;");
        const selected = q.id === selectId ? " selected" : "";
        opts.push(`<option value="${q.id}"${selected}>${label}</option>`);
      }
      select.innerHTML = opts.join("");
      deleteBtn.disabled = !selectId;
    }

    panelEl.querySelector("#nrx-saved-queries").addEventListener("change", (e) => {
      const id = e.target.value;
      const entry = savedQueriesCache.find((q) => q.id === id);
      if (entry) {
        panelEl.querySelector("#nrx-query").value = entry.query;
        setNrqlOnPage(entry.query);
      }
      panelEl.querySelector("#nrx-delete-saved").disabled = !id;
    });

    panelEl.querySelector("#nrx-save-query").addEventListener("click", async () => {
      const errorText = panelEl.querySelector("#nrx-error-text");
      errorText.classList.add("nrx-hidden");
      const queryText = panelEl.querySelector("#nrx-query").value.trim();
      if (!queryText) {
        showError("Cannot save an empty query.");
        return;
      }
      const name = window.prompt("Name this query:");
      if (name === null) return; // cancelled
      const result = await saveQueryToStorage(name, queryText);
      if (!result.ok) {
        showError(result.error);
        return;
      }
      await refreshSavedQueries(result.entry.id);
      const btn = panelEl.querySelector("#nrx-save-query");
      btn.classList.add("nrx-reloaded");
      setTimeout(() => btn.classList.remove("nrx-reloaded"), 1000);
    });

    panelEl.querySelector("#nrx-delete-saved").addEventListener("click", async () => {
      const select = panelEl.querySelector("#nrx-saved-queries");
      const id = select.value;
      if (!id) return;
      const entry = savedQueriesCache.find((q) => q.id === id);
      if (!entry) return;
      if (!window.confirm(`Delete saved query "${entry.name}"?`)) return;
      await deleteSavedQuery(id);
      await refreshSavedQueries();
    });

    refreshSavedQueries();

    panelEl.querySelector("#nrx-copy-results").addEventListener("click", async () => {
      const latestSettings = await getApiKeyFromStorage();
      const apiKey = latestSettings.apiKey;
      const region = latestSettings.region || "US";
      const accountId = currentAccountId;
      const query = panelEl.querySelector("#nrx-query").value.trim();
      const format = panelEl.querySelector("#nrx-format").value;

      if (!apiKey) {
        showError("API Key not configured. Open extension settings first.");
        return;
      }
      if (!accountId || !query) {
        showError("Please fill in Account ID and NRQL Query.");
        return;
      }

      const copyBtn = panelEl.querySelector("#nrx-copy-results");
      const progressBar = panelEl.querySelector("#nrx-progress-bar");
      const progressFill = panelEl.querySelector("#nrx-progress-fill");
      const progressText = panelEl.querySelector("#nrx-progress-text");
      const errorText = panelEl.querySelector("#nrx-error-text");

      copyBtn.disabled = true;
      panelEl.querySelector("#nrx-start-download").disabled = true;
      progressBar.classList.remove("nrx-hidden");
      progressText.classList.remove("nrx-hidden");
      errorText.classList.add("nrx-hidden");

      const allResults = await fetchAllLogs(apiKey, accountId, region, query, (progress) => {
        if (progress.status === "fetching") {
          const pct = Math.min(90, progress.batch * 12);
          progressFill.style.width = `${pct}%`;
          progressText.textContent = `Batch ${progress.batch}: fetched ${progress.fetched} records so far...`;
        } else if (progress.status === "complete") {
          progressFill.style.width = "100%";
          progressText.textContent = `Copied ${progress.fetched} records to clipboard!`;
        } else if (progress.status === "error") {
          errorText.textContent = `Error: ${progress.error}`;
          errorText.classList.remove("nrx-hidden");
          progressText.textContent = progress.fetched > 0
            ? `Stopped at ${progress.fetched} records.`
            : "";
        }
      });

      copyBtn.disabled = false;
      panelEl.querySelector("#nrx-start-download").disabled = false;

      if (allResults && allResults.length > 0) {
        const text = format === "json"
          ? JSON.stringify(allResults, null, 2)
          : jsonToDelimited(allResults, format === "tsv" ? "\t" : ",");
        await navigator.clipboard.writeText(text);
        copyBtn.classList.add("nrx-copied-results");
        setTimeout(() => copyBtn.classList.remove("nrx-copied-results"), 2000);
      }
    });

    panelEl.querySelector("#nrx-start-download").addEventListener("click", async () => {
      const latestSettings = await getApiKeyFromStorage();
      const apiKey = latestSettings.apiKey;
      const region = latestSettings.region || "US";
      const accountId = currentAccountId;
      const query = panelEl.querySelector("#nrx-query").value.trim();
      const format = panelEl.querySelector("#nrx-format").value;

      if (!apiKey) {
        showError("API Key not configured. Open extension settings first.");
        return;
      }
      if (!accountId || !query) {
        showError("Please fill in Account ID and NRQL Query.");
        return;
      }

      const btn = panelEl.querySelector("#nrx-start-download");
      const progressBar = panelEl.querySelector("#nrx-progress-bar");
      const progressFill = panelEl.querySelector("#nrx-progress-fill");
      const progressText = panelEl.querySelector("#nrx-progress-text");
      const errorText = panelEl.querySelector("#nrx-error-text");

      btn.disabled = true;
      btn.textContent = "Downloading...";
      progressBar.classList.remove("nrx-hidden");
      progressText.classList.remove("nrx-hidden");
      errorText.classList.add("nrx-hidden");

      await downloadAllLogs(apiKey, accountId, region, query, format, (progress) => {
        if (progress.status === "fetching") {
          // Indeterminate-style: slowly fill based on batch count
          const pct = Math.min(90, progress.batch * 12);
          progressFill.style.width = `${pct}%`;
          progressText.textContent = `Batch ${progress.batch}: fetched ${progress.fetched} records so far...`;
        } else if (progress.status === "complete") {
          progressFill.style.width = "100%";
          progressText.textContent = `Done! Downloaded ${progress.fetched} records.`;
          btn.disabled = false;
          btn.textContent = "Download All Logs";
        } else if (progress.status === "error") {
          errorText.textContent = `Error: ${progress.error}`;
          errorText.classList.remove("nrx-hidden");
          progressText.textContent = progress.fetched > 0
            ? `Stopped at ${progress.fetched} records.`
            : "";
          btn.disabled = false;
          btn.textContent = "Retry Download";
        }
      });
    });
  }

  function showError(msg) {
    const errorText = panelEl?.querySelector("#nrx-error-text");
    if (errorText) {
      errorText.textContent = msg;
      errorText.classList.remove("nrx-hidden");
    }
  }

  // ─── Initialization ──────────────────────────────────────────────────

  function init() {
    createFab();
    addCopyButtons();
  }

  // Run on page load
  init();

  // Re-run when DOM changes (SPA navigation, new results loaded)
  const observer = new MutationObserver(() => {
    // Debounce
    clearTimeout(observer._timer);
    observer._timer = setTimeout(() => {
      addCopyButtons();
    }, 500);
  });

  observer.observe(document.body, {
    childList: true,
    subtree: true,
  });
})();

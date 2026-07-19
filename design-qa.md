# Design QA

This is the browser acceptance contract for the local application. It records
the behavior that must remain stable, not a history of implementation passes or
machine-specific screenshots.

## Overview

- Today, this week, and this month hydrate independently from the yearly view.
- The annual heatmap uses calendar dates in the configured local timezone;
  future dates are inert and day cards remain inside the viewport.
- Top projects and top sessions use the selected year and agree with the
  corresponding Sessions filters.
- A cold Overview request against the live corpus, including the annual panels,
  completes in under one second on the development machine.

## Sessions

- Search, date/project filtering, recent/cost sorting, pagination, and direct
  navigation retain canonical Codex titles.
- Invalid URL dates and page numbers are removed or normalized instead of being
  sent to the API.
- The project menu supports pointer, Arrow keys, Home/End, Escape, and focus
  restoration.
- Zero-usage, active, interrupted, legacy, usage-only, and unknown-price
  sessions remain visible; replay-only physical files do not become sessions.

## Session detail and Activity

- Summary totals, model/effort mix, tools, agents, status, and estimated cost
  agree with stored usage facts.
- Activity is a keyboard-readable tree grid. Turn, group, and child rows expose
  their hierarchy and expanded state to assistive technology.
- Exchanges are newest-first. Expanded execution preserves authored messages,
  assistant updates, reasoning, tool lifecycle, subagents, reviews, state
  changes, and interruptions. Tool rows expose type, status, timing, cost, and
  tokens without payloads or attachments.
- Large exchanges are fetched in bounded pages and expose an explicit way to
  load the remaining children; expanding one row must not require returning the
  entire trace in one response.
- Authored user text remains primary. App captures and other transport context
  stay available under closed supporting-material disclosures.
- The session ID links to `codex://threads/{session_id}` for full-fidelity
  inspection in Codex.
- Expanded assistant answers retain safe Markdown structure. Raw HTML and unsafe
  links never become active content.

## Stats

- Day, week, month, year, and all-time ranges use exact, non-overlapping bucket
  boundaries and drill into the same Sessions range.
- All-time is data-derived; a caller-provided anchor cannot manufacture empty
  future buckets.
- Bucket labels and the top-level response label are runtime-validated by the
  frontend.
- Every cold Stats range completes in under one second on the development
  machine.

## Pricing and storage

- Prices use integer micro-USD storage and exact integer aggregation. Decimal
  strings cross the settings API; binary floating-point values are never used
  for price input or persistence.
- Manual prices and aliases override refreshed and bundled layers without being
  overwritten by refresh.
- Price mutations invalidate every cost-bearing view.
- Settings shows the local database location and current size, and explains
  that retained session history can continue to grow.

## Verification

Before handoff:

1. Run the Rust unit, integration, corpus, and API-contract suites.
2. Run formatting, all-target compile checks, and strict Clippy.
3. Run frontend unit tests, ESLint, the production build, and the real-browser
   end-to-end suite.
4. Rebuild a fresh SQLite projection from the configured roots; require an
   `integrity_check` result of `ok` and zero foreign-key violations.
5. Measure cold Overview and every Stats range against the live corpus and keep
   each below one second.
6. Walk Overview, Sessions, Summary, Activity, Stats, and Settings in the
   running browser with no console or request failures.

Record environment-specific counts and timings in the task handoff rather than
freezing them in this document.

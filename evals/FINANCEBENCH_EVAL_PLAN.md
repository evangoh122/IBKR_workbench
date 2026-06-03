# FinanceBench Evaluation Plan

**Purpose:** Use the [FinanceBench](https://github.com/patronus-ai/financebench) benchmark
to verify that the documents this project extracts from SEC EDGAR — and the RAG
pipeline built on top of them — actually produce correct, grounded answers.

**Status:** PLAN (not yet implemented)
**Owner:** TBD
**Related audit items:** `PROJECT_ERROR_AUDIT.yaml` → **H1**, **M1** (hard blockers, see §3)

---

## 1. What FinanceBench is and why it fits this project

FinanceBench (Patronus AI, 2023) is a benchmark for financial-document question
answering. Each item pairs a natural-language question with a gold answer and the
**exact evidence** (source filing, page number, evidence text) it was derived from.

- **Open subset:** ~150 questions, publicly released (HuggingFace `PatronusAI/financebench`
  + GitHub). Full set is 10,231 questions (gated).
- **Document types:** 10-K, 10-Q, 8-K, earnings releases — **the same forms**
  `etl/extract_edgar.py` already targets (`form_types=['10-K','10-Q','8-K']`).
- **Question types:** `domain-relevant` (metric lookup), `metrics-generated`
  (numeric/derived), `novel-generated` (reasoning).
- **Companies:** large-cap US issuers (3M, Apple, Amazon, Boeing, Coca-Cola, Pfizer, …),
  all of which the Finviz-built ticker universe (`config/update_tickers.py`) covers.

> ⚠️ Verify field names / counts against the current release before coding — the schema
> below reflects the 2023–2024 open subset and may drift.

**Mapping to our code:**

| FinanceBench needs | Our component | Table |
|---|---|---|
| Structured metrics (revenue, EPS, assets…) | `extract_edgar.run_edgar_facts_etl` + `_CONCEPTS` | `edgar_facts` |
| Filing-level retrieval | `embed_edgar.run_embed_edgar_etl` | `edgar_embeddings` |
| End-to-end answers | `rag_engine.ask_rag` / `EDGARFactsRetriever` | both |

---

## 2. Objectives (what "properly evaluated" means here)

We evaluate the extraction pipeline at **three independent layers**, weakest-coupling first:

- **E1 — Extraction fidelity (structured):** Do the numbers we parsed into `edgar_facts`
  match FinanceBench gold answers for metric questions? *Tests `extract_edgar` directly,
  no LLM, no retrieval.* This is the most direct answer to "are the documents we extract
  properly evaluated."
- **E2 — Oracle answer quality:** Given the *correct* gold passage, can the LLM produce the
  gold answer? *Tests generation in isolation (`rag_engine` prompt + provider), removing
  retrieval as a variable.*
- **E3 — Open-book RAG (end-to-end):** Can `ask_rag()` retrieve the right evidence from our
  own embedded corpus **and** answer correctly? *Tests the whole pipeline.*

Each layer isolates a different failure mode, so a regression points at a specific module.

---

## 3. Prerequisites / blockers (DO THESE FIRST)

These are not optional — the eval is invalid without them.

1. **Fix H1 (vector-store path mismatch).** `embed_tickers`/`embed_edgar` write embeddings
   into `ibkr.duckdb` (via `get_connection`), but `rag_engine` reads `vectors.duckdb`
   (`DUCKDB_PATH`). Until one store is canonical, E3 measures an empty corpus. *Blocks E3.*
2. **Fix M1 (dead `edgar_embeddings`).** No retriever in `rag_engine` queries
   `edgar_embeddings`; `EDGARFactsRetriever` uses the structured `edgar_facts` table.
   For E3 to test *document* retrieval (not just facts), add an `EDGAREmbeddingsRetriever`
   over `edgar_embeddings`, or accept that E3 only exercises the facts path. *Shapes E3 scope.*
3. **Period coverage gap.** `embed_edgar.fetch_latest_10k_with_downloader` fetches only the
   **latest single 10-K**. FinanceBench questions reference **specific historical periods**
   (e.g. FY2022 10-K, Q3-2023 10-Q) and **8-Ks**. The eval harness must ingest the *exact*
   `(company, doc_type, doc_period)` each selected question cites — extend the downloader to
   accept a target filing, or pre-load FinanceBench's own provided PDFs as the oracle corpus.
4. **Facts coverage gap.** `edgar_facts` stores only `10-K`/`10-Q` and the 11 concepts in
   `_CONCEPTS`. E1 is only valid for metric questions whose concept+period is in scope —
   filter the question set accordingly (don't score questions we structurally can't answer).
5. **A judge model + key.** E2/E3 need an LLM judge. Reuse the `chat_engine` provider
   (`CHAT_PROVIDER`), but pin a deterministic judge (temperature 0) and record provider+model
   in every result row for reproducibility. (See audit **H2** — make sure the chosen provider
   is actually implemented.)

---

## 4. Dataset acquisition & licensing

- **Source:** `datasets.load_dataset("PatronusAI/financebench")` (or vendored JSONL from the
  GitHub repo). Gold PDFs are referenced per item; the repo provides retrieval URLs/scripts.
- **License:** open subset is **CC-BY-NC-4.0** (non-commercial). ✅ fine for internal eval;
  ❌ do not ship gold answers/PDFs in a commercial artifact. Keep under `evals/data/` and
  gitignore it (add `evals/data/` to `.gitignore`).
- **Cache:** store the normalized question set once at `evals/data/financebench_open.jsonl`
  with fields we use: `id, question, answer, company, ticker, doc_type, doc_period,
  question_type, evidence[] (text, doc_name, page)`.
- **Ticker mapping:** FinanceBench keys on company/doc name, not ticker. Build a
  `company → ticker → CIK` map by reusing `extract_edgar._build_cik_map` so selected
  questions line up with our `edgar_*` tables.

---

## 5. Metrics & acceptance thresholds

| Layer | Metric | Definition | Initial gate |
|---|---|---|---|
| E1 | **Numeric match rate** | gold answer within ±tolerance of `edgar_facts` value (default 1% rel., unit-normalized) | ≥ 0.85 on in-scope metric Qs |
| E1 | **Coverage** | % of FinanceBench metric Qs whose concept+period exists in `edgar_facts` | report (diagnostic) |
| E2 | **Answer accuracy** | LLM-judge correct vs gold, given oracle passage | ≥ 0.70 |
| E3 | **Evidence recall@k** | gold evidence page/passage appears in retrieved top-k (k=5) | ≥ 0.60 |
| E3 | **Answer accuracy** | LLM-judge correct vs gold, full pipeline | ≥ 0.50 |
| E2/E3 | **Hallucination rate** | judged-incorrect **and** not a refusal (FinanceBench penalizes confident wrong answers) | ≤ 0.10 |
| E2/E3 | **Refusal rate** | "I don't have enough data…" (our prompt's escape hatch) | report |

Thresholds are first-pass placeholders — calibrate after a baseline run, then freeze as
regression gates. The **hallucination rate** is the headline metric: per the FinanceBench
paper, a confidently wrong financial answer is worse than a refusal.

---

## 6. Proposed layout

```
evals/
  FINANCEBENCH_EVAL_PLAN.md         # this file
  __init__.py
  financebench/
    __init__.py
    dataset.py        # load + normalize + company→ticker→CIK mapping (+ scope filters)
    ingest.py         # ensure the exact cited filings are in edgar_facts / edgar_embeddings
    judge.py          # LLM-as-judge (reuses chat_engine provider, temp=0, structured verdict)
    run_e1_facts.py   # extraction-fidelity eval  → edgar_facts vs gold numerics
    run_e2_oracle.py  # oracle answer eval         → gold passage + rag prompt
    run_e3_rag.py     # end-to-end eval            → ask_rag() incl. retrieval
    report.py         # aggregate → evals/results/financebench_<ts>.{json,md}
  data/               # gitignored: cached questions + gold PDFs (CC-BY-NC)
  results/            # gitignored: run outputs
tests/
  test_financebench_smoke.py        # 3–5 pinned items, asserts harness wiring (CI-safe)
```

---

## 7. Phased implementation

**Phase 0 — Unblock (depends on §3).**
Fix H1 + M1; extend `embed_edgar` (or add `evals/financebench/ingest.py`) to fetch a
*specified* filing rather than only the latest 10-K. Add `evals/data/` + `evals/results/`
to `.gitignore`.

**Phase 1 — Dataset layer.** `dataset.py`: load open subset, normalize fields, map
company→ticker→CIK, and emit scope flags (`e1_eligible` if concept+period in `_CONCEPTS`
and form in 10-K/10-Q; `e3_eligible` if filing ingestible). Cache to JSONL.

**Phase 2 — E1 extraction fidelity (highest ROI, no LLM).**
For each `e1_eligible` metric question: run `run_edgar_facts_etl` for its ticker, look up the
matching `(concept, period_end, form_type, unit)` in `edgar_facts`, compare to gold within
tolerance. Emit per-question pass/fail + the coverage number. This alone answers the original
ask: *are the extracted documents' numbers correct?*

**Phase 3 — E2 oracle.** `judge.py` + `run_e2_oracle.py`: feed the gold evidence passage as
`context` into the existing `_RAG_PROMPT`, call the provider, judge vs gold. Isolates
generation quality from retrieval.

**Phase 4 — E3 end-to-end.** Ingest each question's cited filing into our corpus, run
`ask_rag()`, compute evidence recall@k (compare retrieved chunks against gold evidence
page/text) and judged accuracy. Requires the Phase-0 retriever over `edgar_embeddings`.

**Phase 5 — Reporting + CI.** `report.py` aggregates to JSON + a markdown scorecard.
Add a **smoke test** (`tests/test_financebench_smoke.py`, 3–5 pinned items, mocked/cached
LLM) to CI so the harness can't silently break. Gate the *full* paid run behind a manual /
nightly workflow (it costs API calls + downloads PDFs) — keep it out of the PR `test` job.

---

## 8. CI integration notes

- **PR job:** smoke test only — offline, no API keys, no network (cache fixtures).
- **Nightly / manual `workflow_dispatch`:** full E1–E3 with secrets
  (`DEEPSEEK_API_KEY` etc.), upload `evals/results/*` as an artifact, fail if any frozen
  gate from §5 regresses.
- Reuse `conftest.py` ibapi stubs; FinanceBench evals don't touch IBKR.

---

## 9. Risks & limitations

- **Non-commercial license** on the open subset — internal use only; don't redistribute.
- **Latest-only ingestion** (`embed_edgar`) makes E3 unfair until Phase-0 period-targeting
  lands; until then, run E3 against FinanceBench's *provided* PDFs as an oracle corpus.
- **Judge variance / cost** — pin temperature 0, log model+version, sample-audit a slice of
  verdicts by hand before trusting the gate.
- **Concept name drift** — `_CONCEPTS` uses fixed us-gaap tags (e.g. `Revenues`,
  `RevenueFromContractWithCustomerExcludingAssessedTax`). Some issuers report under different
  tags; E1 coverage will be < 100% and that's expected — report it, don't hide it.
- **Small open set (~150)** — treat numbers as directional, not statistically tight; report
  per-`question_type` and per-`doc_type` breakdowns.

---

## 10. Open decisions (need a call before Phase 1)

1. **Subset:** open 150 only, or pursue gated full 10,231? (default: open 150)
2. **Judge provider:** reuse `CHAT_PROVIDER`, or pin a dedicated judge model? (default: reuse,
   temp 0, recorded per row)
3. **E3 corpus:** our embedded `edgar_embeddings`, FinanceBench-provided PDFs, or both?
   (default: both — ours is the real test, theirs is the upper-bound oracle)
4. **Gate strictness:** advisory (report-only) first, or hard-fail CI immediately?
   (default: advisory for 1 baseline run, then freeze gates)

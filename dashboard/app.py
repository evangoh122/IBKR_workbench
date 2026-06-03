"""
dashboard/app.py
IBKR ETL — Streamlit + Plotly dashboard.

Sections:
  1. Stock Quotes — latest prices, OHLCV, spread analysis
  2. Price History — candlestick / line charts per ticker
  3. Options Chain — IV surface, Greeks heatmaps, chain table
  4. Transaction Cost Calculator — slippage model with toggles
  5. ETL Health — run log, row counts, last-seen timestamps
"""

import os
import sys
from pathlib import Path

# ── Path fix so imports resolve from project root ─────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import duckdb
from datetime import datetime, timezone, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st
from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from etl.slippage import calculate_costs, SlippageToggles

DB_PATH = os.getenv("DB_PATH", str(ROOT / "data" / "ibkr.duckdb"))

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IBKR Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme colours ─────────────────────────────────────────────────────────────
CLR_GREEN  = "#00c96f"
CLR_RED    = "#ff4b4b"
CLR_BLUE   = "#4b9dff"
CLR_GOLD   = "#f5c842"
CLR_BG     = "#0e1117"
CLR_CARD   = "#1c2130"
CLR_BORDER = "#2a3246"

st.markdown(f"""
<style>
  .metric-card {{
    background: {CLR_CARD};
    border: 1px solid {CLR_BORDER};
    border-radius: 10px;
    padding: 16px 20px;
    margin-bottom: 8px;
  }}
  .metric-label {{ font-size: 12px; color: #8899aa; text-transform: uppercase; letter-spacing: 1px; }}
  .metric-value {{ font-size: 28px; font-weight: 700; color: #e8eaf0; }}
  .metric-delta {{ font-size: 13px; margin-top: 4px; }}
  .up   {{ color: {CLR_GREEN}; }}
  .down {{ color: {CLR_RED}; }}
  section[data-testid="stSidebar"] {{ background: #131926; }}
</style>
""", unsafe_allow_html=True)


# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn():
    if not Path(DB_PATH).exists():
        return None
    return duckdb.connect(DB_PATH, read_only=True)


def q(sql: str, params=()) -> pd.DataFrame:
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(sql, list(params)).df()
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def no_db():
    st.warning("⚠️  Database not found. Run `python main.py` first to populate data.")
    st.code(f"DB path: {DB_PATH}")
    st.stop()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ IBKR ETL")
    st.markdown("---")
    page = st.radio("Navigate", [
        "💬 Chat",
        "📊 Stock Quotes",
        "📉 Price History",
        "📦 Polygon OHLCV",
        "🔗 Options Chain",
        "💸 Cost Calculator",
        "🩺 ETL Health",
    ])
    st.markdown("---")
    st.caption(f"DB: `{DB_PATH}`")
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 0 — Chat
# ══════════════════════════════════════════════════════════════════════════════
if page == "💬 Chat":
    st.title("💬 Chat with your data")
    
    chat_mode = st.radio("Chat Mode", ["Database (SQL)", "Knowledge Base (RAG)"], horizontal=True, 
                         help="Database mode generates SQL to query numerical data. RAG mode searches SEC filings and ticker descriptions.")
    
    provider = os.getenv("CHAT_PROVIDER", "deepseek").lower()
    st.caption(f"Ask anything about your stocks, options, EDGAR financials, or Polygon history. Powered by {provider.title()}.")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []   # list of {role, content}
    if "chat_results" not in st.session_state:
        st.session_state.chat_results = []   # list of result dicts (parallel to user turns)

    # Check API key
    import os as _os
    if not _os.getenv("DEEPSEEK_API_KEY") and not _os.getenv("OPENAI_API_KEY") and not _os.getenv("ANTHROPIC_API_KEY") and _os.getenv("CHAT_PROVIDER", "ollama") != "ollama":
        st.warning("⚠️  API key not set in .env for your chosen provider.")
        st.stop()

    from etl.chat_engine import chat as _chat
    from rag_engine import ask_rag

    # ── Render existing conversation ───────────────────────────────────────
    for i, msg in enumerate(st.session_state.chat_history):
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            # Show table result below assistant messages that have data
            if msg["role"] == "assistant" and i // 2 < len(st.session_state.chat_results):
                result = st.session_state.chat_results[i // 2]
                if result and result.get("type") == "table" and result.get("data") is not None:
                    with st.expander(f"📊 Query results ({len(result['data'])} rows)"):
                        if result.get("sql"):
                            st.code(result["sql"], language="sql")
                        st.dataframe(result["data"], use_container_width=True, hide_index=True)

    # ── Input ──────────────────────────────────────────────────────────────
    if prompt := st.chat_input("Ask about your data… e.g. 'Show AAPL OHLCV for last 30 days'"):
        # Add user message
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Call AI based on mode
        with st.chat_message("assistant"):
            with st.spinner("Thinking…"):
                if chat_mode == "Database (SQL)":
                    result = _chat(
                        question=prompt,
                        history=st.session_state.chat_history[:-1],
                    )
                    answer = result["answer"]
                else:
                    answer = ask_rag(prompt)
                    result = {"type": "text", "sql": None, "data": None, "answer": answer}

            st.markdown(answer)

            if result.get("type") == "table" and result.get("data") is not None:
                with st.expander(f"📊 Query results ({len(result['data'])} rows)"):
                    if result.get("sql"):
                        st.code(result["sql"], language="sql")
                    st.dataframe(result["data"], use_container_width=True, hide_index=True)
            elif result.get("type") == "error":
                if result.get("sql"):
                    st.code(result["sql"], language="sql")

        st.session_state.chat_history.append({"role": "assistant", "content": answer})
        st.session_state.chat_results.append(result)

    # ── Example prompts ────────────────────────────────────────────────────
    if not st.session_state.chat_history:
        st.markdown("**Try asking:**")
        examples = [
            "Show me AAPL closing prices for the last 30 days",
            "Which 10 tickers have the highest average volume in polygon_bars?",
            "What was NVDA's revenue for the last 4 quarters?",
            "Show the latest ETL run status for each job type",
            "Which tickers have the widest bid-ask spreads right now?",
        ]
        cols = st.columns(len(examples))
        for col, ex in zip(cols, examples):
            with col:
                if st.button(ex, use_container_width=True):
                    st.session_state.chat_history.append({"role": "user", "content": ex})
                    st.rerun()

    if st.button("🗑️ Clear chat", key="clear_chat"):
        st.session_state.chat_history = []
        st.session_state.chat_results = []
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Stock Quotes
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📊 Stock Quotes":
    st.title("📊 Stock Quotes")

    if get_conn() is None:
        no_db()

    latest = q("""
        SELECT s.*
        FROM stock_quotes s
        INNER JOIN (
            SELECT ticker, MAX(ts) AS max_ts FROM stock_quotes GROUP BY ticker
        ) l ON s.ticker=l.ticker AND s.ts=l.max_ts
        ORDER BY s.ticker
    """)

    if latest.empty:
        st.info("No stock data yet. Run the ETL first.")
        st.stop()

    # ── KPI cards ─────────────────────────────────────────────────────────
    st.subheader("Latest Prices")
    cols = st.columns(min(len(latest), 5))
    for i, row in latest.iterrows():
        spread = (row.get("ask", 0) or 0) - (row.get("bid", 0) or 0)
        chg    = ((row.get("last", 0) or 0) - (row.get("close", 0) or 1)) / (row.get("close", 1) or 1) * 100
        cls    = "up" if chg >= 0 else "down"
        arrow  = "▲" if chg >= 0 else "▼"
        with cols[i % 5]:
            st.markdown(f"""
            <div class="metric-card">
              <div class="metric-label">{row['ticker']}</div>
              <div class="metric-value">${row.get('last', 0) or 0:.2f}</div>
              <div class="metric-delta {cls}">{arrow} {abs(chg):.2f}% vs close</div>
              <div style="color:#8899aa;font-size:11px;margin-top:4px">
                Spread: ${spread:.3f} &nbsp;|&nbsp; Vol: {int(row.get('volume',0) or 0):,}
              </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Spread comparison bar chart ────────────────────────────────────────
    st.subheader("Bid-Ask Spread Comparison")
    latest["spread"]     = (latest["ask"] - latest["bid"]).fillna(0)
    latest["spread_bps"] = (latest["spread"] / latest["last"].replace(0, float("nan")) * 10000).fillna(0)

    fig_spread = go.Figure()
    fig_spread.add_trace(go.Bar(
        x=latest["ticker"],
        y=latest["spread_bps"],
        marker_color=CLR_BLUE,
        text=latest["spread_bps"].round(1).astype(str) + " bps",
        textposition="outside",
        name="Spread (bps)",
    ))
    fig_spread.update_layout(
        template="plotly_dark", plot_bgcolor=CLR_BG, paper_bgcolor=CLR_BG,
        yaxis_title="Basis Points", height=350, margin=dict(t=20, b=20),
    )
    st.plotly_chart(fig_spread, use_container_width=True)

    # ── Volume bar chart ───────────────────────────────────────────────────
    st.subheader("Volume")
    fig_vol = go.Figure(go.Bar(
        x=latest["ticker"],
        y=latest["volume"].fillna(0),
        marker_color=CLR_GOLD,
        text=(latest["volume"].fillna(0) / 1e6).round(1).astype(str) + "M",
        textposition="outside",
    ))
    fig_vol.update_layout(
        template="plotly_dark", plot_bgcolor=CLR_BG, paper_bgcolor=CLR_BG,
        yaxis_title="Shares", height=350, margin=dict(t=20, b=20),
    )
    st.plotly_chart(fig_vol, use_container_width=True)

    # ── Full table ─────────────────────────────────────────────────────────
    st.subheader("Full Quote Table")
    display = latest[["ticker","ts","bid","ask","last","close","open","high","low","volume","spread","spread_bps"]].copy()
    display.columns = ["Ticker","Timestamp","Bid","Ask","Last","Close","Open","High","Low","Volume","Spread $","Spread bps"]
    st.dataframe(display, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Price History
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📉 Price History":
    st.title("📉 Price History")

    if get_conn() is None:
        no_db()

    tickers = q("SELECT DISTINCT ticker FROM stock_quotes ORDER BY ticker")["ticker"].tolist()
    if not tickers:
        st.info("No data yet.")
        st.stop()

    c1, c2, c3 = st.columns(3)
    with c1:
        ticker = st.selectbox("Ticker", tickers)
    with c2:
        hours = st.selectbox("Window", [1, 4, 8, 24, 48, 168], index=3,
                             format_func=lambda h: f"{h}h" if h < 24 else f"{h//24}d")
    with c3:
        chart_type = st.selectbox("Chart type", ["Candlestick", "Line (last)", "OHLC"])

    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    hist  = q("""
        SELECT ts, open, high, low, last AS close, bid, ask, volume
        FROM stock_quotes WHERE ticker=? AND ts>=? ORDER BY ts
    """, (ticker, since))

    if hist.empty:
        st.info(f"No history for {ticker} in the last {hours}h.")
        st.stop()

    hist["ts"] = pd.to_datetime(hist["ts"])

    # ── Price chart ────────────────────────────────────────────────────────
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.7, 0.3], vertical_spacing=0.03)

    if chart_type == "Candlestick":
        fig.add_trace(go.Candlestick(
            x=hist["ts"], open=hist["open"], high=hist["high"],
            low=hist["low"], close=hist["close"],
            increasing_line_color=CLR_GREEN, decreasing_line_color=CLR_RED,
            name=ticker,
        ), row=1, col=1)
    elif chart_type == "OHLC":
        fig.add_trace(go.Ohlc(
            x=hist["ts"], open=hist["open"], high=hist["high"],
            low=hist["low"], close=hist["close"],
            increasing_line_color=CLR_GREEN, decreasing_line_color=CLR_RED,
        ), row=1, col=1)
    else:
        fig.add_trace(go.Scatter(
            x=hist["ts"], y=hist["close"],
            line=dict(color=CLR_BLUE, width=2), name="Last",
        ), row=1, col=1)
        # Bid/ask band
        fig.add_trace(go.Scatter(
            x=pd.concat([hist["ts"], hist["ts"].iloc[::-1]]),
            y=pd.concat([hist["ask"], hist["bid"].iloc[::-1]]),
            fill="toself", fillcolor="rgba(75,157,255,0.12)",
            line=dict(width=0), name="Bid-Ask band",
        ), row=1, col=1)

    # Volume bars
    colors = [CLR_GREEN if r["close"] >= (r["open"] or r["close"]) else CLR_RED
              for _, r in hist.iterrows()]
    fig.add_trace(go.Bar(
        x=hist["ts"], y=hist["volume"].fillna(0),
        marker_color=colors, name="Volume", opacity=0.7,
    ), row=2, col=1)

    fig.update_layout(
        template="plotly_dark", plot_bgcolor=CLR_BG, paper_bgcolor=CLR_BG,
        height=600, margin=dict(t=20, b=20),
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="Volume",    row=2, col=1)
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Polygon OHLCV
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📦 Polygon OHLCV":
    st.title("📦 Polygon OHLCV — 2-Year Daily Bars")

    if get_conn() is None:
        no_db()

    tickers = q("SELECT DISTINCT ticker FROM polygon_bars ORDER BY ticker")["ticker"].tolist()
    if not tickers:
        st.info("No polygon bars yet. Run `python main.py --job polygon-bars` first.")
        st.stop()

    c1, c2, c3 = st.columns(3)
    with c1:
        ticker = st.selectbox("Ticker", tickers)
    with c2:
        periods = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "2Y": 730}
        period  = st.selectbox("Period", list(periods.keys()), index=3)
    with c3:
        chart_type = st.selectbox("Chart", ["Candlestick", "OHLC", "Line"])

    since = (datetime.now(timezone.utc) - timedelta(days=periods[period])).date().isoformat()
    bars  = q("""
        SELECT ts, open, high, low, close, volume, vwap
        FROM polygon_bars
        WHERE ticker = ? AND ts >= ? AND timespan = 'day'
        ORDER BY ts
    """, (ticker, since))

    if bars.empty:
        st.info(f"No data for {ticker}. Try running the bars ETL first.")
        st.stop()

    bars["ts"] = pd.to_datetime(bars["ts"])

    # ── Price + Volume chart ───────────────────────────────────────────────
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.7, 0.3], vertical_spacing=0.03)

    if chart_type == "Candlestick":
        fig.add_trace(go.Candlestick(
            x=bars["ts"], open=bars["open"], high=bars["high"],
            low=bars["low"], close=bars["close"],
            increasing_line_color=CLR_GREEN, decreasing_line_color=CLR_RED,
            name=ticker,
        ), row=1, col=1)
    elif chart_type == "OHLC":
        fig.add_trace(go.Ohlc(
            x=bars["ts"], open=bars["open"], high=bars["high"],
            low=bars["low"], close=bars["close"],
            increasing_line_color=CLR_GREEN, decreasing_line_color=CLR_RED,
        ), row=1, col=1)
    else:
        fig.add_trace(go.Scatter(
            x=bars["ts"], y=bars["close"],
            line=dict(color=CLR_BLUE, width=1.5), name="Close",
        ), row=1, col=1)
        if bars["vwap"].notna().any():
            fig.add_trace(go.Scatter(
                x=bars["ts"], y=bars["vwap"],
                line=dict(color=CLR_GOLD, width=1, dash="dot"), name="VWAP",
            ), row=1, col=1)

    colors = [CLR_GREEN if c >= o else CLR_RED
              for c, o in zip(bars["close"].fillna(0), bars["open"].fillna(0))]
    fig.add_trace(go.Bar(
        x=bars["ts"], y=bars["volume"].fillna(0),
        marker_color=colors, name="Volume", opacity=0.6,
    ), row=2, col=1)

    fig.update_layout(
        template="plotly_dark", plot_bgcolor=CLR_BG, paper_bgcolor=CLR_BG,
        height=620, margin=dict(t=20, b=20),
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="Volume",    row=2, col=1)
    st.plotly_chart(fig, use_container_width=True)

    # ── Stats ──────────────────────────────────────────────────────────────
    st.markdown("---")
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("Bars",        f"{len(bars):,}")
    s2.metric("High",        f"${bars['high'].max():.2f}")
    s3.metric("Low",         f"${bars['low'].min():.2f}")
    s4.metric("Avg Volume",  f"{bars['volume'].mean()/1e6:.1f}M")
    ret = (bars["close"].iloc[-1] / bars["close"].iloc[0] - 1) * 100
    s5.metric("Period Return", f"{ret:+.1f}%")

    # ── Raw data table ─────────────────────────────────────────────────────
    with st.expander("Raw data"):
        st.dataframe(bars.sort_values("ts", ascending=False), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Options Chain
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔗 Options Chain":
    st.title("🔗 Options Chain")

    if get_conn() is None:
        no_db()

    tickers = q("SELECT DISTINCT ticker FROM option_quotes ORDER BY ticker")["ticker"].tolist()
    if not tickers:
        st.info("No options data yet. Run `python main.py --refresh-chain` first.")
        st.stop()

    c1, c2 = st.columns(2)
    with c1:
        ticker  = st.selectbox("Underlying", tickers)
    with c2:
        expiries = q("SELECT DISTINCT expiry FROM option_quotes WHERE ticker=? ORDER BY expiry",
                     (ticker,))["expiry"].tolist()
        expiry   = st.selectbox("Expiry", expiries)

    opts = q("""
        SELECT oq.*
        FROM option_quotes oq
        INNER JOIN (
            SELECT ticker,expiry,strike,right,MAX(ts) AS max_ts
            FROM option_quotes GROUP BY ticker,expiry,strike,right
        ) l ON oq.ticker=l.ticker AND oq.expiry=l.expiry
           AND oq.strike=l.strike AND oq.right=l.right AND oq.ts=l.max_ts
        WHERE oq.ticker=? AND oq.expiry=?
        ORDER BY oq.strike, oq.right
    """, (ticker, expiry))

    if opts.empty:
        st.info("No data for this expiry.")
        st.stop()

    calls = opts[opts["right"] == "C"].set_index("strike")
    puts  = opts[opts["right"] == "P"].set_index("strike")

    tab1, tab2, tab3, tab4 = st.tabs(["📋 Chain Table", "📈 IV Smile", "🔥 Greeks Heatmap", "📊 OI & Volume"])

    # ── Tab 1: Chain table ─────────────────────────────────────────────────
    with tab1:
        strikes = sorted(set(calls.index) | set(puts.index))
        rows = []
        for s in strikes:
            c = calls.loc[s] if s in calls.index else {}
            p = puts.loc[s]  if s in puts.index  else {}
            rows.append({
                "Strike":     s,
                "C Bid":      c.get("bid"),   "C Ask":   c.get("ask"),
                "C IV":       c.get("implied_vol"), "C Delta": c.get("delta"),
                "C OI":       c.get("open_interest"), "C Vol": c.get("volume"),
                "P Bid":      p.get("bid"),   "P Ask":   p.get("ask"),
                "P IV":       p.get("implied_vol"), "P Delta": p.get("delta"),
                "P OI":       p.get("open_interest"), "P Vol": p.get("volume"),
            })
        chain_df = pd.DataFrame(rows)

        # Highlight ATM row
        last_px = q("SELECT last FROM stock_quotes WHERE ticker=? ORDER BY ts DESC LIMIT 1",
                    (ticker,))
        if not last_px.empty:
            atm = float(last_px.iloc[0]["last"] or 0)
            st.caption(f"Underlying last: **${atm:.2f}**")

        def style_chain(df):
            return df.style.format({
                "C Bid":   "${:.2f}", "C Ask":   "${:.2f}",
                "P Bid":   "${:.2f}", "P Ask":   "${:.2f}",
                "C IV":    "{:.1%}",  "P IV":    "{:.1%}",
                "C Delta": "{:.3f}",  "P Delta": "{:.3f}",
            }, na_rep="—")

        st.dataframe(style_chain(chain_df), use_container_width=True, hide_index=True)

    # ── Tab 2: IV Smile ────────────────────────────────────────────────────
    with tab2:
        fig_iv = go.Figure()
        if not calls.empty and "implied_vol" in calls.columns:
            fig_iv.add_trace(go.Scatter(
                x=calls.index, y=calls["implied_vol"],
                mode="lines+markers", name="Calls IV",
                line=dict(color=CLR_GREEN, width=2),
            ))
        if not puts.empty and "implied_vol" in puts.columns:
            fig_iv.add_trace(go.Scatter(
                x=puts.index, y=puts["implied_vol"],
                mode="lines+markers", name="Puts IV",
                line=dict(color=CLR_RED, width=2),
            ))
        fig_iv.update_layout(
            template="plotly_dark", plot_bgcolor=CLR_BG, paper_bgcolor=CLR_BG,
            xaxis_title="Strike", yaxis_title="Implied Volatility",
            yaxis_tickformat=".1%", height=420, margin=dict(t=20, b=20),
        )
        st.plotly_chart(fig_iv, use_container_width=True)

    # ── Tab 3: Greeks Heatmap ──────────────────────────────────────────────
    with tab3:
        greek = st.selectbox("Greek", ["delta", "gamma", "theta", "vega"])
        c1, c2 = st.columns(2)

        for col, df, label, color in [
            (c1, calls, "Calls", "Greens"),
            (c2, puts,  "Puts",  "Reds"),
        ]:
            with col:
                st.markdown(f"**{label}**")
                if greek in df.columns and not df[greek].isna().all():
                    fig_hm = go.Figure(go.Bar(
                        x=df.index.tolist(),
                        y=df[greek].tolist(),
                        marker_color=df[greek],
                        marker_colorscale=color,
                        name=f"{label} {greek}",
                    ))
                    fig_hm.update_layout(
                        template="plotly_dark",
                        plot_bgcolor=CLR_BG, paper_bgcolor=CLR_BG,
                        height=350, margin=dict(t=10, b=10),
                        xaxis_title="Strike", yaxis_title=greek.capitalize(),
                    )
                    st.plotly_chart(fig_hm, use_container_width=True)
                else:
                    st.info(f"No {greek} data available.")

    # ── Tab 4: OI & Volume ─────────────────────────────────────────────────
    with tab4:
        fig_oi = make_subplots(rows=1, cols=2,
                               subplot_titles=["Open Interest by Strike", "Volume by Strike"])
        all_strikes = sorted(set(calls.index) | set(puts.index))

        fig_oi.add_trace(go.Bar(
            x=all_strikes,
            y=[calls.loc[s, "open_interest"] if s in calls.index else 0 for s in all_strikes],
            name="Call OI", marker_color=CLR_GREEN, opacity=0.8,
        ), row=1, col=1)
        fig_oi.add_trace(go.Bar(
            x=all_strikes,
            y=[puts.loc[s, "open_interest"] if s in puts.index else 0 for s in all_strikes],
            name="Put OI", marker_color=CLR_RED, opacity=0.8,
        ), row=1, col=1)
        fig_oi.add_trace(go.Bar(
            x=all_strikes,
            y=[calls.loc[s, "volume"] if s in calls.index else 0 for s in all_strikes],
            name="Call Vol", marker_color=CLR_BLUE, opacity=0.8,
        ), row=1, col=2)
        fig_oi.add_trace(go.Bar(
            x=all_strikes,
            y=[puts.loc[s, "volume"] if s in puts.index else 0 for s in all_strikes],
            name="Put Vol", marker_color=CLR_GOLD, opacity=0.8,
        ), row=1, col=2)

        fig_oi.update_layout(
            template="plotly_dark", plot_bgcolor=CLR_BG, paper_bgcolor=CLR_BG,
            height=400, barmode="group", margin=dict(t=40, b=20),
        )
        st.plotly_chart(fig_oi, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Transaction Cost Calculator
# ══════════════════════════════════════════════════════════════════════════════
elif page == "💸 Cost Calculator":
    st.title("💸 Transaction Cost Calculator")
    st.caption("Model round-trip slippage, commissions, and market impact for any trade.")

    # ── Toggles ────────────────────────────────────────────────────────────
    st.subheader("Cost Components")
    tc1, tc2, tc3 = st.columns(3)
    with tc1:
        use_spread = st.toggle("Bid-Ask Spread", value=True,
                               help="Half-spread paid on entry + exit")
    with tc2:
        use_commission = st.toggle("Commission (IBKR)", value=True,
                                   help="IBKR tiered commission + regulatory fees")
    with tc3:
        use_impact = st.toggle("Market Impact", value=True,
                               help="Square-root impact model (Almgren-Chriss simplified)")

    toggles = SlippageToggles(
        spread=use_spread,
        commission=use_commission,
        market_impact=use_impact,
    )

    st.markdown("---")

    # ── Trade inputs ───────────────────────────────────────────────────────
    st.subheader("Trade Parameters")
    asset_type = st.radio("Asset type", ["Stock", "Option"], horizontal=True)

    col1, col2 = st.columns(2)
    with col1:
        ticker   = st.text_input("Ticker", value="AAPL")
        price    = st.number_input("Mid price ($)", min_value=0.01, value=182.50, step=0.01)
        bid      = st.number_input("Bid ($)", min_value=0.00, value=182.45, step=0.01)
        ask      = st.number_input("Ask ($)", min_value=0.00, value=182.55, step=0.01)

    with col2:
        if asset_type == "Stock":
            quantity = st.number_input("Shares", min_value=1, value=1000, step=100)
            adv      = st.number_input("Avg Daily Volume (shares)", min_value=1,
                                        value=50_000_000, step=1_000_000,
                                        help="Used for market impact calculation")
            monthly  = st.number_input("Monthly shares traded (IBKR tier)",
                                        min_value=0, value=0, step=10_000,
                                        help="Affects per-share commission rate")
            mult     = 1.0
        else:
            quantity = st.number_input("Contracts", min_value=1, value=10, step=1)
            mult     = st.number_input("Multiplier", min_value=1, value=100, step=1)
            adv      = st.number_input("Underlying ADV (shares)", min_value=1,
                                        value=50_000_000, step=1_000_000)
            monthly  = 0

    # ── Calculate ──────────────────────────────────────────────────────────
    cb = calculate_costs(
        ticker=ticker,
        asset_type=asset_type.lower(),
        quantity=quantity,
        price=price,
        bid=bid,
        ask=ask,
        multiplier=float(mult),
        adv=adv,
        toggles=toggles,
        monthly_shares=monthly,
    )

    st.markdown("---")
    st.subheader("Results")

    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Total Round-Trip Cost", f"${cb.total_cost:,.2f}")
    r2.metric("Cost (bps)",            f"{cb.cost_bps:.1f} bps")
    r3.metric("Notional Value",        f"${cb.notional:,.2f}")
    r4.metric("Cost / Notional",       f"{cb.total_cost/cb.notional*100:.3f}%" if cb.notional else "—")

    # ── Waterfall breakdown ────────────────────────────────────────────────
    components = []
    values     = []
    if use_spread:
        components.append("Bid-Ask Spread"); values.append(cb.spread_cost)
    if use_commission:
        components.append("Commission");     values.append(cb.commission_cost)
    if use_impact:
        components.append("Market Impact");  values.append(cb.market_impact_cost)
    components.append("Total"); values.append(cb.total_cost)

    measure = ["relative"] * (len(components) - 1) + ["total"]
    fig_wf = go.Figure(go.Waterfall(
        name="Cost breakdown", measure=measure,
        x=components, y=values,
        connector=dict(line=dict(color="rgb(63,63,63)")),
        increasing=dict(marker_color=CLR_RED),
        totals=dict(marker_color=CLR_BLUE),
        text=[f"${v:,.2f}" for v in values],
        textposition="outside",
    ))
    fig_wf.update_layout(
        template="plotly_dark", plot_bgcolor=CLR_BG, paper_bgcolor=CLR_BG,
        title="Cost Breakdown (Round-Trip $)",
        yaxis_title="USD", height=380, margin=dict(t=50, b=20),
    )
    st.plotly_chart(fig_wf, use_container_width=True)

    # ── Sensitivity: cost vs quantity ─────────────────────────────────────
    st.subheader("Sensitivity: Cost vs Quantity")
    if asset_type == "Stock":
        qtys = [100, 500, 1000, 2500, 5000, 10000, 25000, 50000]
        q_label = "Shares"
    else:
        qtys = [1, 5, 10, 25, 50, 100, 250, 500]
        q_label = "Contracts"

    sens_rows = []
    for qty in qtys:
        c = calculate_costs(
            ticker=ticker, asset_type=asset_type.lower(),
            quantity=qty, price=price, bid=bid, ask=ask,
            multiplier=float(mult), adv=adv, toggles=toggles,
            monthly_shares=monthly,
        )
        sens_rows.append({
            q_label:    qty,
            "Spread":   c.spread_cost,
            "Commission": c.commission_cost,
            "Impact":   c.market_impact_cost,
            "Total":    c.total_cost,
            "bps":      c.cost_bps,
        })
    sens_df = pd.DataFrame(sens_rows)

    fig_sens = go.Figure()
    for col, color in [("Spread", CLR_GREEN), ("Commission", CLR_GOLD), ("Impact", CLR_RED)]:
        if col in sens_df.columns and sens_df[col].sum() > 0:
            fig_sens.add_trace(go.Scatter(
                x=sens_df[q_label], y=sens_df[col],
                stackgroup="one", name=col,
                line=dict(color=color),
                fillcolor=color.replace(")", ",0.4)").replace("rgb", "rgba") if "rgb" in color
                           else color + "66",
            ))
    fig_sens.update_layout(
        template="plotly_dark", plot_bgcolor=CLR_BG, paper_bgcolor=CLR_BG,
        xaxis_title=q_label, yaxis_title="Cost ($)",
        height=380, margin=dict(t=20, b=20),
    )
    st.plotly_chart(fig_sens, use_container_width=True)

    # Raw table
    with st.expander("Raw sensitivity table"):
        st.dataframe(sens_df.style.format({
            "Spread": "${:,.2f}", "Commission": "${:,.2f}",
            "Impact": "${:,.2f}", "Total": "${:,.2f}", "bps": "{:.1f}",
        }), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — ETL Health
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🩺 ETL Health":
    st.title("🩺 ETL Health")

    if get_conn() is None:
        no_db()

    # ── Row counts ─────────────────────────────────────────────────────────
    st.subheader("Table Row Counts")
    counts = {}
    for tbl in ["stock_quotes", "option_quotes", "option_chains", "etl_runs"]:
        try:
            n = q(f"SELECT COUNT(*) AS n FROM {tbl}").iloc[0]["n"]
        except Exception:
            n = 0
        counts[tbl] = n

    c1, c2, c3, c4 = st.columns(4)
    for col, (tbl, cnt) in zip([c1, c2, c3, c4], counts.items()):
        col.metric(tbl.replace("_", " ").title(), f"{int(cnt):,}")

    st.markdown("---")

    # ── Run log timeline ───────────────────────────────────────────────────
    st.subheader("ETL Run Log")
    runs = q("SELECT * FROM etl_runs ORDER BY id DESC LIMIT 100")

    if runs.empty:
        st.info("No ETL runs logged yet.")
    else:
        # Status summary
        ok_count  = (runs["status"] == "ok").sum()
        err_count = (runs["status"] == "error").sum()
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Runs",    len(runs))
        m2.metric("✅ Successful", int(ok_count))
        m3.metric("❌ Errors",     int(err_count))

        # Timeline chart
        runs["started_at"] = pd.to_datetime(runs["started_at"])
        fig_runs = px.scatter(
            runs, x="started_at", y="run_type",
            color="status",
            color_discrete_map={"ok": CLR_GREEN, "error": CLR_RED},
            size="rows_written",
            size_max=20,
            hover_data=["message", "rows_written"],
            template="plotly_dark",
        )
        fig_runs.update_layout(
            plot_bgcolor=CLR_BG, paper_bgcolor=CLR_BG,
            height=350, margin=dict(t=20, b=20),
            xaxis_title="Time", yaxis_title="Job Type",
        )
        st.plotly_chart(fig_runs, use_container_width=True)

        # Table
        st.dataframe(
            runs[["run_type","status","rows_written","started_at","message"]],
            use_container_width=True, hide_index=True,
        )

    # ── Last-seen per ticker ───────────────────────────────────────────────
    st.subheader("Last Data Received Per Ticker")
    freshness = q("""
        SELECT ticker, MAX(ts) AS last_seen,
               ROUND(DATE_DIFF('hour', MAX(ts)::TIMESTAMP, NOW()), 1) AS hours_ago
        FROM stock_quotes GROUP BY ticker ORDER BY ticker
    """)
    if not freshness.empty:
        def color_freshness(val):
            try:
                v = float(val)
                if v < 1:   return "color: #00c96f"
                if v < 4:   return "color: #f5c842"
                return "color: #ff4b4b"
            except Exception:
                return ""
        st.dataframe(
            freshness.style.applymap(color_freshness, subset=["hours_ago"]),
            use_container_width=True, hide_index=True,
        )

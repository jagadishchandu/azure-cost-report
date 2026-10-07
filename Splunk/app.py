"""
Splunk Kubernetes Log Fetcher (Streamlit).

Run:   streamlit run app.py

Flow:  Connect -> Cluster (only clusters your Splunk role can see) -> Namespaces
       -> Time range and pods -> Fetch -> explore, filter and download the logs.

Efficiency notes
  * Dropdown lookups use tstats (very cheap) and are cached per session.
  * Only the indexes the chosen cluster lives in are searched, not index=*.
  * The log search keeps just the needed fields and stops early once it has enough events.
  * Result pages are downloaded in parallel.
  * After a fetch, the table, statistics and charts are computed ONCE and stored; clicking
    around (filter box, tabs, sliders) never rebuilds them, and download files are only
    generated when you click the download button.
"""

import html
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from splunk_client import (SplunkClient, SplunkError, build_log_spl, discover_values,
                           filter_clause)

HERE = os.path.dirname(os.path.abspath(__file__))

DEFAULTS = {
    "host": "",
    "port": 8089,
    "scheme": "https",
    "verify_ssl": True,
    "ca_bundle": "",
    "default_indexes": [],
    "cluster_field": "cluster_name",
    "namespace_field": "namespace",
    "pod_field": "pod",
    "use_tstats": True,
    "discovery_lookback_hours": 24,
    "max_events": 50000,
    "hard_max_events": 500000,
    "search_timeout_seconds": 600,
    "parallel_pages": 4,
}

STEPS = ["Connect", "Cluster", "Namespaces", "Time & pods", "Fetch"]
PRESETS = {"Last 15 min": 15, "Last 1 hour": 60, "Last 6 hours": 360, "Last 24 hours": 1440}
TABLE_ROWS = 2000


def load_config() -> dict:
    cfg = dict(DEFAULTS)
    path = os.environ.get("SPLUNK_TOOL_CONFIG") or os.path.join(HERE, "splunk_config.json")
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8-sig") as f:
            cfg.update({k: v for k, v in json.load(f).items() if not k.startswith("_comment")})
    return cfg


def cached(key, fn, ttl=300):
    """Tiny per-session cache so dropdowns do not re-run Splunk searches on every click."""
    store = st.session_state.setdefault("_cache", {})
    hit = store.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    value = fn()
    store[key] = (time.time(), value)
    return value


def safe_name(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("_") or "logs"


def human_duration(delta: timedelta) -> str:
    minutes = int(delta.total_seconds() // 60)
    days, rem = divmod(minutes, 1440)
    hours, mins = divmod(rem, 60)
    parts = [f"{days}d" if days else "", f"{hours}h" if hours else "", f"{mins}m" if mins or not (days or hours) else ""]
    return " ".join(p for p in parts if p)


CSS = """
<style>
.block-container { padding-top: 1.6rem; max-width: 1180px; }
.hero h1 { margin: 0; font-size: 1.9rem; letter-spacing: -0.01em; }
.hero p { margin: .15rem 0 0 0; opacity: .7; }
.steps { display: flex; gap: 8px; margin: 1.1rem 0 1.3rem 0; flex-wrap: wrap; }
.step { flex: 1 1 130px; padding: 9px 12px; border-radius: 10px; font-size: .85rem;
        border: 1px solid rgba(128,128,128,.35); display: flex; align-items: center; gap: 9px; }
.step .dot { width: 22px; height: 22px; border-radius: 50%; display: inline-flex; align-items: center;
             justify-content: center; font-size: .75rem; font-weight: 700;
             background: rgba(128,128,128,.25); flex: 0 0 22px; }
.step.done { border-color: var(--primary-color, #3A7D0A); }
.step.done .dot { background: var(--primary-color, #3A7D0A); color: #fff; }
.step.active { border-color: var(--primary-color, #3A7D0A); box-shadow: 0 0 0 2px rgba(58,125,10,.25); font-weight: 600; }
.step.active .dot { background: var(--primary-color, #3A7D0A); color: #fff; }
.step.todo { opacity: .55; }
.kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 10px; margin: .4rem 0 1rem 0; }
.kpi { border: 1px solid rgba(128,128,128,.3); border-radius: 12px; padding: 11px 14px;
       background: var(--secondary-background-color, rgba(128,128,128,.08)); }
.kpi .label { font-size: .72rem; text-transform: uppercase; letter-spacing: .06em; opacity: .65; }
.kpi .value { font-size: 1.12rem; font-weight: 650; margin-top: 2px; word-break: break-word; }
.kpi .sub { font-size: .78rem; opacity: .65; margin-top: 1px; }
.pill { display: inline-block; padding: 3px 11px; border-radius: 999px; font-size: .8rem; font-weight: 600;
        background: rgba(58,125,10,.16); color: var(--primary-color, #3A7D0A); }
.card-title { font-size: 1.08rem; font-weight: 650; margin: 0 0 .35rem 0; }
.card-title .num { display: inline-flex; width: 24px; height: 24px; border-radius: 50%; margin-right: 8px;
                   align-items: center; justify-content: center; font-size: .8rem;
                   background: var(--primary-color, #3A7D0A); color: #fff; }
</style>
"""


def kpi_html(items) -> str:
    cells = "".join(
        f"<div class='kpi'><div class='label'>{html.escape(label)}</div>"
        f"<div class='value'>{html.escape(str(value))}</div>"
        + (f"<div class='sub'>{html.escape(str(sub))}</div>" if sub else "") + "</div>"
        for label, value, sub in items)
    return f"<div class='kpis'>{cells}</div>"


def card_title(num: int, text: str):
    st.markdown(f"<div class='card-title'><span class='num'>{num}</span>{html.escape(text)}</div>",
                unsafe_allow_html=True)


cfg = load_config()
CF, NF, PF = cfg["cluster_field"], cfg["namespace_field"], cfg["pod_field"]
st.set_page_config(page_title="Splunk Kubernetes Log Fetcher", page_icon=":mag:", layout="wide")
st.markdown(CSS, unsafe_allow_html=True)
st.markdown("<div class='hero'><h1>Splunk Kubernetes Log Fetcher</h1>"
            "<p>Pick a cluster, its namespaces and pods, set a start time, and download the logs.</p></div>",
            unsafe_allow_html=True)
stepper = st.empty()


def show_steps(done: int):
    """done = number of finished steps (0-5). The next one is highlighted."""
    cells = []
    for i, name in enumerate(STEPS):
        cls = "done" if i < done else "active" if i == done else "todo"
        mark = "&#10003;" if i < done else str(i + 1)
        cells.append(f"<div class='step {cls}'><span class='dot'>{mark}</span>{html.escape(name)}</div>")
    stepper.markdown(f"<div class='steps'>{''.join(cells)}</div>", unsafe_allow_html=True)


def log_lines(frame: pd.DataFrame) -> pd.Series:
    """One text line per event, built column-wise (fast) instead of row by row."""
    return (frame["_time"] + "  " + frame[CF] + "/" + frame[NF] + "/" + frame[PF] + "  " + frame["_raw"])


def prepare_result(rows, cluster, start_dt, end_dt, max_events, order) -> dict:
    """Turn Splunk's rows into a DataFrame and compute everything the results page shows,
    exactly once. The raw row list is dropped afterwards to keep memory low."""
    df = pd.DataFrame(rows).reindex(columns=["_time", CF, NF, PF, "_raw"]).fillna("").astype(str)
    df["_t"] = pd.to_datetime(df["_time"], utc=True, errors="coerce", format="ISO8601")
    df = df.sort_values("_t", kind="stable", na_position="last").reset_index(drop=True)   # oldest first
    times = df.pop("_t")
    valid = times.dropna()
    first_t, last_t = (valid.min(), valid.max()) if len(valid) else (pd.NaT, pd.NaT)
    span = (last_t - first_t) if len(valid) else timedelta(0)
    freq = "1min" if span <= timedelta(hours=2) else "5min" if span <= timedelta(hours=12) \
        else "15min" if span <= timedelta(days=2) else "1h"
    return {
        "df": df, "cluster": cluster, "start": start_dt, "end": end_dt,
        "truncated": len(df) >= max_events, "max": max_events, "order": order,
        "first": first_t, "last": last_t, "freq": freq,
        "ns_n": int(df[NF].nunique()), "pod_n": int(df[PF].nunique()),
        "pod_counts": df[PF].value_counts().head(15),
        "series": valid.to_frame("t").assign(n=1).set_index("t")["n"].resample(freq).sum() if len(valid) else None,
    }


# ---------------------------------------------------------------------------
# Sidebar: connection
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### Splunk connection")
    connected = "client" in st.session_state
    if connected:
        st.markdown(f"<span class='pill'>Connected as {html.escape(st.session_state['user'])}</span>",
                    unsafe_allow_html=True)
        st.caption(st.session_state["conn"].split("|")[0])
        if st.button("Disconnect", width="stretch"):
            for k in ("client", "user", "conn", "result", "_cache"):
                st.session_state.pop(k, None)
            st.rerun()
    else:
        host = st.text_input("Splunk host", value=cfg["host"], placeholder="splunk.yourcompany.com")
        port = st.number_input("Management port", value=int(cfg["port"]), step=1,
                               help="The REST API port (default 8089), not the web port 8000.")
        method = st.radio("Sign in with", ["API token", "Username and password"], horizontal=True)
        token = username = password = ""
        if method == "API token":
            token = st.text_input("Token", type="password", value=os.environ.get("SPLUNK_TOKEN", ""),
                                  help="Splunk: Settings > Tokens. Kept in memory only, never saved.")
        else:
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
        if st.button("Connect", type="primary", width="stretch"):
            if not host:
                st.error("Enter the Splunk host.")
            else:
                try:
                    with st.spinner("Connecting..."):
                        verify = cfg["ca_bundle"] or bool(cfg["verify_ssl"])
                        client = SplunkClient(f"{cfg['scheme']}://{host}:{int(port)}",
                                              token=token or None, username=username or None,
                                              password=password or None, verify=verify)
                        client.workers = int(cfg["parallel_pages"])
                        user = client.current_user()
                    st.session_state.update(client=client, user=user,
                                            conn=f"{host}:{int(port)}|{user}", result=None, _cache={})
                    st.rerun()
                except SplunkError as exc:
                    st.error(str(exc))
    st.divider()
    st.caption("Your token or password stays in this browser session and is never written to disk. "
               "You only see clusters, namespaces and pods that your Splunk role is allowed to search.")

if "client" not in st.session_state:
    show_steps(0)
    with st.container(border=True):
        st.markdown("#### Welcome")
        st.write("Connect to Splunk using the panel on the left. Once connected you will be able to:")
        st.markdown("- choose from the **clusters you have access to**\n"
                    "- see **every namespace** in that cluster\n"
                    "- pick the **pods** and a **start date and time**\n"
                    "- fetch the logs and **download** them as CSV, JSON lines or plain text")
    st.stop()

client: SplunkClient = st.session_state["client"]
conn: str = st.session_state["conn"]

# ---------------------------------------------------------------------------
# Step 2: cluster
# ---------------------------------------------------------------------------
show_steps(1)
with st.container(border=True):
    card_title(2, "Choose a cluster")
    try:
        all_indexes = cached(("indexes", conn), client.list_indexes)
    except SplunkError as exc:
        all_indexes = []
        st.warning(f"Could not list indexes ({exc}). Searching all indexes you can access.")

    with st.expander("Search scope (indexes and look-back)"):
        s1, s2 = st.columns([3, 1])
        defaults = [i for i in cfg["default_indexes"] if i in all_indexes]
        chosen = s1.multiselect("Indexes (empty = every index you can access)", all_indexes, default=defaults)
        lookback = s2.number_input("Look-back (hours)", min_value=1, max_value=24 * 90,
                                   value=int(cfg["discovery_lookback_hours"]),
                                   help="How far back to look when listing clusters and namespaces.")
    indexes = tuple(chosen)

    try:
        with st.spinner("Finding the clusters you can access..."):
            clusters = cached(("clusters", conn, indexes, lookback), lambda: discover_values(
                client, CF, indexes, "", f"-{int(lookback)}h", "now", cfg["use_tstats"]))
    except SplunkError as exc:
        st.error(str(exc))
        st.stop()
    if not clusters:
        st.warning(f"No clusters found in the last {int(lookback)} hours. Check the index selection, the "
                   f"look-back, and that '{CF}' is the right field name (see splunk_config.json).")
        st.stop()
    cluster = st.selectbox(f"Cluster ({len(clusters)} available to you)", clusters)

# Searching only the indexes this cluster really lives in is far cheaper for Splunk than
# searching index=* (every index you can access). Found with one quick tstats lookup.
if indexes:
    search_indexes = indexes
else:
    try:
        search_indexes = tuple(cached(("cluster_idx", conn, lookback, cluster), lambda: discover_values(
            client, "index", (), filter_clause(CF, cluster), f"-{int(lookback)}h", "now", cfg["use_tstats"])))
    except SplunkError:
        search_indexes = ()

# ---------------------------------------------------------------------------
# Step 3: namespaces
# ---------------------------------------------------------------------------
show_steps(2)
with st.container(border=True):
    card_title(3, f"Namespaces in {cluster}")
    try:
        with st.spinner("Loading namespaces..."):
            namespaces_all = cached(("ns", conn, search_indexes, lookback, cluster), lambda: discover_values(
                client, NF, search_indexes, filter_clause(CF, cluster), f"-{int(lookback)}h", "now",
                cfg["use_tstats"]))
    except SplunkError as exc:
        st.error(str(exc))
        st.stop()
    if not namespaces_all:
        st.warning(f"No namespaces found for '{cluster}' in the last {int(lookback)} hours.")
        st.stop()
    use_all_ns = st.checkbox(f"All namespaces ({len(namespaces_all)})", value=True,
                             key=f"allns|{conn}|{indexes}|{lookback}|{cluster}")
    if use_all_ns:
        namespaces = namespaces_all
        st.caption(", ".join(namespaces_all[:12]) + (" ..." if len(namespaces_all) > 12 else ""))
    else:
        namespaces = st.multiselect("Select namespaces (type to search)", namespaces_all,
                                    key=f"ns|{conn}|{indexes}|{lookback}|{cluster}")
        if not namespaces:
            st.info("Select at least one namespace.")
            st.stop()
ns_filter = namespaces if len(namespaces) < len(namespaces_all) else None   # all selected = no filter

# ---------------------------------------------------------------------------
# Step 4: time range and pods
# ---------------------------------------------------------------------------
show_steps(3)
with st.container(border=True):
    card_title(4, "Time range and pods")
    preset = st.radio("Time range", list(PRESETS) + ["Custom"], index=1, horizontal=True)
    now = datetime.now().astimezone().replace(second=0, microsecond=0)
    if preset == "Custom":
        tz_choice = st.radio("Time zone", ["My local time", "UTC"], horizontal=True)
        default_start = now - timedelta(hours=1)
        t1, t2, t3, t4 = st.columns(4)
        start_date = t1.date_input("Start date", value=default_start.date())
        start_time = t2.time_input("Start time", value=default_start.time())
        end_date = t3.date_input("End date", value=now.date())
        end_time = t4.time_input("End time", value=now.time())

        def to_datetime(d, t):
            naive = datetime.combine(d, t)
            return naive.replace(tzinfo=timezone.utc) if tz_choice == "UTC" else naive.astimezone()

        start_dt, end_dt = to_datetime(start_date, start_time), to_datetime(end_date, end_time)
    else:
        end_dt, start_dt = now, now - timedelta(minutes=PRESETS[preset])
    if start_dt >= end_dt:
        st.error("The start must be before the end.")
        st.stop()
    earliest, latest = int(start_dt.timestamp()), int(end_dt.timestamp())
    st.caption(f"{start_dt:%Y-%m-%d %H:%M} to {end_dt:%Y-%m-%d %H:%M} ({human_duration(end_dt - start_dt)})")

    st.divider()
    all_pods = st.checkbox("All pods in the selected namespaces", value=True)
    pods = None
    if not all_pods:
        pod_filter = filter_clause(CF, cluster, NF, ns_filter)
        try:
            with st.spinner("Finding pods that logged in this time range..."):
                # Cache key uses 10-minute buckets so "Last 1 hour" does not re-search every minute.
                pods_found = cached(("pods", conn, search_indexes, cluster, tuple(ns_filter or ()),
                                     earliest // 600, latest // 600),
                                    lambda: discover_values(client, PF, search_indexes, pod_filter, earliest,
                                                            latest, cfg["use_tstats"]))
        except SplunkError as exc:
            st.error(str(exc))
            st.stop()
        if not pods_found:
            st.warning("No pods logged anything in this time range. Try a wider range.")
            st.stop()
        pods = st.multiselect(f"Pods with logs in this range ({len(pods_found)} found, type to search)",
                              pods_found)
        if not pods:
            st.info("Select at least one pod, or tick 'All pods'.")
            st.stop()

# ---------------------------------------------------------------------------
# Step 5: summary and fetch
# ---------------------------------------------------------------------------
show_steps(4)
with st.container(border=True):
    card_title(5, "Review and fetch")
    st.markdown(kpi_html([
        ("Cluster", cluster, None),
        ("Namespaces", "All" if ns_filter is None else len(ns_filter),
         f"{len(namespaces_all)} in cluster" if ns_filter is None else f"of {len(namespaces_all)}"),
        ("Pods", "All" if pods is None else len(pods), None),
        ("From", f"{start_dt:%b %d, %H:%M}", f"to {end_dt:%b %d, %H:%M}"),
    ]), unsafe_allow_html=True)

    hard_max = int(cfg["hard_max_events"])
    with st.expander("Options and the search that will run"):
        max_events = st.number_input("Maximum events to fetch", min_value=100, max_value=hard_max,
                                     value=min(int(cfg["max_events"]), hard_max), step=1000,
                                     help="Protects Splunk and your browser.")
        order_label = st.radio(
            "If there are more events than the maximum, keep the",
            ["Newest events (fast)", "Earliest events (slower)"], horizontal=True,
            help="Newest lets Splunk stop searching as soon as it has enough events. Earliest has to sort "
                 "every matching event first, which is much slower on big ranges. If the range holds no "
                 "more events than the maximum, both give exactly the same result.")
        order = "earliest" if order_label.startswith("Earliest") else "newest"
        spl = build_log_spl(search_indexes, CF, cluster, NF, ns_filter, PF, pods, int(max_events), order)
        st.code(spl, language="text")
        st.caption(f"Time range sent to Splunk (epoch seconds): {earliest} to {latest}. "
                   + (f"Searching {len(search_indexes)} index(es): {', '.join(search_indexes)}."
                      if search_indexes else "Searching every index you can access."))

    if st.button("Fetch logs", type="primary", width="stretch"):
        bar = st.progress(0.0, text="Starting the search...")

        def on_progress(done, events, state):
            bar.progress(min(max(done, 0.0), 1.0),
                         text=f"Splunk is searching ({state.title()}): {events:,} events matched so far")

        try:
            started = time.time()
            rows = client.run_search(spl, earliest, latest, max_results=int(max_events),
                                     on_progress=on_progress, timeout=int(cfg["search_timeout_seconds"]))
            bar.progress(1.0, text=f"Preparing {len(rows):,} events...")
            result = prepare_result(rows, cluster, start_dt, end_dt, int(max_events), order)
            result["seconds"] = time.time() - started
            st.session_state["result"] = result
            del rows
            bar.empty()
        except SplunkError as exc:
            bar.empty()
            st.session_state["result"] = None
            st.error(str(exc))

# ---------------------------------------------------------------------------
# Results (everything shown here was computed once, in prepare_result)
# ---------------------------------------------------------------------------
res = st.session_state.get("result")
if res:
    show_steps(5)
    df: pd.DataFrame = res["df"]
    st.markdown("### Results")
    if df.empty:
        st.info("No events matched this cluster, namespace, pod and time range.")
        st.stop()
    if res["truncated"]:
        kept = "newest" if res["order"] == "newest" else "earliest"
        st.warning(f"Stopped at the maximum of {res['max']:,} events (kept the {kept} ones), so other logs in "
                   "this range were not fetched. Narrow the time range or the pods, or raise the maximum.")

    first_t, last_t = res["first"], res["last"]
    st.markdown(kpi_html([
        ("Events fetched", f"{len(df):,}", ("capped" if res["truncated"] else "complete")
         + f" in {res['seconds']:.1f}s"),
        ("Namespaces", res["ns_n"], None),
        ("Pods", res["pod_n"], None),
        ("First to last event", f"{first_t:%H:%M:%S}" if pd.notna(first_t) else "-",
         f"to {last_t:%H:%M:%S}  ({last_t:%Y-%m-%d})" if pd.notna(last_t) else None),
    ]), unsafe_allow_html=True)

    f1, f2 = st.columns([3, 1])
    needle = f1.text_input("Filter the fetched logs", placeholder="e.g. error, timeout, OOMKilled",
                           label_visibility="collapsed")
    case = f2.checkbox("Match case")
    view = df
    if needle:
        view = df[df["_raw"].str.contains(needle, case=case, regex=False)]
        st.caption(f"{len(view):,} of {len(df):,} events contain '{needle}'.")

    tab_table, tab_raw, tab_activity = st.tabs(["Table", "Log view", "Activity"])
    with tab_table:
        st.dataframe(view.head(TABLE_ROWS), width="stretch", height=430, hide_index=True)
        if len(view) > TABLE_ROWS:
            st.caption(f"Showing the first {TABLE_ROWS:,} rows. The downloads below contain everything fetched.")
    with tab_raw:
        n = st.select_slider("Lines to show", options=[100, 250, 500, 1000, 2000], value=250)
        text = "\n".join(log_lines(view.head(n)))
        st.code(text or "(no lines)", language="text", line_numbers=True, wrap_lines=True)
    with tab_activity:
        a1, a2 = st.columns(2)
        with a1:
            st.markdown("**Events per pod (top 15)**")
            st.bar_chart(res["pod_counts"])
        with a2:
            st.markdown("**Events over time**")
            if res["series"] is not None:
                st.bar_chart(res["series"])
                st.caption(f"One bar = {res['freq']}.")
            else:
                st.caption("The events have no readable timestamps.")

    # Download files are only built when the button is clicked (callables), not on every rerun.
    base = safe_name(f"splunk_{res['cluster']}_{res['start']:%Y%m%d_%H%M}")
    st.markdown("**Download everything fetched**")
    d1, d2, d3 = st.columns(3)
    d1.download_button("CSV", lambda: df.to_csv(index=False).encode("utf-8"), file_name=f"{base}.csv",
                       mime="text/csv", width="stretch")
    d2.download_button("JSON lines", lambda: df.to_json(orient="records", lines=True,
                                                         force_ascii=False).encode("utf-8"),
                       file_name=f"{base}.jsonl", mime="application/json", width="stretch")
    d3.download_button("Plain text", lambda: "\n".join(log_lines(df)).encode("utf-8"),
                       file_name=f"{base}.log", mime="text/plain", width="stretch")

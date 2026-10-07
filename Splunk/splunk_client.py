"""
Minimal Splunk REST client plus SPL builders for Kubernetes log discovery.

Talks to the Splunk management port (default 8089):
    POST   /services/auth/login                (only for username/password sign-in)
    GET    /services/authentication/current-context
    GET    /services/data/indexes
    POST   /services/search/jobs               (create a search job)
    GET    /services/search/jobs/<sid>         (poll)
    GET    /services/search/jobs/<sid>/results (page through results)
    DELETE /services/search/jobs/<sid>         (clean up)

What a user can see (indexes, clusters, namespaces, pods) is decided by Splunk
itself from the user's role, so "clusters I have access to" simply means the
clusters that appear in the indexes this user is allowed to search.

Credentials are only ever held in memory and are never written to logs or errors.
"""

import re
import time
from concurrent.futures import ThreadPoolExecutor

import requests

DEFAULT_PAGE_SIZE = 50000          # Splunk's default maxresultrows
DEFAULT_WORKERS = 4                # result pages fetched in parallel


class SplunkError(Exception):
    """Anything that goes wrong talking to Splunk. The message is safe to show."""


# ---------------------------------------------------------------------------
# SPL building (values are quoted/escaped, field names are validated)
# ---------------------------------------------------------------------------

_FIELD_OK = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:\-]*$")


def spl_quote(value) -> str:
    """Return value as a double-quoted SPL string with \\ and " escaped."""
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def spl_field(name: str) -> str:
    """Validate a field name so it can be placed in SPL safely."""
    if not isinstance(name, str) or not _FIELD_OK.match(name):
        raise ValueError(f"Invalid Splunk field name: {name!r}")
    return name


def index_clause(indexes) -> str:
    if not indexes or "*" in indexes:
        return "index=*"
    parts = [f"index={spl_quote(i)}" for i in indexes]
    return parts[0] if len(parts) == 1 else "(" + " OR ".join(parts) + ")"


def or_clause(field: str, values) -> str:
    field = spl_field(field)
    parts = [f"{field}={spl_quote(v)}" for v in values]
    return parts[0] if len(parts) == 1 else "(" + " OR ".join(parts) + ")"


def filter_clause(cluster_field=None, cluster=None, ns_field=None, namespaces=None,
                  pod_field=None, pods=None) -> str:
    parts = []
    if cluster:
        parts.append(or_clause(cluster_field, [cluster]))
    if namespaces:
        parts.append(or_clause(ns_field, namespaces))
    if pods:
        parts.append(or_clause(pod_field, pods))
    return " ".join(parts)


def discovery_spl(field: str, indexes, filters: str = "", use_tstats: bool = True) -> str:
    """SPL that lists the distinct values of `field` (cheap with tstats, which
    needs the field to be an indexed field; the stats form works for any field)."""
    field = spl_field(field)
    idx = index_clause(indexes)
    if use_tstats:
        return " ".join(p for p in ("| tstats count where", idx, filters, "by", field) if p)
    return " ".join(p for p in ("search", idx, filters, "| stats count by", field) if p)


def build_log_spl(indexes, cluster_field, cluster, ns_field, namespaces, pod_field, pods,
                  max_events: int, order: str = "newest") -> str:
    """SPL that returns at most `max_events` matching events.

    order="newest"  (default, fast): Splunk already returns newest events first, so
        `head` lets it stop searching as soon as it has enough. When the range holds
        no more than `max_events` events this is every event, and the app sorts them
        oldest-first itself.
    order="earliest" (slower): sorts ALL matching events first so the cap keeps the
        EARLIEST ones. Only worth it when you expect to hit the cap.
    Only the five needed fields are kept before anything else runs, which cuts the
    data Splunk moves around and sends back."""
    spl_field(cluster_field), spl_field(ns_field), spl_field(pod_field)
    if order not in ("newest", "earliest"):
        raise ValueError(f"Invalid order: {order!r}")
    filters = filter_clause(cluster_field, cluster, ns_field, namespaces, pod_field, pods)
    base = " ".join(p for p in ("search", index_clause(indexes), filters) if p)
    keep = f"| fields + _time, _raw, {cluster_field}, {ns_field}, {pod_field}"
    cap = f"| head {int(max_events)}"
    table = f"| table _time, {cluster_field}, {ns_field}, {pod_field}, _raw"
    if order == "earliest":
        return f"{base} {keep} | sort 0 _time {cap} {table}"
    return f"{base} {keep} {cap} {table}"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class SplunkClient:
    def __init__(self, base_url, token=None, username=None, password=None,
                 verify=True, timeout=60):
        self.base = base_url.rstrip("/")
        self.timeout = timeout
        self.page_size = DEFAULT_PAGE_SIZE
        self.workers = DEFAULT_WORKERS
        self.tstats_ok = {}                   # field -> False once we learn tstats finds nothing for it
        self.session = requests.Session()     # keeps connections alive between calls
        self.session.verify = verify          # True, False, or a CA bundle path
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        elif username and password:
            self._login(username, password)
        else:
            raise SplunkError("Provide an API token, or a username and password.")

    # ----- low level -----
    def _check(self, resp):
        if resp.ok:
            return resp
        detail = ""
        try:
            msgs = resp.json().get("messages") or []
            detail = "; ".join(m.get("text", "") for m in msgs if m.get("text"))
        except Exception:
            detail = (resp.text or "")[:200]
        if resp.status_code == 401:
            raise SplunkError("Authentication failed (401). Check the token or username/password."
                              + (f" Splunk said: {detail}" if detail else ""))
        if resp.status_code == 403:
            raise SplunkError("Access denied (403). Your Splunk role is not allowed to do this."
                              + (f" Splunk said: {detail}" if detail else ""))
        raise SplunkError(f"Splunk returned HTTP {resp.status_code}." + (f" {detail}" if detail else ""))

    def _request(self, method, path, **kwargs):
        try:
            resp = self.session.request(method, self.base + path, timeout=self.timeout, **kwargs)
        except requests.exceptions.SSLError as exc:
            raise SplunkError("TLS certificate check failed. Set 'ca_bundle' to your company CA file "
                              f"in the config (details: {exc.__class__.__name__}).")
        except requests.exceptions.RequestException as exc:
            raise SplunkError(f"Could not reach Splunk at {self.base}: {exc.__class__.__name__}")
        return self._check(resp)

    def _login(self, username, password):
        resp = self._request("POST", "/services/auth/login",
                             data={"username": username, "password": password, "output_mode": "json"})
        key = resp.json().get("sessionKey")
        if not key:
            raise SplunkError("Login did not return a session key.")
        self.session.headers["Authorization"] = f"Splunk {key}"

    # ----- information -----
    def current_user(self) -> str:
        resp = self._request("GET", "/services/authentication/current-context",
                             params={"output_mode": "json"})
        return resp.json()["entry"][0]["content"].get("username", "")

    def list_indexes(self) -> list:
        """Event indexes this user can see (internal '_' indexes are left out)."""
        resp = self._request("GET", "/services/data/indexes",
                             params={"output_mode": "json", "count": 0, "datatype": "event"})
        names = [e["name"] for e in resp.json().get("entry", [])]
        return sorted(n for n in names if not n.startswith("_"))

    # ----- searching -----
    def run_search(self, spl, earliest, latest, max_results=None, on_progress=None,
                   timeout=600, poll_seconds=1.0) -> list:
        """Run `spl` over [earliest, latest] and return the result rows (list of dicts).
        earliest/latest: epoch seconds, or Splunk time strings such as "-24h" / "now"."""
        text = spl.strip()
        if not (text.startswith("|") or text.lower().startswith("search")):
            text = "search " + text
        resp = self._request("POST", "/services/search/jobs", data={
            "search": text, "earliest_time": str(earliest), "latest_time": str(latest),
            "exec_mode": "normal", "output_mode": "json"})
        sid = resp.json()["sid"]
        try:
            content = self._wait(sid, on_progress, timeout, poll_seconds)
            return self._results(sid, max_results, content.get("resultCount"))
        finally:
            self._delete_job(sid)

    def _wait(self, sid, on_progress, timeout, poll_seconds):
        """Poll until the job is done and return its content. Polls quickly at first
        (short searches such as the dropdown lookups finish in well under a second)
        and backs off to `poll_seconds` for long ones."""
        deadline = time.time() + timeout
        delay = 0.15
        while True:
            resp = self._request("GET", f"/services/search/jobs/{sid}", params={"output_mode": "json"})
            content = resp.json()["entry"][0]["content"]
            state = content.get("dispatchState", "")
            if content.get("isFailed") or state == "FAILED":
                msgs = content.get("messages") or []
                text = "; ".join(m.get("text", "") for m in msgs if m.get("text")) or "the search failed"
                raise SplunkError(f"Splunk search failed: {text}")
            if on_progress:
                on_progress(float(content.get("doneProgress", 0) or 0),
                            int(float(content.get("eventCount", 0) or 0)), state)
            if content.get("isDone"):
                return content
            if time.time() > deadline:
                raise SplunkError(f"Search did not finish within {timeout} seconds. "
                                  "Try a shorter time range or fewer namespaces/pods.")
            time.sleep(delay)
            delay = min(delay * 1.6, poll_seconds)

    def _page(self, sid, count, offset):
        resp = self._request("GET", f"/services/search/jobs/{sid}/results",
                             params={"output_mode": "json", "count": count, "offset": offset})
        return resp.json().get("results", [])

    def _results(self, sid, max_results, result_count=None):
        # When Splunk tells us how many results there are, fetch the pages in parallel.
        try:
            total = int(float(result_count)) if result_count is not None else None
        except (TypeError, ValueError):
            total = None
        if total is not None:
            want = total if max_results is None else min(total, max_results)
            offsets = list(range(0, want, self.page_size))
            if not offsets:
                return []
            if len(offsets) == 1:
                return self._page(sid, want, 0)
            counts = [min(self.page_size, want - off) for off in offsets]
            with ThreadPoolExecutor(max_workers=min(self.workers, len(offsets))) as pool:
                batches = list(pool.map(lambda oc: self._page(sid, oc[1], oc[0]), zip(offsets, counts)))
            return [row for batch in batches for row in batch]

        # Unknown total: walk the pages one after another until a short page.
        rows, offset = [], 0
        while True:
            page = self.page_size
            if max_results is not None:
                page = min(page, max_results - len(rows))
                if page <= 0:
                    break
            batch = self._page(sid, page, offset)
            rows.extend(batch)
            if len(batch) < page:
                break
            offset += len(batch)
        return rows

    def _delete_job(self, sid):
        try:
            self.session.delete(f"{self.base}/services/search/jobs/{sid}", timeout=self.timeout)
        except Exception:
            pass          # cleanup only - never hide the real result or error


def discover_values(client: SplunkClient, field, indexes, filters, earliest, latest,
                    use_tstats=True) -> list:
    """Distinct non-empty values of `field`. Tries the fast tstats search first and
    falls back to a regular search if tstats finds nothing (the field is then not an
    indexed field in this Splunk)."""
    attempts = [True, False] if use_tstats else [False]
    if use_tstats and client.tstats_ok.get(field) is False:
        attempts = [False]          # we already learned tstats finds nothing for this field
    last_error = None
    for tstats in attempts:
        try:
            rows = client.run_search(discovery_spl(field, indexes, filters, tstats), earliest, latest)
        except SplunkError as exc:
            last_error = exc
            continue
        values = sorted({r.get(field, "").strip() for r in rows if r.get(field, "").strip()})
        if values:
            if use_tstats and not tstats:
                client.tstats_ok[field] = False     # next time skip the pointless tstats attempt
            return values
    if last_error:
        raise last_error
    return []

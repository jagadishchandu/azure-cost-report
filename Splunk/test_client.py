"""
Offline tests for splunk_client.py against a tiny fake Splunk server (no real Splunk needed).
Run:  python test_client.py
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from splunk_client import (SplunkClient, SplunkError, build_log_spl, discover_values, discovery_spl,
                           filter_clause, spl_field, spl_quote)

LOG_ROWS = [{"_time": f"2026-10-07T10:0{i}:00.000+00:00", "cluster_name": "prod-aks",
             "namespace": "payments", "pod": f"pay-{i % 2}", "_raw": f"line {i}"} for i in range(5)]
STATE = {"fail_tstats": False, "no_count": False, "deleted": [], "searches": [], "polls": 0,
         "inflight": 0, "max_inflight": 0, "tstats_calls": 0}
LOCK = threading.Lock()


class FakeSplunk(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authed(self):
        auth = self.headers.get("Authorization", "")
        if auth in ("Bearer good-token", "Splunk SESSION123"):
            return True
        self._send(401, {"messages": [{"type": "WARN", "text": "call not properly authenticated"}]})
        return False

    def _form(self):
        n = int(self.headers.get("Content-Length", 0))
        return {k: v[0] for k, v in parse_qs(self.rfile.read(n).decode()).items()}

    def do_POST(self):
        path = urlparse(self.path).path
        form = self._form()
        if path == "/services/auth/login":
            if form.get("username") == "test" and form.get("password") == "secret":
                return self._send(200, {"sessionKey": "SESSION123"})
            return self._send(401, {"messages": [{"type": "WARN", "text": "Login failed"}]})
        if not self._authed():
            return
        if path == "/services/search/jobs":
            if "BADSPL" in form["search"]:
                return self._send(400, {"messages": [{"type": "FATAL", "text": "Unknown search command 'badspl'"}]})
            STATE["searches"].append(form)
            STATE["polls"] = 0
            return self._send(201, {"sid": "job1"})
        self._send(404, {})

    def do_DELETE(self):
        STATE["deleted"].append(urlparse(self.path).path)
        self._send(200, {})

    def do_GET(self):
        if not self._authed():
            return
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path == "/services/authentication/current-context":
            return self._send(200, {"entry": [{"content": {"username": "tester"}}]})
        if u.path == "/services/data/indexes":
            return self._send(200, {"entry": [{"name": "k8s"}, {"name": "_internal"}, {"name": "main"}]})
        if u.path == "/services/search/jobs/job1":
            STATE["polls"] += 1
            done = STATE["polls"] >= 2
            content = {"isDone": done, "isFailed": False, "dispatchState": "DONE" if done else "RUNNING",
                       "doneProgress": 1.0 if done else 0.4, "eventCount": 5}
            if done and not STATE["no_count"]:
                content["resultCount"] = str(len(self._rows_for(STATE["searches"][-1]["search"])))
            return self._send(200, {"entry": [{"content": content}]})
        if u.path == "/services/search/jobs/job1/results":
            rows = self._rows_for(STATE["searches"][-1]["search"])
            off, cnt = int(q.get("offset", 0)), int(q.get("count", 100))
            with LOCK:
                STATE["inflight"] += 1
                STATE["max_inflight"] = max(STATE["max_inflight"], STATE["inflight"])
            time.sleep(0.15)                      # long enough for parallel requests to overlap
            with LOCK:
                STATE["inflight"] -= 1
            return self._send(200, {"results": rows[off:off + cnt]})
        self._send(404, {})

    @staticmethod
    def _rows_for(search):
        if search.startswith("| tstats"):
            STATE["tstats_calls"] += 1
            return [] if STATE["fail_tstats"] else FakeSplunk._discovery(search)
        if "| stats count by" in search:
            return FakeSplunk._discovery(search)
        return LOG_ROWS

    @staticmethod
    def _discovery(search):
        if "by index" in search:
            return [{"index": "k8s", "count": "13"}]
        if "by cluster_name" in search:
            return [{"cluster_name": "prod-aks", "count": "10"}, {"cluster_name": "dev-aks", "count": "3"},
                    {"cluster_name": "", "count": "1"}]
        if "by namespace" in search:
            return [{"namespace": "payments", "count": "5"}, {"namespace": "orders", "count": "2"}]
        if "by pod" in search:
            return [{"pod": "pay-0", "count": "3"}, {"pod": "pay-1", "count": "2"}]
        return []


def start_server():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), FakeSplunk)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def expect_error(fn, contains):
    try:
        fn()
    except SplunkError as exc:
        assert contains in str(exc), f"expected {contains!r} in {exc!r}"
        return
    raise AssertionError(f"expected SplunkError containing {contains!r}")


def main():
    srv, base = start_server()

    # --- authentication
    c = SplunkClient(base, token="good-token")
    assert c.current_user() == "tester"
    print("ok  token sign-in")
    c2 = SplunkClient(base, username="test", password="secret")
    assert c2.current_user() == "tester"
    print("ok  username/password sign-in (session key)")
    expect_error(lambda: SplunkClient(base, username="test", password="wrong"), "401")
    expect_error(lambda: SplunkClient(base, token="bad").current_user(), "Authentication failed")
    expect_error(lambda: SplunkClient(base), "token")
    print("ok  bad credentials give clear errors")

    # --- indexes
    assert c.list_indexes() == ["k8s", "main"]
    print("ok  indexes (internal ones hidden)")

    # --- discovery
    assert discover_values(c, "cluster_name", (), "", "-24h", "now") == ["dev-aks", "prod-aks"]
    assert discover_values(c, "namespace", (), filter_clause("cluster_name", "prod-aks"), "-24h", "now") == ["orders", "payments"]
    assert discover_values(c, "pod", (), "", 1, 2) == ["pay-0", "pay-1"]
    print("ok  cluster / namespace / pod discovery (blank values dropped)")
    assert discover_values(c, "index", (), filter_clause("cluster_name", "prod-aks"), "-24h", "now") == ["k8s"]
    print("ok  finds the index(es) a cluster lives in")

    STATE["fail_tstats"] = True
    assert discover_values(c, "cluster_name", (), "", "-24h", "now") == ["dev-aks", "prod-aks"]
    assert STATE["searches"][-1]["search"].startswith("search ") and "stats count by" in STATE["searches"][-1]["search"]
    print("ok  falls back to a normal search when tstats finds nothing")
    STATE["tstats_calls"] = 0
    assert discover_values(c, "cluster_name", (), "", "-24h", "now") == ["dev-aks", "prod-aks"]
    assert STATE["tstats_calls"] == 0, "should not retry tstats once it is known to find nothing"
    STATE["fail_tstats"] = False
    print("ok  remembers tstats does not work for that field (skips the wasted search)")

    # --- fetching logs: SPL shape, parallel paging, cap, cleanup
    spl = build_log_spl(("k8s",), "cluster_name", "prod-aks", "namespace", ["payments"], "pod", None, 100)
    assert spl == ('search index="k8s" cluster_name="prod-aks" namespace="payments" '
                   '| fields + _time, _raw, cluster_name, namespace, pod | head 100 '
                   '| table _time, cluster_name, namespace, pod, _raw'), spl
    assert "sort" not in spl
    earliest_spl = build_log_spl(("k8s",), "cluster_name", "prod-aks", "namespace", None, "pod", None, 100, "earliest")
    assert "| sort 0 _time | head 100" in earliest_spl
    try:
        build_log_spl((), "cluster_name", "x", "namespace", None, "pod", None, 10, "sideways")
        raise AssertionError("bad order accepted")
    except ValueError:
        pass
    print("ok  default search keeps only needed fields and has no sort; 'earliest' adds the sort")

    c.page_size = 2
    STATE["deleted"].clear()
    STATE["max_inflight"] = 0
    rows = c.run_search(spl, 1000, 2000)
    assert [r["_raw"] for r in rows] == [f"line {i}" for i in range(5)], rows
    assert STATE["searches"][-1]["earliest_time"] == "1000" and STATE["searches"][-1]["latest_time"] == "2000"
    assert STATE["deleted"] == ["/services/search/jobs/job1"]
    assert STATE["max_inflight"] >= 2, "pages should be fetched in parallel"
    print(f"ok  3 pages fetched in parallel (up to {STATE['max_inflight']} at once), in order, job cleaned up")

    STATE["no_count"] = True
    STATE["max_inflight"] = 0
    rows = c.run_search(spl, 1000, 2000)
    assert [r["_raw"] for r in rows] == [f"line {i}" for i in range(5)], rows
    assert STATE["max_inflight"] == 1
    STATE["no_count"] = False
    print("ok  still correct (one page at a time) when Splunk gives no result count")

    assert len(c.run_search(spl, 1, 2, max_results=3)) == 3
    assert len(c.run_search(spl, 1, 2, max_results=4)) == 4
    print("ok  maximum-events cap")
    c.page_size = 50000
    t0 = time.time()
    discover_values(c, "pod", (), "", 1, 2)
    print(f"ok  small lookup finished in {time.time() - t0:.2f}s (fast polling)")
    progress = []
    c.run_search(spl, 1, 2, on_progress=lambda d, n, s: progress.append((d, s)))
    assert progress and progress[0][1] == "RUNNING"
    print("ok  progress callback")
    expect_error(lambda: c.run_search("BADSPL", 1, 2), "Unknown search command")
    print("ok  search syntax error surfaces Splunk's message")

    # --- SPL safety
    assert spl_quote('a"b\\c') == '"a\\"b\\\\c"'
    spl = build_log_spl((), "cluster_name", 'x" | delete', "namespace", None, "pod", None, 10)
    assert 'cluster_name="x\\" | delete"' in spl
    for bad in ("bad field", "a|b", "x;y", ""):
        try:
            spl_field(bad)
        except ValueError:
            continue
        raise AssertionError(f"field {bad!r} should be rejected")
    assert discovery_spl("namespace", ["a", "b"], "", True) == '| tstats count where (index="a" OR index="b") by namespace'
    print("ok  values are escaped, bad field names rejected")

    srv.shutdown()
    print("\nALL CLIENT TESTS PASSED")


if __name__ == "__main__":
    main()

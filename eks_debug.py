#!/usr/bin/env python3
"""
EKS Debugger - log in to an EKS cluster with ekslogin.exe, then collect the
basic debugging picture of what happened (and what is happening) in the last
N minutes.

    python eks_debug.py                       # GUI: pick a cluster from the dropdown
    python eks_debug.py --minutes 60          # GUI, default window 60 minutes
    python eks_debug.py --cluster 3           # no GUI: log in to cluster #3 and collect
    python eks_debug.py --cluster 3 --minutes 15
    python eks_debug.py --cluster 1,3,5       # SEVERAL clusters, one after another (also: 2-4, or all)
    python eks_debug.py --list                # show the clusters ekslogin offers
    python eks_debug.py --cluster 3 --skip-login   # already logged in: don't run ekslogin
    python eks_debug.py --cluster 3 --context my-ctx   # force a specific kubectl context
    python eks_debug.py --cluster 3 --profile my-aws-profile   # force a profile from ~/.aws
    python eks_debug.py --list-profiles       # show the profiles found in ~/.aws

After login the script also reads your ~/.aws profiles (ekslogin may have just created/refreshed
them), picks the one for the selected cluster (kubeconfig's own profile, name match, account match),
verifies it with `aws sts get-caller-identity`, and uses it for all AWS-side checks.

After login the script switches kubectl to the selected cluster's context (`kubectl config
use-context`) and pins every call to it with --context, so a different 'current' context can
never send the checks to the wrong cluster.

Several clusters
    In the window select as many clusters as you like (Ctrl/Shift-click, 'Select all', filter box, or
    type numbers like 1,3,5). They run ONE AFTER ANOTHER - each with its own login, kubectl context,
    AWS profile and its own .html/.txt report - and a summary page links them all. A cluster that fails
    is recorded and the next one still runs. Stop skips the clusters not yet started.

Time window
    LOOKBACK_MINUTES (below) is the global default. Override it with --minutes,
    or in the GUI's "Last (minutes)" box.

What is collected (everything is READ-ONLY: get / describe-style reads / logs / top)
    * Cluster: context, kubectl + server version, API server /readyz health
    * AWS (AWS CLI, read-only): cluster status, API endpoint + access, CA data (and whether kubeconfig
      matches), VPC / subnets (free IPs) / security group rules, cluster + node IAM role policies,
      aws-auth / access entries, nodegroups, add-ons, Fargate, EC2 status of nodes, control-plane
      logging and recent errors / 401-403 denials from the CloudWatch control-plane logs
    * Nodes: name + actual EC2 server (instance id from providerID, zone, type, capacity, nodegroup, IP and the
      EC2 Name tag) next to every node, status, pressure conditions, cordoned, recent condition changes,
      CPU/memory REQUESTS vs allocatable, kubectl top
    * Resource utilization: a CPU / memory dashboard in the HTML - cluster gauges, who uses the cluster
      (share by namespace), per-node cards, a namespace explorer (used vs requested vs limit, click a
      namespace for its pods, high usage in amber / red) and top consumers
    * Network & traffic: CNI (VPC CNI IP settings), DNS, kube-proxy, services / ingress / network policies,
      pod IPs per subnet, network-related events, DNS + CNI log errors, kubelet pod/node counters, and
      (AWS) VPC routing / NAT / endpoints / ENIs / load balancers with their health, plus the TRAFFIC of the
      selected window from CloudWatch: node in/out, load balancer requests / 4xx / 5xx / latency, NAT gateway
    * Who to contact: the namespace label elvh-app-support-dl (the SUPPORT DL) is shown next to every
      namespace, and a 'Teams to contact' list groups the problems found by support DL
    * Namespaces: pods used (running / total) vs pods CONFIGURED (desired replicas of Deployments,
      StatefulSets, DaemonSets + standalone pods), ResourceQuota usage, CPU / memory per namespace,
      and the workloads behind them (with HPA min-max)
    * Pods: not-ready / Pending / CrashLoop / ImagePull / OOMKilled / Evicted /
      recent restarts, and pods created in the window
    * Events: Warning events in the window (grouped + latest), notable Normal events
    * Workloads: Deployments / StatefulSets / DaemonSets not fully ready, recent
      rollouts (new ReplicaSets), failed Jobs, core add-ons in kube-system
    * HPA, PVC/PV, LoadBalancer/Service endpoints, Terminating namespaces
    * Logs: last-N-minutes logs (and the previous container's logs after a restart) of unhealthy pods,
      pods with Warning events and core add-ons (coredns, aws-node, kube-proxy, CSI ...); optionally of
      ALL pods (--logs-all). Shown in full, searchable and filterable in the HTML report.
    * A timeline of everything that happened in the window, and a health summary

Output goes to the screen, to reports/eks_debug_<cluster>_<time>.txt AND to an INTERACTIVE
reports/eks_debug_<cluster>_<time>.html (one self-contained file: collapsible sections,
sortable + filterable tables, severity filters, global search, timeline filters, dark mode).
In the GUI the collection is live: a step checklist with timings, a progress bar, findings
appearing as they are found, a Stop button (a partial report is still saved), and options
to switch AWS details / pod logs on or off.
Logs can contain sensitive data - treat the report file accordingly.

Login methods (--login-method)
    exe (default)  the custom ekslogin.exe wrapper, as before.
    cli            the standard AWS CLI (`aws`): signs in if needed (device code with --device-code), lists the
                   clusters, and writes the kubeconfig entry for each selected cluster. Everything after the login
                   (context, cloud details, report) is the same. See the README, section "Login methods".
    e.g.  python eks_debug.py --login-method cli --profile dev --region us-east-1 --cluster 1

Requirements: Python 3.9+ (tkinter ships with it), kubectl on PATH, ekslogin.exe.
"""

import argparse
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Global settings
# ---------------------------------------------------------------------------

LOOKBACK_MINUTES = 30        # <-- the time window. Override with --minutes (or the GUI box)

MAX_LOG_PODS = 20            # unhealthy pods + pods with Warning events whose logs are pulled
MAX_CORE_LOG_PODS = 12       # core add-on pods (coredns, aws-node, kube-proxy, CSI ...) whose logs are pulled
MAX_ALL_LOG_PODS = 60        # cap when 'logs of ALL pods' is on
LOG_TAIL_LINES = 200         # max lines fetched per container log (all of them go into the HTML)
LOG_MAX_BYTES = 262144       # byte cap per container log
LOG_WORKERS = 6              # logs are fetched in parallel
LOG_SHOW_ERROR_LINES = 12    # error-looking lines shown per container in the .txt report
LOG_SHOW_LAST_LINES = 8      # final lines shown per container in the .txt report
MAX_EVENTS = 60              # max detailed Warning events listed
MAX_ROWS = 40                # max rows per table
MAX_TIMELINE = 120           # max timeline entries shown
MAX_NODE_PODS = 25           # max pods listed per node in the per-node tables
SUPPORT_LABEL = "elvh-app-support-dl"   # namespace label that names the team to contact (see: kubectl get namespaces -l <label>)
UTIL_WARN = 75               # % of a limit / allocatable at which usage is shown amber in the HTML
UTIL_CRIT = 90               # ... and red
KUBECTL_TIMEOUT = 90         # seconds per kubectl call
LOGIN_TIMEOUT = 120          # seconds for ekslogin.exe
REPORT_DIR = "reports"

# Optional fixed cluster list {"1": "my-cluster-a", "2": "my-cluster-b"}. If empty, the
# list is read from clusters.json (same format) next to this script, otherwise it is
# parsed from the menu that ekslogin.exe prints.
CLUSTERS = {}

_HERE = os.path.dirname(os.path.abspath(__file__))
EKSLOGIN_EXE = os.path.join(_HERE, "ekslogin.exe") if os.path.isfile(os.path.join(_HERE, "ekslogin.exe")) else ".\\ekslogin.exe"

ERROR_PATTERN = re.compile(r"error|exception|panic|fatal|fail|oom|refused|timeout|timed out|denied|unable|cannot|traceback", re.I)
ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
WAITING_OK = {"PodInitializing", "ContainerCreating"}
NOTABLE_NORMAL_REASONS = {
    "Killing", "Preempted", "Preempting", "Evicted", "NodeNotReady", "NodeReady", "RegisteredNode",
    "RemovingNode", "DeletingNode", "ScalingReplicaSet", "SuccessfulRescale", "TriggeredScaleUp",
    "ScaleDown", "Drain", "NodeNotSchedulable", "NodeSchedulable",
}


# ---------------------------------------------------------------------------
# Login (as provided)
# ---------------------------------------------------------------------------

def ekslogin(cluster_number):
    # ekslogin.exe prints a menu and waits on stdin for the cluster number(s).
    proc = subprocess.run(
        [EKSLOGIN_EXE],
        input=f"{cluster_number}\n",
        capture_output=True,
        text=True,
        shell=True,
    )
    if proc.returncode != 0:
        print(f"[cluster {cluster_number}] ekslogin failed: {proc.stderr.strip()}")
        print(proc.stdout[-1000:])
        return False
    time.sleep(2)  # brief buffer for kubeconfig/context to settle
    return True


def list_clusters(emit=None) -> dict:
    """{'1': 'cluster-name', ...} for the dropdown. Order of preference: the
    CLUSTERS dict, clusters.json next to this script, then the menu that
    ekslogin.exe prints when it is given no selection (stdin closed)."""
    if LOGIN_OPTS["method"] == "cli":
        return list_clusters_cli(emit or print)
    if CLUSTERS:
        return dict(CLUSTERS)
    path = os.path.join(_HERE, "clusters.json")
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return {str(k): str(v) for k, v in json.load(f).items()}
        except Exception:
            pass
    try:
        proc = subprocess.run([EKSLOGIN_EXE], input="", capture_output=True, text=True,
                              shell=True, timeout=30, errors="replace")
    except Exception:
        return {}
    menu = ANSI_PATTERN.sub("", (proc.stdout or "") + "\n" + (proc.stderr or ""))
    clusters = {}
    # Accepts menu lines such as "1. name", "1) name", "[1] name", "1 - name", "1: name"
    for line in menu.splitlines():
        m = re.match(r"^\s*\[?(\d{1,3})\]?\s*[.):\-]?\s+(\S.*?)\s*$", line)
        if m and re.search(r"[A-Za-z]", m.group(2)):
            clusters[m.group(1)] = m.group(2)
    return clusters


# ---------------------------------------------------------------------------
# Login method: the standard AWS CLI (`aws`) instead of ekslogin.exe  (--login-method cli)
# ---------------------------------------------------------------------------

LOGIN_OPTS = {"method": "exe", "device_code": False, "gui": False}   # method: "exe" (ekslogin.exe, default) or "cli" (aws)
LOGIN_LABELS = {"exe": "Custom login (ekslogin)", "cli": "Cloud CLI (aws)"}   # the GUI combobox values
CLI_TARGETS = {}    # cluster number (str) -> what the CLI listing found; fed into AWS_OPTS after the login


def _run_interactive(cmd, emit):
    """Run an interactive login command (browser / device-code flow). Its output is NOT captured: from the
    command line it uses this console; from the GUI (no console) it gets its own console window on Windows.
    Waits until it finishes. Returns the exit code, or None if it could not be started."""
    emit("Running: " + " ".join([os.path.splitext(os.path.basename(cmd[0]))[0], *cmd[1:]]))
    kwargs = {}
    if LOGIN_OPTS["gui"] and os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_CONSOLE
        emit("A console window opens for the sign-in - complete it there (browser / device code). This continues when it closes.")
    try:
        return subprocess.run(cmd, **kwargs).returncode
    except Exception as exc:
        emit(f"ERROR: could not start {cmd[0]}: {exc}")
        return None


def _run_captured(cmd, timeout=120):
    """Run a non-interactive command. Returns (ok, first line of its output or error)."""
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f"timed out after {timeout}s"
    except Exception as exc:
        return False, str(exc)
    text = (proc.stdout if proc.returncode == 0 else (proc.stderr or proc.stdout)) or ""
    return proc.returncode == 0, _first_line(text, 200) if text.strip() else ""


def list_selected_clusters(emit=print):
    """The cluster list of the chosen login method (ekslogin.exe menu / clusters.json, or the aws CLI)."""
    return list_clusters(emit) if LOGIN_OPTS["method"] == "cli" else list_clusters()


def cli_ensure_aws(profile, emit):
    """True when the AWS CLI is installed and authenticated for `profile` (`aws sts get-caller-identity`). If not,
    and the profile has SSO config, runs `aws sso login` interactively and checks again."""
    exe = shutil.which("aws")
    if not exe:
        emit("AWS CLI (aws) was not found on PATH - install it (https://aws.amazon.com/cli/), then run: aws configure   (or: aws sso login --profile NAME)")
        return False
    ident, err = aws_cli(["sts", "get-caller-identity"], {"profile": profile}, timeout=30)
    if not err:
        emit("AWS CLI is signed in" + (f" (profile {profile})" if profile else "") + f" as {ident.get('Arn') if isinstance(ident, dict) else '?'}")
        return True
    emit("AWS CLI is not authenticated" + (f" for profile {profile}" if profile else "") + f": {_first_line(err, 110)}")
    name = profile or os.environ.get("AWS_PROFILE") or "default"
    info = list_aws_profiles().get(name)
    if not (info and info.get("sso")):
        emit(f"Profile '{name}' has no SSO configuration - run `aws configure` (access keys) or `aws configure sso`, then `aws sso login --profile {name}`, and try again.")
        return False
    cmd = [exe, "sso", "login"] + (["--profile", name] if (profile or os.environ.get("AWS_PROFILE")) else []) + (
        ["--use-device-code"] if LOGIN_OPTS["device_code"] else [])
    rc = _run_interactive(cmd, emit)
    if rc != 0:
        emit("aws sso login failed or was cancelled" + (f" (exit code {rc})" if rc else "") + ".")
        return False
    ident, err = aws_cli(["sts", "get-caller-identity"], {"profile": profile}, timeout=30)
    if err:
        emit(f"Still not authenticated after aws sso login: {_first_line(err, 110)}")
        return False
    emit(f"AWS CLI login OK - signed in as {ident.get('Arn') if isinstance(ident, dict) else '?'}")
    return True


def cli_regions(profile, emit):
    """Regions to list EKS clusters in: --region (comma separated allowed), else the profile's region,
    else AWS_REGION / AWS_DEFAULT_REGION."""
    regions = [r.strip() for r in (AWS_OPTS.get("region") or "").split(",") if r.strip()]
    if not regions:
        info = list_aws_profiles().get(profile or os.environ.get("AWS_PROFILE") or "default") or {}
        region = info.get("region") or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        regions = [region] if region else []
    if not regions:
        emit("No AWS region known - pass --region (e.g. --region us-east-1, or several: us-east-1,eu-west-1) or set a region in the profile.")
    return regions


def list_clusters_cli(emit=print):
    """{'1': 'name (region)', ...} from `aws eks list-clusters` (read-only), numbered in the listed order.
    Fills CLI_TARGETS. Logs in first if needed."""
    CLI_TARGETS.clear()
    profile = AWS_OPTS.get("profile")
    if not cli_ensure_aws(profile, emit):
        return {}
    regions = cli_regions(profile, emit)
    found = []
    for region in regions:
        data, err = aws_cli(["eks", "list-clusters"], {"region": region, "profile": profile}, timeout=90)
        if err:
            emit(f"  region {region}: could not list EKS clusters: {_first_line(err, 120)}")
            continue
        names = data.get("clusters") if isinstance(data, dict) else None
        found += [(n, region) for n in (names or [])]
    if not found:
        if regions:
            emit("No EKS clusters found in " + ", ".join(regions) + " - check the profile and region.")
        return {}
    count = Counter(n for n, _ in found)
    clusters = {}
    for i, (name, region) in enumerate(found, start=1):
        CLI_TARGETS[str(i)] = {"name": name, "region": region, "alias": name if count[name] == 1 else f"{name}-{region}"}
        clusters[str(i)] = f"{name} ({region})"
    return clusters


def cli_login(number, label, emit):
    """Log in to cluster `number` with the AWS CLI: make sure `aws` is authenticated, then
    `aws eks update-kubeconfig`. Returns the kubectl context name. Raises RuntimeError when it fails."""
    if not CLI_TARGETS:
        list_clusters_cli(emit)
    tgt = CLI_TARGETS.get(str(number))
    if not tgt:
        raise RuntimeError(f"cluster {number} is not in the AWS CLI cluster list - run --list with --login-method cli to see the numbers")
    profile = AWS_OPTS.get("profile")
    if not cli_ensure_aws(profile, emit):
        raise RuntimeError("AWS CLI is not authenticated (login failed, was cancelled, or aws is missing)")
    cmd = [shutil.which("aws"), "eks", "update-kubeconfig", "--name", tgt["name"], "--region", tgt["region"]]
    if profile:
        cmd += ["--profile", profile]
    cmd += ["--alias", tgt["alias"]]
    emit("Running: aws " + " ".join(cmd[1:]))
    ok, out = _run_captured(cmd, 120)
    if not ok:
        raise RuntimeError(f"aws eks update-kubeconfig failed: {out}")
    emit(out or "kubeconfig updated.")
    return tgt["alias"]


# ---------------------------------------------------------------------------
# kubectl helpers
# ---------------------------------------------------------------------------

KUBE_CONTEXT = None   # set by select_context(): every kubectl call is pinned to this context


def kubectl(args, timeout=KUBECTL_TIMEOUT):
    """Run kubectl (pinned to KUBE_CONTEXT once one is selected). Returns (ok, text). Never raises."""
    exe = shutil.which("kubectl")
    if not exe:
        return False, "kubectl was not found on PATH"
    if KUBE_CONTEXT and not (args and args[0] == "config" and len(args) > 1 and args[1] in ("get-contexts", "use-context", "current-context")):
        args = ["--context", KUBE_CONTEXT, *args]
    try:
        proc = subprocess.run([exe, *args], capture_output=True, text=True,
                              encoding="utf-8", errors="replace", timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f"timed out after {timeout}s"
    except Exception as exc:
        return False, str(exc)
    if proc.returncode == 0:
        return True, proc.stdout.strip()
    return False, (proc.stderr or proc.stdout).strip()


def kjson(args):
    """kubectl get ... -o json. Returns (data_or_None, error_or_None)."""
    ok, out = kubectl([*args, "-o", "json"])
    if not ok:
        return None, out
    try:
        return json.loads(out), None
    except json.JSONDecodeError as exc:
        return None, f"bad JSON from kubectl: {exc}"


def items(data):
    return (data or {}).get("items") or []


def parse_ts(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def age(ts, now):
    if not ts:
        return "-"
    secs = max(0, int((now - ts).total_seconds()))
    if secs < 120:
        return f"{secs}s"
    if secs < 7200:
        return f"{secs // 60}m"
    if secs < 172800:
        return f"{secs // 3600}h"
    return f"{secs // 86400}d"


def parse_cpu(q):
    q = str(q or "0")
    try:
        if q.endswith("m"):
            return float(q[:-1]) / 1000
        if q.endswith("u"):
            return float(q[:-1]) / 1e6
        if q.endswith("n"):
            return float(q[:-1]) / 1e9
        return float(q)
    except ValueError:
        return 0.0


_MEM_UNITS = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50, "Ei": 2**60,
              "k": 1e3, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "E": 1e18}


def parse_mem(q):
    q = str(q or "0")
    try:
        for unit in sorted(_MEM_UNITS, key=len, reverse=True):
            if q.endswith(unit):
                return float(q[:-len(unit)]) * _MEM_UNITS[unit]
        return float(q)
    except ValueError:
        return 0.0


def fmt_gib(b):
    return f"{b / 2**30:.1f}Gi"


# ---------------------------------------------------------------------------
# Report plumbing
# ---------------------------------------------------------------------------

class Report:
    """Collects the report as text lines (streamed to `emit`) AND as structured sections /
    blocks (tables, logs, timeline) that the interactive HTML report is built from."""

    def __init__(self, emit):
        self.lines = []
        self.emit = emit
        self.sections = [{"id": "s0", "title": "Run log", "blocks": []}]
        self.current = self.sections[0]

    def _text(self, line):
        self.lines.append(line)
        self.emit(line)

    def _block(self, kind, *payload):
        blocks = self.current["blocks"]
        if kind == "lines":
            if blocks and blocks[-1][0] == "lines":
                blocks[-1][1].append(payload[0])
            else:
                blocks.append(("lines", [payload[0]]))
        else:
            blocks.append((kind, *payload))

    def add(self, text=""):
        for line in str(text).splitlines() or [""]:
            self._text(line)
            self._block("lines", line)

    def section(self, title):
        self._text("")
        self._text("=" * 78)
        self._text(title)
        self._text("=" * 78)
        self.current = {"id": f"s{len(self.sections)}", "title": title, "blocks": []}
        self.sections.append(self.current)

    def table(self, headers, rows, limit=MAX_ROWS, maxw=58):
        if not rows:
            return
        rows = [["-" if c is None else str(c) for c in r] for r in rows]
        shown = rows[:limit]
        widths = [min(maxw, max([len(h)] + [len(r[i]) for r in shown])) for i, h in enumerate(headers)]

        def cell(text, w):
            return text if len(text) <= w else text[: w - 1] + "~"

        self._text("  ".join(h.ljust(w) for h, w in zip(headers, widths)))
        for r in shown:
            self._text("  ".join(cell(c, w).ljust(w) for c, w in zip(r, widths)).rstrip())
        if len(rows) > limit:
            self._text(f"... and {len(rows) - limit} more")
        self._block("table", list(headers), rows)   # the HTML report keeps ALL rows

    def log(self, title, entries, text_entries=None):
        """entries: [(text, kind)] with kind '' | 'warn' | 'err'. The HTML keeps all of them;
        the text report prints text_entries (a shortened version) when given."""
        def norm(k):
            return "err" if k is True else ("" if k in (False, None) else k)
        entries = [(t, norm(k)) for t, k in entries]
        shown = entries if text_entries is None else [(t, norm(k)) for t, k in text_entries]
        self._text(f"[{title}]")
        for text, kind in shown:
            self._text(("  ERR> " if kind == "err" else "      ") + text)
        self._block("log", title, entries)

    def util(self, data):
        """Structured utilization data: rendered as the interactive dashboard in the HTML only."""
        self._block("util", data)

    def series(self, title, rows, note=""):
        """Time series (sparkline charts) - rendered in the HTML only; the numbers are printed as tables by the caller."""
        if rows:
            self._block("series", title, rows, note)

    def timeline(self, entries):
        """entries: [(datetime, text)]"""
        for ts, text in entries:
            self._text(f"{ts:%H:%M:%S}Z  {text}")
        self._block("timeline", [(f"{ts:%H:%M:%S}", text) for ts, text in entries])


def support_map(ctx):
    """{namespace: value of the support-DL label (or None)} - what
    kubectl get namespaces -l elvh-app-support-dl -o custom-columns=NAME:.metadata.name,SUPPORT_DL:.metadata.labels.elvh-app-support-dl shows."""
    cache = ctx.data.get("_support_map")
    if cache is None:
        cache = {n["metadata"]["name"]: (n["metadata"].get("labels") or {}).get(SUPPORT_LABEL)
                 for n in items(ctx.data.get("namespaces"))}
        ctx.data["_support_map"] = cache
    return cache


def support_of(ctx, ns):
    return support_map(ctx).get(ns) or None


def support_suffix(ctx, namespaces):
    """' (support: dl-a@x.com, dl-b@x.com)' for the namespaces given, or ''."""
    dls = list(dict.fromkeys(d for d in (support_of(ctx, n) for n in sorted(set(namespaces))) if d))
    return f" (support: {', '.join(dls)})" if dls else ""


def contact_rows(ctx):
    """Who to contact: namespaces that have problems, grouped by their support DL."""
    groups = defaultdict(lambda: {"ns": [], "count": 0, "what": []})
    for ns, issues in ctx.ns_issues.items():
        g = groups[support_of(ctx, ns) or "(no support DL label)"]
        g["ns"].append(ns)
        g["count"] += len(issues)
        g["what"] += [f"{ns}: {t}" for t in dict.fromkeys(issues)]
    rows = [[dl, ", ".join(sorted(g["ns"])), g["count"],
             "; ".join(g["what"][:4]) + (f" (+{len(g['what']) - 4} more)" if len(g["what"]) > 4 else "")]
            for dl, g in groups.items()]
    return sorted(rows, key=lambda r: (r[0].startswith("("), -r[2]))


class Ctx:
    def __init__(self, minutes):
        self.minutes = minutes
        self.now = datetime.now(timezone.utc)
        self.since = self.now - timedelta(minutes=minutes)
        self.findings = []        # (severity, text)
        self.findings_full = []   # (severity, text, section_id)
        self.timeline = []        # (datetime, text)
        self.problem_pods = []    # dicts, for log collection
        self.data = {}
        self.meta = {}            # context / server version, shown in the HTML header
        self.ns_issues = defaultdict(list)   # namespace -> problems found (feeds 'Teams to contact')
        self.report = None
        self.cancel = None        # threading.Event: set by the Stop button
        self.on_finding = None    # optional callback(severity, text) - the GUI shows findings live

    def find(self, severity, text):
        self.findings.append((severity, text))
        section_id = self.report.current["id"] if self.report else "s0"
        self.findings_full.append((severity, text, section_id))
        if self.on_finding:
            try:
                self.on_finding(severity, text)
            except Exception:
                pass

    def ns_issue(self, ns, text):
        if ns and text not in self.ns_issues[ns]:
            self.ns_issues[ns].append(text)

    def happened(self, ts, text):
        if ts and ts >= self.since:
            self.timeline.append((ts, text))


# ---------------------------------------------------------------------------

RESOURCES = {
    "nodes": ["get", "nodes"],
    "pods": ["get", "pods", "-A"],
    "events": ["get", "events", "-A"],
    "deployments": ["get", "deployments", "-A"],
    "statefulsets": ["get", "statefulsets", "-A"],
    "daemonsets": ["get", "daemonsets", "-A"],
    "replicasets": ["get", "replicasets", "-A"],
    "jobs": ["get", "jobs", "-A"],
    "hpa": ["get", "hpa", "-A"],
    "pvc": ["get", "pvc", "-A"],
    "pv": ["get", "pv"],
    "services": ["get", "services", "-A"],
    "endpoints": ["get", "endpoints", "-A"],
    "namespaces": ["get", "namespaces"],
    "ingresses": ["get", "ingress", "-A"],
    "networkpolicies": ["get", "networkpolicy", "-A"],
    "resourcequotas": ["get", "resourcequota", "-A"],
}


def load_data(ctx, rep):
    rep.add("Collecting cluster data ...")
    errors = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {name: pool.submit(kjson, args) for name, args in RESOURCES.items()}
        for name, fut in futures.items():
            data, err = fut.result()
            ctx.data[name] = data
            if err:
                errors[name] = err
    for name, err in errors.items():
        first = err.splitlines()[0] if err else "unknown error"
        rep.add(f"  [!] could not read {name}: {first[:160]}")
    if "pods" in errors and "nodes" in errors:
        raise RuntimeError("Cannot read pods or nodes - check the login / context / permissions.")
    rep.add("Collecting live CPU / memory / disk / swap usage ...")
    fetch_usage(ctx, rep)


# ---------------------------------------------------------------------------
# AWS side of EKS (read-only AWS CLI calls: describe / list / get / filter-log-events)
# ---------------------------------------------------------------------------

AWS_OPTS = {"enabled": True, "cluster": None, "region": None, "profile": None,   # "profile" = the one you asked for
            "profile_used": None, "profile_reason": None, "profile_region": None}  # set by select_aws_profile()
MAX_CP_LOG_LINES = 30        # control-plane CloudWatch error lines shown
LOW_SUBNET_IPS = 50          # warn when a cluster subnet has fewer free IPs than this
CP_LOG_TYPES = ["api", "audit", "authenticator", "controllerManager", "scheduler"]
CLUSTER_POLICY = "AmazonEKSClusterPolicy"
NODE_POLICIES = {"AmazonEKSWorkerNodePolicy", "AmazonEKS_CNI_Policy"}
REGISTRY_POLICIES = {"AmazonEC2ContainerRegistryReadOnly", "AmazonEC2ContainerRegistryPullOnly"}
EKS_ARN = re.compile(r"arn:aws[a-z-]*:eks:([^:]+):(\d+):cluster/(.+)")


def aws_cli(args, target, timeout=60):
    """Run `aws ...` (read-only). Returns (parsed_json_or_text_or_None, error_or_None)."""
    exe = shutil.which("aws")
    if not exe:
        return None, "AWS CLI (aws) was not found on PATH"
    cmd = [exe, *args, "--output", "json"]
    if target.get("region"):
        cmd += ["--region", target["region"]]
    if target.get("profile"):
        cmd += ["--profile", target["profile"]]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
    except subprocess.TimeoutExpired:
        return None, f"timed out after {timeout}s"
    except Exception as exc:
        return None, str(exc)
    if proc.returncode != 0:
        return None, (proc.stderr or proc.stdout).strip()
    try:
        return (json.loads(proc.stdout) if proc.stdout.strip() else {}), None
    except json.JSONDecodeError:
        return proc.stdout.strip(), None


def _first_line(err, n=170):
    return (err.strip().splitlines() or ["unknown error"])[-1][:n]


# ---------------------------------------------------------------------------
# AWS profiles from ~/.aws (read AFTER ekslogin, which may create / refresh them)
# ---------------------------------------------------------------------------

AWS_ACCOUNT_IN_ARN = re.compile(r"arn:aws[a-z-]*:iam::(\d{12}):")
MAX_PROFILE_TRIES = 4


def aws_config_paths():
    home = os.path.expanduser("~")
    return (os.environ.get("AWS_CONFIG_FILE") or os.path.join(home, ".aws", "config"),
            os.environ.get("AWS_SHARED_CREDENTIALS_FILE") or os.path.join(home, ".aws", "credentials"))


def list_aws_profiles():
    """{profile_name: {region, account, role_arn, sso, files}} from ~/.aws/config and
    ~/.aws/credentials (honours AWS_CONFIG_FILE / AWS_SHARED_CREDENTIALS_FILE). Only the
    non-secret fields are read - access keys and tokens are never loaded."""
    import configparser
    cfg_path, cred_path = aws_config_paths()
    profiles = {}
    for path, is_config in ((cfg_path, True), (cred_path, False)):
        if not os.path.isfile(path):
            continue
        parser = configparser.RawConfigParser(interpolation=None, strict=False)
        try:
            parser.read(path, encoding="utf-8")
        except Exception:
            continue
        for section in parser.sections():
            if is_config:
                if section == "default":
                    name = "default"
                elif section.startswith("profile "):
                    name = section[len("profile "):].strip()
                else:
                    continue            # [sso-session x], [services x], ...
            else:
                name = section
            get = lambda key: parser.get(section, key, fallback=None)
            info = profiles.setdefault(name, {"region": None, "account": None, "role_arn": None, "sso": False, "files": []})
            info["files"].append("config" if is_config else "credentials")
            info["region"] = info["region"] or get("region")
            role = get("role_arn")
            if role:
                info["role_arn"] = role
            account = get("sso_account_id") or (AWS_ACCOUNT_IN_ARN.match(role).group(1) if role and AWS_ACCOUNT_IN_ARN.match(role) else None)
            info["account"] = info["account"] or account
            info["sso"] = info["sso"] or bool(get("sso_session") or get("sso_start_url"))
    return profiles


def describe_profile(name, info):
    kind = "SSO" if info.get("sso") else ("role" if info.get("role_arn") else "keys")
    return f"{name}  ({info.get('account') or 'account ?'}, {info.get('region') or 'no region'}, {kind})"


def rank_aws_profiles(profiles, label, cluster, account, exec_profile, preferred):
    """Candidate profiles, best first: [(score, name, reason)]."""
    best = {}

    def add(score, name, reason):
        if name in profiles and score > best.get(name, (0, ""))[0]:
            best[name] = (score, reason)

    if preferred:
        add(1000, preferred, "the profile you selected")
    if exec_profile:
        add(900, exec_profile, "the profile your kubeconfig uses for kubectl")
    keys = [k.lower() for k in (cluster, label) if k and not re.fullmatch(r"cluster-\d+", k)]
    for name, info in profiles.items():
        n = name.lower()
        for k in keys:
            if n == k:
                add(800, name, f"its name equals the cluster '{k}'")
            elif k in n:
                add(700, name, f"its name contains the cluster '{k}'")
            elif len(n) > 3 and n in k and n != "default":
                add(500, name, f"the cluster name contains '{n}'")
        if account and info.get("account") == account:
            add(600, name, f"it is for AWS account {account} (the cluster's account)")
    if os.environ.get("AWS_PROFILE"):
        add(300, os.environ["AWS_PROFILE"], "the AWS_PROFILE environment variable")
    add(100, "default", "the default profile")
    for name in profiles:
        add(1, name, "an available profile")
    return sorted(((s, n, r) for n, (s, r) in best.items()), key=lambda x: (-x[0], x[1]))


def select_aws_profile(label, emit, preferred=None):
    """After ekslogin: read the profiles in ~/.aws, rank them for the selected cluster, and use the
    first one that actually has working credentials (checked with sts get-caller-identity).
    The choice is stored in AWS_OPTS['profile_used']. Returns the profile name or None."""
    AWS_OPTS["profile_used"], AWS_OPTS["profile_reason"], AWS_OPTS["profile_region"] = None, None, None
    cfg_path, cred_path = aws_config_paths()
    profiles = list_aws_profiles()
    if not profiles:
        emit(f"No AWS profiles found in {cfg_path} / {cred_path} - using the AWS CLI's default credentials / environment.")
        return None
    emit(f"AWS profiles in ~/.aws ({len(profiles)}): " + ", ".join(sorted(profiles)))
    if preferred and preferred not in profiles:
        emit(f"WARNING: profile '{preferred}' is not in your .aws files - choosing automatically instead.")
        preferred = None

    hints = resolve_aws_target(label)     # cluster / account / region / kubectl's own profile, from the kubeconfig
    ranked = rank_aws_profiles(profiles, label, hints.get("cluster"), hints.get("account"), hints.get("exec_profile"), preferred)
    failed = []
    for score, name, reason in ranked[:MAX_PROFILE_TRIES]:
        info = profiles[name]
        ident, err = aws_cli(["sts", "get-caller-identity"], {"profile": name, "region": info.get("region") or hints.get("region")}, timeout=30)
        if not err:
            AWS_OPTS["profile_used"], AWS_OPTS["profile_reason"] = name, reason
            AWS_OPTS["profile_region"] = info.get("region")
            emit(f"AWS profile '{name}' selected ({reason}); signed in as {ident.get('Arn')}")
            return name
        low = err.lower()
        hint = f"  -> run: aws sso login --profile {name}" if "sso" in low and ("expire" in low or "login" in low or "token" in low) else ""
        emit(f"  profile '{name}' ({reason}) did not work: {_first_line(err, 110)}{hint}")
        failed.append(name)
    if preferred:
        AWS_OPTS["profile_used"], AWS_OPTS["profile_reason"] = preferred, "the profile you selected (credentials NOT verified)"
        AWS_OPTS["profile_region"] = profiles[preferred].get("region")
    emit("WARNING: none of the tried ~/.aws profiles gave working credentials"
         + (f" (tried: {', '.join(failed)})" if failed else "") + " - the AWS section will show the exact error.")
    return AWS_OPTS["profile_used"]


def resolve_aws_target(label):
    """Work out cluster name / region / profile. Priority: command-line options, then the
    kubeconfig that ekslogin just wrote (cluster ARN + the `aws eks get-token` arguments,
    which carry --cluster-name/--region/--profile), then the dropdown label."""
    t = {"cluster": AWS_OPTS["cluster"], "region": AWS_OPTS["region"],
         "profile": AWS_OPTS.get("profile_used") or AWS_OPTS["profile"],
         "server": None, "ca": None, "source": [], "account": None, "exec_profile": None}
    ok, out = kubectl(["config", "view", "--minify", "--raw", "-o", "json"])
    if ok:
        try:
            cfg = json.loads(out)
        except json.JSONDecodeError:
            cfg = {}
        clusters = cfg.get("clusters") or [{}]
        cl = clusters[0].get("cluster", {})
        t["server"], t["ca"] = cl.get("server"), cl.get("certificate-authority-data")
        m = EKS_ARN.match(clusters[0].get("name", "") or "")
        if m:
            t["region"] = t["region"] or m.group(1)
            t["cluster"] = t["cluster"] or m.group(3)
            t["account"] = m.group(2)
            t["source"].append("cluster ARN in kubeconfig")
        users = cfg.get("users") or [{}]
        ex = (users[0].get("user") or {}).get("exec") or {}
        args = ex.get("args") or []

        def after(*flags):
            for i, a in enumerate(args):
                if a in flags and i + 1 < len(args):
                    return args[i + 1]
            return None
        if after("--cluster-name", "-i"):
            t["cluster"] = t["cluster"] or after("--cluster-name", "-i")
            t["source"].append("kubeconfig exec args")
        t["region"] = t["region"] or after("--region")
        t["exec_profile"] = after("--profile")
        for env in ex.get("env") or []:
            if env.get("name") == "AWS_PROFILE" and not t["exec_profile"]:
                t["exec_profile"] = env.get("value")
        t["profile"] = t["profile"] or t["exec_profile"]
    t["region"] = t["region"] or AWS_OPTS.get("profile_region")
    t["region"] = t["region"] or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not t["cluster"] and label and not label.startswith("cluster-"):
        t["cluster"] = label
        t["source"].append("dropdown label")
    return t


def _policy_names(role_arn, target):
    """Attached managed policy names of a role, or (None, error)."""
    role = role_arn.split("/")[-1]
    data, err = aws_cli(["iam", "list-attached-role-policies", "--role-name", role], target)
    if err:
        return None, _first_line(err)
    return {p["PolicyName"] for p in data.get("AttachedPolicies", [])}, None


def _fmt_perm(perm, sg_id):
    proto = "all" if perm.get("IpProtocol") == "-1" else perm.get("IpProtocol")
    if perm.get("IpProtocol") == "-1":
        ports = "all"
    elif perm.get("FromPort") == perm.get("ToPort"):
        ports = str(perm.get("FromPort"))
    else:
        ports = f"{perm.get('FromPort')}-{perm.get('ToPort')}"
    src = [r.get("CidrIp", "") for r in perm.get("IpRanges", [])] + [r.get("CidrIpv6", "") for r in perm.get("Ipv6Ranges", [])]
    src += ["self" if g.get("GroupId") == sg_id else g.get("GroupId", "") for g in perm.get("UserIdGroupPairs", [])]
    return proto, ports, src


def section_aws(rep, ctx, label):
    rep.section("2. AWS EKS CONTROL PLANE & INFRASTRUCTURE (cluster, network, IAM, nodegroups, logging)")
    if not AWS_OPTS["enabled"]:
        rep.add("Skipped (AWS details turned off).")
        return
    target = resolve_aws_target(label)
    if not target["cluster"]:
        rep.add("Could not work out the EKS cluster name from kubeconfig. Pass --aws-cluster NAME [--region R] [--profile P].")
        return
    if not target["region"]:
        rep.add(f"Cluster name '{target['cluster']}' found but no region. Pass --region R (or set AWS_REGION).")
        return
    rep.add(f"Target: cluster={target['cluster']}  region={target['region']}  profile={target['profile'] or '(default)'}"
            + (f"   [from {', '.join(dict.fromkeys(target['source']))}]" if target["source"] else ""))
    if AWS_OPTS.get("profile_reason") and AWS_OPTS.get("profile_used") == target["profile"]:
        rep.add(f"AWS profile from ~/.aws: {target['profile']}  (chosen because {AWS_OPTS['profile_reason']})")
    name = target["cluster"]

    ident, err = aws_cli(["sts", "get-caller-identity"], target, timeout=30)
    if err:
        rep.add(f"AWS credentials: NOT WORKING ({_first_line(err)})")
        if "sso" in err.lower():
            rep.add(f"  If this is an expired SSO session, run:  aws sso login --profile {target['profile'] or '<profile>'}")
        ctx.find("HIGH", "AWS CLI credentials are not valid - AWS-side checks skipped")
        return
    rep.add(f"Signed in as: {ident.get('Arn')}  (account {ident.get('Account')})")

    desc, err = aws_cli(["eks", "describe-cluster", "--name", name], target)
    if err:
        rep.add(f"eks describe-cluster FAILED: {_first_line(err)}")
        ctx.find("MED", f"Could not describe the EKS cluster in AWS ({_first_line(err, 90)})")
        return
    c = desc.get("cluster", {})
    ctx.data["aws_target"], ctx.data["aws_cluster"] = target, c
    rep.add("")
    rep.add("CLUSTER")
    status = c.get("status", "?")
    rep.add(f"  Status           : {status}")
    if status != "ACTIVE":
        ctx.find("CRIT" if status in ("FAILED", "DELETING") else "HIGH", f"EKS cluster status is {status} (expected ACTIVE)")
    rep.add(f"  Kubernetes ver.  : {c.get('version')}   platform {c.get('platformVersion')}")
    created = parse_ts(c.get("createdAt")) if isinstance(c.get("createdAt"), str) else None
    rep.add(f"  Created          : {c.get('createdAt')}" + (f" ({age(created, ctx.now)} ago)" if created else ""))
    rep.add(f"  API server endpoint : {c.get('endpoint')}")
    vpc = c.get("resourcesVpcConfig", {})
    rep.add(f"  Endpoint access  : public={vpc.get('endpointPublicAccess')}  private={vpc.get('endpointPrivateAccess')}"
            f"  public CIDRs={','.join(vpc.get('publicAccessCidrs') or []) or '-'}")
    if vpc.get("endpointPublicAccess") and not vpc.get("endpointPrivateAccess") and vpc.get("publicAccessCidrs") not in (None, [], ["0.0.0.0/0"]):
        ctx.find("INFO", "API endpoint is public but restricted to specific CIDRs - if kubectl/nodes time out, check your IP is allowed")
    if not vpc.get("endpointPublicAccess") and not vpc.get("endpointPrivateAccess"):
        ctx.find("CRIT", "Neither public nor private API endpoint access is enabled")
    net = c.get("kubernetesNetworkConfig", {})
    rep.add(f"  Service CIDR     : {net.get('serviceIpv4Cidr', '-')}  ipFamily={net.get('ipFamily', '-')}")
    for issue in (c.get("health") or {}).get("issues", []) or []:
        rep.add(f"  [!] cluster health issue: {issue.get('code')}: {issue.get('message')}")
        ctx.find("HIGH", f"EKS cluster health issue {issue.get('code')}: {(issue.get('message') or '')[:90]}")

    # certificate authority + does kubeconfig match AWS?
    ca_b64 = (c.get("certificateAuthority") or {}).get("data")
    if ca_b64:
        import base64
        import hashlib
        try:
            fp = hashlib.sha256(base64.b64decode(ca_b64)).hexdigest()[:24]
            rep.add(f"  Certificate authority: present ({len(ca_b64)} base64 chars, sha256 {fp}...)")
        except Exception:
            rep.add("  Certificate authority: present but not valid base64")
            ctx.find("HIGH", "Cluster certificate authority data is not valid base64")
    else:
        rep.add("  Certificate authority: MISSING in describe-cluster")
        ctx.find("HIGH", "Cluster certificate authority data is missing")
    if target.get("server") and c.get("endpoint") and target["server"].rstrip("/") != c["endpoint"].rstrip("/"):
        rep.add(f"  [!] kubeconfig server ({target['server']}) differs from the cluster endpoint")
        ctx.find("HIGH", "kubeconfig API server differs from the EKS cluster endpoint (stale kubeconfig?)")
    if target.get("ca") and ca_b64 and target["ca"].strip() != ca_b64.strip():
        rep.add("  [!] kubeconfig certificate-authority-data differs from the cluster's CA")
        ctx.find("HIGH", "kubeconfig CA differs from the cluster CA (kubectl TLS errors likely)")

    # control-plane logging
    enabled = set()
    for entry in (c.get("logging") or {}).get("clusterLogging", []) or []:
        if entry.get("enabled"):
            enabled |= set(entry.get("types") or [])
    rep.add("")
    rep.add("CONTROL-PLANE LOGGING (CloudWatch group /aws/eks/%s/cluster)" % name)
    rep.add("  " + "   ".join(f"{t}: {'ON' if t in enabled else 'off'}" for t in CP_LOG_TYPES))
    if not enabled:
        ctx.find("MED", "EKS control-plane logging is OFF - no API/audit/authenticator logs to troubleshoot with")
    elif len(enabled) < len(CP_LOG_TYPES):
        ctx.find("INFO", f"Control-plane log types off: {', '.join(t for t in CP_LOG_TYPES if t not in enabled)}")

    # network: subnets + security groups
    _aws_network(rep, ctx, target, c, vpc)
    # IAM: cluster role, nodegroup roles, aws-auth / access entries
    _aws_iam_and_compute(rep, ctx, target, c, name)
    # EC2 status of nodes (esp. NotReady)
    _aws_instance_status(rep, ctx, target)
    # recent errors from the control-plane logs
    if enabled:
        _aws_cp_logs(rep, ctx, target, name, enabled)


def _aws_network(rep, ctx, target, c, vpc):
    rep.add("")
    rep.add("NETWORK")
    rep.add(f"  VPC              : {vpc.get('vpcId')}")
    subnet_ids = vpc.get("subnetIds") or []
    sg_ids = list(dict.fromkeys(([vpc["clusterSecurityGroupId"]] if vpc.get("clusterSecurityGroupId") else []) + (vpc.get("securityGroupIds") or [])))
    rep.add(f"  Cluster security group: {vpc.get('clusterSecurityGroupId')}   additional: {','.join(vpc.get('securityGroupIds') or []) or '-'}")
    if subnet_ids:
        data, err = aws_cli(["ec2", "describe-subnets", "--subnet-ids", *subnet_ids], target)
        if err:
            rep.add(f"  Subnets: {', '.join(subnet_ids)}   (details unavailable: {_first_line(err, 90)})")
        else:
            ctx.data["aws_subnets"] = data.get("Subnets", [])
            rows = []
            for s in sorted(data.get("Subnets", []), key=lambda x: x["AvailabilityZone"]):
                free = s.get("AvailableIpAddressCount", 0)
                note = "VERY LOW IPs" if free < 10 else ("low IPs" if free < LOW_SUBNET_IPS else "")
                if note:
                    ctx.find("HIGH" if free < 10 else "MED", f"Subnet {s['SubnetId']} ({s['AvailabilityZone']}) has only {free} free IPs - pods/nodes may fail to get IPs")
                rows.append([s["SubnetId"], s["AvailabilityZone"], s["CidrBlock"], free, s["State"], note])
            rep.table(["SUBNET", "AZ", "CIDR", "FREE IPs", "STATE", "NOTE"], rows)
    if sg_ids:
        data, err = aws_cli(["ec2", "describe-security-groups", "--group-ids", *sg_ids], target)
        if err:
            rep.add(f"  Security groups: {', '.join(sg_ids)}   (rules unavailable: {_first_line(err, 90)})")
            return
        for sg in data.get("SecurityGroups", []):
            gid = sg["GroupId"]
            rep.add(f"  Security group {gid} ({sg.get('GroupName')}):")
            self_ref = False
            rows = []
            for direction, perms in (("in", sg.get("IpPermissions", [])), ("out", sg.get("IpPermissionsEgress", []))):
                for perm in perms:
                    proto, ports, src = _fmt_perm(perm, gid)
                    rows.append([direction, proto, ports, ", ".join(x for x in src if x) or "-"])
                    if direction == "in" and "self" in src:
                        self_ref = True
                    if direction == "in" and "0.0.0.0/0" in src and (ports in ("22", "all", "3389") or proto == "all"):
                        ctx.find("HIGH", f"Security group {gid} allows {ports} from 0.0.0.0/0 on inbound")
                    if direction == "in" and "0.0.0.0/0" in src and ports == "443":
                        ctx.find("INFO", f"Security group {gid} allows 443 from 0.0.0.0/0")
            rep.table(["DIR", "PROTO", "PORTS", "SOURCE / DESTINATION"], rows, limit=20)
            if gid == vpc.get("clusterSecurityGroupId") and not self_ref:
                ctx.find("MED", f"Cluster security group {gid} has no self-referencing inbound rule - nodes may not reach the API/each other")
            if not any(r[0] == "out" for r in rows):
                ctx.find("HIGH", f"Security group {gid} has no outbound rules - nodes can't reach the API, ECR or the internet")


def _aws_iam_and_compute(rep, ctx, target, c, name):
    rep.add("")
    rep.add("IAM ROLES & PERMISSIONS")
    role_arn = c.get("roleArn")
    if role_arn:
        pols, err = _policy_names(role_arn, target)
        if err:
            rep.add(f"  Cluster role {role_arn}: policies unreadable ({err})")
        else:
            ok = CLUSTER_POLICY in pols
            rep.add(f"  Cluster role {role_arn}: {', '.join(sorted(pols)) or 'no managed policies'}  -> {CLUSTER_POLICY} {'OK' if ok else 'MISSING'}")
            if not ok:
                ctx.find("HIGH", f"Cluster IAM role is missing {CLUSTER_POLICY}")

    # nodegroups
    rep.add("")
    rep.add("MANAGED NODEGROUPS")
    ng_names, err = aws_cli(["eks", "list-nodegroups", "--cluster-name", name], target)
    node_roles = {}
    if err:
        rep.add(f"  could not list nodegroups: {_first_line(err, 100)}")
    else:
        ng_names = ng_names.get("nodegroups", [])
        nodes = items(ctx.data.get("nodes"))
        ready_by_group = Counter()
        for n in nodes:
            grp = n["metadata"].get("labels", {}).get("eks.amazonaws.com/nodegroup")
            ready = any(cd["type"] == "Ready" and cd["status"] == "True" for cd in n.get("status", {}).get("conditions", []))
            if grp and ready:
                ready_by_group[grp] += 1
        if not ng_names:
            rep.add("  none (self-managed nodes, Karpenter or Fargate only)")
        rows = []
        for g in ng_names:
            d, err = aws_cli(["eks", "describe-nodegroup", "--cluster-name", name, "--nodegroup-name", g], target)
            if err:
                rows.append([g, "?", "?", "?", _first_line(err, 80)])
                continue
            ng = d.get("nodegroup", {})
            sc = ng.get("scalingConfig", {})
            node_roles[g] = ng.get("nodeRole")
            notes = []
            if ng.get("status") != "ACTIVE":
                notes.append(f"status {ng.get('status')}")
                ctx.find("HIGH", f"Nodegroup {g} status is {ng.get('status')}")
            for issue in (ng.get("health") or {}).get("issues", []) or []:
                notes.append(f"{issue.get('code')}: {(issue.get('message') or '')[:70]}")
                ctx.find("HIGH", f"Nodegroup {g} health issue {issue.get('code')}: {(issue.get('message') or '')[:80]}")
            want, have = sc.get("desiredSize", 0), ready_by_group.get(g, 0)
            if have < want:
                notes.append(f"only {have}/{want} nodes Ready")
                ctx.find("HIGH", f"Nodegroup {g}: {want} nodes wanted but {have} Ready/registered (check node role, aws-auth, subnets, security groups)")
            rows.append([g, ng.get("status"), f"{have} ready / desired {want} (min {sc.get('minSize')}, max {sc.get('maxSize')})",
                         f"{','.join(ng.get('instanceTypes') or [])} {ng.get('capacityType', '')} {ng.get('amiType', '')}".strip(), "; ".join(notes) or "OK"])
        rep.table(["NODEGROUP", "STATUS", "NODES", "INSTANCE / AMI", "HEALTH"], rows, maxw=90)

    roles = {r for r in node_roles.values() if r}
    for role in sorted(roles):
        pols, err = _policy_names(role, target)
        if err:
            rep.add(f"  Node role {role}: policies unreadable ({err})")
            continue
        missing = sorted(NODE_POLICIES - pols) + ([] if pols & REGISTRY_POLICIES else ["AmazonEC2ContainerRegistryReadOnly/PullOnly"])
        rep.add(f"  Node role {role}: {'OK' if not missing else 'MISSING ' + ', '.join(missing)}")
        if missing:
            ctx.find("HIGH", f"Node role {role.split('/')[-1]} is missing: {', '.join(missing)} (CNI may use IRSA/Pod Identity - verify)")

    # who may join: aws-auth ConfigMap and access entries
    ok, out = kubectl(["get", "configmap", "aws-auth", "-n", "kube-system", "-o", "json"])
    if ok:
        try:
            map_roles = json.loads(out).get("data", {}).get("mapRoles", "")
        except json.JSONDecodeError:
            map_roles = ""
        absent = [r for r in sorted(roles) if r not in map_roles]
        rep.add(f"  aws-auth ConfigMap: present; nodegroup roles mapped: {'all' if not absent else 'MISSING ' + ', '.join(absent)}")
        if absent:
            ctx.find("HIGH", f"aws-auth ConfigMap does not map node role(s): {', '.join(a.split('/')[-1] for a in absent)} - nodes can't join")
    else:
        entries, err = aws_cli(["eks", "list-access-entries", "--cluster-name", name], target)
        if err:
            rep.add(f"  aws-auth ConfigMap not readable and access entries unavailable ({_first_line(err, 80)})")
        else:
            arns = entries.get("accessEntries", [])
            absent = [r for r in sorted(roles) if r not in arns]
            rep.add(f"  EKS access entries: {len(arns)}; nodegroup roles present: {'all' if not absent else 'MISSING ' + ', '.join(absent)}")
            if absent:
                ctx.find("HIGH", f"No EKS access entry for node role(s): {', '.join(a.split('/')[-1] for a in absent)}")

    # add-ons
    rep.add("")
    rep.add("EKS ADD-ONS")
    names, err = aws_cli(["eks", "list-addons", "--cluster-name", name], target)
    if err:
        rep.add(f"  could not list add-ons: {_first_line(err, 100)}")
    else:
        rows = []
        for a in names.get("addons", []):
            d, err = aws_cli(["eks", "describe-addon", "--cluster-name", name, "--addon-name", a], target)
            if err:
                rows.append([a, "?", "?", _first_line(err, 70)])
                continue
            ad = d.get("addon", {})
            issues = [f"{i.get('code')}: {(i.get('message') or '')[:60]}" for i in (ad.get("health") or {}).get("issues", []) or []]
            if ad.get("status") != "ACTIVE" or issues:
                ctx.find("HIGH", f"EKS add-on {a} is {ad.get('status')}" + (f" ({issues[0]})" if issues else ""))
            rows.append([a, ad.get("status"), ad.get("addonVersion"), "; ".join(issues) or "OK"])
        if rows:
            rep.table(["ADD-ON", "STATUS", "VERSION", "HEALTH"], rows, maxw=90)
        else:
            rep.add("  no EKS-managed add-ons installed")

    # Fargate
    profiles, err = aws_cli(["eks", "list-fargate-profiles", "--cluster-name", name], target)
    names = (profiles or {}).get("fargateProfileNames", []) if not err else []
    if names:
        rep.add("")
        rep.add("FARGATE")
        rows = []
        for p in names:
            d, err = aws_cli(["eks", "describe-fargate-profile", "--cluster-name", name, "--fargate-profile-name", p], target)
            fp = (d or {}).get("fargateProfile", {}) if not err else {}
            sel = ", ".join(f"{s.get('namespace')}" + (f"[{','.join(s.get('labels', {}))}]" if s.get("labels") else "") for s in fp.get("selectors", []))
            if fp and fp.get("status") != "ACTIVE":
                ctx.find("HIGH", f"Fargate profile {p} status is {fp.get('status')}")
            rows.append([p, fp.get("status", "?"), sel or "-"])
        rep.table(["PROFILE", "STATUS", "NAMESPACES"], rows)
        fargate_nodes = [n["metadata"]["name"] for n in items(ctx.data.get("nodes"))
                         if n["metadata"].get("labels", {}).get("eks.amazonaws.com/compute-type") == "fargate"]
        rep.add(f"  Fargate nodes registered: {len(fargate_nodes)}")
        ok_ns, _ = kubectl(["get", "namespace", "aws-observability"])
        ok_cm, _ = kubectl(["get", "configmap", "aws-logging", "-n", "aws-observability"])
        rep.add(f"  Fargate built-in log router: {'configured (aws-observability/aws-logging)' if ok_ns and ok_cm else 'NOT configured - Fargate pod logs are not shipped'}")
        if not (ok_ns and ok_cm):
            ctx.find("INFO", "Fargate is used but the built-in log router (aws-observability namespace + aws-logging ConfigMap) is not set up")


def _aws_instance_status(rep, ctx, target):
    """EC2 system/instance status checks for the nodes (unhealthy hardware = NotReady node)."""
    ids = {}
    for n in items(ctx.data.get("nodes")):
        pid = n.get("spec", {}).get("providerID", "")
        m = re.search(r"/(i-[0-9a-f]+)$", pid)
        if m:
            ids[m.group(1)] = n["metadata"]["name"]
    if not ids:
        return
    ec2_info = {}
    info, ierr = aws_cli(["ec2", "describe-instances", "--instance-ids", *list(ids)[:50]], target)
    if not ierr:
        for reservation in (info or {}).get("Reservations", []):
            for inst in reservation.get("Instances", []):
                tags = {t.get("Key"): t.get("Value") for t in inst.get("Tags", []) or []}
                ec2_info[inst["InstanceId"]] = {"name": tags.get("Name"), "state": (inst.get("State") or {}).get("Name"),
                                                "private_dns": inst.get("PrivateDnsName"), "ami": inst.get("ImageId")}
    ctx.data["ec2_info"] = ec2_info
    ctx.data.pop("_node_idents", None)          # so the node tables pick up the EC2 names
    data, err = aws_cli(["ec2", "describe-instance-status", "--include-all-instances", "--instance-ids", *list(ids)[:50]], target)
    rep.add("")
    rep.add("EC2 STATUS OF NODES")
    if err:
        rep.add(f"  unavailable: {_first_line(err, 110)}")
        return
    rows = []
    for s in data.get("InstanceStatuses", []):
        state = s.get("InstanceState", {}).get("Name")
        sysst, inst = s.get("SystemStatus", {}).get("Status"), s.get("InstanceStatus", {}).get("Status")
        bad = state != "running" or sysst not in ("ok", "not-applicable") or inst not in ("ok", "not-applicable")
        if bad:
            ctx.find("CRIT" if state == "running" else "HIGH", f"EC2 instance {s['InstanceId']} ({ids.get(s['InstanceId'], '?')}): state {state}, system {sysst}, instance {inst}")
        rows.append([ids.get(s["InstanceId"], "?"), s["InstanceId"], ec2_info.get(s["InstanceId"], {}).get("name") or "-",
                     state, sysst, inst, "PROBLEM" if bad else "ok"])
    unhealthy = [r for r in rows if r[6] != "ok"]
    rep.add(f"  {len(rows)} instance(s) checked, {len(unhealthy)} with problems.")
    rep.table(["NODE", "INSTANCE-ID", "EC2 NAME", "STATE", "SYSTEM CHECK", "INSTANCE CHECK", ""], unhealthy or rows[:5])


def _aws_cp_logs(rep, ctx, target, name, enabled):
    group = f"/aws/eks/{name}/cluster"
    since_ms = str(int(ctx.since.timestamp() * 1000))
    rep.add("")
    rep.add(f"CONTROL-PLANE LOG ERRORS (last {ctx.minutes} min, from CloudWatch {group})")
    data, err = aws_cli(["logs", "filter-log-events", "--log-group-name", group, "--start-time", since_ms,
                         "--filter-pattern", "?error ?Error ?ERROR ?forbidden ?Unauthorized ?denied ?failed ?timeout",
                         "--max-items", "300"], target, timeout=90)
    if err:
        rep.add(f"  unavailable: {_first_line(err, 120)}  (needs logs:FilterLogEvents)")
    else:
        events = [e for e in (data or {}).get("events", []) if not e.get("logStreamName", "").startswith("kube-apiserver-audit")]
        by_stream = Counter(re.sub(r"-[0-9a-f]{20,}$", "", e.get("logStreamName", "?")) for e in events)
        if not events:
            rep.add("  no error-like entries in the window.")
        else:
            rep.add("  entries by component: " + ", ".join(f"{k} x{v}" for k, v in by_stream.most_common()))
            ctx.find("MED", f"{len(events)} error-like control-plane log entries in window ({', '.join(k for k, _ in by_stream.most_common(3))})")
            for e in sorted(events, key=lambda x: x.get("timestamp", 0))[-MAX_CP_LOG_LINES:]:
                ts = datetime.fromtimestamp(e.get("timestamp", 0) / 1000, tz=timezone.utc)
                rep.add(f"  {ts:%H:%M:%S}Z [{re.sub(r'-[0-9a-f]{20,}$', '', e.get('logStreamName', '?'))[:28]}] {(e.get('message') or '').strip()[:200]}")
                ctx.happened(ts, f"CONTROL PLANE {re.sub(r'-[0-9a-f]{20,}$', '', e.get('logStreamName', '?'))[:24]}: {(e.get('message') or '').strip()[:100]}")

    if "audit" in enabled:
        data, err = aws_cli(["logs", "filter-log-events", "--log-group-name", group, "--start-time", since_ms,
                             "--log-stream-name-prefix", "kube-apiserver-audit",
                             "--filter-pattern", "{ ($.responseStatus.code = 401) || ($.responseStatus.code = 403) }",
                             "--max-items", "300"], target, timeout=90)
        if err:
            rep.add(f"  audit denials unavailable: {_first_line(err, 100)}")
            return
        denied = Counter()
        for e in (data or {}).get("events", []):
            try:
                m = json.loads(e.get("message", "{}"))
            except json.JSONDecodeError:
                continue
            who = (m.get("user") or {}).get("username", "?")
            denied[(who[:50], m.get("verb"), (m.get("objectRef") or {}).get("resource"), (m.get("responseStatus") or {}).get("code"))] += 1
        rep.add("")
        if denied:
            rep.add(f"  API requests DENIED (401/403) in the window: {sum(denied.values())} (top callers)")
            rep.table(["USER", "VERB", "RESOURCE", "CODE", "COUNT"], [[u, v, r, c, n] for (u, v, r, c), n in denied.most_common(10)])
            ctx.find("MED", f"{sum(denied.values())} API request(s) denied (401/403) in window, e.g. {denied.most_common(1)[0][0][0]}")
        else:
            rep.add("  no 401/403 denials in the audit log for the window.")


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------

def section_overview(rep, ctx, label):
    rep.section(f"1. CLUSTER OVERVIEW - {label}")
    rep.add(f"Report time (UTC): {ctx.now:%Y-%m-%d %H:%M:%S}   Window: last {ctx.minutes} min "
            f"(since {ctx.since:%H:%M:%S} UTC)")
    ok, out = kubectl(["config", "current-context"])
    ctx.meta["context"] = out if ok else "unknown"
    rep.add(f"kubectl context : {out if ok else 'unknown (' + out[:80] + ')'}")
    ok, out = kubectl(["version", "-o", "json"])
    if ok:
        try:
            v = json.loads(out)
            rep.add(f"Client version  : {v.get('clientVersion', {}).get('gitVersion', '?')}")
            ctx.meta["server"] = v.get("serverVersion", {}).get("gitVersion", "?")
            rep.add(f"Server version  : {v.get('serverVersion', {}).get('gitVersion', '?')}")
        except json.JSONDecodeError:
            pass
    else:
        ctx.find("CRIT", f"Cannot reach the API server: {out.splitlines()[0][:120] if out else '?'}")
        rep.add(f"Server version  : NOT REACHABLE ({out.splitlines()[0][:120] if out else '?'})")
    ok, out = kubectl(["get", "--raw", "/readyz?verbose"])
    if ok:
        failing = [l.strip() for l in out.splitlines() if l.startswith("[-]")]
        if failing:
            rep.add("API server readyz: FAILING checks:")
            for l in failing:
                rep.add(f"    {l}")
            ctx.find("CRIT", f"API server readyz failing: {len(failing)} check(s)")
        else:
            rep.add("API server readyz: OK")
    else:
        rep.add(f"API server readyz: not readable ({out.splitlines()[0][:100] if out else '?'})")


def _node_requests(pods):
    """Per node name: [cpu cores, memory bytes, ephemeral-storage bytes] REQUESTED by its pods."""
    req = defaultdict(lambda: [0.0, 0.0, 0.0])
    for p in pods:
        node = p.get("spec", {}).get("nodeName")
        if not node or p.get("status", {}).get("phase") in ("Succeeded", "Failed"):
            continue
        r = _pod_resources(p)
        req[node][0] += r["cpu_req"]
        req[node][1] += r["mem_req"]
        req[node][2] += r["eph_req"]
    return req


def _pod_resources(pod):
    """Summed container requests/limits of a pod: cpu (cores), memory/ephemeral-storage (bytes)."""
    out = {"cpu_req": 0.0, "cpu_lim": 0.0, "mem_req": 0.0, "mem_lim": 0.0, "eph_req": 0.0, "eph_lim": 0.0}
    for c in pod.get("spec", {}).get("containers", []):
        res = c.get("resources") or {}
        for kind, key in (("requests", "req"), ("limits", "lim")):
            vals = res.get(kind) or {}
            out[f"cpu_{key}"] += parse_cpu(vals.get("cpu"))
            out[f"mem_{key}"] += parse_mem(vals.get("memory"))
            out[f"eph_{key}"] += parse_mem(vals.get("ephemeral-storage"))
    return out


# --- live usage: kubelet stats (CPU, memory, disk, swap) with a metrics-server fallback ---------

def fetch_usage(ctx, rep):
    """Live usage per node and pod. Preferred source: each node's kubelet summary
    (kubectl get --raw /api/v1/nodes/<n>/proxy/stats/summary) - it has CPU, memory,
    root + image filesystem and swap, and per-pod CPU/memory/disk. It needs the
    'nodes/proxy' permission. Fallback: metrics-server (kubectl top) - CPU/memory only."""
    names = [n["metadata"]["name"] for n in items(ctx.data.get("nodes"))]

    def one(name):
        ok, out = kubectl(["get", "--raw", f"/api/v1/nodes/{name}/proxy/stats/summary"], timeout=60)
        if not ok:
            return name, None, out
        try:
            return name, json.loads(out), None
        except json.JSONDecodeError:
            return name, None, "bad JSON from kubelet stats"

    stats, first_err = {}, None
    with ThreadPoolExecutor(max_workers=8) as pool:
        for name, data, err in pool.map(one, names):
            if data:
                stats[name] = data
            elif err and not first_err:
                first_err = err.splitlines()[0][:140] if err else "unknown error"
    ctx.data["node_stats"] = stats
    ctx.data["stats_error"] = first_err

    top_nodes, top_pods, top_err = {}, {}, None
    ok, out = kubectl(["top", "nodes", "--no-headers"])
    if ok:
        for line in out.splitlines():
            p = line.split()
            if len(p) >= 5:  # NAME CPU(cores) CPU% MEMORY(bytes) MEMORY%
                top_nodes[p[0]] = {"cpu": parse_cpu(p[1]), "mem": parse_mem(p[3])}
    else:
        top_err = out.splitlines()[0][:140] if out else "unknown error"
    ok, out = kubectl(["top", "pods", "-A", "--no-headers"])
    if ok:
        for line in out.splitlines():
            p = line.split()
            if len(p) >= 4:  # NAMESPACE NAME CPU MEMORY
                top_pods[(p[0], p[1])] = {"cpu": parse_cpu(p[2]), "mem": parse_mem(p[3])}
    ctx.data["top_nodes"], ctx.data["top_pods"], ctx.data["top_error"] = top_nodes, top_pods, top_err

    # per-pod usage: kubelet stats first, metrics-server for anything missing
    usage = {}
    for node, s in stats.items():
        for p in s.get("pods", []) or []:
            ref = p.get("podRef", {})
            nano = (p.get("cpu") or {}).get("usageNanoCores")
            usage[(ref.get("namespace"), ref.get("name"))] = {
                "cpu": nano / 1e9 if nano is not None else None,
                "mem": (p.get("memory") or {}).get("workingSetBytes"),
                "disk": (p.get("ephemeral-storage") or {}).get("usedBytes"),
            }
    for key, v in top_pods.items():
        usage.setdefault(key, {"cpu": v["cpu"], "mem": v["mem"], "disk": None})
    ctx.data["pod_usage"] = usage

    if stats:
        rep.add(f"  usage source: kubelet stats from {len(stats)}/{len(names)} node(s)"
                + (f" (others: {first_err})" if first_err and len(stats) < len(names) else ""))
    elif top_nodes:
        rep.add(f"  usage source: metrics-server only (kubelet stats unavailable: {first_err}) - disk and swap will show n/a")
    else:
        rep.add(f"  [!] no live usage available (kubelet stats: {first_err}; metrics-server: {top_err})")


def node_identity(ctx, node):
    """The 'actual server' behind a Kubernetes node: EC2 instance id (from spec.providerID), zone,
    instance type, capacity type, nodegroup, IP, and the EC2 Name tag when the AWS section could read it.
    Same fields as: kubectl get nodes -o custom-columns=NAME:.metadata.name,INSTANCE-ID:.spec.providerID,
    ZONE:.metadata.labels."topology\\.kubernetes\\.io/zone",TYPE:.metadata.labels."node\\.kubernetes\\.io/instance-type"."""
    meta, spec, st = node["metadata"], node.get("spec", {}), node.get("status", {})
    labels = meta.get("labels", {})
    pid = spec.get("providerID", "") or ""
    m = re.search(r"/(i-[0-9a-f]+)$", pid)
    iid = m.group(1) if m else (pid.rsplit("/", 1)[-1] if pid else "")
    addresses = {a.get("type"): a.get("address") for a in st.get("addresses", []) or []}
    ec2 = (ctx.data.get("ec2_info") or {}).get(iid, {})
    return {
        "instance_id": iid or "-", "provider_id": pid or "-",
        "zone": labels.get("topology.kubernetes.io/zone") or labels.get("failure-domain.beta.kubernetes.io/zone") or "-",
        "type": labels.get("node.kubernetes.io/instance-type") or labels.get("beta.kubernetes.io/instance-type") or "-",
        "capacity": labels.get("eks.amazonaws.com/capacityType") or labels.get("karpenter.sh/capacity-type") or "-",
        "nodegroup": (labels.get("eks.amazonaws.com/nodegroup") or labels.get("karpenter.sh/nodepool")
                      or labels.get("eks.amazonaws.com/compute-type") or "-"),
        "ip": addresses.get("InternalIP", "-"), "ec2_name": ec2.get("name") or "-", "ec2_state": ec2.get("state") or "-",
    }


def node_idents(ctx):
    cache = ctx.data.get("_node_idents")
    if cache is None:
        cache = {n["metadata"]["name"]: node_identity(ctx, n) for n in items(ctx.data.get("nodes"))}
        ctx.data["_node_idents"] = cache
    return cache


def node_tag(ctx, name):
    """'ip-10-0-1-10.ec2.internal [i-0abc123]' - the node name with its EC2 instance id."""
    ident = node_idents(ctx).get(name)
    return f"{name} [{ident['instance_id']}]" if ident and ident["instance_id"] != "-" else (name or "-")


def node_usage(ctx, name, node):
    """cpu (cores), mem (bytes), disk/imagefs used+capacity, swap used+total for one node."""
    u = {"cpu": None, "mem": None, "disk_used": None, "disk_cap": None,
         "img_used": None, "img_cap": None, "swap_used": None, "swap_total": None, "source": None}
    s = (ctx.data.get("node_stats") or {}).get(name)
    if s:
        n = s.get("node", {})
        if (n.get("cpu") or {}).get("usageNanoCores") is not None:
            u["cpu"] = n["cpu"]["usageNanoCores"] / 1e9
        mem = n.get("memory") or {}
        u["mem"] = mem.get("workingSetBytes", mem.get("usageBytes"))
        fs = n.get("fs") or {}
        u["disk_used"], u["disk_cap"] = fs.get("usedBytes"), fs.get("capacityBytes")
        img = (n.get("runtime") or {}).get("imageFs") or {}
        u["img_used"], u["img_cap"] = img.get("usedBytes"), img.get("capacityBytes")
        swap = n.get("swap") or {}
        if swap.get("swapUsageBytes") is not None:
            u["swap_used"] = swap["swapUsageBytes"]
            if swap.get("swapAvailableBytes") is not None:
                u["swap_total"] = swap["swapUsageBytes"] + swap["swapAvailableBytes"]
        u["source"] = "kubelet"
    t = (ctx.data.get("top_nodes") or {}).get(name)
    if t and u["cpu"] is None:
        u["cpu"], u["mem"], u["source"] = t["cpu"], u["mem"] or t["mem"], u["source"] or "metrics-server"
    if u["swap_total"] is None:
        cap = ((node.get("status", {}).get("nodeInfo") or {}).get("swap") or {}).get("capacity")
        if cap:
            u["swap_total"] = cap
    return u


def _pct(used, total):
    return 100.0 * used / total if used is not None and total else None


def _fp(p):
    return "n/a" if p is None else f"{p:.0f}%"


def _res(used, total, fmt):
    """'used/total (pct%)' or 'n/a'."""
    if used is None:
        return "n/a"
    return f"{fmt(used)}/{fmt(total)} ({_fp(_pct(used, total))})" if total else fmt(used)


def _cores(c):
    return "n/a" if c is None else (f"{c * 1000:.0f}m" if c < 1 else f"{c:.2f}")


def _mi(b):
    return "n/a" if b is None else f"{b / 2**20:.0f}Mi"


def section_nodes(rep, ctx):
    rep.section("3. NODES - STATUS, CPU, MEMORY, DISK, SWAP")
    nodes = items(ctx.data.get("nodes"))
    if not nodes:
        rep.add("No node data.")
        return
    pods = items(ctx.data.get("pods"))
    requests = _node_requests(pods)
    running, active = Counter(), Counter()
    for p in pods:
        node_name = p.get("spec", {}).get("nodeName")
        phase = p.get("status", {}).get("phase")
        if node_name and phase == "Running":
            running[node_name] += 1
        if node_name and phase in ("Running", "Pending"):
            active[node_name] += 1

    usage_rows, info_rows, inv_rows, bad = [], [], [], 0
    for n in nodes:
        meta, st, spec = n["metadata"], n.get("status", {}), n.get("spec", {})
        name = meta["name"]
        ident = node_identity(ctx, n)
        nlabel = f"{name} [{ident['instance_id']}]" if ident["instance_id"] != "-" else name
        conds = {c["type"]: c for c in st.get("conditions", [])}
        ready = conds.get("Ready", {}).get("status")
        status = "Ready" if ready == "True" else "NotReady"
        if spec.get("unschedulable"):
            status += ",SchedulingDisabled"
        flags = []
        if ready != "True":
            flags.append("NOT READY: " + (conds.get("Ready", {}).get("message") or conds.get("Ready", {}).get("reason") or "")[:70])
            ctx.find("CRIT", f"Node {nlabel} is NotReady")
        for ctype in ("MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable"):
            if conds.get(ctype, {}).get("status") == "True":
                flags.append(ctype)
                ctx.find("HIGH", f"Node {nlabel} has {ctype}")
        if spec.get("unschedulable"):
            ctx.find("MED", f"Node {nlabel} is cordoned (SchedulingDisabled)")
        for ctype, c in conds.items():
            t = parse_ts(c.get("lastTransitionTime"))
            if t and t >= ctx.since:
                flags.append(f"{ctype} changed {age(t, ctx.now)} ago")
                ctx.happened(t, f"NODE {nlabel}: condition {ctype}={c.get('status')} ({c.get('reason', '')})")
        created = parse_ts(meta.get("creationTimestamp"))
        if created and created >= ctx.since:
            flags.append("NEW node")
            ctx.happened(created, f"NODE {nlabel}: joined the cluster")

        labels = meta.get("labels", {})
        roles = ",".join(sorted(k.split("/", 1)[1] for k in labels if k.startswith("node-role.kubernetes.io/"))) or "<none>"
        alloc = st.get("allocatable", {})
        a_cpu, a_mem = parse_cpu(alloc.get("cpu")), parse_mem(alloc.get("memory"))
        a_eph, max_pods = parse_mem(alloc.get("ephemeral-storage")), int(parse_cpu(alloc.get("pods")) or 0)
        cpu_req, mem_req, eph_req = requests.get(name, [0.0, 0.0, 0.0])
        u = node_usage(ctx, name, n)

        cpu_pct, mem_pct = _pct(u["cpu"], a_cpu), _pct(u["mem"], a_mem)
        disk_pct, img_pct = _pct(u["disk_used"], u["disk_cap"]), _pct(u["img_used"], u["img_cap"])
        pod_pct = _pct(active[name], max_pods)
        if cpu_pct is not None and cpu_pct > 85:
            flags.append(f"CPU {cpu_pct:.0f}%")
            ctx.find("HIGH" if cpu_pct > 95 else "MED", f"Node {nlabel} CPU usage {cpu_pct:.0f}% of allocatable")
        if mem_pct is not None and mem_pct > 80:
            flags.append(f"MEM {mem_pct:.0f}%")
            ctx.find("HIGH" if mem_pct > 90 else "MED", f"Node {nlabel} memory usage {mem_pct:.0f}% of allocatable")
        for label, pct in (("DISK", disk_pct), ("IMAGEFS", img_pct)):
            if pct is not None and pct > 75:
                flags.append(f"{label} {pct:.0f}%")
                ctx.find("HIGH" if pct > 85 else "MED", f"Node {nlabel} {label.lower()} {pct:.0f}% full")
        if u["swap_used"]:
            flags.append(f"SWAP in use {_mi(u['swap_used'])}")
            ctx.find("MED", f"Node {nlabel} is using swap ({_mi(u['swap_used'])}) - memory pressure sign")
        if pod_pct is not None and pod_pct > 90:
            flags.append(f"PODS {active[name]}/{max_pods}")
            ctx.find("MED", f"Node {nlabel} is nearly at its max pod count ({active[name]}/{max_pods})")
        req_cpu_pct, req_mem_pct = _pct(cpu_req, a_cpu), _pct(mem_req, a_mem)
        if (req_cpu_pct or 0) > 85 or (req_mem_pct or 0) > 85:
            flags.append("HIGH REQUESTS")
            ctx.find("MED", f"Node {nlabel} requests are high (cpu {_fp(req_cpu_pct)}, mem {_fp(req_mem_pct)})")
        if flags:
            bad += 1

        if u["swap_total"] is not None or u["swap_used"] is not None:
            swap_text = (_res(u["swap_used"] if u["swap_used"] is not None else 0, u["swap_total"], _mi)
                         if u["swap_total"] else _mi(u["swap_used"]))
        else:
            swap_text = "none/not reported"
        disk_text = _res(u["disk_used"], u["disk_cap"], fmt_gib) if u["disk_used"] is not None else (f"n/a (alloc {fmt_gib(a_eph)})" if a_eph else "n/a")
        inv_rows.append([name, ident["instance_id"], ident["ec2_name"], ident["zone"], ident["type"], ident["capacity"],
                         ident["nodegroup"], ident["ip"], status, ident["provider_id"]])
        usage_rows.append([
            name, ident["instance_id"], status, f"{running[name]}/{max_pods or '?'}" + (f" (+{active[name] - running[name]} pending)" if active[name] > running[name] else ""),
            _res(u["cpu"], a_cpu, _cores), _res(u["mem"], a_mem, fmt_gib), disk_text,
            _res(u["img_used"], u["img_cap"], fmt_gib) if u["img_used"] is not None else "n/a", swap_text])
        info_rows.append([name, ident["instance_id"], roles, ident["type"], ident["zone"],
                          st.get("nodeInfo", {}).get("kubeletVersion", "-"), age(created, ctx.now),
                          _fp(req_cpu_pct), _fp(req_mem_pct), _res(eph_req, a_eph, fmt_gib) if a_eph else "-", "; ".join(flags)])

    rep.add(f"{len(nodes)} node(s), {bad} with findings.")
    rep.add("")
    rep.add("Node inventory - the node name with its actual EC2 server (INSTANCE-ID comes from spec.providerID; EC2 NAME is the instance's Name tag, read when the AWS section is on):")
    rep.table(["NODE", "INSTANCE-ID", "EC2 NAME", "ZONE", "TYPE", "CAPACITY", "NODEGROUP", "INTERNAL IP", "STATUS", "PROVIDER-ID"], inv_rows, maxw=64)
    rep.add("")
    rep.add("Live usage per node (CPU/MEM vs allocatable, DISK = node root filesystem, PODS = running/max):")
    rep.table(["NODE", "INSTANCE-ID", "STATUS", "PODS", "CPU used/alloc", "MEMORY used/alloc", "DISK used/total", "IMAGEFS", "SWAP"],
              usage_rows, maxw=40)
    rep.add("")
    rep.add("Scheduling view (CPUreq/MEMreq = sum of pod REQUESTS vs allocatable; EPHEMERAL = requested/allocatable):")
    rep.table(["NODE", "INSTANCE-ID", "ROLES", "TYPE", "ZONE", "VERSION", "AGE", "CPUreq", "MEMreq", "EPHEMERAL", "FINDINGS"],
              info_rows, maxw=110)
    notready = [(n["metadata"]["name"], re.search(r"/(i-[0-9a-f]+)$", n.get("spec", {}).get("providerID", "") or ""))
                for n in nodes
                if not any(c["type"] == "Ready" and c["status"] == "True" for c in n.get("status", {}).get("conditions", []))]
    if notready:
        rep.add("")
        rep.add("Node logs are not readable through kubectl. For the NotReady node(s), connect with SSM/SSH and run:")
        for node_name, m in notready[:5]:
            target_id = m.group(1) if m else "<instance-id>"
            rep.add(f"  {node_name}:  aws ssm start-session --target {target_id}")
        rep.add("  then:  sudo journalctl -u kubelet --since '30 min ago' --no-pager | tail -200")
        rep.add("         sudo journalctl -u containerd --since '30 min ago' --no-pager | tail -100   (and: dmesg -T | tail -50)")
    if not ctx.data.get("node_stats"):
        rep.add("")
        rep.add("Note: disk and swap come from the kubelet and need the 'nodes/proxy' permission"
                + (f" ({ctx.data.get('stats_error')})" if ctx.data.get("stats_error") else "")
                + ". Without it CPU/memory come from metrics-server (kubectl top) when installed.")


def section_node_pods(rep, ctx):
    rep.section("5. PODS ON EACH NODE - CPU, MEMORY, DISK per pod")
    pods = items(ctx.data.get("pods"))
    usage = ctx.data.get("pod_usage") or {}
    by_node = defaultdict(list)
    for p in pods:
        if p.get("spec", {}).get("nodeName") and p.get("status", {}).get("phase") in ("Running", "Pending"):
            by_node[p["spec"]["nodeName"]].append(p)
    if not by_node:
        rep.add("No pods are scheduled on nodes.")
        return
    rep.add("USE = live usage now; REQ/LIM = what the pod asked for / may use. MEM use is the working set.")
    for node in sorted(by_node):
        rows = []
        for p in by_node[node]:
            meta, st = p["metadata"], p.get("status", {})
            key = (meta["namespace"], meta["name"])
            use = usage.get(key) or {}
            res = _pod_resources(p)
            status = st.get("phase", "?")
            for cs in st.get("containerStatuses") or []:
                waiting = (cs.get("state") or {}).get("waiting") or {}
                if waiting.get("reason") and waiting["reason"] not in WAITING_OK:
                    status = waiting["reason"]
                    break
            restarts = sum(cs.get("restartCount", 0) for cs in (st.get("containerStatuses") or []))
            notes = []
            mem_lim_pct = _pct(use.get("mem"), res["mem_lim"])
            if mem_lim_pct is not None and mem_lim_pct > 90:
                notes.append(f"MEM {mem_lim_pct:.0f}% of limit")
                ctx.find("MED", f"Pod {key[0]}/{key[1]} is using {mem_lim_pct:.0f}% of its memory limit (OOMKill risk)")
            cpu_lim_pct = _pct(use.get("cpu"), res["cpu_lim"])
            if cpu_lim_pct is not None and cpu_lim_pct > 90:
                notes.append(f"CPU {cpu_lim_pct:.0f}% of limit (throttling)")
            eph_lim_pct = _pct(use.get("disk"), res["eph_lim"])
            if eph_lim_pct is not None and eph_lim_pct > 90:
                notes.append(f"DISK {eph_lim_pct:.0f}% of limit")
            rows.append((use.get("mem") or 0, [
                f"{key[0]}/{key[1]}", support_of(ctx, key[0]) or "-", status, restarts,
                _cores(use.get("cpu")), _cores(res["cpu_req"]) if res["cpu_req"] else "-", _cores(res["cpu_lim"]) if res["cpu_lim"] else "-",
                _mi(use.get("mem")), _mi(res["mem_req"]) if res["mem_req"] else "-", _mi(res["mem_lim"]) if res["mem_lim"] else "-",
                _mi(use.get("disk")), "; ".join(notes)]))
        rows.sort(key=lambda r: r[0], reverse=True)
        rep.add("")
        running = sum(1 for p in by_node[node] if p.get("status", {}).get("phase") == "Running")
        ident = node_idents(ctx).get(node) or {}
        who = (f"  [{ident.get('instance_id', '-')} | {ident.get('zone', '-')} | {ident.get('type', '-')}"
               + (f" | EC2 {ident['ec2_name']}" if ident.get("ec2_name", "-") != "-" else "") + "]") if ident else ""
        rep.add(f"NODE {node}{who}  -  {len(by_node[node])} pod(s) ({running} running)")
        rep.table(["POD", "SUPPORT DL", "STATUS", "RST", "CPU use", "CPU req", "CPU lim", "MEM use", "MEM req", "MEM lim", "DISK use", "NOTES"],
                  [r[1] for r in rows], limit=MAX_NODE_PODS, maxw=60)


def _qty(key, value):
    """A quota / usage quantity as a number: cores for cpu, bytes for memory/storage, else a count."""
    k = key.lower()
    try:
        if "cpu" in k:
            return parse_cpu(value)
        if any(x in k for x in ("memory", "storage")):
            return parse_mem(value)
        return float(value)
    except (TypeError, ValueError):
        return parse_mem(value)


def _fmt_qty(key, q):
    k = key.lower()
    if "cpu" in k:
        return _cores(q)
    if any(x in k for x in ("memory", "storage")):
        return fmt_gib(q) if q >= 2**30 else _mi(q)
    return f"{q:.0f}"


def section_namespaces(rep, ctx):
    """Per namespace: pods used (running / total) against what is CONFIGURED - the desired replicas of its
    Deployments, StatefulSets and DaemonSets (plus standalone pods) - and against its ResourceQuota."""
    rep.section("6. NAMESPACES - PODS USED vs CONFIGURED")
    pods = items(ctx.data.get("pods"))
    if not pods:
        rep.add("No pod data.")
        return
    usage = ctx.data.get("pod_usage") or {}

    rs_owner = {}
    for r in items(ctx.data.get("replicasets")):
        dep = next((o["name"] for o in (r["metadata"].get("ownerReferences") or []) if o.get("kind") == "Deployment"), None)
        rs_owner[(r["metadata"]["namespace"], r["metadata"]["name"])] = dep

    ns = defaultdict(lambda: {"total": 0, "phases": Counter(), "running_cfg": 0, "job_pods": 0, "restarts": 0, "desired": 0,
                              "cpu_use": 0.0, "mem_use": 0.0, "disk_use": 0.0, "has_use": False,
                              "cpu_req": 0.0, "mem_req": 0.0})
    wl_running = Counter()
    for p in pods:
        meta, st = p["metadata"], p.get("status", {})
        n, name = meta["namespace"], meta["name"]
        d = ns[n]
        phase = st.get("phase", "Unknown")
        d["total"] += 1
        d["phases"][phase] += 1
        d["restarts"] += sum(cs.get("restartCount", 0) for cs in st.get("containerStatuses") or [])
        owners = meta.get("ownerReferences") or []
        owner = next((o for o in owners if o.get("controller")), owners[0] if owners else None)
        if owner:
            kind, oname = owner.get("kind"), owner.get("name")
            if kind == "ReplicaSet" and rs_owner.get((n, oname)):
                kind, oname = "Deployment", rs_owner[(n, oname)]
        else:
            kind, oname = "Pod", name
            d["desired"] += 1                      # a standalone pod is configured by itself
        if kind == "Job":
            d["job_pods"] += 1
        if phase == "Running":
            wl_running[(n, kind, oname)] += 1
            if kind in ("Deployment", "ReplicaSet", "StatefulSet", "DaemonSet", "Pod"):
                d["running_cfg"] += 1
        if phase in ("Running", "Pending"):
            r = _pod_resources(p)
            d["cpu_req"] += r["cpu_req"]
            d["mem_req"] += r["mem_req"]
        u = usage.get((n, name))
        if u:
            d["has_use"] = True
            d["cpu_use"] += u.get("cpu") or 0
            d["mem_use"] += u.get("mem") or 0
            d["disk_use"] += u.get("disk") or 0

    # what is configured: desired replicas of the controllers
    hpa = {(h["metadata"]["namespace"], (h.get("spec", {}).get("scaleTargetRef") or {}).get("name")): h
           for h in items(ctx.data.get("hpa"))}
    wl_rows = []
    for kind, key in (("Deployment", "deployments"), ("StatefulSet", "statefulsets"), ("DaemonSet", "daemonsets")):
        for w in items(ctx.data.get(key)):
            meta, spec, st = w["metadata"], w.get("spec", {}), w.get("status", {})
            n, name = meta["namespace"], meta["name"]
            if kind == "DaemonSet":
                desired, ready = st.get("desiredNumberScheduled", 0), st.get("numberReady", 0)
                avail = st.get("numberAvailable", ready)
            else:
                desired, ready = spec.get("replicas", 1), st.get("readyReplicas", 0)
                avail = st.get("availableReplicas", ready)
            ns[n]["desired"] += desired
            h = hpa.get((n, name))
            hpa_text = (f"{h.get('spec', {}).get('minReplicas', 1)}-{h.get('spec', {}).get('maxReplicas', '?')} "
                        f"(now {h.get('status', {}).get('currentReplicas', '?')})") if h else "-"
            wl_rows.append([n, kind, name, desired, ready, avail, wl_running[(n, kind, name)], hpa_text,
                            "OK" if ready >= desired else f"{desired - ready} not ready"])

    # ResourceQuota
    quotas = defaultdict(list)       # namespace -> [(quota, resource, used, hard)]
    for q in items(ctx.data.get("resourcequotas")):
        qn = q["metadata"]["namespace"]
        hard, used = q.get("status", {}).get("hard") or {}, q.get("status", {}).get("used") or {}
        for key, hv in hard.items():
            quotas[qn].append((q["metadata"]["name"], key, _qty(key, used.get(key, 0)), _qty(key, hv)))
            ns[qn]                     # make sure the namespace appears

    nodes = items(ctx.data.get("nodes"))
    capacity = sum(int(parse_cpu((n.get("status", {}).get("allocatable") or {}).get("pods"))) for n in nodes)
    active = sum(d["phases"]["Running"] + d["phases"]["Pending"] for d in ns.values())
    running_all = sum(d["phases"]["Running"] for d in ns.values())
    desired_all = sum(d["desired"] for d in ns.values())
    rep.add(f"Cluster: {len(ns)} namespace(s), {len(pods)} pod(s) in total, {running_all} running; "
            f"{desired_all} pod(s) configured by workloads (desired replicas + standalone pods)."
            + (f" Node pod capacity: {active}/{capacity} used ({_fp(_pct(active, capacity))})." if capacity else ""))
    if capacity and _pct(active, capacity) is not None and _pct(active, capacity) > 85:
        ctx.find("MED", f"Cluster is using {_pct(active, capacity):.0f}% of its node pod capacity ({active}/{capacity})")

    owners = []
    for n in sorted([x["metadata"]["name"] for x in items(ctx.data.get("namespaces"))] or list(ns)):
        d = ns.get(n)
        owners.append([n, support_of(ctx, n) or "NOT SET", d["total"] if d else 0, d["phases"]["Running"] if d else 0])
    rep.add("")
    rep.add(f"Who to contact for each namespace (label '{SUPPORT_LABEL}'; same as: kubectl get namespaces -l {SUPPORT_LABEL} "
            f"-o custom-columns=NAME:.metadata.name,SUPPORT_DL:.metadata.labels.{SUPPORT_LABEL}):")
    rep.table(["NAMESPACE", f"SUPPORT DL", "PODS", "RUNNING"], owners, maxw=70)
    unlabeled = [r[0] for r in owners if r[1] == "NOT SET" and r[2] > 0]
    if unlabeled:
        ctx.find("INFO", f"{len(unlabeled)} namespace(s) with pods have no '{SUPPORT_LABEL}' label, so there is no team to contact: "
                         + ", ".join(unlabeled[:8]) + (" ..." if len(unlabeled) > 8 else ""))

    rows = []
    for n, d in sorted(ns.items(), key=lambda kv: -kv[1]["total"]):
        running, missing = d["phases"]["Running"], max(0, d["desired"] - d["running_cfg"])
        pod_quota = next(((u, h) for qname, key, u, h in quotas.get(n, []) if key in ("pods", "count/pods")), None)
        notes = []
        if missing:
            notes.append(f"{missing} configured pod(s) NOT running")
            ctx.find("HIGH" if d["running_cfg"] == 0 else "MED",
                     f"Namespace {n}{support_suffix(ctx, [n])}: only {d['running_cfg']} of {d['desired']} configured pod(s) running")
            ctx.ns_issue(n, f"only {d['running_cfg']} of {d['desired']} configured pod(s) running")
        if pod_quota and _pct(pod_quota[0], pod_quota[1]) is not None and _pct(pod_quota[0], pod_quota[1]) >= 90:
            notes.append("POD QUOTA nearly full")
        rows.append([n, support_of(ctx, n) or "-", d["total"], running, d["phases"]["Pending"], d["phases"]["Failed"] + d["phases"]["Unknown"],
                     d["phases"]["Succeeded"], d["desired"], "OK" if not missing else f"{missing} missing",
                     (f"{pod_quota[0]:.0f}/{pod_quota[1]:.0f} ({_fp(_pct(pod_quota[0], pod_quota[1]))})" if pod_quota else "-"),
                     "; ".join(notes)])
    rep.add("")
    rep.add("Pods used vs configured per namespace (CONFIGURED = desired replicas of Deployments / StatefulSets / DaemonSets + standalone pods;")
    rep.add("QUOTA = pods used/limit from the namespace's ResourceQuota, if it has one):")
    rep.table(["NAMESPACE", "SUPPORT DL", "PODS", "RUNNING", "PENDING", "FAILED", "COMPLETED", "CONFIGURED", "STATUS", "POD QUOTA", "NOTES"], rows, maxw=60)

    res_rows = []
    for n, d in sorted(ns.items(), key=lambda kv: -kv[1]["mem_use"]):
        if not d["total"]:
            continue
        res_rows.append([n, support_of(ctx, n) or "-", d["total"], _cores(d["cpu_use"]) if d["has_use"] else "n/a", _cores(d["cpu_req"]) if d["cpu_req"] else "-",
                         _mi(d["mem_use"]) if d["has_use"] else "n/a", _mi(d["mem_req"]) if d["mem_req"] else "-",
                         _mi(d["disk_use"]) if d["has_use"] and d["disk_use"] else "n/a", d["restarts"]])
    rep.add("")
    rep.add("Resources used by each namespace's pods (USE = live, REQ = requested):")
    rep.table(["NAMESPACE", "SUPPORT DL", "PODS", "CPU use", "CPU req", "MEM use", "MEM req", "DISK use", "RESTARTS"], res_rows)

    wl_rows.sort(key=lambda r: (r[8] == "OK", r[0], r[2]))
    if wl_rows:
        rep.add("")
        rep.add(f"Workloads behind those pods ({len(wl_rows)}; DESIRED = configured replicas, RUNNING = pods running now):")
        rep.table(["NAMESPACE", "SUPPORT DL", "KIND", "NAME", "DESIRED", "READY", "AVAILABLE", "RUNNING", "HPA min-max", "STATUS"],
                  [[r[0], support_of(ctx, r[0]) or "-"] + r[1:] for r in wl_rows])

    quota_rows = []
    for n in sorted(quotas):
        for qname, key, used, hard in quotas[n]:
            pct = _pct(used, hard)
            if pct is not None and pct >= 75:
                ctx.find("HIGH" if pct >= 90 else "MED", f"Namespace {n}{support_suffix(ctx, [n])}: quota '{qname}' {key} is {pct:.0f}% used ({_fmt_qty(key, used)}/{_fmt_qty(key, hard)})")
                ctx.ns_issue(n, f"quota '{qname}' {key} is {pct:.0f}% used")
            quota_rows.append([n, support_of(ctx, n) or "-", qname, key, f"{_fmt_qty(key, used)}/{_fmt_qty(key, hard)} ({_fp(pct)})"])
    if quota_rows:
        rep.add("")
        rep.add("Resource quotas (used/limit):")
        rep.table(["NAMESPACE", "SUPPORT DL", "QUOTA", "RESOURCE", "USED / LIMIT"], quota_rows)


def pod_analysis(pod, ctx):
    """Return a dict describing the pod's health, or None if it is fine."""
    meta, spec, st = pod["metadata"], pod.get("spec", {}), pod.get("status", {})
    ns, name = meta.get("namespace", "?"), meta["name"]
    phase = st.get("phase", "Unknown")
    if phase == "Succeeded":
        return None
    problems, restarts, last_restart = [], 0, None
    ready_c = total_c = 0
    status = phase
    if meta.get("deletionTimestamp"):
        status = "Terminating"
    if st.get("reason") == "Evicted":
        status = "Evicted"
        problems.append("Evicted: " + (st.get("message") or "")[:100])
    statuses = (st.get("initContainerStatuses") or []) + (st.get("containerStatuses") or [])
    for cs in statuses:
        is_init = cs in (st.get("initContainerStatuses") or [])
        if not is_init:
            total_c += 1
            ready_c += 1 if cs.get("ready") else 0
        restarts += cs.get("restartCount", 0)
        state = cs.get("state") or {}
        if "waiting" in state:
            reason = state["waiting"].get("reason", "")
            if reason and reason not in WAITING_OK:
                status = reason
                problems.append(f"{cs['name']} waiting: {reason} {(state['waiting'].get('message') or '')[:80]}".strip())
        term = state.get("terminated")
        if term and term.get("exitCode", 0) != 0:
            status = term.get("reason") or status
            problems.append(f"{cs['name']} terminated exit {term.get('exitCode')} ({term.get('reason', '')})")
        last = (cs.get("lastState") or {}).get("terminated")
        if last:
            fin = parse_ts(last.get("finishedAt"))
            if fin and fin >= ctx.since:
                if not last_restart or fin > last_restart[0]:
                    last_restart = (fin, f"{cs['name']}: {last.get('reason', 'terminated')} (exit {last.get('exitCode')})")
                ctx.happened(fin, f"POD {ns}/{name}: container {cs['name']} terminated - "
                                  f"{last.get('reason', '')} exit {last.get('exitCode')}")
                problems.append(f"restarted {age(fin, ctx.now)} ago: {last.get('reason', '')} exit {last.get('exitCode')}")
            if last.get("reason") == "OOMKilled" and fin and fin >= ctx.since:
                problems.append(f"{cs['name']} OOMKilled")
    if phase == "Pending":
        for c in st.get("conditions", []):
            if c.get("type") == "PodScheduled" and c.get("status") == "False":
                problems.append(f"unschedulable: {(c.get('message') or c.get('reason') or '')[:140]}")
    elif phase in ("Failed", "Unknown"):
        problems.append(f"phase {phase}")
    elif phase == "Running" and total_c and ready_c < total_c:
        problems.append(f"not ready ({ready_c}/{total_c})")
    created = parse_ts(meta.get("creationTimestamp"))
    if created and created >= ctx.since:
        ctx.happened(created, f"POD {ns}/{name}: created ({phase})")
    if not problems:
        return None
    return {
        "ns": ns, "name": name, "status": status, "ready": f"{ready_c}/{total_c}", "restarts": restarts,
        "last_restart": (f"{age(last_restart[0], ctx.now)} ago {last_restart[1]}" if last_restart else "-"),
        "node": spec.get("nodeName") or "<none>", "age": age(created, ctx.now), "problems": problems,
        "pod": pod, "restarted_in_window": bool(last_restart),
    }


def section_pods(rep, ctx):
    rep.section("7. UNHEALTHY PODS")
    pods = items(ctx.data.get("pods"))
    if not pods:
        rep.add("No pod data.")
        return
    phases = Counter(p.get("status", {}).get("phase", "Unknown") for p in pods)
    rep.add(f"{len(pods)} pod(s): " + ", ".join(f"{k}={v}" for k, v in sorted(phases.items())))
    bad = [a for a in (pod_analysis(p, ctx) for p in pods) if a]
    if not bad:
        rep.add("No unhealthy pods found.")
        return
    order = {"CrashLoopBackOff": 0, "OOMKilled": 1, "Error": 2, "Evicted": 3, "ImagePullBackOff": 4,
             "ErrImagePull": 4, "Pending": 5}
    bad.sort(key=lambda a: (order.get(a["status"], 9), -a["restarts"]))
    ctx.problem_pods = bad
    for a in bad:
        ctx.ns_issue(a["ns"], f"pod {a['name']} is {a['status']}")
    rep.add(f"{len(bad)} pod(s) with problems:")
    rep.table(["NAMESPACE/POD", "SUPPORT DL", "STATUS", "READY", "RESTARTS", "NODE", "AGE", "WHY"],
              [[f"{a['ns']}/{a['name']}", support_of(ctx, a["ns"]) or "-", a["status"], a["ready"], a["restarts"], node_tag(ctx, a["node"]), a["age"],
                "; ".join(dict.fromkeys(a["problems"]))[:200]] for a in bad], maxw=110)
    by_status = Counter(a["status"] for a in bad)
    ctx.find("HIGH" if any(s in by_status for s in ("CrashLoopBackOff", "OOMKilled", "Error", "Evicted")) else "MED",
             "Unhealthy pods: " + ", ".join(f"{v} {k}" for k, v in by_status.most_common()) + support_suffix(ctx, {a["ns"] for a in bad}))
    oom = [a for a in bad if any("OOMKilled" in p for p in a["problems"])]
    if oom:
        ctx.find("HIGH", f"OOMKilled in window: {', '.join(a['ns'] + '/' + a['name'] for a in oom[:5])}" + support_suffix(ctx, {a["ns"] for a in oom}))
    pending = [a for a in bad if a["status"] == "Pending"]
    if pending:
        ctx.find("HIGH", f"{len(pending)} Pending pod(s) - see scheduling reasons above" + support_suffix(ctx, {a["ns"] for a in pending}))


def _event_time(e):
    return (parse_ts(e.get("lastTimestamp")) or parse_ts(e.get("eventTime"))
            or parse_ts((e.get("series") or {}).get("lastObservedTime"))
            or parse_ts(e.get("metadata", {}).get("creationTimestamp")))


def section_events(rep, ctx):
    rep.section(f"8. EVENTS (last {ctx.minutes} min)")
    events = items(ctx.data.get("events"))
    recent = [(t, e) for e in events for t in [_event_time(e)] if t and t >= ctx.since]
    if not events:
        rep.add("No event data (events expire after ~1h by default).")
        return
    warnings = [(t, e) for t, e in recent if e.get("type") == "Warning"]
    rep.add(f"{len(recent)} event(s) in window, {len(warnings)} Warning.")
    if warnings:
        count = Counter()
        for t, e in warnings:
            count[e.get("reason", "?")] += (e.get("series") or {}).get("count") or e.get("count") or 1
        rep.add("")
        rep.add("Warning events by reason:")
        rep.table(["REASON", "OCCURRENCES"], [[r, c] for r, c in count.most_common()])
        rep.add("")
        rep.add(f"Latest Warning events (up to {MAX_EVENTS}):")
        rows = []
        for t, e in sorted(warnings, key=lambda x: x[0], reverse=True)[:MAX_EVENTS]:
            obj = e.get("involvedObject") or e.get("regarding") or {}
            obj_name = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else obj.get("name", "?")
            rows.append([age(t, ctx.now) + " ago", e.get("reason", "?"),
                         f"{obj.get('kind', '?')} {obj.get('namespace', '')}/{obj_name}".replace(" /", " "),
                         support_of(ctx, obj.get("namespace")) or "-",
                         ((e.get("series") or {}).get("count") or e.get("count") or 1),
                         (e.get("message") or e.get("note") or "").replace("\n", " ")[:140]])
        rep.table(["WHEN", "REASON", "OBJECT", "SUPPORT DL", "COUNT", "MESSAGE"], rows, limit=MAX_EVENTS)
        top = ", ".join(f"{r} x{c}" for r, c in count.most_common(4))
        ctx.find("MED", f"{len(warnings)} Warning events in window (top: {top})")
        for t, e in warnings:
            obj = e.get("involvedObject") or e.get("regarding") or {}
            ctx.happened(t, f"EVENT {e.get('reason', '?')} on {obj.get('kind', '?')} "
                            f"{(obj['namespace'] + '/') if obj.get('namespace') else ''}"
                            f"{node_tag(ctx, obj.get('name')) if obj.get('kind') == 'Node' else obj.get('name', '?')}: "
                            f"{(e.get('message') or e.get('note') or '')[:110]}")
    notable = [(t, e) for t, e in recent if e.get("type") != "Warning" and e.get("reason") in NOTABLE_NORMAL_REASONS]
    if notable:
        rep.add("")
        rep.add("Notable Normal events (scaling, kills, node changes):")
        rows = []
        for t, e in sorted(notable, key=lambda x: x[0], reverse=True)[:25]:
            obj = e.get("involvedObject") or {}
            shown = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else obj.get("name", "?")
            rows.append([age(t, ctx.now) + " ago", e.get("reason"), f"{obj.get('kind', '?')} {shown}",
                         support_of(ctx, obj.get("namespace")) or "-",
                         (e.get("message") or "").replace("\n", " ")[:110]])
            ctx.happened(t, f"EVENT(normal) {e.get('reason')} {obj.get('kind', '?')} {shown}: "
                            f"{(e.get('message') or '')[:90]}")
        rep.table(["WHEN", "REASON", "OBJECT", "SUPPORT DL", "MESSAGE"], rows, limit=25)


def section_workloads(rep, ctx):
    rep.section("9. WORKLOADS")
    rows = []
    for d in items(ctx.data.get("deployments")):
        meta, spec, st = d["metadata"], d.get("spec", {}), d.get("status", {})
        want, ready = spec.get("replicas", 1), st.get("readyReplicas", 0)
        conds = {c["type"]: c for c in st.get("conditions", [])}
        issues = []
        if ready < want:
            issues.append(f"ready {ready}/{want}")
        if conds.get("Progressing", {}).get("status") == "False":
            issues.append("Progressing=False: " + (conds["Progressing"].get("reason") or ""))
        if conds.get("Available", {}).get("status") == "False":
            issues.append("Available=False")
        if issues:
            rows.append(["Deployment", f"{meta['namespace']}/{meta['name']}", f"{ready}/{want}", "; ".join(issues)])
    for s in items(ctx.data.get("statefulsets")):
        meta, st = s["metadata"], s.get("status", {})
        want, ready = s.get("spec", {}).get("replicas", 1), st.get("readyReplicas", 0)
        if ready < want:
            rows.append(["StatefulSet", f"{meta['namespace']}/{meta['name']}", f"{ready}/{want}", f"ready {ready}/{want}"])
    for ds in items(ctx.data.get("daemonsets")):
        meta, st = ds["metadata"], ds.get("status", {})
        want, ready = st.get("desiredNumberScheduled", 0), st.get("numberReady", 0)
        if ready < want or st.get("numberMisscheduled"):
            rows.append(["DaemonSet", f"{meta['namespace']}/{meta['name']}", f"{ready}/{want}",
                         f"ready {ready}/{want}, misscheduled {st.get('numberMisscheduled', 0)}"])
    if rows:
        rep.add(f"{len(rows)} workload(s) not fully ready:")
        for r in rows:
            ctx.ns_issue(r[1].split("/")[0], f"{r[0]} {r[1].split('/', 1)[1]} not fully ready ({r[2]})")
        rep.table(["KIND", "NAMESPACE/NAME", "SUPPORT DL", "READY", "ISSUE"],
                  [[r[0], r[1], support_of(ctx, r[1].split("/")[0]) or "-", r[2], r[3]] for r in rows])
        ctx.find("HIGH", f"{len(rows)} workload(s) not fully ready (e.g. {rows[0][1]})" + support_suffix(ctx, {r[1].split("/")[0] for r in rows}))
    else:
        rep.add("All Deployments / StatefulSets / DaemonSets are fully ready.")

    recent = []
    for r in items(ctx.data.get("replicasets")):
        created = parse_ts(r["metadata"].get("creationTimestamp"))
        if created and created >= ctx.since:
            owner = ((r["metadata"].get("ownerReferences") or [{}])[0]).get("name", "-")
            recent.append([f"{r['metadata']['namespace']}/{owner}", support_of(ctx, r["metadata"]["namespace"]) or "-", r["metadata"]["name"],
                           f"{r.get('status', {}).get('readyReplicas', 0)}/{r.get('spec', {}).get('replicas', 0)}",
                           age(created, ctx.now) + " ago"])
            ctx.happened(created, f"ROLLOUT {r['metadata']['namespace']}/{owner}: new ReplicaSet {r['metadata']['name']}")
    if recent:
        rep.add("")
        rep.add(f"Recent rollouts / scale changes (new ReplicaSets in the last {ctx.minutes} min):")
        rep.table(["DEPLOYMENT", "SUPPORT DL", "REPLICASET", "READY", "CREATED"], recent)
        ctx.find("INFO", f"{len(recent)} new ReplicaSet(s) in window (deployments/rollouts)")

    failed = []
    for j in items(ctx.data.get("jobs")):
        st = j.get("status", {})
        for c in st.get("conditions", []) or []:
            if c.get("type") == "Failed" and c.get("status") == "True":
                t = parse_ts(c.get("lastTransitionTime"))
                if t and t >= ctx.since:
                    failed.append([f"{j['metadata']['namespace']}/{j['metadata']['name']}", support_of(ctx, j['metadata']['namespace']) or "-", st.get("failed", 0),
                                   c.get("reason", ""), age(t, ctx.now) + " ago"])
                    ctx.happened(t, f"JOB {j['metadata']['namespace']}/{j['metadata']['name']} FAILED ({c.get('reason', '')})")
    if failed:
        rep.add("")
        rep.add("Jobs that failed in the window:")
        rep.table(["JOB", "SUPPORT DL", "FAILED PODS", "REASON", "WHEN"], failed)
        for r in failed:
            ctx.ns_issue(r[0].split("/")[0], f"job {r[0].split('/', 1)[1]} failed ({r[3]})")
        ctx.find("HIGH", f"{len(failed)} Job(s) failed in window" + support_suffix(ctx, {r[0].split("/")[0] for r in failed}))

    rep.add("")
    rep.add("Core add-ons (kube-system):")
    core = []
    for kind, key in (("Deployment", "deployments"), ("DaemonSet", "daemonsets")):
        for w in items(ctx.data.get(key)):
            if w["metadata"]["namespace"] != "kube-system":
                continue
            st = w.get("status", {})
            if kind == "Deployment":
                want, ready = w.get("spec", {}).get("replicas", 1), st.get("readyReplicas", 0)
            else:
                want, ready = st.get("desiredNumberScheduled", 0), st.get("numberReady", 0)
            core.append([kind, w["metadata"]["name"], f"{ready}/{want}", "OK" if ready >= want else "DEGRADED"])
            if ready < want:
                ctx.find("HIGH", f"Core add-on kube-system/{w['metadata']['name']} degraded ({ready}/{want})")
    rep.table(["KIND", "NAME", "READY", "STATE"], core)


# ---------------------------------------------------------------------------
# Network & traffic: CNI / DNS / services / ingress / policies / VPC, and traffic in the selected window
# ---------------------------------------------------------------------------

import ipaddress

TRAFFIC_SAMPLE_SECONDS = 10      # live traffic sample from the kubelet (0 = skip); the WINDOW traffic comes from CloudWatch
NET_EVENT_PATTERN = re.compile(
    r"network|cni|sandbox|ip address|insufficientfreeaddresses|\bdns\b|\beni\b|\broute|loadbalancer|"
    r"connection refused|i/o timeout|no route|unreachable|failed to (assign|allocate)|securitygroup", re.I)
CNI_SETTINGS = [
    ("ENABLE_PREFIX_DELEGATION", "false", "assign /28 prefixes instead of single IPs: many more pods per node"),
    ("WARM_IP_TARGET", "-", "free IPs kept ready on each node (high = more IPs consumed)"),
    ("WARM_ENI_TARGET", "1", "spare ENIs kept attached (each holds a full set of VPC IPs)"),
    ("MINIMUM_IP_TARGET", "-", "minimum IPs allocated per node"),
    ("WARM_PREFIX_TARGET", "1", "spare /28 prefixes (with prefix delegation)"),
    ("AWS_VPC_K8S_CNI_CUSTOM_NETWORK_CFG", "false", "pods use secondary-CIDR subnets (ENIConfig)"),
    ("ENABLE_POD_ENI", "false", "security groups for pods"),
    ("AWS_VPC_K8S_CNI_EXTERNALSNAT", "false", "false = pod traffic leaving the VPC is SNAT'd to the node IP"),
    ("NETWORK_POLICY_ENFORCING_MODE", "-", "VPC CNI network policy engine (standard / strict)"),
    ("POD_SECURITY_GROUP_ENFORCING_MODE", "-", "how pod security groups are enforced"),
    ("AWS_VPC_K8S_CNI_VETHPREFIX", "eni", "host-side veth prefix"),
]


def _fmt_bytes(b):
    if b is None:
        return "n/a"
    for unit, size in (("GiB", 2**30), ("MiB", 2**20), ("KiB", 2**10)):
        if b >= size:
            return f"{b / size:.2f} {unit}" if unit == "GiB" else f"{b / size:.1f} {unit}"
    return f"{b:.0f} B"


def _fmt_rate(b):
    return "n/a" if b is None else _fmt_bytes(b) + "/s"


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _kobj(args):
    """kubectl get <one object> -o json, or None."""
    data, _err = kjson(args)
    return data


def _net_cluster_settings(rep, ctx):
    rep.add("")
    rep.add("CLUSTER NETWORK SETTINGS")
    cluster = ctx.data.get("aws_cluster") or {}
    net = cluster.get("kubernetesNetworkConfig", {})
    services = {(s["metadata"]["namespace"], s["metadata"]["name"]): s for s in items(ctx.data.get("services"))}
    dns_svc = services.get(("kube-system", "kube-dns"), {})
    k8s_svc = services.get(("default", "kubernetes"), {})
    rep.add(f"  Service CIDR     : {net.get('serviceIpv4Cidr') or '(AWS section off) kubernetes service IP ' + (k8s_svc.get('spec', {}).get('clusterIP') or '?')}"
            f"   ip family: {net.get('ipFamily', '-')}")
    rep.add(f"  DNS (kube-dns)   : service IP {dns_svc.get('spec', {}).get('clusterIP', '?')}  ports "
            f"{','.join(str(p.get('port')) + '/' + p.get('protocol', '') for p in dns_svc.get('spec', {}).get('ports', [])) or '?'}")
    pod_cidrs = sorted({c for n in items(ctx.data.get("nodes")) for c in (n.get("spec", {}).get("podCIDRs") or [n.get("spec", {}).get("podCIDR")]) if c})
    rep.add(f"  Pod CIDRs (nodes): {', '.join(pod_cidrs[:6]) + (' ...' if len(pod_cidrs) > 6 else '') if pod_cidrs else 'none set - pods take IPs straight from the VPC subnets (VPC CNI)'}")

    # kube-proxy mode
    mode = None
    cm = _kobj(["get", "configmap", "kube-proxy-config", "-n", "kube-system"])
    if cm:
        m = re.search(r"^\s*mode:\s*\"?([\w-]*)\"?", (cm.get("data") or {}).get("config", ""), re.M)
        mode = (m.group(1) if m else "") or "iptables (default)"
    rep.add(f"  kube-proxy mode  : {mode or 'unknown (kube-proxy-config not readable)'}")

    # CoreDNS
    core = next((d for d in items(ctx.data.get("deployments")) if d["metadata"]["namespace"] == "kube-system" and d["metadata"]["name"] == "coredns"), None)
    if core:
        rep.add(f"  CoreDNS          : {core.get('status', {}).get('readyReplicas', 0)}/{core.get('spec', {}).get('replicas', '?')} ready")
    corefile = ((_kobj(["get", "configmap", "coredns", "-n", "kube-system"]) or {}).get("data") or {}).get("Corefile", "")
    forwards = re.findall(r"forward\s+\.\s+(\S+)", corefile)
    if forwards:
        rep.add(f"  CoreDNS upstream : {', '.join(forwards)}")

    # VPC CNI
    ds = _kobj(["get", "daemonset", "aws-node", "-n", "kube-system"])
    if not ds:
        rep.add("  CNI              : aws-node (VPC CNI) not found - this cluster may use another CNI")
        ctx.find("INFO", "aws-node (Amazon VPC CNI) DaemonSet not found - another CNI may be in use")
        return
    cont = next((c for c in ds.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []) if c.get("name") == "aws-node"), {})
    env = {e["name"]: e.get("value", "(from secret/configmap)") for e in cont.get("env", []) if "name" in e}
    st = ds.get("status", {})
    rep.add(f"  CNI              : Amazon VPC CNI  image {cont.get('image', '?').split('/')[-1]}  "
            f"ready {st.get('numberReady', 0)}/{st.get('desiredNumberScheduled', 0)}")
    rows = [[k, env.get(k, f"{d} (default)" if d != "-" else "not set"), why] for k, d, why in CNI_SETTINGS]
    rep.add("")
    rep.add("VPC CNI (aws-node) settings that decide how many IPs / pods a node can hold:")
    rep.table(["SETTING", "VALUE", "WHAT IT DOES"], rows, maxw=90)


def _net_services_ingress_policies(rep, ctx):
    services = items(ctx.data.get("services"))
    by_type = Counter(s.get("spec", {}).get("type", "ClusterIP") for s in services)
    rep.add("")
    rep.add("SERVICES: " + ", ".join(f"{v} {k}" for k, v in by_type.most_common()) + f"  (total {len(services)})")
    rows, public = [], []
    for s in services:
        spec, meta = s.get("spec", {}), s["metadata"]
        stype = spec.get("type", "ClusterIP")
        if stype == "ClusterIP":
            continue
        ann = meta.get("annotations") or {}
        lb = (s.get("status", {}).get("loadBalancer") or {}).get("ingress") or []
        address = (lb[0].get("hostname") or lb[0].get("ip")) if lb else ("-" if stype != "ExternalName" else spec.get("externalName", "-"))
        scheme = "-"
        if stype == "LoadBalancer":
            internal = (ann.get("service.beta.kubernetes.io/aws-load-balancer-internal", "").lower() == "true"
                        or "internal" in ann.get("service.beta.kubernetes.io/aws-load-balancer-scheme", "").lower())
            scheme = "internal" if internal else "INTERNET-FACING"
            if not internal:
                public.append(f"{meta['namespace']}/{meta['name']}")
        kind = ann.get("service.beta.kubernetes.io/aws-load-balancer-type") or ("classic/NLB" if stype == "LoadBalancer" else "-")
        ports = ", ".join(f"{p.get('port')}" + (f":{p['nodePort']}" if p.get("nodePort") else "") + "/" + p.get("protocol", "TCP") for p in spec.get("ports", [])[:4])
        rows.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], stype, scheme, kind, address[:60], ports])
    if rows:
        rep.table(["NAMESPACE", "SUPPORT DL", "SERVICE", "TYPE", "EXPOSURE", "LB KIND", "EXTERNAL ADDRESS", "PORTS (port[:nodePort])"], rows, maxw=64)
    if public:
        ctx.find("INFO", f"{len(public)} LoadBalancer Service(s) are internet-facing (no internal annotation): "
                         + ", ".join(public[:6]) + (" ..." if len(public) > 6 else "")
                         + support_suffix(ctx, {x.split("/")[0] for x in public}))

    ings = items(ctx.data.get("ingresses"))
    rep.add("")
    if not ings:
        rep.add("INGRESSES: none (or not readable)")
    else:
        irows = []
        for i in ings:
            meta, spec = i["metadata"], i.get("spec", {})
            lb = (i.get("status", {}).get("loadBalancer") or {}).get("ingress") or []
            address = (lb[0].get("hostname") or lb[0].get("ip")) if lb else ""
            hosts = sorted({r.get("host") or "*" for r in spec.get("rules", []) or []})
            paths = sum(len((r.get("http") or {}).get("paths", [])) for r in spec.get("rules", []) or [])
            klass = spec.get("ingressClassName") or (meta.get("annotations") or {}).get("kubernetes.io/ingress.class") or "-"
            irows.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], klass, ", ".join(hosts)[:60],
                          address[:60] or "NO ADDRESS", "yes" if spec.get("tls") else "no", paths])
            if not address:
                ctx.find("MED", f"Ingress {meta['namespace']}/{meta['name']} has no load balancer address" + support_suffix(ctx, [meta["namespace"]]))
                ctx.ns_issue(meta["namespace"], f"ingress {meta['name']} has no address")
        rep.add(f"INGRESSES ({len(ings)}):")
        rep.table(["NAMESPACE", "SUPPORT DL", "INGRESS", "CLASS", "HOSTS", "ADDRESS", "TLS", "PATHS"], irows, maxw=64)

    pols = items(ctx.data.get("networkpolicies"))
    pods = items(ctx.data.get("pods"))
    pod_ns = Counter(p["metadata"]["namespace"] for p in pods if p.get("status", {}).get("phase") in ("Running", "Pending"))
    per_ns = defaultdict(lambda: {"n": 0, "deny_in": False, "deny_out": False})
    for pol in pols:
        d = per_ns[pol["metadata"]["namespace"]]
        d["n"] += 1
        spec = pol.get("spec", {})
        types = spec.get("policyTypes") or ["Ingress"]
        if not spec.get("podSelector"):
            if "Ingress" in types and not spec.get("ingress"):
                d["deny_in"] = True
            if "Egress" in types and not spec.get("egress"):
                d["deny_out"] = True
    rep.add("")
    prow = [[n, support_of(ctx, n) or "-", pod_ns[n], per_ns[n]["n"],
             ("ingress " if per_ns[n]["deny_in"] else "") + ("egress" if per_ns[n]["deny_out"] else "") or ("none" if not per_ns[n]["n"] else "no")]
            for n in sorted(set(pod_ns) | set(per_ns))]
    rep.add(f"NETWORK POLICIES: {len(pols)} in {len(per_ns)} namespace(s)  (DEFAULT-DENY = a policy that selects every pod and allows nothing)")
    rep.table(["NAMESPACE", "SUPPORT DL", "PODS", "POLICIES", "DEFAULT-DENY"], prow)
    open_ns = [n for n in pod_ns if not per_ns[n]["n"] and n not in ("kube-system",)]
    if open_ns:
        ctx.find("INFO", f"{len(open_ns)} namespace(s) with pods have no NetworkPolicy (all pod-to-pod traffic allowed unless a mesh/CNI restricts it): "
                         + ", ".join(sorted(open_ns)[:8]) + (" ..." if len(open_ns) > 8 else ""))


def _net_pod_ips(rep, ctx):
    pods = items(ctx.data.get("pods"))
    host_net = sum(1 for p in pods if p.get("spec", {}).get("hostNetwork"))
    with_ip = [p for p in pods if p.get("status", {}).get("podIP") and not p.get("spec", {}).get("hostNetwork")
               and p.get("status", {}).get("phase") in ("Running", "Pending")]
    rep.add("")
    rep.add(f"POD IPs: {len(with_ip)} pod(s) hold a VPC/pod IP; {host_net} pod(s) use the node's network (hostNetwork).")
    subnets = ctx.data.get("aws_subnets") or []
    if not subnets:
        rep.add("  (pod IPs per subnet needs the AWS section: subnets unknown)")
        return
    nets = []
    for sn in subnets:
        try:
            nets.append((sn, ipaddress.ip_network(sn["CidrBlock"])))
        except (KeyError, ValueError):
            pass
    used, by_ns = Counter(), defaultdict(Counter)
    for p in with_ip:
        try:
            ip = ipaddress.ip_address(p["status"]["podIP"])
        except ValueError:
            continue
        for sn, net in nets:
            if ip.version == net.version and ip in net:
                used[sn["SubnetId"]] += 1
                by_ns[sn["SubnetId"]][p["metadata"]["namespace"]] += 1
                break
    rows = []
    for sn, net in nets:
        top = ", ".join(f"{n} ({c})" for n, c in by_ns[sn["SubnetId"]].most_common(3)) or "-"
        rows.append([sn["SubnetId"], sn.get("AvailabilityZone"), sn["CidrBlock"], sn.get("AvailableIpAddressCount"), used[sn["SubnetId"]], top])
    rep.add("Pod IPs per subnet (FREE IPs = what the subnet still has; POD IPs = pods using this subnet now):")
    rep.table(["SUBNET", "AZ", "CIDR", "FREE IPs", "POD IPs", "TOP NAMESPACES"], rows, maxw=70)


def _net_events_and_dns_logs(rep, ctx):
    events = []
    for e in items(ctx.data.get("events")):
        t = _event_time(e)
        text = f"{e.get('reason', '')} {e.get('message') or e.get('note') or ''}"
        if e.get("type") == "Warning" and t and t >= ctx.since and NET_EVENT_PATTERN.search(text):
            events.append((t, e))
    rep.add("")
    if events:
        rows = []
        for t, e in sorted(events, key=lambda x: x[0], reverse=True)[:30]:
            obj = e.get("involvedObject") or e.get("regarding") or {}
            name = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else f"{(obj.get('namespace') + '/') if obj.get('namespace') else ''}{obj.get('name', '?')}"
            rows.append([age(t, ctx.now) + " ago", e.get("reason", "?"), f"{obj.get('kind', '?')} {name}", support_of(ctx, obj.get("namespace")) or "-",
                         (e.get("series") or {}).get("count") or e.get("count") or 1, (e.get("message") or e.get("note") or "").replace("\n", " ")[:130]])
            ctx.ns_issue(obj.get("namespace"), f"network warning: {e.get('reason', '?')}")
        rep.add(f"NETWORK-RELATED WARNING EVENTS in the last {ctx.minutes} min ({len(events)}):")
        rep.table(["WHEN", "REASON", "OBJECT", "SUPPORT DL", "COUNT", "MESSAGE"], rows, maxw=70)
        ctx.find("MED", f"{len(events)} network-related Warning event(s) in the window (e.g. {rows[0][1]})")
    else:
        rep.add(f"NETWORK-RELATED WARNING EVENTS in the last {ctx.minutes} min: none")

    rows = []
    for title, selector, patterns in (
            ("CoreDNS (DNS)", "k8s-app=kube-dns", {"SERVFAIL": r"SERVFAIL", "REFUSED": r"REFUSED", "timeouts": r"i/o timeout|timed out",
                                                    "NXDOMAIN": r"NXDOMAIN", "errors": r"\[ERROR\]|plugin/errors"}),
            ("aws-node (VPC CNI)", "k8s-app=aws-node", {"no free IP": r"InsufficientFreeAddressesInSubnet|no available IP|failed to assign|unable to assign",
                                                         "ENI issues": r"ENI|eni.*(fail|error)", "errors": r"\"level\":\"error\"|\berror\b|ERROR"})):
        ok, out = kubectl(["logs", "-n", "kube-system", "-l", selector, "--all-containers", "--prefix", f"--since={ctx.minutes}m",
                           "--tail=800", "--max-log-requests=20"], timeout=90)
        if not ok:
            rows.append([title, "-", "-", "unavailable: " + (out.splitlines()[0][:80] if out else "?"), ""])
            continue
        lines = out.splitlines()
        counts = {k: sum(1 for l in lines if re.search(p, l)) for k, p in patterns.items()}
        sample = next((l for l in reversed(lines) if re.search(r"error|fail|timeout|SERVFAIL|REFUSED", l, re.I)), "")
        rows.append([title, len(lines), counts.get("errors", 0), ", ".join(f"{k} {v}" for k, v in counts.items() if v and k != "errors") or "-", sample[:110]])
        if counts.get("timeouts", 0) + counts.get("SERVFAIL", 0) >= 10:
            ctx.find("MED", f"{title}: {counts.get('timeouts', 0)} timeouts and {counts.get('SERVFAIL', 0)} SERVFAIL in the window - DNS problems likely")
        if counts.get("no free IP", 0):
            ctx.find("HIGH", f"aws-node logged {counts['no free IP']} 'no free IP' errors in the window - subnet or ENI IP exhaustion")
    rep.add("")
    rep.add(f"DNS and CNI logs in the last {ctx.minutes} min (what the platform pods themselves reported):")
    rep.table(["COMPONENT", "LOG LINES", "ERROR-LIKE", "BREAKDOWN", "LATEST ERROR"], rows, maxw=70)


def _net_totals(iface_stats):
    """(rxBytes, txBytes, rxErrors, txErrors) from a kubelet 'network' stanza (interfaces list or flat)."""
    if not iface_stats:
        return None
    ifaces = iface_stats.get("interfaces")
    if ifaces:
        return (sum(i.get("rxBytes", 0) for i in ifaces), sum(i.get("txBytes", 0) for i in ifaces),
                sum(i.get("rxErrors", 0) for i in ifaces), sum(i.get("txErrors", 0) for i in ifaces))
    if "rxBytes" in iface_stats:
        return (iface_stats.get("rxBytes", 0), iface_stats.get("txBytes", 0), iface_stats.get("rxErrors", 0), iface_stats.get("txErrors", 0))
    return None


def _net_pod_traffic(rep, ctx):
    stats = ctx.data.get("node_stats") or {}
    rep.add("")
    rep.add("POD / NODE NETWORK COUNTERS (from each node's kubelet)")
    if not stats:
        rep.add("  unavailable: the kubelet stats need the 'nodes/proxy' permission. (Traffic over the window comes from CloudWatch below.)")
        return

    def snapshot(data_by_node):
        node_tot, pod_tot = {}, {}
        for node, s in data_by_node.items():
            node_tot[node] = _net_totals((s.get("node") or {}).get("network"))
            for p in s.get("pods", []) or []:
                ref = p.get("podRef", {})
                t = _net_totals(p.get("network"))
                if t:
                    pod_tot[(ref.get("namespace"), ref.get("name"))] = (t, node)
        return node_tot, pod_tot

    node_a, pod_a = snapshot(stats)
    t0, rates_node, rates_pod = time.time(), {}, {}
    if TRAFFIC_SAMPLE_SECONDS > 0:
        rep.emit(f"  sampling live traffic for {TRAFFIC_SAMPLE_SECONDS}s ...")
        time.sleep(TRAFFIC_SAMPLE_SECONDS)

        def one(node):
            ok, out = kubectl(["get", "--raw", f"/api/v1/nodes/{node}/proxy/stats/summary"], timeout=60)
            try:
                return node, json.loads(out) if ok else None
            except json.JSONDecodeError:
                return node, None
        with ThreadPoolExecutor(max_workers=8) as pool:
            fresh = {n: d for n, d in pool.map(one, list(stats)) if d}
        dt = max(1.0, time.time() - t0)
        node_b, pod_b = snapshot(fresh)
        for node, tb in node_b.items():
            ta = node_a.get(node)
            if ta and tb:
                rates_node[node] = ((tb[0] - ta[0]) / dt, (tb[1] - ta[1]) / dt)
        for key, (tb, _node) in pod_b.items():
            if key in pod_a:
                ta = pod_a[key][0]
                rates_pod[key] = ((tb[0] - ta[0]) / dt, (tb[1] - ta[1]) / dt)
        pod_a = {k: v for k, v in pod_b.items()} or pod_a
        node_a = node_b or node_a

    rows = []
    for node, t in sorted(node_a.items()):
        if not t:
            continue
        r = rates_node.get(node)
        rows.append([node_tag(ctx, node), _fmt_bytes(t[0]), _fmt_bytes(t[1]), _fmt_rate(r[0]) if r else "-", _fmt_rate(r[1]) if r else "-", t[2] + t[3]])
        if t[2] + t[3] > 0:
            ctx.find("MED", f"Node {node_tag(ctx, node)} has {t[2] + t[3]} network errors since boot (rx {t[2]}, tx {t[3]})")
    rep.add("  Per node (TOTAL = since the node booted; NOW = live rate over the sample):")
    rep.table(["NODE", "RX TOTAL", "TX TOTAL", "RX NOW", "TX NOW", "ERRORS"], rows, maxw=60)

    prow = []
    for (ns, name), (t, node) in pod_a.items():
        r = rates_pod.get((ns, name))
        prow.append(((r[0] + r[1]) if r else 0, t[0] + t[1], [f"{ns}/{name}", support_of(ctx, ns) or "-", node_tag(ctx, node), _fmt_bytes(t[0]), _fmt_bytes(t[1]),
                                                           _fmt_rate(r[0]) if r else "-", _fmt_rate(r[1]) if r else "-", t[2] + t[3]]))
    prow.sort(key=lambda x: (-x[0], -x[1]))
    rep.add("")
    rep.add("  Top pods by network traffic (TOTAL = since the pod started - not the selected window; NOW = live rate):")
    rep.table(["POD", "SUPPORT DL", "NODE", "RX TOTAL", "TX TOTAL", "RX NOW", "TX NOW", "ERRORS"], [x[2] for x in prow[:15]], maxw=60)
    ns_tot = defaultdict(lambda: [0, 0])
    for (ns, _name), (t, _node) in pod_a.items():
        ns_tot[ns][0] += t[0]
        ns_tot[ns][1] += t[1]
    rep.add("")
    rep.add("  By namespace (TOTAL since pods started):")
    rep.table(["NAMESPACE", "SUPPORT DL", "RX TOTAL", "TX TOTAL"],
              [[n, support_of(ctx, n) or "-", _fmt_bytes(v[0]), _fmt_bytes(v[1])] for n, v in sorted(ns_tot.items(), key=lambda kv: -(kv[1][0] + kv[1][1]))[:15]])


# --- AWS: VPC, routing, NAT, endpoints, ENIs, load balancers -------------------------------------------

def _aws_vpc_routing(rep, ctx, target, cluster):
    vpc_id = (cluster.get("resourcesVpcConfig") or {}).get("vpcId")
    rep.add("")
    rep.add(f"VPC {vpc_id}: ADDRESSING, ROUTING, NAT, ENDPOINTS")
    nat_ids = []
    vpcs, err = aws_cli(["ec2", "describe-vpcs", "--vpc-ids", vpc_id], target)
    if err:
        rep.add(f"  VPC details unavailable: {_first_line(err, 100)}")
    else:
        v = (vpcs.get("Vpcs") or [{}])[0]
        cidrs = [a["CidrBlock"] for a in v.get("CidrBlockAssociationSet", [])] or [v.get("CidrBlock")]
        rep.add(f"  CIDR block(s): {', '.join(c for c in cidrs if c)}"
                + (f"   IPv6: {', '.join(a['Ipv6CidrBlock'] for a in v.get('Ipv6CidrBlockAssociationSet', []))}" if v.get("Ipv6CidrBlockAssociationSet") else ""))
    rts, err = aws_cli(["ec2", "describe-route-tables", "--filters", f"Name=vpc-id,Values={vpc_id}"], target)
    subnet_rows, no_egress = [], []
    if err:
        rep.add(f"  Route tables unavailable: {_first_line(err, 100)}")
    else:
        tables = rts.get("RouteTables", [])
        main = next((t for t in tables if any(a.get("Main") for a in t.get("Associations", []))), None)
        by_subnet = {a["SubnetId"]: t for t in tables for a in t.get("Associations", []) if a.get("SubnetId")}

        def default_route(table):
            for r in (table or {}).get("Routes", []):
                if r.get("DestinationCidrBlock") == "0.0.0.0/0" and r.get("State", "active") == "active":
                    return (r.get("GatewayId") or r.get("NatGatewayId") or r.get("TransitGatewayId")
                            or r.get("VpcPeeringConnectionId") or r.get("NetworkInterfaceId") or "?")
            return None
        for sn in sorted(ctx.data.get("aws_subnets") or [], key=lambda x: x.get("AvailabilityZone", "")):
            table = by_subnet.get(sn["SubnetId"]) or main
            route = default_route(table)
            kind = "public (internet gateway)" if route and route.startswith("igw-") else ("private (NAT)" if route and route.startswith("nat-")
                    else ("private (transit/other)" if route else "ISOLATED (no default route)"))
            if not route:
                no_egress.append(sn["SubnetId"])
            subnet_rows.append([sn["SubnetId"], sn.get("AvailabilityZone"), sn["CidrBlock"], sn.get("AvailableIpAddressCount"), kind, route or "none",
                                (table or {}).get("RouteTableId", "?")])
        if subnet_rows:
            rep.table(["SUBNET", "AZ", "CIDR", "FREE IPs", "TYPE", "DEFAULT ROUTE", "ROUTE TABLE"], subnet_rows, maxw=40)
        if no_egress:
            ctx.find("HIGH", f"Subnet(s) {', '.join(no_egress)} have no default route - nodes there can't reach the internet "
                             f"(ECR, STS, ...) unless VPC endpoints exist")
    nats, err = aws_cli(["ec2", "describe-nat-gateways", "--filter", f"Name=vpc-id,Values={vpc_id}"], target)
    rep.add("")
    if err:
        rep.add(f"  NAT gateways unavailable: {_first_line(err, 100)}")
    else:
        rows = []
        for g in nats.get("NatGateways", []):
            if g.get("State") == "deleted":
                continue
            nat_ids.append(g["NatGatewayId"])
            rows.append([g["NatGatewayId"], g.get("State"), g.get("SubnetId"), (g.get("NatGatewayAddresses") or [{}])[0].get("PublicIp", "-")])
            if g.get("State") != "available":
                ctx.find("HIGH", f"NAT gateway {g['NatGatewayId']} is {g.get('State')}")
        rep.add(f"  NAT gateways ({len(rows)}):" if rows else "  NAT gateways: none")
        rep.table(["NAT GATEWAY", "STATE", "SUBNET", "PUBLIC IP"], rows)
    eps, err = aws_cli(["ec2", "describe-vpc-endpoints", "--filters", f"Name=vpc-id,Values={vpc_id}"], target)
    if err:
        rep.add(f"  VPC endpoints unavailable: {_first_line(err, 100)}")
    else:
        rows = [[e["ServiceName"].split(".", 3)[-1], e.get("VpcEndpointType"), e.get("State")] for e in eps.get("VpcEndpoints", [])]
        have = " ".join(r[0] for r in rows)
        rep.add(f"  VPC endpoints ({len(rows)}):" if rows else "  VPC endpoints: none")
        rep.table(["SERVICE", "TYPE", "STATE"], rows)
        if (no_egress or not nat_ids) and no_egress:
            missing = [s for s in ("ecr.api", "ecr.dkr", "s3", "sts", "ec2") if s not in have]
            if missing:
                ctx.find("MED", f"Isolated subnets and no VPC endpoint for: {', '.join(missing)} - nodes may fail to pull images / get credentials")
    return nat_ids


def _aws_node_enis(rep, ctx, target):
    idents = node_idents(ctx)
    ids = {i["instance_id"]: n for n, i in idents.items() if i["instance_id"].startswith("i-")}
    if not ids:
        return
    data, err = aws_cli(["ec2", "describe-network-interfaces", "--filters", "Name=attachment.instance-id,Values=" + ",".join(list(ids)[:50])], target)
    rep.add("")
    rep.add("NODE NETWORK INTERFACES (ENIs) AND IPs")
    if err:
        rep.add(f"  unavailable: {_first_line(err, 100)}")
        return
    per = defaultdict(lambda: {"enis": 0, "ips": 0, "subnets": set(), "sgs": set()})
    for ni in data.get("NetworkInterfaces", []):
        iid = (ni.get("Attachment") or {}).get("InstanceId")
        d = per[iid]
        d["enis"] += 1
        d["ips"] += len(ni.get("PrivateIpAddresses", []))
        d["subnets"].add(ni.get("SubnetId"))
        d["sgs"].update(g["GroupId"] for g in ni.get("Groups", []))
    pods = items(ctx.data.get("pods"))
    on_node = Counter(p.get("spec", {}).get("nodeName") for p in pods
                      if not p.get("spec", {}).get("hostNetwork") and p.get("status", {}).get("phase") in ("Running", "Pending"))
    rows = []
    for iid, node in sorted(ids.items(), key=lambda kv: kv[1]):
        d = per.get(iid)
        if not d:
            continue
        note = ""
        if on_node[node] > d["ips"]:
            note = "more pods than IPs"
            ctx.find("MED", f"Node {node_tag(ctx, node)} has {on_node[node]} pods but only {d['ips']} IPs on its ENIs")
        rows.append([node_tag(ctx, node), d["enis"], d["ips"], on_node[node], ", ".join(sorted(d["subnets"])), ", ".join(sorted(d["sgs"]))[:60], note])
    rep.table(["NODE", "ENIs", "PRIVATE IPs", "PODS (VPC IP)", "SUBNETS", "SECURITY GROUPS", "NOTE"], rows, maxw=64)


def _aws_load_balancers(rep, ctx, target, vpc_id):
    """Load balancers in this VPC with their target health. Returns [(kind, id_for_metrics, name, dns)]."""
    found = []
    rep.add("")
    rep.add("LOAD BALANCERS IN THE VPC")
    v2, err = aws_cli(["elbv2", "describe-load-balancers"], target, timeout=90)
    rows = []
    if err:
        rep.add(f"  ALB/NLB unavailable: {_first_line(err, 100)}")
    else:
        for lb in [x for x in v2.get("LoadBalancers", []) if x.get("VpcId") == vpc_id][:12]:
            arn = lb["LoadBalancerArn"]
            suffix = arn.split("loadbalancer/", 1)[-1]
            tgs, terr = aws_cli(["elbv2", "describe-target-groups", "--load-balancer-arn", arn], target)
            healthy = unhealthy = 0
            reasons = []
            for tg in ([] if terr else tgs.get("TargetGroups", [])):
                th, herr = aws_cli(["elbv2", "describe-target-health", "--target-group-arn", tg["TargetGroupArn"]], target)
                for d in ([] if herr else th.get("TargetHealthDescriptions", [])):
                    st = (d.get("TargetHealth") or {})
                    if st.get("State") == "healthy":
                        healthy += 1
                    elif st.get("State") in ("unhealthy", "unavailable"):
                        unhealthy += 1
                        reasons.append(st.get("Reason") or st.get("State"))
            note = ""
            if unhealthy:
                note = f"{unhealthy} UNHEALTHY target(s): " + ", ".join(dict.fromkeys(reasons))[:60]
                ctx.find("MED" if healthy else "HIGH", f"Load balancer {lb['LoadBalancerName']}: {unhealthy} unhealthy target(s), {healthy} healthy")
            rows.append([lb["LoadBalancerName"], lb.get("Type"), lb.get("Scheme"), (lb.get("State") or {}).get("Code"), healthy, unhealthy, lb.get("DNSName", "")[:50], note])
            found.append(("alb" if lb.get("Type") == "application" else "nlb", suffix, lb["LoadBalancerName"], lb.get("DNSName", "")))
    classic, err = aws_cli(["elb", "describe-load-balancers"], target, timeout=90)
    if not err:
        for lb in [x for x in classic.get("LoadBalancerDescriptions", []) if x.get("VPCId") == vpc_id][:12]:
            ih, herr = aws_cli(["elb", "describe-instance-health", "--load-balancer-name", lb["LoadBalancerName"]], target)
            states = Counter(s.get("State") for s in ([] if herr else ih.get("InstanceStates", [])))
            note = ""
            if states.get("OutOfService"):
                note = f"{states['OutOfService']} OUT OF SERVICE"
                ctx.find("MED", f"Classic load balancer {lb['LoadBalancerName']}: {states['OutOfService']} instance(s) OutOfService")
            rows.append([lb["LoadBalancerName"], "classic", lb.get("Scheme"), "active", states.get("InService", 0), states.get("OutOfService", 0), lb.get("DNSName", "")[:50], note])
            found.append(("classic", lb["LoadBalancerName"], lb["LoadBalancerName"], lb.get("DNSName", "")))
    rep.table(["LOAD BALANCER", "TYPE", "SCHEME", "STATE", "HEALTHY", "UNHEALTHY", "DNS NAME", "NOTE"], rows, maxw=56)
    return found


# --- CloudWatch: TRAFFIC in the selected window --------------------------------------------------------

def _cw_points(ctx, target, namespace, metric, dims, stat, period):
    """[(datetime, value)] from CloudWatch for the selected window, or (None, error)."""
    args = ["cloudwatch", "get-metric-statistics", "--namespace", namespace, "--metric-name", metric,
            "--dimensions", *[f"Name={k},Value={v}" for k, v in dims.items()],
            "--start-time", _iso(ctx.since), "--end-time", _iso(ctx.now), "--period", str(period), "--statistics", stat]
    data, err = aws_cli(args, target, timeout=60)
    if err:
        return None, err
    pts = sorted(((parse_ts(d.get("Timestamp")), d.get(stat)) for d in data.get("Datapoints", [])), key=lambda x: x[0] or ctx.now)
    return [(t, v) for t, v in pts if t is not None and v is not None], None


def _line(label, pts, fmt, per_second=None, total=True):
    """A series line for the HTML charts + its numbers. per_second: divide each value by this period."""
    vals = [(v / per_second) if per_second else v for _, v in pts]
    return {"l": label, "f": fmt, "p": [round(v, 3) for v in vals], "ts": [int(t.timestamp()) for t, _ in pts],
            "avg": (sum(vals) / len(vals)) if vals else None, "max": max(vals) if vals else None,
            "sum": sum(v for _, v in pts) if total else None}


def _aws_traffic(rep, ctx, target, nat_ids, lbs):
    mins = ctx.minutes
    rep.add("")
    rep.add("=" * 78)
    rep.add(f"TRAFFIC IN THE SELECTED WINDOW (last {mins} min, from CloudWatch)")
    rep.add("=" * 78)

    # ---- nodes (EC2 NetworkIn / NetworkOut; basic monitoring = 5 minute points)
    idents = node_idents(ctx)
    ids = {i["instance_id"]: n for n, i in idents.items() if i["instance_id"].startswith("i-")}
    series_rows, rows, totals = [], [], {"in": defaultdict(float), "out": defaultdict(float)}
    if ids:
        period = 300
        jobs = [(iid, m) for iid in list(ids)[:30] for m in ("NetworkIn", "NetworkOut")]

        def fetch(job):
            iid, metric = job
            return job, _cw_points(ctx, target, "AWS/EC2", metric, {"InstanceId": iid}, "Sum", period)
        results = {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            for job, (pts, err) in pool.map(fetch, jobs):
                results[job] = (pts, err)
        first_err = next((e for _, (p, e) in results.items() if e), None)
        for iid, node in sorted(ids.items(), key=lambda kv: kv[1]):
            pin, _ = results.get((iid, "NetworkIn"), (None, None))
            pout, _ = results.get((iid, "NetworkOut"), (None, None))
            if not pin and not pout:
                continue
            lin, lout = _line("in", pin or [], "Bps", period), _line("out", pout or [], "Bps", period)
            for t, v in (pin or []):
                totals["in"][t] += v / period
            for t, v in (pout or []):
                totals["out"][t] += v / period
            series_rows.append({"n": node, "s": f"{iid} | {idents[node]['zone']} | {idents[node]['type']}", "lines": [lin, lout]})
            rows.append([node_tag(ctx, node), _fmt_rate(lin["avg"]), _fmt_rate(lin["max"]), _fmt_bytes(lin["sum"]),
                         _fmt_rate(lout["avg"]), _fmt_rate(lout["max"]), _fmt_bytes(lout["sum"])])
        if not rows:
            rep.add(f"  Node traffic: no CloudWatch data returned" + (f" ({_first_line(first_err, 100)})" if first_err else "")
                    + ". EC2 network metrics need cloudwatch:GetMetricStatistics.")
        else:
            if totals["in"] or totals["out"]:
                keys = sorted(set(totals["in"]) | set(totals["out"]))
                allin = _line("in", [(k, totals["in"].get(k, 0.0)) for k in keys], "Bps", None, total=False)
                allout = _line("out", [(k, totals["out"].get(k, 0.0)) for k in keys], "Bps", None, total=False)
                series_rows.insert(0, {"n": "ALL NODES (sum)", "s": f"{len(rows)} nodes", "lines": [allin, allout]})
                rep.add(f"  All nodes together: in avg {_fmt_rate(allin['avg'])} peak {_fmt_rate(allin['max'])}; "
                        f"out avg {_fmt_rate(allout['avg'])} peak {_fmt_rate(allout['max'])}")
            rep.add(f"  Per node over the window (EC2 NetworkIn / NetworkOut, {period // 60}-minute points; avg/peak are rates, TOTAL is bytes in the window):")
            rep.table(["NODE", "IN avg", "IN peak", "IN total", "OUT avg", "OUT peak", "OUT total"], rows, maxw=60)
            busiest = max(series_rows[1:] or series_rows, key=lambda r: (r["lines"][1]["max"] or 0) + (r["lines"][0]["max"] or 0))
            ctx.find("INFO", f"Busiest node on the network in the window: {busiest['n']} (peak in {_fmt_rate(busiest['lines'][0]['max'])}, out {_fmt_rate(busiest['lines'][1]['max'])})")
    rep.series(f"Node network traffic, last {mins} min (bytes per second)", series_rows,
               "EC2 NetworkIn / NetworkOut. Basic monitoring gives one point per 5 minutes, so short windows show few points.")

    period = 60 if mins <= 360 else 300
    # ---- load balancers
    lb_series, lb_rows = [], []
    for kind, ident, name, dns in lbs[:10]:
        if kind == "alb":
            ns_, dim = "AWS/ApplicationELB", {"LoadBalancer": ident}
            jobs = [("req", "RequestCount", "Sum"), ("2xx", "HTTPCode_Target_2XX_Count", "Sum"), ("4xx", "HTTPCode_Target_4XX_Count", "Sum"),
                    ("5xx", "HTTPCode_Target_5XX_Count", "Sum"), ("elb5xx", "HTTPCode_ELB_5XX_Count", "Sum"),
                    ("lat", "TargetResponseTime", "Average"), ("bytes", "ProcessedBytes", "Sum")]
        elif kind == "nlb":
            ns_, dim = "AWS/NetworkELB", {"LoadBalancer": ident}
            jobs = [("flows", "NewFlowCount", "Sum"), ("active", "ActiveFlowCount", "Average"), ("bytes", "ProcessedBytes", "Sum"),
                    ("rst", "TCP_Target_Reset_Count", "Sum")]
        else:
            ns_, dim = "AWS/ELB", {"LoadBalancerName": ident}
            jobs = [("req", "RequestCount", "Sum"), ("2xx", "HTTPCode_Backend_2XX", "Sum"), ("4xx", "HTTPCode_Backend_4XX", "Sum"),
                    ("5xx", "HTTPCode_Backend_5XX", "Sum"), ("elb5xx", "HTTPCode_ELB_5XX", "Sum"), ("lat", "Latency", "Average"), ("bytes", "EstimatedProcessedBytes", "Sum")]
        got = {}
        for key, metric, stat in jobs:
            pts, err = _cw_points(ctx, target, ns_, metric, dim, stat, period)
            if pts is not None:
                got[key] = pts
        if not got:
            lb_rows.append([name, kind, "no CloudWatch data", "", "", "", "", ""])
            continue
        tot = lambda k: sum(v for _, v in got.get(k, []))
        req, e4, e5 = tot("req"), tot("4xx"), tot("5xx") + tot("elb5xx")
        lat = [v for _, v in got.get("lat", [])]
        err_pct = (100 * e5 / req) if req else 0.0
        lb_rows.append([name, kind, f"{req:.0f}" if kind != "nlb" else f"{tot('flows'):.0f} flows", f"{tot('2xx'):.0f}", f"{e4:.0f}", f"{e5:.0f} ({err_pct:.2f}%)",
                        (f"{sum(lat) / len(lat) * 1000:.0f} ms avg / {max(lat) * 1000:.0f} ms peak" if lat else "-"),
                        _fmt_bytes(tot("bytes")) if got.get("bytes") else "-"])
        if req and err_pct >= 1:
            ctx.find("HIGH" if err_pct >= 5 else "MED", f"Load balancer {name}: {e5:.0f} server errors (5xx) = {err_pct:.1f}% of {req:.0f} requests in the window")
        lines = []
        if got.get("req"):
            lines.append(_line("requests", got["req"], "count"))
        if got.get("2xx"):
            lines.append(_line("2xx", got["2xx"], "count"))
        if got.get("4xx"):
            lines.append(_line("4xx", got["4xx"], "count"))
        if got.get("5xx"):
            lines.append(_line("5xx", got["5xx"], "count"))
        if got.get("lat"):
            lines.append(_line("latency", got["lat"], "sec", None, total=False))
        if got.get("flows"):
            lines.append(_line("new flows", got["flows"], "count"))
        if got.get("active"):
            lines.append(_line("active flows", got["active"], "count", None, total=False))
        if got.get("bytes"):
            lines.append(_line("bytes", got["bytes"], "B"))
        lb_series.append({"n": name, "s": f"{kind} | {dns[:46]}", "lines": lines})
    if lb_rows:
        rep.add("")
        rep.add(f"  Load balancer traffic in the window ({period}-second points; 5xx % = server errors / requests):")
        rep.table(["LOAD BALANCER", "TYPE", "REQUESTS", "2xx", "4xx", "5xx (error rate)", "TARGET LATENCY", "BYTES"], lb_rows, maxw=40)
        rep.series(f"Load balancer traffic, last {mins} min", lb_series, "Per-minute counts from CloudWatch (ALB: RequestCount and HTTP codes, NLB: flows, classic: backend codes).")

    # ---- NAT gateways
    nat_series, nat_rows = [], []
    for nat in nat_ids[:6]:
        got = {}
        for key, metric in (("out", "BytesOutToDestination"), ("in", "BytesInFromDestination"), ("drop", "PacketsDropCount"), ("port", "ErrorPortAllocation")):
            pts, err = _cw_points(ctx, target, "AWS/NATGateway", metric, {"NatGatewayId": nat}, "Sum", period)
            if pts is not None:
                got[key] = pts
        if not got:
            continue
        tot = lambda k: sum(v for _, v in got.get(k, []))
        nat_rows.append([nat, _fmt_bytes(tot("out")), _fmt_bytes(tot("in")), f"{tot('drop'):.0f}", f"{tot('port'):.0f}"])
        if tot("port") > 0:
            ctx.find("HIGH", f"NAT gateway {nat}: {tot('port'):.0f} port allocation errors in the window (port exhaustion - too many connections to one destination)")
        elif tot("drop") > 0:
            ctx.find("MED", f"NAT gateway {nat}: {tot('drop'):.0f} dropped packets in the window")
        nat_series.append({"n": nat, "s": "NAT gateway", "lines": [_line("out to internet", got.get("out", []), "Bps", period), _line("in from internet", got.get("in", []), "Bps", period)]})
    if nat_rows:
        rep.add("")
        rep.add("  NAT gateway traffic in the window (egress to the internet from private subnets):")
        rep.table(["NAT GATEWAY", "BYTES OUT", "BYTES IN", "DROPPED PACKETS", "PORT ALLOC ERRORS"], nat_rows)
        rep.series(f"NAT gateway traffic, last {mins} min (bytes per second)", nat_series, "")


def _net_aws(rep, ctx, target, cluster):
    vpc_id = (cluster.get("resourcesVpcConfig") or {}).get("vpcId")
    if not vpc_id:
        rep.add("  (no VPC id from the cluster description)")
        return
    nat_ids, lbs = [], []
    try:
        nat_ids = _aws_vpc_routing(rep, ctx, target, cluster) or []
    except Exception as exc:
        rep.add(f"[!] VPC routing step failed: {exc}")
    try:
        _aws_node_enis(rep, ctx, target)
    except Exception as exc:
        rep.add(f"[!] node ENI step failed: {exc}")
    try:
        lbs = _aws_load_balancers(rep, ctx, target, vpc_id) or []
    except Exception as exc:
        rep.add(f"[!] load balancer step failed: {exc}")
    if ctx.cancel is not None and ctx.cancel.is_set():
        return
    try:
        _aws_traffic(rep, ctx, target, nat_ids, lbs)
    except Exception as exc:
        rep.add(f"[!] traffic step failed: {exc}")


def section_network_details(rep, ctx, label):
    rep.section(f"10. NETWORK & TRAFFIC - CNI, DNS, SERVICES, INGRESS, VPC, TRAFFIC (last {ctx.minutes} min)")
    for fn in (_net_cluster_settings, _net_services_ingress_policies, _net_pod_ips, _net_events_and_dns_logs, _net_pod_traffic):
        if ctx.cancel is not None and ctx.cancel.is_set():
            return
        try:
            fn(rep, ctx)
        except Exception as exc:
            rep.add(f"[!] {fn.__name__} failed: {exc}")
    target, cluster = ctx.data.get("aws_target"), ctx.data.get("aws_cluster")
    rep.add("")
    if AWS_OPTS["enabled"] and target and cluster:
        _net_aws(rep, ctx, target, cluster)
    else:
        rep.add("AWS NETWORK AND TRAFFIC: skipped - the AWS section is off or could not describe the cluster. "
                "(VPC routing, NAT, load balancers and the traffic over the window need AWS access.)")


def section_scaling_storage_network(rep, ctx):
    rep.section("11. AUTOSCALING, STORAGE, NETWORKING")
    rows = []
    for h in items(ctx.data.get("hpa")):
        meta, spec, st = h["metadata"], h.get("spec", {}), h.get("status", {})
        cur, desired, mx = st.get("currentReplicas", 0), st.get("desiredReplicas", 0), spec.get("maxReplicas", 0)
        issues = []
        if mx and cur >= mx:
            issues.append("AT MAX replicas")
        for c in st.get("conditions", []) or []:
            if c.get("type") in ("ScalingActive", "AbleToScale") and c.get("status") == "False":
                issues.append(f"{c['type']}=False ({c.get('reason', '')})")
        if issues:
            rows.append([f"{meta['namespace']}/{meta['name']}", support_of(ctx, meta["namespace"]) or "-", f"{cur}/{desired}/{mx}", "; ".join(issues)])
    if rows:
        rep.add("HPAs with issues (current/desired/max):")
        rep.table(["HPA", "SUPPORT DL", "REPLICAS", "ISSUE"], rows)
        ctx.find("MED", f"{len(rows)} HPA(s) at max or unable to scale" + support_suffix(ctx, {r[0].split("/")[0] for r in rows}))
    else:
        rep.add("HPAs: no issues found (or none defined).")

    rep.add("")
    pvcs = [[f"{p['metadata']['namespace']}/{p['metadata']['name']}", support_of(ctx, p["metadata"]["namespace"]) or "-",
             p.get("status", {}).get("phase", "?"),
             p.get("spec", {}).get("storageClassName", "-"), age(parse_ts(p["metadata"].get("creationTimestamp")), ctx.now)]
            for p in items(ctx.data.get("pvc")) if p.get("status", {}).get("phase") != "Bound"]
    pvs = [[p["metadata"]["name"], "-", p.get("status", {}).get("phase", "?"), "-", "-"]
           for p in items(ctx.data.get("pv")) if p.get("status", {}).get("phase") in ("Failed",)]
    if pvcs or pvs:
        rep.add("Storage problems (PVC not Bound / PV Failed):")
        rep.table(["NAME", "SUPPORT DL", "PHASE", "STORAGECLASS", "AGE"], pvcs + pvs)
        for r in pvcs:
            ctx.ns_issue(r[0].split("/")[0], f"PVC {r[0].split('/', 1)[1]} is {r[2]}")
        ctx.find("HIGH", f"{len(pvcs)} PVC(s) not Bound, {len(pvs)} PV(s) Failed" + support_suffix(ctx, {r[0].split("/")[0] for r in pvcs}))
    else:
        rep.add("Storage: all PVCs Bound.")

    rep.add("")
    svc_rows = []
    endpoints = {(e["metadata"]["namespace"], e["metadata"]["name"]): e for e in items(ctx.data.get("endpoints"))}
    for s in items(ctx.data.get("services")):
        meta, spec = s["metadata"], s.get("spec", {})
        key = (meta["namespace"], meta["name"])
        if spec.get("type") == "LoadBalancer" and not (s.get("status", {}).get("loadBalancer") or {}).get("ingress"):
            svc_rows.append([f"{key[0]}/{key[1]}", support_of(ctx, key[0]) or "-", "LoadBalancer", "no external address yet"])
        if spec.get("selector") and key in endpoints and key[1] != "kubernetes":
            subsets = endpoints[key].get("subsets") or []
            if not any(sub.get("addresses") for sub in subsets):
                svc_rows.append([f"{key[0]}/{key[1]}", support_of(ctx, key[0]) or "-", spec.get("type", "ClusterIP"), "no ready endpoints"])
    if svc_rows:
        rep.add("Services with problems:")
        rep.table(["SERVICE", "SUPPORT DL", "TYPE", "ISSUE"], svc_rows)
        ctx.find("MED", f"{len(svc_rows)} Service(s) with no endpoints / pending LoadBalancer" + support_suffix(ctx, {r[0].split("/")[0] for r in svc_rows}))
        for r in svc_rows:
            ctx.ns_issue(r[0].split("/")[0], f"service {r[0].split('/', 1)[1]}: {r[3]}")
    else:
        rep.add("Services: all selector services have ready endpoints; no pending LoadBalancers.")

    term = [n["metadata"]["name"] for n in items(ctx.data.get("namespaces")) if n.get("status", {}).get("phase") == "Terminating"]
    if term:
        rep.add("")
        rep.add("Namespaces stuck Terminating: " + ", ".join(term))
        ctx.find("MED", f"Namespace(s) Terminating: {', '.join(term[:5])}" + support_suffix(ctx, term))
        for n in term:
            ctx.ns_issue(n, "namespace stuck Terminating")


def _r(x, nd=4):
    return None if x is None else round(x, nd)


def build_utilization(ctx):
    """Everything the utilization dashboard needs, as plain numbers (cores / bytes; None = unknown):
    per-node CPU / memory / disk, per-namespace totals and, for every pod, usage vs request vs limit."""
    pods = items(ctx.data.get("pods"))
    nodes = items(ctx.data.get("nodes"))
    usage = ctx.data.get("pod_usage") or {}
    requests = _node_requests(pods)
    active = Counter()
    for p in pods:
        node_name = p.get("spec", {}).get("nodeName")
        if node_name and p.get("status", {}).get("phase") in ("Running", "Pending"):
            active[node_name] += 1

    node_rows = []
    cl = {"ca": 0.0, "ma": 0.0, "cr": 0.0, "mr": 0.0, "cu": 0.0, "mu": 0.0, "has_cu": False, "has_mu": False, "pods": 0, "mp": 0}
    for n in nodes:
        st = n.get("status", {})
        alloc = st.get("allocatable", {})
        name = n["metadata"]["name"]
        a_cpu, a_mem = parse_cpu(alloc.get("cpu")), parse_mem(alloc.get("memory"))
        max_pods = int(parse_cpu(alloc.get("pods")) or 0)
        u = node_usage(ctx, name, n)
        ready = any(c["type"] == "Ready" and c["status"] == "True" for c in st.get("conditions", []))
        status = ("Ready" if ready else "NotReady") + (",Cordoned" if n.get("spec", {}).get("unschedulable") else "")
        creq = requests.get(name, [0.0, 0.0, 0.0])
        ident = node_identity(ctx, n)
        node_rows.append({"name": name, "id": ident["instance_id"], "zone": ident["zone"], "type": ident["type"], "ec2": ident["ec2_name"],
                          "st": status, "cu": _r(u["cpu"]), "ca": _r(a_cpu), "cr": _r(creq[0]),
                          "mu": u["mem"], "ma": a_mem, "mr": creq[1], "du": u["disk_used"], "dc": u["disk_cap"],
                          "su": u["swap_used"], "pods": active[name], "mp": max_pods})
        cl["ca"] += a_cpu
        cl["ma"] += a_mem
        cl["cr"] += creq[0]
        cl["mr"] += creq[1]
        cl["pods"] += active[name]
        cl["mp"] += max_pods
        if u["cpu"] is not None:
            cl["cu"] += u["cpu"]
            cl["has_cu"] = True
        if u["mem"] is not None:
            cl["mu"] += u["mem"]
            cl["has_mu"] = True

    nsmap = defaultdict(list)
    for p in pods:
        st = p.get("status", {})
        if st.get("phase") not in ("Running", "Pending"):
            continue
        meta = p["metadata"]
        u = usage.get((meta["namespace"], meta["name"])) or {}
        res = _pod_resources(p)
        status = st.get("phase", "?")
        for cs in st.get("containerStatuses") or []:
            waiting = (cs.get("state") or {}).get("waiting") or {}
            if waiting.get("reason") and waiting["reason"] not in WAITING_OK:
                status = waiting["reason"]
                break
        nsmap[meta["namespace"]].append({
            "n": meta["name"], "nd": p.get("spec", {}).get("nodeName") or "", "st": status,
            "r": sum(cs.get("restartCount", 0) for cs in st.get("containerStatuses") or []),
            "cu": _r(u.get("cpu")), "cr": _r(res["cpu_req"]) or None, "cl": _r(res["cpu_lim"]) or None,
            "mu": u.get("mem"), "mr": res["mem_req"] or None, "ml": res["mem_lim"] or None, "du": u.get("disk")})
    namespaces = []
    for ns, plist in nsmap.items():
        known_cpu = any(p["cu"] is not None for p in plist)
        known_mem = any(p["mu"] is not None for p in plist)
        total = lambda key: sum(p[key] or 0 for p in plist)
        plist.sort(key=lambda p: -(p["mu"] or p["mr"] or 0))
        namespaces.append({
            "name": ns, "dl": support_of(ctx, ns), "pods": len(plist), "running": sum(1 for p in plist if p["st"] == "Running"),
            "cu": _r(total("cu")) if known_cpu else None, "cr": _r(total("cr")),
            "cl": _r(total("cl")) if all(p["cl"] for p in plist) else None,
            "mu": total("mu") if known_mem else None, "mr": total("mr"),
            "ml": total("ml") if all(p["ml"] for p in plist) else None,
            "ncl": sum(1 for p in plist if not p["cl"]), "nml": sum(1 for p in plist if not p["ml"]),
            "du": total("du") if any(p["du"] is not None for p in plist) else None,
            "rs": sum(p["r"] for p in plist), "pl": plist})
    has_usage = any(x["cu"] is not None or x["mu"] is not None for x in namespaces) or cl["has_cu"] or cl["has_mu"]
    if not cl["has_cu"]:
        cl["cu"] = sum(x["cu"] or 0 for x in namespaces) if any(x["cu"] is not None for x in namespaces) else None
    if not cl["has_mu"]:
        cl["mu"] = sum(x["mu"] or 0 for x in namespaces) if any(x["mu"] is not None for x in namespaces) else None
    cluster = {k: (_r(v) if isinstance(v, float) and k.startswith("c") else v) for k, v in cl.items() if not k.startswith("has_")}
    return {"hasUsage": has_usage, "cluster": cluster, "nodes": node_rows, "namespaces": namespaces,
            "nodeIds": {x["name"]: x["id"] for x in node_rows if x["id"] != "-"},
            "warn": UTIL_WARN, "crit": UTIL_CRIT}


def section_utilization(rep, ctx):
    rep.section("4. RESOURCE UTILIZATION - CPU & MEMORY BY NAMESPACE")
    data = build_utilization(ctx)
    if not data["namespaces"] and not data["nodes"]:
        rep.add("No node or pod data.")
        return
    c = data["cluster"]
    cpu_p, mem_p = _pct(c["cu"], c["ca"]), _pct(c["mu"], c["ma"])
    rep.add(f"Cluster CPU    : {_cores(c['cu']) if c['cu'] is not None else 'n/a'} used of {c['ca']:.1f} cores allocatable ({_fp(cpu_p)}); "
            f"{c['cr']:.1f} cores requested ({_fp(_pct(c['cr'], c['ca']))})")
    rep.add(f"Cluster memory : {fmt_gib(c['mu']) if c['mu'] is not None else 'n/a'} used of {fmt_gib(c['ma'])} allocatable ({_fp(mem_p)}); "
            f"{fmt_gib(c['mr'])} requested ({_fp(_pct(c['mr'], c['ma']))})")
    if not data["hasUsage"]:
        rep.add("Live usage is not available (needs the kubelet stats permission or metrics-server), so this section shows what pods REQUEST and are LIMITED to.")

    rep.util(data)       # the interactive dashboard comes first in the HTML; the table below is the detailed list

    key_mem = "mu" if data["hasUsage"] else "mr"
    rows = []
    for x in sorted(data["namespaces"], key=lambda x: -(x[key_mem] or 0))[:MAX_ROWS]:
        high = sum(1 for p in x["pl"]
                   if (_pct(p["mu"], p["ml"]) or 0) >= UTIL_CRIT or (_pct(p["cu"], p["cl"]) or 0) >= UTIL_CRIT)
        rows.append([x["name"], x["dl"] or "-", x["pods"],
                     _cores(x["cu"]) if x["cu"] is not None else "n/a", _cores(x["cr"]) if x["cr"] else "-", _cores(x["cl"]) if x["cl"] else "partial/none",
                     _fp(_pct(x["cu"] if data["hasUsage"] else x["cr"], c["ca"])),
                     _mi(x["mu"]) if x["mu"] is not None else "n/a", _mi(x["mr"]) if x["mr"] else "-", _mi(x["ml"]) if x["ml"] else "partial/none",
                     _fp(_pct(x["mu"] if data["hasUsage"] else x["mr"], c["ma"])), high or "-"])
    for x in data["namespaces"]:
        for pd in x["pl"]:
            for what, use, lim in (("memory", pd["mu"], pd["ml"]), ("CPU", pd["cu"], pd["cl"])):
                pc = _pct(use, lim)
                if pc is not None and pc >= UTIL_CRIT:
                    ctx.ns_issue(x["name"], f"pod {pd['n']} is at {pc:.0f}% of its {what} limit")
    rep.add("")
    rep.add("Namespaces ranked by memory (% = share of the cluster's allocatable; HIGH PODS = pods at 90%+ of their limit):")
    rep.table(["NAMESPACE", "SUPPORT DL", "PODS", "CPU use", "CPU req", "CPU lim", "CPU %cl", "MEM use", "MEM req", "MEM lim", "MEM %cl", "HIGH PODS"], rows)

    for metric, key, fmt in (("CPU", "cu", lambda v: f"{v:.2f} cores"), ("memory", "mu", _mi)):
        top = max((x for x in data["namespaces"] if x[key]), key=lambda x: x[key], default=None)
        total = c[key]
        if top and total:
            ctx.find("INFO", f"Top {metric} consumer: namespace {top['name']} ({fmt(top[key])}, "
                             f"{100 * top[key] / total:.0f}% of the cluster's {metric} use)")


def section_top(rep, ctx):
    rep.section("12. TOP RESOURCE CONSUMERS (live)")
    usage = ctx.data.get("pod_usage") or {}
    if not usage:
        rep.add("No live pod usage available (needs the kubelet stats permission or metrics-server).")
        return
    pods = {(p["metadata"]["namespace"], p["metadata"]["name"]): p for p in items(ctx.data.get("pods"))}
    for label, key in (("CPU", "cpu"), ("memory", "mem")):
        ranked = sorted(((k, u) for k, u in usage.items() if u.get(key) is not None), key=lambda kv: -kv[1][key])[:10]
        rows = []
        for (ns, name), u in ranked:
            res = _pod_resources(pods[(ns, name)]) if (ns, name) in pods else {"cpu_lim": 0, "mem_lim": 0}
            lim = res["cpu_lim"] if key == "cpu" else res["mem_lim"]
            rows.append([ns, support_of(ctx, ns) or "-", name, _cores(u["cpu"]) if u.get("cpu") is not None else "n/a", _mi(u["mem"]) if u.get("mem") is not None else "n/a",
                         _mi(u["disk"]) if u.get("disk") else "-", _fp(_pct(u[key], lim)) if lim else "no limit"])
        rep.add(f"Top 10 pods by {label}:")
        rep.table(["NAMESPACE", "SUPPORT DL", "POD", "CPU", "MEMORY", "DISK", f"% of {label} limit"], rows)
        rep.add("")


CORE_ADDON_PREFIXES = ("coredns", "aws-node", "kube-proxy", "ebs-csi", "efs-csi", "metrics-server", "cluster-autoscaler",
                       "karpenter", "aws-load-balancer-controller", "external-dns", "cert-manager", "fluent-bit",
                       "cloudwatch-agent", "adot", "node-local-dns")
WARN_PATTERN = re.compile(r"\b(warn|warning)\b", re.I)
TIMESTAMP_PREFIX = re.compile(r"^\d{4}-\d\d-\d\dT[\d:.]+Z?\s")
CONTAINERS_PER_POD = 3


def _container_logs(ns, pod, container, minutes, previous=False):
    """Logs of one container for the last `minutes`. Returns (lines, error_or_None)."""
    args = ["logs", "-n", ns, pod, "-c", container, f"--since={minutes}m", f"--tail={LOG_TAIL_LINES}",
            "--timestamps", f"--limit-bytes={LOG_MAX_BYTES}"]
    if previous:
        args.append("--previous")
    ok, out = kubectl(args, timeout=60)
    if ok:
        return out.splitlines(), None
    return [], (out.splitlines()[0][:140] if out else "unknown error")


def pick_log_targets(ctx, options):
    """Which pods to read logs from, and why:
       1. unhealthy pods              (crash loops, not ready, restarts ...)
       2. pods named in Warning events during the window
       3. core add-ons                (coredns, aws-node, kube-proxy, CSI drivers, autoscalers ...)
       4. every running pod           (only with the 'all pods' option, optionally limited to namespaces)"""
    pods = {(p["metadata"]["namespace"], p["metadata"]["name"]): p for p in items(ctx.data.get("pods"))}
    targets, seen = [], set()

    def add(key, reason):
        pod = pods.get(key)
        if not pod or key in seen or pod.get("status", {}).get("phase") in ("Pending", "Succeeded", "Unknown"):
            return False
        seen.add(key)
        targets.append((key, reason))
        return True

    count = 0
    for a in ctx.problem_pods:
        if count < MAX_LOG_PODS and add((a["ns"], a["name"]), f"unhealthy: {a['status']}"):
            count += 1
    warned = Counter()
    for e in items(ctx.data.get("events")):
        t = _event_time(e)
        obj = e.get("involvedObject") or e.get("regarding") or {}
        if e.get("type") == "Warning" and t and t >= ctx.since and obj.get("kind") == "Pod":
            warned[(obj.get("namespace"), obj.get("name"))] += (e.get("series") or {}).get("count") or e.get("count") or 1
    for key, n in warned.most_common():
        if count < MAX_LOG_PODS and add(key, f"{n} warning event(s)"):
            count += 1
    core = 0
    for key in sorted(pods):
        if core < MAX_CORE_LOG_PODS and key[1].startswith(CORE_ADDON_PREFIXES) and add(key, "core add-on"):
            core += 1
    if options.get("all_logs"):
        wanted = {x.strip() for x in str(options.get("log_namespaces") or "").split(",") if x.strip()}
        extra = 0
        for key in sorted(pods):
            if extra < MAX_ALL_LOG_PODS and (not wanted or key[0] in wanted) and add(key, "all pods"):
                extra += 1
    return targets, pods


def _containers_to_read(pod):
    by_name = {c["name"]: c for c in pod.get("status", {}).get("containerStatuses") or []}
    names = [c["name"] for c in pod.get("spec", {}).get("containers", [])]
    names.sort(key=lambda n: 0 if (n in by_name and (not by_name[n].get("ready") or by_name[n].get("restartCount"))) else 1)
    return names[:CONTAINERS_PER_POD], by_name


def section_logs(rep, ctx, options=None):
    from concurrent.futures import as_completed
    options = options or {}
    scope = "unhealthy pods, pods with warning events, core add-ons" + (", ALL pods" if options.get("all_logs") else "")
    rep.section(f"13. LOGS (last {ctx.minutes} min) - {scope}")
    targets, pods = pick_log_targets(ctx, options)
    if not targets:
        rep.add("No pods qualified for log collection (nothing unhealthy, no Warning events, no core add-ons found).")
        rep.add("Turn on 'Logs of ALL pods' (or use --logs-all) to read every running pod.")
        return

    jobs = []   # (namespace, pod, container, previous?, reason, restart_count)
    for (ns, name), reason in targets:
        names, by_name = _containers_to_read(pods[(ns, name)])
        for cname in names:
            restarts = by_name.get(cname, {}).get("restartCount", 0)
            jobs.append((ns, name, cname, False, reason, restarts))
            if restarts:
                jobs.append((ns, name, cname, True, reason, restarts))
    rep.add(f"Reading {len(jobs)} log stream(s) from {len(targets)} pod(s) "
            f"(up to {LOG_TAIL_LINES} lines each, last {ctx.minutes} min) ...")

    results = [None] * len(jobs)

    def work(i):
        if ctx.cancel is not None and ctx.cancel.is_set():
            return i, None, "cancelled"
        j = jobs[i]
        lines, err = _container_logs(j[0], j[1], j[2], ctx.minutes, previous=j[3])
        return i, lines, err

    finished = 0
    with ThreadPoolExecutor(max_workers=LOG_WORKERS) as pool:
        futures = [pool.submit(work, i) for i in range(len(jobs))]
        for fut in as_completed(futures):
            i, lines, err = fut.result()
            results[i] = None if err == "cancelled" else (lines, err)
            finished += 1
            if finished % 5 == 0 or finished == len(jobs):
                rep.emit(f"  ... logs {finished}/{len(jobs)}")

    rows, blocks = [], []
    for (ns, name, cname, prev, reason, restarts), res in zip(jobs, results):
        if res is None:
            continue
        lines, err = res
        kind = "previous (before last restart)" if prev else "current"
        errs = [l for l in lines if ERROR_PATTERN.search(l)]
        warns = [l for l in lines if not ERROR_PATTERN.search(l) and WARN_PATTERN.search(l)]
        last = TIMESTAMP_PREFIX.sub("", lines[-1]) if lines else (f"(no logs: {err})" if err else "(no output in this window)")
        rows.append([f"{ns}/{name}", support_of(ctx, ns) or "-", cname, "previous" if prev else "current", reason, len(lines), len(errs), len(warns), last[:90]])
        entries = [(l[:400], "err" if ERROR_PATTERN.search(l) else ("warn" if WARN_PATTERN.search(l) else "")) for l in lines]
        if not entries:
            entries = [(f"(no logs: {err})" if err else "(no output in this window)", "warn")]
        shown_errs = [(t, k) for t, k in entries if k == "err"][-LOG_SHOW_ERROR_LINES:]
        text_entries = shown_errs + [(t, k) for t, k in entries[-LOG_SHOW_LAST_LINES:] if (t, k) not in shown_errs]
        title = f"{ns}/{name} [{cname}] {kind}: {len(lines)} line(s), {len(errs)} error-like, {len(warns)} warning - {reason}"
        blocks.append((title, entries, text_entries))
        if errs and reason.startswith("unhealthy"):
            ctx.find("MED", f"Error-like logs in {ns}/{name} [{cname}] {kind.split(' ')[0]}: {len(errs)} line(s), "
                            f"latest: {TIMESTAMP_PREFIX.sub('', errs[-1])[:90]}")

    rep.add("")
    rep.add(f"Overview of {len(rows)} log stream(s) (click the column headers in the HTML to sort):")
    rep.table(["POD", "SUPPORT DL", "CONTAINER", "LOG", "WHY COLLECTED", "LINES", "ERRORS", "WARNINGS", "LAST LINE"], rows, maxw=90)
    errs_total = sum(r[6] for r in rows)
    rep.add(f"Total: {sum(r[5] for r in rows)} line(s), {errs_total} error-like.")
    for title, entries, text_entries in blocks:
        rep.add("")
        rep.log(title, entries, text_entries)
    if ctx.cancel is not None and ctx.cancel.is_set():
        rep.add("(log collection was stopped early)")


def section_timeline(rep, ctx):
    rep.section(f"14. TIMELINE - what happened in the last {ctx.minutes} min (oldest first)")
    if not ctx.timeline:
        rep.add("Nothing notable recorded in this window.")
        return
    entries = sorted(set(ctx.timeline), key=lambda x: x[0])
    skipped = max(0, len(entries) - MAX_TIMELINE)
    if skipped:
        rep.add(f"({skipped} older entries not shown)")
    rep.timeline(entries[-MAX_TIMELINE:])


def build_summary(ctx, label):
    order = {"CRIT": 0, "HIGH": 1, "MED": 2, "INFO": 3}
    lines = ["=" * 78, f"HEALTH SUMMARY - {label}  (last {ctx.minutes} min, {ctx.now:%Y-%m-%d %H:%M:%S} UTC)", "=" * 78]
    if not ctx.findings:
        lines.append("No problems detected in the collected data.")
    else:
        counts = Counter(s for s, _ in ctx.findings)
        lines.append("Findings: " + ", ".join(f"{counts[s]} {s}" for s in ("CRIT", "HIGH", "MED", "INFO") if counts[s]))
        for sev, text in sorted(ctx.findings, key=lambda x: order[x[0]]):
            lines.append(f"  [{sev}] {text}")
    contacts = contact_rows(ctx)
    if contacts:
        lines += ["", "TEAMS TO CONTACT (namespaces with problems, by support DL):"]
        for dl, namespaces, count, what in contacts:
            lines.append(f"  {dl}  ->  {namespaces}  ({count} issue(s))")
            lines.append(f"      {what}")
    return lines


# ---------------------------------------------------------------------------
# Interactive HTML report (one self-contained file: no internet, no external libraries)
# ---------------------------------------------------------------------------

import html as _html

_HTML_CSS = r"""
:root{--bg:#f4f6f9;--card:#fff;--text:#1b2433;--muted:#667085;--line:#e3e7ee;--accent:#2563eb;--code:#f1f3f7;
--crit:#b42318;--critbg:#fde4e1;--high:#b54708;--highbg:#feeccb;--med:#8a6d00;--medbg:#fff6cc;--info:#175cd3;--infobg:#dbeafe;--good:#067647;--goodbg:#d9f5e3}
[data-theme=dark]{--bg:#0f141b;--card:#171e28;--text:#e6eaf0;--muted:#98a2b3;--line:#2a3442;--accent:#6ea8ff;--code:#1d2632;
--crit:#ff8a80;--critbg:#3a1d1b;--high:#ffb454;--highbg:#3b2a12;--med:#f1d36a;--medbg:#3a3312;--info:#7fb5ff;--infobg:#17294a;--good:#5fd08a;--goodbg:#143325}
*{box-sizing:border-box}html{scroll-behavior:smooth}
body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
header{position:sticky;top:0;z-index:20;background:var(--card);border-bottom:1px solid var(--line);padding:10px 18px}
header h1{margin:0 0 2px;font-size:18px}.meta{color:var(--muted);font-size:12px}
.toolbar{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px;align-items:center}
.toolbar input[type=search]{flex:1 1 280px;min-width:200px;padding:7px 10px;border:1px solid var(--line);border-radius:8px;background:var(--bg);color:var(--text)}
button,.btn{cursor:pointer;border:1px solid var(--line);background:var(--card);color:var(--text);border-radius:8px;padding:6px 10px;font:inherit}
button:hover{border-color:var(--accent);color:var(--accent)}
.layout{display:grid;grid-template-columns:250px 1fr;gap:16px;padding:16px 18px;max-width:1700px;margin:0 auto}
nav{position:sticky;top:118px;align-self:start;max-height:calc(100vh - 135px);overflow:auto;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:8px}
nav a{display:flex;justify-content:space-between;gap:6px;padding:6px 8px;border-radius:6px;color:var(--text);text-decoration:none;font-size:13px}
nav a:hover,nav a.on{background:var(--code)}
.badge{font-size:11px;border-radius:10px;padding:0 7px;font-weight:600;white-space:nowrap}
.b-crit{background:var(--critbg);color:var(--crit)}.b-high{background:var(--highbg);color:var(--high)}.b-med{background:var(--medbg);color:var(--med)}.b-info{background:var(--infobg);color:var(--info)}
main{min-width:0}
.cards{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:12px}
.sevcard{flex:1 1 130px;border:2px solid transparent;border-radius:12px;padding:12px;cursor:pointer;user-select:none;text-align:left}
.sevcard b{display:block;font-size:26px;line-height:1.1}.sevcard.off{opacity:.35}
.c-crit{background:var(--critbg);color:var(--crit)}.c-high{background:var(--highbg);color:var(--high)}.c-med{background:var(--medbg);color:var(--med)}.c-info{background:var(--infobg);color:var(--info)}.c-ok{background:var(--goodbg);color:var(--good)}
details.sec{background:var(--card);border:1px solid var(--line);border-radius:12px;margin-bottom:14px;overflow:hidden}
details.sec>summary{cursor:pointer;padding:12px 16px;font-weight:650;font-size:15px;list-style:none;display:flex;justify-content:space-between;gap:10px}
details.sec>summary::-webkit-details-marker{display:none}
details.sec>summary:before{content:"\25B8";margin-right:8px;color:var(--muted);transition:.15s}details.sec[open]>summary:before{transform:rotate(90deg)}
.secbody{padding:4px 16px 16px;border-top:1px solid var(--line)}
.flash{animation:flash 1.6s}@keyframes flash{0%,60%{box-shadow:0 0 0 3px var(--accent)}100%{box-shadow:none}}
pre.lines,.logbody pre{background:var(--code);border-radius:8px;padding:10px 12px;margin:10px 0;overflow:auto;white-space:pre-wrap;word-break:break-word;font:12px/1.5 Consolas,"Cascadia Mono",monospace}
.ln.sev-crit{color:var(--crit);font-weight:700}.ln.sev-high{color:var(--high);font-weight:600}.ln.sev-med{color:var(--med)}.ln.sev-info{color:var(--info)}
.ln.warn{color:var(--high)}.ln.err,.ll.err{color:var(--crit)}.ln.sub{font-weight:700;color:var(--accent)}
.tablewrap{margin:12px 0;border:1px solid var(--line);border-radius:10px;overflow:hidden}
.tbtools{display:flex;gap:8px;align-items:center;padding:6px 8px;background:var(--code);flex-wrap:wrap}
.tbtools input{padding:4px 8px;border:1px solid var(--line);border-radius:6px;background:var(--card);color:var(--text);min-width:170px}
.tcount{color:var(--muted);font-size:12px;margin-right:auto}
.tscroll{overflow:auto;max-height:520px}
table.data{border-collapse:collapse;width:100%;font-size:12.5px}
table.data th{position:sticky;top:0;background:var(--card);text-align:left;padding:7px 10px;border-bottom:2px solid var(--line);cursor:pointer;white-space:nowrap;user-select:none}
table.data th:hover{color:var(--accent)}table.data th.asc:after{content:" \25B2"}table.data th.desc:after{content:" \25BC"}
table.data td{padding:6px 10px;border-bottom:1px solid var(--line);vertical-align:top;max-width:520px;word-break:break-word}
table.data tr:hover td{background:var(--code)}
td.bad{color:var(--crit);font-weight:650}td.warn{color:var(--high);font-weight:600}td.good{color:var(--good);font-weight:600}
.bar{display:block;height:5px;border-radius:3px;background:var(--line);margin-top:3px;min-width:70px}.bar i{display:block;height:100%;border-radius:3px;background:var(--good)}
.bar.w i{background:#e8a317}.bar.c i{background:var(--crit)}
.sevtag{font-size:11px;font-weight:700;border-radius:5px;padding:1px 7px}
details.log{margin:10px 0;border:1px solid var(--line);border-radius:8px}details.log>summary{cursor:pointer;padding:7px 10px;background:var(--code);font-size:13px}
.logbody{padding:0 10px 6px}.logbody label{font-size:12px;color:var(--muted)}.ll{display:block}.ll.hide{display:none}.ll.warn{color:var(--high)}.logbody pre.nw{white-space:pre}.copylog{padding:2px 8px;font-size:12px;margin-left:8px}
.tlfilters{display:flex;flex-wrap:wrap;gap:6px;margin:10px 0}.chip{padding:3px 10px;border-radius:14px;border:1px solid var(--line);cursor:pointer;font-size:12px;background:var(--card)}.chip.on{background:var(--accent);color:#fff;border-color:var(--accent)}
ul.timeline{list-style:none;margin:0;padding:0 0 0 14px;border-left:2px solid var(--line)}
ul.timeline li{position:relative;padding:5px 0 5px 12px}ul.timeline li:before{content:"";position:absolute;left:-20px;top:11px;width:9px;height:9px;border-radius:50%;background:var(--accent)}
ul.timeline time{font-family:Consolas,monospace;color:var(--muted);margin-right:8px}.kind{font-size:11px;font-weight:700;background:var(--code);border-radius:4px;padding:1px 6px;margin-right:6px}
.hidden{display:none!important}.small{font-size:12px;color:var(--muted)}
footer{padding:10px 18px 30px;color:var(--muted);font-size:12px;text-align:center}
@media(max-width:900px){.layout{grid-template-columns:1fr}nav{position:static;max-height:none}}
@media print{header,nav,.tbtools,.toolbar{display:none}.layout{display:block}details.sec{break-inside:avoid}}
"""

_HTML_JS = r"""
(function(){
const $=(s,r)=>(r||document).querySelector(s), $$=(s,r)=>Array.from((r||document).querySelectorAll(s));
const root=document.documentElement;
try{const t=localStorage.getItem('eks-theme');if(t)root.setAttribute('data-theme',t);else if(matchMedia('(prefers-color-scheme: dark)').matches)root.setAttribute('data-theme','dark');}catch(e){}
$('#theme').onclick=()=>{const d=root.getAttribute('data-theme')==='dark'?'light':'dark';root.setAttribute('data-theme',d);try{localStorage.setItem('eks-theme',d)}catch(e){}};
$('#expand').onclick=()=>$$('details.sec,details.log').forEach(d=>d.open=true);
$('#collapse').onclick=()=>$$('details.sec').forEach(d=>d.open=false);
$('#print').onclick=()=>window.print();
window.addEventListener('beforeprint',()=>$$('details').forEach(d=>d.open=true));
$('#dl').onclick=()=>{const raw=$('#rawtext').textContent.replace(/<\\\/script/gi,'</script');const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([raw],{type:'text/plain'}));a.download=document.title.replace(/[^\w.-]+/g,'_')+'.txt';a.click();};
document.addEventListener('keydown',e=>{if(e.key==='/'&&document.activeElement.tagName!=='INPUT'){e.preventDefault();$('#q').focus();}});

// ---- numbers with units, for sorting
const U={Ki:1024,Mi:1048576,Gi:1073741824,Ti:1099511627776,m:.001,k:1e3,K:1e3,M:1e6,G:1e9,T:1e12};
function key(t){t=(t||'').replace(/,/g,'').trim();let m=t.match(/\((\d+(?:\.\d+)?)%\)\s*$/);if(m)return parseFloat(m[1]);
 m=t.match(/^(-?\d+(?:\.\d+)?)\s*(Ki|Mi|Gi|Ti|m|k|K|M|G|T)?/);if(m)return parseFloat(m[1])*(U[m[2]]||1);return null;}
const BAD=/^(NotReady|CrashLoopBackOff|Error|Failed|Evicted|ImagePullBackOff|ErrImagePull|OOMKilled|DEGRADED|CREATE_FAILED|FAILED|PROBLEM|impaired|CRIT|Unknown|MISSING|FAILING)/i;
const WARN=/^(Pending|Terminating|Ready,SchedulingDisabled|SchedulingDisabled|UPDATING|CREATING|low IPs|VERY LOW|HIGH|AT MAX|insufficient|NEW node)/i;
const GOOD=/^(Ready|Running|ACTIVE|OK|ok|Bound|Succeeded|Completed|available)$/;
$$('table.data').forEach(tb=>{
 const heads=$$('th',tb);const rows=$$('tbody tr',tb);
 rows.forEach(tr=>$$('td',tr).forEach((td,i)=>{
  const t=td.textContent.trim(),h=(heads[i]?heads[i].textContent:'');
  if(tb.id!=='findings'){
   if(BAD.test(t)||/MISSING|NOT READY|DEGRADED|FAILING|PROBLEM|impaired/.test(t))td.classList.add('bad');
   else if(WARN.test(t)||/HIGH REQUESTS|AT MAX|low IPs|SWAP in use|MEM \d+% of limit/.test(t))td.classList.add('warn');
   else if(GOOD.test(t))td.classList.add('good');
   const p=t.match(/\((\d+)%\)\s*$/)||(/CPU|MEM|DISK|IMAGEFS|SWAP|req/i.test(h)&&t.match(/^(\d+)%$/));
   if(p){const v=Math.min(100,parseInt(p[1],10));const b=document.createElement('span');b.className='bar'+(v>=90?' c':(v>=75?' w':''));b.innerHTML='<i style="width:'+v+'%"></i>';td.appendChild(b);}
  }}));
 heads.forEach((th,ci)=>th.onclick=()=>{const asc=!th.classList.contains('asc');heads.forEach(x=>x.classList.remove('asc','desc'));th.classList.add(asc?'asc':'desc');
  const body=$('tbody',tb);const arr=$$('tr',body);
  arr.sort((a,b)=>{const x=a.children[ci].textContent,y=b.children[ci].textContent,kx=key(x),ky=key(y);
   let r=(kx!==null&&ky!==null)?kx-ky:x.localeCompare(y,undefined,{numeric:true,sensitivity:'base'});return asc?r:-r;});
  arr.forEach(r=>body.appendChild(r));});
});
// ---- per-table filter + CSV
function visibleCount(w){const n=$$('tbody tr',w).filter(r=>!r.classList.contains('hidden')).length;const c=$('.tcount',w);if(c)c.textContent=n+' of '+$$('tbody tr',w).length+' rows';}
$$('.tablewrap').forEach(w=>{visibleCount(w);const f=$('.tfilter',w);
 f.oninput=()=>{const t=f.value.toLowerCase();$$('tbody tr',w).forEach(r=>r.dataset.tf=(!t||r.textContent.toLowerCase().includes(t))?'1':'0');applyAll();};
 $('.csv',w).onclick=()=>{const rows=[$$('th',w).map(h=>h.textContent)].concat($$('tbody tr',w).filter(r=>!r.classList.contains('hidden')).map(r=>$$('td',r).map(c=>c.textContent)));
  const csv=rows.map(r=>r.map(c=>'"'+c.replace(/"/g,'""')+'"').join(',')).join('\n');const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv'}));a.download='table.csv';a.click();};});
// ---- severity cards + global search + timeline chips + logs
const sevOn={CRIT:true,HIGH:true,MED:true,INFO:true};const kindOn={};
function applyAll(){
 const q=($('#q').value||'').toLowerCase();let hits=0;
 $$('#findings tbody tr').forEach(r=>{const ok=sevOn[r.dataset.sev]&&(!q||r.textContent.toLowerCase().includes(q));r.classList.toggle('hidden',!ok);if(q&&ok)hits++;});
 $$('.tablewrap').forEach(w=>{if(w.querySelector('#findings'))return;$$('tbody tr',w).forEach(r=>{const ok=(!q||r.textContent.toLowerCase().includes(q))&&r.dataset.tf!=='0';r.classList.toggle('hidden',!ok);if(q&&ok)hits++;});visibleCount(w);});
 $$('ul.timeline li').forEach(li=>{const ok=(kindOn[li.dataset.kind]!==false)&&(!q||li.textContent.toLowerCase().includes(q));li.classList.toggle('hidden',!ok);if(q&&ok)hits++;});
 $$('.logbody').forEach(b=>{const only=$('.errsonly',b).checked;$$('.ll',b).forEach(l=>{const ok=(!only||l.classList.contains('err'))&&(!q||l.textContent.toLowerCase().includes(q));l.classList.toggle('hide',!ok);if(q&&ok)hits++;});});
 $$('pre.lines').forEach(p=>$$('.ln',p).forEach(l=>{const ok=!q||l.textContent.toLowerCase().includes(q);l.classList.toggle('hidden',!ok);if(q&&ok)hits++;}));
 $('#hits').textContent=q?(hits+' match'+(hits===1?'':'es')):'';
 if(q)$$('details.sec').forEach(d=>{d.open=true;});
}
$('#q').oninput=applyAll;
$$('.sevcard').forEach(c=>c.onclick=()=>{const s=c.dataset.sev;sevOn[s]=!sevOn[s];c.classList.toggle('off',!sevOn[s]);applyAll();});
$$('.chip').forEach(c=>{kindOn[c.dataset.kind]=true;c.onclick=()=>{kindOn[c.dataset.kind]=!(kindOn[c.dataset.kind]!==false);c.classList.toggle('on',kindOn[c.dataset.kind]);applyAll();};});
$$('.errsonly').forEach(c=>c.onchange=applyAll);
$$('.nowrap').forEach(c=>c.onchange=()=>c.closest('.logbody').querySelector('pre').classList.toggle('nw',c.checked));
$$('.copylog').forEach(b=>b.onclick=()=>{const t=Array.from(b.closest('.logbody').querySelectorAll('.ll')).filter(l=>!l.classList.contains('hide')).map(l=>l.textContent).join('\n');try{navigator.clipboard.writeText(t);b.textContent='Copied';setTimeout(()=>b.textContent='Copy',1200);}catch(e){}});
// ---- jump links + nav highlight
function jump(id){const d=document.getElementById(id);if(!d)return;d.open=true;d.scrollIntoView({behavior:'smooth',block:'start'});d.classList.remove('flash');void d.offsetWidth;d.classList.add('flash');}
$$('a.jump').forEach(a=>a.onclick=e=>{e.preventDefault();jump(a.getAttribute('href').slice(1));});
const io=new IntersectionObserver(es=>es.forEach(e=>{if(e.isIntersecting){$$('nav a').forEach(a=>a.classList.toggle('on',a.getAttribute('href')==='#'+e.target.id));}}),{rootMargin:'-20% 0px -70% 0px'});
$$('details.sec').forEach(d=>io.observe(d));
})();
"""

_SEV_ORDER = {"CRIT": 0, "HIGH": 1, "MED": 2, "INFO": 3}


_UTIL_CSS = r"""
.util{margin:8px 0}.util h3{margin:18px 0 8px;font-size:14px}.util .note{background:var(--medbg);color:var(--med);border-radius:8px;padding:8px 12px;margin:8px 0}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:10px}
.tile{border:1px solid var(--line);border-radius:10px;padding:10px 12px;background:var(--card)}
.tile .tl{color:var(--muted);font-size:12px}.tile .tv{font-size:20px;font-weight:650;margin:2px 0 6px}.tile .tv small{font-size:12px;color:var(--muted);font-weight:400}.tile .ts{font-size:11px;color:var(--muted);margin-top:4px}
.gauge{position:relative;height:10px;border-radius:5px;background:var(--line);overflow:visible}
.gauge .g{display:block;height:100%;border-radius:5px;background:var(--good)}.gauge .g.w{background:#e8a317}.gauge .g.c{background:var(--crit)}
.m{position:absolute;top:-3px;width:2px;height:16px;background:var(--text);opacity:.65}.m.lim{background:var(--crit);opacity:.9;width:2px}
.stack{display:flex;height:26px;border-radius:6px;overflow:hidden;border:1px solid var(--line);margin:4px 0}
.stack span{display:block;height:100%;min-width:2px;cursor:default}
.legend{display:flex;flex-wrap:wrap;gap:4px 12px;font-size:12px;margin:4px 0 10px}.legend b{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:4px;vertical-align:-1px}
.nodegrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:10px}
.nodecard{border:1px solid var(--line);border-radius:10px;padding:10px 12px;background:var(--card)}
.nodecard.hot{border-color:var(--crit)}.nodecard.warm{border-color:#e8a317}
.nid{font-size:11px;color:var(--muted);margin:-2px 0 6px;word-break:break-all}
.nodecard h4{margin:0 0 6px;font-size:13px;display:flex;justify-content:space-between;gap:8px;word-break:break-all}
.nrow{display:grid;grid-template-columns:62px 1fr;gap:8px;align-items:center;margin:5px 0;font-size:12px}
.nrow .nl{color:var(--muted)}.nrow .nv{margin-top:2px;font-size:11px;color:var(--muted)}
.ctl{display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin:8px 0}.ctl select,.ctl input[type=search]{padding:5px 8px;border:1px solid var(--line);border-radius:6px;background:var(--card);color:var(--text)}
.ctl label{font-size:12px;color:var(--muted)}
.seg{display:inline-flex;border:1px solid var(--line);border-radius:8px;overflow:hidden}.seg button{border:0;border-radius:0;padding:5px 12px;background:var(--card)}.seg button.on{background:var(--accent);color:#fff}
.nsrow{border:1px solid var(--line);border-radius:10px;margin:6px 0;background:var(--card)}
.nsmain{display:grid;grid-template-columns:190px minmax(160px,1fr) 240px;gap:12px;align-items:center;padding:9px 12px;cursor:pointer}
.nsmain:hover{background:var(--code);border-radius:10px}.nsname{font-weight:650;word-break:break-all}.nsname small{display:block;color:var(--muted);font-weight:400;font-size:11px}
.nsval{font-size:12px;text-align:right}.nsval small{color:var(--muted)}
.ub{position:relative;height:14px;border-radius:4px;background:var(--line)}.ub .f{display:block;height:100%;border-radius:4px;background:var(--accent)}
.ub.w .f{background:#e8a317}.ub.c .f{background:var(--crit)}.ub.ok .f{background:var(--good)}
.pill{font-size:11px;border-radius:9px;padding:0 7px;margin-left:4px;font-weight:600}.pill.c{background:var(--critbg);color:var(--crit)}.pill.w{background:var(--highbg);color:var(--high)}
.nsdetail{display:none;padding:2px 12px 10px;border-top:1px solid var(--line)}.nsrow.open .nsdetail{display:block}
.podhead,.podrow{display:grid;grid-template-columns:minmax(150px,1.3fr) 1.6fr 1.3fr 150px;gap:10px;align-items:center;padding:4px 0;font-size:12px}
.podhead{color:var(--muted);font-weight:600;border-bottom:1px solid var(--line);margin-top:6px}.podrow{border-bottom:1px dashed var(--line)}
.podrow .pn{word-break:break-all}.podrow .pm{color:var(--muted);font-size:11px;word-break:break-all}
.toplists{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:14px}
.toplist .tr{display:grid;grid-template-columns:minmax(120px,1.1fr) 1.4fr 80px;gap:8px;align-items:center;font-size:12px;margin:5px 0}
.toplist .tr span.nm{word-break:break-all}.toplist h4{margin:0 0 6px;font-size:13px}
@media(max-width:900px){.nsmain{grid-template-columns:1fr}.nsval{text-align:left}.podhead{display:none}.podrow{grid-template-columns:1fr}}
"""

_UTIL_JS = r"""
(function(){
const root=document.getElementById('util'),dataEl=document.getElementById('util-json');if(!root||!dataEl)return;
const D=JSON.parse(dataEl.textContent);
const el=(t,c,h)=>{const e=document.createElement(t);if(c)e.className=c;if(h!==undefined)e.innerHTML=h;return e;};
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const GI=1073741824,MI=1048576,WARN=D.warn||75,CRIT=D.crit||90;
const fCpu=c=>c==null?'n/a':(c<1?Math.round(c*1000)+'m':c.toFixed(2));
const fMem=b=>b==null?'n/a':(b>=GI?(b/GI).toFixed(1)+'Gi':Math.round(b/MI)+'Mi');
const fPct=p=>p==null?'n/a':Math.round(p)+'%';
const pct=(u,t)=>(u==null||!t)?null:100*u/t;
const sev=p=>p==null?'':(p>=CRIT?'c':(p>=WARN?'w':'ok'));
const nsColor=n=>{let h=0;for(const ch of n)h=(h*31+ch.charCodeAt(0))%360;return 'hsl('+h+',55%,52%)';};
const C=D.cluster;
if(!D.hasUsage)root.appendChild(el('div','note','<b>Live CPU / memory usage is not available</b> (it needs the kubelet stats permission or metrics-server). Everything below shows what pods <b>request</b> and are <b>limited</b> to instead of what they use right now.'));

// ---- cluster tiles
function tile(label,use,total,fmt,req){
  const p=pct(use,total),rp=pct(req,total),t=el('div','tile');
  t.innerHTML='<div class="tl">'+label+'</div><div class="tv">'+(use==null?'n/a':fmt(use))+' <small>of '+fmt(total)+' allocatable ('+fPct(p)+')</small></div>'
   +'<div class="gauge"><i class="g '+sev(p)+'" style="width:'+Math.min(100,p||0)+'%"></i>'+(rp!=null?'<i class="m" style="left:'+Math.min(100,rp)+'%" title="requested"></i>':'')+'</div>'
   +'<div class="ts">requested '+fmt(req)+' ('+fPct(rp)+') &nbsp;|&nbsp; the tick marks the requested amount</div>';
  return t;}
const tiles=el('div','tiles');
tiles.appendChild(tile('CLUSTER CPU',C.cu,C.ca,fCpu,C.cr));
tiles.appendChild(tile('CLUSTER MEMORY',C.mu,C.ma,fMem,C.mr));
(function(){const t=el('div','tile'),p=pct(C.pods,C.mp);
  t.innerHTML='<div class="tl">PODS (running + pending) vs node capacity</div><div class="tv">'+C.pods+' <small>of '+C.mp+' ('+fPct(p)+')</small></div><div class="gauge"><i class="g '+sev(p)+'" style="width:'+Math.min(100,p||0)+'%"></i></div>';tiles.appendChild(t);})();
root.appendChild(tiles);

// ---- who uses the cluster: stacked share by namespace
function stacked(title,key,reqKey,fmt,total){
  const useK=D.hasUsage?key:reqKey;let arr=D.namespaces.filter(n=>n[useK]).sort((a,b)=>b[useK]-a[useK]);
  const sum=arr.reduce((s,n)=>s+n[useK],0);if(!sum)return;
  const top=arr.slice(0,10),rest=arr.slice(10).reduce((s,n)=>s+n[useK],0);
  const wrap=el('div');wrap.appendChild(el('h3',null,title+(D.hasUsage?' (used)':' (requested)')));
  const bar=el('div','stack'),leg=el('div','legend');
  top.forEach(n=>{const s=el('span');s.style.width=(100*n[useK]/sum)+'%';s.style.background=nsColor(n.name);s.title=n.name+': '+fmt(n[useK])+' ('+Math.round(100*n[useK]/sum)+'% of what pods use; '+fPct(pct(n[useK],total))+' of allocatable)';bar.appendChild(s);
    leg.appendChild(el('span',null,'<b style="background:'+nsColor(n.name)+'"></b>'+esc(n.name)+' '+fmt(n[useK])+' ('+Math.round(100*n[useK]/sum)+'%)'));});
  if(rest){const s=el('span');s.style.width=(100*rest/sum)+'%';s.style.background='#98a2b3';s.title='other namespaces: '+fmt(rest);bar.appendChild(s);leg.appendChild(el('span',null,'<b style="background:#98a2b3"></b>others '+fmt(rest)));}
  wrap.appendChild(bar);wrap.appendChild(leg);root.appendChild(wrap);}
stacked('CPU share by namespace','cu','cr',fCpu,C.ca);
stacked('Memory share by namespace','mu','mr',fMem,C.ma);

// ---- nodes
root.appendChild(el('h3',null,'Nodes (sorted by the most loaded)'));
function nrow(label,use,total,fmt,req,extra){
  const p=pct(use,total),r=el('div','nrow');
  r.innerHTML='<div class="nl">'+label+'</div><div><div class="gauge"><i class="g '+sev(p)+'" style="width:'+Math.min(100,p||0)+'%"></i>'+(req!=null&&total?'<i class="m" style="left:'+Math.min(100,pct(req,total))+'%" title="requested"></i>':'')+'</div>'
   +'<div class="nv">'+(use==null?'no live data':fmt(use)+' of '+fmt(total)+' ('+fPct(p)+')')+(req!=null?' &middot; requested '+fmt(req)+' ('+fPct(pct(req,total))+')':'')+(extra||'')+'</div></div>';
  return r;}
const grid=el('div','nodegrid');
const load=n=>Math.max(pct(n.cu!=null?n.cu:n.cr,n.ca)||0,pct(n.mu!=null?n.mu:n.mr,n.ma)||0,pct(n.du,n.dc)||0);
D.nodes.slice().sort((a,b)=>load(b)-load(a)).forEach(n=>{
  const l=load(n),c=el('div','nodecard'+(l>=CRIT?' hot':(l>=WARN?' warm':'')));
  c.innerHTML='<h4><span>'+esc(n.name)+'</span><span class="badge '+(/NotReady/.test(n.st)?'b-crit':(/Cordoned/.test(n.st)?'b-med':'b-info'))+'">'+esc(n.st)+'</span></h4>'
    +'<div class="nid"><b>'+esc(n.id||'-')+'</b> &middot; '+esc(n.zone||'-')+' &middot; '+esc(n.type||'-')+(n.ec2&&n.ec2!=='-'?' &middot; EC2 name: '+esc(n.ec2):'')+'</div>';
  c.appendChild(nrow('CPU',n.cu,n.ca,fCpu,n.cr));c.appendChild(nrow('Memory',n.mu,n.ma,fMem,n.mr));
  if(n.du!=null)c.appendChild(nrow('Disk',n.du,n.dc,fMem));
  c.appendChild(nrow('Pods',n.pods,n.mp,x=>String(Math.round(x))));
  if(n.su)c.appendChild(el('div','nv','<span class="pill c">swap in use '+fMem(n.su)+'</span>'));
  grid.appendChild(c);});
root.appendChild(grid);

// ---- namespace explorer
root.appendChild(el('h3',null,'By namespace (click a namespace to see its pods)'));
const S={metric:'mem',mode:D.hasUsage?'use':'req',sort:'use',high:false,q:''};
const KEY={cpu:{use:'cu',req:'cr',lim:'cl',nolim:'ncl',fmt:fCpu,tot:C.ca,name:'CPU'},mem:{use:'mu',req:'mr',lim:'ml',nolim:'nml',fmt:fMem,tot:C.ma,name:'Memory'},disk:{use:'du',req:null,lim:null,fmt:fMem,tot:null,name:'Disk'}};
const ctl=el('div','ctl');
ctl.innerHTML='<span class="seg" id="u-metric"><button data-v="cpu">CPU</button><button data-v="mem" class="on">Memory</button><button data-v="disk">Disk</button></span>'
 +'<span class="seg" id="u-mode"><button data-v="use"'+(D.hasUsage?' class="on"':' disabled title="no live usage"')+'>Used</button><button data-v="req"'+(D.hasUsage?'':' class="on"')+'>Requested</button></span>'
 +'<label>Sort <select id="u-sort"><option value="use">highest value</option><option value="limpct">closest to limit (worst pod)</option><option value="share">share of cluster</option><option value="rs">most restarts</option><option value="name">name</option></select></label>'
 +'<label><input type="checkbox" id="u-high"> only namespaces with a pod at '+WARN+'%+ of its limit</label><input type="search" id="u-q" placeholder="Filter namespace or support DL...">';
root.appendChild(ctl);
const list=el('div','nslist');root.appendChild(list);
function worst(n,k){let w=null;n.pl.forEach(p=>{const x=pct(p[k.use],p[k.lim]);if(x!=null&&(w==null||x>w))w=x;});return w;}
function render(){
  const k=KEY[S.metric],valKey=S.mode==='use'?k.use:k.req;
  let rows=D.namespaces.filter(n=>(!S.q||n.name.toLowerCase().includes(S.q)||(n.dl||'').toLowerCase().includes(S.q))).map(n=>({n:n,v:n[valKey],w:k.lim?worst(n,k):null}));
  if(S.high)rows=rows.filter(r=>r.w!=null&&r.w>=WARN);
  const key={use:r=>-(r.v||0),limpct:r=>-(r.w==null?-1:r.w),share:r=>-(r.v||0),rs:r=>-r.n.rs,name:r=>r.n.name};
  rows.sort((a,b)=>S.sort==='name'?a.n.name.localeCompare(b.n.name):(key[S.sort](a)-key[S.sort](b)));
  const max=Math.max.apply(null,[1e-9].concat(rows.map(r=>Math.max(r.v||0,(r.n[k.req]||0)))));
  list.innerHTML='';
  if(!rows.length){list.appendChild(el('div','small','No namespaces match.'));return;}
  rows.forEach(r=>{
    const n=r.n,row=el('div','nsrow'),main=el('div','nsmain');
    const nHigh=k.lim?n.pl.filter(p=>pct(p[k.use],p[k.lim])>=CRIT).length:0,nWarn=k.lim?n.pl.filter(p=>{const x=pct(p[k.use],p[k.lim]);return x>=WARN&&x<CRIT;}).length:0;
    main.innerHTML='<div class="nsname"><span style="display:inline-block;width:9px;height:9px;border-radius:2px;background:'+nsColor(n.name)+';margin-right:6px"></span>'+esc(n.name)+(nHigh?'<span class="pill c">'+nHigh+' at '+CRIT+'%+</span>':'')+(nWarn?'<span class="pill w">'+nWarn+' at '+WARN+'%+</span>':'')
      +'<small>'+n.pods+' pods &middot; '+n.rs+' restarts'+(n.dl?'<br>support: <b>'+esc(n.dl)+'</b>':'<br>support: <i>no label</i>')+'</small></div>';
    const barCell=el('div'),sc=(w=>w==null?0:Math.min(100,100*w/max));
    const b=el('div','ub '+(r.w!=null?sev(r.w):''));
    b.innerHTML='<i class="f" style="width:'+sc(r.v)+'%"></i>'+(S.mode==='use'&&n[k.req]?'<i class="m" style="left:'+sc(n[k.req])+'%" title="requested '+k.fmt(n[k.req])+'"></i>':'')
      +(k.lim&&n[k.lim]&&sc(n[k.lim])<100?'<i class="m lim" style="left:'+sc(n[k.lim])+'%" title="limits total '+k.fmt(n[k.lim])+'"></i>':'');
    barCell.appendChild(b);main.appendChild(barCell);
    const share=k.tot?pct(r.v,k.tot):null;
    main.appendChild(el('div','nsval','<b>'+(r.v==null?'n/a':k.fmt(r.v))+'</b> '+(S.mode==='use'?'used':'requested')+(share!=null?' <small>('+fPct(share)+' of cluster)</small>':'')
      +(k.req&&S.mode==='use'?'<br><small>requested '+(n[k.req]?k.fmt(n[k.req]):'-')+(k.lim?' &middot; limit '+(n[k.lim]?k.fmt(n[k.lim]):(n[k.nolim]&&n[k.nolim]<n.pods?'set on '+(n.pods-n[k.nolim])+' of '+n.pods+' pods':'none')):'')+'</small>':'')
      +(r.w!=null?'<br><small>worst pod: '+fPct(r.w)+' of its limit</small>':'')));
    const det=el('div','nsdetail');
    det.innerHTML='<div class="podhead"><span>Pod</span><span>'+k.name+' (bar = used; ticks = request / limit)</span><span>Used / request / limit</span><span>Node &middot; status</span></div>';
    const pmax=Math.max.apply(null,[1e-9].concat(n.pl.map(p=>Math.max(p[valKey]||0,p[k.req]||0))));
    n.pl.slice().sort((a,b)=>(b[valKey]||0)-(a[valKey]||0)).forEach(p=>{
      const lp=k.lim?pct(p[k.use],p[k.lim]):null,pr=el('div','podrow'),ps=v=>v==null?0:Math.min(100,100*v/pmax);
      pr.innerHTML='<span class="pn">'+esc(p.n)+(p.r?' <span class="pill w">'+p.r+' restarts</span>':'')+'</span>'
       +'<span><div class="ub '+(lp!=null?sev(lp):'')+'"><i class="f" style="width:'+ps(p[valKey])+'%"></i>'+(k.req&&p[k.req]?'<i class="m" style="left:'+ps(p[k.req])+'%"></i>':'')+(k.lim&&p[k.lim]&&ps(p[k.lim])<100?'<i class="m lim" style="left:'+ps(p[k.lim])+'%"></i>':'')+'</div></span>'
       +'<span>'+(p[k.use]==null?'n/a':k.fmt(p[k.use]))+' / '+(k.req&&p[k.req]?k.fmt(p[k.req]):'-')+' / '+(k.lim&&p[k.lim]?k.fmt(p[k.lim]):'none')+(lp!=null?' <b class="'+(lp>=CRIT?'c':(lp>=WARN?'w':''))+'" style="'+(lp>=CRIT?'color:var(--crit)':(lp>=WARN?'color:var(--high)':''))+'">('+fPct(lp)+' of limit)</b>':'')+'</span>'
       +'<span class="pm">'+esc(p.nd)+(D.nodeIds&&D.nodeIds[p.nd]?' ['+esc(D.nodeIds[p.nd])+']':'')+' &middot; '+esc(p.st)+'</span>';
      det.appendChild(pr);});
    main.onclick=()=>row.classList.toggle('open');row.appendChild(main);row.appendChild(det);list.appendChild(row);});
}
function seg(id,key){Array.from(document.querySelectorAll('#'+id+' button')).forEach(b=>b.onclick=()=>{if(b.disabled)return;S[key]=b.dataset.v;Array.from(document.querySelectorAll('#'+id+' button')).forEach(x=>x.classList.toggle('on',x===b));render();});}
seg('u-metric','metric');seg('u-mode','mode');
document.getElementById('u-sort').onchange=e=>{S.sort=e.target.value;render();};
document.getElementById('u-high').onchange=e=>{S.high=e.target.checked;render();};
document.getElementById('u-q').oninput=e=>{S.q=e.target.value.toLowerCase();render();};
render();

// ---- top consumers
root.appendChild(el('h3',null,'Top consumers (pods)'));
const all=[];const DLMAP={};D.namespaces.forEach(n=>{DLMAP[n.name]=n.dl;n.pl.forEach(p=>all.push(Object.assign({ns:n.name},p)));});
const tops=el('div','toplists');
function toplist(title,valFn,fmt,sevFn,subFn){
  const items=all.map(p=>({p:p,v:valFn(p)})).filter(x=>x.v!=null&&x.v>0).sort((a,b)=>b.v-a.v).slice(0,12),box=el('div','toplist');
  box.appendChild(el('h4',null,title));if(!items.length){box.appendChild(el('div','small','no data'));tops.appendChild(box);return;}
  const mx=items[0].v;items.forEach(x=>{const r=el('div','tr'),s=sevFn?sevFn(x.p):'';
    r.innerHTML='<span class="nm" title="'+esc(x.p.ns+'/'+x.p.n+(DLMAP[x.p.ns]?'  -  support: '+DLMAP[x.p.ns]:''))+'"><b style="display:inline-block;width:8px;height:8px;border-radius:2px;background:'+nsColor(x.p.ns)+';margin-right:5px"></b>'+esc(x.p.ns)+'/'+esc(x.p.n)+'</span>'
     +'<div class="ub '+s+'"><i class="f" style="width:'+(100*x.v/mx)+'%"></i></div><span>'+fmt(x.v)+(subFn?'<br><small class="small">'+subFn(x.p)+'</small>':'')+'</span>';box.appendChild(r);});
  tops.appendChild(box);}
if(D.hasUsage){
  toplist('Top CPU',p=>p.cu,fCpu,p=>sev(pct(p.cu,p.cl)),p=>p.cl?fPct(pct(p.cu,p.cl))+' of limit':'no limit');
  toplist('Top memory',p=>p.mu,fMem,p=>sev(pct(p.mu,p.ml)),p=>p.ml?fPct(pct(p.mu,p.ml))+' of limit':'no limit');
  toplist('Closest to memory limit (OOM risk)',p=>pct(p.mu,p.ml),fPct,p=>sev(pct(p.mu,p.ml)),p=>fMem(p.mu)+' / '+fMem(p.ml));
  toplist('Closest to CPU limit (throttling)',p=>pct(p.cu,p.cl),fPct,p=>sev(pct(p.cu,p.cl)),p=>fCpu(p.cu)+' / '+fCpu(p.cl));
  toplist('Using more memory than requested',p=>(p.mu!=null&&p.mr)?pct(p.mu,p.mr):null,fPct,null,p=>fMem(p.mu)+' vs requested '+fMem(p.mr));
}else{
  toplist('Largest memory requests',p=>p.mr,fMem,null,null);toplist('Largest CPU requests',p=>p.cr,fCpu,null,null);
}
root.appendChild(tops);
})();
"""


def _util_block_html(data):
    payload = json.dumps(data, separators=(",", ":")).replace("</", "<\\/")
    return ('<div id="util" class="util"></div>'
            '<script type="application/json" id="util-json">%s</script>' % payload)


_SERIES_CSS = r"""
.series{margin:12px 0}.series h3{margin:0 0 4px;font-size:14px}.series .snote{color:var(--muted);font-size:12px;margin-bottom:8px}
.sgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:10px}
.scard{border:1px solid var(--line);border-radius:10px;padding:9px 12px;background:var(--card)}
.scard .sn{font-weight:650;font-size:13px;word-break:break-all}.scard .ss{color:var(--muted);font-size:11px;margin-bottom:4px;word-break:break-all}
.sline{display:grid;grid-template-columns:78px 1fr;gap:8px;align-items:center;margin:4px 0}
.sline .sl{font-size:11px;color:var(--muted)}.sline svg{width:100%;height:34px;display:block}
.sline .sv{grid-column:1 / span 2;font-size:11px;color:var(--muted);margin-top:-2px}
.sline polyline{fill:none;stroke-width:1.8;stroke-linejoin:round}.sline .area{stroke:none;opacity:.14}
.c0{stroke:#2563eb;fill:#2563eb}.c1{stroke:#e8a317;fill:#e8a317}.c2{stroke:#16a34a;fill:#16a34a}.c3{stroke:#dc2626;fill:#dc2626}.c4{stroke:#7c3aed;fill:#7c3aed}.c5{stroke:#0891b2;fill:#0891b2}
.sline .base{stroke:var(--line);stroke-width:1}
"""

_SERIES_JS = r"""
(function(){
const GI=1073741824,MI=1048576,KI=1024;
const fB=b=>b==null?'n/a':(b>=GI?(b/GI).toFixed(2)+' GiB':b>=MI?(b/MI).toFixed(1)+' MiB':b>=KI?(b/KI).toFixed(1)+' KiB':Math.round(b)+' B');
const F={Bps:v=>fB(v)+'/s',B:fB,count:v=>v==null?'n/a':(v>=1e6?(v/1e6).toFixed(2)+'M':v>=1e3?(v/1e3).toFixed(1)+'k':(Math.round(v*10)/10)+''),sec:v=>v==null?'n/a':(v<1?Math.round(v*1000)+' ms':v.toFixed(2)+' s')};
const hhmm=t=>{const d=new Date(t*1000);return ('0'+d.getHours()).slice(-2)+':'+('0'+d.getMinutes()).slice(-2);};
Array.from(document.querySelectorAll('.series')).forEach(root=>{
  const D=JSON.parse(root.querySelector('script').textContent),grid=root.querySelector('.sgrid');
  D.rows.forEach(r=>{
    const card=document.createElement('div');card.className='scard';
    card.innerHTML='<div class="sn">'+r.n.replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))+'</div><div class="ss">'+(r.s||'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]))+'</div>';
    // in/out byte rates share one scale so they are comparable; count lines (requests, 4xx, 5xx ...) are scaled on their own
    const maxByFmt={};r.lines.forEach(l=>{const m=Math.max.apply(null,[0].concat(l.p));maxByFmt[l.f]=Math.max(maxByFmt[l.f]||0,m);});
    r.lines.forEach((l,i)=>{
      const W=300,H=34,n=l.p.length,mx=(l.f==='Bps'?maxByFmt.Bps:Math.max.apply(null,[0].concat(l.p)))||1,div=document.createElement('div');div.className='sline';
      let svg='<svg viewBox="0 0 '+W+' '+H+'" preserveAspectRatio="none"><line class="base" x1="0" y1="'+(H-1)+'" x2="'+W+'" y2="'+(H-1)+'"/>';
      if(n>0){const x=k=>n===1?W/2:k*(W-4)/(n-1)+2,y=v=>H-3-(H-6)*(mx?v/mx:0),pts=l.p.map((v,k)=>x(k).toFixed(1)+','+y(v).toFixed(1));
        svg+='<polyline class="area c'+(i%6)+'" points="'+x(0).toFixed(1)+','+(H-1)+' '+pts.join(' ')+' '+x(n-1).toFixed(1)+','+(H-1)+'"/>'+'<polyline class="c'+(i%6)+'" points="'+pts.join(' ')+'"/>';
        l.p.forEach((v,k)=>{svg+='<circle cx="'+x(k).toFixed(1)+'" cy="'+y(v).toFixed(1)+'" r="2.2" class="c'+(i%6)+'"><title>'+hhmm(l.ts[k])+'  '+F[l.f](v)+'</title></circle>';});}
      svg+='</svg>';
      div.innerHTML='<div class="sl">'+l.l+'</div>'+svg+'<div class="sv">'+(n?('avg '+F[l.f](l.avg)+' &middot; peak '+F[l.f](l.max)+(l.sum!=null&&l.f!=='sec'&&l.f!=='Bps'?' &middot; total '+F[l.f==='count'?'count':'B'](l.sum):(l.sum!=null&&l.f==='Bps'?' &middot; total '+fB(l.sum):''))):'no data points')+'</div>';
      card.appendChild(div);});
    grid.appendChild(card);});
});
})();
"""


def _series_block_html(title, rows, note):
    payload = json.dumps({"rows": rows}, separators=(",", ":")).replace("</", "<\\/")
    return ('<div class="series"><h3>%s</h3>%s<div class="sgrid"></div><script type="application/json">%s</script></div>'
            % (_html.escape(title), ('<div class="snote">%s</div>' % _html.escape(note)) if note else "", payload))


def _line_class(line):
    s = line.strip()
    m = re.match(r"\[(CRIT|HIGH|MED|INFO)\]", s)
    if m:
        return "sev-" + m.group(1).lower()
    if s.startswith("[!]") or "NOT REACHABLE" in s or "FAILING" in s or "NOT configured" in s:
        return "warn"
    if s.startswith("ERR>"):
        return "err"
    if s.startswith("---") or (s.startswith("NODE ") and " pod(s)" in s):
        return "sub"
    return ""


def _html_table(headers, rows):
    esc = _html.escape
    head = "".join("<th>%s</th>" % esc(h) for h in headers)
    body = "".join("<tr>" + "".join("<td>%s</td>" % esc(str(c)) for c in r) + "</tr>" for r in rows)
    return ('<div class="tablewrap"><div class="tbtools"><input class="tfilter" type="search" placeholder="Filter rows...">'
            '<span class="tcount"></span><button class="csv" type="button">CSV</button></div>'
            '<div class="tscroll"><table class="data"><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div></div>' % (head, body))


_ACRONYMS = ("AWS", "EKS", "CPU", "IAM", "EC2", "HPA", "PVC", "DNS", "API", "SSM")


def _short_title(title):
    """'3. NODES - STATUS, CPU, ...' -> 'Nodes'; 'CLUSTER OVERVIEW - prod-eks' -> 'Cluster overview'."""
    t = re.sub(r"^\d+\.\s*", "", title).split(" (")[0].split(" - ")[0].strip()
    if t.isupper():
        t = t.capitalize()
        for word in _ACRONYMS:
            t = re.sub(r"\b%s\b" % word, word, t, flags=re.I)
    return t


def render_html(label, ctx, rep, steps_log, raw_text):
    """The whole report as ONE self-contained interactive HTML page (collapsible sections,
    sortable/filterable tables, severity filters, global search, timeline filters, dark mode)."""
    esc = _html.escape
    findings = sorted(ctx.findings_full, key=lambda f: _SEV_ORDER.get(f[0], 9))
    counts = Counter(f[0] for f in findings)
    sec_titles = {s["id"]: s["title"] for s in rep.sections}
    per_section = defaultdict(Counter)
    for sev, _, sid in findings:
        per_section[sid][sev] += 1

    # --- navigation
    nav = ['<a href="#summary" class="jump">Health summary</a>']
    for s in rep.sections:
        if s["id"] == "s0" or not s["blocks"]:
            continue
        worst = next((sv for sv in ("CRIT", "HIGH", "MED", "INFO") if per_section[s["id"]][sv]), None)
        badge = f'<span class="badge b-{worst.lower()}">{sum(per_section[s["id"]].values())}</span>' if worst else ""
        nav.append('<a href="#%s" class="jump"><span>%s</span>%s</a>' % (s["id"], esc(_short_title(s["title"])), badge))

    # --- summary
    cards = []
    for sev, cls in (("CRIT", "c-crit"), ("HIGH", "c-high"), ("MED", "c-med"), ("INFO", "c-info")):
        cards.append(f'<div class="sevcard {cls}" data-sev="{sev}" title="click to show/hide"><b>{counts[sev]}</b>{sev}</div>')
    rows = []
    for sev, text, sid in findings:
        where = '<a class="jump" href="#%s">%s</a>' % (sid, esc(_short_title(sec_titles.get(sid, ""))))
        rows.append('<tr data-sev="%s"><td><span class="sevtag badge b-%s">%s</span></td><td>%s</td><td>%s</td></tr>'
                    % (sev, sev.lower(), sev, esc(text), where))
    if findings:
        summary_html = (f'<div class="cards">{"".join(cards)}</div><div class="tablewrap"><div class="tbtools"><span class="tcount"></span></div>'
                        f'<div class="tscroll"><table class="data" id="findings"><thead><tr><th>Severity</th><th>Finding</th><th>Where</th></tr></thead>'
                        f'<tbody>{"".join(rows)}</tbody></table></div></div>')
        # the findings table has no per-table filter/CSV; give it the same hooks, hidden
        summary_html = summary_html.replace('<span class="tcount"></span>', '<span class="tcount"></span><input class="tfilter" style="display:none"><button class="csv" style="display:none">CSV</button>')
    else:
        summary_html = '<div class="cards"><div class="sevcard c-ok"><b>OK</b>No problems detected in the collected data</div></div>'

    contacts = contact_rows(ctx)
    if contacts:
        summary_html += ('<h3 style="margin:18px 0 6px;font-size:14px">Teams to contact - namespaces with problems, grouped by support DL ('
                         + esc(SUPPORT_LABEL) + ')</h3>' + _html_table(["SUPPORT DL", "NAMESPACES", "ISSUES", "WHAT"], contacts))

    # --- sections
    def render_block(block):
        kind = block[0]
        if kind == "lines":
            lines = block[1]
            if not any(l.strip() for l in lines):
                return ""
            spans = "".join(f'<span class="ln {_line_class(l)}">{esc(l)}</span>\n' for l in lines)
            return f'<pre class="lines">{spans}</pre>'
        if kind == "table":
            _, headers, trs = block
            head = "".join(f"<th>{esc(h)}</th>" for h in headers)
            body = "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in trs)
            return ('<div class="tablewrap"><div class="tbtools"><input class="tfilter" type="search" placeholder="Filter rows...">'
                    '<span class="tcount"></span><button class="csv" type="button">CSV</button></div>'
                    f'<div class="tscroll"><table class="data"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div></div>')
        if kind == "log":
            _, title, entries = block
            nerr = sum(1 for _, k in entries if k == "err")
            nwarn = sum(1 for _, k in entries if k == "warn")
            lines = "".join('<span class="ll%s">%s</span>' % ((" " + k) if k else "", esc(t)) for t, k in entries)
            badges = ('<span class="badge b-high">%d error-like</span>' % nerr) if nerr else '<span class="badge b-info">0 errors</span>'
            if nwarn:
                badges += ' <span class="badge b-med">%d warning</span>' % nwarn
            return ('<details class="log"%s><summary>%s %s</summary><div class="logbody">'
                    '<label><input type="checkbox" class="errsonly"> errors only</label> '
                    '<label><input type="checkbox" class="nowrap"> no wrap</label> '
                    '<button type="button" class="copylog">Copy</button><pre>%s</pre></div></details>'
                    % (" open" if nerr else "", esc(title), badges, lines))
        if kind == "util":
            return _util_block_html(block[1])
        if kind == "series":
            return _series_block_html(block[1], block[2], block[3])
        if kind == "timeline":
            entries = block[1]
            kinds = []
            items = []
            for ts, text in entries:
                k = (re.match(r"^([A-Za-z()]+)", text) or [None, "OTHER"])[1].upper()
                if k not in kinds:
                    kinds.append(k)
                items.append(f'<li data-kind="{esc(k)}"><time>{esc(ts)}Z</time><span class="kind">{esc(k)}</span>{esc(text)}</li>')
            chips = "".join(f'<span class="chip on" data-kind="{esc(k)}">{esc(k)}</span>' for k in kinds)
            return f'<div class="tlfilters">{chips}</div><ul class="timeline">{"".join(items)}</ul>'
        return ""

    sections_html = []
    for s in rep.sections:
        if s["id"] == "s0" or not s["blocks"]:
            continue
        worst = next((sv for sv in ("CRIT", "HIGH", "MED", "INFO") if per_section[s["id"]][sv]), None)
        badge = f'<span class="badge b-{worst.lower()}">{sum(per_section[s["id"]].values())} finding(s)</span>' if worst else ""
        body = "".join(render_block(b) for b in s["blocks"])
        sections_html.append(f'<details class="sec" id="{s["id"]}" open><summary><span>{esc(s["title"])}</span>{badge}</summary><div class="secbody">{body}</div></details>')

    run_rows = "".join(f"<tr><td>{esc(t)}</td><td>{esc(st)}</td><td>{esc(sec)}</td></tr>" for t, st, sec in steps_log)
    steps_html = (f'<details class="sec" id="steps"><summary><span>Collection steps</span></summary><div class="secbody">'
                  f'<div class="tablewrap"><div class="tbtools"><span class="tcount"></span><input class="tfilter" style="display:none"><button class="csv" style="display:none">CSV</button></div>'
                  f'<div class="tscroll"><table class="data"><thead><tr><th>Step</th><th>Result</th><th>Time</th></tr></thead><tbody>{run_rows}</tbody></table></div></div></div></details>')

    meta = [f"Context: {ctx.meta.get('context', '?')}", f"Server: {ctx.meta.get('server', '?')}",
            f"Window: last {ctx.minutes} min", f"Generated: {ctx.now:%Y-%m-%d %H:%M:%S} UTC"]
    raw = raw_text.replace("</script", "<\\/script")
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>EKS debug - {esc(label)}</title><style>{_HTML_CSS}{_UTIL_CSS}{_SERIES_CSS}</style></head><body>
<header><h1>EKS debug report - {esc(label)}</h1><div class="meta">{esc("  |  ".join(meta))}</div>
<div class="toolbar"><input id="q" type="search" placeholder="Search everything ( press / )"><span id="hits" class="small"></span>
<button id="expand" type="button">Expand all</button><button id="collapse" type="button">Collapse all</button>
<button id="theme" type="button">Dark / light</button><button id="print" type="button">Print</button><button id="dl" type="button">Download .txt</button></div></header>
<div class="layout"><nav>{"".join(nav)}</nav><main>
<details class="sec" id="summary" open><summary><span>Health summary</span><span class="badge b-info">{len(findings)} finding(s)</span></summary><div class="secbody">{summary_html}</div></details>
{"".join(sections_html)}{steps_html}</main></div>
<footer>Generated by eks_debug.py - read-only data. Pod logs may contain sensitive information.</footer>
<script type="text/plain" id="rawtext">{raw}</script><script>{_HTML_JS}</script><script>{_UTIL_JS}</script><script>{_SERIES_JS}</script></body></html>"""
    return page


# ---------------------------------------------------------------------------
# Orchestration (step by step, with progress + cancel)
# ---------------------------------------------------------------------------

def run_steps(options=None):
    """The ordered collection steps: (key, title, optional_option_name_or_None)."""
    return [
        ("overview", "Cluster overview", None),
        ("data", "Collect cluster data", None),
        ("aws", "AWS EKS details", "aws"),
        ("nodes", "Nodes: CPU / memory / disk / swap", None),
        ("utilization", "Resource utilization by namespace", None),
        ("nodepods", "Pods on each node", None),
        ("namespaces", "Namespaces: pods used vs configured", None),
        ("pods", "Unhealthy pods", None),
        ("events", "Events", None),
        ("workloads", "Workloads", None),
        ("netdetail", "Network & traffic in the window", None),
        ("network", "Autoscaling, storage, network", None),
        ("top", "Top consumers", None),
        ("logs", "Pod logs", "logs"),
        ("timeline", "Timeline", None),
    ]


def run_debug(label, minutes, emit, progress=None, cancel=None, options=None, on_finding=None):
    """Collect everything for the CURRENT kubectl context and write the .txt and the
    interactive .html report. `progress(key, status, seconds)` is called for every step
    (status: running / done / failed / skipped); `cancel` is a threading.Event - when it is
    set the remaining steps are skipped and a partial report is still written.
    Returns the path of the HTML report."""
    options = {"aws": AWS_OPTS["enabled"], "logs": True, "all_logs": False, "log_namespaces": "", **(options or {})}
    AWS_OPTS["enabled"] = bool(options["aws"])
    ctx = Ctx(minutes)
    rep = Report(emit)
    ctx.report, ctx.on_finding, ctx.cancel = rep, on_finding, cancel
    note = progress or (lambda *a, **k: None)
    steps_log = []

    runners = {
        "overview": lambda: section_overview(rep, ctx, label),
        "data": lambda: load_data(ctx, rep),
        "aws": lambda: section_aws(rep, ctx, label),
        "nodes": lambda: section_nodes(rep, ctx),
        "utilization": lambda: section_utilization(rep, ctx),
        "nodepods": lambda: section_node_pods(rep, ctx),
        "namespaces": lambda: section_namespaces(rep, ctx),
        "pods": lambda: section_pods(rep, ctx),
        "events": lambda: section_events(rep, ctx),
        "workloads": lambda: section_workloads(rep, ctx),
        "netdetail": lambda: section_network_details(rep, ctx, label),
        "network": lambda: section_scaling_storage_network(rep, ctx),
        "top": lambda: section_top(rep, ctx),
        "logs": lambda: section_logs(rep, ctx, options),
        "timeline": lambda: section_timeline(rep, ctx),
    }
    stopped = False
    for key, title, opt in run_steps(options):
        if stopped or (cancel is not None and cancel.is_set()):
            if not stopped:
                rep.add("")
                rep.add("Stopped by user - the remaining steps were skipped. The report below is partial.")
            stopped = True
            note(key, "skipped", None)
            steps_log.append((title, "skipped (stopped)", "-"))
            continue
        if opt and not options.get(opt, True):
            note(key, "skipped", None)
            steps_log.append((title, "skipped (turned off)", "-"))
            continue
        note(key, "running", None)
        t0 = time.time()
        try:
            runners[key]()
            status = "done"
        except Exception as exc:  # one broken step must not lose the rest
            rep.add(f"[!] step '{title}' failed: {exc}")
            status = "failed"
            if key == "data":      # nothing else can work without the cluster data
                note(key, status, time.time() - t0)
                steps_log.append((title, f"failed: {exc}", f"{time.time() - t0:.1f}s"))
                raise
        secs = time.time() - t0
        note(key, status, secs)
        steps_log.append((title, status, f"{secs:.1f}s"))

    summary = build_summary(ctx, label)
    rep.add("")
    for line in summary:
        rep.add(line)
    raw_text = "\n".join(summary + [""] + rep.lines[: len(rep.lines) - len(summary) - 1])

    note("report", "running", None)
    os.makedirs(REPORT_DIR, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", label)[:60]
    base0 = os.path.abspath(os.path.join(REPORT_DIR, f"eks_debug_{safe}_{datetime.now():%Y%m%d_%H%M%S}"))
    base, n = base0, 1
    while os.path.exists(base + ".html") or os.path.exists(base + ".txt"):   # never overwrite an earlier report
        n += 1
        base = f"{base0}_{n}"
    with open(base + ".txt", "w", encoding="utf-8") as f:
        f.write(raw_text)
    with open(base + ".html", "w", encoding="utf-8") as f:
        f.write(render_html(label, ctx, rep, steps_log, raw_text))
    note("report", "done", None)
    emit("")
    emit(f"Report saved to: {base}.txt")
    emit(f"Interactive HTML report: {base}.html")
    result = ReportPath(base + ".html")
    result.txt = base + ".txt"
    result.counts = Counter(f[0] for f in ctx.findings_full)
    result.findings = list(ctx.findings_full)
    result.section_titles = {sec["id"]: sec["title"] for sec in rep.sections}
    result.partial = stopped
    result.meta = dict(ctx.meta)
    return result


def find_context(label):
    """Pick the kubectl context that belongs to the selected cluster. Returns
    (context_name_or_None, [candidates], current_context). Matching looks at the context
    name and at the cluster it points to (EKS contexts are usually the cluster ARN, or
    an alias equal to the cluster name): exact name, then '.../<name>', then contains."""
    ok, cur = kubectl(["config", "current-context"])
    current = cur if ok else None
    ok, out = kubectl(["config", "view", "-o", "json"])
    contexts = []
    if ok:
        try:
            contexts = [(c.get("name", ""), (c.get("context") or {}).get("cluster", ""))
                        for c in (json.loads(out).get("contexts") or [])]
        except json.JSONDecodeError:
            pass
    want = (label or "").strip().lower()
    if not want or re.fullmatch(r"cluster-\d+", want):
        return None, [], current

    def score(name, cluster):
        best = 0
        for text in (name.lower(), cluster.lower()):
            tail = text.split("/")[-1]
            if text == want:
                best = max(best, 100)
            elif tail == want:
                best = max(best, 90)
            elif want in text:
                best = max(best, 50)
        return best

    scored = sorted(((score(n, c), n) for n, c in contexts if score(n, c) > 0), reverse=True)
    if not scored:
        return None, [], current
    top = [n for s, n in scored if s == scored[0][0]]
    if len(top) > 1 and current in top:
        return current, top, current
    return (top[0] if len(top) == 1 else None), top, current


def select_context(label, emit, forced=None):
    """Make kubectl point at the selected cluster: `kubectl config use-context <ctx>`, then
    pin every later kubectl call to it with --context. Returns the context name (or None
    if no matching context could be found - the current one is then used, with a warning)."""
    global KUBE_CONTEXT
    KUBE_CONTEXT = None
    if forced:
        ctx_name, candidates, current = forced, [forced], None
    else:
        ctx_name, candidates, current = find_context(label)
    if not ctx_name:
        if len(candidates) > 1:
            emit(f"WARNING: several kubectl contexts match '{label}': {', '.join(candidates[:6])} - not switching. "
                 f"Using the current context ({current}). Pass --context NAME to choose.")
        else:
            emit(f"WARNING: no kubectl context matching '{label}' was found - using the current context "
                 f"({current or 'none'}). Check it is the right cluster, or pass --context NAME.")
        return None
    ok, out = kubectl(["config", "use-context", ctx_name])
    if not ok:
        emit(f"WARNING: could not switch kubectl to context '{ctx_name}': {out.splitlines()[0][:120] if out else '?'} "
             f"- using the current context ({current}).")
        return None
    ok, now = kubectl(["config", "current-context"])
    KUBE_CONTEXT = ctx_name
    if current and current != ctx_name:
        emit(f"kubectl context switched: {current}  ->  {ctx_name}")
    else:
        emit(f"kubectl context is {ctx_name}" + ("" if ok and now == ctx_name else f" (kubectl reports {now})"))
    return ctx_name


def login_and_debug(cluster_number, label, minutes, emit, skip_login=False, context=None,
                    progress=None, cancel=None, options=None, on_finding=None):
    note = progress or (lambda *a, **k: None)
    cli = LOGIN_OPTS["method"] == "cli"
    tgt = CLI_TARGETS.get(str(cluster_number)) if cli else None
    ctx_label = label
    if not skip_login:
        note("login", "running", None)
        t0 = time.time()
        if cli:
            emit(f"Logging in to cluster {cluster_number} ({label}) with the AWS CLI ...")
            try:
                ctx = cli_login(cluster_number, label, emit)
            except RuntimeError:
                note("login", "failed", time.time() - t0)
                raise
            context = context or ctx
            tgt = CLI_TARGETS.get(str(cluster_number))
        else:
            emit(f"Logging in to cluster {cluster_number} ({label}) with ekslogin ...")
            if not ekslogin(cluster_number):
                note("login", "failed", time.time() - t0)
                raise RuntimeError(f"ekslogin failed for cluster {cluster_number}")
        emit("Login OK.")
        note("login", "done", time.time() - t0)
    else:
        note("login", "skipped", None)
        if tgt:
            ctx_label = tgt["name"]
    saved = {k: AWS_OPTS[k] for k in ("cluster", "region")}
    if tgt:   # the CLI listing already knows the cluster: hand it to the existing AWS profile step so nothing is guessed
        AWS_OPTS.update(cluster=tgt["name"], region=tgt["region"])
    try:
        note("context", "running", None)
        t0 = time.time()
        select_context(ctx_label, emit, forced=context)
        note("context", "done", time.time() - t0)
        if (options or {}).get("aws", AWS_OPTS["enabled"]) and AWS_OPTS["enabled"]:
            note("profile", "running", None)
            t0 = time.time()
            try:
                select_aws_profile(label, emit, preferred=AWS_OPTS["profile"])
            except Exception as exc:  # never block the kubectl data because of AWS profile trouble
                emit(f"WARNING: could not choose an AWS profile: {exc}")
            note("profile", "done", time.time() - t0)
        else:
            note("profile", "skipped", None)
        return run_debug(label, minutes, emit, progress, cancel, options, on_finding)
    finally:
        AWS_OPTS.update(saved)

class ReportPath(str):
    """The path of a cluster's HTML report. It also carries what the run found, which the
    multi-cluster summary page is built from."""
    txt = None
    counts = None
    findings = None
    section_titles = None
    partial = False
    meta = None


def _clusters_named(word, known):
    """Numbers of the clusters called `word` (the full list entry, or its name without the '(region ...)' part)."""
    word = word.strip().lower()
    return [k for k, v in known.items() if word in (str(v).strip().lower(), str(v).split(" (")[0].strip().lower())]


def parse_cluster_selection(text, known):
    """'1,3,5' / '2-4' / 'all' / a cluster name -> [(number, label), ...] (numbers in the order given, no duplicates)."""
    text = (text or "").strip().lower()
    if text == "all":
        numbers = sorted(known, key=lambda k: int(k) if str(k).isdigit() else 10**9)
    else:
        numbers = []
        for part in re.split(r"[,\s;]+", text):
            m = re.fullmatch(r"(\d+)-(\d+)", part)
            if m:
                numbers += [str(i) for i in range(int(m.group(1)), int(m.group(2)) + 1)]
            elif part.isdigit():
                numbers.append(str(int(part)))
            elif part:
                numbers += _clusters_named(part, known)
    seen, out = set(), []
    for n in numbers:
        if n not in seen:
            seen.add(n)
            out.append((n, known.get(n) or f"cluster-{n}"))
    return out


def run_clusters(selected, minutes, emit, skip_login=False, context=None, progress=None, cancel=None,
                 options=None, on_finding=None, on_cluster=None):
    """Run the whole debug for several clusters, ONE AFTER ANOTHER (ekslogin, the kubectl context
    and the AWS profile are shared state, so they must not overlap). Every cluster gets its own
    .txt and .html report; with more than one cluster a summary page linking them is written too.
    A failing cluster is recorded and the next one still runs. `cancel` stops after the current step
    of the current cluster, and the remaining clusters are marked 'not run'.
    Returns {"items": [per-cluster dicts], "index": path of the summary page or None}."""
    notify = on_cluster or (lambda *a, **k: None)
    n = len(selected)
    if n > 1 and context:
        emit("NOTE: --context applies to a single cluster and is ignored when several are selected.")
    entries = []
    for i, (number, label) in enumerate(selected, start=1):
        entry = {"number": number, "label": label, "status": "not run", "html": None, "txt": None,
                 "counts": Counter(), "findings": [], "titles": {}, "secs": None, "error": None}
        entries.append(entry)
        if cancel is not None and cancel.is_set():
            entry["status"] = "not run (stopped)"
            notify(i, n, label, entry["status"], entry)
            continue
        emit("")
        emit("#" * 78)
        emit(f"# CLUSTER {i} of {n}: {label}  (#{number})")
        emit("#" * 78)
        notify(i, n, label, "running", entry)
        t0 = time.time()
        finding_cb = on_finding
        if on_finding and n > 1:
            finding_cb = lambda sev, text, _l=label: on_finding(sev, f"[{_l}] {text}")
        try:
            result = login_and_debug(number, label, minutes, emit, skip_login, context if n == 1 else None,
                                     progress, cancel, options, finding_cb)
            entry.update(status="stopped (partial)" if getattr(result, "partial", False) else "ok",
                         html=str(result), txt=getattr(result, "txt", None), counts=getattr(result, "counts", Counter()),
                         findings=getattr(result, "findings", []), titles=getattr(result, "section_titles", {}))
        except Exception as exc:
            entry.update(status="failed", error=str(exc))
            emit(f"ERROR on cluster {label}: {exc}")
        entry["secs"] = time.time() - t0
        notify(i, n, label, entry["status"], entry)

    index = None
    if n > 1:
        try:
            index = write_cluster_index(entries, minutes)
            emit("")
            emit(f"Multi-cluster summary: {index}")
        except Exception as exc:
            emit(f"WARNING: could not write the summary page: {exc}")
    return {"items": entries, "index": index}


def write_cluster_index(entries, minutes):
    os.makedirs(REPORT_DIR, exist_ok=True)
    base0 = os.path.abspath(os.path.join(REPORT_DIR, f"eks_debug_summary_{datetime.now():%Y%m%d_%H%M%S}"))
    path, n = base0 + ".html", 1
    while os.path.exists(path):
        n += 1
        path = f"{base0}_{n}.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(render_index_html(entries, minutes))
    return path


def render_index_html(entries, minutes):
    """One page for a multi-cluster run: a row per cluster (status, finding counts, link to its report)
    and every finding from every cluster in one sortable, filterable table."""
    esc = _html.escape
    total = Counter()
    for en in entries:
        total.update(en["counts"])
    rows, finding_rows, raw = [], [], []
    for en in entries:
        c = en["counts"]
        shown = {"ok": "OK", "failed": "FAILED"}.get(en["status"], en["status"])
        link = os.path.basename(en["html"]) if en["html"] else None
        txt = os.path.basename(en["txt"]) if en.get("txt") else None
        ordered = sorted(en["findings"], key=lambda f: _SEV_ORDER.get(f[0], 9))
        top = (ordered[0][1] if ordered else (en["error"] or ("no problems detected" if en["status"] == "ok" else "-")))[:140]
        report = ('<a href="%s">open report</a>' % esc(link) + (' &nbsp; <a href="%s">.txt</a>' % esc(txt) if txt else "")) if link else "-"
        rows.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                    % (esc(en["label"]), esc(shown), c["CRIT"], c["HIGH"], c["MED"], c["INFO"],
                       ("%.0fs" % en["secs"]) if en["secs"] else "-", esc(top), report))
        for sev, text, sid in ordered:
            where = ('<a href="%s#%s">%s</a>' % (esc(link), esc(sid), esc(_short_title(en["titles"].get(sid, sid))))) if link else "-"
            finding_rows.append('<tr data-sev="%s"><td><span class="sevtag badge b-%s">%s</span></td><td>%s</td><td>%s</td><td>%s</td></tr>'
                                % (sev, sev.lower(), sev, esc(en["label"]), esc(text), where))
        raw.append(f"== {en['label']}: {shown}  CRIT {c['CRIT']}  HIGH {c['HIGH']}  MED {c['MED']}  INFO {c['INFO']}")
        raw += [f"   [{sev}] {text}" for sev, text, _ in ordered]
        if en["error"]:
            raw.append(f"   ERROR: {en['error']}")
    cards = "".join('<div class="sevcard %s" data-sev="%s" title="click to show/hide"><b>%d</b>%s</div>' % (cls, sev, total[sev], sev)
                    for sev, cls in (("CRIT", "c-crit"), ("HIGH", "c-high"), ("MED", "c-med"), ("INFO", "c-info")))
    stamp = f"{datetime.now():%Y-%m-%d %H:%M:%S}"
    clusters_table = ('<div class="tablewrap"><div class="tbtools"><input class="tfilter" type="search" placeholder="Filter clusters...">'
                      '<span class="tcount"></span><button class="csv" type="button">CSV</button></div><div class="tscroll">'
                      '<table class="data"><thead><tr><th>Cluster</th><th>Status</th><th>CRIT</th><th>HIGH</th><th>MED</th><th>INFO</th>'
                      '<th>Time</th><th>Top finding</th><th>Report</th></tr></thead><tbody>%s</tbody></table></div></div>' % "".join(rows))
    findings_table = ('<div class="tablewrap"><div class="tbtools"><span class="tcount"></span><input class="tfilter" style="display:none">'
                      '<button class="csv" style="display:none">CSV</button></div><div class="tscroll"><table class="data" id="findings"><thead>'
                      '<tr><th>Severity</th><th>Cluster</th><th>Finding</th><th>Where</th></tr></thead><tbody>%s</tbody></table></div></div>'
                      % ("".join(finding_rows) or '<tr data-sev="INFO"><td></td><td></td><td>No findings</td><td></td></tr>'))
    raw_text = "\n".join([f"EKS DEBUG - {len(entries)} clusters, last {minutes} min, {stamp}", ""] + raw).replace("</script", "<\\/script")
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>EKS debug - {len(entries)} clusters</title><style>{_HTML_CSS}</style></head><body>
<header><h1>EKS debug - summary of {len(entries)} clusters</h1><div class="meta">Window: last {minutes} min  |  Generated: {stamp}</div>
<div class="toolbar"><input id="q" type="search" placeholder="Search everything ( press / )"><span id="hits" class="small"></span>
<button id="expand" type="button">Expand all</button><button id="collapse" type="button">Collapse all</button>
<button id="theme" type="button">Dark / light</button><button id="print" type="button">Print</button><button id="dl" type="button">Download .txt</button></div></header>
<div class="layout"><nav><a href="#clusters" class="jump">Clusters</a><a href="#allfindings" class="jump">All findings</a></nav><main>
<details class="sec" id="clusters" open><summary><span>Clusters</span><span class="badge b-info">{len(entries)}</span></summary><div class="secbody">{clusters_table}</div></details>
<details class="sec" id="allfindings" open><summary><span>All findings (every cluster)</span><span class="badge b-info">{sum(total.values())}</span></summary><div class="secbody"><div class="cards">{cards}</div>{findings_table}</div></details>
</main></div><footer>Generated by eks_debug.py - read-only data. The per-cluster reports are the files linked above (keep them in the same folder).</footer>
<script type="text/plain" id="rawtext">{raw_text}</script><script>{_HTML_JS}</script></body></html>"""


# ---------------------------------------------------------------------------
# GUI: live, interactive collection
# ---------------------------------------------------------------------------

_GUI = {}   # widgets of the running window (used by the tests)


def run_gui(default_minutes, skip_login=False, context=None):
    import pathlib
    import tkinter as tk
    import webbrowser
    from tkinter import ttk, scrolledtext

    root = tk.Tk()
    root.title("EKS Debugger")
    root.geometry("1360x940")
    msgs = queue.Queue()
    state = {"busy": False, "cancel": None, "html": None, "t0": None, "finished": 0, "total": 1,
             "counts": Counter(), "clusters": {}, "chosen": set(), "reports": {}, "n": 1}
    ICON = {"pending": "o", "running": ">>", "done": "OK", "failed": "FAILED", "skipped": "-"}

    # ---- top bar: window, actions
    top = ttk.Frame(root, padding=8)
    top.pack(fill="x")
    ttk.Label(top, text="Last (minutes):").pack(side="left")
    minutes_var = tk.StringVar(value=str(default_minutes))
    ttk.Spinbox(top, from_=1, to=1440, width=6, textvariable=minutes_var).pack(side="left", padx=(4, 0))
    aws_var = tk.BooleanVar(value=AWS_OPTS["enabled"])
    logs_var = tk.BooleanVar(value=True)
    open_var = tk.BooleanVar(value=True)
    alllogs_var = tk.BooleanVar(value=False)
    ns_var = tk.StringVar(value="")
    run_btn = ttk.Button(top, text="Login && Debug selected cluster(s)")
    run_btn.pack(side="left", padx=(16, 4))
    stop_btn = ttk.Button(top, text="Stop", state="disabled")
    stop_btn.pack(side="left")
    refresh_btn = ttk.Button(top, text="Reload clusters")
    refresh_btn.pack(side="left", padx=4)

    top2 = ttk.Frame(root, padding=(8, 0, 8, 4))
    top2.pack(fill="x")
    ttk.Checkbutton(top2, text="AWS details", variable=aws_var).pack(side="left")
    ttk.Checkbutton(top2, text="Pod logs", variable=logs_var).pack(side="left", padx=(10, 0))
    ttk.Checkbutton(top2, text="Logs of ALL pods", variable=alllogs_var).pack(side="left", padx=(10, 0))
    ttk.Label(top2, text="only namespaces (comma separated, blank = all):").pack(side="left", padx=(10, 2))
    ttk.Entry(top2, textvariable=ns_var, width=26).pack(side="left")
    ttk.Checkbutton(top2, text="Open report when done", variable=open_var).pack(side="left", padx=(16, 0))

    top3 = ttk.Frame(root, padding=(8, 0, 8, 4))
    top3.pack(fill="x")
    ttk.Label(top3, text="AWS profile (from ~/.aws):").pack(side="left")
    profile_combo = ttk.Combobox(top3, width=62)
    profile_combo.pack(side="left", padx=6)
    ttk.Label(top3, text="(auto) picks the profile for each selected cluster after login and checks it works").pack(side="left")

    def load_profiles():
        try:
            found = list_aws_profiles()
        except Exception:
            found = {}
        profile_combo["values"] = ["(auto)"] + [describe_profile(n, i) for n, i in sorted(found.items())]
        if not profile_combo.get():
            profile_combo.set("(auto)")

    # ---- login method: the custom login exe (default), or the standard AWS CLI
    top4 = ttk.Frame(root, padding=(8, 0, 8, 4))
    top4.pack(fill="x")
    ttk.Label(top4, text="Login method:").pack(side="left")
    method_combo = ttk.Combobox(top4, width=28, state="readonly", values=[LOGIN_LABELS["exe"], LOGIN_LABELS["cli"]])
    method_combo.set(LOGIN_LABELS[LOGIN_OPTS["method"]])
    method_combo.pack(side="left", padx=6)
    device_var = tk.BooleanVar(value=LOGIN_OPTS["device_code"])
    ttk.Checkbutton(top4, text="device-code login (no browser pop-up)", variable=device_var).pack(side="left", padx=(8, 0))
    ttk.Label(top4, text="Region(s):").pack(side="left", padx=(12, 2))
    region0 = AWS_OPTS["region"]
    region_var = tk.StringVar(value=region0 or "")
    ttk.Entry(top4, textvariable=region_var, width=18).pack(side="left")
    ttk.Label(top4, text="Cloud CLI: the cluster list comes from aws; a console window opens if you have to sign in").pack(side="left", padx=(12, 0))
    LOGIN_OPTS["gui"] = True        # no console of our own: interactive logins get a window

    # ---- cluster picker: a list you can select SEVERAL clusters in
    cl_box = ttk.LabelFrame(root, text="Clusters  -  click one, Ctrl/Shift-click for several (they run one after another)", padding=6)
    cl_box.pack(fill="x", padx=8, pady=(2, 4))
    cl_left = ttk.Frame(cl_box)
    cl_left.pack(side="left", fill="x", expand=True)
    cluster_tree = ttk.Treeview(cl_left, show="tree", selectmode="extended", height=5)
    cluster_tree.column("#0", width=600)
    cl_scroll = ttk.Scrollbar(cl_left, orient="vertical", command=cluster_tree.yview)
    cluster_tree.configure(yscrollcommand=cl_scroll.set)
    cl_scroll.pack(side="right", fill="y")
    cluster_tree.pack(side="left", fill="x", expand=True)
    cl_right = ttk.Frame(cl_box)
    cl_right.pack(side="left", padx=(12, 0))
    filter_var = tk.StringVar(value="")
    manual_var = tk.StringVar(value="")
    ttk.Label(cl_right, text="Filter:").grid(row=0, column=0, sticky="e")
    ttk.Entry(cl_right, textvariable=filter_var, width=24).grid(row=0, column=1, padx=4, pady=1)
    select_all_btn = ttk.Button(cl_right, text="Select all (shown)")
    select_all_btn.grid(row=1, column=0, columnspan=1, pady=2, sticky="ew")
    clear_btn = ttk.Button(cl_right, text="Clear")
    clear_btn.grid(row=1, column=1, pady=2, sticky="w", padx=4)
    ttk.Label(cl_right, text="or type numbers:").grid(row=2, column=0, sticky="e")
    ttk.Entry(cl_right, textvariable=manual_var, width=24).grid(row=2, column=1, padx=4, pady=1)
    ttk.Label(cl_right, text="e.g.  1,3,5   2-4   all").grid(row=3, column=1, sticky="w", padx=4)
    sel_text = tk.StringVar(value="Selected: none")
    ttk.Label(root, textvariable=sel_text, padding=(12, 0, 8, 2)).pack(fill="x")

    # ---- body: steps + clusters-in-run + live findings (left), live log (right)
    body = ttk.PanedWindow(root, orient="horizontal")
    body.pack(fill="both", expand=True, padx=8)
    left = ttk.Frame(body, width=450)
    body.add(left, weight=0)
    steps_box = ttk.LabelFrame(left, text="Collection steps (current cluster)", padding=4)
    steps_box.pack(fill="x")
    steps = ttk.Treeview(steps_box, columns=("status", "time"), height=12, show="tree headings", selectmode="none")
    steps.heading("#0", text="Step")
    steps.heading("status", text="Status")
    steps.heading("time", text="Time")
    steps.column("#0", width=270)
    steps.column("status", width=80, anchor="center")
    steps.column("time", width=60, anchor="e")
    steps.pack(fill="x")
    for tag, color in (("running", "#1f4e79"), ("done", "#067647"), ("failed", "#c00000"), ("skipped", "#888888")):
        steps.tag_configure(tag, foreground=color)
    steps.tag_configure("running", font=("Segoe UI", 9, "bold"))

    run_box = ttk.LabelFrame(left, text="Clusters in this run (double-click a finished one to open its report)", padding=4)
    run_box.pack(fill="x", pady=(6, 0))
    run_tree = ttk.Treeview(run_box, columns=("status", "crit", "high"), height=4, show="tree headings", selectmode="browse")
    run_tree.heading("#0", text="Cluster")
    run_tree.heading("status", text="Status")
    run_tree.heading("crit", text="CRIT")
    run_tree.heading("high", text="HIGH")
    run_tree.column("#0", width=210)
    run_tree.column("status", width=110, anchor="center")
    run_tree.column("crit", width=50, anchor="center")
    run_tree.column("high", width=50, anchor="center")
    run_tree.pack(fill="x")
    for tag, color in (("running", "#1f4e79"), ("ok", "#067647"), ("failed", "#c00000"), ("partial", "#9a7d0a"), ("notrun", "#888888")):
        run_tree.tag_configure(tag, foreground=color)

    find_box = ttk.LabelFrame(left, text="Findings (live - updates while collecting)", padding=4)
    find_box.pack(fill="both", expand=True, pady=(6, 0))
    counters = ttk.Frame(find_box)
    counters.pack(fill="x")
    counter_vars = {}
    for sev, color in (("CRIT", "#c00000"), ("HIGH", "#d35400"), ("MED", "#9a7d0a"), ("INFO", "#1f6feb")):
        counter_vars[sev] = tk.StringVar(value=f"{sev} 0")
        tk.Label(counters, textvariable=counter_vars[sev], fg=color, font=("Segoe UI", 10, "bold")).pack(side="left", padx=(0, 14))
    findings = ttk.Treeview(find_box, columns=("sev", "text"), show="headings", height=8)
    findings.heading("sev", text="Sev")
    findings.heading("text", text="Finding")
    findings.column("sev", width=50, anchor="center")
    findings.column("text", width=380)
    fs = ttk.Scrollbar(find_box, orient="vertical", command=findings.yview)
    findings.configure(yscrollcommand=fs.set)
    fs.pack(side="right", fill="y")
    findings.pack(fill="both", expand=True)
    for tag, color in (("CRIT", "#c00000"), ("HIGH", "#d35400"), ("MED", "#9a7d0a"), ("INFO", "#1f6feb")):
        findings.tag_configure(tag, foreground=color)

    right = ttk.Frame(body)
    body.add(right, weight=1)
    text = scrolledtext.ScrolledText(right, font=("Consolas", 9), wrap="none")
    text.pack(fill="both", expand=True)
    for tag, color in (("CRIT", "#c00000"), ("HIGH", "#d35400"), ("MED", "#9a7d0a"), ("HEAD", "#1f4e79"), ("ERR", "#c00000"), ("WARN", "#d35400")):
        text.tag_configure(tag, foreground=color)
    text.tag_configure("HEAD", font=("Consolas", 9, "bold"))

    # ---- bottom: progress + status + actions
    bottom = ttk.Frame(root, padding=8)
    bottom.pack(fill="x")
    progress_bar = ttk.Progressbar(bottom, mode="determinate", length=340)
    progress_bar.pack(side="left")
    status = tk.StringVar(value="Loading cluster list ...")
    ttk.Label(bottom, textvariable=status).pack(side="left", padx=10)
    elapsed = tk.StringVar(value="")
    ttk.Label(bottom, textvariable=elapsed).pack(side="left")
    folder_btn = ttk.Button(bottom, text="Open reports folder")
    folder_btn.pack(side="right")
    open_btn = ttk.Button(bottom, text="Open HTML report", state="disabled")
    open_btn.pack(side="right", padx=6)

    def write(line):
        s = line.lstrip()
        tag = None
        if s.startswith("[CRIT]"):
            tag = "CRIT"
        elif s.startswith("[HIGH]"):
            tag = "HIGH"
        elif s.startswith("[MED]"):
            tag = "MED"
        elif s.startswith("ERR>"):
            tag = "ERR"
        elif s.startswith("[!]") or s.startswith("WARNING") or s.startswith("ERROR"):
            tag = "WARN"
        elif line.startswith("=") or line.startswith("#"):
            tag = "HEAD"
        text.insert("end", line + "\n", tag)
        text.see("end")

    # ---- cluster list helpers
    def rebuild_cluster_list():
        want = filter_var.get().strip().lower()
        cluster_tree.delete(*cluster_tree.get_children())
        shown = []
        for k, v in sorted(state["clusters"].items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 10**9):
            if not want or want in f"{k} {v}".lower():
                cluster_tree.insert("", "end", iid=k, text=f"{k} - {v}")
                shown.append(k)
        cluster_tree.selection_set([k for k in shown if k in state["chosen"]])
        update_selected_label()

    def on_tree_select(_event=None):
        visible = set(cluster_tree.get_children())
        state["chosen"] = (state["chosen"] - visible) | set(cluster_tree.selection())
        update_selected_label()

    def typed_numbers():
        return [n for n, _ in parse_cluster_selection(manual_var.get(), state["clusters"])] if manual_var.get().strip() else []

    def chosen_clusters():
        """[(number, label)] from the list selection plus typed numbers, in numeric order."""
        numbers = set(state["chosen"]) | set(typed_numbers())
        order = sorted(numbers, key=lambda k: int(k) if k.isdigit() else 10**9)
        return [(n, state["clusters"].get(n) or f"cluster-{n}") for n in order]

    def update_selected_label():
        sel = chosen_clusters()
        sel_text.set("Selected: none" if not sel else f"Selected {len(sel)}: " + ", ".join(f"{n} {l}" for n, l in sel)[:230])

    def select_all():
        state["chosen"] |= set(cluster_tree.get_children())
        cluster_tree.selection_set(list(state["chosen"] & set(cluster_tree.get_children())))
        update_selected_label()

    def clear_selection():
        state["chosen"] = set()
        manual_var.set("")
        cluster_tree.selection_remove(cluster_tree.selection())
        update_selected_label()

    def plan(options):
        rows = [("login", "Login (Cloud CLI: aws)" if LOGIN_OPTS["method"] == "cli" else "Login (ekslogin)"), ("context", "Select kubectl context"), ("profile", "Select AWS profile (~/.aws)")]
        rows += [(k, t) for k, t, _ in run_steps(options)] + [("report", "Write HTML report")]
        return rows

    def reset_steps():
        steps.delete(*steps.get_children())
        rows = plan({})
        for key, title in rows:
            steps.insert("", "end", iid=key, text=title, values=(ICON["pending"], ""))
        state["total"], state["finished"] = len(rows), 0
        progress_bar.configure(maximum=len(rows), value=0)

    def reset_run(selected):
        findings.delete(*findings.get_children())
        state["counts"] = Counter()
        for sev in counter_vars:
            counter_vars[sev].set(f"{sev} 0")
        run_tree.delete(*run_tree.get_children())
        state["reports"] = {}
        for number, label in selected:
            run_tree.insert("", "end", iid=number, text=f"{number} - {label}", values=("waiting", "", ""), tags=("notrun",))

    def sync_login_opts():
        """Read the login method and the profile / region boxes into the options a run (or a cluster listing) uses."""
        LOGIN_OPTS["method"] = "cli" if method_combo.get() == LOGIN_LABELS["cli"] else "exe"
        LOGIN_OPTS["device_code"] = device_var.get()
        chosen = profile_combo.get().strip().split("  ")[0].strip()
        AWS_OPTS["profile"] = None if chosen in ("", "(auto)") else chosen
        AWS_OPTS["region"] = (region_var.get().strip() or None) if LOGIN_OPTS["method"] == "cli" else region0

    def load_clusters():
        if state["busy"] and method_combo.get() == LOGIN_LABELS["cli"]:
            status.set("Wait for the current run to finish (or press Stop) before reloading the Cloud CLI cluster list.")
            return
        refresh_btn.state(["disabled"])
        method_combo.configure(state="disabled")
        sync_login_opts()
        cli = LOGIN_OPTS["method"] == "cli"
        status.set("Loading cluster list from the aws CLI (you may be asked to sign in) ..." if cli else "Loading cluster list ...")
        threading.Thread(target=lambda: msgs.put(("clusters", list_clusters(lambda l: msgs.put(("line", l))) if cli else list_clusters())),
                         daemon=True).start()

    def on_method_change(_event=None):
        if state["busy"]:
            method_combo.set(LOGIN_LABELS[LOGIN_OPTS["method"]])
            status.set("Wait for the current run to finish (or press Stop) before changing the login method.")
            return
        state["clusters"], state["chosen"] = {}, set()
        manual_var.set("")
        rebuild_cluster_list()
        load_clusters()
        reset_steps()

    def start():
        if state["busy"]:
            return
        selected = chosen_clusters()
        if not selected:
            status.set("Select at least one cluster in the list (or type numbers like 1,3).")
            return
        try:
            minutes = max(1, int(minutes_var.get()))
        except ValueError:
            status.set("Minutes must be a number.")
            return
        options = {"aws": aws_var.get(), "logs": logs_var.get(), "all_logs": alllogs_var.get(), "log_namespaces": ns_var.get()}
        sync_login_opts()
        state.update(busy=True, cancel=threading.Event(), html=None, t0=time.time(), n=len(selected))
        method_combo.configure(state="disabled")
        run_btn.state(["disabled"])
        stop_btn.state(["!disabled"])
        open_btn.state(["disabled"])
        text.delete("1.0", "end")
        reset_steps()
        reset_run(selected)
        status.set(f"Starting {len(selected)} cluster(s) ...")

        def work():
            try:
                results = run_clusters(
                    selected, minutes, lambda l: msgs.put(("line", l)), skip_login, context,
                    progress=lambda k, st, secs: msgs.put(("step", k, st, secs)), cancel=state["cancel"], options=options,
                    on_finding=lambda sev, t: msgs.put(("finding", sev, t)),
                    on_cluster=lambda i, n, label, st, entry: msgs.put(("cluster", i, n, label, st, entry)))
                msgs.put(("done", results))
            except Exception as exc:
                msgs.put(("error", str(exc)))
        threading.Thread(target=work, daemon=True).start()

    def stop():
        if state["cancel"] is not None:
            state["cancel"].set()
            stop_btn.state(["disabled"])
            status.set("Stopping after the current step ... a partial report is saved, remaining clusters are skipped.")

    def open_path(path):
        if path:
            webbrowser.open(pathlib.Path(path).as_uri())

    def open_report():
        open_path(state["html"])

    def open_cluster_report(_event=None):
        sel = run_tree.selection()
        if sel and state["reports"].get(sel[0]):
            open_path(state["reports"][sel[0]])

    def open_folder():
        os.makedirs(REPORT_DIR, exist_ok=True)
        try:
            os.startfile(os.path.abspath(REPORT_DIR))  # Windows
        except Exception:
            webbrowser.open(pathlib.Path(os.path.abspath(REPORT_DIR)).as_uri())

    def finish(ok_text):
        state["busy"] = False
        method_combo.configure(state="readonly")
        run_btn.state(["!disabled"])
        stop_btn.state(["disabled"])
        status.set(ok_text)

    RUN_TAG = {"running": "running", "ok": "ok", "failed": "failed", "stopped (partial)": "partial"}

    def poll():
        try:
            while True:
                msg = msgs.get_nowait()
                kind = msg[0]
                if kind == "line":
                    write(msg[1])
                elif kind == "step":
                    _, key, st, secs = msg
                    if steps.exists(key):
                        steps.item(key, values=(ICON.get(st, st), f"{secs:.1f}s" if secs else ""), tags=(st,))
                    if st in ("done", "skipped", "failed"):
                        state["finished"] += 1
                        progress_bar.configure(value=state["finished"])
                    elif st == "running" and steps.exists(key):
                        steps.see(key)
                        status.set(f"{steps.item(key, 'text')} ...")
                elif kind == "cluster":
                    _, i, n, label, st, entry = msg
                    number = entry["number"]
                    shown = {"ok": "OK", "failed": "FAILED", "running": "running ..."}.get(st, st)
                    c = entry["counts"] if entry.get("counts") else Counter()
                    if run_tree.exists(number):
                        run_tree.item(number, values=(shown, c["CRIT"] if st != "running" else "", c["HIGH"] if st != "running" else ""),
                                      tags=(RUN_TAG.get(st, "notrun"),))
                    if st == "running":
                        reset_steps()                       # fresh checklist for this cluster
                        status.set(f"Cluster {i} of {n}: {label} ...")
                    elif entry.get("html"):
                        state["reports"][number] = entry["html"]
                elif kind == "finding":
                    _, sev, t = msg
                    state["counts"][sev] += 1
                    counter_vars[sev].set(f"{sev} {state['counts'][sev]}")
                    findings.insert("", "end", values=(sev, t), tags=(sev,))
                    findings.yview_moveto(1.0)
                elif kind == "clusters":
                    state["clusters"] = msg[1]
                    rebuild_cluster_list()
                    refresh_btn.state(["!disabled"])
                    if not state["busy"]:
                        method_combo.configure(state="readonly")
                    if msg[1]:
                        status.set(f"{len(msg[1])} cluster(s) found. Select one or more, then press Login & Debug.")
                    else:
                        status.set("No clusters found with the aws CLI - see the log (sign-in / profile / region), then press Reload clusters."
                                   if LOGIN_OPTS["method"] == "cli" else
                                   "Could not read the cluster list from ekslogin - type the cluster number(s) in the box.")
                elif kind == "done":
                    results = msg[1]
                    ok = [r for r in results["items"] if r["html"]]
                    state["html"] = results["index"] or (ok[0]["html"] if ok else None)
                    load_profiles()          # ekslogin may have created / refreshed profiles
                    progress_bar.configure(value=state["total"])
                    if state["html"]:
                        open_btn.state(["!disabled"])
                    bad = [r["label"] for r in results["items"] if r["status"] == "failed"]
                    finish(f"Done: {len(ok)} of {len(results['items'])} cluster(s) reported"
                           + (f" ({len(bad)} failed: {', '.join(bad)})" if bad else "")
                           + ". Click 'Open HTML report'." if state["html"] else "Finished, but no report could be written - see the log.")
                    if state["html"] and open_var.get():
                        open_report()
                elif kind == "error":
                    write(f"ERROR: {msg[1]}")
                    finish(f"ERROR: {msg[1]}")
        except queue.Empty:
            pass
        if state["busy"] and state["t0"]:
            s = int(time.time() - state["t0"])
            elapsed.set(f"elapsed {s // 60}:{s % 60:02d}")
        root.after(150, poll)

    run_btn.configure(command=start)
    stop_btn.configure(command=stop)
    refresh_btn.configure(command=load_clusters)
    open_btn.configure(command=open_report)
    folder_btn.configure(command=open_folder)
    select_all_btn.configure(command=select_all)
    clear_btn.configure(command=clear_selection)
    cluster_tree.bind("<<TreeviewSelect>>", on_tree_select)
    run_tree.bind("<Double-1>", open_cluster_report)
    filter_var.trace_add("write", lambda *_: rebuild_cluster_list())
    manual_var.trace_add("write", lambda *_: update_selected_label())
    reset_steps()
    load_profiles()
    _GUI.update(root=root, cluster_tree=cluster_tree, run_btn=run_btn, stop_btn=stop_btn, open_btn=open_btn, steps=steps,
                findings=findings, text=text, status=status, state=state, counter_vars=counter_vars,
                open_var=open_var, aws_var=aws_var, logs_var=logs_var, progress=progress_bar,
                alllogs_var=alllogs_var, ns_var=ns_var, profile_combo=profile_combo, load_profiles=load_profiles,
                select_all_btn=select_all_btn, clear_btn=clear_btn, filter_var=filter_var, manual_var=manual_var,
                run_tree=run_tree, sel_text=sel_text, chosen_clusters=chosen_clusters,
                method_combo=method_combo, device_var=device_var, on_method_change=on_method_change)
    method_combo.bind("<<ComboboxSelected>>", on_method_change)
    load_clusters()
    poll()
    root.mainloop()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli_progress(key, status, secs):
    if status != "running":
        print(f"  [{key}] {status}" + (f" ({secs:.1f}s)" if secs else ""), flush=True)


def main():
    global LOOKBACK_MINUTES, EKSLOGIN_EXE, LOG_TAIL_LINES, SUPPORT_LABEL, TRAFFIC_SAMPLE_SECONDS
    parser = argparse.ArgumentParser(description="EKS debugger: log in, then show what happened in the last N minutes")
    parser.add_argument("--minutes", type=int, default=None, help=f"time window in minutes (default {LOOKBACK_MINUTES})")
    parser.add_argument("--cluster", help="cluster number(s) or name to log in to and debug (no GUI): 3 | 1,3,5 | 2-4 | all | my-cluster. "
                                          "Several clusters run one after another and get a combined summary page")
    parser.add_argument("--name", help="label for the report when ONE cluster is given with --cluster (default: from the cluster list)")
    parser.add_argument("--list", action="store_true", help="print the clusters ekslogin offers and exit")
    parser.add_argument("--skip-login", action="store_true", help="don't run ekslogin; use the current kubectl context")
    parser.add_argument("--ekslogin", help="path to ekslogin.exe")
    parser.add_argument("--no-gui", action="store_true", help="never open the GUI")
    parser.add_argument("--login-method", choices=["exe", "cli"], default="exe",
                        help="how to log in: exe = the custom ekslogin.exe (default); cli = the standard AWS CLI (aws) - then --list / --cluster use the cluster list read from aws")
    parser.add_argument("--device-code", action="store_true",
                        help="with --login-method cli: sign in without a browser pop-up (aws sso login --use-device-code)")
    parser.add_argument("--aws-cluster", help="EKS cluster name for the AWS checks (default: read from kubeconfig)")
    parser.add_argument("--region", help="AWS region for the AWS checks (default: read from kubeconfig)")
    parser.add_argument("--profile", help="AWS profile (from ~/.aws) to use for the AWS checks. Default: chosen automatically after login")
    parser.add_argument("--context", help="kubectl context to use (default: matched from the selected cluster)")
    parser.add_argument("--list-profiles", action="store_true", help="list the AWS profiles found in ~/.aws and exit")
    parser.add_argument("--open", action="store_true", help="open the HTML report in your browser when done")
    parser.add_argument("--no-logs", action="store_true", help="skip pulling pod logs")
    parser.add_argument("--logs-all", action="store_true", help="also read logs of ALL running pods (capped), not just unhealthy / warning / core add-on pods")
    parser.add_argument("--log-namespaces", default="", help="with --logs-all: only these namespaces (comma separated)")
    parser.add_argument("--log-lines", type=int, default=None, help=f"max log lines per container (default {LOG_TAIL_LINES})")
    parser.add_argument("--traffic-sample", type=int, default=None,
                        help=f"seconds to sample live pod/node traffic from the kubelet (default {TRAFFIC_SAMPLE_SECONDS}, 0 = skip)")
    parser.add_argument("--no-aws", action="store_true", help="skip the AWS CLI section (kubectl data only)")
    parser.add_argument("--support-label", default=None, help=f"namespace label that names the team to contact (default {SUPPORT_LABEL})")
    args = parser.parse_args()

    if args.minutes:
        LOOKBACK_MINUTES = args.minutes
    if args.log_lines:
        LOG_TAIL_LINES = args.log_lines
    if args.support_label:
        SUPPORT_LABEL = args.support_label
    if args.traffic_sample is not None:
        TRAFFIC_SAMPLE_SECONDS = max(0, args.traffic_sample)
    AWS_OPTS.update(cluster=args.aws_cluster, region=args.region, profile=args.profile, enabled=not args.no_aws)
    if args.ekslogin:
        EKSLOGIN_EXE = args.ekslogin
    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass
    LOGIN_OPTS.update(method=args.login_method, device_code=args.device_code)

    if args.list_profiles:
        found = list_aws_profiles()
        cfg_path, cred_path = aws_config_paths()
        print(f"Profiles in {cfg_path} and {cred_path}:")
        for n, i in sorted(found.items()):
            print("  " + describe_profile(n, i))
        if not found:
            print("  (none)")
        return

    if args.list:
        clusters = list_selected_clusters()
        if not clusters:
            print("No clusters found (see the messages above)." if LOGIN_OPTS["method"] == "cli" else
                  "No clusters found (could not parse the ekslogin menu). Create clusters.json: {\"1\": \"name\", ...}")
        for k, v in sorted(clusters.items(), key=lambda kv: int(kv[0])):
            print(f"{k} - {v}")
        return

    if args.cluster or args.skip_login and args.no_gui:
        known = list_selected_clusters()
        selected = parse_cluster_selection(args.cluster or "0", known)
        if not selected:
            print("ERROR: no valid cluster in --cluster (use a number such as 3, 1,3,5, 2-4, all, or a cluster name from --list).", file=sys.stderr)
            sys.exit(1)
        if len(selected) == 1 and args.name:
            selected = [(selected[0][0], args.name)]
        results = run_clusters(
            selected, LOOKBACK_MINUTES, lambda l: print(l, flush=True), args.skip_login, args.context,
            progress=_cli_progress, options={"aws": not args.no_aws, "logs": not args.no_logs,
                                             "all_logs": args.logs_all, "log_namespaces": args.log_namespaces})
        print("")
        print("RESULT")
        for r in results["items"]:
            c = r["counts"]
            print(f"  {r['label']:<32} {r['status']:<18} CRIT {c['CRIT']} HIGH {c['HIGH']} MED {c['MED']} INFO {c['INFO']}"
                  + (f"   {r['html']}" if r["html"] else f"   {r['error'] or ''}"))
        if results["index"]:
            print(f"  Summary of all clusters: {results['index']}")
        target = results["index"] or next((r["html"] for r in results["items"] if r["html"]), None)
        if args.open and target:
            import pathlib
            import webbrowser
            webbrowser.open(pathlib.Path(target).as_uri())
        if any(r["status"] == "failed" for r in results["items"]):
            sys.exit(1)
        return

    run_gui(LOOKBACK_MINUTES, args.skip_login, args.context)


if __name__ == "__main__":
    main()

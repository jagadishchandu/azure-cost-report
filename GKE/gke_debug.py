#!/usr/bin/env python3
"""
GKE Debugger - log in to a GKE cluster with gkelogin.exe, then collect the basic debugging picture of what
happened (and what is happening) in the last N minutes, and save it as an interactive HTML report + a text report.

    python gke_debug.py                       # GUI: select one OR SEVERAL clusters in the list
    python gke_debug.py --minutes 60          # GUI, default window 60 minutes
    python gke_debug.py --cluster 3           # no GUI: log in to cluster #3 and collect
    python gke_debug.py --cluster 1,3,5       # several clusters, one after another (also: 2-4, or all)
    python gke_debug.py --list                # show the clusters gkelogin offers
    python gke_debug.py --list --all-clusters # EVERY cluster the signed-in gcloud user can access (name, project, location) + the menu entries
    python gke_debug.py --all-clusters --cluster my-gke     # select among every accessible cluster (number | 1,3,5 | 2-4 | all | name)
    python gke_debug.py --cluster 3 --skip-login            # already logged in: don't run gkelogin
    python gke_debug.py --cluster 3 --context my-ctx        # force a specific kubectl context
    python gke_debug.py --cluster 3 --project my-gcp-proj   # force a GCP project
    python gke_debug.py --list-projects                     # show the GCP projects `gcloud` can see
    python gke_debug.py --cluster 3 --workers 8             # collection tasks that run in parallel after the login (default 8; 1 = one after another)
    python gke_debug.py --cluster 3 --only-networking       # only the Network and traffic section (--no-networking = everything else)
    python gke_debug.py --cluster 3 --sections nodes,pods   # exactly these sections;  --skip-sections logs,timeline;  --list-sections shows the ids

After the login the collection runs in PARALLEL (sections and the independent reads inside them; at most 6 kubectl and 4 gcloud / API calls at the
same moment) and the report is merged in the fixed section order, so it is the same as with --workers 1. Every section has a tick box in the window
(and --sections / --skip-sections on the command line); an unticked section is never collected and shows as one 'Skipped by choice' line.

Everything is READ-ONLY (kubectl get / top / logs, gcloud list / describe / get-*, Cloud Logging and Cloud
Monitoring reads).

What is collected
    * Cluster: context, versions, API server readiness
    * GCP (gcloud): cluster status, node pools, upgrades, network (subnet + pod / service ranges and their IP
      capacity, firewall rules, routes, Cloud NAT), node service account + IAM roles, add-ons, node VM and managed
      instance group health, GKE operations, control-plane / autoscaler / audit logs from Cloud Logging
    * Nodes: name + the actual VM (Compute Engine instance, zone, machine type, spot / preemptible, pool), status,
      CPU / memory / disk, resource utilization dashboard by namespace, pods on each node, namespaces (pods used vs
      configured, quotas)
    * Network & traffic (written in full words, with a glossary before each block, a status OK / Warning / Problem /
      Not available for every check and a 10-row traffic issue checklist): pod networking (CNI plugin health, IP address
      exhaustion, CNI logs, stuck pods), node networking (conditions, interface errors, MTU hints, cloud API throttling),
      kube-proxy and service routing, DNS (pods, logs, NodeLocal DNSCache, ndots), network policies and firewalls, load
      balancers / ingress / TLS certificates, conntrack and Cloud NAT port exhaustion, observability and packet-capture
      guidance, API server throttling / admission webhooks / etcd, and the TRAFFIC of the selected window from Cloud
      Monitoring (node bytes received / sent, load balancer requests / server errors / latency, Cloud NAT drops)
    * Who to contact: the namespace label elvh-app-support-dl next to every namespace + a Teams-to-contact list
    * Pods, events, workloads, autoscaling / storage, top consumers, pod logs (unhealthy / warning / core add-ons / all)
    * A timeline of everything that happened in the window, and a health summary

Login methods (--login-method)
    exe (default)  the custom gkelogin.exe wrapper, as before.
    cli            the standard Google Cloud CLI (`gcloud`): signs in if needed (no-browser flow with --device-code), lists the
                   clusters, and writes the kubeconfig entry for each selected cluster.
    --all-clusters (either method) list every cluster you can access (all projects); with the exe method a cluster that is not in
                   the gkelogin menu is logged in with `gcloud container clusters get-credentials`. Everything after the login
                   (context, cloud details, report) is the same. See the README, section "Login methods".
    e.g.  python gke_debug.py --login-method cli --project my-proj --cluster my-gke

Requirements: Python 3.9+, kubectl on PATH, gkelogin.exe, and the Google Cloud CLI (`gcloud`, logged in) for the GCP parts.
"""

import argparse
import contextlib
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Global settings
# ---------------------------------------------------------------------------

LOOKBACK_MINUTES = 30        # <-- the time window. Override with --minutes (or the GUI box)

MAX_LOG_PODS = 20            # unhealthy pods + pods with Warning events whose logs are pulled
MAX_CORE_LOG_PODS = 12       # core add-on pods (kube-dns, netd / anetd, gke-metadata-server, CSI ...) whose logs are pulled
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
LOGIN_TIMEOUT = 120          # seconds for gkelogin.exe
REPORT_DIR = "reports"

# --- parallel collection (after the login) -----------------------------------------------------------------
PARALLEL_WORKERS = 8         # collection tasks that run at the same time (--workers N, GUI untouched). 1 = one after another, exactly as before
KUBECTL_CONCURRENCY = 6      # never more than this many kubectl calls at the same moment
GCLOUD_CONCURRENCY = 4       # ... and this many gcloud / Google API (Cloud Monitoring, Cloud Logging) calls

# --- branding: the window and the HTML report are drawn from these (change them to re-skin the tool) -------
CLOUD_NAME = "Google Cloud"
PRODUCT_TITLE = "Google Kubernetes Engine (GKE) Debugger"
BRAND_PRIMARY = "#4285F4"    # Google blue: buttons, links, focus, headers
BRAND_ACCENT = "#34A853"     # Google green: success, 'collected', the Run button
BRAND_RED, BRAND_YELLOW = "#EA4335", "#FBBC05"
BRAND_COLORS = (BRAND_PRIMARY, BRAND_RED, BRAND_YELLOW, BRAND_ACCENT)   # the four Google colours, in logo order
BRAND_DARK = "#174EA6"       # titles on light backgrounds
BRAND_SOFT = "#E8F0FE"       # soft blue panels, decorative clouds
BRAND_HEADER_BG = "#F8FAFF"

# The logo is a stylised cloud made of four coloured arcs with the Kubernetes helm (ship's wheel) inside. It is drawn from
# primitives only (no image files, no copied artwork): arcs on a 100 x 64 box -> (centre x, centre y, radius, start deg, extent deg, colour no.)
_LOGO_ARCS = [(27, 42, 13, 105, 165, 0), (46, 28, 19, 20, 135, 1), (68, 38, 16, -95, 175, 2)]
_LOGO_BASE = (27, 55, 68, 54)          # the cloud's flat bottom line: x1, y1, x2, y2 (colour no. 3)
_LOGO_HELM = (47, 37, 9)               # helm centre x, y and radius


def _arc_point(cx, cy, r, deg):
    import math
    return cx + r * math.cos(math.radians(deg)), cy - r * math.sin(math.radians(deg))


def draw_logo(canvas, x=0, y=0, size=96, bg="#FFFFFF", tag="logo"):
    """Draw the cloud + helm logo on a tkinter Canvas with its top-left corner at (x, y); `size` is the width in pixels."""
    import math
    k = size / 100.0
    P = lambda px, py: (x + px * k, y + py * k)
    lw = max(3, round(7 * k))
    # soft white body so the logo also reads on a coloured band
    for cx, cy, r, *_ in _LOGO_ARCS:
        a, b = P(cx - r, cy - r), P(cx + r, cy + r)
        canvas.create_oval(a[0], a[1], b[0], b[1], fill=bg, outline=bg, tags=tag)
    a, b = P(_LOGO_BASE[0], _LOGO_ARCS[0][1]), P(_LOGO_BASE[2], _LOGO_BASE[3])
    canvas.create_rectangle(a[0], a[1], b[0], b[1], fill=bg, outline=bg, tags=tag)
    for cx, cy, r, start, extent, col in _LOGO_ARCS:
        a, b = P(cx - r, cy - r), P(cx + r, cy + r)
        canvas.create_arc(a[0], a[1], b[0], b[1], start=start, extent=extent, style="arc", outline=BRAND_COLORS[col], width=lw, tags=tag)
        for deg in (start, start + extent):                          # round caps
            px, py = P(*_arc_point(cx, cy, r, deg))
            canvas.create_oval(px - lw / 2, py - lw / 2, px + lw / 2, py + lw / 2, fill=BRAND_COLORS[col], outline=BRAND_COLORS[col], tags=tag)
    p1, p2 = P(_LOGO_BASE[0], _LOGO_BASE[1]), P(_LOGO_BASE[2], _LOGO_BASE[3])
    canvas.create_line(p1[0], p1[1], p2[0], p2[1], fill=BRAND_COLORS[3], width=lw, tags=tag)
    for px, py in (p1, p2):
        canvas.create_oval(px - lw / 2, py - lw / 2, px + lw / 2, py + lw / 2, fill=BRAND_COLORS[3], outline=BRAND_COLORS[3], tags=tag)
    # the Kubernetes helm: a ring, a hub and seven spokes with a knob on each
    hx, hy, hr = _LOGO_HELM
    c = P(hx, hy)
    ring = hr * k
    canvas.create_oval(c[0] - ring, c[1] - ring, c[0] + ring, c[1] + ring, outline=BRAND_DARK, width=max(1.5, 1.8 * k), tags=tag)
    canvas.create_oval(c[0] - ring * .28, c[1] - ring * .28, c[0] + ring * .28, c[1] + ring * .28, fill=BRAND_DARK, outline=BRAND_DARK, tags=tag)
    for i in range(7):
        ang = math.radians(90 + i * 360 / 7)
        e = (c[0] + math.cos(ang) * ring * 1.45, c[1] - math.sin(ang) * ring * 1.45)
        canvas.create_line(c[0], c[1], e[0], e[1], fill=BRAND_DARK, width=max(1.5, 1.8 * k), tags=tag)
        canvas.create_oval(e[0] - ring * .17, e[1] - ring * .17, e[0] + ring * .17, e[1] + ring * .17, fill=BRAND_DARK, outline=BRAND_DARK, tags=tag)


def logo_svg(width=64):
    """The same logo as an inline SVG string (for the HTML report). Nothing is loaded from the internet."""
    import math
    parts = []

    def pt(cx, cy, r, deg):
        px, py = _arc_point(cx, cy, r, deg)
        return f"{px:.2f} {py:.2f}"
    parts.append('<rect x="27" y="40" width="41" height="15" fill="#fff"/>')
    for cx, cy, r, *_ in _LOGO_ARCS:
        parts.append(f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="#fff"/>')
    for cx, cy, r, start, extent, col in _LOGO_ARCS:
        parts.append(f'<path d="M {pt(cx, cy, r, start)} A {r} {r} 0 {1 if extent > 180 else 0} 0 {pt(cx, cy, r, start + extent)}" fill="none" '
                     f'stroke="{BRAND_COLORS[col]}" stroke-width="6" stroke-linecap="round"/>')
    parts.append(f'<line x1="{_LOGO_BASE[0]}" y1="{_LOGO_BASE[1]}" x2="{_LOGO_BASE[2]}" y2="{_LOGO_BASE[3]}" stroke="{BRAND_COLORS[3]}" stroke-width="6" stroke-linecap="round"/>')
    hx, hy, hr = _LOGO_HELM
    parts.append(f'<circle cx="{hx}" cy="{hy}" r="{hr}" fill="none" stroke="{BRAND_DARK}" stroke-width="1.8"/><circle cx="{hx}" cy="{hy}" r="{hr * .28:.2f}" fill="{BRAND_DARK}"/>')
    for i in range(7):
        ang = math.radians(90 + i * 360 / 7)
        ex, ey = hx + math.cos(ang) * hr * 1.45, hy - math.sin(ang) * hr * 1.45
        parts.append(f'<line x1="{hx}" y1="{hy}" x2="{ex:.2f}" y2="{ey:.2f}" stroke="{BRAND_DARK}" stroke-width="1.8"/><circle cx="{ex:.2f}" cy="{ey:.2f}" r="{hr * .17:.2f}" fill="{BRAND_DARK}"/>')
    return (f'<svg class="logo" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 64" width="{width}" height="{round(width * .64)}" role="img" '
            f'aria-label="{CLOUD_NAME} cloud with the Kubernetes helm">' + "".join(parts) + "</svg>")


# Symbols for the window and the report. Each has a plain fallback for a Tk build that cannot draw characters above U+FFFF.
SYMBOL_FALLBACK = {"\U0001F5A5": "▣", "\U0001F310": "◍", "\U0001F4C4": "▤", "\U0001F504": "↻", "\U0001F510": "⚿", "\U0001F50D": "⌕",
                   "\U0001F534": "●", "\U0001F7E0": "●", "\U0001F7E1": "●", "\U0001F535": "●", "\U0001F4CA": "▥",
                   "\U0001F4E6": "▤", "\U0001F5C2": "▦", "\U0001F514": "♪", "\U0001F680": "▲", "\U0001F3C6": "★", "\U0001F552": "◷",
                   "\U0001F4C8": "▲", "\U0001F6E0": "⚙"}
SEVERITY_DOT = {"CRIT": "\U0001F534", "HIGH": "\U0001F7E0", "MED": "\U0001F7E1", "INFO": "\U0001F535"}

# --- the report sections: ONE registry drives the scheduler, the report, the table of contents, the health summary and the window ---
# id: what --sections / --skip-sections accept; step: the progress key; num: the number in the report; needs: [(section id, parts or None)] fetched
# silently (collected but not shown) when this section is ticked and that one is not; after: only an ORDER (when both run, that one goes first - this
# section labels nodes with the virtual machine that section reads); group: 'networking' is what --only-networking / --no-networking switch.
SECTIONS = [
    {"id": "overview", "step": "overview", "num": 1, "title": "Cluster overview", "icon": "⎈", "needs": [], "weight": 1,
     "desc": "kubectl context, client and server versions, API server readiness."},
    {"id": "gcp", "step": "gcp", "num": 2, "title": "Google Kubernetes Engine cluster and infrastructure", "icon": "☁", "needs": [], "weight": 9,
     "desc": "Cloud side from gcloud: cluster status, node pools, subnet and firewall, node identity, add-ons, operations, control-plane logs."},
    {"id": "nodes", "step": "nodes", "num": 3, "title": "Nodes", "icon": "\U0001F5A5", "needs": [], "after": ["gcp"], "weight": 3,
     "desc": "Node status, processor, memory, disk and swap, and the virtual machine behind every node."},
    {"id": "utilization", "step": "utilization", "num": 4, "title": "Resource utilization", "icon": "\U0001F4CA", "needs": [], "after": ["gcp"], "weight": 3,
     "desc": "Processor and memory use by namespace and pod against requests and limits (interactive dashboard)."},
    {"id": "nodepods", "step": "nodepods", "num": 5, "title": "Pods on each node", "icon": "\U0001F4E6", "needs": [], "after": ["gcp"], "weight": 3,
     "desc": "Which pods run on which node, with their processor, memory and disk use."},
    {"id": "namespaces", "step": "namespaces", "num": 6, "title": "Namespaces", "icon": "\U0001F5C2", "needs": [], "weight": 3,
     "desc": "Pods used versus configured, quotas and the support team of every namespace."},
    {"id": "pods", "step": "pods", "num": 7, "title": "Unhealthy pods", "icon": "⚠", "needs": [], "after": ["gcp"], "weight": 3,
     "desc": "Crash loops, pods that are not ready, image pull errors, restarts."},
    {"id": "events", "step": "events", "num": 8, "title": "Events", "icon": "\U0001F514", "needs": [], "after": ["gcp"], "weight": 2,
     "desc": "Warning events of the window, grouped by reason."},
    {"id": "workloads", "step": "workloads", "num": 9, "title": "Workloads", "icon": "⚙", "needs": [], "weight": 2,
     "desc": "Deployments, stateful sets, daemon sets and jobs that are not at their desired state."},
    {"id": "networking", "step": "netdetail", "num": 10, "title": "Network and traffic", "icon": "\U0001F310", "needs": [("gcp", ("network", "vm"))], "weight": 10,
     "group": "networking", "desc": "Dataplane, DNS, services, ingress, firewalls, load balancer health, Cloud NAT and the traffic of the window."},
    {"id": "scaling", "step": "network", "num": 11, "title": "Autoscaling, storage and services", "icon": "\U0001F504", "needs": [], "weight": 1,
     "desc": "Autoscalers at their limit, volumes not bound, services without endpoints."},
    {"id": "top", "step": "top", "num": 12, "title": "Top resource consumers", "icon": "\U0001F3C6", "needs": [], "weight": 1,
     "desc": "The ten pods using the most processor and memory right now."},
    {"id": "logs", "step": "logs", "num": 13, "title": "Pod logs", "icon": "\U0001F4C4", "needs": [("pods", None)], "weight": 8,
     "desc": "Recent logs of unhealthy pods, pods with warnings and core add-ons."},
    {"id": "timeline", "step": "timeline", "num": 14, "title": "Timeline", "icon": "\U0001F552", "needs": [], "weight": 1,
     "desc": "Everything that happened in the window, oldest first."},
]
SECTION_BY_ID = {s["id"]: s for s in SECTIONS}
SECTION_BY_STEP = {s["step"]: s for s in SECTIONS}

# What every section shows, in plain language (shown in a box under the section heading in the HTML report and as one line in the text report).
# {m} = the length of the time window in minutes. 'how' = a short hint on how to use the section.
_TABLE_HINT = "Click a column header to sort a table, type in the filter box above a table to narrow its rows, and use the CSV button to export it."
_SECTION_TEXT = {
    "overview": ("The cluster connection this report was made with: the kubectl context, the client and server versions of Kubernetes and whether the Kubernetes API server reports itself ready. "
                 "It is read with kubectl at the moment the report is generated (no time window). A line that says NOT REACHABLE or FAILING is shown in orange and means the cluster could not be read.",
                 "If the server version is NOT REACHABLE, fix the connection or the login first; every other section depends on it."),
    "gcp": ("The Google Cloud side of the cluster: cluster status and version, node pools, subnet and address ranges, firewall rules, Cloud NAT, the node service account and its roles, add-ons, "
            "the Compute Engine virtual machines behind the nodes, cluster operations and error logs. It is read with gcloud and the Cloud Monitoring and Cloud Logging services (read only); "
            "operations and logs cover the last {m} minutes, everything else is the configuration right now. States such as ERROR, DEGRADED, NOT STABLE or TOO BROAD are problems; 'ok' means nothing was found.",
            "Start with the cluster and node pool tables; then read the notes column of each table. " + _TABLE_HINT),
    "nodes": ("One row per worker node: whether it is Ready, how much processor and memory it uses against what it can offer, its disk and swap, its age and version, and the Compute Engine virtual machine behind it. "
              "The data comes from kubectl (node objects and live usage from the kubelet or metrics-server) as it is now, and from gcloud for the virtual machine. "
              "Red or orange numbers are close to the limit (75 percent and above is orange, 90 percent and above is red).",
              "Look for nodes that are not Ready or whose usage is orange or red. " + _TABLE_HINT),
    "utilization": ("How much processor and memory each namespace and pod uses compared with what it requested and is allowed to use, and how full the cluster is. "
                    "It is calculated from kubectl pod specifications and the live usage now (kubelet or metrics-server); it is a snapshot, not a time window. "
                    "Bars are green below 75 percent, amber from 75 percent and red from 90 percent of the limit or of the allocatable capacity.",
                    "Use the buttons and the filter of the dashboard to switch between processor and memory, and to rank namespaces or pods. " + _TABLE_HINT),
    "nodepods": ("Which pods run on which node, with their status, restarts, processor, memory and disk use, requests and limits. "
                 "It comes from kubectl (pods, and live usage from the kubelet) as it is now. Orange and red values are close to the pod's limit.",
                 "Find a node in the list, then look for pods with many restarts or usage close to the limit. " + _TABLE_HINT),
    "namespaces": ("Every namespace with its number of pods used compared with the number configured, its resource usage and quotas, and which team supports it. "
                   "It comes from kubectl (namespaces, pods, workloads and resource quotas) as it is now; the support team is read from the namespace label shown in the table. "
                   "A status other than OK means pods are missing, pending or failed, or a quota is almost used up.",
                   "Use the support distribution list column to find who to contact. " + _TABLE_HINT),
    "pods": ("The pods that are not healthy: crash loops, not ready, waiting for an image, pending or failed, and pods that restarted. "
             "It comes from kubectl pod status as it is now. The column 'why' gives the most likely reason in plain words.",
             "Work from the top: the pods are listed with the most serious first. " + _TABLE_HINT),
    "events": ("The Kubernetes Warning events of the last {m} minutes, first counted by reason and then listed one by one, plus notable normal events such as scaling and node changes. "
               "It comes from kubectl events; Kubernetes keeps events for only about one hour. Many occurrences of the same reason point to a repeating problem.",
               "Read the reason table first, then the latest events for the object name. " + _TABLE_HINT),
    "workloads": ("Deployments, stateful sets, daemon sets and jobs that are not at their desired state, recent rollouts of the last {m} minutes, failed jobs and the core add-ons in the kube-system namespace. "
                  "It comes from kubectl as it is now. 'Ready' compares running pods with the desired number.",
                  "A workload whose ready count is lower than desired needs attention. " + _TABLE_HINT),
    "networking": ("A complete picture of the cluster network: dataplane and address ranges, pod networking, services and their endpoints, cluster name resolution (DNS), network policies and firewalls, load balancers and ingress, "
                   "certificates, connection tracking and port exhaustion, API server throttling, and the traffic of the last {m} minutes. It is read with kubectl and gcloud, and traffic numbers come from Cloud Monitoring and the kubelet. "
                   "Each check ends in a status: OK (healthy), Warning (worth a look), Problem (a likely cause of trouble) or Not available (could not be checked; never a pass).",
                   "Start with the traffic issue checklist near the end of this section. " + _TABLE_HINT),
    "scaling": ("Autoscalers that are at their limit, storage claims and volumes that are not bound, and services without ready endpoints or without an address. "
                "It comes from kubectl as it is now. Anything listed here is a problem or a likely cause of one; an empty result is shown as one sentence saying so.",
                "Check the horizontal pod autoscaler table for workloads that cannot scale, then storage and services. " + _TABLE_HINT),
    "top": ("The ten pods that use the most processor and the ten that use the most memory right now. "
            "It comes from the live usage of kubectl top or the kubelet; it is a snapshot, not an average over the window.",
            "Compare the usage with the limit column to see which pods may be throttled or killed. " + _TABLE_HINT),
    "logs": ("Recent log lines (last {m} minutes) of unhealthy pods, pods with Warning events and core add-on pods, with an overview table of how many error-like lines each log has. "
             "The lines are read with kubectl logs; they may contain sensitive data. Lines that look like errors are shown in red, warnings in orange.",
             "Open a log, tick 'errors only' to hide everything else, and use Copy to take it with you. " + _TABLE_HINT),
    "timeline": ("Everything notable that happened in the last {m} minutes in one list, oldest first: events, restarts, scaling, node changes, operations and control plane errors. "
                 "It is merged from the other sections, so a section that was not collected adds nothing here.",
                 "Use the chips above the list to show or hide a kind of entry."),
}
for _s in SECTIONS:
    _s["shows"], _s["how"] = _SECTION_TEXT[_s["id"]]


def section_text(sid, minutes):
    """(what this section shows, how to use it) for a registry id, with the window filled in."""
    sec = SECTION_BY_ID[sid]
    return sec["shows"].replace("{m}", str(minutes)), sec["how"].replace("{m}", str(minutes))

SECTION_ALIASES = {"gke": "gcp", "google": "gcp", "cloud": "gcp", "network": "networking", "net": "networking", "netdetail": "networking",
                   "traffic": "networking", "autoscaling": "scaling", "storage": "scaling", "log": "logs", "pod": "pods", "node": "nodes",
                   "namespace": "namespaces", "event": "events", "workload": "workloads", "util": "utilization", "usage": "utilization"}
NETWORKING_IDS = [s["id"] for s in SECTIONS if s.get("group") == "networking"]
# the order the collection steps run in (the data step sits second because every other step reads what it fetched)
STEP_ORDER = ["overview", "data", "gcp", "nodes", "utilization", "nodepods", "namespaces", "pods", "events", "workloads", "netdetail", "network", "top", "logs", "timeline"]
USAGE_SECTIONS = {"nodes", "utilization", "nodepods", "namespaces", "top", "networking"}   # sections that read the live kubelet / metrics-server usage


def section_id(word):
    """'Networking' / 'netdetail' / 'gke' -> the registry id, or None."""
    w = str(word or "").strip().lower().replace(" ", "-").replace("_", "-")
    if w in SECTION_BY_ID:
        return w
    return SECTION_ALIASES.get(w) or next((s["id"] for s in SECTIONS if w in (s["step"], s["title"].lower().replace(" ", "-"))), None)


def parse_section_list(text):
    """'nodes,pods,networking' -> ['nodes', 'pods', 'networking'] (registry ids; unknown names raise ValueError with the valid ones)."""
    out, bad = [], []
    for word in re.split(r"[,\s;]+", str(text or "").strip()):
        if not word:
            continue
        sid = section_id(word)
        (out if sid else bad).append(sid or word)
    if bad:
        raise ValueError("unknown section(s): " + ", ".join(bad) + ". Valid ids: " + ", ".join(s["id"] for s in SECTIONS))
    return list(dict.fromkeys(out))


def resolve_sections(options=None):
    """Which sections run. -> {"selected": [ids in report order], "skipped": [ids], "silent": {id: parts or None}, "timeline_all": bool}.
    Unticked sections are never collected. A ticked section that needs the data of an unticked one gets that data collected silently
    ('silent': the unticked section runs only the parts needed, its text and findings are dropped)."""
    o = options or {}
    chosen = o.get("sections")
    skip = {section_id(x) or x for x in (o.get("skip_sections") or [])}
    wanted = None if chosen is None else {section_id(x) or x for x in chosen}
    selected = [s["id"] for s in SECTIONS if (wanted is None or s["id"] in wanted) and s["id"] not in skip]
    if o.get("logs") is False and "logs" in selected:                     # the older switches are aliases of unticking the section
        selected.remove("logs")
    if o.get("gcp") is False and "gcp" in selected:
        selected.remove("gcp")
    silent = {}
    gcp_hard_off = o.get("gcp") is False
    for sid in selected:
        for need, parts in SECTION_BY_ID[sid]["needs"]:
            if need in selected or (need == "gcp" and gcp_hard_off):
                continue
            if need in silent and silent[need] is None:
                continue
            silent[need] = None if parts is None else tuple(sorted(set(silent.get(need) or ()) | set(parts)))
    return {"selected": selected, "skipped": [s["id"] for s in SECTIONS if s["id"] not in selected], "silent": silent}

# Optional fixed cluster list {"1": "my-cluster-a", "2": "my-cluster-b"}. If empty, the
# list is read from clusters.json (same format) next to this script, otherwise it is
# parsed from the menu that gkelogin.exe prints.
CLUSTERS = {}

_HERE = os.path.dirname(os.path.abspath(__file__))

ERROR_PATTERN = re.compile(r"error|exception|panic|fatal|fail|oom|refused|timeout|timed out|denied|unable|cannot|traceback", re.I)
ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
WAITING_OK = {"PodInitializing", "ContainerCreating"}
NOTABLE_NORMAL_REASONS = {
    "Killing", "Preempted", "Preempting", "Evicted", "NodeNotReady", "NodeReady", "RegisteredNode",
    "RemovingNode", "DeletingNode", "ScalingReplicaSet", "SuccessfulRescale", "TriggeredScaleUp",
    "ScaleDown", "Drain", "NodeNotSchedulable", "NodeSchedulable",
}


GKELOGIN_EXE = os.path.join(_HERE, "gkelogin.exe") if os.path.isfile(os.path.join(_HERE, "gkelogin.exe")) else ".\\gkelogin.exe"


# ---------------------------------------------------------------------------
# Login (gkelogin: pipes the cluster number to gkelogin.exe)
# ---------------------------------------------------------------------------

def gkelogin(cluster_number):
    # gkelogin.exe prints a menu and waits on stdin for the cluster number(s).
    proc = subprocess.run(
        [GKELOGIN_EXE],
        input=f"{cluster_number}\n",
        capture_output=True,
        text=True,
        shell=True,
    )
    if proc.returncode != 0:
        print(f"[cluster {cluster_number}] gkelogin failed: {proc.stderr.strip()}")
        print(proc.stdout[-1000:])
        return False
    time.sleep(2)  # brief buffer for kubeconfig/context to settle
    return True


def exe_menu_clusters() -> dict:
    """{'1': 'cluster-name', ...} offered by the custom login (gkelogin). Order of preference: the CLUSTERS dict, clusters.json next to
    this script, then the menu that gkelogin.exe prints when it is given no selection (stdin closed)."""
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
        proc = subprocess.run([GKELOGIN_EXE], input="", capture_output=True, text=True,
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


def list_clusters(emit=None) -> dict:
    """{'1': 'cluster-name', ...} for the cluster list.
    - login method cli: every cluster `gcloud` can reach (see list_clusters_cli);
    - login method exe + --all-clusters: every cluster `gcloud` can reach, and the ones that are in the gkelogin menu are
      logged in with gkelogin as before (the others with `gcloud container clusters get-credentials`);
    - otherwise: only the gkelogin menu (CLUSTERS dict, clusters.json, or the menu gkelogin.exe prints)."""
    out = emit or print
    if LOGIN_OPTS["method"] == "cli":
        return list_clusters_cli(out)
    menu = exe_menu_clusters()
    if LOGIN_OPTS.get("all_clusters"):
        return list_clusters_cli(out, menu=menu)
    CLI_TARGETS.clear()
    return menu


# ---------------------------------------------------------------------------
# Login method: the standard Google Cloud CLI (`gcloud`) instead of gkelogin.exe  (--login-method cli)
# ---------------------------------------------------------------------------

SIGNIN_METHOD_LABELS = {"manual": "I run the command myself (recommended)", "captured": "Show URL here and paste the code (captured)",
                        "console": "Open a console window for me"}      # the sign-in method selector (step 2) / --signin-method values
SIGNIN_KEYS = {v: k for k, v in SIGNIN_METHOD_LABELS.items()}
_SESSION = {}      # choices remembered while the program runs (the sign-in method)


def default_signin_method():
    """manual unless the environment says otherwise (GKE_DEBUG_SIGNIN_METHOD=captured|console|manual; used by automated tests)."""
    v = (os.environ.get("GKE_DEBUG_SIGNIN_METHOD") or "").strip().lower()
    return v if v in SIGNIN_METHOD_LABELS else "manual"


LOGIN_OPTS = {"method": "exe", "device_code": True, "gui": False, "all_clusters": False, "account": None,
              "signin": default_signin_method()}   # method: "exe" (gkelogin.exe, default) or "cli" (gcloud); all_clusters: list every cluster gcloud can see; signin: manual (default) | captured | console
LOGIN_LABELS = {"exe": "Custom login (gkelogin)", "cli": "Cloud CLI (gcloud)"}   # the GUI combobox values
CLI_TARGETS = {}    # cluster number (str) -> what the CLI listing found; fed into GCP_OPTS after the login


def _run_interactive(cmd, emit):
    """Run an interactive login command (browser / device-code flow). Its output is NOT captured: from the
    command line it uses this console; from the GUI (no console) it gets its own console window on Windows.
    Waits until it finishes. Returns the exit code, or None if it could not be started."""
    emit("Running: " + " ".join([os.path.splitext(os.path.basename(cmd[0]))[0], *cmd[1:]]))
    why = gcloud_violation(cmd[1:], local_ok=True) if os.path.splitext(os.path.basename(str(cmd[0])))[0].lower() == "gcloud" else "'%s' is not allowed" % cmd[0]
    if why:                         # only the user's own sign-in (local-only) or a read command
        emit("ERROR: " + guard_block("login", cmd[1:], why))
        return None
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
    """Run a non-interactive command. Returns (ok, first line of its output or error). Only gcloud read commands, or `container clusters get-credentials`
    (LOCAL-ONLY: it writes this machine's kubeconfig, never the cluster)."""
    why = gcloud_violation(cmd[1:], local_ok=True) if os.path.splitext(os.path.basename(str(cmd[0])))[0].lower() == "gcloud" else "'%s' is not allowed" % cmd[0]
    if why:
        return False, guard_block("login", cmd[1:], why)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, env=_gcloud_env())
    except subprocess.TimeoutExpired:
        return False, f"timed out after {timeout}s"
    except Exception as exc:
        return False, str(exc)
    text = (proc.stdout if proc.returncode == 0 else (proc.stderr or proc.stdout)) or ""
    return proc.returncode == 0, _first_line(text, 200) if text.strip() else ""


def list_selected_clusters(emit=print):
    """The cluster list of the chosen login method (gkelogin.exe menu / clusters.json, or the gcloud CLI / --all-clusters)."""
    return list_clusters(emit) if (LOGIN_OPTS["method"] == "cli" or LOGIN_OPTS.get("all_clusters")) else list_clusters()


def gcloud_accounts():
    """([{account, active}], error_or_None): every account gcloud holds credentials for (`gcloud auth list`, read-only)."""
    data, err = gcloud(["auth", "list"], None, 30, project=False)
    if err or not isinstance(data, list):
        return [], (err or "unexpected output from gcloud auth list")
    rows = [{"account": str(d.get("account")), "active": str(d.get("status") or "").upper() == "ACTIVE"}
            for d in data if isinstance(d, dict) and d.get("account")]
    return rows, None


def _gcloud_active_account():
    """(account, error): the account this tool uses - the pinned one (LOGIN_OPTS['account'], when gcloud holds credentials for it),
    otherwise gcloud's ACTIVE account."""
    rows, err = gcloud_accounts()
    if err:
        return None, err
    pin = LOGIN_OPTS.get("account")
    if pin:
        if any(r["account"] == pin for r in rows):
            return pin, None
        return None, f"gcloud has no credentials for {pin} - sign in with that account first"
    for r in rows:
        if r["active"]:
            return r["account"], None
    return None, "gcloud has no active account"


# ---- credential state: which accounts are usable / expired (read-only: `gcloud auth print-access-token --account X`, the token is thrown away)
EXPIRED_PATTERN = re.compile(r"reauthentication (?:required|failed)|invalid_grant|problem refreshing your current auth tokens|"
                             r"do not currently have an active account|token has been expired or revoked|gcloud auth login to obtain new credentials|"
                             r"credentials? (?:have |has |are |is )?(?:expired|revoked)|invalid authentication credentials|"
                             r"unauthenticated|expected oauth 2 access token", re.I)
_TOKEN_LIKE = re.compile(r"\b(?:ya29\.|1//)[A-Za-z0-9_\-.]{8,}|\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-.]{5,}")
AUTH_STATE = {"expired": set(), "status": {}, "last": None}        # expired accounts, {account: (state, time)}, the account last seen in use
EXPIRY_EVENTS = queue.Queue()                                        # (account, reason) - the window reads it and shows the banner
CREDS_EXPIRED_TEXT = "credentials expired - sign in again"


def mask_tokens(text):
    """Hides anything shaped like an access / refresh / id token and the verification code in text that may be shown."""
    return mask_secrets(_TOKEN_LIKE.sub("[token hidden]", str(text)))


def is_expired_error(text):
    return bool(EXPIRED_PATTERN.search(str(text or "")))


def mark_expired(account, reason=""):
    """Remember that `account` (None = the one in use) needs a new sign-in and tell the window."""
    acct = account or LOGIN_OPTS.get("account") or AUTH_STATE.get("last")
    if not acct:
        return None
    new = acct not in AUTH_STATE["expired"]
    AUTH_STATE["expired"].add(acct)
    AUTH_STATE["status"][acct] = ("expired", time.time())
    if new:
        EXPIRY_EVENTS.put((acct, mask_tokens(_first_line(reason, 160)) if reason else ""))
    return acct


def account_status(account, rows=None):
    """Read-only status of one account: {"state": "active" | "expired" | "signed_out" | "unknown", "detail"}. `gcloud auth print-access-token
    --account X` working means usable; the token is discarded at once (never stored, printed or logged)."""
    if rows is not None and not any(r["account"] == account for r in rows):
        return {"state": "signed_out", "detail": "Not signed in with gcloud"}
    out, err = gcloud(["auth", "print-access-token", "--account", account], None, 60, project=False, fmt=None)
    ok = not err and isinstance(out, str) and bool(out.strip())
    out = None                                                         # the token is not kept
    if ok:
        AUTH_STATE["expired"].discard(account)
        AUTH_STATE["status"][account] = ("active", time.time())
        return {"state": "active", "detail": "Active"}
    if err and is_expired_error(err):
        mark_expired(account, err)
        return {"state": "expired", "detail": "Credentials expired - sign in again"}
    return {"state": "unknown", "detail": mask_tokens(_first_line(err or "no token returned", 120))}


def check_accounts(accounts, rows=None, on_result=None, cap=4):
    """Status of several accounts, at most `cap` gcloud calls at once. Returns {account: status}."""
    from concurrent.futures import ThreadPoolExecutor
    results = {}

    def one(a):
        try:
            res = account_status(a, rows)
        except Exception as exc:
            res = {"state": "unknown", "detail": mask_tokens(str(exc))[:120]}
        results[a] = res
        if on_result:
            on_result(a, res)
    with ThreadPoolExecutor(max_workers=max(1, min(cap, 4))) as ex:
        list(ex.map(one, list(accounts)))
    return results


STATUS_TEXT = {"active": "Active", "expired": "Credentials expired - sign in again", "signed_out": "Not signed in", "unknown": "Unknown"}


def print_accounts(emit=print):
    """--list-accounts: every account gcloud knows, which one is active, and whether its credentials still work."""
    rows, err = gcloud_accounts()
    if err:
        emit("Could not read the account list: " + mask_tokens(_first_line(err, 160)))
        return False
    if not rows:
        emit("gcloud has no signed-in account. Run this tool with --login-method cli to sign in (device code), or: gcloud auth login")
        return True
    res = check_accounts([r["account"] for r in rows], rows)
    emit("Accounts known to gcloud (read-only check; '*' = active in gcloud, '>' = pinned with --account):")
    for r in rows:
        st = res.get(r["account"]) or {"state": "unknown", "detail": ""}
        mark = ">" if LOGIN_OPTS.get("account") == r["account"] else ("*" if r["active"] else " ")
        kind = " (service account)" if r["account"].endswith(".gserviceaccount.com") else ""
        why = "" if st["state"] == "active" else (" - " + st["detail"] if st["detail"] and st["state"] == "unknown" else "")
        emit(f"  {mark} {r['account']}{kind}   [{STATUS_TEXT[st['state']]}]{why}")
        if st["state"] == "expired":
            emit(f"      Credentials for {r['account']} expired. Sign in again: run with --login-method cli --account {r['account']} (or: gcloud auth login --no-launch-browser --account {r['account']}).")
    return True


def pin_account(account):
    """Use `account` for everything this tool runs from now on (None = follow gcloud's active account). Nothing is written to gcloud's
    configuration: the account is added as `--account <email>` to every gcloud call (and CLOUDSDK_CORE_ACCOUNT for kubectl's auth plugin)."""
    LOGIN_OPTS["account"] = account or None
    AUTH_STATE["last"] = account or AUTH_STATE.get("last")
    with _TOKEN_LOCK:
        _TOKEN["value"], _TOKEN["at"], _TOKEN["account"] = None, 0.0, None


def login_status(account=None):
    """Read-only sign-in check for the window (`gcloud auth list`; nothing is changed, no sign-in is started).
    Returns {"state": "ok" | "not_signed_in" | "no_cli", "who", "detail", "hint", "accounts", "active"}."""
    if not shutil.which("gcloud"):
        return {"state": "no_cli", "who": None, "detail": "The Google Cloud CLI (gcloud) is not installed (it was not found on PATH).",
                "hint": "Install it from https://cloud.google.com/sdk/docs/install, then press 'Check status'.", "accounts": [], "active": None}
    rows, err = gcloud_accounts()
    active = next((r["account"] for r in rows if r["active"]), None)
    pin = LOGIN_OPTS.get("account")
    who = pin if pin and any(r["account"] == pin for r in rows) else (None if pin else active)
    if not who:
        why = (f"gcloud has no credentials for {pin}." if pin and not err else _first_line(err, 160) if err else "gcloud has no active account.")
        return {"state": "not_signed_in", "who": None, "detail": why, "accounts": rows, "active": active,
                "hint": (("Run 'gcloud auth login --no-launch-browser' in your own terminal (the commands are shown in step 2), then press 'I have signed in - Verify'.")
                         if (LOGIN_OPTS.get("signin") or "manual") == "manual" else
                         "Press 'Sign in' (device code: you get a link and a code box right here) to sign in." if LOGIN_OPTS.get("device_code")
                         else "Press 'Sign in' (runs: gcloud auth login) and complete the sign-in in the console window / browser that opens.")}
    AUTH_STATE["last"] = who
    return {"state": "ok", "who": who, "detail": "", "hint": "", "accounts": rows, "active": active}


# ---- device-code sign-in: `gcloud auth login --no-launch-browser` with its output captured (no console window) -------------------------
# gcloud has no true "device code": with --no-launch-browser it prints a sign-in link; after signing in (in any browser, on any machine) Google
# shows a verification code that must be pasted back into gcloud's input. This session shows the link and takes the code in the window.
NO_URL_SECONDS = 6.0               # no link in gcloud's output after this long: the window switches to the manual commands
ROLLING_LINES = 400                # output lines kept
SIGNIN_WAIT_SECONDS = 300          # how long the window waits for the code before it gives up (shown as a countdown)
SIGNIN_URL_PATTERN = re.compile(r"https?://\S+")
_URL_CHARS = re.compile(r"^[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+$")
_CODE_LIKE = re.compile(r"\b4/[A-Za-z0-9_\-]{12,}")                 # what a Google verification code looks like (masked in any output)
SIGNIN_STEPS = ("1. Open the link below (any browser, on any computer).",
                "2. Sign in with the Google account you want and allow access.",
                "3. Copy the verification code Google shows and paste it into the box below, then press 'Submit code'.")


class UrlAssembler:
    """Finds the sign-in URL in gcloud's output lines. The URL is long and may be wrapped over several lines: lines that follow the
    first one and look like a URL continuation (no spaces, URL characters only) are joined; a blank / ordinary line ends it."""

    def __init__(self):
        self.parts = None
        self.url = None

    def feed(self, line):
        """Returns the finished URL the moment it is complete, else None."""
        text = ANSI_PATTERN.sub("", str(line)).strip()
        if self.url:
            return None
        if self.parts is None:
            m = SIGNIN_URL_PATTERN.search(text)
            if m:
                self.parts = [m.group(0)]
            return None
        if text and _URL_CHARS.match(text):
            self.parts.append(text)
            return None
        return self._finish()

    def idle(self):
        """Called when gcloud went quiet: whatever was collected is the URL."""
        return self._finish() if (self.parts and not self.url) else None

    def _finish(self):
        self.url = "".join(self.parts)
        return self.url


def mask_secrets(text, code=None):
    """Hides the verification code (and anything shaped like one) in a line of output."""
    out = str(text)
    if code and len(code) >= 4:
        out = out.replace(code, "[code hidden]")
    return _CODE_LIKE.sub("[code hidden]", out)


class SignInSession:
    """One `gcloud auth login --no-launch-browser [--account HINT]` run with captured, streamed output and a piped stdin.
    on_event(kind, data): "started", "line" (a masked output line), "url", "nourl" (raw lines), "tick" (seconds left), "finished" (dict)."""

    def __init__(self, account_hint=None, on_event=None, wait_seconds=None):
        self.hint = (account_hint or "").strip() or None
        self.on_event = on_event or (lambda kind, data=None: None)
        self.wait = wait_seconds or SIGNIN_WAIT_SECONDS
        self.proc = None
        self.url = None
        self.lines = []
        self.code_sent = False
        self._code = None
        self.cancelled = False
        self.expired = False
        self.result = None
        self.done = threading.Event()
        self._q = queue.Queue()
        self._t0 = None
        self._lock = threading.Lock()
        self._nourl_said = False

    def command(self):
        exe = shutil.which("gcloud") or "gcloud"
        return [exe, "auth", "login", "--no-launch-browser"] + (["--account", self.hint] if self.hint else [])

    def command_text(self):
        return " ".join(["gcloud"] + self.command()[1:])

    def start(self):
        """Starts gcloud. Returns None, or a plain-language error (nothing was started)."""
        if not shutil.which("gcloud"):
            return "The Google Cloud CLI (gcloud) was not found on PATH - install it from https://cloud.google.com/sdk/docs/install"
        cmd = self.command()
        why = gcloud_violation(cmd[1:], local_ok=True)
        if why:
            return guard_block("login", cmd[1:], why)
        env = dict(os.environ, CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK="1", PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        env.pop("CLOUDSDK_CORE_DISABLE_PROMPTS", None)      # this login is interactive (the code is typed in): prompts must stay enabled
        kwargs = {"creationflags": 0x08000000} if os.name == "nt" else {}          # CREATE_NO_WINDOW: the output is shown in this window instead
        try:
            self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                         encoding="utf-8", errors="replace", bufsize=1, env=env, **kwargs)
        except Exception as exc:
            return f"could not start gcloud: {exc}"
        self._t0 = time.time()
        self.on_event("started", self.command_text())
        threading.Thread(target=self._read, daemon=True).start()
        threading.Thread(target=self._work, daemon=True).start()
        return None

    def _read(self):
        """Reads the output in raw chunks (os.read) as it arrives - NOT line by line: gcloud's prompt ('Enter verification code: ') has no newline, and a
        link without a trailing newline must still be seen. stderr is merged into the same pipe. (Test doubles without a file descriptor are read by line.)"""
        out = self.proc.stdout
        try:
            fd = out.fileno()
            if not isinstance(fd, int):
                raise TypeError
        except Exception:
            fd = None
        try:
            if fd is not None:
                import codecs
                dec = codecs.getincrementaldecoder("utf-8")(errors="replace")
                while True:
                    data = os.read(fd, 4096)
                    if not data:
                        break
                    text = dec.decode(data)
                    if text:
                        self._q.put(text)
            else:
                for line in iter(out.readline, ""):
                    self._q.put(line)
        except Exception:
            pass
        self._q.put(None)

    def _line(self, raw, asm):
        """One complete output line: keep it (masked), look for the link, report it."""
        raw = ANSI_PATTERN.sub("", raw).rstrip("\r\n")
        shown = mask_secrets(raw, self._code)
        if shown.strip():
            self.lines.append(shown)
            del self.lines[:-ROLLING_LINES]
        in_url = asm.parts is not None and not asm.url
        url = asm.feed(raw)
        if url:
            self._got_url(url)
        elif asm.parts is not None and not asm.url:
            in_url = True
        if shown.strip() and not (in_url and not url):
            self.on_event("line", shown)

    def _work(self):
        asm = UrlAssembler()
        last_tick = None
        last_item = time.time()
        partial = ""
        eof = False
        while not eof:
            try:
                item = self._q.get(timeout=0.25)
            except queue.Empty:
                item = False
            now = time.time()
            left = max(0, int(self.wait - (now - self._t0) + 0.999))
            if left != last_tick:
                last_tick = left
                self.on_event("tick", left)
            if item is None:
                eof = True
            elif item is not False:
                last_item = now
                self.on_event("raw", mask_secrets(ANSI_PATTERN.sub("", item), self._code))      # exactly what gcloud printed (masked), for the raw-output box
                parts = re.split(r"\r\n|\n|\r", partial + item)
                partial = parts.pop()
                for raw in parts:
                    self._line(raw, asm)
            elif now - last_item > 0.6:                      # gcloud went quiet (it waits for the code): the unfinished line and the link so far are complete
                if partial.strip():
                    self._line(partial, asm)
                    partial = ""
                url = asm.idle() if now - last_item > 1.0 else None
                if url:
                    self._got_url(url)
            if not eof and now - self._t0 > NO_URL_SECONDS and not self.url and not self.code_sent and not self._nourl_said:
                self._nourl_said = True
                self.on_event("nourl", list(self.lines) + ([mask_secrets(partial, self._code)] if partial.strip() else []))
            if not eof and now - self._t0 > self.wait and not self.cancelled and not self.expired:
                self.expired = True
                self.terminate()
        if partial.strip():
            self._line(partial, asm)
        url = asm.idle()
        if url:
            self._got_url(url)
        try:
            rc = self.proc.wait(timeout=10)
        except Exception:
            self.terminate()
            rc = self.proc.poll()
        if not self.url:
            self.on_event("nourl", list(self.lines))
        self.result = {"rc": rc, "ok": rc == 0 and not self.cancelled and not self.expired, "cancelled": self.cancelled, "expired": self.expired,
                       "code_sent": self.code_sent, "error": self._explain(rc), "last_lines": [l for l in self.lines if l.strip()][-8:],
                       "command": self.command_text(), "causes": [] if (rc == 0 or self.cancelled) else likely_signin_causes(self.lines, rc)}
        self.done.set()
        self.on_event("finished", self.result)

    def _got_url(self, url):
        if not self.url:
            self.url = url
            self.on_event("url", url)

    def _explain(self, rc):
        if self.cancelled:
            return "Sign-in cancelled."
        if self.expired:
            return "The sign-in timed out before a code was entered. Press 'Sign in' to get a new link."
        if rc == 0:
            return ""
        cand = [l for l in self.lines if l.strip() and "http" not in l]
        last = next((l for l in reversed(cand) if "error" in l.lower()), cand[-1] if cand else "")
        if self.code_sent:
            return ("gcloud did not accept the verification code (it may be wrong, incomplete or expired). Press 'Sign in' for a new link "
                    "and paste the new code." + (f" (gcloud said: {_first_line(last, 140)})" if last else ""))
        return "gcloud stopped before a code was entered" + (f": {_first_line(last, 140)}" if last else ".") + " Press 'Sign in' to try again."

    def submit(self, code):
        """Writes the verification code + newline to gcloud's input. The code is never logged or kept in any output."""
        code = (code or "").strip()
        if not code:
            return False
        if not self.proc or self.done.is_set() or self.proc.poll() is not None:
            return False
        try:
            with self._lock:
                self._code = code
                self.proc.stdin.write(code + "\n")
                self.proc.stdin.flush()
            self.code_sent = True
        except Exception:
            return False
        return True

    def terminate(self):
        try:
            if self.proc and self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=3)
                except Exception:
                    self.proc.kill()
        except Exception:
            pass

    def cancel(self):
        self.cancelled = True
        self.terminate()


def signin_box_lines(url):
    """The sign-in instructions as a framed box (command-line mode / live log). The verification code is never part of it."""
    width = 78
    body = ["SIGN IN TO GOOGLE CLOUD (device code / no-browser sign-in)", ""] + list(SIGNIN_STEPS) + ["", "Sign-in link:"]
    body += [url[i:i + width - 4] for i in range(0, len(url), width - 4)] if url else ["(not found yet)"]
    return ["+" + "-" * (width - 2) + "+"] + ["| " + l.ljust(width - 4) + " |" for l in body] + ["+" + "-" * (width - 2) + "+"]


def cli_device_login(emit, account_hint=None, ask_code=None, wait_seconds=None):
    """Console sign-in with `gcloud auth login --no-launch-browser`: prints the framed instructions + link, asks for the verification code
    (hidden input) and passes it to gcloud. Returns (ok, error_text)."""
    got = {"url": None, "raw": None}
    ready = threading.Event()

    def on_event(kind, data=None):
        if kind == "url":
            got["url"] = data
            ready.set()
        elif kind == "nourl":
            got["raw"] = data
            ready.set()
        elif kind == "finished":
            ready.set()
    sess = SignInSession(account_hint, on_event, wait_seconds)
    err = sess.start()
    if err:
        return False, err
    emit("Running: " + " ".join(["gcloud"] + sess.command()[1:]))
    ready.wait(timeout=60)
    if got["url"]:
        for l in signin_box_lines(got["url"]):
            emit(l)
        if not sess.done.is_set():
            def default_ask():
                import getpass
                return getpass.getpass("Paste the verification code here (input is hidden) and press Enter: ")
            try:
                code = (ask_code or default_ask)()
            except (EOFError, KeyboardInterrupt):
                sess.cancel()
                code = ""
            if code and not sess.submit(code):
                emit("The sign-in already ended - the code was not sent.")
    else:
        emit("Could not find the sign-in link in gcloud's output. Raw output:")
        for l in (got.get("raw") or sess.lines)[-20:]:
            emit("  " + l)
    sess.done.wait(timeout=120)
    if not sess.done.is_set():
        sess.cancel()
        sess.done.wait(timeout=10)
    res = sess.result or {"ok": False, "error": "sign-in did not finish"}
    return bool(res["ok"]), res.get("error") or ""


# ---------------------------------------------------------------------------
# Signing in.  Three methods (step 2 of the window, --signin-method on the command line):
#   manual   (DEFAULT)  the tool SHOWS the exact commands (gcloud auth login --no-launch-browser, ...); the user runs one in their own Command Prompt /
#                       PowerShell, then presses 'I have signed in - Verify' (the tool only runs the read-only `gcloud auth list`, the token check and the
#                       project list). The window also checks every few seconds and notices the sign-in by itself. 'Open a terminal for me' starts a
#                       visible PowerShell window with the chosen `gcloud auth login` form - a LOCAL-ONLY, user-initiated exception limited to exactly
#                       `gcloud auth login [--no-launch-browser] [--account X]`.
#   captured            `gcloud auth login --no-launch-browser` runs with its output captured; the link is shown in the window and the code is pasted back.
#                       When no link appears (~6 s) or it fails, the window switches to the manual commands.
#   console             the same command in its own visible console window; the tool waits and then re-checks the sign-in.
# The tool never installs anything: the install hints for gcloud and its components (gke-gcloud-auth-plugin) are TEXT only.
# ---------------------------------------------------------------------------

MANUAL_POLL_SECONDS = 5         # manual mode: how often the window checks whether the sign-in happened
MANUAL_POLL_CAP = 900           # ... and for how long (seconds) before it stops waiting
MANUAL_INSTRUCTIONS = "Sign in from your own Command Prompt or PowerShell. If the Google Cloud CLI is not installed yet, install it first, then run this command:"
MANUAL_STEPS = ("Then: gcloud prints a URL - open it in a browser, sign in with your Google account and allow access, copy the verification code Google shows, "
                "paste it back into the terminal and press Enter. Return to this window and press 'I have signed in - Verify'.")
GCLOUD_INSTALL_URL = "https://cloud.google.com/sdk/docs/install"
GCLOUD_INSTALL_WINGET = "winget install -e --id Google.CloudSDK"
GCLOUD_MISSING_TEXT = "Google Cloud CLI (gcloud) not found on this computer - install it first"
GCLOUD_INSTALL_HINT = (f"Install it from {GCLOUD_INSTALL_URL}  -  on Windows you can run:  {GCLOUD_INSTALL_WINGET}  "
                       "(then open a NEW terminal window). This tool never installs anything.")
PLUGIN_INSTALL_CMD = "gcloud components install gke-gcloud-auth-plugin"
NO_URL_REASON = "No URL received from gcloud yet. Run one of these commands in your own terminal, then press Verify."
FAILED_REASON = "The automatic sign-in did not complete. Run one of these commands in your own terminal, then press Verify."
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+$")
_TERMINAL_FORMS = ("device", "device-account", "browser")


def manual_commands(account=None, plugin_missing=None):
    """The numbered commands of the manual sign-in. Each: {n, key, cmd, note[, text_only]}. `account` = the account hint (adds the 1b variant). They are
    shown as TEXT; the tool itself never runs them (only 'Open a terminal for me' starts the `gcloud auth login` forms, on the user's request).
    The helpers marked text_only (application-default sign-in, the kubectl plugin) are never run by this tool, not even from the terminal button."""
    account = account if (account and EMAIL_RE.match(account)) else None
    items = [{"n": "1", "key": "device", "cmd": "gcloud auth login --no-launch-browser",
              "note": "Recommended: prints a URL - open it, sign in, copy the verification code and paste it back into the terminal."}]
    if account:
        items.append({"n": "1b", "key": "device-account", "cmd": f"gcloud auth login --no-launch-browser --account {account}",
                      "note": "Same, for the account in the 'hint' box."})
    items += [{"n": "2", "key": "browser", "cmd": "gcloud auth login", "note": "Normal flow: opens your browser by itself."},
              {"n": "3a", "key": "list", "cmd": "gcloud auth list", "note": "Shows the accounts gcloud holds credentials for (the active one is marked)."},
              {"n": "3b", "key": "projects", "cmd": "gcloud projects list", "note": "Verify: lists the projects this account can see."},
              {"n": "4a", "key": "adc", "cmd": "gcloud auth application-default login", "text_only": True,
               "note": "Only if another tool asks for application-default credentials (this tool does not need it)."}]
    if plugin_missing is None:
        plugin_missing = not shutil.which("gke-gcloud-auth-plugin")
    if plugin_missing:
        items.append({"n": "4b", "key": "plugin", "cmd": PLUGIN_INSTALL_CMD, "text_only": True,
                      "note": "kubectl needs this plugin to sign in to GKE and it was not found. Shown as text only - run it yourself if you want it."})
    return items


_GCLOUD_VERSION = {}       # exe path -> first line of `gcloud --version` (only successful reads are remembered)


def gcloud_cli_line():
    """('Google Cloud CLI installed ...' text, found): PATH lookup only (no process); the version is read separately by gcloud_version()."""
    exe = shutil.which("gcloud")
    if not exe:
        return GCLOUD_MISSING_TEXT, False
    ver = _GCLOUD_VERSION.get(exe)
    return (f"Google Cloud CLI installed: {ver}" if ver else f"Google Cloud CLI installed ({exe})"), True


def gcloud_version(timeout=60):
    """First line of `gcloud --version` (e.g. 'Google Cloud SDK 480.0.0'), or None. The only information command the tool runs besides the read verbs."""
    exe = shutil.which("gcloud")
    if not exe:
        return None
    if exe in _GCLOUD_VERSION:
        return _GCLOUD_VERSION[exe]
    guard_allow()
    try:
        proc = subprocess.run([exe, "--version"], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout,
                              stdin=subprocess.DEVNULL, env=_gcloud_env())
    except Exception:
        return None
    first = next((" ".join(l.split()) for l in str(getattr(proc, "stdout", "") or "").splitlines() if l.strip()), "")
    if getattr(proc, "returncode", 1) != 0 or not first:
        return None
    _GCLOUD_VERSION[exe] = first[:120]
    return _GCLOUD_VERSION[exe]


def manual_signin_block(account=None, reason=None, gcloud_missing=None, plugin_missing=None):
    """The numbered command block as text lines (command line; same content as the window's manual panel, incl. the install hint when gcloud is missing)."""
    if gcloud_missing is None:
        gcloud_missing = not shutil.which("gcloud")
    rows = []
    if reason:
        rows += [reason, ""]
    rows += [MANUAL_INSTRUCTIONS, ""]
    if gcloud_missing:
        rows += [GCLOUD_MISSING_TEXT, "  Install it from " + GCLOUD_INSTALL_URL, "  On Windows you can run:  " + GCLOUD_INSTALL_WINGET,
                 "  (then open a NEW terminal window; this tool never installs anything)", ""]
    for item in manual_commands(account, plugin_missing):
        rows += [f"  {item['n']}. {item['cmd']}", f"        {item['note']}"]
    rows += ["", MANUAL_STEPS.replace("press 'I have signed in - Verify'.", "press Enter here.")]
    width = max(len(r) for r in rows) + 2
    return ["+" + "-" * width + "+"] + ["| " + r.ljust(width - 1) + "|" for r in rows] + ["+" + "-" * width + "+"]


def likely_signin_causes(lines, rc=None):
    """Plain-language likely causes of a sign-in process that ended without success (from its output)."""
    low = " ".join(str(l) for l in (lines or [])).lower()
    causes = []
    if re.search(r"not recognized|no such file|cannot find|not found", low):
        causes.append("gcloud (or Python for gcloud) was not found - install the Google Cloud CLI and open a NEW terminal.")
    if re.search(r"ssl|certificate|proxy|connection|network|timed out|unreachable|name resolution|getaddrinfo", low):
        causes.append("a network, proxy or certificate problem - check the VPN / proxy settings, then try again.")
    if re.search(r"invalid_grant|invalid verification code|bad request|did not accept", low):
        causes.append("the verification code was wrong, incomplete or expired - start again and paste the new code.")
    if re.search(r"reauthentication|policy|organization|blocked|access_denied|admin", low):
        causes.append("your organization may require re-authentication or blocks this sign-in - ask your administrator, or use the browser flow (gcloud auth login).")
    if re.search(r"prompt|non-interactive|eof|stdin|tty", low):
        causes.append("gcloud could not ask for the code (no interactive input) - run the command in your own terminal instead.")
    if not causes:
        causes.append("the sign-in was closed or interrupted before it finished - run it again, or run the command in your own terminal.")
    return causes


def verify_signin(account=None):
    """The 'Verify' check (read-only: `gcloud auth list`, the token usability check, `gcloud projects list`). Returns login_status()'s dict; on success it
    also has "n_projects". A failed one has "state": "not_signed_in" and the reason in "detail" ("expired": True when the credentials are expired)."""
    st = login_status(account)
    if st["state"] != "ok":
        return st
    tok = account_status(st["who"])
    st = dict(st, tok=tok)
    if tok["state"] == "expired":
        return dict(st, state="not_signed_in", expired=True, detail=tok.get("detail") or "credentials expired - Reauthentication required",
                    hint="Run one of the commands shown in step 2 again.")
    if tok["state"] == "unknown":
        return dict(st, state="not_signed_in", detail=tok.get("detail") or "the credentials could not be used", hint="Run one of the commands shown in step 2 again.")
    rows, err = load_accounts()
    st["n_projects"] = len(rows)
    st["projects_error"] = err
    return st


def verify_signin_follow(account=None):
    """verify_signin(), and when the pinned / chosen account does not work but gcloud now has ANOTHER active account (the user signed in with a different
    one), follow it: that account is pinned (nothing is written to gcloud) and verified instead."""
    res = verify_signin(account)
    if res.get("state") == "ok":
        return res
    pin = LOGIN_OPTS.get("account")
    rows, _err = gcloud_accounts()
    act = next((r["account"] for r in rows if r["active"]), None)
    if pin and act and act != pin:
        pin_account(act)
        alt = verify_signin(account)
        if alt.get("state") == "ok":
            return alt
        pin_account(pin)
    return res


def signin_failure_help(res, account=None):
    """The exact error and which command to try next, after a failed verification. `res` = the dict of verify_signin() (or just the error text)."""
    res = res if isinstance(res, dict) else {"detail": str(res or "")}
    detail = str(res.get("detail") or "unknown error")
    cmds = {i["key"]: i["cmd"] for i in manual_commands(account, True)}
    low = detail.lower()
    if res.get("state") == "no_cli":
        nxt = f"{GCLOUD_MISSING_TEXT}. Install it from {GCLOUD_INSTALL_URL} (Windows: {GCLOUD_INSTALL_WINGET}), open a NEW terminal window and run: {cmds['device']}"
    elif res.get("expired") or is_expired_error(detail):
        nxt = f"Reauthentication required - the sign-in expired or was not completed. Run: {cmds['device']}"
    elif "no credentials for" in low:
        nxt = f"gcloud has no credentials for this account yet. Run: {cmds.get('device-account') or cmds['device']}"
    else:
        nxt = f"You are not signed in yet. Run: {cmds['device']}   (or: {cmds['browser']})"
    return f"Verification failed: {_first_line(mask_tokens(detail), 200)}\nNext: {nxt}   Then press 'I have signed in - Verify' again."


def terminal_signin_plan(form, account=None):
    """(Popen args, displayed command, None) or (None, None, reason): the PowerShell window of 'Open a terminal for me'. User-initiated LOCAL-ONLY
    exception: only `gcloud auth login [--no-launch-browser] [--account X]`, checked by the same strict guard as every other sign-in command."""
    if form not in _TERMINAL_FORMS:
        return None, None, f"'{form}' is not one of the sign-in commands"
    if form == "device-account" and not (account and EMAIL_RE.match(account)):
        return None, None, "Type your Google account (an email address) in the 'hint' box first."
    flags = {"device": ["--no-launch-browser"], "device-account": ["--no-launch-browser", "--account", account or ""], "browser": []}[form]
    if not shutil.which("gcloud"):
        return None, None, GCLOUD_MISSING_TEXT + ". " + GCLOUD_INSTALL_HINT
    why = gcloud_violation(["auth", "login", *flags], local_ok=True)
    if why:
        return None, None, guard_block("login", ["auth", "login", *flags], why)
    line = " ".join(["gcloud", "auth", "login", *flags])
    shell = shutil.which("powershell") or shutil.which("pwsh") or "powershell"
    return [shell, "-NoExit", "-Command", line], line, None


def open_terminal_signin(form, account=None, popen=None):
    """Start a visible PowerShell window that runs the chosen `gcloud auth login` form and stays open (CREATE_NEW_CONSOLE). The tool does not wait for it.
    Returns (ok, command text or reason)."""
    args, line, why = terminal_signin_plan(form, account)
    if why:
        return False, why
    if os.name != "nt":
        return False, "Opening a terminal is only done on Windows - copy the command and run it in your own terminal."
    env = dict(os.environ)
    env.pop("CLOUDSDK_CORE_DISABLE_PROMPTS", None)           # an interactive sign-in: prompts must stay enabled
    try:
        (popen or subprocess.Popen)(args, creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0x10), env=env)
    except Exception as exc:
        return False, f"could not open a terminal: {exc}"
    return True, line


def manual_signin_cli(emit=print, reason=None, input_fn=None, status_fn=None):
    """Command line, manual method: print the numbered command block, wait for Enter, verify (read-only), repeat on failure.
    Returns True when signed in; False on Ctrl+C / no input (stdin is not interactive)."""
    account = LOGIN_OPTS.get("account")
    for line in manual_signin_block(account, reason):
        emit(line)
    ask = input_fn or input
    while True:
        try:
            ask("Press Enter after you have signed in, or Ctrl+C to stop: ")
        except (KeyboardInterrupt, EOFError):
            emit("")
            emit("Stopped waiting - no sign-in was verified. Run one of the commands above in your terminal, then run this tool again.")
            return False
        st = (status_fn or verify_signin)()
        if st.get("state") == "ok":
            n = st.get("n_projects")
            emit(f"Signed in as {st.get('who') or '?'}" + (f" - {n} project{'s' if n != 1 else ''}" if n is not None else ""))
            return True
        for line in signin_failure_help(st, account).splitlines():
            emit(line)


def cli_sign_in(emit, account=None):
    """The interactive sign-in of the window's 'Sign in' button (gcloud auth login, honouring the no-browser option)."""
    return cli_ensure_gcloud(emit)


def cli_ensure_gcloud(emit):
    """True when gcloud is installed and has an ACTIVE (or the pinned) account (`gcloud auth list`). If not, signs in: with the device code /
    no-browser flow (default; the console shows the link and asks for the code) or `gcloud auth login` in a console / browser window."""
    exe = shutil.which("gcloud")
    method = LOGIN_OPTS.get("signin") or "manual"
    if not exe:
        if method == "manual" and not LOGIN_OPTS["gui"]:
            if not manual_signin_cli(emit, "Google Cloud CLI (gcloud) was not found on PATH."):
                return False
            exe = shutil.which("gcloud")
            if not exe:
                emit("gcloud is still not found on PATH. Open a NEW terminal window (the PATH of this one does not know the new install) and run this tool again.")
                return False
        else:
            emit("Google Cloud CLI (gcloud) was not found on PATH - install it (" + GCLOUD_INSTALL_URL + "; Windows: " + GCLOUD_INSTALL_WINGET
                 + "), open a NEW terminal window, then run: gcloud auth login --no-launch-browser")
            return False

    def active():
        return _gcloud_active_account()[0]
    who = active()
    hint = LOGIN_OPTS.get("account")
    if who:
        known = AUTH_STATE["status"].get(who)
        st = ("active", 0) if (known and known[0] == "active" and time.time() - known[1] < 600) else None
        state_now = st[0] if st else account_status(who)["state"]
        if state_now != "expired":
            emit(f"gcloud is signed in as {who}")
            return True
        emit(f"Credentials for {who} expired (the sign-in session ended). Signing in again ...")
        hint = who
    else:
        emit("gcloud has no active account" + (f" for {hint}" if hint else "") + ".")
    if LOGIN_OPTS["gui"] and (method == "manual" or (method == "captured" and LOGIN_OPTS["device_code"])):
        emit("Sign in with the 'Sign in' button in step 2 (the commands are shown there; for the captured method it shows the link and the code box), "
             "then run again.")
        return False
    if method == "manual":
        reason = f"Credentials for {who} expired (the sign-in session ended)." if who else ("gcloud has no active account" + (f" for {hint}" if hint else "") + ".")
        if not manual_signin_cli(emit, reason):
            return False
    elif method == "captured" and LOGIN_OPTS["device_code"]:
        ok, why = cli_device_login(emit, hint)
        if not ok:
            emit("gcloud auth login failed or was cancelled" + (f": {why}" if why else "") + ".")
            emit("You can sign in yourself instead: run `gcloud auth login --no-launch-browser` in your own terminal, then run this tool again.")
            return False
    else:                                                  # console: gcloud in its own console window
        flags = ["--no-launch-browser"] if (LOGIN_OPTS["device_code"] and method == "console") else []
        rc = _run_interactive([exe, "auth", "login"] + flags + (["--account", hint] if hint else []), emit)
        if rc != 0:
            emit("gcloud auth login failed or was cancelled" + (f" (exit code {rc})" if rc else "") + ".")
            return False
    who = active()
    if who and account_status(who)["state"] == "expired":
        emit(f"Credentials for {who} are still expired after the sign-in.")
        return False
    if not who and not hint:
        rows, _err = gcloud_accounts()
        who = next((r["account"] for r in rows if r["active"]), None)
    if not who:
        emit("Still no active gcloud account after gcloud auth login.")
        return False
    emit(f"gcloud login OK - signed in as {who}")
    return True


def load_accounts():
    """Every GCP project `gcloud` can see (no cap): ([{id, name, code, info, usable}], error_or_None), sorted by name."""
    projects, err = _gcp_projects()
    rows = [{"id": pid, "name": i.get("name") or pid, "code": pid,
             "info": " ".join(x for x in ((i.get("state") or ""), ("[default]" if i.get("default") else "")) if x),
             "usable": i.get("state") in (None, "ACTIVE")} for pid, i in projects.items()]
    return sorted(rows, key=lambda a: (a["name"].lower(), a["id"])), err


CONFIRM_OVER = 20            # collecting clusters from more projects than this asks for a confirmation first
CONFIRM_HOOK = {"fn": None}  # tests replace it: fn(count) -> bool


def confirm_many(n):
    """One-line confirmation before a large cluster search (window only)."""
    if CONFIRM_HOOK["fn"]:
        return bool(CONFIRM_HOOK["fn"](n))
    from tkinter import messagebox
    return messagebox.askyesno("Collect clusters", f"This will search {n} projects and can take several minutes. Continue?")


def parse_project_scope(value):
    """--project value -> (scope, single_project): 'all' -> ("all", None); 'a,b,c' -> (["a","b","c"], None); 'a' -> (None, "a"); None -> (None, None)."""
    v = (value or "").strip()
    if not v:
        return None, None
    if v.lower() == "all":
        return "all", None
    parts = [x.strip() for x in v.split(",") if x.strip()]
    if len(parts) > 1:
        return list(dict.fromkeys(parts)), None
    return None, parts[0] if parts else None


LIST_WORKERS = 8             # parallel `gcloud container clusters list` calls
INVENTORY_MIN_PROJECTS = 25  # from this many projects on (and all of them chosen), try ONE Cloud Asset Inventory search first
CLUSTER_NAME_IN_ASSET = re.compile(r"/projects/([^/]+)/locations/([^/]+)/clusters/([^/]+)$")


def _gke_row(c, pid, pname=None):
    loc = c.get("location") or c.get("zone")
    return {"name": c.get("name"), "location": loc, "project": pid, "where": loc or "?", "account": pid, "account_name": pname or pid,
            "key": f"{pid}/{loc}/{c.get('name')}".lower()}


def _gke_inventory(emit, cancel=None):
    """GKE clusters of the whole organization(s) with Cloud Asset Inventory (`gcloud asset search-all-resources`), when it is available
    to you. Returns a list of cluster dicts, or None to fall back to listing project by project."""
    orgs, err = gcloud(["organizations", "list"], None, 60, project=False)
    if err or not isinstance(orgs, list) or not orgs:
        return None
    rows = []
    for org in orgs:
        if cancel is not None and cancel.is_set():
            return rows
        oid = str((org or {}).get("name") or "").split("/")[-1]
        data, err = gcloud(["asset", "search-all-resources", f"--scope=organizations/{oid}",
                            "--asset-types=container.googleapis.com/Cluster"], None, 240, project=False)
        if err or not isinstance(data, list):
            return None
        for r in data:
            m = CLUSTER_NAME_IN_ASSET.search(str((r or {}).get("name") or ""))
            if m:
                rows.append({"name": m.group(3), "location": m.group(2), "_project": m.group(1)})
    return rows


EXE_MENU = {"loaded": False, "clusters": {}}     # the gkelogin menu as the window last read it (the tests reset it)
LAST_SCAN = {"projects": 0, "failed": [], "errors": {}, "found": 0, "with_clusters": 0, "inventory": False}   # what the latest scan_clusters() saw


def _plural(n, noun):
    return f"{n} {noun}" + ("" if n == 1 else "s")


def scan_summary(rows=None, limit=10):
    """One clear line for the latest scan: 'Found 57 clusters in 12 projects (300 searched), 3 projects failed: a, b, c' (rows = the
    clusters that count, when the caller has already merged / filtered them)."""
    s = LAST_SCAN
    failed = list(s.get("failed") or [])
    found = len(rows) if rows is not None else s.get("found", 0)
    with_clusters = len({c.get("project") for c in rows}) if rows is not None else s.get("with_clusters", 0)
    text = f"Found {_plural(found, 'cluster')} in {_plural(with_clusters, 'project')}"
    if s.get("projects", 0) > with_clusters:
        text += f" ({s['projects']} searched)"
    if failed:
        text += f", {_plural(len(failed), 'project')} failed: " + ", ".join(failed[:limit]) + (" ..." if len(failed) > limit else "")
    return text


def scan_clusters(accounts, emit=print, progress=None, cancel=None, on_batch=None, inventory=False):
    """GKE clusters in the given projects ([{id, name}], no limit): returns (clusters, failed_count), de-duplicated and in project
    order. Projects are listed with `gcloud container clusters list --project P` in LIST_WORKERS parallel calls (gcloud has no
    cross-project list). With inventory=True (all projects chosen, at least INVENTORY_MIN_PROJECTS) ONE Cloud Asset Inventory
    search is tried first and used when it works. progress(done, total) counts projects; on_batch(new_clusters) gets clusters as they arrive.
    A project that cannot be listed (no access, Kubernetes Engine API off) is logged and skipped; the totals are kept in LAST_SCAN
    and summarised by scan_summary()."""
    note = progress or (lambda *a: None)
    total = len(accounts)
    names = {a["id"]: a.get("name") for a in accounts}
    order = {a["id"]: i for i, a in enumerate(accounts)}
    found, seen, failed, done, errors, used_inventory = [], set(), [], 0, {}, False

    def add(rows):
        new = []
        for c in rows:
            if c["name"] and c["key"] not in seen:
                seen.add(c["key"])
                found.append(c)
                new.append(c)
        if new and on_batch:
            on_batch(new)

    def stopped():
        return cancel is not None and cancel.is_set()

    todo = [a["id"] for a in accounts]
    if inventory and total >= INVENTORY_MIN_PROJECTS and None not in todo:
        rows = _gke_inventory(emit, cancel)
        if rows is not None:
            emit(f"Listed GKE clusters of {total} projects with Cloud Asset Inventory (it can lag a few minutes behind real changes).")
            add([_gke_row(r, r["_project"], names.get(r["_project"])) for r in rows if r["_project"] in names])
            note(total, total)
            todo, used_inventory = [], True
    if todo:
        def one(pid):
            if stopped():
                return pid, None, "cancelled"
            data, err = gcloud(["container", "clusters", "list"], {"project": pid}, timeout=90)
            if err or not isinstance(data, list):
                return pid, None, err or "unexpected output"
            return pid, [_gke_row(c, pid, names.get(pid)) for c in data if isinstance(c, dict)], None
        with ThreadPoolExecutor(max_workers=max(1, min(LIST_WORKERS, len(todo)))) as pool:
            futures = [pool.submit(one, pid) for pid in todo]
            for fut in as_completed(futures):
                if fut.cancelled():
                    continue
                pid, rows, err = fut.result()
                if err == "cancelled":
                    continue
                done += 1
                if rows is None:
                    failed.append(pid)
                    errors[pid] = _first_line(err or "unexpected output", 120)
                    emit(f"  project {pid}: skipped - could not list GKE clusters: {errors[pid]}")
                else:
                    add(rows)
                note(done, total)
                if stopped():
                    for f in futures:
                        f.cancel()
    found.sort(key=lambda c: order.get(c["project"], len(order)))      # stable: keeps the listed order inside a project
    failed.sort(key=lambda p: order.get(p, len(order)))
    LAST_SCAN.update(projects=total, failed=list(failed), errors=dict(errors), found=len(found),
                     with_clusters=len({c["project"] for c in found}), inventory=used_inventory)
    if total > 1 or failed:
        emit(scan_summary())
    return found, len(failed)


def _numkey(k):
    return int(k) if str(k).isdigit() else 10**9


def merge_menu(found, menu):
    """Mark which clusters of the gcloud listing are also in the gkelogin menu (key 'exe_number': log in with gkelogin as before), and
    append the menu entries gcloud did not find (key 'exe_only') so nothing the menu offers is lost. A menu entry whose name belongs to
    SEVERAL clusters (same name in two projects) is ambiguous: none of them is mapped to the exe - they use
    `gcloud container clusters get-credentials`, which knows the exact project and location."""
    menu = menu or {}
    by_name = defaultdict(list)
    for c in found:
        by_name[(c.get("name") or "").lower()].append(c)
    rows = list(found)
    for num, label in sorted(menu.items(), key=lambda kv: _numkey(kv[0])):
        text = str(label)
        base = re.split(r"\s*[\(\[]", text)[0].strip()
        cands = by_name.get(base.lower())
        if not cands:
            cands = [c for n, cs in by_name.items() if n and re.search(r"(?<![\w.-])" + re.escape(n) + r"(?![\w.-])", text.lower()) for c in cs]
        if len(cands) == 1 and not cands[0].get("exe_number"):
            cands[0]["exe_number"] = str(num)
        elif not cands:
            rows.append({"name": base or text, "location": None, "project": None, "where": "gkelogin menu", "account": None, "account_name": "",
                         "key": f"menu/{num}", "exe_number": str(num), "exe_only": True, "menu_label": text})
    return rows


def register_clusters(found, multi=False):
    """Number the clusters 1..N in the given order, fill CLI_TARGETS and return {'1': 'name (location/project)'}."""
    CLI_TARGETS.clear()
    clusters = {}
    for i, c in enumerate(found, start=1):
        CLI_TARGETS[str(i)] = c
        if c.get("exe_only"):
            clusters[str(i)] = f"{c['name']} (gkelogin menu #{c['exe_number']})"
        else:
            clusters[str(i)] = f"{c['name']} ({c['location'] or '?'}/{c['project']})"
    return clusters


def describe_cluster(number):
    """'name | project | location' of a numbered cluster of the gcloud listing (for --list)."""
    t = CLI_TARGETS.get(str(number)) or {}
    return (f"name: {t.get('name') or '-'} | project: {t.get('project') or '-'} | location: {t.get('location') or '-'}"
            + (f" | gkelogin menu #{t['exe_number']}" if t.get("exe_number") else ""))


def list_clusters_cli(emit=print, accounts=None, progress=None, cancel=None, on_batch=None, inventory=False, all_projects=False, menu=None):
    """{'1': 'name (location/project)', ...} from `gcloud container clusters list` (read-only), numbered in the listed order: the chosen
    project, or every project gcloud can see when none is chosen or all_projects=True (no limit; parallel, see scan_clusters; one Cloud
    Asset Inventory search first when it works). Fills CLI_TARGETS and prints the 'Found N clusters in M projects ...' summary.
    `menu` ({number: name}) is the gkelogin menu: its entries are mapped to the clusters found (see merge_menu) and kept when gcloud
    cannot see them. If gcloud is not usable the menu is returned as it is.
    `accounts` ([{id, name}]) is what the window passes (it has already checked the sign-in); from the command line it is None."""
    CLI_TARGETS.clear()
    if accounts is None:
        if not cli_ensure_gcloud(emit):
            if menu:
                emit("The Google Cloud CLI (gcloud) is not available / not signed in - showing only the clusters from the gkelogin menu.")
                return dict(menu)
            return {}
        scope = GCP_OPTS.get("scope")
        if scope == "all" or all_projects:
            projects = [pid for pid, i in list_gcp_projects().items() if i.get("state") in (None, "ACTIVE")]
            emit("Cluster search scope: ALL projects gcloud can see (--project all)" + (f" - {len(projects)} projects, this can take several minutes." if projects else "."))
        elif isinstance(scope, list):
            projects = list(scope)
            emit(f"Cluster search scope: {len(projects)} project(s) from --project: " + ", ".join(projects[:10]) + (" ..." if len(projects) > 10 else ""))
        elif GCP_OPTS.get("project"):
            projects = [GCP_OPTS["project"]]
            emit(f"Cluster search scope: project {projects[0]} (--project)")
        else:
            dp = gcp_default_project()
            projects = [dp] if dp else []
            if dp:
                emit(f"Cluster search scope: only the currently configured project {dp} (gcloud config). Use --project a,b,c or --project all to search more.")
        if not projects:
            emit("No project to search: pass --project ID, --project a,b,c or --project all (or: gcloud config set project ID).")
            return dict(menu) if menu else {}
        accounts = [{"id": pid, "name": pid} for pid in projects]
        inventory = len(projects) >= INVENTORY_MIN_PROJECTS
    found, failed = scan_clusters(accounts, emit, progress, cancel, on_batch, inventory)
    rows = merge_menu(found, menu) if menu is not None else found
    if not rows:
        emit("No GKE clusters found in " + (accounts[0]["id"] if len(accounts) == 1 else f"{len(accounts)} project(s)") + " - check the project and your permissions.")
        return {}
    return register_clusters(rows, len(accounts) > 1)


def cli_login(number, label, emit):
    """Log in to cluster `number` with gcloud: make sure there is an active account, then
    `gcloud container clusters get-credentials`. Returns the kubectl context name. Raises RuntimeError when it fails."""
    if not CLI_TARGETS:
        list_clusters_cli(emit)
    tgt = CLI_TARGETS.get(str(number))
    if not tgt:
        raise RuntimeError(f"cluster {number} is not in the gcloud cluster list - run --list with --login-method cli to see the numbers")
    if not cli_ensure_gcloud(emit):
        raise RuntimeError("gcloud is not logged in (login failed, was cancelled, or gcloud is missing)")
    if not shutil.which("gke-gcloud-auth-plugin"):
        emit("WARNING: gke-gcloud-auth-plugin was not found on PATH - kubectl cannot authenticate to GKE without it. "
             "Install it with: gcloud components install gke-gcloud-auth-plugin")
    cmd = [shutil.which("gcloud"), "container", "clusters", "get-credentials", tgt["name"], "--location", tgt["location"],
           "--project", tgt["project"]]
    emit("Running: gcloud " + " ".join(cmd[1:]))
    ok, out = _run_captured(cmd, 120)
    if not ok:
        raise RuntimeError(f"gcloud container clusters get-credentials failed: {out}")
    emit(out or "kubeconfig updated.")
    return f"gke_{tgt['project']}_{tgt['location']}_{tgt['name']}"


# ---------------------------------------------------------------------------
# READ-ONLY GUARANTEE - enforced here, in the only places that start kubectl / gcloud or call a Google API.
# This tool only reads. It never installs, creates, changes or deletes anything on the cluster or in the cloud account, and it never runs
# anything inside the cluster. Every kubectl / gcloud command is checked against an ALLOW-LIST before a process is started; anything else is
# refused ("blocked: read-only mode - '<verb>' is not allowed"), no process is spawned, the attempt is recorded and shown in the report.
# LOCAL-ONLY exceptions (they write only on THIS machine, never to the cluster or the cloud): the user's own interactive sign-in
# (exactly `gcloud auth login [--no-launch-browser] [--account X]`), `gcloud container clusters get-credentials` (writes the local kubeconfig), `kubectl config use-context` (local kubeconfig)
# and the custom gkelogin.exe the user chose. Install hints are printed text only; nothing is ever installed or downloaded.
# ---------------------------------------------------------------------------

KUBECTL_READ_VERBS = ("get", "logs", "top", "version", "api-resources", "api-versions", "cluster-info", "explain")
KUBECTL_CONFIG_READ = ("get-contexts", "current-context", "view")
KUBECTL_CONFIG_LOCAL_ONLY = ("use-context",)           # local-only: switches the context in THIS machine's kubeconfig
KUBECTL_AUTH_READ = ("can-i",)
_KUBECTL_FLAG_WITH_VALUE = ("--context", "-n", "--namespace", "--kubeconfig", "--cluster", "--user", "--request-timeout", "-s", "--server", "--as")
_KUBECTL_BAD_FLAGS = ("-f", "--filename", "-k", "--kustomize", "--data", "--data-binary", "--patch", "--from-file", "--follow", "--overwrite",
                      "--force", "--output-directory", "--raw-data")
# every gcloud command path the tool runs (one table): only read verbs
READ_ONLY_CLOUD_COMMANDS = (
    ("auth", "list"), ("auth", "print-access-token"), ("config", "get-value"),
    ("projects", "list"), ("projects", "describe"), ("projects", "get-iam-policy"), ("organizations", "list"),
    ("asset", "search-all-resources"), ("iam", "service-accounts", "describe"),
    ("container", "clusters", "list"), ("container", "clusters", "describe"), ("container", "get-server-config"), ("container", "operations", "list"),
    ("compute", "instances", "list"), ("compute", "instance-groups", "managed", "list"), ("compute", "instance-groups", "managed", "list-errors"),
    ("compute", "networks", "describe"), ("compute", "networks", "subnets", "describe"), ("compute", "firewall-rules", "list"),
    ("compute", "routers", "list"), ("compute", "routers", "get-nat-mapping-info"), ("compute", "routes", "list"), ("compute", "addresses", "list"),
    ("compute", "forwarding-rules", "list"), ("compute", "backend-services", "list"), ("compute", "backend-services", "get-health"),
    ("compute", "packet-mirrorings", "list"), ("network-management", "connectivity-tests", "list"), ("logging", "read"),
)
LOCAL_ONLY_CLOUD_COMMANDS = (("auth", "login"), ("container", "clusters", "get-credentials"))     # local machine only (sign-in, local kubeconfig)
AUTH_LOGIN_FLAGS = ("--no-launch-browser",)             # the only flags `gcloud auth login` may carry, besides `--account X` (never --no-browser, --cred-file, --brief ...)
# `--account X` is a global flag: it only says WHICH signed-in account a read command uses. The tool adds it to every gcloud call; nothing is written.
READ_ONLY_API_URLS = (("GET", "https://monitoring.googleapis.com/v3/projects/", "/timeSeries"),         # Cloud Monitoring timeSeries.list
                      ("POST", "https://logging.googleapis.com/v2/entries:list", ""))                    # Cloud Logging entries.list (a read, sent as POST)


class _Guard:
    def __init__(self):
        self.lock = threading.Lock()
        self.allowed = 0
        self.blocked = []        # (tool, command text, reason)

    def snapshot(self):
        with self.lock:
            return self.allowed, len(self.blocked)

    def since(self, snap):
        with self.lock:
            return self.allowed - snap[0], list(self.blocked[snap[1]:])


GUARD = _Guard()


def guard_allow():
    with GUARD.lock:
        GUARD.allowed += 1


def guard_block(tool, args, reason):
    """Record a refused command (thread-safe) and return the message the caller gets instead of a result."""
    with GUARD.lock:
        GUARD.blocked.append((tool, " ".join(str(a) for a in (args or []))[:200], reason))
    return f"blocked: read-only mode - {reason}"


def kubectl_violation(args, local_ok=True):
    """None when the kubectl arguments are on the allow-list, else the reason ("'apply' is not allowed")."""
    a = [str(x) for x in (args or [])]
    i = 0
    while i < len(a) and a[i].startswith("-"):
        i += 2 if a[i] in _KUBECTL_FLAG_WITH_VALUE else 1
    if i >= len(a):
        return "'' is not allowed (no command)"
    verb, rest = a[i], a[i + 1:]
    if verb == "config":
        sub = rest[0] if rest else ""
        if sub in KUBECTL_CONFIG_READ and "--raw" not in rest:
            return None
        if sub in KUBECTL_CONFIG_LOCAL_ONLY and local_ok:
            return None
        return f"'config {sub}' is not allowed".replace("config  ", "config ")
    if verb == "auth":
        return None if (rest[:1] and rest[0] in KUBECTL_AUTH_READ) else f"'auth {rest[0] if rest else ''}' is not allowed"
    if verb not in KUBECTL_READ_VERBS:
        return f"'{verb}' is not allowed"
    if verb == "cluster-info" and rest[:1] == ["dump"]:
        return "'cluster-info dump' is not allowed"
    bad = [x for x in rest if x in _KUBECTL_BAD_FLAGS or x.startswith("--filename=") or x.startswith("--data=") or x.startswith("--patch=")]
    if bad:
        return f"'{verb} {bad[0]}' is not allowed"
    if verb == "get" and "--raw" in rest:
        j = rest.index("--raw")
        path = rest[j + 1] if j + 1 < len(rest) else ""
        if not path.startswith("/"):
            return "'get --raw' needs a path"
    return None


def gcloud_violation(args, local_ok=False):
    """None when the gcloud arguments (without the executable) are on the READ_ONLY_CLOUD_COMMANDS allow-list."""
    path = []
    rest = []
    a = [str(x) for x in (args or [])]
    i = 0
    while i < len(a):                          # the global flag --account X (or --account=X) is allowed anywhere; it is taken out first
        if a[i] == "--account":
            i += 2
            continue
        if a[i].startswith("--account="):
            i += 1
            continue
        if a[i].startswith("-") or rest:
            rest.append(a[i])
        else:
            path.append(a[i])
        i += 1
    for entry in READ_ONLY_CLOUD_COMMANDS:
        if tuple(path[:len(entry)]) == entry:
            return None
    if local_ok:
        for entry in LOCAL_ONLY_CLOUD_COMMANDS:
            if tuple(path[:len(entry)]) == entry:
                if entry == ("auth", "login"):        # exactly: auth login [--no-launch-browser] [--account X] - nothing else
                    if len(path) != 2 or any(x not in AUTH_LOGIN_FLAGS for x in rest):
                        return "'auth login' is only allowed as: gcloud auth login [--no-launch-browser] [--account EMAIL]"
                return None
    return f"'{' '.join(path[:3]) or ' '.join(str(x) for x in (args or [])[:2])}' is not allowed"


def api_violation(method, url):
    for m, prefix, must in READ_ONLY_API_URLS:
        if method == m and url.startswith(prefix) and (not must or must in url):
            return None
    return f"'{method} {str(url).split('?')[0][:80]}' is not allowed"


def _gcloud_env():
    """Environment of every non-interactive gcloud call: nothing may prompt, auto-update or install a component."""
    env = dict(os.environ, CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK="1", CLOUDSDK_CORE_DISABLE_PROMPTS="1")
    if LOGIN_OPTS.get("account"):
        env["CLOUDSDK_CORE_ACCOUNT"] = LOGIN_OPTS["account"]       # the same pin for what gcloud runs internally (nothing is written to its config)
    return env


def guard_summary_lines(delta):
    """The read-only statement for the text report: (statement, [blocked attempts])."""
    n, blocked = delta
    return (f"This tool only reads. It does not install, create, change or delete anything on the cluster or in the cloud account, and it runs nothing "
            f"inside the cluster. {n} read call(s), {len(blocked)} blocked."), blocked


# ---------------------------------------------------------------------------
# kubectl helpers
# ---------------------------------------------------------------------------

KUBE_CONTEXT = None   # set by select_context(): every kubectl call is pinned to this context


def kubectl(args, timeout=KUBECTL_TIMEOUT):
    """Run kubectl (pinned to KUBE_CONTEXT once one is selected). Returns (ok, text). Never raises. READ-ONLY: only the allow-listed verbs run."""
    why = kubectl_violation(args)
    if why:
        return False, guard_block("kubectl", args, why)          # no process is started
    exe = shutil.which("kubectl")
    if not exe:
        return False, "kubectl was not found on PATH"
    guard_allow()
    if KUBE_CONTEXT and not (args and args[0] == "config" and len(args) > 1 and args[1] in ("get-contexts", "use-context", "current-context")):
        args = ["--context", KUBE_CONTEXT, *args]
    try:
        extra = {"env": dict(os.environ, CLOUDSDK_CORE_ACCOUNT=LOGIN_OPTS["account"])} if LOGIN_OPTS.get("account") else {}   # the gke auth plugin runs gcloud with this
        proc = subprocess.run([exe, *args], capture_output=True, text=True,
                              encoding="utf-8", errors="replace", timeout=timeout, **extra)
    except subprocess.TimeoutExpired:
        return False, f"timed out after {timeout}s"
    except Exception as exc:
        return False, str(exc)
    if proc.returncode == 0:
        return True, proc.stdout.strip()
    return False, (proc.stderr or proc.stdout).strip()


def kjson(args):
    """kubectl get ... -o json. Returns (data_or_None, error_or_None). READ-ONLY (checked here and again in kubectl())."""
    why = kubectl_violation([*args, "-o", "json"])
    if why:
        return None, guard_block("kubectl", [*args, "-o", "json"], why)
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

# Terms that are used in the report and are not obvious: (full name, plain-language meaning). Every network block that uses one of them
# prints a small glossary of exactly those terms right before its tables, and the network section ends with the complete list.
GLOSSARY = {
    "API": ("Application programming interface", "The way programs talk to a service; the Kubernetes API server is what kubectl and every controller call to read and change objects."),
    "GKE": ("Google Kubernetes Engine", "Google Cloud's managed Kubernetes service."),
    "CNI": ("Container Network Interface", "The plug-in on every node that gives each pod its network interface and IP address and connects it to the rest of the cluster."),
    "DNS": ("Domain Name System", "Turns names such as my-service.my-namespace.svc into IP addresses. When it is slow or failing, applications report timeouts or 'unknown host'."),
    "MTU": ("Maximum Transmission Unit", "The largest network packet, in bytes, that a link carries without splitting it. If two networks disagree, large requests can hang while small ones work."),
    "NAT": ("Network address translation", "Rewrites private pod / node addresses to public ones for traffic that leaves the cloud network. Every connection uses one NAT port, so ports can run out."),
    "SNAT": ("Source network address translation", "Network address translation applied to the source address of outgoing traffic (also called masquerading)."),
    "CIDR": ("Classless Inter-Domain Routing notation", "A short way to write an address range, for example 10.4.0.0/14 means all addresses that start with 10.4 to 10.7."),
    "VPC": ("Virtual private cloud network", "Your private network in Google Cloud: it holds the subnets, routes and firewall rules the cluster uses."),
    "NEG": ("Network endpoint group", "A list of pod IP addresses that a Google Cloud load balancer sends traffic to directly (container-native load balancing)."),
    "Load balancer": ("Load balancer", "A service that spreads incoming traffic over several healthy backends and stops sending to unhealthy ones."),
    "TLS": ("Transport Layer Security", "The encryption (HTTPS) between client and server; it needs a valid certificate that has not expired."),
    "Dataplane V2": ("GKE Dataplane V2", "Google's networking layer for GKE based on Cilium and eBPF. It replaces kube-proxy and has network policy built in."),
    "Cilium": ("Cilium", "The open-source networking and security software (eBPF) behind GKE Dataplane V2; its node agent is called anetd."),
    "anetd": ("Dataplane V2 node agent (anetd)", "The pod on every node that runs Cilium: it wires pod networking, service load balancing and network policy on that node."),
    "netd": ("GKE node networking daemon (netd)", "The pod on every node (clusters without Dataplane V2) that sets up pod routes, masquerading and the CNI configuration."),
    "Calico": ("Calico", "A network-policy engine that GKE can use to enforce NetworkPolicy objects on clusters without Dataplane V2."),
    "ip-masq-agent": ("IP masquerade agent", "Keeps the rules that decide which pod traffic is source-address-translated (masqueraded) when it leaves the cluster."),
    "kube-proxy": ("Kubernetes network proxy", "Runs on every node and programs the rules that send traffic for a Service address to one of its pods."),
    "iptables": ("Linux iptables", "The classic Linux packet-filter rules kube-proxy writes. With many services the rule list gets long and slow to update."),
    "IPVS": ("IP Virtual Server", "A faster Linux load-balancing mode kube-proxy can use instead of iptables when there are very many services."),
    "conntrack": ("Connection tracking table", "The Linux table that remembers every active network connection. When it is full, new connections are dropped."),
    "NetworkPolicy": ("Kubernetes NetworkPolicy", "A rule that says which pods may talk to which other pods. A 'default deny' policy blocks everything that is not explicitly allowed."),
    "ClusterIP": ("Service type ClusterIP", "A Service reachable only inside the cluster through one stable virtual IP address."),
    "NodePort": ("Service type NodePort", "A Service that is also opened on one port (30000-32767) of every node, so it needs a firewall rule to be reachable."),
    "LoadBalancer": ("Service type LoadBalancer", "A Service that asks Google Cloud for a load balancer with its own external or internal IP address."),
    "Ingress": ("Kubernetes Ingress", "A rule set for HTTP(S) traffic: which host and path goes to which Service. GKE turns it into a Google Cloud HTTP(S) load balancer."),
    "BackendConfig": ("GKE BackendConfig", "A GKE setting object attached to a Service that tunes its load balancer backend: timeouts, health check, connection draining."),
    "Gateway API": ("Kubernetes Gateway API", "The newer way to describe load balancers and routes (Gateway and HTTPRoute objects)."),
    "kube-dns": ("kube-dns", "The default cluster DNS server in GKE (kube-dns pods with dnsmasq)."),
    "CoreDNS": ("CoreDNS", "A DNS server used as the cluster DNS in some clusters; configured through a Corefile."),
    "NodeLocal DNSCache": ("NodeLocal DNS cache", "A small DNS cache on every node. It answers repeated lookups locally, which lowers DNS latency and load on kube-dns."),
    "ndots": ("resolv.conf option ndots", "How many dots a name needs before it is tried as is. The Kubernetes default 5 makes many lookups try several search suffixes first."),
    "stubDomains": ("kube-dns stub domains", "Per-domain DNS servers (for example a company domain) that kube-dns forwards to."),
    "upstream nameservers": ("kube-dns upstream nameservers", "The DNS servers kube-dns asks for names that are not inside the cluster."),
    "Corefile": ("CoreDNS configuration file", "The text that tells CoreDNS which plug-ins to run and where to forward names."),
    "ContainerCreating": ("Pod state ContainerCreating", "The pod was placed on a node but its containers have not started yet, often because the network or storage set-up is stuck."),
    "FailedCreatePodSandBox": ("Event FailedCreatePodSandBox", "Kubernetes could not create the pod's network sandbox. Typical causes: no free pod IP address or a broken CNI plug-in."),
    "hostNetwork": ("Pod setting hostNetwork", "The pod uses the node's own network instead of getting its own pod IP address."),
    "NetworkUnavailable": ("Node condition NetworkUnavailable", "The node reports that its pod network is not set up yet (routes or CNI missing)."),
    "MemoryPressure": ("Node condition MemoryPressure", "The node is running out of memory and may evict pods."),
    "DiskPressure": ("Node condition DiskPressure", "The node is running out of disk space and may evict pods."),
    "PIDPressure": ("Node condition PIDPressure", "The node has too many processes running."),
    "NotReady": ("Node state NotReady", "The node's kubelet is not reporting healthy, so pods on it may be unreachable."),
    "RATE_LIMIT_EXCEEDED": ("Google Cloud API rate limit error", "Google Cloud refused calls because too many were made too quickly; load balancer or node changes can then be delayed."),
    "Private Google Access": ("Private Google Access", "Lets machines without external IP addresses reach Google APIs over Google's internal network."),
    "VPC peering": ("VPC network peering", "A private link between two virtual private cloud networks; GKE uses it between the control plane and a private cluster."),
    "Authorized networks": ("Control plane authorized networks", "The list of IP ranges that may reach the Kubernetes API server."),
    "Health check ranges": ("Google health check source ranges", "130.211.0.0/22 and 35.191.0.0/16: Google load balancer health checks come from here, so the firewall must allow them."),
    "OUT_OF_RESOURCES": ("Cloud NAT drop reason OUT_OF_RESOURCES", "Packets were dropped because the NAT gateway had no free ports or addresses left."),
    "5xx": ("HTTP status 500-599", "Server error responses. A rising share means the backends are failing."),
    "p95 latency": ("95th percentile latency", "95 out of 100 requests were faster than this time; it shows slow requests that an average hides."),
    "502/503/504": ("HTTP status 502, 503 and 504", "Bad gateway, service unavailable and gateway timeout: typical load balancer or ingress errors when backends are down or slow."),
    "Hubble": ("Hubble", "The Cilium observability tool that shows which pod talked to which pod and whether the traffic was allowed or dropped."),
    "VPC Flow Logs": ("Virtual private cloud flow logs", "Samples of network connections (source, destination, bytes) written to Cloud Logging for a subnet."),
    "Connectivity Tests": ("Network Intelligence Center Connectivity Tests", "A Google Cloud tool that checks, without sending traffic, whether a path between two endpoints is allowed by routes and firewalls."),
    "Packet Mirroring": ("Packet Mirroring", "A Google Cloud feature that copies the packets of selected machines to a collector for analysis."),
    "tcpdump": ("tcpdump", "A command-line tool that records the network packets on a machine."),
    "Admission webhook": ("Kubernetes admission webhook", "A service the API server calls before it stores an object; if it is down or slow, creating or changing objects can fail."),
    "failurePolicy": ("Webhook failurePolicy", "What the API server does when the webhook does not answer: Fail rejects the request, Ignore lets it through."),
    "etcd": ("etcd", "The database that stores all Kubernetes objects. On GKE it is managed by Google and normally not visible."),
    "HTTP 429": ("HTTP status 429 Too Many Requests", "The API server refused a call because the caller sent too many requests (throttling)."),
    "Priority and fairness": ("API Priority and Fairness", "The API server's queueing system; requests it rejects are counted by apiserver_flowcontrol_rejected_requests_total."),
    "DaemonSet": ("Kubernetes DaemonSet", "A workload that runs exactly one pod on every node, used for node agents such as networking."),
    "kubelet": ("Kubelet", "The agent on each node that starts pods and reports the node's health and statistics."),
    "Secondary range": ("Subnet secondary IP range", "An extra address range of a subnet; GKE takes pod addresses and service addresses from secondary ranges."),
    "API server": ("Kubernetes API server", "The control plane component that stores and serves every Kubernetes object; kubectl and all controllers talk to it."),
    "Network interface": ("Network interface counters", "Packets and bytes counted by the node's network card; errors or drops here point at the node or the network under it."),
    "IAM": ("Identity and Access Management", "Google Cloud's permission system: who (a user or service account) may do what (a role) on which resource."),
    "Service account": ("Service account", "A non-human identity that programs and virtual machines use to call Google Cloud; the nodes run as one and get their permissions from its roles."),
    "Role": ("IAM role", "A named set of permissions, for example roles/logging.logWriter. roles/editor and roles/owner are very broad and too much for a node."),
    "Compute Engine": ("Google Compute Engine", "Google Cloud's virtual machine service; every GKE node is a Compute Engine virtual machine."),
    "Managed instance group": ("Managed instance group", "A group of identical virtual machines kept at a target size by Google Cloud; each GKE node pool is made of one per zone."),
    "Node pool": ("GKE node pool", "A group of nodes with the same machine type, disk and settings, resized and upgraded together."),
    "Control plane": ("Kubernetes control plane", "The managed part of the cluster (API server, scheduler, controllers) that Google runs for you; it is not one of your nodes."),
    "Autopilot": ("GKE Autopilot mode", "A GKE mode where Google manages the nodes for you; in Standard mode you manage the node pools."),
    "Spot": ("Spot virtual machine", "A cheaper virtual machine that Google Cloud can stop at any time."),
    "Preemptible": ("Preemptible virtual machine", "An older kind of cheap virtual machine that Google Cloud stops after at most 24 hours or when it needs the capacity."),
    "Release channel": ("GKE release channel", "Rapid, Regular or Stable: how quickly the cluster receives new Kubernetes versions."),
    "Workload Identity": ("GKE Workload Identity", "Lets a pod use its own Google Cloud identity instead of the node's service account."),
    "Shielded nodes": ("Shielded GKE nodes", "Nodes with verified boot and integrity checks that protect against tampered node images."),
    "Binary Authorization": ("Binary Authorization", "A control that only allows container images that were signed or approved to run in the cluster."),
    "ABAC": ("Attribute-based access control (legacy)", "An old authorization mode for Kubernetes; it grants broad rights and should be switched off in favour of RBAC."),
    "RBAC": ("Role-based access control", "The Kubernetes permission system: roles say what is allowed and bindings give them to users and service accounts."),
    "VPC-native": ("VPC-native cluster (alias IP ranges)", "A cluster whose pods get addresses from a subnet range of the virtual private cloud network, instead of custom routes."),
    "Cloud Logging": ("Google Cloud Logging", "The Google Cloud service that stores logs of the cluster and its control plane."),
    "Cloud Monitoring": ("Google Cloud Monitoring", "The Google Cloud service that stores metrics (numbers over time) such as network traffic and load balancer requests."),
    "Managed Prometheus": ("Google Cloud Managed Service for Prometheus", "A managed way to collect Prometheus metrics from the cluster into Cloud Monitoring."),
    "Cluster autoscaler": ("GKE cluster autoscaler", "Adds nodes when pods cannot be scheduled and removes nodes that are not needed."),
    "Cloud NAT": ("Cloud NAT", "Google Cloud's network address translation service that gives machines without external IP addresses access to the internet."),
    "Private cluster": ("Private cluster", "A cluster whose nodes have no external IP addresses and whose control plane can be limited to private access."),
    "kubectl": ("Kubernetes command-line tool", "The program this tool uses (read only) to read objects and usage from the cluster."),
    "gcloud": ("Google Cloud command-line tool", "The program this tool uses (read only) to read the Google Cloud side of the cluster."),
    "metrics-server": ("Kubernetes metrics server", "A cluster component that measures the processor and memory use of nodes and pods; used when the kubelet statistics cannot be read."),
    "Request": ("Resource request", "The amount of processor or memory a container asks for; the scheduler reserves it on a node."),
    "Limit": ("Resource limit", "The most processor or memory a container may use; above the memory limit it is killed, above the processor limit it is slowed down."),
    "Allocatable": ("Allocatable capacity", "What a node can give to pods after the share kept for the system and Kubernetes itself."),
    "Working set": ("Memory working set", "The memory a process actively uses and that cannot be taken away without hurting it; this is the memory number shown for pods."),
    "Swap": ("Swap space", "Disk space used as slow extra memory; heavy swap use means the node is short of memory."),
    "Ephemeral storage": ("Ephemeral storage", "Temporary disk space of a pod (logs, caches, writable layers) that disappears when the pod goes away."),
    "Image filesystem": ("Image filesystem (imagefs)", "The disk area where container images and writable layers are stored; when it is full, the node cannot start new pods."),
    "Namespace": ("Kubernetes namespace", "A named area of the cluster that groups related objects, usually one per team or application."),
    "Pod": ("Kubernetes pod", "The smallest unit that runs: one or more containers sharing one IP address."),
    "Deployment": ("Kubernetes Deployment", "A workload that keeps a number of identical pods running and rolls out new versions."),
    "StatefulSet": ("Kubernetes StatefulSet", "A workload for pods that need a stable name and their own storage, such as databases."),
    "ReplicaSet": ("Kubernetes ReplicaSet", "The object created by a Deployment that keeps the wanted number of pod copies; a new one appears for each rollout."),
    "Job": ("Kubernetes Job", "A workload that runs pods to completion once, such as a batch task."),
    "Horizontal pod autoscaler": ("Horizontal pod autoscaler (HPA)", "Adds or removes pod copies of a workload depending on its load; it cannot add more than its maximum."),
    "Persistent volume claim": ("Persistent volume claim (PVC)", "A pod's request for storage; it must be Bound to a persistent volume before the pod can start."),
    "Persistent volume": ("Persistent volume (PV)", "A piece of storage in the cluster (for example a Google Cloud disk) that a claim is bound to."),
    "StorageClass": ("Kubernetes StorageClass", "Describes the kind of storage (for example SSD disk) that is created when a claim asks for it."),
    "ResourceQuota": ("Kubernetes ResourceQuota", "A limit on how many pods or how much processor, memory or storage a namespace may use in total."),
    "Distribution list": ("Support distribution list", "The e-mail group of the team that supports a namespace, read from the namespace label named in the table."),
    "CrashLoopBackOff": ("Pod state CrashLoopBackOff", "The container keeps crashing and Kubernetes waits longer and longer before starting it again."),
    "ImagePullBackOff": ("Pod state ImagePullBackOff / ErrImagePull", "The container image could not be downloaded: wrong name or tag, missing permission or the registry cannot be reached."),
    "OOMKilled": ("Out-of-memory (OOM) kill", "The container used more memory than its limit, or the node ran out of memory, and the system stopped it."),
    "Pending": ("Pod phase Pending", "The pod exists but is not running yet: it is waiting for a node, a volume or an image."),
    "Evicted": ("Pod state Evicted", "The node removed the pod because it was short of memory, disk or other resources."),
    "Terminating": ("Namespace or pod state Terminating", "Deletion was requested but not finished; it can hang when something blocks it."),
    "Ready": ("Ready state", "A node or pod that passes its health check and can take work or traffic; 'ready' in a count means ready out of desired."),
    "kube-system": ("kube-system namespace", "The namespace where Kubernetes and GKE keep their own add-on pods such as DNS, networking agents and the metrics server."),
    "konnectivity-agent": ("Konnectivity agent", "Keeps a secure tunnel from the managed control plane to the nodes so that logs, exec and webhooks work."),
    "gke-metadata-server": ("GKE metadata server", "The pod on each node that gives pods their Workload Identity credentials."),
    "CSI": ("Container Storage Interface driver", "The plug-in that creates, attaches and mounts storage such as Google Cloud persistent disks for pods."),
    "Service agent": ("GKE service agent", "A Google-managed service account that GKE itself uses to create and change nodes, disks and load balancers in your project."),
    "Stockout": ("Zone stockout", "Google Cloud has no free machines of the requested type in that zone, so new nodes cannot be created."),
    "Quota": ("Google Cloud quota", "A limit on how many resources (machines, addresses, processors) a project may use in a region."),
    "Instance identifier": ("Compute Engine instance identifier (ID)", "The number that uniquely names a virtual machine in Google Cloud; it identifies the machine behind a node even when names are reused."),
    "Provider identifier": ("Kubernetes node provider identifier (ID)", "The text gce://project/zone/name stored on a node that tells which Google Cloud virtual machine it is."),
    "Warning event": ("Kubernetes Warning event", "A record Kubernetes writes when something went wrong, for example a pod that could not start or a probe that failed. Kubernetes keeps events for about one hour."),
    "Authorized networks (control plane)": ("Control plane authorized networks", "The list of address ranges allowed to reach the Kubernetes API server; when it is off the API server is open to the internet and protected only by login."),
}


MISSING_ABOUT = []      # (kind, title or headers) of every table / block created WITHOUT an `about`; the tests fail when this is not empty
_MISSING_LOCK = threading.Lock()


def _missing_about(kind, what):
    with _MISSING_LOCK:
        MISSING_ABOUT.append((kind, what))


def _default_about(headers):
    return "One row per item; the columns are " + ", ".join(h.lower() for h in headers[:6]) + (", ..." if len(headers) > 6 else "") + "."


# Full words for column headers: an exact match first, then word by word. The tables always show the result.
_HEADER_EXACT = {
    "SUPPORT DL": "SUPPORT DISTRIBUTION LIST", "K8S VERSION": "KUBERNETES VERSION", "MAX PODS/NODE": "MAXIMUM PODS PER NODE",
    "CIDR": "ADDRESS RANGE", "FREE (est.)": "FREE (ESTIMATED)", "SOURCE / DEST": "SOURCE OR DESTINATION",
    "NAT IPs": "NETWORK ADDRESS TRANSLATION IP ADDRESSES", "MIN PORTS/VM": "MINIMUM PORTS PER VIRTUAL MACHINE",
    "ENDPOINT-INDEP.": "ENDPOINT-INDEPENDENT MAPPING", "NAT": "NETWORK ADDRESS TRANSLATION GATEWAY",
    "CPUreq": "PROCESSOR (CPU) REQUESTED", "MEMreq": "MEMORY REQUESTED", "CPU %cl": "PROCESSOR (CPU) % OF CLUSTER", "MEM %cl": "MEMORY % OF CLUSTER",
    "CPU used/alloc": "PROCESSOR (CPU) USED / ALLOCATABLE", "MEMORY used/alloc": "MEMORY USED / ALLOCATABLE", "DISK used/total": "DISK USED / TOTAL",
    "HPA min-max": "HORIZONTAL POD AUTOSCALER MINIMUM-MAXIMUM REPLICAS", "HPA": "HORIZONTAL POD AUTOSCALER", "RST": "RESTARTS", "ERRORS": "ERROR COUNT",
    "TLS": "TRANSPORT LAYER SECURITY", "IP": "IP ADDRESS", "": "DETAIL", "PROVIDER-ID": "PROVIDER IDENTIFIER", "INSTANCE ID": "INSTANCE IDENTIFIER",
    "GCE INSTANCE": "COMPUTE ENGINE INSTANCE", "STORAGECLASS": "STORAGE CLASS", "EPHEMERAL": "EPHEMERAL STORAGE REQUESTED / ALLOCATABLE",
    "IMAGEFS": "IMAGE FILESYSTEM USED / TOTAL", "REPLICASET": "REPLICA SET",
}
_HEADER_WORD = {
    "DL": "DISTRIBUTION LIST", "CPU": "PROCESSOR (CPU)", "MEM": "MEMORY", "PDB": "POD DISRUPTION BUDGET", "RS": "REPLICA SET", "TTL": "TIME TO LIVE",
    "OOM": "OUT-OF-MEMORY (OOM)", "GW": "GATEWAY", "SVC": "SERVICE", "SG": "SECURITY GROUP", "PV": "PERSISTENT VOLUME", "use": "USED", "req": "REQUESTED", "lim": "LIMIT", "alloc": "ALLOCATABLE",
    "IMAGEFS": "IMAGE FILESYSTEM", "RX": "BYTES RECEIVED", "TX": "BYTES TRANSMITTED", "VPC": "VIRTUAL PRIVATE CLOUD NETWORK",
    "NEG": "NETWORK ENDPOINT GROUP", "LB": "LOAD BALANCER", "NS": "NAMESPACE", "SVC": "SERVICE", "MIG": "MANAGED INSTANCE GROUP",
    "SA": "SERVICE ACCOUNT", "IPs": "IP ADDRESSES", "VM": "VIRTUAL MACHINE", "VMs": "VIRTUAL MACHINES", "K8S": "KUBERNETES",
    "GCE": "COMPUTE ENGINE", "ID": "IDENTIFIER", "HPA": "HORIZONTAL POD AUTOSCALER", "DNS": "DOMAIN NAME SYSTEM", "CNI": "CONTAINER NETWORK INTERFACE",
    "MTU": "MAXIMUM TRANSMISSION UNIT", "SNAT": "SOURCE NETWORK ADDRESS TRANSLATION", "PVC": "PERSISTENT VOLUME CLAIM", "NAT": "NETWORK ADDRESS TRANSLATION",
}


def _full_header(h):
    h = str(h)
    if h in _HEADER_EXACT:
        return _HEADER_EXACT[h]
    h = re.sub(r"(?<![(A-Za-z0-9])[A-Za-z][A-Za-z0-9]*", lambda m: _HEADER_WORD.get(m.group(0), m.group(0)), h)
    return re.sub(r"\bIP\b(?!\s+ADDRESS)", "IP ADDRESS", h)


# ---------------------------------------------------------------------------
# Parallel collection engine (used after the login; --workers 1 gives the old one-after-another order)
#   * sections are tasks that run concurrently when their data dependencies allow; every task writes into its OWN sub-report and
#     findings sink, and the results are merged in the fixed report order - so the report is the same whatever finishes first
#   * inside heavy sections independent blocks / calls run concurrently too (run_blocks, pf, pmap)
#   * semaphores cap the calls actually running at the same time: kubectl KUBECTL_CONCURRENCY, gcloud / Google API GCLOUD_CONCURRENCY
# ---------------------------------------------------------------------------

_TLS = threading.local()        # per worker thread: the sub-report ('rep') and findings sink ('sink') the running task writes to
_EMIT_LOCK = threading.RLock()  # one line at a time reaches the window / console
_RUN = None                     # the _Run of the collection in progress (None outside run_debug)


class _Sink:
    """What one collection task found. Kept apart until the tasks are merged in report order."""

    def __init__(self, silent=False):
        self.findings, self.full, self.timeline, self.ns, self.checks = [], [], [], [], []
        self.silent = silent

    def absorb(self, other):
        self.findings += other.findings
        self.full += other.full
        self.timeline += other.timeline
        self.ns += other.ns
        self.checks += other.checks


class _Done:
    """A finished call with the same .result() as a Future (used when nothing runs in parallel)."""

    def __init__(self, value=None, exc=None):
        self.value, self.exc = value, exc

    def result(self, timeout=None):
        if self.exc is not None:
            raise self.exc
        return self.value

    def done(self):
        return True

    def cancel(self):
        return False


class _Run:
    """The state of one run_debug: worker pools, call limits, the 'x of y tasks done' counter and the Stop flag."""

    def __init__(self, workers, cancel=None, on_tasks=None):
        self.workers = max(1, int(workers or 1))
        self.cancel = cancel
        self.kube = threading.Semaphore(KUBECTL_CONCURRENCY)
        self.cloud = threading.Semaphore(GCLOUD_CONCURRENCY)
        self.lock = threading.Lock()
        self.total = self.done = 0
        self.on_tasks = on_tasks
        self._last = 0.0
        self._pools = {}
        self.inflight = {"kube": 0, "cloud": 0}
        self.peak = {"kube": 0, "cloud": 0}
        self.calls = {"kube": 0, "cloud": 0}

    def parallel(self):
        return self.workers > 1

    def stopped(self):
        return self.cancel is not None and self.cancel.is_set()

    def pool(self, name):
        with self.lock:
            if name not in self._pools:
                size = {"section": self.workers, "block": max(self.workers, 4), "leaf": KUBECTL_CONCURRENCY + GCLOUD_CONCURRENCY + 2}[name]
                self._pools[name] = ThreadPoolExecutor(max_workers=size, thread_name_prefix="gke-" + name)
            return self._pools[name]

    def task_added(self, n=1):
        with self.lock:
            self.total += n
        self._report()

    def task_done(self, n=1):
        with self.lock:
            self.done += n
        self._report()

    def _report(self, force=False):
        if not self.on_tasks:
            return
        now = time.time()
        if force or now - self._last >= 0.15 or self.done >= self.total:
            self._last = now
            try:
                self.on_tasks(self.done, self.total)
            except Exception:
                pass

    def shutdown(self, cancel_pending=False):
        for pool in list(self._pools.values()):
            try:
                pool.shutdown(wait=not cancel_pending, **({"cancel_futures": True} if cancel_pending else {}))
            except Exception:
                pass
        self._pools.clear()


def _limited(fn, kind):
    """Wrap a call (kubectl / gcloud / gcp_api, or whatever replaced it) with the concurrency cap and the Stop check."""
    if getattr(fn, "_is_limit", False):
        return fn
    refusal = (False, "cancelled") if kind == "kube" else (None, "cancelled")

    def limited(*a, **k):
        run = _RUN
        if run is None or not run.parallel():
            return fn(*a, **k)
        held = getattr(_TLS, "held", None)
        if held is None:
            held = _TLS.held = {"kube": 0, "cloud": 0}
        if held[kind]:                       # gcp_api -> gcloud (token) inside one call: the slot is already ours
            return fn(*a, **k)
        if run.stopped():
            return refusal
        sem = run.kube if kind == "kube" else run.cloud
        with sem:
            if run.stopped():
                return refusal
            held[kind] += 1
            with run.lock:
                run.inflight[kind] += 1
                run.calls[kind] += 1
                run.peak[kind] = max(run.peak[kind], run.inflight[kind])
            try:
                return fn(*a, **k)
            finally:
                held[kind] -= 1
                with run.lock:
                    run.inflight[kind] -= 1
    limited._is_limit = True
    limited.__wrapped__ = fn
    limited.__name__ = getattr(fn, "__name__", "limited")
    return limited


class _limits:
    """with _limits(run): the module's kubectl / gcloud / gcp_api are capped for the duration of one collection."""

    def __init__(self, run):
        self.run = run

    def __enter__(self):
        global _RUN, kubectl, gcloud, gcp_api
        self.saved = (kubectl, gcloud, gcp_api)
        _RUN = self.run
        if self.run.parallel():
            kubectl, gcloud, gcp_api = _limited(kubectl, "kube"), _limited(gcloud, "cloud"), _limited(gcp_api, "cloud")
        return self.run

    def __exit__(self, *exc):
        global _RUN, kubectl, gcloud, gcp_api
        self.run.shutdown(cancel_pending=self.run.stopped())
        cur = (kubectl, gcloud, gcp_api)
        kubectl, gcloud, gcp_api = tuple(old if getattr(now, "_is_limit", False) else now for old, now in zip(self.saved, cur))
        _RUN = None
        return False


def _parallel():
    return _RUN is not None and _RUN.parallel()


def pf(fn, *args, **kwargs):
    """Start a read-only call now and read its result later with .result(). With one worker it simply runs the call at once."""
    run = _RUN
    if run is None or not run.parallel() or run.stopped():
        try:
            return _Done(fn(*args, **kwargs))
        except Exception as exc:
            return _Done(exc=exc)
    run.task_added()

    def call():
        try:
            return fn(*args, **kwargs)
        finally:
            run.task_done()
    try:
        return run.pool("leaf").submit(call)
    except RuntimeError:                 # the pool was shut down by Stop
        run.task_done()
        return _Done(exc=RuntimeError("cancelled"))


def pmap(fn, seq):
    """[fn(x) for x in seq] with the calls running concurrently; the results keep the order of seq."""
    futs = [pf(fn, x) for x in seq]
    return [f.result() for f in futs]


def run_blocks(rep, ctx, blocks, first=()):
    """Run independent report blocks (callables taking a Report). With several workers they run concurrently, each into its own sub-report;
    their text, findings and checks are merged in the ORDER OF THE LIST, so the report does not depend on which block finishes first.
    `first`: indexes started first (the slow ones); it changes the start order only, never the report order."""
    run = _RUN
    if run is None or not run.parallel() or len(blocks) < 2:
        for blk in blocks:
            blk(rep)
        return
    parent_sink = getattr(_TLS, "sink", None)
    kids = [rep.child() for _ in blocks]
    sinks = [_Sink(silent=bool(parent_sink and parent_sink.silent)) for _ in blocks]

    def work(i):
        saved = (getattr(_TLS, "rep", None), getattr(_TLS, "sink", None))
        _TLS.rep, _TLS.sink = kids[i], sinks[i]
        try:
            if not run.stopped():
                blocks[i](kids[i])
        except Exception as exc:
            kids[i].add(f"[!] block failed: {exc}")
        finally:
            _TLS.rep, _TLS.sink = saved
            run.task_done()
    start = list(first) + [i for i in range(len(blocks)) if i not in set(first)]
    run.task_added(len(blocks))
    futs = []
    for i in start:
        try:
            futs.append(run.pool("block").submit(work, i))
        except RuntimeError:
            run.task_done()
    for f in futs:
        f.result()
    for i in range(len(blocks)):
        rep.merge(kids[i])
        if parent_sink is not None:
            parent_sink.absorb(sinks[i])
        else:
            ctx.commit(sinks[i])


def timing_lines(timing):
    """The timing summary (console / window log): total wall time, the sum of the steps and what that means."""
    if not timing:
        return []
    total, summed, workers = timing["total"], timing["sum"], timing["workers"]
    speed = (summed / total) if total > 0 else 1.0
    lines = [f"Timing summary: collected in {total:.1f}s with {workers} worker(s); the steps add up to {summed:.1f}s"
             + (f" ({speed:.1f}x faster than one after another)" if workers > 1 and speed >= 1.05 else "") + "."]
    lines.append("  " + "   ".join(f"{t}: {s}" for t, s in timing["steps"] if s not in ("-", "")))
    return lines


class Report:
    """Collects the report as text lines (streamed to `emit`) AND as structured sections /
    blocks (tables, logs, timeline) that the interactive HTML report is built from."""

    STATES = ("OK", "Warning", "Problem", "Not available")

    def __init__(self, emit, buffered=False):
        self.lines = []
        self.emit = emit               # the live line sink (window / console); progress-only lines (rep.emit) go straight to it
        self.buffered = buffered       # True (a sub-report of a parallel task): lines are kept and reach `emit` when the parent merges them
        self.lock = threading.RLock()
        self.sections = [{"id": "s0", "title": "Run log", "blocks": []}]
        self.current = self.sections[0]
        self.next_id = None            # a sub-report of one section is given that section's id up front (s3 ...), so ids never depend on timing
        self.minutes = None            # the length of the time window (named in the section introductions)
        self.used_terms = []           # glossary terms printed so far in the current section (for the complete glossary at its end)

    def child(self, buffered=None, section_id=None):
        """A sub-report for one parallel task. Its text and blocks are put into this report, in order, with merge()."""
        kid = Report(self.emit, buffered=_parallel() if buffered is None else buffered)
        kid.sections = [{"id": self.current["id"], "title": self.current["title"], "blocks": []}]
        kid.current = kid.sections[0]
        kid.minutes = self.minutes
        kid.used_terms = list(self.used_terms)
        kid.next_id = section_id
        return kid

    def merge(self, kid):
        """Append a finished sub-report: its lines (streamed to `emit` now when they were buffered), its blocks and its sections."""
        with self.lock:
            for line in kid.lines:
                self.lines.append(line)
                if kid.buffered:
                    with _EMIT_LOCK:
                        self.emit(line)
            first, rest = kid.sections[0], kid.sections[1:]
            for blk in first["blocks"]:
                self._block(*blk)
            if rest:
                self.sections.extend(rest)
                self.current = rest[-1]
                self.used_terms = list(kid.used_terms)
            else:
                self.used_terms += [t for t in kid.used_terms if t not in self.used_terms]

    def skipped(self, num, title):
        """A section that was not collected by choice: one line in the text, one small entry in the HTML table of contents."""
        with self.lock:
            line = f"Skipped by choice: {num}. {title}"
            self._text("")
            self._text(line)
            sec = {"id": f"s{num}", "title": f"{num}. {title} (skipped by choice)", "blocks": [("lines", [line])], "skipped": True}
            self.sections.append(sec)
            self.current = sec

    def _text(self, line):
        with self.lock:
            self.lines.append(line)
            if not self.buffered:
                with _EMIT_LOCK:
                    self.emit(line)

    def _block(self, kind, *payload):
        with self.lock:
            blocks = self.current["blocks"]
            if kind == "lines":
                if blocks and blocks[-1][0] == "lines":
                    blocks[-1][1].extend(payload[0] if isinstance(payload[0], list) else [payload[0]])
                else:
                    blocks.append(("lines", list(payload[0]) if isinstance(payload[0], list) else [payload[0]]))
            else:
                blocks.append((kind, *payload))

    def add(self, text=""):
        with self.lock:
            for line in str(text).splitlines() or [""]:
                self._text(line)
                self._block("lines", line)

    def section(self, title):
        with self.lock:
            self._text("")
            self._text("=" * 78)
            self._text(title)
            self._text("=" * 78)
            self.current = {"id": self.next_id or f"s{len(self.sections)}", "title": title, "blocks": []}
            self.sections.append(self.current)
            self.used_terms = []
            num = re.match(r"\s*(\d+)\.", title)
            reg = next((x for x in SECTIONS if num and x["num"] == int(num.group(1))), None)
            if reg is not None:       # 'What this section shows' (+ how to use it): one box in the HTML, two lines in the text
                shows, how = section_text(reg["id"], self.minutes if self.minutes is not None else LOOKBACK_MINUTES)
                self._text(f"What this section shows: {shows}")
                how_txt = how.replace(_TABLE_HINT, "").strip()
                if how_txt:
                    self._text(f"How to use it: {how_txt}")
                self._block("intro", shows, how)

    def sub(self, title, about=None, terms=None):
        """A titled block inside a section: heading, one plain line 'what this shows', and (before anything else of the block) the
        glossary of the `terms` the block uses."""
        self._text("")
        self._text(f"--- {title} ---")
        if not about:
            _missing_about("block", title)
        if about:
            self._text(f"  What this block shows: {about}")
        self._block("sub", title, about)
        if terms:
            self.glossary(terms)

    def glossary(self, terms, title="Glossary: what these terms mean"):
        rows = [[t, GLOSSARY[t][0], GLOSSARY[t][1]] for t in terms if t in GLOSSARY]
        if not rows:
            return
        for t in terms:
            if t in GLOSSARY and t not in self.used_terms:
                self.used_terms.append(t)
        about = "the short terms and technical words used in the block below, spelled out and explained in plain language."
        self._text(f"  {title}")
        self._text(f"  What this table shows: {about}")
        self._print_table(["TERM", "FULL NAME", "PLAIN-LANGUAGE MEANING"], rows, len(rows), 200)
        self._block("glossary", title, ["TERM", "FULL NAME", "PLAIN-LANGUAGE MEANING"], rows, about)

    def status(self, state, evidence, meaning):
        """A check result: state is OK / Warning / Problem / Not available, with the evidence found and what to do next."""
        assert state in self.STATES, state
        self._text(f"  Status: {state} - {evidence}")
        self._text(f"  What this means / what to do next: {meaning}")
        self._block("status", state, evidence, meaning)

    def _print_table(self, headers, rows, limit, maxw):
        shown = rows[:limit]
        widths = [min(maxw, max([len(h)] + [len(r[i]) for r in shown])) for i, h in enumerate(headers)]

        def cell(text, w):
            return text if len(text) <= w else text[: w - 1] + "~"

        self._text("  ".join(h.ljust(w) for h, w in zip(headers, widths)))
        for r in shown:
            self._text("  ".join(cell(c, w).ljust(w) for c, w in zip(r, widths)).rstrip())
        if len(rows) > limit:
            self._text(f"... and {len(rows) - limit} more")

    def table(self, headers, rows, limit=MAX_ROWS, maxw=58, about=None, terms=None):
        """A table with 'What this table shows' (`about`, required: a missing one is recorded in MISSING_ABOUT and the tests fail) and,
        right before it, a glossary of the `terms` its cells use."""
        if not rows:
            return
        headers = [_full_header(h) for h in headers]
        rows = [["-" if c is None else str(c) for c in r] for r in rows]
        if terms:
            self.glossary(terms)
        if not about:
            _missing_about("table", tuple(headers))
            about = _default_about(headers)
        if about:
            self._text(f"  What this table shows: {about}")
        self._print_table(headers, rows, limit, maxw)
        self._block("table", list(headers), rows, about)   # the HTML report keeps ALL rows

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

    def util(self, data, about=None):
        """Structured utilization data: rendered as the interactive dashboard in the HTML only."""
        if not about:
            _missing_about("block", "utilization dashboard")
        self._block("util", data, about)

    def series(self, title, rows, note="", about=None):
        """Time series (sparkline charts) - rendered in the HTML only; the numbers are printed as tables by the caller."""
        if rows:
            if not about:
                _missing_about("block", title)
            self._block("series", title, rows, note, about)

    def timeline(self, entries, about=None):
        """entries: [(datetime, text)]"""
        if not about:
            _missing_about("block", "timeline")
        if about:
            self._text(f"  What this block shows: {about}")
        for ts, text in entries:
            self._text(f"{ts:%H:%M:%S}Z  {text}")
        self._block("timeline", [(f"{ts:%H:%M:%S}", text) for ts, text in entries], about)


def _locked(fn):
    def wrapper(self, *a, **k):
        with self.lock:
            return fn(self, *a, **k)
    wrapper.__name__, wrapper.__doc__ = fn.__name__, fn.__doc__
    return wrapper


for _name in ("sub", "glossary", "status", "table", "log", "util", "series", "timeline"):    # one writer at a time, also for the compound calls
    setattr(Report, _name, _locked(getattr(Report, _name)))


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
        self._report = None
        self.cancel = None        # threading.Event: set by the Stop button
        self.on_finding = None    # optional callback(severity, text) - the GUI shows findings live
        self.lock = threading.RLock()
        self._fill = {}

    @property
    def report(self):
        """The report the CURRENT thread writes to: a parallel task's own sub-report, else the main one."""
        return getattr(_TLS, "rep", None) or self._report

    @report.setter
    def report(self, value):
        self._report = value

    def cached(self, key, fn):
        """ctx.data[key], computed once (by `fn`) even when several tasks ask for it at the same time."""
        with self.lock:
            lock = self._fill.setdefault(key, threading.Lock())
        with lock:
            if key not in self.data:
                self.data[key] = fn()
            return self.data[key]

    def find(self, severity, text):
        rep = self.report
        section_id = rep.current["id"] if rep else "s0"
        sink = getattr(_TLS, "sink", None)
        if sink is None:
            with self.lock:
                self.findings.append((severity, text))
                self.findings_full.append((severity, text, section_id))
        else:
            sink.findings.append((severity, text))
            sink.full.append((severity, text, section_id))
            if sink.silent:
                return                 # collected only because another section needs its data: not shown, not counted
        if self.on_finding:
            try:
                self.on_finding(severity, text)
            except Exception:
                pass

    def ns_issue(self, ns, text):
        sink = getattr(_TLS, "sink", None)
        if sink is not None:
            if ns:
                sink.ns.append((ns, text))
            return
        self._ns_issue(ns, text)

    def _ns_issue(self, ns, text):
        with self.lock:
            if ns and text not in self.ns_issues[ns]:
                self.ns_issues[ns].append(text)

    def happened(self, ts, text):
        if ts and ts >= self.since:
            sink = getattr(_TLS, "sink", None)
            if sink is not None:
                sink.timeline.append((ts, text))
            else:
                with self.lock:
                    self.timeline.append((ts, text))

    def add_check(self, key, state, evidence):
        """Remember the result of a network check for the traffic issue checklist."""
        sink = getattr(_TLS, "sink", None)
        if sink is not None:
            sink.checks.append((key, state, evidence))
        else:
            with self.lock:
                self.data.setdefault("net_checks", {}).setdefault(key, []).append((state, evidence))

    def collected_checks(self):
        """The network checks recorded so far in this section, in the order of the report blocks."""
        with self.lock:
            out = {k: list(v) for k, v in (self.data.get("net_checks") or {}).items()}
        sink = getattr(_TLS, "sink", None)
        for key, state, evidence in (sink.checks if sink else []):
            out.setdefault(key, []).append((state, evidence))
        return out

    def commit(self, sink):
        """Add what a finished task found (called in report order)."""
        with self.lock:
            self.findings += sink.findings
            self.findings_full += sink.full
            self.timeline += sink.timeline
            for ns, text in sink.ns:
                self._ns_issue(ns, text)
            for key, state, evidence in sink.checks:
                self.data.setdefault("net_checks", {}).setdefault(key, []).append((state, evidence))


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


def load_data(ctx, rep, usage=True):
    """Read every resource the sections work from (one kubectl call each, several at once) and, when a ticked section needs it, the live
    usage of the nodes and pods (`usage`: kubelet stats + metrics-server)."""
    rep.add("Collecting cluster data ...")
    errors = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {name: pool.submit(kjson, args) for name, args in RESOURCES.items()}
        top_f = ({"nodes": pool.submit(kubectl, ["top", "nodes", "--no-headers"]), "pods": pool.submit(kubectl, ["top", "pods", "-A", "--no-headers"])}
                 if usage else None)          # metrics-server reads start together with the resource reads
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
    if usage:
        rep.add("Collecting live CPU / memory / disk / swap usage ...")
        fetch_usage(ctx, rep, top_f)
    else:
        ctx.data["node_stats"], ctx.data["stats_error"] = {}, None
        ctx.data["top_nodes"], ctx.data["top_pods"], ctx.data["top_error"], ctx.data["pod_usage"] = {}, {}, None, {}


# ---------------------------------------------------------------------------
# GCP side of GKE (read-only: `gcloud ... list / describe / get-*`, Cloud Logging reads, Cloud Monitoring reads)
# ---------------------------------------------------------------------------

GCP_OPTS = {"enabled": True, "cluster": None, "location": None, "project": None,   # "project" = the one you asked for
            "project_used": None, "project_reason": None, "target": None, "cluster_info": None}
MAX_PROJECT_TRIES = 3
MAX_CP_LOG_LINES = 30        # control-plane error lines shown
LOW_SUBNET_IPS = 50          # warn when a cluster subnet has fewer free IPs than this
LOGGING_COMPONENTS = ["SYSTEM_COMPONENTS", "WORKLOADS", "APISERVER", "SCHEDULER", "CONTROLLER_MANAGER"]
DEFAULT_SA = re.compile(r"^\d+-compute@developer\.gserviceaccount\.com$")
_TOKEN = {"value": None, "at": 0.0, "account": None}
_TOKEN_LOCK = threading.Lock()
_API_ALLOWED = ("https://monitoring.googleapis.com/v3/", "https://logging.googleapis.com/v2/entries:list")


def gcloud(args, target=None, timeout=90, project=True, fmt="json"):
    """Run `gcloud ...` (read-only). Returns (parsed_json_or_text_or_None, error_or_None)."""
    why = gcloud_violation(args)
    if why:
        return None, guard_block("gcloud", args, why)             # no process is started
    exe = shutil.which("gcloud")
    if not exe:
        return None, "Google Cloud CLI (gcloud) was not found on PATH"
    guard_allow()
    cmd = [exe, *args, "--quiet"]
    pin = LOGIN_OPTS.get("account")
    if pin and "--account" not in args and tuple(str(x) for x in args[:2]) != ("auth", "list") and tuple(str(x) for x in args[:3]) != ("config", "get-value", "account"):
        cmd += ["--account", pin]                          # the chosen account is pinned on every call (`auth list` lists all accounts, so it is the one exception)
    if fmt:
        cmd.append(f"--format={fmt}")
    proj = (target or {}).get("project")
    if proj and project:
        cmd += ["--project", proj]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, env=_gcloud_env())
    except subprocess.TimeoutExpired:
        return None, f"timed out after {timeout}s"
    except Exception as exc:
        return None, str(exc)
    if proc.returncode != 0:
        text = (proc.stderr or proc.stdout).strip() or f"gcloud exited with {proc.returncode}"
        if is_expired_error(text):
            who = cmd[cmd.index("--account") + 1] if "--account" in cmd else None
            mark_expired(who, text)
            return None, mask_tokens(CREDS_EXPIRED_TEXT + " (" + _first_line(text, 120) + ")")
        return None, text
    try:
        return (json.loads(proc.stdout) if proc.stdout.strip() else {}), None
    except json.JSONDecodeError:
        return proc.stdout.strip(), None


def _first_line(err, n=170):
    return (str(err).strip().splitlines() or ["unknown error"])[-1][:n]


def gcp_token():
    """An access token from `gcloud auth print-access-token [--account X]` (kept in memory ~30 min, never printed or saved)."""
    if _TOKEN["value"] and time.time() - _TOKEN["at"] < 1800 and _TOKEN.get("account") == LOGIN_OPTS.get("account"):
        return _TOKEN["value"], None
    with _TOKEN_LOCK:                      # several parallel Monitoring / Logging calls must not each ask gcloud for a token
        if _TOKEN["value"] and time.time() - _TOKEN["at"] < 1800 and _TOKEN.get("account") == LOGIN_OPTS.get("account"):
            return _TOKEN["value"], None
        out, err = gcloud(["auth", "print-access-token"], None, 60, project=False, fmt=None)
        if err or not isinstance(out, str) or not out.strip():
            return None, err or "no access token (run: gcloud auth login)"
        _TOKEN["value"], _TOKEN["at"], _TOKEN["account"] = out.strip(), time.time(), LOGIN_OPTS.get("account")
        return _TOKEN["value"], None


def gcp_api(method, url, body=None, timeout=90):
    """A READ-ONLY Google API call (Cloud Monitoring timeSeries.list, Cloud Logging entries.list) with the gcloud
    access token. Returns (json, error). Anything else is refused."""
    why = api_violation(method, url)
    if why:
        return None, guard_block("api", [method, str(url).split("?")[0]], why)      # only Monitoring timeSeries.list and Logging entries.list
    guard_allow()
    token, err = gcp_token()
    if not token:
        return None, err
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode("utf-8") if body is not None else None,
                                 headers={"Authorization": "Bearer " + token, "Accept": "application/json", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
        return (json.loads(raw) if raw.strip() else {}), None
    except urllib.error.HTTPError as exc:
        try:
            msg = (json.loads(exc.read().decode("utf-8", "replace")).get("error") or {}).get("message")
        except Exception:
            msg = None
        if exc.code == 401:
            with _TOKEN_LOCK:
                _TOKEN["value"] = None
            mark_expired(None, f"HTTP 401 {msg or ''}")
            return None, CREDS_EXPIRED_TEXT + f" (HTTP 401: {msg or exc.reason})"
        return None, f"HTTP {exc.code}: {msg or exc.reason}"
    except Exception as exc:
        return None, str(exc)


def gcp_logs(target, flt, minutes, since, limit=200):
    """Cloud Logging entries (newest first) for a filter. Returns (entries, error). Uses `gcloud logging read`; on
    Windows the Cloud Logging API is tried first (a filter with quotes does not survive gcloud.cmd reliably)."""
    project = target["project"]

    def via_api():
        data, err = gcp_api("POST", "https://logging.googleapis.com/v2/entries:list",
                            {"resourceNames": [f"projects/{project}"], "filter": f'{flt} AND timestamp>="{_iso(since)}"',
                             "orderBy": "timestamp desc", "pageSize": limit})
        return (None, err) if err else ((data or {}).get("entries") or [], None)

    def via_cli():
        data, err = gcloud(["logging", "read", flt, f"--limit={limit}", "--order=desc", f"--freshness={minutes}m"], {"project": project}, 120)
        if err:
            return None, err
        return (data if isinstance(data, list) else []), None
    first_err = None
    for fn in ((via_api, via_cli) if os.name == "nt" else (via_cli, via_api)):
        entries, err = fn()
        if err is None:
            return entries, None
        first_err = first_err or err
    return None, first_err


def _log_text(e):
    jp, pp = e.get("jsonPayload") or {}, e.get("protoPayload") or {}
    for k in ("message", "log", "msg"):
        if jp.get(k):
            return str(jp[k]).strip()
    if e.get("textPayload"):
        return str(e["textPayload"]).strip()
    if (pp.get("status") or {}).get("message"):
        return str(pp["status"]["message"]).strip()
    return json.dumps(jp or pp)[:200]


# --- URL / name helpers ------------------------------------------------------------------------------------

def _url_name(url):
    return (url or "").rstrip("/").rsplit("/", 1)[-1]


def _url_project(url, default=None):
    m = re.search(r"projects/([^/]+)/", url or "")
    return m.group(1) if m else default


def _url_region(url):
    m = re.search(r"regions/([^/]+)", url or "")
    return m.group(1) if m else None


def _url_zone(url):
    m = re.search(r"zones/([^/]+)", url or "")
    return m.group(1) if m else None


def _region_of(location):
    """'us-central1-a' -> 'us-central1'; a region stays as it is."""
    return location.rsplit("-", 1)[0] if re.search(r"-[a-z]$", location or "") else location


def _ver(v):
    """'1.29.4-gke.1043002' -> (1, 29, 4, 1043002) for comparing versions."""
    m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?(?:-gke\.(\d+))?", str(v or ""))
    return tuple(int(x or 0) for x in m.groups()) if m else (0, 0, 0, 0)


def _pool_zones(p):
    return [z for z in (p.get("locations") or []) if z]


# --- projects (which GCP project the cluster lives in) ----------------------------------------------------

def gcp_default_project():
    data, _err = gcloud(["config", "get-value", "project"], None, 30, project=False)
    return data.strip() if isinstance(data, str) and data.strip() and data.strip() != "(unset)" else None


def _gcp_projects():
    """({project_id: {name, number, state, default}}, error_or_None) from `gcloud projects list` (needs `gcloud auth login`). All of them, no cap."""
    data, err = gcloud(["projects", "list"], None, 240, project=False)
    if err or not isinstance(data, list):
        return {}, (err or "unexpected output from gcloud projects list")
    default = gcp_default_project()
    return {p["projectId"]: {"name": p.get("name"), "number": p.get("projectNumber"), "state": p.get("lifecycleState"),
                             "default": p["projectId"] == default}
            for p in data if p.get("projectId")}, None


def list_gcp_projects():
    """{project_id: {name, number, state, default}} from `gcloud projects list` (needs `gcloud auth login`)."""
    return _gcp_projects()[0]


def describe_project(pid, info):
    return f"{info.get('name') or pid}  ({pid})" + ("  [default]" if info.get("default") else "") + (
        f"  {info['state']}" if info.get("state") not in (None, "ACTIVE") else "")


def _node_hint():
    """Where the cluster lives, from the first node's providerID (gce://<project>/<zone>/<instance>), the kubectl
    context name (gke_<project>_<location>_<cluster>) and the API server address."""
    hint = {"project": None, "ctx_project": None, "location": None, "cluster": None, "server": None, "context": None, "zone": None}
    ok, out = kubectl(["get", "nodes", "-o", "jsonpath={.items[0].spec.providerID}"])
    if ok:
        m = re.search(r"gce://([^/]+)/([^/]+)/", out)
        if m:
            hint["project"], hint["zone"] = m.group(1), m.group(2)
    ok, out = kubectl(["config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}"])
    hint["server"] = out if ok else None
    ok, out = kubectl(["config", "current-context"])
    hint["context"] = out if ok else None
    m = re.match(r"^gke_([^_]+)_([^_]+)_([^_]+)$", hint["context"] or "")
    if m:
        hint["ctx_project"], hint["location"], hint["cluster"] = m.groups()
    return hint


def find_gke_cluster(project, label, hint):
    """The GKE cluster object (from `gcloud container clusters list`) that matches the kubectl context, the API
    endpoint, or the cluster name. Returns (cluster_or_None, error_or_None)."""
    if GCP_OPTS["cluster"] and GCP_OPTS["location"]:
        data, err = gcloud(["container", "clusters", "describe", GCP_OPTS["cluster"], "--location", GCP_OPTS["location"]], {"project": project})
        return (data if isinstance(data, dict) else None), err
    data, err = gcloud(["container", "clusters", "list"], {"project": project}, timeout=120)
    if err or not isinstance(data, list):
        return None, err or "unexpected output from gcloud container clusters list"
    host = ((hint.get("server") or "").split("//")[-1].split(":")[0]).lower()
    wanted = [x.lower() for x in ((label or ""), (GCP_OPTS["cluster"] or ""), (hint.get("cluster") or "")) if x]
    for c in data:
        if hint.get("cluster") and (c.get("name") or "") == hint["cluster"] and (not hint.get("location") or c.get("location") == hint["location"]):
            return c, None
    for c in data:
        ends = {(c.get("endpoint") or "").lower(), ((c.get("privateClusterConfig") or {}).get("privateEndpoint") or "").lower(),
                ((c.get("privateClusterConfig") or {}).get("publicEndpoint") or "").lower()} - {""}
        if host and host in ends:
            return c, None
    for c in data:
        if (c.get("name") or "").lower() in wanted and c.get("name"):
            return c, None
    return None, None


def select_gcp_project(label, emit, preferred=None):
    """After gkelogin: read the GCP projects you can use, pick the one the cluster's nodes live in (providerID /
    kubectl context), and find the GKE cluster in it. Stores the result in GCP_OPTS. Returns the project id or None."""
    for k in ("project_used", "project_reason", "target", "cluster_info"):
        GCP_OPTS[k] = None
    _TOKEN["value"] = None
    if not shutil.which("gcloud"):
        emit("Google Cloud CLI (gcloud) not found on PATH - the GCP section will be skipped (kubectl data still works). Install it and run: gcloud auth login")
        return None
    acct, _err = gcloud(["config", "get-value", "account"], None, 30, project=False)
    projects = list_gcp_projects()
    hint = _node_hint()
    default = gcp_default_project()
    if not projects and not (hint["project"] or hint["ctx_project"] or default or preferred or GCP_OPTS["cluster"]):
        emit("No GCP projects found" + ("" if isinstance(acct, str) and acct else " (gcloud is not logged in)")
             + " - run `gcloud auth login` (the GCP section will be skipped; kubectl data still works).")
        return None
    if projects:
        emit(f"GCP projects ({len(projects)}): " + ", ".join(projects)[:300])
    else:
        emit("Could not list GCP projects (no resourcemanager.projects.list permission or not logged in) - trying the project taken from the nodes / context.")
    if preferred and projects and preferred not in projects:
        emit(f"WARNING: project '{preferred}' is not in your project list - trying it anyway.")
    ranked = {}

    def add(score, pid, why):
        if pid and score > ranked.get(pid, (0, ""))[0]:
            ranked[pid] = (score, why)
    if preferred:
        add(1000, preferred, "the project you selected")
    add(900, hint["project"], "the nodes' VMs live in it (from their providerID gce://project/zone/instance)")
    add(850, hint["ctx_project"], "it is in the kubectl context name (gke_<project>_<location>_<cluster>)")
    add(100, default, "your default gcloud project")
    for pid in projects:
        add(1, pid, "an available project")
    order = sorted(((s, pid, why) for pid, (s, why) in ranked.items()), key=lambda x: -x[0])
    tried = []
    for score, pid, why in order[:MAX_PROJECT_TRIES]:
        cluster, err = find_gke_cluster(pid, label, hint)
        if cluster:
            GCP_OPTS["project_used"], GCP_OPTS["project_reason"], GCP_OPTS["cluster_info"] = pid, why, cluster
            loc = cluster.get("location") or cluster.get("zone")
            GCP_OPTS["target"] = {"project": pid, "cluster": cluster.get("name"), "location": loc, "region": _region_of(loc),
                                  "id": f"projects/{pid}/locations/{loc}/clusters/{cluster.get('name')}",
                                  "number": (projects.get(pid) or {}).get("number")}
            emit(f"GCP project '{pid}' selected ({why}); GKE cluster {cluster.get('name')} in {loc}")
            return pid
        tried.append(pid)
        emit(f"  project '{pid}' ({why}): cluster not found" + (f" - {_first_line(err, 100)}" if err else ""))
    for pid in (hint["project"], hint["ctx_project"]):
        if pid:
            GCP_OPTS["project_used"], GCP_OPTS["project_reason"] = pid, "the nodes' project (cluster object not readable)"
            GCP_OPTS["target"] = {"project": pid, "cluster": hint.get("cluster") or label, "location": hint.get("location") or _region_of(hint.get("zone")),
                                  "region": None, "id": None, "number": (projects.get(pid) or {}).get("number")}
            break
    emit("WARNING: could not find the GKE cluster through `gcloud container clusters list` (tried: " + ", ".join(tried) + "). "
         "Check you have container.clusters.list, or pass --gke-cluster NAME --location LOCATION [--project ID].")
    return GCP_OPTS["project_used"]


def resolve_gcp_target(label):
    """The target chosen by select_gcp_project (project, cluster, location)."""
    t = GCP_OPTS.get("target")
    if t and t.get("cluster") and t.get("location") and GCP_OPTS.get("cluster_info"):
        return dict(t)
    return None


# --- the GCP section ---------------------------------------------------------------------------------------

def _gcp_net_info(target, c):
    """({name, project, region} of the cluster's VPC, subnet name) - read from the cluster object, no call."""
    nc = c.get("networkConfig") or {}
    net_url, sub_url = nc.get("network") or c.get("network") or "", nc.get("subnetwork") or c.get("subnetwork") or ""
    net_project = _url_project(net_url or sub_url, target["project"])
    region = _url_region(sub_url) or target.get("region") or _region_of(target["location"])
    return {"name": _url_name(net_url), "project": net_project, "region": region}, _url_name(sub_url)


def section_gcp(rep, ctx, label, only=None):
    """`only`: run just these blocks ('cluster', 'nodepools', 'network', 'identity', 'addons', 'vm', 'operations', 'cplogs'); used when another
    section needs a part of this one's data and this section itself is not ticked."""
    rep.section("2. GOOGLE KUBERNETES ENGINE CLUSTER AND INFRASTRUCTURE (cluster, node pools, network, identity, logging)")
    if not GCP_OPTS["enabled"]:
        rep.add("Skipped (GCP details turned off).")
        return
    target = resolve_gcp_target(label)
    if not target:
        rep.add("Could not identify the GKE cluster in GCP (gcloud not installed / not logged in / no project access). "
                "Run `gcloud auth login`, or pass --gke-cluster NAME --location LOCATION [--project ID].")
        return
    c = GCP_OPTS.get("cluster_info") or {}
    ctx.data["gcp_target"], ctx.data["gcp_cluster"] = target, c
    rep.add(f"Target: cluster={target['cluster']}  location={target['location']}  project={target['project']}")
    if GCP_OPTS.get("project_reason"):
        rep.add(f"GCP project: chosen because {GCP_OPTS['project_reason']}")
    acct, err = gcloud(["config", "get-value", "account"], target, 30, project=False)
    if err or not isinstance(acct, str) or not acct:
        rep.add(f"GCP credentials: NOT WORKING ({_first_line(err or 'no active account')})")
        rep.add("  Try:  gcloud auth login")
        ctx.find("HIGH", "gcloud is not logged in - GCP-side checks skipped")
        return
    rep.add(f"Signed in as: {acct}   project '{target['project']}'")
    if not c.get("nodePools") and not c.get("status"):
        data, err = gcloud(["container", "clusters", "describe", target["cluster"], "--location", target["location"]], target)
        if err or not isinstance(data, dict):
            rep.add(f"gcloud container clusters describe FAILED: {_first_line(err or 'no data')}")
            ctx.find("MED", f"Could not read the GKE cluster in GCP ({_first_line(err or 'no data', 90)})")
            return
        c = data
        ctx.data["gcp_cluster"] = c
    ctx.data["gcp_net"] = _gcp_net_info(target, c)[0]          # known before the blocks run (the identity block reads it)
    steps = [("cluster", _gcp_cluster), ("nodepools", _gcp_nodepools), ("network", _gcp_network), ("identity", _gcp_identity),
             ("addons", _gcp_addons), ("vm", _gcp_vm_health), ("operations", _gcp_operations), ("cplogs", _gcp_cp_logs)]
    if only:
        steps = [st for st in steps if st[0] in only]

    def block(step):
        def run(r):
            if ctx.cancel is not None and ctx.cancel.is_set():
                return
            try:
                step(r, ctx, target, c)
            except Exception as exc:
                r.add(f"[!] {step.__name__.strip('_')} failed: {exc}")
        return run
    run_blocks(rep, ctx, [block(fn) for _name, fn in steps], first=[i for i, (name, _f) in enumerate(steps) if name in ("vm", "network", "cplogs")])


def _gcp_cluster(rep, ctx, target, c):
    rep.sub("Cluster: status, versions, access, network and security settings",
            "the main settings of the Google Kubernetes Engine cluster as Google Cloud reports them (gcloud container clusters describe): status and mode, Kubernetes versions, who can reach the API server, network and address ranges, and the security options.",
            ["Control plane", "Autopilot", "Release channel", "Private cluster", "Authorized networks (control plane)", "CIDR", "VPC", "VPC-native", "Dataplane V2", "NetworkPolicy",
             "Workload Identity", "Shielded nodes", "Binary Authorization", "ABAC"])
    status = c.get("status", "?")
    mode = "Autopilot" if (c.get("autopilot") or {}).get("enabled") else "Standard"
    rep.add(f"  Status           : {status}   mode {mode}   {'regional' if _region_of(target['location']) == target['location'] else 'zonal'} cluster in {target['location']}"
            f"   node zones {','.join(c.get('locations') or []) or '-'}")
    if status in ("ERROR", "DEGRADED"):
        ctx.find("CRIT" if status == "ERROR" else "HIGH", f"GKE cluster status is {status}: {(c.get('statusMessage') or '')[:100]}")
    elif status not in ("RUNNING", "?"):
        ctx.find("MED", f"GKE cluster status is {status} (an upgrade / repair / resize is in progress)")
    for cond in c.get("conditions") or []:
        rep.add(f"  Condition        : {cond.get('code')}: {(cond.get('message') or '')[:150]}")
        ctx.find("HIGH", f"GKE cluster condition {cond.get('code')}: {(cond.get('message') or '')[:100]}")
    if c.get("statusMessage"):
        rep.add(f"  Status message   : {c['statusMessage'][:200]}")
    channel = (c.get("releaseChannel") or {}).get("channel") or "UNSPECIFIED"
    master, node_ver = c.get("currentMasterVersion"), c.get("currentNodeVersion")
    rep.add(f"  Versions         : control plane {master}   nodes {node_ver}   release channel {channel}")
    if master and node_ver and _ver(master)[:2] != _ver(node_ver)[:2]:
        rep.add("  (control plane and nodes are on different minor versions - an upgrade may be in progress or nodes are lagging)")
        if _ver(master)[1] - _ver(node_ver)[1] >= 2:
            ctx.find("HIGH", f"Node version {node_ver} is 2+ minor versions behind the control plane {master} (outside the supported skew)")
    endpoint = c.get("endpoint")
    priv = c.get("privateClusterConfig") or {}
    man = c.get("masterAuthorizedNetworksConfig") or {}
    cidrs = [b.get("cidrBlock") for b in man.get("cidrBlocks") or []]
    rep.add(f"  Access           : API endpoint {endpoint or priv.get('publicEndpoint') or '-'}   private nodes={bool(priv.get('enablePrivateNodes'))}  "
            f"private endpoint={bool(priv.get('enablePrivateEndpoint'))}  master CIDR {priv.get('masterIpv4CidrBlock') or '-'}   "
            f"authorized networks={'ON: ' + ','.join(str(x) for x in cidrs) if man.get('enabled') else 'OFF (open to the internet)'}")
    if not priv.get("enablePrivateEndpoint") and not man.get("enabled"):
        ctx.find("INFO", "GKE control-plane endpoint is public with no master authorized networks (reachable from the whole internet, protected only by authentication)")
    nc = c.get("networkConfig") or {}
    np_ = c.get("networkPolicy") or {}
    dp = nc.get("datapathProvider") or "LEGACY_DATAPATH"
    rep.add(f"  Network          : VPC {_url_name(nc.get('network') or c.get('network'))}  subnet {_url_name(nc.get('subnetwork') or c.get('subnetwork'))}  "
            f"dataplane={'Dataplane V2 (Cilium / eBPF)' if dp == 'ADVANCED_DATAPATH' else 'legacy (iptables)'}  "
            f"network policy={np_.get('provider') if np_.get('enabled') else ('Dataplane V2 built-in' if dp == 'ADVANCED_DATAPATH' else 'off')}  "
            f"intra-node visibility={bool(nc.get('enableIntraNodeVisibility'))}")
    ip = c.get("ipAllocationPolicy") or {}
    rep.add(f"  IP ranges        : pods {ip.get('clusterIpv4CidrBlock') or c.get('clusterIpv4Cidr') or '-'} ({ip.get('clusterSecondaryRangeName') or 'auto'})   "
            f"services {ip.get('servicesIpv4CidrBlock') or c.get('servicesIpv4Cidr') or '-'} ({ip.get('servicesSecondaryRangeName') or 'auto'})   "
            f"VPC-native={bool(ip.get('useIpAliases'))}  stack {ip.get('stackType') or 'IPV4'}")
    if ip and not ip.get("useIpAliases"):
        ctx.find("INFO", "Cluster is routes-based (not VPC-native): pods use custom routes; VPC-native (alias IP) is recommended")
    wi = (c.get("workloadIdentityConfig") or {}).get("workloadPool")
    rep.add(f"  Security         : workload identity={wi or 'off'}   shielded nodes={bool((c.get('shieldedNodes') or {}).get('enabled'))}   "
            f"binary authorization={(c.get('binaryAuthorization') or {}).get('evaluationMode') or ('on' if (c.get('binaryAuthorization') or {}).get('enabled') else 'off')}   "
            f"legacy ABAC={bool((c.get('legacyAbac') or {}).get('enabled'))}")
    if (c.get("legacyAbac") or {}).get("enabled"):
        ctx.find("HIGH", "Legacy ABAC authorization is enabled on the GKE cluster (grants broad permissions; disable it and use RBAC)")
    if not wi and mode == "Standard":
        ctx.find("INFO", "Workload Identity is not enabled (pods use the node service account to reach Google APIs)")
    sc, err = gcloud(["container", "get-server-config", "--location", target["location"]], target, 60)
    if not err and isinstance(sc, dict) and master:
        pool = next((ch for ch in sc.get("channels") or [] if ch.get("channel") == channel), None)
        versions = (pool or {}).get("validVersions") or sc.get("validMasterVersions") or []
        newer = sorted({v for v in versions if _ver(v) > _ver(master)}, key=_ver)
        if newer:
            rep.add(f"  Upgrades available: {', '.join(newer[-5:])}" + (f"   (channel {channel} default {pool.get('defaultVersion')})" if pool else ""))
            ctx.find("INFO", f"GKE upgrade available: {', '.join(newer[-3:])} (control plane runs {master})")
        else:
            rep.add("  Upgrades available: none newer than the control plane in this channel")
    elif err:
        rep.add(f"  Upgrades available: unavailable ({_first_line(err, 90)})")


def _gcp_migs(ctx, target):
    """{mig name: managed instance group} for the project (cached). The GKE node pools are managed instance groups."""
    def read():
        data, err = gcloud(["compute", "instance-groups", "managed", "list"], target, 120)
        return ({m.get("name"): m for m in data if isinstance(m, dict)} if isinstance(data, list) else {}, err if err else None)
    return ctx.cached("_gcp_migs", read)


def _pool_nodes(p, migs):
    """How many nodes the pool's managed instance groups want right now (None if unknown)."""
    sizes = [migs[_url_name(u)].get("targetSize") for u in (p.get("instanceGroupUrls") or []) if _url_name(u) in migs]
    return sum(s or 0 for s in sizes) if sizes else None


def _pool_max_nodes(p):
    a = p.get("autoscaling") or {}
    if not a.get("enabled"):
        return None
    if a.get("totalMaxNodeCount"):
        return a["totalMaxNodeCount"]
    return (a.get("maxNodeCount") or 0) * max(1, len(_pool_zones(p))) or None


def _gcp_nodepools(rep, ctx, target, c):
    rep.sub("Node pools", "the node pools of the cluster as Google Cloud reports them: machine type, how many nodes are ready compared with the number wanted, versions, zones and health.",
            ["Node pool", "Spot", "Preemptible", "Cluster autoscaler"])
    migs, _err = _gcp_migs(ctx, target)
    ready_by_pool = Counter()
    for n in items(ctx.data.get("nodes")):
        pool = n["metadata"].get("labels", {}).get("cloud.google.com/gke-nodepool")
        if pool and any(cd["type"] == "Ready" and cd["status"] == "True" for cd in n.get("status", {}).get("conditions", [])):
            ready_by_pool[pool] += 1
    rows = []
    for p in c.get("nodePools") or []:
        notes = []
        st = p.get("status", "?")
        if st in ("ERROR", "RUNNING_WITH_ERROR"):
            notes.append(f"{st}: {(p.get('statusMessage') or '')[:60]}")
            ctx.find("HIGH", f"Node pool {p['name']} status is {st}: {(p.get('statusMessage') or '')[:100]}")
        elif st not in ("RUNNING", "?"):
            notes.append(st)
            ctx.find("MED", f"Node pool {p['name']} status is {st}")
        want, have = _pool_nodes(p, migs), ready_by_pool.get(p["name"], 0)
        if want is not None and have < want:
            notes.append(f"only {have}/{want} nodes Ready")
            ctx.find("HIGH", f"Node pool {p['name']}: {want} nodes wanted but {have} Ready (check the instance group errors, quota, IP ranges, firewall)")
        mx = _pool_max_nodes(p)
        if mx and want is not None and want >= mx:
            notes.append("AT MAX (autoscaler can't add nodes)")
            ctx.find("MED", f"Node pool {p['name']} is at its autoscaler maximum ({mx})")
        cfg = p.get("config") or {}
        a = p.get("autoscaling") or {}
        prio = "SPOT" if cfg.get("spot") else ("PREEMPTIBLE" if cfg.get("preemptible") else "standard")
        mgmt = p.get("management") or {}
        if not mgmt.get("autoRepair", True):
            notes.append("auto-repair OFF")
        if mgmt.get("autoRepair") is False:
            ctx.find("INFO", f"Node pool {p['name']} has auto-repair turned off (unhealthy nodes are not replaced automatically)")
        if a.get("enabled"):
            if a.get("totalMaxNodeCount"):
                lim = f" (min {a.get('totalMinNodeCount') or 0}, max {a['totalMaxNodeCount']} total)"
            else:
                lim = f" (min {a.get('minNodeCount') or 0}, max {a.get('maxNodeCount')} per zone)"
        else:
            lim = " (no autoscaler)"
        rows.append([p["name"], cfg.get("machineType"), f"{have} ready / " + (f"{want} wanted" if want is not None else "? wanted") + lim,
                     (f"{cfg.get('diskSizeGb')}GB " if cfg.get("diskSizeGb") else "") + f"{cfg.get('diskType', '')} {cfg.get('imageType', '')}".strip() or "-", p.get("version"),
                     ",".join(_pool_zones(p)) or "-", (p.get("maxPodsConstraint") or {}).get("maxPodsPerNode") or "110", prio, st,
                     "; ".join(notes) or "OK"])
    rep.table(["NODE POOL", "MACHINE TYPE", "NODES", "DISK / IMAGE", "K8S VERSION", "ZONES", "MAX PODS/NODE", "PRIORITY", "STATUS", "HEALTH"], rows, maxw=70,
              about="One row per node pool: its machine type, how many nodes are ready compared with the number wanted and the autoscaler limits, disk and image, Kubernetes version, zones, the maximum pods per node, whether it uses standard, spot or preemptible machines, its status and a health note.")
    ctx.data["gcp_pools"] = [p["name"] for p in c.get("nodePools") or []]


def _block_prefix(pool, default_max=110):
    """Prefix length of the pod range block GKE gives each node of this pool (e.g. 24 -> /24 = 256 pod IPs)."""
    if pool.get("podIpv4CidrSize"):
        return int(pool["podIpv4CidrSize"])
    mp = int((pool.get("maxPodsConstraint") or {}).get("maxPodsPerNode") or default_max)
    for prefix, pods in ((28, 8), (27, 16), (26, 32), (25, 64), (24, 110), (23, 256), (22, 512)):
        if mp <= pods:
            return prefix
    return 24


def _fw_ports(allowed):
    out = []
    for a in allowed or []:
        proto = a.get("IPProtocol", "?")
        ports = a.get("ports")
        out.append(f"{proto}:{','.join(ports)}" if ports else proto)
    return ", ".join(out) or "-"


def _port_open(allowed, wanted):
    """True when the rule's allowed list opens the port (or every port of tcp / all protocols)."""
    for a in allowed or []:
        proto = (a.get("IPProtocol") or "").lower()
        if proto not in ("tcp", "all"):
            continue
        ports = a.get("ports")
        if not ports or proto == "all":
            return True
        for p in ports:
            lo, _, hi = str(p).partition("-")
            try:
                if int(lo) <= wanted <= int(hi or lo):
                    return True
            except ValueError:
                pass
    return False


def _gcp_network(rep, ctx, target, c):
    rep.sub("Network: subnet, address ranges, firewall rules and Cloud NAT (network address translation)",
            "the virtual private cloud network the cluster runs in: the subnet and its address ranges (with the free addresses left for nodes, pods and services), the firewall rules and the Cloud NAT gateways.",
            ["VPC", "CIDR", "Secondary range", "Private Google Access", "VPC Flow Logs", "Cloud NAT"])
    nc = c.get("networkConfig") or {}
    ip = c.get("ipAllocationPolicy") or {}
    net_url, sub_url = nc.get("network") or c.get("network") or "", nc.get("subnetwork") or c.get("subnetwork") or ""
    net_name, sub_name = _url_name(net_url), _url_name(sub_url)
    net_project = _url_project(net_url or sub_url, target["project"])
    region = _url_region(sub_url) or target.get("region") or _region_of(target["location"])
    ntarget = {"project": net_project}
    ctx.data["gcp_net"] = {"name": net_name, "project": net_project, "region": region}
    if net_project != target["project"]:
        rep.add(f"  Shared VPC: the network lives in host project {net_project}")
    pools = c.get("nodePools") or []
    migs, _err = _gcp_migs(ctx, target)
    nodes_now = sum(1 for _ in items(ctx.data.get("nodes")))
    nodes_max = 0
    for p in pools:
        now = _pool_nodes(p, migs)
        mx = _pool_max_nodes(p)
        nodes_max += max(now or 0, mx or 0) or (p.get("initialNodeCount") or 0) * max(1, len(_pool_zones(p)))
    nodes_max = max(nodes_max, nodes_now)
    fw_f = pf(gcloud, ["compute", "firewall-rules", "list"], ntarget, 120)          # the three independent reads run at the same time
    nat_f = pf(gcloud, ["compute", "routers", "list"], ntarget, 90)
    sn, err = gcloud(["compute", "networks", "subnets", "describe", sub_name, "--region", region], ntarget, 60) if sub_name else (None, "no subnetwork in the cluster object")
    ranges, subnets = [], []
    if err or not isinstance(sn, dict):
        rep.add(f"  Subnet {sub_name or '?'}: details unavailable ({_first_line(err or 'no data', 90)})")
    else:
        prim = sn.get("ipCidrRange")
        try:
            capacity = max(0, ipaddress.ip_network(prim, strict=False).num_addresses - 4)   # GCP reserves 4 addresses per primary range
        except (TypeError, ValueError):
            capacity = 0
        free = max(0, capacity - nodes_now)
        sn["_free"], sn["_used"], sn["_capacity"] = free, nodes_now, capacity
        subnets.append(sn)
        note = ""
        if capacity and free < 10:
            note = "VERY LOW IPs"
            ctx.find("HIGH", f"Subnet {sn.get('name')} has only ~{free} free node IPs ({nodes_now}/{capacity} used by this cluster's nodes)")
        elif capacity and free < LOW_SUBNET_IPS:
            note = "low IPs"
            ctx.find("MED", f"Subnet {sn.get('name')} has only ~{free} free node IPs ({nodes_now}/{capacity} used by this cluster's nodes)")
        if capacity and nodes_max > capacity:
            note = (note + "; " if note else "") + f"CANNOT HOLD scale-out ({nodes_max} nodes)"
            ctx.find("HIGH", f"Subnet {sn.get('name')} ({capacity} usable IPs) cannot hold the node pools' maximum size: {nodes_max} nodes")
        ranges.append({"kind": "nodes", "name": f"{sn.get('name')} (primary, nodes)", "cidr": prim, "free": free, "total": capacity, "used": nodes_now})
        rows = [[f"{sn.get('name')} (primary: nodes)", prim, capacity, f"{nodes_now} nodes (this cluster)", free, f"{nodes_max} nodes at max size", note or "ok"]]
        # secondary ranges: pods and services
        pod_range = ip.get("clusterSecondaryRangeName")
        svc_range = ip.get("servicesSecondaryRangeName")
        sec = {r.get("rangeName"): r.get("ipCidrRange") for r in sn.get("secondaryIpRanges") or []}
        by_range = defaultdict(list)
        for p in pools:
            by_range[((p.get("networkConfig") or {}).get("podRange") or pod_range or "(pod range)")].append(p)
        for rname, plist in by_range.items():
            cidr = sec.get(rname) or ip.get("clusterIpv4CidrBlock") or c.get("clusterIpv4Cidr")
            try:
                total = ipaddress.ip_network(cidr, strict=False).num_addresses
            except (TypeError, ValueError):
                continue
            used = need = n_now = n_max = 0
            blocks = []
            for p in plist:
                prefix = _block_prefix(p, int((c.get("defaultMaxPodsConstraint") or {}).get("maxPodsPerNode") or 110))
                now = _pool_nodes(p, migs)
                if now is None:
                    now = sum(1 for n in items(ctx.data.get("nodes")) if n["metadata"].get("labels", {}).get("cloud.google.com/gke-nodepool") == p["name"])
                mx = max(_pool_max_nodes(p) or 0, now)
                blocks.append((mx, prefix))
                n_now += now
                n_max += mx
                used += now * 2 ** (32 - prefix)
                need += mx * 2 ** (32 - prefix)
            block = max(blocks)[1] if blocks else 24      # the block size of the pool that grows the most
            node_cap = total // (2 ** (32 - block)) if blocks else 0
            pct = 100 * used / total if total else 0
            note = ""
            if need > total:
                note = f"CANNOT HOLD scale-out ({n_max} nodes need {need} IPs)"
                ctx.find("HIGH", f"Pod range {rname} ({cidr}) has {total} addresses but the pools can grow to {n_max} nodes needing {need} pod IPs (a block of /{block} or smaller per node)")
            elif pct >= 90:
                note = "pod range nearly full"
                ctx.find("HIGH", f"Pod IP range {rname} ({cidr}) is {pct:.0f}% reserved by {n_now} nodes (~{node_cap} nodes fit)")
            elif pct >= 75:
                note = "pod range filling up"
                ctx.find("MED", f"Pod IP range {rname} ({cidr}) is {pct:.0f}% reserved by {n_now} nodes (~{node_cap} nodes fit)")
            free_pods = max(0, total - used)
            ranges.append({"kind": "pods", "name": f"{rname} (pods)", "cidr": cidr, "free": free_pods, "total": total, "used": used})
            rows.append([f"{rname} (secondary: pods)", cidr, total, f"{n_now} nodes reserve {used} IPs ({pct:.0f}%)", free_pods,
                         f"{n_max} nodes = {need} IPs (range holds ~{node_cap} nodes of /{block})", note or "ok"])
        if svc_range or ip.get("servicesIpv4CidrBlock"):
            cidr = sec.get(svc_range) or ip.get("servicesIpv4CidrBlock") or c.get("servicesIpv4Cidr")
            try:
                total = ipaddress.ip_network(cidr, strict=False).num_addresses
            except (TypeError, ValueError):
                total = 0
            nsvc = sum(1 for s in items(ctx.data.get("services")) if (s.get("spec", {}).get("clusterIP") or "None") not in ("None", ""))
            note = ""
            if total and nsvc >= 0.8 * total:
                note = "service range nearly full"
                ctx.find("HIGH", f"Service IP range {cidr} is {100 * nsvc / total:.0f}% used ({nsvc} ClusterIPs of {total})")
            ranges.append({"kind": "services", "name": f"{svc_range or 'services'} (services)", "cidr": cidr, "free": max(0, total - nsvc), "total": total, "used": nsvc})
            rows.append([f"{svc_range or 'services'} (secondary: services)", cidr, total, f"{nsvc} ClusterIPs", max(0, total - nsvc), "-", note or "ok"])
        rep.add(f"  Subnet {sn.get('name')} in {region}: private Google access={bool(sn.get('privateIpGoogleAccess'))}  flow logs={bool((sn.get('logConfig') or {}).get('enable') or sn.get('enableFlowLogs'))}  "
                f"stack {sn.get('stackType') or 'IPV4_ONLY'}")
        if not sn.get("privateIpGoogleAccess") and (c.get("privateClusterConfig") or {}).get("enablePrivateNodes"):
            ctx.find("MED", f"Subnet {sn.get('name')} has Private Google Access OFF but the cluster has private nodes (nodes can't reach Google APIs / gcr.io / Artifact Registry without Cloud NAT)")
        rep.add("  Address ranges (GKE reserves a block of the pod range per node: /24 for the default 110 pods per node; "
                "node IPs counted are only this cluster's nodes - internal load balancers and other VMs in the subnet are not):")
        rep.table(["RANGE", "CIDR", "ADDRESSES", "USED BY THIS CLUSTER", "FREE (est.)", "NEEDED AT MAX SIZE", "NOTE"], rows, maxw=48,
                  about="One row per address range of the subnet (nodes, pods, services): its size, how many addresses this cluster uses, how many are free, and how many would be needed if every node pool grew to its maximum.")
    ctx.data["gcp_subnets"], ctx.data["gcp_ranges"] = subnets, ranges
    _gcp_firewalls(rep, ctx, target, c, ntarget, net_name, fw_f)
    _gcp_nat_config(rep, ctx, target, c, ntarget, net_name, region, nat_f)


def _gcp_firewalls(rep, ctx, target, c, ntarget, net_name, pre=None):
    data, err = pre.result() if pre is not None else gcloud(["compute", "firewall-rules", "list"], ntarget, 120)
    if err or not isinstance(data, list):
        rep.add(f"  Firewall rules unavailable ({_first_line(err or 'no data', 90)}) - needs compute.firewalls.list (roles/compute.viewer)")
        return
    rules = [r for r in data if _url_name(r.get("network")) == net_name]
    ctx.data["gcp_fw_rules"] = rules
    rep.add(f"  Firewall rules on VPC {net_name}: {len(rules)} ({sum(1 for r in rules if r.get('disabled'))} disabled)")
    rows = []
    for r in sorted(rules, key=lambda r: r.get("priority", 1000)):
        if r.get("disabled"):
            continue
        allowed = r.get("allowed") or r.get("denied") or []
        src = ",".join(r.get("sourceRanges") or r.get("destinationRanges") or []) or ",".join(r.get("sourceTags") or []) or "-"
        tgt = ",".join(r.get("targetTags") or r.get("targetServiceAccounts") or []) or "(all)"
        action = "ALLOW" if r.get("allowed") else "DENY"
        risky = ""
        if r.get("direction") == "INGRESS" and action == "ALLOW" and "0.0.0.0/0" in (r.get("sourceRanges") or []):
            for port, what in ((22, "SSH"), (3389, "RDP")):
                if _port_open(r.get("allowed"), port):
                    risky = f"{what} OPEN TO THE INTERNET"
                    ctx.find("HIGH", f"Firewall rule {r.get('name')} allows {what} ({port}) from 0.0.0.0/0 (target {tgt})")
                    break
            else:
                if any((a.get("IPProtocol") or "").lower() == "all" for a in r.get("allowed") or []):
                    risky = "ALL PORTS OPEN TO THE INTERNET"
                    ctx.find("HIGH", f"Firewall rule {r.get('name')} allows ALL protocols/ports from 0.0.0.0/0 (target {tgt})")
        gke = (r.get("name") or "").startswith("gke-" + (target["cluster"] or "")[:20]) or "gke-" in (r.get("name") or "")
        rows.append((0 if risky else (1 if gke else 2), [r.get("priority"), r.get("direction"), action, _fw_ports(allowed), src, tgt, r.get("name"), risky]))
    rows.sort(key=lambda x: x[0])
    rep.add("  Rules (risky first, then the gke-* rules GKE manages; VPC implied rules - deny all ingress / allow all egress - also apply):")
    rep.table(["PRIORITY", "DIRECTION", "ACTION", "PROTOCOL:PORTS", "SOURCE / DEST", "TARGET", "RULE", "NOTE"], [x[1] for x in rows], limit=25, maxw=44,
              about="The enabled firewall rules of the network, risky ones first and then the rules GKE manages: priority, direction, allow or deny, protocol and ports, source or destination, which machines it targets, and a note when it is open to the whole internet.")
    if (c.get("privateClusterConfig") or {}).get("enablePrivateNodes") and not any(
            (r.get("name") or "").startswith("gke-") and r.get("direction") == "INGRESS" for r in rules):
        ctx.find("INFO", "No gke-* ingress firewall rules found on this VPC - GKE normally creates them (control plane -> nodes 10250/443); check they were not deleted")


def _gcp_nat_config(rep, ctx, target, c, ntarget, net_name, region, pre=None):
    """Cloud Routers / Cloud NAT gateways of the cluster's network and region."""
    data, err = pre.result() if pre is not None else gcloud(["compute", "routers", "list"], ntarget, 90)
    nats = []
    if err or not isinstance(data, list):
        rep.add(f"  Cloud Routers / NAT unavailable ({_first_line(err or 'no data', 90)})")
        ctx.data["gcp_nats"] = None
        return
    rows = []
    for r in data:
        if _url_name(r.get("network")) != net_name or (_url_region(r.get("region")) or r.get("region")) not in (region, None):
            continue
        for nat in r.get("nats") or []:
            nats.append({"router": r.get("name"), "name": nat.get("name"), "region": _url_region(r.get("region")) or r.get("region") or region, "cfg": nat})
            rows.append([r.get("name"), nat.get("name"), nat.get("natIpAllocateOption"), ", ".join(_url_name(x) for x in nat.get("natIps") or []) or "(auto)",
                         nat.get("sourceSubnetworkIpRangesToNat"), nat.get("minPortsPerVm") or "64", "on" if nat.get("enableDynamicPortAllocation") else "off",
                         "on" if nat.get("enableEndpointIndependentMapping") else "off", (nat.get("logConfig") or {}).get("filter") if (nat.get("logConfig") or {}).get("enable") else "off"])
            if nat.get("natIpAllocateOption") == "AUTO_ONLY" and nat.get("enableDynamicPortAllocation") is False and int(nat.get("minPortsPerVm") or 64) < 128:
                ctx.find("INFO", f"Cloud NAT {nat.get('name')}: only {nat.get('minPortsPerVm') or 64} ports per VM and dynamic port allocation is off - nodes running many pods can run out of NAT ports")
    ctx.data["gcp_nats"] = nats
    if rows:
        rep.add(f"  Cloud NAT gateways on VPC {net_name} in {region}:")
        rep.table(["ROUTER", "NAT", "IP ALLOCATION", "NAT IPs", "SOURCE RANGES", "MIN PORTS/VM", "DYNAMIC PORTS", "ENDPOINT-INDEP.", "LOGGING"], rows, maxw=40,
                  about="One row per Cloud NAT gateway of the network and region: its router, how its IP addresses are allocated, which subnet ranges it serves, the ports given to each virtual machine and whether logging is on.",
                  terms=["NAT", "Cloud NAT"])
    elif (c.get("privateClusterConfig") or {}).get("enablePrivateNodes"):
        rep.add(f"  Cloud NAT: NONE on VPC {net_name} in {region}")
        ctx.find("HIGH", f"Private-node cluster but no Cloud NAT on VPC {net_name} in {region}: pods can't reach the internet or external registries (only Google APIs via Private Google Access)")
    else:
        rep.add(f"  Cloud NAT: none on VPC {net_name} in {region} (nodes have external IPs, so they reach the internet directly)")


def _sa_roles(policy, member):
    return sorted({b.get("role") for b in (policy or {}).get("bindings") or [] if member in (b.get("members") or [])})


def _gcp_identity(rep, ctx, target, c):
    rep.sub("Node service account and Identity and Access Management (IAM) roles (project-level bindings)",
            "which service account the nodes run as and which roles it has on the project; a node account with Owner or Editor is too broad. Only roles granted on the project are visible here.",
            ["IAM", "Service account", "Role", "Service agent", "Workload Identity"])
    proj = target["project"]
    number = target.get("number")
    policy_f = pf(gcloud, ["projects", "get-iam-policy", proj], None, 90, project=False)
    if not number:
        pd, _e = gcloud(["projects", "describe", proj], None, 30, project=False)
        number = (pd or {}).get("projectNumber") if isinstance(pd, dict) else None
    default_email = f"{number}-compute@developer.gserviceaccount.com" if number else None
    sas = {}
    for p in c.get("nodePools") or []:
        sa = (p.get("config") or {}).get("serviceAccount") or (c.get("nodeConfig") or {}).get("serviceAccount") or "default"
        email = default_email if sa == "default" else sa
        sas.setdefault(email or "default (Compute Engine default service account)", []).append(p["name"])
    if (c.get("autopilot") or {}).get("enabled") and not sas:
        sas[(c.get("autoscaling") or {}).get("autoprovisioningNodePoolDefaults", {}).get("serviceAccount") or default_email or "default"] = ["autopilot"]
    agent = f"service-{number}@container-engine-robot.iam.gserviceaccount.com" if number else None
    sa_f = {email: pf(gcloud, ["iam", "service-accounts", "describe", email], None, 30, project=False)
            for email in sas if email and "@" in str(email) and not str(email).startswith("default")}      # one read per node service account, at once
    policy, err = policy_f.result()
    if err or not isinstance(policy, dict):
        rep.add(f"  IAM policy unavailable ({_first_line(err or 'no data', 100)}) - needs resourcemanager.projects.getIamPolicy (roles/iam.securityReviewer)")
    rows = []
    for email, pools in sas.items():
        is_default = bool(DEFAULT_SA.match(email or "")) or str(email).startswith("default")
        roles = _sa_roles(policy, f"serviceAccount:{email}") if isinstance(policy, dict) else []
        for role in roles or [None]:
            note = ""
            if role in ("roles/editor", "roles/owner") and is_default:
                note = "TOO BROAD for a node account"
            rows.append([email, ",".join(pools), "default Compute Engine service account" if is_default else "custom service account", role or ("(none visible)" if isinstance(policy, dict) else "-"), note])
        if isinstance(policy, dict):
            if is_default and any(r in ("roles/editor", "roles/owner") for r in roles):
                ctx.find("HIGH", f"Node pool(s) {','.join(pools)} run as the default Compute Engine service account, which has {'Owner' if 'roles/owner' in roles else 'Editor'} on the project (every pod can use those rights unless Workload Identity is on)")
            elif is_default:
                ctx.find("INFO", f"Node pool(s) {','.join(pools)} use the default Compute Engine service account (a dedicated least-privilege node SA is recommended)")
            elif not any(r in ("roles/container.defaultNodeServiceAccount", "roles/logging.logWriter") for r in roles):
                ctx.find("INFO", f"Custom node service account {email} has no roles/container.defaultNodeServiceAccount or logging.logWriter binding at project level (logs / metrics may be missing; bindings on a folder or org are not visible here)")
        if email and "@" in str(email) and not str(email).startswith("default"):
            d, _e = sa_f[email].result()
            if isinstance(d, dict) and d.get("disabled"):
                rows.append([email, ",".join(pools), "-", "(DISABLED)", "service account is disabled"])
                ctx.find("HIGH", f"Node service account {email} is DISABLED - nodes can't pull images or write logs / metrics")
    if agent and isinstance(policy, dict):
        roles = _sa_roles(policy, f"serviceAccount:{agent}")
        for role in roles or [None]:
            rows.append([agent, "-", "GKE service agent", role or "(none visible)", "" if role else "expected roles/container.serviceAgent"])
        if "roles/container.serviceAgent" not in roles and net_is_local(ctx, target):
            ctx.find("MED", f"GKE service agent {agent} has no roles/container.serviceAgent binding visible (cluster operations such as upgrades or node creation can fail)")
    rep.table(["SERVICE ACCOUNT", "NODE POOLS", "KIND", "ROLE", "NOTE"], rows, maxw=70,
              about="One row per role of each service account the nodes use (and of the GKE service agent): the account, the node pools that run as it, what kind of account it is, the role it holds on the project and a note when the role is too broad or missing.")
    rep.add("  (Only project-level bindings are listed; roles granted on the folder / organization or on single resources are not visible here.)")


def net_is_local(ctx, target):
    """True when the VPC is in the cluster's own project (no Shared VPC), so the service-agent role is expected here."""
    return (ctx.data.get("gcp_net") or {}).get("project", target["project"]) == target["project"]


def _gcp_addons(rep, ctx, target, c):
    rep.sub("Google Kubernetes Engine add-ons", "the optional GKE features and whether each one is switched on for this cluster (the addonsConfig of the cluster).",
            ["Horizontal pod autoscaler", "NetworkPolicy", "Calico"])
    rows = []
    enabled = {}
    for name, a in sorted((c.get("addonsConfig") or {}).items()):
        if not isinstance(a, dict):
            continue
        on = (not a["disabled"]) if "disabled" in a else bool(a.get("enabled"))
        enabled[name] = on
        extra = ""
        if name == "networkPolicyConfig":
            extra = "calico/ network policy enforcement"
        rows.append([name, "enabled" if on else "disabled", extra])
    rep.table(["ADD-ON", "STATE", "DETAIL"], rows,
              about="One row per GKE add-on (HTTP load balancing, horizontal pod autoscaling, network policy ...) saying whether it is enabled or disabled.")
    if enabled.get("httpLoadBalancing") is False:
        ctx.find("INFO", "The HTTP load balancing add-on is disabled - Ingress objects with the gce class will not get a load balancer")
    if enabled.get("horizontalPodAutoscaling") is False:
        ctx.find("INFO", "The Horizontal Pod Autoscaling add-on is disabled - HPAs will not scale")
    ls, ms = c.get("loggingService"), c.get("monitoringService")
    if ls == "none" or ms == "none":
        ctx.find("MED", f"Cloud Logging / Cloud Monitoring is turned off for the cluster (logging={ls}, monitoring={ms}) - no GCP-side container logs or metrics")


def _gcp_vm_health(rep, ctx, target, c):
    """The Compute Engine VMs behind the nodes: status (RUNNING / TERMINATED ...), and the managed instance groups."""
    rep.sub("Node virtual machines (Compute Engine instances) and managed instance groups",
            "the Compute Engine virtual machines behind the nodes (are they running, where, which size) and the managed instance groups that keep them at their size; problems are listed first.",
            ["Compute Engine", "Managed instance group", "Spot", "Preemptible", "Stockout", "Quota"])
    pf(_gcp_migs, ctx, target)                       # the managed instance groups are read while the instances are
    data, err = gcloud(["compute", "instances", "list"], target, 150)
    nodes = {n["metadata"]["name"].lower(): n for n in items(ctx.data.get("nodes"))}
    vm_info, rows = {}, []
    if err or not isinstance(data, list):
        rep.add(f"  Instances unavailable: {_first_line(err or 'no data', 110)}   (needs compute.instances.list - roles/compute.viewer)")
    else:
        cluster = target["cluster"]
        for i in data:
            name = (i.get("name") or "").lower()
            labels = i.get("labels") or {}
            if name not in nodes and labels.get("goog-k8s-cluster-name") != cluster:
                continue
            status = i.get("status") or "?"
            sched = i.get("scheduling") or {}
            prio = "SPOT" if (sched.get("provisioningModel") == "SPOT") else ("PREEMPTIBLE" if sched.get("preemptible") else "standard")
            nic = (i.get("networkInterfaces") or [{}])[0]
            ext = next((a.get("natIP") for a in nic.get("accessConfigs") or [] if a.get("natIP")), None)
            vm_info[name] = {"name": i.get("name"), "state": status, "id": str(i.get("id") or ""), "zone": _url_name(i.get("zone")),
                             "type": _url_name(i.get("machineType")), "prio": prio}
            if name not in nodes:
                bad, why = True, "VM of this cluster but NOT a registered Kubernetes node"
                ctx.find("MED", f"VM {i.get('name')} ({status}) belongs to the cluster but is not a node (it never joined, or was removed from the API)")
            else:
                bad = status != "RUNNING"
                why = "PROBLEM" if bad else "ok"
                if bad:
                    ctx.find("MED" if status in ("STAGING", "PROVISIONING", "REPAIRING") else "HIGH", f"VM {i.get('name')} is {status} (Compute Engine) while it is still a cluster node")
            extra = ""
            if ext and (c.get("privateClusterConfig") or {}).get("enablePrivateNodes"):
                extra = "has an external IP although the cluster has private nodes"
            rows.append((bad or bool(extra), [i.get("name"), vm_info[name]["id"], status, vm_info[name]["zone"], vm_info[name]["type"], prio, nic.get("networkIP", "-"), ext or "-",
                                              (why if bad else "ok") + ("; " + extra if extra else "")]))
        missing = [n for n in nodes if n not in vm_info]
        if missing and rows:
            rep.add(f"  {len(missing)} Kubernetes node(s) have no Compute Engine VM in project {target['project']}: {', '.join(missing[:5])}")
            ctx.find("MED", f"{len(missing)} node(s) not found as VMs in project {target['project']} (deleted VM, other project or listing limited by permissions): {', '.join(missing[:3])}")
        problems = [r for flag, r in rows if flag]
        rep.add(f"  {len(rows)} VM(s) checked, {len(problems)} with problems / notes.")
        rep.table(["NODE", "INSTANCE ID", "STATUS", "ZONE", "MACHINE TYPE", "PRIORITY", "INTERNAL IP", "EXTERNAL IP", ""], problems or [r for _f, r in rows[:6]], maxw=44,
                  about="The virtual machines of this cluster that have a problem or a note (all of them, up to six, when none has): the node, its Compute Engine instance identifier, status, zone, machine type, priority, internal and external IP address, and the detail.")
    ctx.data["vm_info"] = vm_info
    ctx.data.pop("_node_idents", None)
    migs, merr = _gcp_migs(ctx, target)
    mine = {}
    for p in c.get("nodePools") or []:
        for u in p.get("instanceGroupUrls") or []:
            if _url_name(u) in migs:
                mine[_url_name(u)] = (p["name"], migs[_url_name(u)])
    if merr and not migs:
        rep.add(f"  Managed instance groups unavailable: {_first_line(merr, 110)}")
        return
    mrows, unstable = [], []
    for name, (pool, m) in mine.items():
        st = (m.get("status") or {})
        stable = st.get("isStable", True)
        acts = {k: v for k, v in (m.get("currentActions") or {}).items() if v and k != "none"}
        zone = _url_zone(m.get("zone")) or _url_region(m.get("region")) or "-"
        mrows.append([name, pool, zone, m.get("targetSize"), "stable" if stable else "NOT STABLE",
                      ", ".join(f"{k} {v}" for k, v in acts.items()) or "-", "yes" if m.get("autoHealingPolicies") else "no"])
        if not stable:
            unstable.append((name, m))
            ctx.find("MED", f"Managed instance group {name} (pool {pool}) is not stable: " + (", ".join(f"{k} {v}" for k, v in acts.items()) or "changes pending"))
    if mrows:
        rep.sub("Managed instance groups behind the node pools", "the groups of virtual machines Google Cloud keeps at the size of each node pool, per zone, and whether they are stable.")
        rep.table(["INSTANCE GROUP", "NODE POOL", "ZONE", "TARGET SIZE", "STATE", "ACTIONS IN PROGRESS", "AUTOHEALING"], mrows, maxw=50,
                  about="One row per managed instance group (one per zone of a node pool): its target size, whether it is stable, what it is doing right now (creating or deleting machines) and whether automatic repair of failing machines is on.")
    err_rows = []

    def list_errors(item):
        name, m = item
        zone, region = _url_zone(m.get("zone")), _url_region(m.get("region"))
        return gcloud(["compute", "instance-groups", "managed", "list-errors", name] + (["--zone", zone] if zone else ["--region", region or target["region"]]), target, 60)
    err_calls = [pf(list_errors, item) for item in unstable[:6]]
    for (name, m), call in zip(unstable[:6], err_calls):
        errs, e2 = call.result()
        if e2 or not isinstance(errs, list):
            continue
        for e in errs[:20]:
            ts = parse_ts(e.get("timestamp"))
            if ts and ts < ctx.since:
                continue
            first = ((e.get("error") or {}).get("errors") or [{}])[0]
            err_rows.append([ts.strftime("%H:%M:%S") if ts else "-", name, (e.get("instanceActionDetails") or {}).get("action", "-"),
                             first.get("code", "-"), (first.get("message") or "")[:110]])
            ctx.find("HIGH", f"Instance group {name}: {first.get('code', 'error')} - {(first.get('message') or '')[:90]}")
            if ts:
                ctx.happened(ts, f"INSTANCE GROUP {name}: {first.get('code', 'error')} {(first.get('message') or '')[:80]}")
    if err_rows:
        rep.sub("Instance group errors in the window", "why nodes could not be created or were removed in the selected window: quota, stockout, address exhaustion and similar errors reported by the instance groups.")
        rep.table(["TIME", "INSTANCE GROUP", "ACTION", "CODE", "MESSAGE"], err_rows, maxw=70,
                  about="One row per error of an instance group in the window: when it happened (UTC), the group, the action that failed, the error code and the message from Google Cloud.")


def _gcp_operations(rep, ctx, target, c):
    """GKE operations on this cluster in the window (upgrades, repairs, resizes) - what changed the cluster."""
    rep.sub(f"Google Kubernetes Engine operations on this cluster (last {ctx.minutes} minutes, and any still running)",
            "what changed the cluster in the window: upgrades, repairs, resizes and other operations that Google Cloud ran, with their result.")
    data, err = gcloud(["container", "operations", "list"], target, 90)
    if err or not isinstance(data, list):
        rep.add(f"  unavailable: {_first_line(err or 'no data', 110)}")
        return
    rows = []
    for o in data:
        if f"/clusters/{target['cluster']}" not in (o.get("targetLink") or "") and (o.get("targetLink") or "").rsplit("/", 1)[-1] != target["cluster"]:
            continue
        start, end = parse_ts(o.get("startTime")), parse_ts(o.get("endTime"))
        running = o.get("status") in ("RUNNING", "PENDING", "ABORTING")
        if not running and not (start and start >= ctx.since) and not (end and end >= ctx.since):
            continue
        errmsg = (o.get("error") or {}).get("message") or (o.get("statusMessage") or "")
        target_name = (o.get("targetLink") or "").split("/clusters/")[-1]
        rows.append([start.strftime("%H:%M:%S") if start else "-", o.get("operationType"), o.get("status"), target_name, errmsg[:100] or (o.get("detail") or "")[:100]])
        if running:
            ctx.find("INFO", f"GKE operation in progress: {o.get('operationType')} on {target_name} (nodes may be recreated / drained)")
        if errmsg and o.get("status") == "DONE":
            ctx.find("HIGH", f"GKE operation {o.get('operationType')} on {target_name} FAILED: {errmsg[:100]}")
        if start:
            ctx.happened(start, f"GKE OPERATION {o.get('operationType')} {o.get('status')} on {target_name}" + (f": {errmsg[:80]}" if errmsg else ""))
    if rows:
        rep.table(["START", "OPERATION", "STATUS", "TARGET", "DETAIL / ERROR"], rows, maxw=70,
                  about="One row per cluster operation in the window or still running: when it started (UTC), the kind of operation, its status, what it acted on and the detail or error message.")
    else:
        rep.add("  no GKE operations for this cluster in the window.")


def _gcp_cp_logs(rep, ctx, target, c):
    rep.sub("Cluster logging and monitoring settings", "which logs and metrics the cluster sends to Cloud Logging and Cloud Monitoring; without them the Google Cloud side has nothing to troubleshoot with.",
            ["Cloud Logging", "Cloud Monitoring", "Managed Prometheus", "Control plane"])
    lc = ((c.get("loggingConfig") or {}).get("componentConfig") or {}).get("enableComponents") or []
    mc = ((c.get("monitoringConfig") or {}).get("componentConfig") or {}).get("enableComponents") or []
    rep.add("  Logging  : " + "   ".join(f"{k}: {'ON' if k in lc else 'off'}" for k in LOGGING_COMPONENTS) + f"   (service: {c.get('loggingService') or '-'})")
    rep.add("  Monitoring: " + (", ".join(mc) or "(none)") + f"   managed Prometheus={bool(((c.get('monitoringConfig') or {}).get('managedPrometheusConfig') or {}).get('enabled'))}   (service: {c.get('monitoringService') or '-'})")
    if c.get("loggingService") != "none" and not lc and not c.get("loggingService"):
        ctx.find("MED", "Cloud Logging for the cluster is OFF - no control-plane / system / workload logs to troubleshoot with")
    elif "APISERVER" not in lc and c.get("loggingService") != "none":
        ctx.find("INFO", "Control-plane logs (API server / scheduler / controller manager) are not enabled for Cloud Logging")
    name, mins = target["cluster"], ctx.minutes
    scope = f'resource.labels.cluster_name="{name}"'
    flt_cp = f'(resource.type="k8s_control_plane_component" OR resource.type="k8s_cluster") AND {scope} AND severity>=ERROR'
    flt_ca = f'resource.type="k8s_cluster" AND {scope} AND logName="projects/{target["project"]}/logs/container.googleapis.com%2Fcluster-autoscaler-visibility"'
    flt_au = f'protoPayload.serviceName="k8s.io" AND {scope} AND (protoPayload.status.code=7 OR protoPayload.status.code=16)'
    flt_ad = f'protoPayload.serviceName="container.googleapis.com" AND protoPayload.resourceName:"clusters/{name}" AND protoPayload.status.code>0'
    q_cp, q_ca, q_au, q_ad = (pf(gcp_logs, target, flt_cp, mins, ctx.since, 200), pf(gcp_logs, target, flt_ca, mins, ctx.since, 200),    # the four Cloud Logging
                              pf(gcp_logs, target, flt_au, mins, ctx.since, 500), pf(gcp_logs, target, flt_ad, mins, ctx.since, 100))       # queries run together
    entries, err = q_cp.result()
    rep.sub(f"Control plane and cluster log errors (last {mins} minutes, from Cloud Logging)",
            "error-level log entries written by the managed control plane and the cluster in the window, counted per component and listed newest first.")
    if err or entries is None:
        rep.add(f"  unavailable: {_first_line(err or 'no data', 120)}   (needs roles/logging.viewer)")
    elif not entries:
        rep.add("  no error-severity entries in the window.")
    else:
        by_comp = Counter(((e.get("resource") or {}).get("labels") or {}).get("component_name") or _url_name((e.get("logName") or "").replace("%2F", "/")) for e in entries)
        rep.add("  entries by component: " + ", ".join(f"{k} x{v}" for k, v in by_comp.most_common()))
        ctx.find("MED", f"{len(entries)} error-level control-plane / cluster log entries in window ({', '.join(str(k) for k, _ in by_comp.most_common(3))})")
        for e in entries[:MAX_CP_LOG_LINES][::-1]:
            ts = parse_ts(e.get("timestamp"))
            comp = ((e.get("resource") or {}).get("labels") or {}).get("component_name") or "cluster"
            rep.add(f"  {ts:%H:%M:%S}Z [{comp[:24]}] {_log_text(e)[:200]}" if ts else f"  [{comp}] {_log_text(e)[:200]}")
            if ts:
                ctx.happened(ts, f"CONTROL PLANE {comp[:24]}: {_log_text(e)[:100]}")
    # cluster autoscaler visibility events
    entries, err = q_ca.result()
    rep.sub(f"Cluster autoscaler events (last {mins} minutes, from the Cloud Logging cluster-autoscaler-visibility log)",
            "what the cluster autoscaler decided in the window: nodes added, nodes removed, and the reasons it could not scale up or down.", ["Cluster autoscaler"])
    if err or entries is None:
        rep.add(f"  unavailable: {_first_line(err or 'no data', 120)}")
    elif not entries:
        rep.add("  no autoscaler events in the window (or autoscaler visibility logs are off).")
    else:
        kinds, problems = Counter(), []
        for e in entries:
            jp = e.get("jsonPayload") or {}
            ts = parse_ts(e.get("timestamp"))
            dec = jp.get("decision") or {}
            if dec.get("scaleUp"):
                kinds["scale up"] += 1
                if ts:
                    ctx.happened(ts, "AUTOSCALER scale up: " + ", ".join(f"{(m.get('mig') or {}).get('name', '?')} +{m.get('requestedNodes', '?')}" for m in dec["scaleUp"].get("increasedMigs") or [])[:100])
            if dec.get("scaleDown"):
                kinds["scale down"] += 1
                if ts:
                    ctx.happened(ts, "AUTOSCALER scale down: " + ", ".join(str((n.get("node") or {}).get("name", "?")) for n in dec["scaleDown"].get("nodesToBeRemoved") or [])[:100])
            nd = jp.get("noDecisionStatus") or {}
            if nd.get("noScaleUp"):
                kinds["no scale up"] += 1
                groups = (nd["noScaleUp"].get("unhandledPodGroups") or [])
                reasons = {r.get("reason", {}).get("messageId") for g in groups for r in g.get("rejectedMigs") or []} | \
                          {(g.get("napFailureReasons") or [{}])[0].get("messageId") for g in groups if g.get("napFailureReasons")}
                reasons.discard(None)
                problems.append((ts, f"cannot scale up for {len(groups)} pod group(s): {', '.join(sorted(reasons)) or 'reason not given'}"))
            if nd.get("noScaleDown") and (nd["noScaleDown"].get("nodes") or nd["noScaleDown"].get("reason")):
                kinds["no scale down"] += 1
            for r in (jp.get("resultInfo") or {}).get("results") or []:
                em = r.get("errorMsg")
                if em:
                    kinds["error"] += 1
                    problems.append((ts, f"autoscaler error {em.get('messageId')} {' '.join(str(x) for x in em.get('parameters') or [])}".strip()))
        rep.add("  events: " + (", ".join(f"{k} x{v}" for k, v in kinds.most_common()) or f"{len(entries)} other"))
        for ts, text in problems[:10]:
            rep.add(f"  {ts:%H:%M:%S}Z {text[:170]}" if ts else f"  {text[:170]}")
            if ts:
                ctx.happened(ts, "AUTOSCALER " + text[:100])
        if kinds.get("error"):
            ctx.find("HIGH", f"Cluster autoscaler reported {kinds['error']} error(s) in the window, e.g. {next((t for _, t in problems if 'error' in t), '')[:90]}")
        if kinds.get("no scale up"):
            ctx.find("MED", f"Cluster autoscaler could not scale up ({kinds['no scale up']} event(s)): " + next((t for _, t in problems if 'cannot scale up' in t), '')[:90])
    # audit denials
    entries, err = q_au.result()
    rep.sub("Denied and failed Kubernetes and Google Kubernetes Engine API calls (audit log)",
            "calls that were refused for missing permission or login (codes 7 and 16) or that failed on the GKE administration API in the window, grouped by who made them.")
    if err or entries is None:
        rep.add(f"  Kubernetes audit denials unavailable: {_first_line(err or 'no data', 100)}")
    elif entries:
        agg = Counter()
        for e in entries:
            pp = e.get("protoPayload") or {}
            agg[((pp.get("authenticationInfo") or {}).get("principalEmail") or "?", (pp.get("methodName") or "?").replace("io.k8s.", ""), (pp.get("status") or {}).get("code"))] += 1
        rep.add(f"  Kubernetes API requests DENIED (PERMISSION_DENIED 7 / UNAUTHENTICATED 16) in the window: {len(entries)} (top callers)")
        rep.table(["PRINCIPAL", "METHOD", "CODE", "COUNT"], [[u, m, k, n] for (u, m, k), n in agg.most_common(10)],
                  about="The callers whose Kubernetes API requests were denied in the window: the user or service account, the method, the result code (7 = permission denied, 16 = not authenticated) and how often it happened.")
        ctx.find("MED", f"{len(entries)} Kubernetes API request(s) denied (PERMISSION_DENIED / UNAUTHENTICATED) in window, e.g. {agg.most_common(1)[0][0][0]}")
    else:
        rep.add("  no PERMISSION_DENIED / UNAUTHENTICATED entries for the Kubernetes API in the audit log for the window.")
    entries, err = q_ad.result()
    if err or entries is None:
        rep.add(f"  GKE admin API failures unavailable: {_first_line(err or 'no data', 100)}")
    elif entries:
        agg = Counter()
        for e in entries:
            pp = e.get("protoPayload") or {}
            agg[((pp.get("authenticationInfo") or {}).get("principalEmail") or "?", (pp.get("methodName") or "?").rsplit(".", 1)[-1], (pp.get("status") or {}).get("code"), (pp.get("status") or {}).get("message", "")[:70])] += 1
        rep.add(f"  GKE admin API calls that FAILED (audit log): {len(entries)}")
        rep.table(["PRINCIPAL", "METHOD", "CODE", "MESSAGE", "COUNT"], [[u, m, k, msg, n] for (u, m, k, msg), n in agg.most_common(10)],
                  about="The Google Kubernetes Engine administration API calls that failed in the window: who made them, the method, the result code, the error message and how often it happened.")
        ctx.find("MED", f"{len(entries)} failed GKE admin API call(s) in window, e.g. {agg.most_common(1)[0][0][1]} ({agg.most_common(1)[0][0][3]})")
    else:
        rep.add("  no failed GKE admin API calls in the audit log for the window.")


def section_overview(rep, ctx, label):
    rep.section(f"1. CLUSTER OVERVIEW - {label}")
    rep.add(f"Report time (UTC): {ctx.now:%Y-%m-%d %H:%M:%S}   Window: last {ctx.minutes} minutes "
            f"(since {ctx.since:%H:%M:%S} UTC)")
    rep.sub("Cluster connection, versions and API server readiness",
            "which kubectl context this report was made with, the client and server versions of Kubernetes, and whether the Kubernetes API server says it is ready (a failing readiness check is listed by name).",
            ["kubectl", "API server", "Control plane"])
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

def fetch_usage(ctx, rep, top_calls=None):
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
    if top_calls:                                    # already running (started with the resource reads)
        ok, out = top_calls["nodes"].result()
    else:
        ok, out = kubectl(["top", "nodes", "--no-headers"])
    if ok:
        for line in out.splitlines():
            p = line.split()
            if len(p) >= 5:  # NAME CPU(cores) CPU% MEMORY(bytes) MEMORY%
                top_nodes[p[0]] = {"cpu": parse_cpu(p[1]), "mem": parse_mem(p[3])}
    else:
        top_err = out.splitlines()[0][:140] if out else "unknown error"
    ok, out = top_calls["pods"].result() if top_calls else kubectl(["top", "pods", "-A", "--no-headers"])
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
    """The 'actual server' behind a Kubernetes node on GKE: the Compute Engine instance (its numeric id when the GCP
    section could read it, else its name), zone, machine type, spot / preemptible / on-demand, node pool and IP.
    providerID looks like gce://<project>/<zone>/<instance-name>"""
    meta, spec, st = node["metadata"], node.get("spec", {}), node.get("status", {})
    labels = meta.get("labels", {})
    pid = spec.get("providerID", "") or ""
    m = re.match(r"gce://([^/]+)/([^/]+)/([^/]+)$", pid)
    project, pzone, inst = m.groups() if m else (None, None, None)
    vm_name = inst or "-"
    addresses = {a.get("type"): a.get("address") for a in st.get("addresses", []) or []}
    vms = ctx.data.get("vm_info") or {}
    info = vms.get(meta["name"].lower()) or vms.get((inst or "").lower()) or {}
    if (labels.get("cloud.google.com/gke-spot") or "").lower() == "true" or info.get("prio") == "SPOT":
        capacity = "spot"
    elif (labels.get("cloud.google.com/gke-preemptible") or "").lower() == "true" or info.get("prio") == "PREEMPTIBLE":
        capacity = "preemptible"
    else:
        capacity = "on-demand"
    return {
        "instance_id": info.get("id") or inst or "-", "provider_id": pid or "-", "project": project or "-",
        "zone": labels.get("topology.kubernetes.io/zone") or labels.get("failure-domain.beta.kubernetes.io/zone") or pzone or info.get("zone") or "-",
        "type": labels.get("node.kubernetes.io/instance-type") or labels.get("beta.kubernetes.io/instance-type") or info.get("type") or "-",
        "capacity": capacity,
        "nodegroup": labels.get("cloud.google.com/gke-nodepool") or "-",
        "ip": addresses.get("InternalIP", "-"), "vm_name": vm_name, "vm_state": info.get("state") or "-",
    }


def node_idents(ctx):
    cache = ctx.data.get("_node_idents")
    if cache is None:
        cache = {n["metadata"]["name"]: node_identity(ctx, n) for n in items(ctx.data.get("nodes"))}
        ctx.data["_node_idents"] = cache
    return cache


def node_tag(ctx, name):
    """'gke-prod-pool-1-abc [123456789012]' - the node name with its Compute Engine instance id (when it differs from the name)."""
    ident = node_idents(ctx).get(name)
    return f"{name} [{ident['instance_id']}]" if ident and ident["instance_id"] not in ("-", name) else (name or "-")


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
    rep.section("3. NODES - STATUS, PROCESSOR, MEMORY, DISK, SWAP")
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
        nlabel = f"{name} [{ident['instance_id']}]" if ident["instance_id"] not in ("-", name) else name
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
        inv_rows.append([name, ident["instance_id"], ident["vm_name"], ident["zone"], ident["type"], ident["capacity"],
                         ident["nodegroup"], ident["ip"], status, ident["provider_id"]])
        usage_rows.append([
            name, ident["instance_id"], status, f"{running[name]}/{max_pods or '?'}" + (f" (+{active[name] - running[name]} pending)" if active[name] > running[name] else ""),
            _res(u["cpu"], a_cpu, _cores), _res(u["mem"], a_mem, fmt_gib), disk_text,
            _res(u["img_used"], u["img_cap"], fmt_gib) if u["img_used"] is not None else "n/a", swap_text])
        info_rows.append([name, ident["instance_id"], roles, ident["type"], ident["zone"],
                          st.get("nodeInfo", {}).get("kubeletVersion", "-"), age(created, ctx.now),
                          _fp(req_cpu_pct), _fp(req_mem_pct), _res(eph_req, a_eph, fmt_gib) if a_eph else "-", "; ".join(flags)])

    rep.add(f"{len(nodes)} node(s), {bad} with findings.")
    rep.sub("Node inventory: every node and the virtual machine behind it",
            "which Compute Engine virtual machine each node is (the instance identifier when Google Cloud could be read, else the instance name, and the name from the provider identifier gce://project/zone/instance), with its zone, machine type, node pool and status.",
            ["Compute Engine", "Instance identifier", "Provider identifier", "Node pool", "NotReady"])
    rep.table(["NODE", "INSTANCE ID", "GCE INSTANCE", "ZONE", "MACHINE TYPE", "CAPACITY", "NODE POOL", "INTERNAL IP", "STATUS", "PROVIDER-ID"], inv_rows, maxw=64,
              about="One row per worker node: its name, the Compute Engine virtual machine it runs on (instance identifier and name), zone, machine type, whether it is standard or spot capacity, node pool, internal IP address, Ready status and provider identifier.")
    rep.sub("Live usage per node", "how much processor, memory, disk, image disk and swap each node uses right now compared with what it can offer to pods, and how many pods run on it out of the maximum.",
            ["Allocatable", "Image filesystem", "Swap", "Working set"])
    rep.table(["NODE", "INSTANCE ID", "STATUS", "PODS", "CPU used/alloc", "MEMORY used/alloc", "DISK used/total", "IMAGEFS", "SWAP"],
              usage_rows, maxw=40,
              about="One row per worker node: its status, running pods out of the maximum, processor and memory used out of allocatable, root disk and image disk used out of the total, and swap; values at 75 percent or more are orange and at 90 percent or more red.")
    rep.sub("Scheduling view: what the pods request on each node", "how much processor and memory the pods on each node have reserved (requested) compared with what the node can allocate, plus node roles, versions, age and findings.",
            ["Request", "Allocatable", "Ephemeral storage"])
    rep.table(["NODE", "INSTANCE ID", "ROLES", "MACHINE TYPE", "ZONE", "VERSION", "AGE", "CPUreq", "MEMreq", "EPHEMERAL", "FINDINGS"],
              info_rows, maxw=110,
              about="One row per worker node: its roles, machine type, zone, Kubernetes version and age, the share of processor, memory and temporary disk space that pods have requested on it, and every finding for the node.")
    notready = [(n["metadata"]["name"], node_idents(ctx).get(n["metadata"]["name"]) or {}) for n in nodes
                if not any(c["type"] == "Ready" and c["status"] == "True" for c in n.get("status", {}).get("conditions", []))]
    if notready:
        rep.sub("How to read the logs of a node that is not Ready", "kubectl cannot read node logs, so these are the Google Cloud ways to read them for the nodes that are not Ready; this tool does not run them.")
        rep.add("Node logs are not readable through kubectl. For the NotReady node(s) you can read them through GCP (nothing is changed):")
        for node_name, ident in notready[:5]:
            rep.add(f"  {node_name}:  gcloud compute instances get-serial-port-output {ident.get('vm_name') or node_name} --zone {ident.get('zone') or '<zone>'} "
                    f"--project {ident.get('project') or '<project>'}")
        rep.add("  or:  kubectl debug node/<node> -it --image=busybox   (creates a debug pod)")
        rep.add("  and: Cloud Logging -> resource.type=\"gce_instance\" for the instance (kubelet / container runtime logs), Compute Engine -> VM instances -> the node -> Serial console")
    if not ctx.data.get("node_stats"):
        rep.add("")
        rep.add("Note: disk and swap come from the kubelet and need the 'nodes/proxy' permission"
                + (f" ({ctx.data.get('stats_error')})" if ctx.data.get("stats_error") else "")
                + ". Without it CPU/memory come from metrics-server (kubectl top) when installed.")


def section_node_pods(rep, ctx):
    rep.section("5. PODS ON EACH NODE - PROCESSOR, MEMORY, DISK per pod")
    pods = items(ctx.data.get("pods"))
    usage = ctx.data.get("pod_usage") or {}
    by_node = defaultdict(list)
    for p in pods:
        if p.get("spec", {}).get("nodeName") and p.get("status", {}).get("phase") in ("Running", "Pending"):
            by_node[p["spec"]["nodeName"]].append(p)
    if not by_node:
        rep.add("No pods are scheduled on nodes.")
        return
    rep.sub("How to read the pod tables", "what the usage, request and limit columns mean: usage is measured now, a request is what the pod reserved and a limit is the most it may use. Memory usage is the working set.",
            ["Request", "Limit", "Working set", "OOMKilled"])
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
        running = sum(1 for p in by_node[node] if p.get("status", {}).get("phase") == "Running")
        ident = node_idents(ctx).get(node) or {}
        who = (f"  [{ident.get('instance_id', '-')} | {ident.get('zone', '-')} | {ident.get('type', '-')}"
               + (f" | VM {ident['vm_name']}" if ident.get("vm_name", "-") not in ("-", node) else "") + "]") if ident else ""
        rep.sub(f"Node {node}{who}: {len(by_node[node])} pod(s) ({running} running)",
                "the pods placed on this node, the biggest memory users first: status, restarts, processor, memory and disk usage against their requests and limits, and notes when a pod is close to a limit.")
        rep.table(["POD", "SUPPORT DL", "STATUS", "RST", "CPU use", "CPU req", "CPU lim", "MEM use", "MEM req", "MEM lim", "DISK use", "NOTES"],
                  [r[1] for r in rows], limit=MAX_NODE_PODS, maxw=60,
                  about=f"One row per pod on node {node}: namespace and pod name, the support distribution list, status, restarts, processor, memory and disk used now, and the requests and limits of processor and memory.")


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
    rep.sub("Who to contact for each namespace",
            f"the support team of every namespace, read from the namespace label '{SUPPORT_LABEL}' (the same as: kubectl get namespaces -l {SUPPORT_LABEL}); NOT SET means the namespace has no label and no team to contact.",
            ["Namespace", "Distribution list"])
    rep.table(["NAMESPACE", f"SUPPORT DL", "PODS", "RUNNING"], owners, maxw=70,
              about="One row per namespace: its support distribution list (the team to contact), how many pods it has and how many of them are running.")
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
    rep.sub("Pods used versus configured per namespace",
            "for every namespace, how many pods exist and in which phase compared with how many its workloads are configured to run (desired replicas of Deployments, StatefulSets and DaemonSets plus standalone pods), and how much of its pod quota is used.",
            ["Pending", "ResourceQuota", "Deployment", "StatefulSet"])
    rep.table(["NAMESPACE", "SUPPORT DL", "PODS", "RUNNING", "PENDING", "FAILED", "COMPLETED", "CONFIGURED", "STATUS", "POD QUOTA", "NOTES"], rows, maxw=60,
              about="One row per namespace: pods by phase (running, pending, failed, completed), how many pods are configured, whether any configured pod is missing, the pod quota used out of its limit, and notes.")

    res_rows = []
    for n, d in sorted(ns.items(), key=lambda kv: -kv[1]["mem_use"]):
        if not d["total"]:
            continue
        res_rows.append([n, support_of(ctx, n) or "-", d["total"], _cores(d["cpu_use"]) if d["has_use"] else "n/a", _cores(d["cpu_req"]) if d["cpu_req"] else "-",
                         _mi(d["mem_use"]) if d["has_use"] else "n/a", _mi(d["mem_req"]) if d["mem_req"] else "-",
                         _mi(d["disk_use"]) if d["has_use"] and d["disk_use"] else "n/a", d["restarts"]])
    rep.sub("Resources used by each namespace's pods", "the processor, memory and disk that the pods of each namespace use now compared with what they requested, and how often their containers restarted; the biggest memory users first.",
            ["Request", "Working set"])
    rep.table(["NAMESPACE", "SUPPORT DL", "PODS", "CPU use", "CPU req", "MEM use", "MEM req", "DISK use", "RESTARTS"], res_rows,
              about="One row per namespace that has pods: how many pods, the processor, memory and disk used now, the processor and memory requested, and the total number of container restarts.")

    wl_rows.sort(key=lambda r: (r[8] == "OK", r[0], r[2]))
    if wl_rows:
        rep.sub(f"Workloads behind those pods ({len(wl_rows)})", "the Deployments, StatefulSets and DaemonSets of each namespace: the replicas configured (desired) compared with the pods running, ready and available now, and the autoscaler limits if it has one.",
                ["Deployment", "StatefulSet", "DaemonSet", "Horizontal pod autoscaler"])
        rep.table(["NAMESPACE", "SUPPORT DL", "KIND", "NAME", "DESIRED", "READY", "AVAILABLE", "RUNNING", "HPA min-max", "STATUS"],
                  [[r[0], support_of(ctx, r[0]) or "-"] + r[1:] for r in wl_rows],
                  about="One row per workload: its namespace and support team, its kind and name, the replicas desired, ready, available and running, the horizontal pod autoscaler minimum and maximum, and its status; workloads that are not at their desired state come first.")

    quota_rows = []
    for n in sorted(quotas):
        for qname, key, used, hard in quotas[n]:
            pct = _pct(used, hard)
            if pct is not None and pct >= 75:
                ctx.find("HIGH" if pct >= 90 else "MED", f"Namespace {n}{support_suffix(ctx, [n])}: quota '{qname}' {key} is {pct:.0f}% used ({_fmt_qty(key, used)}/{_fmt_qty(key, hard)})")
                ctx.ns_issue(n, f"quota '{qname}' {key} is {pct:.0f}% used")
            quota_rows.append([n, support_of(ctx, n) or "-", qname, key, f"{_fmt_qty(key, used)}/{_fmt_qty(key, hard)} ({_fp(pct)})"])
    if quota_rows:
        rep.sub("Resource quotas", "the limits set per namespace on pods, processor, memory or storage, and how much of each is already used.", ["ResourceQuota"])
        rep.table(["NAMESPACE", "SUPPORT DL", "QUOTA", "RESOURCE", "USED / LIMIT"], quota_rows,
                  about="One row per quota item: the namespace and its support team, the quota name, which resource it limits, and how much is used out of the limit with the percentage.")


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
    rep.sub(f"Pods with problems ({len(bad)})", "every pod that is crash looping, not ready, waiting for an image, pending, failed or evicted, the most serious first, with the most likely reason.",
            ["CrashLoopBackOff", "ImagePullBackOff", "OOMKilled", "Pending", "Evicted"])
    rep.table(["NAMESPACE/POD", "SUPPORT DL", "STATUS", "READY", "RESTARTS", "NODE", "AGE", "WHY"],
              [[f"{a['ns']}/{a['name']}", support_of(ctx, a["ns"]) or "-", a["status"], a["ready"], a["restarts"], node_tag(ctx, a["node"]), a["age"],
                "; ".join(dict.fromkeys(a["problems"]))[:200]] for a in bad], maxw=110,
              about="One row per unhealthy pod: namespace and pod name, the support team, its status, how many containers are ready, the restarts, the node it runs on, its age and the reasons found.")
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
        rep.sub("Warning events by reason", f"how often each kind of Warning event happened in the last {ctx.minutes} minutes; a high count of one reason points at a repeating problem.", ["Warning event"])
        rep.table(["REASON", "OCCURRENCES"], [[r, c] for r, c in count.most_common()],
                  about="One row per event reason: the reason code Kubernetes gave and the number of Warning events with that reason in the window, the most frequent first.")
        rep.sub(f"Latest Warning events (up to {MAX_EVENTS})", "the newest Warning events one by one, with the object they are about and the message Kubernetes wrote.")
        rows = []
        for t, e in sorted(warnings, key=lambda x: x[0], reverse=True)[:MAX_EVENTS]:
            obj = e.get("involvedObject") or e.get("regarding") or {}
            obj_name = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else obj.get("name", "?")
            rows.append([age(t, ctx.now) + " ago", e.get("reason", "?"),
                         f"{obj.get('kind', '?')} {obj.get('namespace', '')}/{obj_name}".replace(" /", " "),
                         support_of(ctx, obj.get("namespace")) or "-",
                         ((e.get("series") or {}).get("count") or e.get("count") or 1),
                         (e.get("message") or e.get("note") or "").replace("\n", " ")[:140]])
        rep.table(["WHEN", "REASON", "OBJECT", "SUPPORT DL", "COUNT", "MESSAGE"], rows, limit=MAX_EVENTS,
                  about="One row per Warning event, newest first: how long ago it happened, its reason, the object it is about (kind, namespace and name), the support team of the namespace, how many times it repeated and the message.")
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
        rep.sub("Notable normal events (scaling, kills, node changes)", "events that are not warnings but explain changes in the window, such as scaling, containers being killed and node changes.")
        rows = []
        for t, e in sorted(notable, key=lambda x: x[0], reverse=True)[:25]:
            obj = e.get("involvedObject") or {}
            shown = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else obj.get("name", "?")
            rows.append([age(t, ctx.now) + " ago", e.get("reason"), f"{obj.get('kind', '?')} {shown}",
                         support_of(ctx, obj.get("namespace")) or "-",
                         (e.get("message") or "").replace("\n", " ")[:110]])
            ctx.happened(t, f"EVENT(normal) {e.get('reason')} {obj.get('kind', '?')} {shown}: "
                            f"{(e.get('message') or '')[:90]}")
        rep.table(["WHEN", "REASON", "OBJECT", "SUPPORT DL", "MESSAGE"], rows, limit=25,
                  about="One row per notable normal event, newest first: how long ago, its reason, the object it is about, the support team and the message.")


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
        rep.sub(f"Workloads that are not fully ready ({len(rows)})", "Deployments, StatefulSets and DaemonSets that have fewer ready pods than they want, with the reason.", ["Deployment", "StatefulSet", "DaemonSet", "Ready"])
        for r in rows:
            ctx.ns_issue(r[1].split("/")[0], f"{r[0]} {r[1].split('/', 1)[1]} not fully ready ({r[2]})")
        rep.table(["KIND", "NAMESPACE/NAME", "SUPPORT DL", "READY", "ISSUE"],
                  [[r[0], r[1], support_of(ctx, r[1].split("/")[0]) or "-", r[2], r[3]] for r in rows],
                  about="One row per workload that is not at its desired state: its kind, namespace and name, the support team, ready pods out of desired, and the issue found.")
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
        rep.sub(f"Recent rollouts and scale changes (new ReplicaSets in the last {ctx.minutes} minutes)", "Deployments that rolled out a new version or changed size in the window; every rollout creates a new ReplicaSet.", ["Deployment", "ReplicaSet"])
        rep.table(["DEPLOYMENT", "SUPPORT DL", "REPLICASET", "READY", "CREATED"], recent,
                  about="One row per new ReplicaSet in the window: the deployment it belongs to, the support team, the ReplicaSet name, its ready pods out of desired and how long ago it was created.")
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
        rep.sub("Jobs that failed in the window", "batch jobs that ended in failure in the selected window and why.", ["Job"])
        rep.table(["JOB", "SUPPORT DL", "FAILED PODS", "REASON", "WHEN"], failed,
                  about="One row per failed job: its namespace and name, the support team, how many of its pods failed, the failure reason and how long ago it failed.")
        for r in failed:
            ctx.ns_issue(r[0].split("/")[0], f"job {r[0].split('/', 1)[1]} failed ({r[3]})")
        ctx.find("HIGH", f"{len(failed)} Job(s) failed in window" + support_suffix(ctx, {r[0].split("/")[0] for r in failed}))

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
    cand = [("kube-dns", "kube-dns"), ("konnectivity-agent", "konnectivity-agent"), ("gke-metadata-server", "gke-metadata-server"), ("kube-proxy", "kube-proxy"),
            ("netd", "netd"), ("anetd", "anetd"), ("Calico", "calico"), ("CSI", "csi")]
    core_terms = ["kube-system"] + [t for t, sub_name in cand if any(sub_name in c[1].lower() for c in core)]
    rep.sub("Core add-ons (kube-system)", "the Kubernetes and GKE add-ons that every cluster needs (DNS, networking, storage, metrics), as Deployments and DaemonSets in the kube-system namespace, and whether they are fully ready.",
            core_terms + ["DaemonSet", "Deployment"])
    rep.table(["KIND", "NAME", "READY", "STATE"], core,
              about="One row per Deployment or DaemonSet in the kube-system namespace: its kind, name, ready pods out of desired, and OK or DEGRADED.")


# ---------------------------------------------------------------------------
# Network & traffic: CNI / DNS / services / ingress / policies / routing, and traffic in the selected window
# ---------------------------------------------------------------------------

import ipaddress

TRAFFIC_SAMPLE_SECONDS = 10      # live traffic sample from the kubelet (0 = skip); the WINDOW traffic comes from Cloud Monitoring
NET_EVENT_PATTERN = re.compile(
    r"network|cni|sandbox|ip address|ip addresses|ip_space|ipspace|exhaust|insufficientfreeaddresses|\bdns\b|\broute|loadbalancer|"
    r"connection refused|i/o timeout|no route|unreachable|failed to (assign|allocate)|firewall|syncloadbalancer|\bneg\b", re.I)


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


CNI_COMPONENTS = [  # (name or name prefix in kube-system, what it is)
    ("netd", "GKE node networking daemon (Container Network Interface setup, routes, masquerade) on clusters without Dataplane V2"),
    ("anetd", "Dataplane V2 agent (Cilium / eBPF): networking, service load balancing and network policy"),
    ("cilium", "Cilium agent"),
    ("calico-node", "Calico network policy enforcement"),
    ("calico-typha", "Calico Typha (policy fan-out)"),
    ("ip-masq-agent", "Source address translation rules for traffic leaving the pod range (non-masquerade ranges)"),
    ("kube-dns", "Cluster Domain Name System (kube-dns with dnsmasq + sidecar)"),
    ("kube-dns-autoscaler", "Scales kube-dns with the cluster size"),
    ("node-local-dns", "NodeLocal DNS cache on every node"),
    ("konnectivity-agent", "Tunnel from the managed control plane to the nodes"),
    ("gke-metadata-server", "Workload Identity metadata server"),
    ("l7-default-backend", "Default backend (404) of the GKE Ingress load balancer"),
    ("kube-proxy", "Service load balancing on each node (not present with Dataplane V2)"),
]

NODELOCAL_DNS_NODES = 50          # from this many nodes on, NodeLocal DNS cache is recommended
GKE_HEALTH_CHECK_RANGES = ("130.211.0.0/22", "35.191.0.0/16")
_STATE_RANK = {"OK": 0, "Not available": 1, "Warning": 2, "Problem": 3}


# --- small helpers for the network checks ------------------------------------------------------------------------

def _kj(args):
    """kubectl get ... -o json that never raises: (data_or_None, error_or_None)."""
    try:
        return kjson(args)
    except Exception as exc:
        return None, str(exc)


def _kt(args, timeout=90):
    """kubectl that never raises: (ok, text)."""
    try:
        return kubectl(args, timeout=timeout)
    except Exception as exc:
        return False, str(exc)


def _img_tag(image):
    """'gke.gcr.io/netd:v1.2@sha256:...' -> 'v1.2'."""
    img = str(image or "").split("@")[0].rsplit("/", 1)[-1]
    return img.split(":", 1)[1] if ":" in img else (img or "?")


def _pod_restarts(pod):
    return sum(int(c.get("restartCount") or 0) for c in (pod.get("status", {}).get("containerStatuses") or []))


def _pod_ready(pod):
    return any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod.get("status", {}).get("conditions") or [])


def _ks_pods(ctx, *prefixes, label=None):
    """kube-system pods whose name starts with one of the prefixes (or that carry the k8s-app / component label)."""
    out = []
    for p in items(ctx.data.get("pods")):
        meta = p["metadata"]
        if meta.get("namespace") != "kube-system":
            continue
        lbl = meta.get("labels") or {}
        if any(meta["name"] == x or meta["name"].startswith(x + "-") for x in prefixes) or (label and label in (lbl.get("k8s-app"), lbl.get("component"))):
            out.append(p)
    return out


def _workload(ctx, name, namespace="kube-system"):
    """(kind, ready, wanted, image tag) of a DaemonSet or Deployment, or None."""
    for kind, key in (("DaemonSet", "daemonsets"), ("Deployment", "deployments")):
        for o in items(ctx.data.get(key)):
            if o["metadata"]["name"] == name and o["metadata"]["namespace"] == namespace:
                st, spec = o.get("status", {}), o.get("spec", {})
                cont = (spec.get("template", {}).get("spec", {}).get("containers") or [{}])[0]
                if kind == "DaemonSet":
                    return kind, st.get("numberReady", 0), st.get("desiredNumberScheduled", 0), _img_tag(cont.get("image"))
                return kind, st.get("readyReplicas", 0), spec.get("replicas", 0), _img_tag(cont.get("image"))
    return None


def _is_dataplane_v2(ctx):
    """True / False when known (from the cluster object, or from anetd being present), None when unknown."""
    cluster = ctx.data.get("gcp_cluster") or {}
    if cluster:
        return (cluster.get("networkConfig") or {}).get("datapathProvider") == "ADVANCED_DATAPATH"
    if _workload(ctx, "anetd"):
        return True
    return None


def _combine(states):
    """The worst of several states: Problem > Warning > Not available > OK. Nothing to combine = Not available."""
    states = [s for s in states if s]
    return max(states, key=lambda s: _STATE_RANK[s]) if states else "Not available"


def _net_status(rep, ctx, key, state, evidence, meaning, find=None):
    """Print the status block of a check and remember it for the traffic issue checklist. find='title' also raises a finding."""
    ctx.add_check(key, state, evidence)
    rep.status(state, evidence, meaning)
    if find and state in ("Problem", "Warning"):
        ctx.find("HIGH" if state == "Problem" else "MED", f"{find}: {evidence}"[:230])


def _read_logs(ctx, selector, namespace="kube-system"):
    """The recent log lines of the pods with a label (read-only `kubectl logs`): (lines, None) or (None, error)."""
    ok, out = _kt(["logs", "-n", namespace, "-l", selector, "--all-containers", "--prefix", f"--since={ctx.minutes}m", "--tail=800",
                   "--max-log-requests=20"], timeout=90)
    if not ok:
        return None, (out.splitlines()[0][:100] if out else "kubectl logs failed")
    return out.splitlines(), None


# --- 10.0 how to read -----------------------------------------------------------------------------------------

def _net_intro(rep, ctx):
    rep.sub("How to read this section", "the legend of the status labels and the rules this tool follows while collecting the network picture.")
    rep.add("  Status labels: OK = checked and healthy.  Warning = worth a look.  Problem = a likely cause of traffic trouble.")
    rep.add("                 Not available = could NOT be checked (the reason is shown); this is never a pass.")
    rep.add("  Everything is read-only: no pod exec, no node login and no packet capture is performed. Short technical terms are explained in a")
    rep.add("  small glossary table right before the block that uses them, and in one complete glossary at the end of this section.")


# --- 10.1 cluster network settings ------------------------------------------------------------------------------

def _net_cluster_settings(rep, ctx):
    rep.sub("Cluster network settings", "the main networking choices of the cluster: which dataplane it uses, its address ranges, cluster DNS and source address translation.",
            ["Dataplane V2", "Cilium", "kube-proxy", "SNAT", "CIDR", "kube-dns", "NetworkPolicy"])
    cluster = ctx.data.get("gcp_cluster") or {}
    nc, ip, np_ = cluster.get("networkConfig") or {}, cluster.get("ipAllocationPolicy") or {}, cluster.get("networkPolicy") or {}
    services = {(s["metadata"]["namespace"], s["metadata"]["name"]): s for s in items(ctx.data.get("services"))}
    dns_svc = services.get(("kube-system", "kube-dns"), {})
    k8s_svc = services.get(("default", "kubernetes"), {})
    daemonsets = {d["metadata"]["name"]: d for d in items(ctx.data.get("daemonsets")) if d["metadata"]["namespace"] == "kube-system"}
    v2 = nc.get("datapathProvider") == "ADVANCED_DATAPATH" or (not cluster and "anetd" in daemonsets)
    if cluster or daemonsets:
        rep.add(f"  Dataplane                 : {'Dataplane V2 (Cilium / eBPF, anetd) - no kube-proxy' if v2 else 'legacy dataplane (kube-proxy iptables + netd)'}   "
                f"network policy {np_.get('provider') if np_.get('enabled') else ('built into Dataplane V2' if v2 else 'off') if cluster else '(Google Cloud section off)'}   "
                f"intra-node visibility {bool(nc.get('enableIntraNodeVisibility')) if cluster else '-'}")
    rep.add(f"  Service address range     : {ip.get('servicesIpv4CidrBlock') or cluster.get('servicesIpv4Cidr') or '-'}   (kubernetes service IP address {k8s_svc.get('spec', {}).get('clusterIP') or '?'})   "
            f"stack {ip.get('stackType') or 'IPV4'}")
    dns_mode = (cluster.get("dnsConfig") or {}).get("clusterDns") or "PLATFORM_DEFAULT"
    rep.add(f"  Cluster DNS (kube-dns)    : service IP address {dns_svc.get('spec', {}).get('clusterIP', '?')}  ports "
            f"{','.join(str(p.get('port')) + '/' + p.get('protocol', '') for p in dns_svc.get('spec', {}).get('ports', [])) or '?'}   "
            f"provider {'kube-dns' if dns_mode == 'PLATFORM_DEFAULT' else 'Cloud DNS (' + str((cluster.get('dnsConfig') or {}).get('clusterDnsScope') or 'cluster') + ' scope)'}")
    pod_cidrs = sorted({c for n in items(ctx.data.get("nodes")) for c in (n.get("spec", {}).get("podCIDRs") or [n.get("spec", {}).get("podCIDR")]) if c})
    rep.add(f"  Pod address range         : {ip.get('clusterIpv4CidrBlock') or cluster.get('clusterIpv4Cidr') or '-'}   per-node blocks: "
            f"{', '.join(pod_cidrs[:6]) + (' ...' if len(pod_cidrs) > 6 else '') if pod_cidrs else 'none set on the nodes'}")
    cm = (_kj(["get", "configmap", "ip-masq-agent", "-n", "kube-system"])[0])
    nonmasq = re.findall(r"nonMasqueradeCIDRs:\s*\n((?:\s*-\s*\S+\s*\n?)+)", (cm or {}).get("data", {}).get("config", "")) if cm else []
    rep.add("  Source address translation: " + (("custom ip-masq-agent config, non-masquerade ranges: " + ", ".join(re.findall(r"-\s*(\S+)", nonmasq[0]))[:100]) if nonmasq else
            ("ip-masq-agent config present" if cm else "default (GKE masquerades traffic to non-RFC1918 destinations)"))
            + (f"   default source address translation disabled={bool((nc.get('defaultSnatStatus') or {}).get('disabled'))}" if cluster else ""))
    rows = []
    resources = [("DaemonSet", d) for d in items(ctx.data.get("daemonsets"))] + [("Deployment", d) for d in items(ctx.data.get("deployments"))]
    for kind, obj in resources:
        if obj["metadata"]["namespace"] != "kube-system":
            continue
        name = obj["metadata"]["name"]
        best = max((c for c in CNI_COMPONENTS if name == c[0] or name.startswith(c[0] + "-")), key=lambda c: len(c[0]), default=None)
        if not best:
            continue
        st, spec = obj.get("status", {}), obj.get("spec", {})
        cont = (spec.get("template", {}).get("spec", {}).get("containers") or [{}])[0]
        if kind == "DaemonSet":
            ready, want = st.get("numberReady", 0), st.get("desiredNumberScheduled", 0)
        else:
            ready, want = st.get("readyReplicas", 0), spec.get("replicas", 0)
        rows.append([name, kind, f"{ready}/{want}", cont.get("image", "?").split("/")[-1][:50], "OK" if ready >= want else "DEGRADED", best[1]])
        if ready < want:
            ctx.find("HIGH", f"Network component kube-system/{name} is degraded ({ready}/{want} ready)")
    rep.add("")
    if rows:
        rep.table(["COMPONENT", "KIND", "READY", "IMAGE", "STATE", "WHAT IT DOES"], rows, maxw=70,
                  about="the networking add-ons that run in the kube-system namespace, how many copies are ready, which image version they run, and what each one does.")
    else:
        rep.add("  none of the usual GKE network components were found")
    if cluster and not v2 and not (ip.get("useIpAliases")):
        rep.add("  Note: this cluster is routes-based; pod traffic uses virtual private cloud custom routes (route quota applies).")


# --- A. pod-level networking -------------------------------------------------------------------------------------

def _net_cni_health(rep, ctx):
    rep.sub("Pod-level networking: Container Network Interface (CNI) plugin health",
            "whether the node networking agents that give pods their network (Dataplane V2 anetd, netd, Calico, ip-masq-agent) are running on every node, and which versions.",
            ["CNI", "Dataplane V2", "Cilium", "anetd", "netd", "Calico", "ip-masq-agent", "DaemonSet"])
    v2 = _is_dataplane_v2(ctx)
    cluster = ctx.data.get("gcp_cluster") or {}
    ip = cluster.get("ipAllocationPolicy") or {}
    if ctx.data.get("daemonsets") is None:
        _net_status(rep, ctx, "cni", "Not available", "the DaemonSets of kube-system could not be read",
                    "Check that the signed-in user may list DaemonSets (kubectl get daemonsets -n kube-system), then run again.")
        return
    rows, bad, states = [], [], []
    for name, what in (("anetd", "Dataplane V2 agent"), ("cilium", "Cilium agent"), ("netd", "GKE node networking daemon"), ("calico-node", "Calico policy agent"),
                       ("calico-typha", "Calico fan-out"), ("ip-masq-agent", "masquerade rules")):
        w = _workload(ctx, name)
        if not w:
            continue
        kind, ready, want, tag = w
        pods = _ks_pods(ctx, name)
        restarts = sum(_pod_restarts(p) for p in pods)
        state = "OK" if ready >= want and want > 0 else ("Problem" if ready < want else "Warning")
        if ready < want:
            bad.append(f"{name} {ready}/{want}")
        rows.append([name, kind, f"{ready}/{want}", tag, restarts, state, what])
        states.append(state)
    rep.table(["COMPONENT", "KIND", "PODS READY", "VERSION", "RESTARTS", "STATE", "ROLE"], rows, maxw=50,
              about="each networking agent found in kube-system with how many of its pods are ready (one per node expected), its image version, and its total restarts.")
    if v2 is True and not _workload(ctx, "anetd"):
        _net_status(rep, ctx, "cni", "Problem", "Dataplane V2 is enabled but no anetd DaemonSet was found",
                    "Pods cannot get reliable networking without the Dataplane V2 agent; check the cluster in the Google Cloud console and the kube-system events.", "CNI plugin health")
    elif not rows:
        _net_status(rep, ctx, "cni", "Not available", "no networking agent DaemonSet (anetd / netd / calico-node / ip-masq-agent) is visible in kube-system",
                    "On a GKE cluster without Dataplane V2 the Container Network Interface is built into the node image and has no DaemonSet to check; "
                    "use the node and pod checks below instead.")
    elif bad:
        _net_status(rep, ctx, "cni", "Problem", "not ready: " + ", ".join(bad),
                    "Pods on the nodes without a ready agent can stay in ContainerCreating or lose connectivity. Look at the agent pods (kubectl describe pod -n kube-system) "
                    "and their logs in the next blocks.")
    else:
        _net_status(rep, ctx, "cni", "OK", f"all {len(rows)} networking agent(s) are ready on every node that needs them: " + ", ".join(f"{r[0]} {r[2]}" for r in rows),
                    "No action needed. Compare the versions with the cluster version if you upgraded recently.")
    if cluster:
        mode = ("Dataplane V2 (eBPF, no bridge)" if v2 else ("VPC-native with alias IP addresses (no cbr0 bridge needed)" if ip.get("useIpAliases") else
                                                                "routes-based (kubenet): pods are attached through a Linux bridge, usually cbr0"))
    else:
        mode = "unknown (the Google Cloud section is off)"
    rep.add(f"  Bridge note (cbr0): pod networking mode is {mode}. The bridge interface itself lives on the node and cannot be read without node access (never done by this tool).")


def _net_ip_exhaustion(rep, ctx):
    rep.sub("Pod-level networking: IP address exhaustion (node subnet, pod range, service range)",
            "how many IP addresses are left in the three address ranges a cluster uses; when one runs out, new nodes, pods or services cannot be created.",
            ["Secondary range", "CIDR", "ClusterIP", "FailedCreatePodSandBox"])
    ranges = ctx.data.get("gcp_ranges")
    rows, states, worst = [], [], []
    kinds = {"nodes": "Node subnet (primary range)", "pods": "Pod range (secondary)", "services": "Service range (secondary)"}
    for r in ranges or []:
        total, used, free = r.get("total"), r.get("used"), r.get("free")
        pct = (100 * used / total) if total and used is not None else None
        state = "OK"
        if r["kind"] == "nodes" and free is not None:
            state = "Problem" if free < 10 else ("Warning" if free < LOW_SUBNET_IPS else "OK")
        elif pct is not None:
            limit_p, limit_w = (80, 70) if r["kind"] == "services" else (90, 75)
            state = "Problem" if pct >= limit_p else ("Warning" if pct >= limit_w else "OK")
        states.append(state)
        if state != "OK":
            worst.append(f"{r['name']} {state.lower()}")
        rows.append([kinds.get(r["kind"], r["kind"]), r["name"], r.get("cidr") or "-", total if total is not None else "-", used if used is not None else "-",
                     free if free is not None else "-", f"{pct:.0f}%" if pct is not None else "-", state])
    if rows:
        rep.table(["RANGE KIND", "NAME", "ADDRESS RANGE", "TOTAL ADDRESSES", "USED BY THIS CLUSTER", "FREE (ESTIMATED)", "PERCENT USED", "RESULT"], rows, maxw=40,
                  about="for every range: how many addresses it holds, how many this cluster uses (nodes = VMs, pods = per-node blocks reserved from the pod range, services = ClusterIP "
                        "addresses), how many are free and a result. Pod ranges are consumed in blocks per node, so they fill up faster than the pod count suggests.")
    pods = items(ctx.data.get("pods"))
    host_net = sum(1 for p in pods if p.get("spec", {}).get("hostNetwork"))
    with_ip = [p for p in pods if p.get("status", {}).get("podIP") and not p.get("spec", {}).get("hostNetwork") and p.get("status", {}).get("phase") in ("Running", "Pending")]
    rep.add(f"  Pods holding a pod IP address: {len(with_ip)}; pods on the node's own network (hostNetwork): {host_net}.")
    nets = []
    for r in ranges or []:
        if r["kind"] == "pods":
            try:
                nets.append((r["name"], ipaddress.ip_network(r["cidr"], strict=False), r["free"]))
            except (TypeError, ValueError):
                pass
    cl = ctx.data.get("gcp_cluster") or {}
    if not nets:
        for pref in {(cl.get("ipAllocationPolicy") or {}).get("clusterIpv4CidrBlock") or cl.get("clusterIpv4Cidr")} - {None}:
            try:
                nets.append(("cluster pod range", ipaddress.ip_network(pref, strict=False), None))
            except (TypeError, ValueError):
                pass
    if nets:
        used, by_ns = Counter(), defaultdict(Counter)
        for p in with_ip:
            try:
                ip_ = ipaddress.ip_address(p["status"]["podIP"])
            except ValueError:
                continue
            for name, net, _free in nets:
                if ip_.version == net.version and ip_ in net:
                    key = (name, str(net))
                    used[key] += 1
                    by_ns[key][p["metadata"]["namespace"]] += 1
                    break
        prow = []
        for name, net, free in nets:
            key = (name, str(net))
            prow.append([name, str(net), "n/a" if free is None else free, used[key], ", ".join(f"{n} ({c})" for n, c in by_ns[key].most_common(3)) or "-"])
        rep.table(["POD RANGE", "ADDRESS RANGE", "FREE ADDRESSES", "PODS HOLDING AN ADDRESS", "TOP NAMESPACES"], prow, maxw=70,
                  about="how many pods currently hold an address inside each pod range (FREE ADDRESSES = addresses not yet handed out to nodes as per-node blocks), and which namespaces use most.")
    if not ranges:
        _net_status(rep, ctx, "ip", "Not available", "the subnet and secondary ranges are unknown because the Google Cloud section is off or could not read the cluster",
                    "Run with Google Cloud details turned on (gcloud auth login) to see free addresses per range.")
    elif _combine(states) in ("Problem", "Warning"):
        _net_status(rep, ctx, "ip", _combine(states), "; ".join(worst),
                    "Running out of addresses blocks scale-out: add a secondary range / a bigger subnet, lower the maximum pods per node, or add a discontiguous pod range. "
                    "Existing pods keep working.")
    else:
        _net_status(rep, ctx, "ip", "OK", "all ranges have room: " + "; ".join(f"{r[1]} {r[6]} used" for r in rows),
                    "No action needed. Re-check before a large scale-out; pod ranges are consumed one block per node.")


def _net_cni_logs(rep, ctx):
    rep.sub("Pod-level networking: Container Network Interface (CNI) daemon logs",
            f"error counts in the last {ctx.minutes} min of the logs of the networking agents themselves; failed IP allocations and exhausted ranges show up here first.",
            ["CNI", "anetd", "netd", "Calico", "FailedCreatePodSandBox"])
    rows, states, notes = [], [], []
    no_ip = r"no IP addresses available|IP.*exhaust|IP_SPACE_EXHAUSTED|failed to allocate for range|range is full|range exhausted"
    checks = (("anetd (Dataplane V2 agent)", "k8s-app=cilium", "anetd"), ("netd (GKE node networking daemon)", "k8s-app=netd", "netd"),
              ("calico-node (network policy)", "k8s-app=calico-node", "calico-node"))
    present = {ds for _t, _s, ds in checks if any(d["metadata"]["name"] == ds for d in items(ctx.data.get("daemonsets")))}
    log_calls = {ds: pf(_read_logs, ctx, selector) for _t, selector, ds in checks if ds in present}      # one log read per networking agent, together
    for title, selector, ds in checks:
        if ds not in present:
            continue
        lines, err = log_calls[ds].result()
        if lines is None:
            rows.append([title, "-", "-", "unavailable: " + err, "", "Not available"])
            states.append("Not available")
            notes.append(f"{ds}: logs unreadable ({err})")
            continue
        counts = {"no free IP address": sum(1 for l in lines if re.search(no_ip, l, re.I)),
                  "request failures": sum(1 for l in lines if re.search(r"failed|unable|cannot", l, re.I)),
                  "errors": sum(1 for l in lines if re.search(r"\berror\b|ERROR|level=error", l))}
        sample = next((l for l in reversed(lines) if re.search(r"error|fail|timeout|exhaust", l, re.I)), "")
        state = "Problem" if counts["no free IP address"] else ("Warning" if counts["errors"] or counts["request failures"] else "OK")
        states.append(state)
        rows.append([title, len(lines), counts["errors"], ", ".join(f"{k} {v}" for k, v in counts.items() if v and k != "errors") or "-", sample[:110], state])
        if counts["no free IP address"]:
            ctx.find("HIGH", f"{title} logged {counts['no free IP address']} 'no free IP' errors in the window - pod IP range exhaustion")
            notes.append(f"{ds}: {counts['no free IP address']} no-free-IP-address errors")
        elif state == "Warning":
            notes.append(f"{ds}: {counts['errors']} error line(s)")
    rep.table(["COMPONENT", "LOG LINES READ", "ERROR-LIKE LINES", "BREAKDOWN", "LATEST ERROR LINE", "RESULT"], rows, maxw=70,
              about="how many log lines were read from each networking agent and how many look like errors, split by cause (for example no free IP address), plus the latest error line.")
    if not rows:
        _net_status(rep, ctx, "cni_logs", "Not available", "no networking agent (anetd / netd / calico-node) runs as a DaemonSet in this cluster, so there are no daemon logs to read",
                    "Use the pod events and node checks below; on GKE without these agents the network set-up is part of the node image.")
        return
    st = _combine(states)
    if st == "OK":
        _net_status(rep, ctx, "cni_logs", "OK", f"no error lines in the last {ctx.minutes} min from " + ", ".join(r[0].split(" ")[0] for r in rows),
                    "No action needed.")
    elif st == "Not available":
        _net_status(rep, ctx, "cni_logs", "Not available", "; ".join(notes), "Grant permission to read pod logs in kube-system (pods/log) or open the logs in Cloud Logging.")
    else:
        _net_status(rep, ctx, "cni_logs", st, "; ".join(notes),
                    "'No free IP address' means the pod range of that node is used up: add pod IP capacity or fewer pods per node. Other errors: read the full lines in the pod logs section.")


def _net_stuck_pods(rep, ctx):
    rep.sub("Pod-level networking: pods stuck in ContainerCreating and FailedCreatePodSandBox events",
            "pods that were scheduled but never started their containers, and the events that say the network sandbox could not be created.",
            ["ContainerCreating", "FailedCreatePodSandBox", "CNI"])
    pods_raw = ctx.data.get("pods")
    if pods_raw is None:
        _net_status(rep, ctx, "stuck", "Not available", "pods could not be read", "Check permissions (kubectl get pods -A) and run again.")
        return
    rows, stuck_ns = [], set()
    sandbox = []
    for e in items(ctx.data.get("events")):
        text = f"{e.get('reason', '')} {e.get('message') or e.get('note') or ''}"
        if re.search(r"FailedCreatePodSandBox|failed to create pod sandbox|failed to setup network for sandbox", text, re.I):
            sandbox.append(e)
    by_pod = {}
    for e in sandbox:
        obj = e.get("involvedObject") or e.get("regarding") or {}
        by_pod[(obj.get("namespace"), obj.get("name"))] = (e.get("message") or e.get("note") or "").replace("\n", " ")
    for p in items(pods_raw):
        meta = p["metadata"]
        waiting = [c.get("state", {}).get("waiting", {}).get("reason") for c in (p.get("status", {}).get("containerStatuses") or []) + (p.get("status", {}).get("initContainerStatuses") or [])]
        created = parse_ts(meta.get("creationTimestamp"))
        secs = (ctx.now - created).total_seconds() if created else 0
        creating = p.get("status", {}).get("phase") == "Pending" and p.get("spec", {}).get("nodeName") and (
            "ContainerCreating" in waiting or (not any(waiting) and any(c.get("type") == "PodScheduled" and c.get("status") == "True" for c in p.get("status", {}).get("conditions") or [])))
        key = (meta.get("namespace"), meta["name"])
        if (creating and secs > 120) or key in by_pod:
            rows.append([f"{meta.get('namespace')}/{meta['name']}", support_of(ctx, meta.get("namespace")) or "-", "ContainerCreating" if creating else p.get("status", {}).get("phase", "?"),
                         age(created, ctx.now) if created else "-", node_tag(ctx, p.get("spec", {}).get("nodeName")) if p.get("spec", {}).get("nodeName") else "-",
                         by_pod.get(key, "(no sandbox event seen)")[:130]])
            stuck_ns.add(meta.get("namespace"))
            ctx.ns_issue(meta.get("namespace"), f"pod {meta['name']} stuck creating its network sandbox" if key in by_pod else f"pod {meta['name']} stuck in ContainerCreating")
    rep.table(["NAMESPACE AND POD", "SUPPORT DISTRIBUTION LIST", "STATE", "AGE", "NODE", "SANDBOX EVENT MESSAGE"], rows, maxw=80,
              about="pods that are scheduled but still in ContainerCreating after 2 minutes, or that have a FailedCreatePodSandBox event, with the node and the message Kubernetes gave.")
    if sandbox or rows:
        n_events = len(sandbox)
        _net_status(rep, ctx, "stuck", "Problem" if n_events or len(rows) >= 3 else "Warning",
                    f"{len(rows)} pod(s) stuck creating, {n_events} sandbox failure event(s)" + (" e.g. " + next(iter(by_pod.values()))[:100] if by_pod else ""),
                    "The network plug-in could not set up the pod. Typical causes: no free pod IP address on that node, the CNI agent not ready on the node, or the node running out of "
                    "resources. Check the IP address exhaustion and CNI blocks above and describe one pod (kubectl describe pod).", "Pods stuck creating")
    else:
        _net_status(rep, ctx, "stuck", "OK", f"no pod has been in ContainerCreating for more than 2 minutes and no sandbox failure event exists among {len(items(pods_raw))} pods",
                    "No action needed.")


def _net_events(rep, ctx):
    events = []
    for e in items(ctx.data.get("events")):
        t = _event_time(e)
        text = f"{e.get('reason', '')} {e.get('message') or e.get('note') or ''}"
        if e.get("type") == "Warning" and t and t >= ctx.since and NET_EVENT_PATTERN.search(text):
            events.append((t, e))
    rep.sub("Pod-level networking: network-related warning events",
            f"Kubernetes warning events of the last {ctx.minutes} min whose text points at networking (DNS, routes, load balancers, sandboxes, firewall).")
    if events:
        rows = []
        for t, e in sorted(events, key=lambda x: x[0], reverse=True)[:30]:
            obj = e.get("involvedObject") or e.get("regarding") or {}
            name = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else f"{(obj.get('namespace') + '/') if obj.get('namespace') else ''}{obj.get('name', '?')}"
            rows.append([age(t, ctx.now) + " ago", e.get("reason", "?"), f"{obj.get('kind', '?')} {name}", support_of(ctx, obj.get("namespace")) or "-",
                         (e.get("series") or {}).get("count") or e.get("count") or 1, (e.get("message") or e.get("note") or "").replace("\n", " ")[:130]])
            ctx.ns_issue(obj.get("namespace"), f"network warning: {e.get('reason', '?')}")
        rep.table(["WHEN", "REASON", "OBJECT", "SUPPORT DISTRIBUTION LIST", "OCCURRENCES", "MESSAGE"], rows, maxw=70,
                  about="the newest network-related Warning events: how long ago, the reason Kubernetes gave, which object it concerns, how often it repeated and the message.")
        ctx.find("MED", f"{len(events)} network-related Warning event(s) in the window (e.g. {rows[0][1]})")
        _net_status(rep, ctx, "events", "Warning", f"{len(events)} network-related warning event(s) in the window",
                    "Read the messages: repeated reasons from one object point at one broken component; many objects with the same reason point at the node or the platform.")
    elif ctx.data.get("events") is None:
        _net_status(rep, ctx, "events", "Not available", "events could not be read", "Check permissions (kubectl get events -A).")
    else:
        _net_status(rep, ctx, "events", "OK", f"no network-related warning events in the last {ctx.minutes} min", "No action needed.")


def _net_init_order(rep, ctx):
    rep.sub("Pod-level networking: Container Network Interface (CNI) start-up order, agent restarts and kube-proxy readiness",
            "whether the networking agents were restarting or not ready on some nodes; a pod that starts before its node's agent is ready stays in ContainerCreating.",
            ["CNI", "anetd", "netd", "kube-proxy", "NetworkUnavailable"])
    if ctx.data.get("pods") is None:
        _net_status(rep, ctx, "init", "Not available", "pods could not be read", "Check permissions and run again.")
        return
    rows, states, notes = [], [], []
    for title, prefixes, label in (("anetd (Dataplane V2 agent)", ("anetd",), "cilium"), ("netd (node networking daemon)", ("netd",), "netd"),
                                   ("calico-node", ("calico-node",), "calico-node"), ("kube-proxy", ("kube-proxy",), "kube-proxy")):
        pods = _ks_pods(ctx, *prefixes, label=label)
        if not pods:
            continue
        not_ready = sum(1 for p in pods if not _pod_ready(p))
        rs = [_pod_restarts(p) for p in pods]
        state = "Problem" if not_ready else ("Warning" if max(rs or [0]) >= 3 else "OK")
        states.append(state)
        if state != "OK":
            notes.append(f"{title.split(' ')[0]}: {not_ready} not ready, up to {max(rs or [0])} restarts")
        rows.append([title, len(pods), not_ready, sum(rs), max(rs or [0]), state])
    tainted = [n["metadata"]["name"] for n in items(ctx.data.get("nodes")) for t in (n.get("spec", {}).get("taints") or [])
               if t.get("key") in ("node.cilium.io/agent-not-ready", "node.kubernetes.io/network-unavailable")]
    net_unavail = [n["metadata"]["name"] for n in items(ctx.data.get("nodes")) if any(c.get("type") == "NetworkUnavailable" and c.get("status") == "True"
                                                                                       for c in n.get("status", {}).get("conditions") or [])]
    rep.table(["COMPONENT", "PODS", "PODS NOT READY", "TOTAL RESTARTS", "MOST RESTARTS ON ONE POD", "RESULT"], rows, maxw=50,
              about="readiness and restart counts of the networking agents and kube-proxy; one pod per node is expected, so a not-ready pod means one node has no working pod networking.")
    if tainted or net_unavail:
        states.append("Problem")
        notes.append(f"{len(set(tainted) | set(net_unavail))} node(s) tainted / NetworkUnavailable: " + ", ".join(sorted(set(tainted) | set(net_unavail))[:4]))
    if not rows:
        _net_status(rep, ctx, "init", "Not available", "no networking agent or kube-proxy pod is visible in kube-system",
                    "Nothing to compare; rely on the node conditions and stuck-pod checks.")
    elif _combine(states) in ("Problem", "Warning"):
        _net_status(rep, ctx, "init", _combine(states), "; ".join(notes),
                    "Agent restarts or a node still marked network-unavailable mean pods may have started before networking was ready. Check the agent logs and wait until the node taint clears; "
                    "if it persists, recreate that node.", "Networking start-up order")
    else:
        _net_status(rep, ctx, "init", "OK", "all networking agent pods are ready with fewer than 3 restarts and no node is tainted as network-unavailable",
                    "No action needed.")


# --- B. node networking ------------------------------------------------------------------------------------------

def _net_node_conditions(rep, ctx):
    rep.sub("Node networking: node readiness and pressure conditions",
            "the health conditions every node reports; a NotReady or network-unavailable node loses its pods' connectivity, and memory, disk or process pressure leads to evictions.",
            ["NotReady", "MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable", "kubelet"])
    nodes = items(ctx.data.get("nodes"))
    if not nodes:
        _net_status(rep, ctx, "nodes", "Not available", "nodes could not be read", "Check permissions (kubectl get nodes).")
        return
    rows, problems, warns = [], [], []
    for n in nodes:
        cond = {c.get("type"): c.get("status") for c in n.get("status", {}).get("conditions") or []}
        ready = cond.get("Ready")
        flags = {k: cond.get(k) == "True" for k in ("MemoryPressure", "DiskPressure", "PIDPressure", "NetworkUnavailable")}
        note = []
        if ready != "True":
            note.append("NOT READY")
            problems.append(n["metadata"]["name"])
        if flags["NetworkUnavailable"]:
            note.append("network unavailable")
            problems.append(n["metadata"]["name"])
        for k in ("MemoryPressure", "DiskPressure", "PIDPressure"):
            if flags[k]:
                note.append(k)
                warns.append(n["metadata"]["name"])
        yes = lambda b: "YES" if b else "no"
        rows.append((0 if note else 1, [node_tag(ctx, n["metadata"]["name"]), "yes" if ready == "True" else ("NO" if ready else "unknown"), yes(flags["MemoryPressure"]),
                                         yes(flags["DiskPressure"]), yes(flags["PIDPressure"]), yes(flags["NetworkUnavailable"]), "; ".join(note) or "ok"]))
    rows.sort(key=lambda x: x[0])
    rep.table(["NODE", "READY", "MEMORY PRESSURE", "DISK PRESSURE", "PROCESS PRESSURE", "NETWORK UNAVAILABLE", "NOTE"], [r for _k, r in rows], limit=30, maxw=50,
              about="the conditions each node reports about itself (problem nodes first): ready, memory pressure, disk pressure, too many processes, and whether its pod network is set up.")
    if problems:
        _net_status(rep, ctx, "nodes", "Problem", f"{len(set(problems))} node(s) NotReady or network-unavailable: " + ", ".join(sorted(set(problems))[:4]),
                    "Pods on these nodes are unreachable or will be rescheduled. Check the node VM state and kubelet logs; GKE auto-repair replaces nodes that stay NotReady.")
    elif warns:
        _net_status(rep, ctx, "nodes", "Warning", f"{len(set(warns))} node(s) report memory, disk or process pressure: " + ", ".join(sorted(set(warns))[:4]),
                    "Pressure leads to pod evictions and slow networking. Free resources or add nodes.", "Node pressure")
    else:
        _net_status(rep, ctx, "nodes", "OK", f"all {len(nodes)} nodes are Ready with no memory, disk, process or network-unavailable condition", "No action needed.")


def _net_mtu(rep, ctx, target):
    rep.sub("Node networking: Maximum Transmission Unit (MTU) hints",
            "the packet size limit of the virtual private cloud network and where it could mismatch with VPN links; the interface MTU on nodes and pods cannot be read without node access.",
            ["MTU", "VPC", "VPC peering"])
    if not (GCP_OPTS["enabled"] and target):
        _net_status(rep, ctx, "mtu", "Not available", "the Google Cloud section is off or could not read the cluster, so the network MTU is unknown",
                    "Turn Google Cloud details on (gcloud auth login). The MTU of node and pod interfaces cannot be read without node access, which this tool never uses.")
        return
    net = ctx.data.get("gcp_net") or {}
    name = net.get("name")
    ntarget = {"project": net.get("project") or target["project"]}
    data, err = gcloud(["compute", "networks", "describe", name], ntarget, 60) if name else (None, "network name unknown")
    if err or not isinstance(data, dict):
        _net_status(rep, ctx, "mtu", "Not available", f"`gcloud compute networks describe` failed: {_first_line(err or 'no data', 90)}",
                    "Needs compute.networks.get (roles/compute.viewer). The MTU of node and pod interfaces cannot be read without node access.")
        return
    ctx.data["gcp_vpc"] = data
    mtu = int(data.get("mtu") or 1460)
    routes, _e = _gcp_routes(ctx, target, ntarget)
    vpn = [r.get("name") for r in routes or [] if r.get("nextHopVpnTunnel") and _url_name(r.get("network")) == name]
    peerings = data.get("peerings") or []
    rows = [["Virtual private cloud network MTU", f"{mtu} bytes", "Set on the network; every VM and GKE node uses it (default 1460, up to 8896 with jumbo frames)."],
            ["VPN routes on this network", ", ".join(vpn[:3]) or "none", "Cloud VPN tunnels carry at most 1460 bytes per packet."],
            ["Network peerings", ", ".join(f"{p.get('name')} ({p.get('state')})" for p in peerings[:3]) or "none", "Peered networks should use the same MTU."],
            ["Node and pod interface MTU", "cannot be read without node access", "Usually equals the network MTU; Dataplane V2 / Calico overlays can lower it by the tunnel header size."]]
    rep.table(["ITEM", "VALUE", "MEANING"], rows, maxw=80,
              about="the packet size limit configured on the virtual private cloud network and the places where a different limit could cause hanging large requests.")
    if mtu > 1460 and vpn:
        _net_status(rep, ctx, "mtu", "Warning", f"network MTU is {mtu} but traffic to on-premises goes through VPN tunnel route(s) ({', '.join(vpn[:2])}) that carry at most 1460",
                    "Large packets may be dropped or fragmented on the VPN path. Lower the network MTU to 1460 or make sure path MTU discovery is allowed by the firewalls.", "MTU hint")
    elif mtu not in (1460, 1500, 8896):
        _net_status(rep, ctx, "mtu", "Warning", f"unusual network MTU {mtu}", "Check that every connected network (peering, VPN, Interconnect) uses a compatible MTU.", "MTU hint")
    else:
        _net_status(rep, ctx, "mtu", "OK", f"network MTU is {mtu} bytes; no VPN route on this network with a smaller limit was found (node and pod interface MTU needs node access)",
                    "If only large requests hang, compare the MTU of both ends with `ip link` on a node (manual step, node access needed).")


def _net_throttling(rep, ctx, target):
    rep.sub("Node networking: Google Cloud API throttling and quota evidence",
            f"whether Google Cloud refused calls in the last {ctx.minutes} min because of rate limits or quotas; that delays load balancer and node changes.",
            ["API", "RATE_LIMIT_EXCEEDED"])
    quota_events = []
    for e in items(ctx.data.get("events")):
        t = _event_time(e)
        text = f"{e.get('reason', '')} {e.get('message') or e.get('note') or ''}"
        if t and t >= ctx.since and re.search(r"RATE_LIMIT_EXCEEDED|rateLimitExceeded|quota.*exceeded|Quota .* exceeded|googleapi: Error 429", text, re.I):
            quota_events.append(e)
    entries, err = (None, "Google Cloud section is off") if not (GCP_OPTS["enabled"] and target) else gcp_logs(
        target, '(protoPayload.status.message:"RATE_LIMIT_EXCEEDED" OR protoPayload.status.message:"Quota exceeded" OR '
                'jsonPayload.message:"RATE_LIMIT_EXCEEDED" OR textPayload:"RATE_LIMIT_EXCEEDED") AND severity>=WARNING', ctx.minutes, ctx.since, 100)
    rows = []
    if entries:
        agg = Counter()
        for e in entries:
            pp = e.get("protoPayload") or {}
            agg[(pp.get("serviceName") or ((e.get("resource") or {}).get("type")) or "?", (pp.get("methodName") or "?").rsplit(".", 1)[-1], _log_text(e)[:90])] += 1
        rows = [[n, svc, m, msg] for (svc, m, msg), n in agg.most_common(10)]
    rep.table(["OCCURRENCES", "SERVICE", "METHOD", "MESSAGE"], rows, maxw=70,
              about="log entries from Cloud Logging that mention a rate limit or quota error, grouped by service and method.")
    if quota_events:
        rep.add(f"  Kubernetes events with quota / rate limit text in the window: {len(quota_events)} (e.g. {(quota_events[0].get('message') or '')[:100]})")
    if entries:
        _net_status(rep, ctx, "throttle", "Warning", f"{len(entries)} rate-limit / quota log entries in the window" + (f" and {len(quota_events)} event(s)" if quota_events else ""),
                    "Google Cloud is throttling API calls: slow down automation, spread calls, or ask for a higher quota (IAM and Admin > Quotas).", "Cloud API throttling")
    elif quota_events:
        _net_status(rep, ctx, "throttle", "Warning", f"{len(quota_events)} Kubernetes event(s) mention a quota or rate limit",
                    "Read the event messages; the cloud controller is being throttled or a quota is exhausted.", "Cloud API throttling")
    elif err or entries is None:
        _net_status(rep, ctx, "throttle", "Not available", f"Cloud Logging could not be read: {_first_line(err or 'no data', 90)}" + ("; no throttling events in Kubernetes" if not quota_events else ""),
                    "Needs roles/logging.viewer. Without it, throttling can only be seen in Kubernetes events.")
    else:
        _net_status(rep, ctx, "throttle", "OK", f"no rate-limit or quota errors in Cloud Logging or in Kubernetes events in the last {ctx.minutes} min", "No action needed.")


# --- C. kube-proxy and service routing ---------------------------------------------------------------------------

def _net_kube_proxy(rep, ctx):
    rep.sub("kube-proxy and service routing: kube-proxy health and mode",
            "whether the component that routes Service addresses to pods is running on every node, and which mode it uses (iptables or IPVS), or that Dataplane V2 replaces it.",
            ["kube-proxy", "iptables", "IPVS", "Dataplane V2"])
    v2 = _is_dataplane_v2(ctx)
    if v2:
        a = _workload(ctx, "anetd")
        ev = "kube-proxy is replaced by Dataplane V2: anetd does the Service load balancing" + (f" ({a[1]}/{a[2]} anetd pods ready)" if a else "")
        _net_status(rep, ctx, "kproxy", "OK" if (not a or a[1] >= a[2]) else "Problem", ev,
                    "No kube-proxy to check. Service routing health equals anetd health (see the Container Network Interface block)." if (not a or a[1] >= a[2])
                    else "anetd is not ready on every node, so Service routing is broken on those nodes; see the Container Network Interface block.")
        return
    pods = _ks_pods(ctx, "kube-proxy", label="kube-proxy")
    ds = _workload(ctx, "kube-proxy")
    if ctx.data.get("pods") is None:
        _net_status(rep, ctx, "kproxy", "Not available", "pods could not be read", "Check permissions and run again.")
        return
    mode, mode_src = None, "default"
    for cmname in ("kube-proxy-config", "kube-proxy"):
        cm = _kj(["get", "configmap", cmname, "-n", "kube-system"])[0]
        text = " ".join(str(v) for v in ((cm or {}).get("data") or {}).values())
        m = re.search(r"mode:\s*\"?(\w*)\"?", text)
        if cm and m:
            mode, mode_src = (m.group(1) or "iptables"), f"ConfigMap {cmname}"
            break
    n_nodes = len(items(ctx.data.get("nodes")))
    n_svc = len(items(ctx.data.get("services")))
    ready = sum(1 for p in pods if _pod_ready(p))
    total = len(pods) if pods else (ds[2] if ds else 0)
    if ds:
        ready, total = ds[1], ds[2]
    tag = _img_tag(((pods[0].get("spec", {}).get("containers") or [{}])[0]).get("image")) if pods else (ds[3] if ds else "-")
    rows = [["Dataplane", "legacy (kube-proxy)" if v2 is False else "unknown (no cluster details)"], ["kube-proxy pods ready", f"{ready}/{total}" + (f" (nodes: {n_nodes})" if n_nodes else "")],
            ["Mode", f"{mode or 'iptables'} ({mode_src}{'; GKE default, not readable here' if not mode else ''})"], ["Version", tag], ["Services handled", n_svc]]
    rep.table(["ITEM", "VALUE"], rows, maxw=80, about="kube-proxy readiness on the nodes, the packet-forwarding mode it uses, its version and how many Services it has to program.")
    if not pods and not ds:
        st = "Problem" if v2 is False else "Not available"
        _net_status(rep, ctx, "kproxy", st, "no kube-proxy pod or DaemonSet was found in kube-system" + ("" if v2 is False else " (cluster details unavailable, so Dataplane V2 cannot be ruled out)"),
                    "Without kube-proxy (and without Dataplane V2) Service addresses do not route. Check the kube-system pods and the node image." if v2 is False
                    else "Turn Google Cloud details on to see whether Dataplane V2 replaces kube-proxy.", "kube-proxy" if v2 is False else None)
    elif total and ready < total:
        _net_status(rep, ctx, "kproxy", "Problem", f"only {ready} of {total} kube-proxy pods are ready",
                    "On nodes without a ready kube-proxy, Service addresses (ClusterIP, NodePort) do not work. Describe the pod and read its logs.", "kube-proxy")
    elif (mode or "iptables") == "iptables" and n_svc >= 1000:
        _net_status(rep, ctx, "kproxy", "Warning", f"mode iptables with {n_svc} Services: every update rewrites a very long rule list",
                    "Consider IPVS mode or Dataplane V2 for clusters with thousands of Services; slow rule syncing delays endpoint updates.", "kube-proxy")
    else:
        _net_status(rep, ctx, "kproxy", "OK", f"{ready}/{total} kube-proxy pods ready, mode {mode or 'iptables'}", "No action needed.")


def _net_kube_proxy_logs(rep, ctx):
    if _is_dataplane_v2(ctx):
        return
    rep.sub("kube-proxy and service routing: recent kube-proxy log errors",
            f"error lines in the last {ctx.minutes} min from kube-proxy; rule-sync failures here explain Services that stop routing.",
            ["kube-proxy", "iptables", "conntrack"])
    if not (_ks_pods(ctx, "kube-proxy", label="kube-proxy") or _workload(ctx, "kube-proxy")):
        _net_status(rep, ctx, "kproxy_logs", "Not available", "no kube-proxy pods to read logs from", "See the kube-proxy block above.")
        return
    lines, err = _read_logs(ctx, "component=kube-proxy")
    if lines is None or not lines:
        l2, err2 = _read_logs(ctx, "k8s-app=kube-proxy")
        if l2 is not None and (l2 or lines is None):
            lines, err = l2, err2
    if lines is None:
        _net_status(rep, ctx, "kproxy_logs", "Not available", f"kube-proxy logs unreadable: {err}", "Grant pods/log permission in kube-system or read the logs in Cloud Logging.")
        return
    errs = [l for l in lines if re.search(r"\berror\b|failed|unable|iptables-restore|conntrack", l, re.I) and not re.search(r"level=info", l)]
    rows = [["kube-proxy", len(lines), len(errs), (errs[-1] if errs else "")[:110]]]
    rep.table(["COMPONENT", "LOG LINES READ", "ERROR-LIKE LINES", "LATEST ERROR LINE"], rows, maxw=80,
              about="how many kube-proxy log lines were read and how many look like errors, with the newest error.")
    if errs:
        _net_status(rep, ctx, "kproxy_logs", "Warning", f"{len(errs)} error-like line(s) in the last {ctx.minutes} min, e.g. {errs[-1][:90]}",
                    "Repeated rule-sync or conntrack errors mean Service routing may be stale on that node; restart the pod only after reading the full message.", "kube-proxy logs")
    else:
        _net_status(rep, ctx, "kproxy_logs", "OK", f"no error lines among {len(lines)} kube-proxy log lines", "No action needed.")


def _net_svc_no_endpoints(rep, ctx):
    rep.sub("kube-proxy and service routing: Services with no ready endpoints",
            "Services that select pods but have no ready backend pod, so every request to them fails; the usual cause is a selector that matches nothing or pods that are not ready.",
            ["ClusterIP", "NodePort", "LoadBalancer"])
    if ctx.data.get("services") is None or ctx.data.get("endpoints") is None:
        _net_status(rep, ctx, "noep", "Not available", "Services or Endpoints could not be read", "Check permissions (kubectl get services,endpoints -A).")
        return
    endpoints = {(e["metadata"]["namespace"], e["metadata"]["name"]): e for e in items(ctx.data.get("endpoints"))}
    pods = items(ctx.data.get("pods"))
    rows, front = [], 0
    for s in items(ctx.data.get("services")):
        meta, spec = s["metadata"], s.get("spec", {})
        key = (meta["namespace"], meta["name"])
        if not spec.get("selector") or spec.get("type") == "ExternalName" or key[1] == "kubernetes":
            continue
        subsets = (endpoints.get(key) or {}).get("subsets") or []
        ready = sum(len(x.get("addresses") or []) for x in subsets)
        not_ready = sum(len(x.get("notReadyAddresses") or []) for x in subsets)
        if ready:
            continue
        sel = spec["selector"]
        matching = [p for p in pods if p["metadata"]["namespace"] == key[0] and all((p["metadata"].get("labels") or {}).get(k) == v for k, v in sel.items())]
        hint = ("no pod matches the selector" if not matching else (f"{len(matching)} pod(s) match but none is ready" + (f" ({not_ready} not-ready address(es))" if not_ready else "")))
        stype = spec.get("type", "ClusterIP")
        front += stype in ("LoadBalancer", "NodePort")
        rows.append([key[0], support_of(ctx, key[0]) or "-", key[1], stype, spec.get("clusterIP", "-"), ready, not_ready, hint])
        ctx.ns_issue(key[0], f"service {key[1]}: no ready endpoints")
    rep.table(["NAMESPACE", "SUPPORT DISTRIBUTION LIST", "SERVICE", "SERVICE TYPE", "CLUSTER IP ADDRESS", "READY ENDPOINTS", "NOT READY ENDPOINTS", "LIKELY CAUSE"], rows, maxw=60,
              about="Services that have a selector but no ready endpoint, with the likely cause worked out from the pods that match the selector.")
    if rows:
        _net_status(rep, ctx, "noep", "Problem" if front else "Warning", f"{len(rows)} Service(s) have no ready endpoint" + (f", {front} of them are NodePort / LoadBalancer" if front else ""),
                    "Fix the selector (label mismatch) or the pod readiness probe; until then requests to these Services get connection refused or time out.", "Services without endpoints")
    else:
        _net_status(rep, ctx, "noep", "OK", "every Service with a selector has at least one ready endpoint", "No action needed.")


def _net_service_inventory(rep, ctx):
    services = items(ctx.data.get("services"))
    by_type = Counter(s.get("spec", {}).get("type", "ClusterIP") for s in services)
    # --- ClusterIP
    rep.sub("kube-proxy and service routing: ClusterIP services", "how many virtual-IP Services exist per namespace; they are only reachable inside the cluster.", ["ClusterIP", "CIDR"])
    if ctx.data.get("services") is None:
        _net_status(rep, ctx, "clusterip", "Not available", "Services could not be read", "Check permissions (kubectl get services -A).")
    else:
        per_ns = defaultdict(lambda: [0, 0])
        for s in services:
            if s.get("spec", {}).get("type", "ClusterIP") == "ClusterIP":
                per_ns[s["metadata"]["namespace"]][0] += 1
                per_ns[s["metadata"]["namespace"]][1] += (s.get("spec", {}).get("clusterIP") == "None")
        rows = [[ns, support_of(ctx, ns) or "-", v[0], v[1]] for ns, v in sorted(per_ns.items(), key=lambda kv: -kv[1][0])[:25]]
        rep.table(["NAMESPACE", "SUPPORT DISTRIBUTION LIST", "CLUSTER IP SERVICES", "OF WHICH HEADLESS (NO VIRTUAL IP)"], rows, maxw=50,
                  about="how many ClusterIP Services each namespace has and how many of them are headless (no virtual IP, DNS returns the pod addresses directly).")
        _net_status(rep, ctx, "clusterip", "OK", "Services in the cluster: " + ", ".join(f"{v} {k}" for k, v in by_type.most_common()) + f" (total {len(services)})",
                    "No action needed. The service address range usage is in the IP address exhaustion block.")
    # --- NodePort
    rep.sub("kube-proxy and service routing: NodePort services and the node port firewall rule",
            "Services that are opened on a port (30000-32767) of every node, and whether a firewall rule on the virtual private cloud network allows that port.",
            ["NodePort", "VPC", "Health check ranges"])
    rules = ctx.data.get("gcp_fw_rules")
    np_rows, uncovered = [], 0
    for s in services:
        spec, meta = s.get("spec", {}), s["metadata"]
        ports = [p for p in spec.get("ports", []) if p.get("nodePort")]
        if spec.get("type") not in ("NodePort", "LoadBalancer") or not ports:
            continue
        verdicts = []
        for p in ports[:4]:
            if rules is None:
                verdicts.append(f"{p['nodePort']}: unknown (firewall rules not readable)")
                continue
            hit = [r for r in rules if not r.get("disabled") and r.get("direction") == "INGRESS" and r.get("allowed") and _port_open(r.get("allowed"), int(p["nodePort"]))]
            if hit:
                verdicts.append(f"{p['nodePort']}: allowed by {hit[0].get('name')} from {','.join(hit[0].get('sourceRanges') or ['(tags)'])[:40]}")
            else:
                verdicts.append(f"{p['nodePort']}: no allow rule (implied deny)")
                if spec.get("type") == "NodePort":
                    uncovered += 1
        np_rows.append([meta["namespace"], meta["name"], spec.get("type"), ", ".join(str(p["nodePort"]) for p in ports[:4]), "; ".join(verdicts)])
    rep.table(["NAMESPACE", "SERVICE", "SERVICE TYPE", "NODE PORTS", "FIREWALL CHECK (NODE PORT RANGE 30000-32767)"], np_rows, maxw=70,
              about="every Service that opens a node port, and for each port whether an ingress allow rule of the virtual private cloud network covers it. LoadBalancer Services get their rules from GKE automatically.")
    plain = [r for r in np_rows if r[2] == "NodePort"]
    if not plain:
        _net_status(rep, ctx, "nodeport", "OK", "no Service of type NodePort exists" + (f" ({len(np_rows)} LoadBalancer Service(s) also use node ports)" if np_rows else ""), "No action needed.")
    elif rules is None:
        _net_status(rep, ctx, "nodeport", "Not available", f"{len(plain)} NodePort Service(s) exist but the firewall rules could not be read",
                    "Needs compute.firewalls.list. Without it you cannot tell whether the node port range is reachable.")
    elif uncovered:
        _net_status(rep, ctx, "nodeport", "Warning", f"{uncovered} node port(s) of NodePort Services have no ingress allow rule",
                    "Create a firewall rule that allows tcp:30000-32767 from the sources that need it (a load balancer uses the health check ranges), or use a LoadBalancer / Ingress.", "NodePort firewall")
    else:
        _net_status(rep, ctx, "nodeport", "OK", f"all node ports of {len(plain)} NodePort Service(s) are covered by an allow rule", "No action needed; check the sources of the rules are as narrow as possible.")
    # --- LoadBalancer
    rep.sub("kube-proxy and service routing: LoadBalancer services",
            "Services of type LoadBalancer with the address Google Cloud gave them and whether they are reachable from the internet; backend health is in the load balancer blocks further down.",
            ["LoadBalancer", "Load balancer"])
    lb_rows, public, pending = [], [], 0
    for s in services:
        spec, meta = s.get("spec", {}), s["metadata"]
        stype = spec.get("type", "ClusterIP")
        if stype == "ClusterIP":
            continue
        ann = meta.get("annotations") or {}
        lb = (s.get("status", {}).get("loadBalancer") or {}).get("ingress") or []
        address = (lb[0].get("hostname") or lb[0].get("ip")) if lb else ("-" if stype != "ExternalName" else spec.get("externalName", "-"))
        scheme, kind = "-", "-"
        if stype == "LoadBalancer":
            internal = (ann.get("networking.gke.io/load-balancer-type") or ann.get("cloud.google.com/load-balancer-type") or "").lower() == "internal"
            scheme = "internal" if internal else "INTERNET-FACING"
            kind = "Internal passthrough load balancer" if internal else "External passthrough load balancer"
            if not internal:
                public.append(f"{meta['namespace']}/{meta['name']}")
            if not lb:
                pending += 1
        ports = ", ".join(f"{p.get('port')}" + (f":{p['nodePort']}" if p.get("nodePort") else "") + "/" + p.get("protocol", "TCP") for p in spec.get("ports", [])[:4])
        lb_rows.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], stype, scheme, kind, address[:60], ports])
    rep.table(["NAMESPACE", "SUPPORT DISTRIBUTION LIST", "SERVICE", "SERVICE TYPE", "EXPOSURE", "LOAD BALANCER KIND", "EXTERNAL ADDRESS", "PORTS (PORT[:NODE PORT])"], lb_rows, maxw=64,
              about="every Service that is not plain ClusterIP: its type, whether the load balancer is internal or internet-facing, the address it got and its ports.")
    if public:
        ctx.find("INFO", f"{len(public)} LoadBalancer Service(s) are internet-facing (no networking.gke.io/load-balancer-type: Internal annotation): "
                         + ", ".join(public[:6]) + (" ..." if len(public) > 6 else "") + support_suffix(ctx, {x.split("/")[0] for x in public}))
    if ctx.data.get("services") is None:
        _net_status(rep, ctx, "lbsvc", "Not available", "Services could not be read", "Check permissions.")
    elif pending:
        _net_status(rep, ctx, "lbsvc", "Warning", f"{pending} LoadBalancer Service(s) have no external address yet",
                    "Describe the Service (kubectl describe service) for events: quota, a missing subnet or a firewall problem keeps the load balancer from being created.", "LoadBalancer services")
    else:
        _net_status(rep, ctx, "lbsvc", "OK", f"{sum(1 for r in lb_rows if r[3] == 'LoadBalancer')} LoadBalancer Service(s), all with an address; {len(public)} internet-facing", "No action needed.")


# --- D. DNS ------------------------------------------------------------------------------------------------------

def _net_dns(rep, ctx):
    rep.sub("Domain Name System (DNS): cluster DNS pods, restarts and configuration",
            "whether the pods that answer names inside the cluster (kube-dns or CoreDNS) are ready and stable, and how they are configured (forwarders, stub domains, Cloud DNS).",
            ["DNS", "kube-dns", "CoreDNS", "Corefile", "stubDomains", "upstream nameservers", "NodeLocal DNSCache"])
    pods = [p for p in items(ctx.data.get("pods")) if p["metadata"]["namespace"] == "kube-system" and
            ((p["metadata"].get("labels") or {}).get("k8s-app") in ("kube-dns", "coredns") or p["metadata"]["name"].startswith(("kube-dns-", "coredns-"))) and
            "autoscaler" not in p["metadata"]["name"]]
    if ctx.data.get("pods") is None:
        _net_status(rep, ctx, "dns", "Not available", "pods could not be read", "Check permissions and run again.")
        return
    cluster = ctx.data.get("gcp_cluster") or {}
    dnscfg = cluster.get("dnsConfig") or {}
    provider = "Cloud DNS for GKE (" + str(dnscfg.get("clusterDnsScope") or "cluster") + " scope)" if dnscfg.get("clusterDns") == "CLOUD_DNS" else (
        "kube-dns (default)" if cluster else "unknown (Google Cloud section off)")
    ready = sum(1 for p in pods if _pod_ready(p))
    restarts = [_pod_restarts(p) for p in pods]
    kcm = (_kj(["get", "configmap", "kube-dns", "-n", "kube-system"])[0] or {}).get("data") or {}
    core = (_kj(["get", "configmap", "coredns", "-n", "kube-system"])[0] or {}).get("data") or {}
    forwards = re.findall(r"forward\s+\.\s+([^\n{]+)", core.get("Corefile", ""))
    cache = re.findall(r"cache\s+(\d+)", core.get("Corefile", ""))
    rows = [["Cluster DNS provider", provider], ["DNS pods ready", f"{ready}/{len(pods)}" if pods else "none found"],
            ["Restarts of DNS pods", f"total {sum(restarts)}, most on one pod {max(restarts or [0])}"],
            ["kube-dns ConfigMap: stub domains", kcm.get("stubDomains") or "none set"], ["kube-dns ConfigMap: upstream nameservers", kcm.get("upstreamNameservers") or "none set (uses the node's resolvers)"],
            ["CoreDNS Corefile: forwarders", ", ".join(x.strip() for x in forwards) or "not used"], ["CoreDNS Corefile: cache seconds", ", ".join(cache) or "not set / not used"]]
    rep.table(["ITEM", "VALUE"], rows, maxw=90, about="the cluster DNS provider, how many DNS pods are ready and how often they restarted, and the forwarding settings from the kube-dns ConfigMap or the CoreDNS Corefile.")
    if not pods:
        _net_status(rep, ctx, "dns", "Problem" if cluster else "Not available", "no kube-dns / CoreDNS pod was found in kube-system" + ("" if cluster else " (cluster details unavailable)"),
                    "Without cluster DNS, names such as service.namespace.svc cannot be resolved. Check the kube-system pods; with Cloud DNS for GKE there are no pods (then this is fine).", "DNS" if cluster else None)
    elif ready == 0:
        _net_status(rep, ctx, "dns", "Problem", f"0 of {len(pods)} DNS pods are ready", "All name lookups in the cluster fail. Describe the DNS pods and check the nodes they run on.", "DNS")
    elif ready < len(pods) or max(restarts or [0]) >= 3:
        _net_status(rep, ctx, "dns", "Warning", f"{ready}/{len(pods)} DNS pods ready, most restarts on one pod {max(restarts or [0])}",
                    "Fewer DNS pods mean more load on the rest and intermittent timeouts. Check why a pod is not ready or restarting (resources, node problems).", "DNS")
    else:
        _net_status(rep, ctx, "dns", "OK", f"{ready}/{len(pods)} DNS pods ready, no unusual restarts", "No action needed.")


def _net_dns_logs(rep, ctx):
    rep.sub("Domain Name System (DNS): error logs", f"error lines in the last {ctx.minutes} min from the cluster DNS pods: SERVFAIL, REFUSED, timeouts towards upstream servers.",
            ["DNS", "kube-dns", "CoreDNS"])
    lines, err = _read_logs(ctx, "k8s-app=kube-dns")
    if lines is None:
        _net_status(rep, ctx, "dns_logs", "Not available", f"DNS pod logs unreadable: {err}", "Grant pods/log permission in kube-system, or read the logs in Cloud Logging.")
        return
    patterns = {"SERVFAIL": r"SERVFAIL", "REFUSED": r"REFUSED", "timeouts": r"i/o timeout|timed out", "NXDOMAIN": r"NXDOMAIN", "errors": r"\[ERROR\]|plugin/errors"}
    counts = {k: sum(1 for l in lines if re.search(p, l)) for k, p in patterns.items()}
    sample = next((l for l in reversed(lines) if re.search(r"error|fail|timeout|SERVFAIL|REFUSED", l, re.I)), "")
    rows = [["kube-dns / CoreDNS", len(lines), counts["errors"], ", ".join(f"{k} {v}" for k, v in counts.items() if v and k != "errors") or "-", sample[:110]]]
    rep.table(["COMPONENT", "LOG LINES READ", "ERROR-LIKE LINES", "BREAKDOWN", "LATEST ERROR LINE"], rows, maxw=70,
              about="how many DNS log lines were read, how many are errors and which kinds (SERVFAIL, REFUSED, timeouts, NXDOMAIN), plus the newest error line.")
    if counts["timeouts"] + counts["SERVFAIL"] >= 10:
        ctx.find("MED", f"kube-dns (DNS): {counts['timeouts']} timeouts and {counts['SERVFAIL']} SERVFAIL in the window - DNS problems likely")
        _net_status(rep, ctx, "dns_logs", "Problem", f"{counts['timeouts']} timeouts and {counts['SERVFAIL']} SERVFAIL answers in the last {ctx.minutes} min",
                    "Applications will see slow or failed lookups. Check the upstream resolvers (stub domains / forwarders), the DNS pod load and the node network.")
    elif counts["errors"] or counts["timeouts"] or counts["SERVFAIL"] or counts["REFUSED"]:
        _net_status(rep, ctx, "dns_logs", "Warning", "some DNS error lines: " + (", ".join(f"{k} {v}" for k, v in counts.items() if v) or "-"),
                    "A few errors are normal; watch whether the numbers grow. NXDOMAIN alone only means a name does not exist.")
    else:
        _net_status(rep, ctx, "dns_logs", "OK", f"no DNS error lines among {len(lines)} log lines", "No action needed.")


def _net_nodelocal_dns(rep, ctx):
    rep.sub("Domain Name System (DNS): NodeLocal DNSCache", "whether every node has a local DNS cache, which cuts DNS latency and load on the cluster DNS pods; recommended for larger clusters.",
            ["NodeLocal DNSCache", "DNS", "DaemonSet"])
    cluster = ctx.data.get("gcp_cluster") or {}
    addon = ((cluster.get("addonsConfig") or {}).get("dnsCacheConfig") or {}).get("enabled")
    ds = _workload(ctx, "node-local-dns")
    present = bool(ds) or bool(addon)
    n_nodes = len(items(ctx.data.get("nodes")))
    rows = [["Add-on dnsCacheConfig", "enabled" if addon else ("disabled" if cluster else "unknown")],
            ["node-local-dns DaemonSet", f"{ds[1]}/{ds[2]} ready" if ds else "not found"], ["Nodes", n_nodes]]
    rep.table(["ITEM", "VALUE"], rows, maxw=70, about="whether the NodeLocal DNS cache add-on is switched on and its DaemonSet is ready, next to the number of nodes.")
    if ctx.data.get("daemonsets") is None and not cluster:
        _net_status(rep, ctx, "nodelocal", "Not available", "neither the DaemonSets nor the cluster add-ons could be read", "Check permissions or turn Google Cloud details on.")
    elif present and ds and ds[1] < ds[2]:
        _net_status(rep, ctx, "nodelocal", "Warning", f"NodeLocal DNSCache is on but only {ds[1]}/{ds[2]} pods are ready", "Pods on nodes without the cache fall back to kube-dns; check the node-local-dns pods.", "NodeLocal DNSCache")
    elif present:
        _net_status(rep, ctx, "nodelocal", "OK", "NodeLocal DNSCache is enabled", "No action needed.")
    elif n_nodes >= NODELOCAL_DNS_NODES:
        _net_status(rep, ctx, "nodelocal", "Warning", f"NodeLocal DNSCache is not enabled and the cluster has {n_nodes} nodes",
                    "For clusters of this size, enable NodeLocal DNSCache (GKE add-on) to reduce DNS latency and conntrack pressure.", "NodeLocal DNSCache")
    else:
        _net_status(rep, ctx, "nodelocal", "OK", f"NodeLocal DNSCache is not enabled; the cluster is small ({n_nodes} nodes, recommendation starts at {NODELOCAL_DNS_NODES})",
                    "Consider enabling it if you see DNS timeouts or the cluster grows.")


def _net_ndots(rep, ctx):
    rep.sub("Domain Name System (DNS): pod name-resolution settings (ndots) from a sample of pods",
            "how many dots a name needs before pods try it as written; the Kubernetes default of 5 makes every external name be tried with several suffixes first, multiplying DNS queries.",
            ["ndots", "DNS", "hostNetwork"])
    pods = [p for p in items(ctx.data.get("pods")) if p.get("status", {}).get("phase") == "Running"]
    if ctx.data.get("pods") is None:
        _net_status(rep, ctx, "ndots", "Not available", "pods could not be read", "Check permissions.")
        return
    sample = pods[:300]
    eff, by_ns = Counter(), defaultdict(Counter)
    for p in sample:
        spec = p.get("spec", {})
        policy = spec.get("dnsPolicy", "ClusterFirst")
        opts = {o.get("name"): o.get("value") for o in (spec.get("dnsConfig") or {}).get("options") or []}
        if "ndots" in opts:
            val = f"ndots:{opts['ndots']} (set in the pod)"
        elif policy == "ClusterFirstWithHostNet" or (policy == "ClusterFirst" and not spec.get("hostNetwork")):
            val = "ndots:5 (Kubernetes default)"
        elif policy == "Default" or spec.get("hostNetwork"):
            val = "node resolver settings (not readable)"
        else:
            val = "set by dnsConfig only"
        eff[val] += 1
        by_ns[val][p["metadata"]["namespace"]] += 1
    rows = [[k, v, f"{100 * v / len(sample):.0f}%" if sample else "-", ", ".join(f"{n} ({c})" for n, c in by_ns[k].most_common(3))] for k, v in eff.most_common()]
    rep.table(["EFFECTIVE SETTING", "PODS IN SAMPLE", "SHARE OF SAMPLE", "TOP NAMESPACES"], rows, maxw=70,
              about=f"the name-resolution setting that applies to each of the {len(sample)} sampled running pods, worked out from the pod specs (the real resolv.conf inside pods is not read: no pod exec).")
    d5 = eff.get("ndots:5 (Kubernetes default)", 0)
    if not sample:
        _net_status(rep, ctx, "ndots", "Not available", "no running pods to sample", "Nothing to check.")
    elif d5:
        ctx.find("INFO", f"{d5} of {len(sample)} sampled pods use the default ndots:5 - external names are tried with several search suffixes first (more DNS queries); "
                         "set dnsConfig ndots to 2 for chatty applications or use fully qualified names ending with a dot")
        _net_status(rep, ctx, "ndots", "OK", f"informational: {d5} of {len(sample)} sampled pods use the default ndots:5",
                    "Not an error. If DNS load or latency is a problem, lower ndots (for example 2) in the pod's dnsConfig or write names with a trailing dot.")
    else:
        _net_status(rep, ctx, "ndots", "OK", f"none of the {len(sample)} sampled pods uses the default ndots:5", "No action needed.")


# --- E. network policies and cloud firewalls ----------------------------------------------------------------------

def _net_policies(rep, ctx):
    rep.sub("Network policies and cloud firewalls: NetworkPolicy objects per namespace",
            "which namespaces restrict pod-to-pod traffic and whether each has a default-deny policy (one that selects every pod and allows nothing).", ["NetworkPolicy"])
    pols = items(ctx.data.get("networkpolicies"))
    if ctx.data.get("networkpolicies") is None:
        _net_status(rep, ctx, "policies", "Not available", "NetworkPolicy objects could not be read", "Check permissions (kubectl get networkpolicy -A).")
        return
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

    def verdict(n):
        d = per_ns[n]
        if d["deny_in"] and d["deny_out"]:
            return "yes: ingress and egress"
        if d["deny_in"] or d["deny_out"]:
            return "yes: " + ("ingress only" if d["deny_in"] else "egress only")
        return "no (other policies only)" if d["n"] else "no (no policy at all)"
    prow = [[n, support_of(ctx, n) or "-", pod_ns[n], per_ns[n]["n"], verdict(n)] for n in sorted(set(pod_ns) | set(per_ns))]
    rep.table(["NAMESPACE", "SUPPORT DISTRIBUTION LIST", "PODS", "NUMBER OF POLICIES", "DEFAULT DENY POLICY PRESENT"], prow,
              about=f"{len(pols)} NetworkPolicy object(s) in {len(per_ns)} namespace(s): for each namespace with pods, how many policies exist and whether a default-deny policy is among them.")
    open_ns = [n for n in pod_ns if not per_ns[n]["n"] and n not in ("kube-system",)]
    no_default = [n for n in pod_ns if n not in ("kube-system",) and not (per_ns[n]["deny_in"] or per_ns[n]["deny_out"])]
    if open_ns:
        ctx.find("INFO", f"{len(open_ns)} namespace(s) with pods have no NetworkPolicy (all pod-to-pod traffic allowed unless a mesh/CNI restricts it): "
                         + ", ".join(sorted(open_ns)[:8]) + (" ..." if len(open_ns) > 8 else ""))
    if not pols:
        _net_status(rep, ctx, "policies", "Warning", "no NetworkPolicy exists: all pod-to-pod traffic is allowed",
                    "If you need isolation, add a default-deny policy per namespace and allow only the needed flows. If it is not needed, this is fine.")
    elif no_default:
        _net_status(rep, ctx, "policies", "Warning", f"{len(no_default)} of {len(pod_ns) - ('kube-system' in pod_ns)} application namespace(s) have no default-deny policy: " + ", ".join(sorted(no_default)[:6]),
                    "Without default-deny, pods are open to anything not explicitly blocked. When traffic is blocked unexpectedly, look at the policies of the destination namespace first.")
    else:
        _net_status(rep, ctx, "policies", "OK", "every application namespace with pods has a default-deny policy", "When traffic is blocked unexpectedly, check that an allow rule exists for it in the destination namespace.")


def _net_policy_engine(rep, ctx):
    rep.sub("Network policies and cloud firewalls: policy engine and enforcement",
            "which component enforces NetworkPolicy objects and whether enforcement is switched on for the cluster; policies without an engine have no effect at all.",
            ["NetworkPolicy", "Dataplane V2", "Calico"])
    cluster = ctx.data.get("gcp_cluster") or {}
    pols = items(ctx.data.get("networkpolicies"))
    np_ = cluster.get("networkPolicy") or {}
    v2 = _is_dataplane_v2(ctx)
    calico = _workload(ctx, "calico-node")
    addon_off = ((cluster.get("addonsConfig") or {}).get("networkPolicyConfig") or {}).get("disabled")
    engine = "Dataplane V2 (Cilium), always enforcing" if v2 else ("Calico" if (np_.get("enabled") and np_.get("provider") == "CALICO") else ("none enabled" if cluster else "unknown"))
    rows = [["Policy engine", engine], ["Cluster network policy setting", ("enabled, provider " + str(np_.get("provider"))) if np_.get("enabled") else ("built into Dataplane V2" if v2 else ("disabled" if cluster else "unknown"))],
            ["Network policy add-on", "disabled" if addon_off else ("enabled" if addon_off is False else "unknown")],
            ["calico-node pods", f"{calico[1]}/{calico[2]} ready" if calico else "not found"], ["NetworkPolicy objects", len(pols)]]
    rep.table(["ITEM", "VALUE"], rows, maxw=70, about="which engine enforces network policy, whether the cluster setting and add-on are on, whether Calico pods are ready, and how many policy objects exist.")
    if not cluster and v2 is None and not calico:
        _net_status(rep, ctx, "engine", "Not available", "cluster details are unavailable and no policy engine pods were found", "Turn Google Cloud details on to read the cluster's network policy setting.")
    elif pols and engine in ("none enabled",):
        _net_status(rep, ctx, "engine", "Problem", f"{len(pols)} NetworkPolicy object(s) exist but no policy engine is enabled on the cluster",
                    "The policies are NOT enforced: traffic is not blocked. Enable network policy enforcement (Dataplane V2, or Calico) on the cluster.", "Network policy enforcement")
    elif calico and calico[1] < calico[2]:
        _net_status(rep, ctx, "engine", "Warning", f"Calico is the engine but only {calico[1]}/{calico[2]} calico-node pods are ready",
                    "Nodes without a ready agent enforce stale or no policy; check the calico-node pods.", "Network policy enforcement")
    elif engine == "none enabled":
        _net_status(rep, ctx, "engine", "OK", "no policy engine is enabled and no NetworkPolicy exists, so nothing is expected to be enforced", "Enable Dataplane V2 or Calico before relying on NetworkPolicy.")
    else:
        _net_status(rep, ctx, "engine", "OK", f"policy engine: {engine}", "No action needed.")


def _net_firewalls(rep, ctx):
    rep.sub("Network policies and cloud firewalls: virtual private cloud firewall rules",
            "the firewall rules of the virtual private cloud network that matter for GKE traffic: risky open rules, the Google health check ranges, the node port range and pod-to-pod rules.",
            ["VPC", "Health check ranges", "NodePort", "CIDR"])
    rules = ctx.data.get("gcp_fw_rules")
    if rules is None:
        _net_status(rep, ctx, "fw", "Not available", "firewall rules could not be read (the Google Cloud section is off or needs compute.firewalls.list / roles/compute.viewer)",
                    "Without the rules you cannot confirm that health checks, node ports and pod traffic are allowed. Use the Network Intelligence Center Connectivity Tests as an alternative.")
        return
    live = [r for r in rules if not r.get("disabled")]
    ing = [r for r in live if r.get("direction") == "INGRESS" and r.get("allowed")]
    srcs = lambda r: set(r.get("sourceRanges") or [])
    hc_ok = any(any(h in srcs(r) for h in GKE_HEALTH_CHECK_RANGES) or "0.0.0.0/0" in srcs(r) for r in ing)
    internet_all = [r.get("name") for r in ing if "0.0.0.0/0" in srcs(r) and any((a.get("IPProtocol") or "").lower() == "all" for a in r.get("allowed") or [])]
    ssh_open = [r.get("name") for r in ing if "0.0.0.0/0" in srcs(r) and (_port_open(r.get("allowed"), 22) or _port_open(r.get("allowed"), 3389))]
    np_open = any(_port_open(r.get("allowed"), 30000) or _port_open(r.get("allowed"), 32767) for r in ing)
    pod_rule = [r.get("name") for r in ing if (r.get("name") or "").startswith("gke-") and "all" in (r.get("name") or "")]
    rows = [["Rules on this network", f"{len(rules)} ({len(rules) - len(live)} disabled)", "-"],
            ["Ingress rules open to the whole internet on all ports", ", ".join(internet_all[:3]) or "none", "Problem" if internet_all else "OK"],
            ["Ingress rules open to the internet on SSH or RDP", ", ".join(ssh_open[:3]) or "none", "Warning" if ssh_open else "OK"],
            ["Google health check ranges allowed (130.211.0.0/22, 35.191.0.0/16)", "yes" if hc_ok else "no rule found", "OK" if hc_ok else "Warning"],
            ["Node port range 30000-32767 allowed", "yes (some rule covers it)" if np_open else "no rule covers the range", "OK" if np_open else "Warning"],
            ["GKE pod-to-pod / node rules (gke-*)", ", ".join(pod_rule[:3]) or "no gke-*-all rule found", "OK" if pod_rule else "Warning"]]
    rep.table(["CHECK", "WHAT WAS FOUND", "RESULT"], rows, maxw=70,
              about="six common firewall questions answered from the rules: how many rules exist, dangerous open rules, and whether health checks, node ports and pod traffic are allowed.")
    states = [r[2] for r in rows if r[2] != "-"]
    if internet_all:
        _net_status(rep, ctx, "fw", "Problem", "rule(s) allow ALL protocols from the internet: " + ", ".join(internet_all[:3]), "Close or narrow these rules at once; they expose every port of the targets.")
    elif "Warning" in states:
        why = [r[0] for r in rows if r[2] == "Warning"]
        _net_status(rep, ctx, "fw", "Warning", "; ".join(why),
                    "Load balancer backends are marked unhealthy when health check ranges are blocked, and NodePort services are unreachable without a rule. Add narrow allow rules only where needed.")
    else:
        _net_status(rep, ctx, "fw", "OK", f"{len(rules)} rule(s) read; health check ranges, node ports and pod traffic are allowed; nothing is open to the internet on all ports",
                    "No action needed. Remember the implied rules: deny all ingress, allow all egress.")


def _net_private_routing(rep, ctx, target):
    rep.sub("Network policies and cloud firewalls: Cloud NAT (network address translation), virtual private cloud (VPC) peering and private cluster routing",
            "how the cluster reaches the internet and Google APIs (Cloud NAT, Private Google Access), how the control plane is connected (VPC peering) and who may reach the API server.",
            ["NAT", "VPC peering", "Private Google Access", "Authorized networks", "VPC"])
    c = ctx.data.get("gcp_cluster") or {}
    if not c:
        _net_status(rep, ctx, "routing", "Not available", "the cluster object could not be read (Google Cloud section off or failed)", "Turn Google Cloud details on (gcloud auth login).")
        return
    priv = c.get("privateClusterConfig") or {}
    man = c.get("masterAuthorizedNetworksConfig") or {}
    cidrs = [b.get("cidrBlock") for b in man.get("cidrBlocks") or []]
    subnets = ctx.data.get("gcp_subnets") or []
    pga = subnets[0].get("privateIpGoogleAccess") if subnets else None
    nats = ctx.data.get("gcp_nats")
    vpc = ctx.data.get("gcp_vpc") or {}
    peerings = vpc.get("peerings") or []
    rows = [["Private nodes", "yes" if priv.get("enablePrivateNodes") else "no", "Nodes without external IP addresses need Cloud NAT or Private Google Access to reach anything outside."],
            ["Private control plane endpoint", "yes" if priv.get("enablePrivateEndpoint") else "no (public endpoint)", "A private endpoint is reachable only from inside the network."],
            ["Control plane peering", priv.get("peeringName") or ("(not a private cluster)" if not priv.get("enablePrivateNodes") else "(name not reported)"), "GKE connects the managed control plane to your network with a VPC peering."],
            ["Control plane address range", priv.get("masterIpv4CidrBlock") or "-", "Must not overlap other ranges that are routed to this network."],
            ["Authorized networks", ("on: " + ", ".join(str(x) for x in cidrs[:4])) if man.get("enabled") else "off", "Only these ranges may reach the API server when on."],
            ["Private Google Access on the node subnet", "on" if pga else ("off" if pga is not None else "unknown"), "Lets nodes without external IP addresses reach Google APIs."],
            ["Cloud NAT gateways", (", ".join(n["name"] for n in nats[:3]) or "none") if nats is not None else "unknown (not readable)", "Gives private nodes and pods outbound internet access."],
            ["Network peerings of the VPC", ", ".join(f"{p.get('name')} ({p.get('state')})" for p in peerings[:4]) or ("none" if vpc else "unknown"), "Each peering should be ACTIVE; INACTIVE means the other side deleted it."]]
    rep.table(["SETTING", "VALUE", "WHY IT MATTERS"], rows, maxw=70, about="the settings that decide outbound internet access, how the control plane is connected and who may reach the API server.")
    inactive = [p.get("name") for p in peerings if p.get("state") != "ACTIVE"]
    if inactive:
        _net_status(rep, ctx, "routing", "Problem", "VPC peering not ACTIVE: " + ", ".join(inactive[:3]), "Traffic over an inactive peering is dropped; recreate it from both sides.", "Virtual private cloud peering")
    elif priv.get("enablePrivateNodes") and nats is not None and not nats and not pga:
        _net_status(rep, ctx, "routing", "Problem", "private nodes but neither Cloud NAT nor Private Google Access: pods cannot reach the internet or Google APIs",
                    "Create a Cloud NAT gateway for the subnet and/or enable Private Google Access.", "Private cluster routing")
    elif priv.get("enablePrivateNodes") and nats is None:
        _net_status(rep, ctx, "routing", "Not available", "private nodes, but Cloud NAT configuration could not be read", "Needs compute.routers.list; check the NAT gateways manually.")
    elif not priv.get("enablePrivateEndpoint") and not man.get("enabled"):
        _net_status(rep, ctx, "routing", "Warning", "the API server endpoint is public and no authorized networks limit who can reach it",
                    "Protected only by authentication. Add authorized networks to restrict access.")
    else:
        _net_status(rep, ctx, "routing", "OK", "outbound routing and control plane access are configured consistently", "No action needed.")


# --- F. load balancers and ingress -------------------------------------------------------------------------------

def _net_ingresses(rep, ctx):
    rep.sub("Load balancers and ingress: Ingress objects",
            "every Ingress with its class (which load balancer type it creates), whether it is internet-facing, its address, TLS and backend health as GKE reports it.",
            ["Ingress", "TLS", "Load balancer", "NEG"])
    ings = items(ctx.data.get("ingresses"))
    if ctx.data.get("ingresses") is None:
        _net_status(rep, ctx, "ingress", "Not available", "Ingress objects could not be read", "Check permissions (kubectl get ingress -A).")
        return
    if not ings:
        _net_status(rep, ctx, "ingress", "OK", "no Ingress objects exist in the cluster", "No action needed.")
        return
    irows, public_ing, worst = [], [], []
    for i in ings:
        meta, spec = i["metadata"], i.get("spec", {})
        ann = meta.get("annotations") or {}
        lb = (i.get("status", {}).get("loadBalancer") or {}).get("ingress") or []
        address = (lb[0].get("hostname") or lb[0].get("ip")) if lb else ""
        hosts = sorted({r.get("host") or "*" for r in spec.get("rules", []) or []})
        paths = sum(len((r.get("http") or {}).get("paths", [])) for r in spec.get("rules", []) or [])
        klass = spec.get("ingressClassName") or ann.get("kubernetes.io/ingress.class") or "-"
        if klass == "gce-internal":
            exposure = "internal"
        elif klass in ("gce", "gce-multi-cluster", "-"):
            exposure = "INTERNET-FACING"
            public_ing.append(f"{meta['namespace']}/{meta['name']}")
        else:
            exposure = "-"
        backends = "-"
        try:
            states = json.loads(ann.get("ingress.kubernetes.io/backends") or "{}")
        except json.JSONDecodeError:
            states = {}
        if states:
            bad = {k: v for k, v in states.items() if str(v).upper() != "HEALTHY"}
            backends = f"{len(states) - len(bad)}/{len(states)} healthy"
            if bad:
                worst.append("Problem" if len(bad) == len(states) else "Warning")
                ctx.find("HIGH" if len(bad) == len(states) else "MED", f"Ingress {meta['namespace']}/{meta['name']}: {len(bad)} of {len(states)} load balancer backend(s) not HEALTHY ("
                         + ", ".join(f"{k}={v}" for k, v in list(bad.items())[:2]) + ")" + support_suffix(ctx, [meta["namespace"]]))
                ctx.ns_issue(meta["namespace"], f"ingress {meta['name']}: backends not healthy")
        irows.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], klass, exposure, ", ".join(hosts)[:60],
                      address[:60] or "NO ADDRESS", "yes" if spec.get("tls") or ann.get("networking.gke.io/managed-certificates") else "no", paths, backends])
        if not address:
            worst.append("Warning")
            ctx.find("MED", f"Ingress {meta['namespace']}/{meta['name']} has no load balancer address" + support_suffix(ctx, [meta["namespace"]]))
            ctx.ns_issue(meta["namespace"], f"ingress {meta['name']} has no address")
    if public_ing:
        ctx.find("INFO", f"{len(public_ing)} Ingress(es) use an external (internet-facing) HTTP(S) load balancer: " + ", ".join(public_ing[:6])
                 + (" ..." if len(public_ing) > 6 else "") + support_suffix(ctx, {x.split("/")[0] for x in public_ing}))
    rep.table(["NAMESPACE", "SUPPORT DISTRIBUTION LIST", "INGRESS", "CLASS", "EXPOSURE", "HOSTS", "ADDRESS", "ENCRYPTED WITH TRANSPORT LAYER SECURITY", "NUMBER OF PATHS", "BACKEND HEALTH (AS REPORTED BY GOOGLE KUBERNETES ENGINE)"], irows, maxw=64,
              about=f"{len(ings)} Ingress object(s): the class decides the load balancer type, ADDRESS is the IP address Google Cloud assigned, and BACKEND HEALTH counts the healthy backends.")
    st = _combine(worst) if worst else "OK"
    if st in ("Problem", "Warning"):
        _net_status(rep, ctx, "ingress", st, "some Ingress objects have no address or unhealthy backends (see the table)",
                    "No address: the load balancer was not created (check events and quota). Unhealthy backends: check pod readiness, the health check path and the firewall for Google health check ranges.")
    else:
        _net_status(rep, ctx, "ingress", "OK", f"{len(ings)} Ingress object(s), all with an address; no unhealthy backend reported", "No action needed.")


def _net_controllers(rep, ctx):
    rep.sub("Load balancers and ingress: ingress controllers and recent 502 / 503 / 504 errors",
            f"the software that turns Ingress and Gateway objects into load balancers (GKE's managed controller, nginx, Gateway API) with pod health and the 502, 503, 504 error counts of the last {ctx.minutes} min.",
            ["Ingress", "Gateway API", "502/503/504"])
    rows, states = [], []
    ings = items(ctx.data.get("ingresses"))
    classes = Counter((i.get("spec", {}).get("ingressClassName") or (i["metadata"].get("annotations") or {}).get("kubernetes.io/ingress.class") or "gce") for i in ings)
    if any(k in ("gce", "gce-internal", "gce-multi-cluster") for k in classes):
        d = _workload(ctx, "l7-default-backend")
        rows.append(["GKE Ingress controller (Google-managed)", "outside the cluster", "-", "-", "-", "-", "-", "-",
                     f"default backend {d[1]}/{d[2]} ready" if d else "default backend not visible"])
        if d and d[1] < d[2]:
            states.append("Warning")
    nginx = [p for p in items(ctx.data.get("pods")) if (p["metadata"].get("labels") or {}).get("app.kubernetes.io/name") in ("ingress-nginx", "nginx-ingress")
             or p["metadata"]["name"].startswith(("ingress-nginx-controller", "nginx-ingress-controller", "nginx-ingress-"))]
    if nginx:
        ns = nginx[0]["metadata"]["namespace"]
        lbl = (nginx[0]["metadata"].get("labels") or {}).get("app.kubernetes.io/name") or "ingress-nginx"
        lines, err = _read_logs(ctx, f"app.kubernetes.io/name={lbl}", ns)
        ready = sum(1 for p in nginx if _pod_ready(p))
        rs = sum(_pod_restarts(p) for p in nginx)
        if lines is None:
            rows.append(["nginx ingress controller", ns, f"{ready}/{len(nginx)}", rs, "-", "-", "-", "-", f"logs unavailable: {err}"])
            states.append("Not available")
        else:
            c = {code: sum(1 for l in lines if re.search(r'"\s%s\s' % code, l)) for code in ("502", "503", "504")}
            errl = sum(1 for l in lines if re.search(r"\[error\]|\berror\b", l, re.I))
            rows.append(["nginx ingress controller", ns, f"{ready}/{len(nginx)}", rs, len(lines), c["502"], c["503"], c["504"], f"{sum(c.values())} 5xx response(s), {errl} error line(s)"])
            total5 = sum(c.values())
            states.append("Problem" if ready < len(nginx) or total5 >= 10 else ("Warning" if total5 or errl else "OK"))
            if total5:
                ctx.find("MED", f"nginx ingress controller logged {total5} 502/503/504 responses in the window")
    gw, gerr = _kj(["get", "gateways.gateway.networking.k8s.io", "-A"])
    if gw and items(gw):
        for g in items(gw)[:8]:
            conds = {c.get("type"): c.get("status") for c in g.get("status", {}).get("conditions") or []}
            ok = conds.get("Programmed", conds.get("Ready")) == "True"
            rows.append([f"Gateway {g['metadata']['namespace']}/{g['metadata']['name']}", g.get("spec", {}).get("gatewayClassName", "-"), "programmed" if ok else "NOT programmed", "-", "-", "-", "-", "-", "Gateway API"])
            states.append("OK" if ok else "Warning")
    rep.table(["CONTROLLER", "NAMESPACE OR CLASS", "PODS READY", "RESTARTS", "LOG LINES READ", "HTTP 502", "HTTP 503", "HTTP 504", "NOTE"], rows, maxw=50,
              about="each ingress controller or Gateway found, with pod readiness and how many 502, 503 and 504 responses its logs show (GKE's managed controller has no pods in the cluster).")
    if not rows:
        _net_status(rep, ctx, "controllers", "OK" if ctx.data.get("ingresses") is not None else "Not available",
                    "no ingress controller pod or Gateway object found and no GKE Ingress uses the managed controller" if ctx.data.get("ingresses") is not None else "pods and Ingress objects could not be read",
                    "No action needed." if ctx.data.get("ingresses") is not None else "Check permissions.")
    else:
        st = _combine(states)
        _net_status(rep, ctx, "controllers", st, ("; ".join(f"{r[0]} {r[8]}" for r in rows))[:230],
                    "502 = the backend closed the connection, 503 = no healthy backend, 504 = the backend was too slow. Check pod readiness, readiness probe paths and backend timeouts."
                    if st != "OK" else "No action needed.", "Ingress controllers" if st in ("Problem", "Warning") else None)


def _net_backend_annotations(rep, ctx):
    rep.sub("Load balancers and ingress: BackendConfig and network endpoint group (NEG) annotations",
            "how Services are connected to the Google Cloud load balancer: NEG annotations (container-native load balancing) and the BackendConfig objects that set timeouts and health checks.",
            ["BackendConfig", "NEG", "Load balancer", "Ingress"])
    services = items(ctx.data.get("services"))
    bcs_raw, bcerr = _kj(["get", "backendconfigs.cloud.google.com", "-A"])
    bcs = {(b["metadata"]["namespace"], b["metadata"]["name"]): b for b in items(bcs_raw)}
    rows, missing = [], []
    for s in services:
        ann = s["metadata"].get("annotations") or {}
        neg, bc, negst = ann.get("cloud.google.com/neg"), ann.get("cloud.google.com/backend-config"), ann.get("cloud.google.com/neg-status")
        if not (neg or bc or negst):
            continue
        refs = []
        if bc:
            try:
                refs = list((json.loads(bc) or {}).values())
            except json.JSONDecodeError:
                refs = [bc]
        absent = [r for r in refs if bcs_raw is not None and (s["metadata"]["namespace"], r) not in bcs]
        if absent:
            missing += [f"{s['metadata']['namespace']}/{r}" for r in absent]
        zones = ""
        try:
            zones = ", ".join(sorted((json.loads(negst).get("zones") or []))) if negst else ""
        except (json.JSONDecodeError, AttributeError):
            zones = ""
        rows.append([s["metadata"]["namespace"], s["metadata"]["name"], neg or "-", ", ".join(refs) or "-", zones or "-",
                     ("BackendConfig NOT FOUND: " + ", ".join(absent)) if absent else "ok"])
    rep.table(["NAMESPACE", "SERVICE", "NETWORK ENDPOINT GROUP ANNOTATION", "BACKENDCONFIG REFERENCED", "NETWORK ENDPOINT GROUP ZONES", "CHECK"], rows, maxw=60,
              about="Services that use container-native load balancing (a network endpoint group annotation) or reference a BackendConfig, and whether the referenced BackendConfig exists.")
    if bcs:
        brow = []
        for (ns, name), b in bcs.items():
            sp = b.get("spec", {})
            hc = sp.get("healthCheck") or {}
            brow.append([ns, name, sp.get("timeoutSec", "-"), (sp.get("connectionDraining") or {}).get("drainingTimeoutSec", "-"),
                         (hc.get("requestPath") or "-") + (f" port {hc['port']}" if hc.get("port") else ""), "on" if (sp.get("cdn") or {}).get("enabled") else "off",
                         (sp.get("securityPolicy") or {}).get("name", "-")])
        rep.table(["NAMESPACE", "BACKENDCONFIG", "BACKEND TIMEOUT (SECONDS)", "CONNECTION DRAINING (SECONDS)", "HEALTH CHECK PATH", "CONTENT DELIVERY NETWORK", "SECURITY POLICY"], brow, maxw=50,
                  about="the BackendConfig objects: the backend timeout (30 seconds default, a common cause of 504), connection draining, the load balancer health check and optional security policy.")
    if missing:
        _net_status(rep, ctx, "backendcfg", "Problem", "Service(s) reference a BackendConfig that does not exist: " + ", ".join(missing[:4]),
                    "The load balancer falls back to defaults or fails to sync. Create the BackendConfig in the Service's namespace or remove the annotation.", "BackendConfig")
    elif bcs_raw is None and any(r[3] != "-" for r in rows):
        _net_status(rep, ctx, "backendcfg", "Not available", f"Services reference BackendConfigs but the objects could not be read: {_first_line(bcerr or 'no data', 80)}", "Check permissions or whether the CRD exists.")
    elif not rows:
        _net_status(rep, ctx, "backendcfg", "OK" if ctx.data.get("services") is not None else "Not available",
                    "no Service uses a network endpoint group or BackendConfig annotation" if ctx.data.get("services") is not None else "Services could not be read",
                    "No action needed. GKE Ingress then uses the default node-based backends; container-native load balancing (NEG) is recommended.")
    else:
        _net_status(rep, ctx, "backendcfg", "OK", f"{len(rows)} Service(s) with NEG / BackendConfig annotations, all references exist", "No action needed.")


def _cert_expiry(pem_text):
    """Expiry (datetime, UTC) of the first certificate in a PEM text, or None. Uses Python's own certificate decoder; nothing is installed or sent anywhere."""
    import ssl
    import tempfile
    path = None
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as f:
            f.write(pem_text)
            path = f.name
        info = ssl._ssl._test_decode_cert(path)
        return datetime.strptime(info["notAfter"], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
    except Exception:
        return None
    finally:
        if path:
            try:
                os.remove(path)
            except OSError:
                pass


def _net_tls(rep, ctx):
    rep.sub("Load balancers and ingress: Transport Layer Security (TLS) certificates used by Ingress objects",
            "the certificates behind HTTPS Ingress objects (Kubernetes TLS Secrets and Google-managed certificates) with their status and expiry date; an expired certificate breaks HTTPS for all users.",
            ["TLS", "Ingress"])
    import base64
    ings = items(ctx.data.get("ingresses"))
    if ctx.data.get("ingresses") is None:
        _net_status(rep, ctx, "tls", "Not available", "Ingress objects could not be read", "Check permissions.")
        return
    mc_raw, _mcerr = _kj(["get", "managedcertificates.networking.gke.io", "-A"])
    mcs = {(m["metadata"]["namespace"], m["metadata"]["name"]): m for m in items(mc_raw)}
    rows, states, read_fail = [], [], 0
    secret_calls = {}
    for i in ings:
        for t in i.get("spec", {}).get("tls") or []:
            if t.get("secretName") and (i["metadata"]["namespace"], t["secretName"]) not in secret_calls:
                secret_calls[(i["metadata"]["namespace"], t["secretName"])] = pf(_kt, ["get", "secret", t["secretName"], "-n", i["metadata"]["namespace"], "-o",
                                                                                      r"jsonpath={.data.tls\.crt}"], 30)   # only the PUBLIC certificate is read, never the key
    for i in ings:
        meta, spec = i["metadata"], i.get("spec", {})
        ns, name = meta["namespace"], meta["name"]
        for t in spec.get("tls") or []:
            sec = t.get("secretName")
            if not sec:
                continue
            ok, out = secret_calls[(ns, sec)].result()
            exp, note, state = None, "", "Not available"
            if ok and out:
                try:
                    exp = _cert_expiry(base64.b64decode(out).decode("utf-8", "replace"))
                except Exception:
                    exp = None
            if exp:
                days = int((exp - ctx.now).total_seconds() // 86400)
                state = "Problem" if days < 14 else ("Warning" if days < 30 else "OK")
                note = f"expires in {days} day(s)" if days >= 0 else f"EXPIRED {-days} day(s) ago"
            else:
                read_fail += 1
                note = "certificate not readable" + (f": {out.splitlines()[0][:60]}" if (not ok and out) else "")
            rows.append([ns, name, "Kubernetes TLS Secret", sec, "-", f"{exp:%Y-%m-%d}" if exp else "-", state, note])
            states.append(state)
        for mname in [x.strip() for x in ((meta.get("annotations") or {}).get("networking.gke.io/managed-certificates") or "").split(",") if x.strip()]:
            m = mcs.get((ns, mname))
            if not m:
                rows.append([ns, name, "Google-managed certificate", mname, "object not found", "-", "Not available", "ManagedCertificate not readable or missing"])
                states.append("Not available")
                continue
            st = m.get("status", {})
            cs = st.get("certificateStatus") or "unknown"
            exp = parse_ts(st.get("expireTime"))
            if cs == "Active":
                days = int((exp - ctx.now).total_seconds() // 86400) if exp else None
                state = "OK" if days is None or days >= 14 else "Warning"
            elif cs in ("Provisioning", "ProvisioningFailedPermanently", "Failed", "FailedNotVisible", "RenewalFailed"):
                state = "Warning" if cs == "Provisioning" else "Problem"
            else:
                state = "Warning"
            doms = ", ".join(f"{d.get('domain')}={d.get('status')}" for d in st.get("domainStatus") or [])[:60]
            rows.append([ns, name, "Google-managed certificate", mname, cs, f"{exp:%Y-%m-%d}" if exp else "-", state, doms or "-"])
            states.append(state)
    rep.table(["NAMESPACE", "INGRESS", "CERTIFICATE SOURCE", "CERTIFICATE NAME", "CERTIFICATE STATUS", "EXPIRES ON", "RESULT", "NOTE"], rows, maxw=50,
              about="every certificate an Ingress refers to: where it comes from, its status (managed certificates) or expiry date (TLS Secrets, only the public certificate is read) and a result.")
    if not rows:
        _net_status(rep, ctx, "tls", "OK", "no Ingress references a TLS Secret or a Google-managed certificate", "No action needed.")
        return
    st = _combine(states)
    if st in ("Problem", "Warning"):
        bad = [f"{r[1]}: {r[7] if r[7] != '-' else r[4]}" for r in rows if r[6] in ("Problem", "Warning")][:3]
        _net_status(rep, ctx, "tls", st, "; ".join(bad), "Renew the certificate before it expires (cert-manager, or re-issue the Secret). Google-managed certificates need the domain's DNS to point at the load balancer address.", "TLS certificates")
    elif st == "Not available":
        _net_status(rep, ctx, "tls", "Not available", f"{read_fail or len(rows)} certificate(s) could not be read", "Needs permission to get Secrets (public certificate only) and ManagedCertificate objects.")
    else:
        _net_status(rep, ctx, "tls", "OK", f"{len(rows)} certificate(s), all valid for at least 30 days or Active", "No action needed.")


# --- G. conntrack ------------------------------------------------------------------------------------------------

def _net_conntrack(rep, ctx):
    rep.sub("Connection tracking (conntrack) and port exhaustion: connection tracking table usage",
            "how full the Linux connection tracking table of each node is, read from node-exporter metrics through the Kubernetes API proxy; a full table drops new connections.",
            ["conntrack", "DaemonSet"])
    exporters = [p for p in items(ctx.data.get("pods")) if p.get("status", {}).get("phase") == "Running" and (
        "node-exporter" in p["metadata"]["name"] or (p["metadata"].get("labels") or {}).get("app.kubernetes.io/name") == "node-exporter"
        or (p["metadata"].get("labels") or {}).get("app") == "node-exporter")]
    if not exporters:
        _net_status(rep, ctx, "conntrack", "Not available", "needs node-exporter (or node access): no node-exporter pod exists, so the conntrack counters are not exposed",
                    "Deploy node-exporter (it exposes node_nf_conntrack_entries and node_nf_conntrack_entries_limit) or inspect `conntrack -S` on a node manually.")
        return
    rows, hot, unreadable = [], [], 0
    metric_calls = []
    for p in exporters[:12]:
        port = 9100
        for c in p.get("spec", {}).get("containers") or []:
            for cp in c.get("ports") or []:
                if cp.get("name") in ("metrics", "http-metrics") or cp.get("containerPort") == 9100:
                    port = cp.get("containerPort") or 9100
        ns, name = p["metadata"]["namespace"], p["metadata"]["name"]
        metric_calls.append(pf(_kt, ["get", "--raw", f"/api/v1/namespaces/{ns}/pods/{name}:{port}/proxy/metrics"], 45))
    for p, call in zip(exporters[:12], metric_calls):
        name = p["metadata"]["name"]
        ok, out = call.result()
        if not ok:
            unreadable += 1
            continue
        ent = re.search(r"^node_nf_conntrack_entries\s+([0-9.eE+]+)", out, re.M)
        lim = re.search(r"^node_nf_conntrack_entries_limit\s+([0-9.eE+]+)", out, re.M)
        if not (ent and lim):
            unreadable += 1
            continue
        e_, l_ = float(ent.group(1)), float(lim.group(1))
        pct = 100 * e_ / l_ if l_ else 0
        rows.append([node_tag(ctx, p.get("spec", {}).get("nodeName") or name), f"{e_:.0f}", f"{l_:.0f}", f"{pct:.1f}%"])
        if pct >= 80:
            hot.append((pct, p.get("spec", {}).get("nodeName") or name))
    rep.table(["NODE", "CONNECTIONS TRACKED", "TABLE LIMIT", "PERCENT USED"], rows, maxw=50,
              about="for each node-exporter pod: how many connections the node tracks now, the table limit and the percentage used (read through the API server proxy, no pod exec).")
    if not rows:
        _net_status(rep, ctx, "conntrack", "Not available", f"node-exporter pods exist but the conntrack metrics could not be read from {unreadable} pod(s)",
                    "The API proxy may be blocked or the exporter hides the conntrack collector. Check on a node manually (conntrack -S) if you suspect it.")
    elif hot:
        top = max(hot)
        _net_status(rep, ctx, "conntrack", "Problem" if top[0] >= 95 else "Warning", f"{len(hot)} node(s) above 80% of the conntrack limit, highest {top[0]:.0f}% on {top[1]}",
                    "When the table is full, new connections are dropped (symptoms: random timeouts, 'nf_conntrack: table full' in the node log). Raise nf_conntrack_max, reduce short-lived connections or enable NodeLocal DNSCache.",
                    "Conntrack usage")
    else:
        _net_status(rep, ctx, "conntrack", "OK", f"{len(rows)} node(s) read, highest use {max(float(r[3][:-1]) for r in rows):.1f}% of the limit", "No action needed.")


# --- H. observability --------------------------------------------------------------------------------------------

def _net_observability(rep, ctx, target):
    rep.sub("Observability and packet capture: network observability features",
            "which tools exist to see network traffic after the fact (Dataplane V2 observability with Hubble, flow logs, monitoring, Connectivity Tests, Packet Mirroring).",
            ["Hubble", "VPC Flow Logs", "Connectivity Tests", "Packet Mirroring", "Dataplane V2"])
    c = ctx.data.get("gcp_cluster") or {}
    if not c:
        _net_status(rep, ctx, "observ", "Not available", "the cluster object could not be read (Google Cloud section off or failed)", "Turn Google Cloud details on (gcloud auth login).")
        ctx.add_check("capture", "Not available", "cluster details unavailable")
        return
    mon = c.get("monitoringConfig") or {}
    adv = mon.get("advancedDatapathObservabilityConfig") or {}
    relay = bool(adv.get("enableRelay")) or bool(_workload(ctx, "hubble-relay")) or bool(_ks_pods(ctx, "hubble-relay"))
    dpv2 = _is_dataplane_v2(ctx)
    subnets = ctx.data.get("gcp_subnets") or []
    flow = None
    if subnets:
        flow = bool((subnets[0].get("logConfig") or {}).get("enable") or subnets[0].get("enableFlowLogs"))
    comps = ((mon.get("componentConfig") or {}).get("enableComponents")) or []
    gmp = bool((mon.get("managedPrometheusConfig") or {}).get("enabled"))
    ct, cterr = gcloud(["network-management", "connectivity-tests", "list"], target, 60) if (GCP_OPTS["enabled"] and target) else (None, "Google Cloud section off")
    pm, pmerr = gcloud(["compute", "packet-mirrorings", "list"], target, 60) if (GCP_OPTS["enabled"] and target) else (None, "Google Cloud section off")
    rows = [["Dataplane V2 observability (Hubble)", ("enabled" if (adv.get("enableMetrics") or relay) else "not enabled") if dpv2 else "not applicable (needs Dataplane V2)",
             "Flow metrics and the Hubble relay show which pod talked to which pod and what was dropped."],
            ["Virtual private cloud flow logs on the node subnet", ("enabled" if flow else "not enabled") if flow is not None else "unknown (subnet not readable)", "Samples of connections per subnet in Cloud Logging."],
            ["Cloud Monitoring and Managed Prometheus", ("on: " + ", ".join(comps[:4]) if comps else "default system metrics") + ("; managed Prometheus on" if gmp else "; managed Prometheus off"),
             "Cluster and load balancer metrics; needed for the traffic charts in this report."],
            ["Network Intelligence Center Connectivity Tests", (f"available ({len(ct)} test(s) exist)" if isinstance(ct, list) else f"not available: {_first_line(cterr or 'no data', 70)}"),
             "Checks routes and firewalls between two endpoints without sending traffic."],
            ["Packet Mirroring policies", (f"{len(pm)} polic(ies)" if isinstance(pm, list) else f"not available: {_first_line(pmerr or 'no data', 70)}"), "Copies packets of chosen machines to a collector."]]
    rep.table(["FEATURE", "STATE", "WHAT IT GIVES YOU"], rows, maxw=80, about="the tools available to investigate network traffic and whether each one is switched on for this cluster.")
    traffic_tools = bool(flow) or bool(adv.get("enableMetrics") or relay)
    unknown = flow is None and not dpv2
    capture_ready = bool(isinstance(pm, list) and pm) or relay
    ctx.add_check("capture", *(
        ("OK", "Packet Mirroring policy or Hubble relay present") if capture_ready else
        ("Not available", "no Packet Mirroring policy and no Hubble relay: capture needs node access or must be set up first")))
    if unknown:
        _net_status(rep, ctx, "observ", "Not available", "subnet and Dataplane V2 details are not readable, so flow logs and Hubble cannot be confirmed",
                    "Check the subnet's flow log setting in the Google Cloud console.")
    elif traffic_tools:
        _net_status(rep, ctx, "observ", "OK", "traffic-level observability is on: " + ", ".join(x for x, y in (("flow logs", flow), ("Hubble", adv.get("enableMetrics") or relay)) if y),
                    "Use these to see who talked to whom after an incident. Keep flow log sampling in mind: they are samples, not every packet.")
    else:
        _net_status(rep, ctx, "observ", "Warning", "neither VPC Flow Logs on the subnet nor Dataplane V2 observability (Hubble) is enabled",
                    "You cannot look back at which connections failed. Enable flow logs on the node subnet (small cost) or Dataplane V2 observability before the next incident.")


def _net_capture_guidance(rep, ctx):
    rep.sub("Observability and packet capture: how to capture packets (guidance only)",
            "the usual tools for a deeper look when the checks above are not enough; this tool NEVER runs any of them: they need node access or change something.",
            ["tcpdump", "Packet Mirroring", "Hubble", "Connectivity Tests"])
    rows = [["Capture on a node with toolbox and tcpdump", "on the node (node login needed)", "gcloud compute ssh NODE --zone ZONE --tunnel-through-iap   then run: toolbox tcpdump -i any -nn -w /media/root/tmp/cap.pcap host POD_IP"],
            ["Follow flows with Hubble", "Dataplane V2 clusters with observability on", "hubble observe --namespace NAMESPACE --pod POD --verdict DROPPED --last 100"],
            ["Packet Mirroring", "Google Cloud, copies packets to a collector", "gcloud compute packet-mirrorings list   (read-only); creating a policy is a change made by you in the console or with gcloud"],
            ["Ephemeral debug container", "inside the pod's network namespace", "kubectl debug -it POD --image=nicolaka/netshoot --target=CONTAINER   (changes the pod; run it yourself if allowed)"],
            ["Connectivity Tests", "Google Cloud analysis, no traffic sent", "gcloud network-management connectivity-tests list   (read-only); a new test is created by you in the console"]]
    rep.table(["METHOD", "WHERE IT RUNS OR WHAT IT NEEDS", "EXAMPLE COMMAND (NOT RUN BY THIS TOOL)"], rows, maxw=110,
              about="five ways to look at real packets or flows; the commands are examples for you to run yourself, replace the capitalised words with real names.")
    _net_status(rep, ctx, "capture_guidance", "Not available", "this tool does not capture packets: capture needs node login, a pod change or a Packet Mirroring policy",
                "Use the table above when the status blocks do not explain the failure; start with Hubble or flow logs if they are enabled, they need no node access.")


# --- I. control plane / API server -------------------------------------------------------------------------------

def _net_apiserver(rep, ctx):
    rep.sub("Control plane and API server: request throttling",
            "whether the API server has refused requests because callers sent too many (HTTP 429 and API Priority and Fairness rejections); throttled controllers react slowly to network changes.",
            ["API", "API server", "HTTP 429", "Priority and fairness"])
    ok, out = _kt(["get", "--raw", "/metrics"], timeout=90)
    if not ok:
        _net_status(rep, ctx, "apiserver", "Not available", f"the API server /metrics endpoint could not be read: {(out.splitlines() or ['no answer'])[0][:100]}",
                    "Needs permission for the non-resource URL /metrics (usually cluster-admin). Alternatively read the apiserver_* metrics in Cloud Monitoring / Managed Prometheus.")
        return
    rejected = 0.0
    by_level = Counter()
    for m in re.finditer(r"^apiserver_flowcontrol_rejected_requests_total\{([^}]*)\}\s+([0-9.eE+]+)", out, re.M):
        v = float(m.group(2))
        rejected += v
        lvl = re.search(r'priority_level="([^"]*)"', m.group(1))
        by_level[lvl.group(1) if lvl else "?"] += v
    total = r429 = 0.0
    for m in re.finditer(r"^apiserver_request_total\{([^}]*)\}\s+([0-9.eE+]+)", out, re.M):
        v = float(m.group(2))
        total += v
        if re.search(r'code="429"', m.group(1)):
            r429 += v
    rows = [["Requests rejected by API Priority and Fairness", f"{rejected:.0f}", ", ".join(f"{k} {v:.0f}" for k, v in by_level.most_common(3) if v) or "none"],
            ["Requests answered with HTTP 429", f"{r429:.0f}", f"{100 * r429 / total:.2f}% of {total:.0f} requests" if total else "-"]]
    rep.table(["COUNTER", "VALUE SINCE THE KUBERNETES API SERVER STARTED", "DETAIL"], rows, maxw=70,
              about="two cumulative counters from the API server: requests refused by its queueing system and requests answered with 429 Too Many Requests (not limited to the report window).")
    if not (total or rejected):
        _net_status(rep, ctx, "apiserver", "Not available", "the metrics were readable but contain no apiserver_request_total / flow control counters", "The metrics may be filtered; check Cloud Monitoring instead.")
    elif rejected >= 100 or (total and r429 / total >= 0.01):
        _net_status(rep, ctx, "apiserver", "Problem", f"{rejected:.0f} rejected requests and {r429:.0f} HTTP 429 answers since the API server started",
                    "The API server is under pressure: find the noisy client (audit logs), add backoff to controllers or reduce list/watch calls.", "API server throttling")
    elif rejected or r429:
        _net_status(rep, ctx, "apiserver", "Warning", f"{rejected:.0f} rejected requests and {r429:.0f} HTTP 429 answers since the API server started",
                    "A small number is common after control plane upgrades. Compare two readings over time before acting.")
    else:
        _net_status(rep, ctx, "apiserver", "OK", f"no throttled requests among {total:.0f} counted since the API server started", "No action needed.")


def _net_webhooks(rep, ctx):
    rep.sub("Control plane and API server: admission webhook configuration and failures",
            "the webhooks the API server calls before it stores objects, with their failure policy and timeout; a broken webhook with failurePolicy Fail blocks creating or changing objects.",
            ["API", "API server", "Admission webhook", "failurePolicy"])
    rows, bad, unknown = [], [], 0
    endpoints = {(e["metadata"]["namespace"], e["metadata"]["name"]): e for e in items(ctx.data.get("endpoints"))}
    got_any = False
    wh_calls = [pf(_kj, ["get", res]) for _kind, res in (("Validating", "validatingwebhookconfigurations"), ("Mutating", "mutatingwebhookconfigurations"))]
    for (kind, res), call in zip((("Validating", "validatingwebhookconfigurations"), ("Mutating", "mutatingwebhookconfigurations")), wh_calls):
        data, err = call.result()
        if data is None:
            unknown += 1
            continue
        got_any = True
        for cfg in items(data):
            for wh in cfg.get("webhooks") or []:
                cc = wh.get("clientConfig") or {}
                svc = cc.get("service") or {}
                target = f"{svc.get('namespace')}/{svc.get('name')}" if svc else (cc.get("url") or "-")
                ready = None
                if svc:
                    e = endpoints.get((svc.get("namespace"), svc.get("name")))
                    ready = sum(len(x.get("addresses") or []) for x in (e or {}).get("subsets") or []) if e is not None else 0
                policy = wh.get("failurePolicy") or "Fail"
                note = "ok"
                if policy == "Fail" and ready == 0:
                    note = "failurePolicy Fail and NO ready endpoint: requests matching this webhook are rejected"
                    bad.append(f"{cfg['metadata']['name']}/{wh.get('name')}")
                rows.append([kind, cfg["metadata"]["name"], wh.get("name"), policy, wh.get("timeoutSeconds", 10), target[:50], "-" if ready is None else ready, note])
    rep.table(["KIND", "CONFIGURATION", "WEBHOOK", "FAILURE POLICY", "TIMEOUT (SECONDS)", "SERVICE OR URL", "READY ENDPOINTS", "NOTE"], rows, maxw=50,
              about="every admission webhook with what happens when it does not answer (Fail rejects, Ignore lets the request through), its timeout and whether its Service has a ready pod.")
    hook_events = [e for e in items(ctx.data.get("events")) if (_event_time(e) or ctx.since) >= ctx.since and re.search(r"webhook", f"{e.get('reason', '')} {e.get('message') or e.get('note') or ''}", re.I)
                   and e.get("type") == "Warning"]
    if hook_events:
        rep.add(f"  Warning events that mention a webhook in the window: {len(hook_events)} (e.g. {(hook_events[0].get('message') or hook_events[0].get('note') or '')[:110]})")
    if not got_any:
        _net_status(rep, ctx, "webhooks", "Not available", "webhook configurations could not be read", "Needs permission to list validatingwebhookconfigurations and mutatingwebhookconfigurations.")
    elif bad or hook_events:
        _net_status(rep, ctx, "webhooks", "Problem" if bad else "Warning",
                    (f"{len(bad)} webhook(s) with failurePolicy Fail and no ready endpoint: " + ", ".join(bad[:3])) if bad else f"{len(hook_events)} warning event(s) mention a webhook",
                    "Fix or scale up the webhook service; as a stop-gap set failurePolicy to Ignore or delete the configuration (a change you must make deliberately). Slow webhooks (timeout up to 30 s) stall every matching request.",
                    "Admission webhooks")
    else:
        _net_status(rep, ctx, "webhooks", "OK", f"{len(rows)} webhook(s); every Fail-policy webhook has a ready endpoint and no webhook warning events in the window", "No action needed.")


def _net_etcd(rep, ctx):
    rep.sub("Control plane and API server: etcd health", "the health of etcd, the database behind the API server, when the cluster exposes it.", ["API", "etcd"])
    ok, out = _kt(["get", "--raw", "/readyz/etcd"], timeout=30)
    if ok and out.strip().lower().startswith("ok"):
        _net_status(rep, ctx, "etcd", "OK", "the API server reports etcd ready (/readyz/etcd: ok)", "No action needed.")
    elif ok:
        _net_status(rep, ctx, "etcd", "Warning", f"/readyz/etcd answered: {out.strip()[:80]}", "Check the control plane status in the Google Cloud console.", "etcd")
    else:
        _net_status(rep, ctx, "etcd", "Not available", "etcd is managed by Google on GKE and its health endpoint is not exposed to this user: " + (out.splitlines() or ["no answer"])[0][:70],
                    "Look at the control plane logs in Cloud Logging (component etcd / apiserver) and the cluster status in the Google Cloud console.")


# --- K. checklist and glossary ----------------------------------------------------------------------------------

CHECKLIST = [  # (check, [keys of the blocks it is built from], what to do when it is not OK)
    ("Pod reachability evidence", ["stuck", "noep", "events"], "Look at pods stuck creating, Services without endpoints and the network warning events; describe one failing pod."),
    ("Container Network Interface (CNI) logs", ["cni", "cni_logs", "init"], "Read the networking agent logs for the node of a failing pod; check for no-free-IP-address errors and restarts."),
    ("Node health", ["nodes", "throttle"], "Check NotReady / pressure nodes and the node VM state; GKE auto-repair replaces broken nodes."),
    ("kube-proxy", ["kproxy", "kproxy_logs"], "Make sure kube-proxy (or Dataplane V2) is ready on every node and its log has no sync errors."),
    ("Domain Name System (DNS)", ["dns", "dns_logs", "nodelocal"], "Check the DNS pods, upstream resolvers and consider NodeLocal DNSCache for slow lookups."),
    ("Network policies", ["policies", "engine"], "Check that an allow rule exists for the blocked flow and that an engine enforces policies."),
    ("Cloud firewalls", ["fw", "nodeport"], "Allow the Google health check ranges and the node port range where needed; check risky open rules."),
    ("Load balancer health checks", ["lbhealth", "ingress", "controllers", "backendcfg", "tls"], "Check backend health, readiness probe paths, backend timeouts and certificate expiry."),
    ("Packet capture availability", ["capture"], "Packet capture needs Hubble, Packet Mirroring or node access; set one up before the next incident."),
    ("Provider observability tools", ["observ", "mtu", "routing"], "Enable flow logs or Dataplane V2 observability and check routing, peering and MTU settings."),
]


def _net_checklist(rep, ctx):
    rep.sub("Traffic issue checklist",
            "the ten standard questions of a traffic investigation answered from everything collected above; the result is the worst status of the blocks behind it.",
            ["CNI", "DNS", "kube-proxy", "NetworkPolicy", "Load balancer", "Packet Mirroring"])
    checks = ctx.collected_checks()
    rows = []
    for name, keys, todo in CHECKLIST:
        got = [(k, st, ev) for k in keys for st, ev in checks.get(k, [])]
        if not got:
            rows.append([name, "Not available", "this part of the report did not run or produced no result", "Run again with Google Cloud details on and kubectl permissions."])
            continue
        result = _combine([st for _k, st, _e in got])
        worst = [f"{ev}" for _k, st, ev in got if st == result] or [got[0][2]]
        evidence = "; ".join(worst)[:240]
        if result == "OK":
            evidence = f"{len(got)} check(s) OK: " + "; ".join(ev for _k, _s, ev in got[:2])[:200]
        rows.append([name, result, evidence, "No action needed." if result == "OK" else todo])
    rep.table(["CHECK", "RESULT", "EVIDENCE FOUND", "WHAT TO DO NEXT"], rows, maxw=80,
              about="one row per standard check with OK, Warning, Problem or Not available (never OK when the data could not be read), the evidence behind it and the next step.")
    ctx.data["net_checklist"] = rows
    bad = [r for r in rows if r[1] in ("Problem", "Warning")]
    rep.add(f"  Summary: {sum(1 for r in rows if r[1] == 'OK')} OK, {sum(1 for r in rows if r[1] == 'Warning')} Warning, {sum(1 for r in rows if r[1] == 'Problem')} Problem, "
            f"{sum(1 for r in rows if r[1] == 'Not available')} Not available" + (": start with " + ", ".join(r[0] for r in bad[:3]) if bad else "."))


def _net_full_glossary(rep, ctx):
    terms = [t for t in GLOSSARY if t in rep.used_terms]
    rep.sub("Complete glossary of the terms used in this section", "every short term and technical word that appears in this section, spelled out and explained once, in the order of the glossary.")
    rep.glossary(terms, "Glossary: what these terms mean (complete list for this section)")


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
    rep.sub("Node networking: node network interface counters, errors and live traffic",
            "bytes and errors counted on every node's network interface since it booted, plus a short live sample, read from each node's kubelet; interface errors point at the node or the network under it.",
            ["Network interface", "kubelet", "Network interface"])
    if not stats:
        _net_status(rep, ctx, "ifaces", "Not available", "unavailable: the kubelet statistics need the 'nodes/proxy' permission (the traffic over the window comes from Cloud Monitoring below)",
                    "Grant nodes/proxy get permission to see node and pod interface counters. Packet drops are not reported by the kubelet; use flow logs or a node-level capture for drops.")
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

    rows, err_nodes = [], []
    for node, t in sorted(node_a.items()):
        if not t:
            continue
        r = rates_node.get(node)
        rows.append([node_tag(ctx, node), _fmt_bytes(t[0]), _fmt_bytes(t[1]), _fmt_rate(r[0]) if r else "-", _fmt_rate(r[1]) if r else "-", t[2], t[3]])
        if t[2] + t[3] > 0:
            err_nodes.append(node_tag(ctx, node))
            ctx.find("MED", f"Node {node_tag(ctx, node)} has {t[2] + t[3]} network errors since boot (received {t[2]}, transmitted {t[3]})")
    rep.table(["NODE", "BYTES RECEIVED (SINCE BOOT)", "BYTES TRANSMITTED (SINCE BOOT)", "RECEIVE RATE NOW", "TRANSMIT RATE NOW", "RECEIVE ERRORS", "TRANSMIT ERRORS"], rows, maxw=60,
              about="per node: total bytes received and transmitted since the node booted, the live rate over the short sample, and the error counters of the network interface.")
    if not rows:
        _net_status(rep, ctx, "ifaces", "Not available", "the kubelet answered but reported no network counters", "The statistics endpoint may be restricted; use Cloud Monitoring traffic below.")
    elif err_nodes:
        _net_status(rep, ctx, "ifaces", "Warning", f"{len(err_nodes)} node(s) report interface errors since boot: " + ", ".join(err_nodes[:3]),
                    "A few old errors are normal; growing numbers point at the node network card, MTU mismatch or an overloaded node. Drops are not reported by the kubelet.")
    else:
        _net_status(rep, ctx, "ifaces", "OK", f"{len(rows)} node(s) report zero interface errors since boot (drops are not reported by the kubelet)", "No action needed.")

    prow = []
    for (ns, name), (t, node) in pod_a.items():
        r = rates_pod.get((ns, name))
        prow.append(((r[0] + r[1]) if r else 0, t[0] + t[1], [f"{ns}/{name}", support_of(ctx, ns) or "-", node_tag(ctx, node), _fmt_bytes(t[0]), _fmt_bytes(t[1]),
                                                           _fmt_rate(r[0]) if r else "-", _fmt_rate(r[1]) if r else "-", t[2] + t[3]]))
    prow.sort(key=lambda x: (-x[0], -x[1]))
    rep.sub("Node networking: top pods and namespaces by network traffic",
            "which pods and namespaces moved the most bytes; totals count since the pod started, not the selected window, and the live rate comes from the short sample.")
    rep.table(["POD", "SUPPORT DISTRIBUTION LIST", "NODE", "BYTES RECEIVED (SINCE POD START)", "BYTES TRANSMITTED (SINCE POD START)", "RECEIVE RATE NOW", "TRANSMIT RATE NOW", "INTERFACE ERRORS"],
              [x[2] for x in prow[:15]], maxw=60, about="the 15 pods with the most traffic, busiest right now first: bytes received and transmitted since the pod started, the live rate and the interface errors.")
    ns_tot = defaultdict(lambda: [0, 0])
    for (ns, _name), (t, _node) in pod_a.items():
        ns_tot[ns][0] += t[0]
        ns_tot[ns][1] += t[1]
    rep.table(["NAMESPACE", "SUPPORT DISTRIBUTION LIST", "BYTES RECEIVED (SINCE PODS STARTED)", "BYTES TRANSMITTED (SINCE PODS STARTED)"],
              [[n, support_of(ctx, n) or "-", _fmt_bytes(v[0]), _fmt_bytes(v[1])] for n, v in sorted(ns_tot.items(), key=lambda kv: -(kv[1][0] + kv[1][1]))[:15]],
              about="the traffic of all pods of a namespace added up (top 15 namespaces).")


def _gcp_routes(ctx, target, ntarget):
    """The routes of the project (cached): (list_or_None, error_or_None)."""
    def read():
        data, err = gcloud(["compute", "routes", "list"], ntarget, 90)
        return (data, None) if isinstance(data, list) else (None, err or "no data")
    return ctx.cached("_gcp_routes", read)


# --- GCP: routing, load balancers, NAT, static IPs, and TRAFFIC in the selected window -----------------------

def _gcp_series(ctx, target, metric, flt="", aligner="ALIGN_SUM", reducer="REDUCE_SUM", group_by=None, period=60):
    """{group key: [(datetime, value)]} from the Cloud Monitoring API (timeSeries.list, read-only) for the selected
    window, or (None, error). The key is the group-by label values joined with '|' ('' when not grouped)."""
    params = [("filter", f'metric.type="{metric}"' + (f" AND {flt}" if flt else "")),
              ("interval.startTime", _iso(ctx.since)), ("interval.endTime", _iso(ctx.now)),
              ("aggregation.alignmentPeriod", f"{period}s"), ("aggregation.perSeriesAligner", aligner)]
    if reducer:
        params.append(("aggregation.crossSeriesReducer", reducer))
    for g in group_by or []:
        params.append(("aggregation.groupByFields", g))
    base = f"https://monitoring.googleapis.com/v3/projects/{target['project']}/timeSeries?" + urllib.parse.urlencode(params)
    series, token = {}, None
    for _page in range(6):
        data, err = gcp_api("GET", base + (("&pageToken=" + urllib.parse.quote(token)) if token else ""), None, 90)
        if err:
            return None, err
        for ts in (data or {}).get("timeSeries") or []:
            labels = {"metric": (ts.get("metric") or {}).get("labels") or {}, "resource": (ts.get("resource") or {}).get("labels") or {}}
            key = "|".join(str(labels[g.split(".", 1)[0]].get(g.rsplit(".", 1)[-1], "")) for g in group_by or [])
            pts = []
            for p in ts.get("points") or []:
                t = parse_ts((p.get("interval") or {}).get("endTime"))
                v = p.get("value") or {}
                val = v.get("doubleValue", v.get("int64Value", v.get("boolValue")))
                if isinstance(val, bool):
                    val = 1.0 if val else 0.0
                try:
                    val = float(val)
                except (TypeError, ValueError):
                    continue
                if t is not None:
                    pts.append((t, val))
            series.setdefault(key, []).extend(pts)
        token = (data or {}).get("nextPageToken")
        if not token:
            break
    return {k: sorted(v, key=lambda x: x[0]) for k, v in series.items()}, None


def _line(label, pts, fmt, per_second=None, total=True):
    """A series line for the HTML charts + its numbers. per_second: divide each value by this many seconds."""
    vals = [(v / per_second) if per_second else v for _, v in pts]
    return {"l": label, "f": fmt, "p": [round(v, 3) for v in vals], "ts": [int(t.timestamp()) for t, _ in pts],
            "avg": (sum(vals) / len(vals)) if vals else None, "max": max(vals) if vals else None,
            "sum": sum(v for _, v in pts) if total else None}


def _k8s_lb_hints(ctx):
    """({ip: 'Service ns/name'}, {forwarding rule name: 'Ingress ns/name'}) from what kubectl shows the cluster owns."""
    ips, names = {}, {}
    for kind, key in (("Service", "services"), ("Ingress", "ingresses")):
        for o in items(ctx.data.get(key)):
            meta = o["metadata"]
            who = f"{kind} {meta['namespace']}/{meta['name']}"
            for lb in (o.get("status", {}).get("loadBalancer") or {}).get("ingress") or []:
                if lb.get("ip"):
                    ips[lb["ip"]] = who
            for ann in ("ingress.kubernetes.io/forwarding-rule", "ingress.kubernetes.io/https-forwarding-rule"):
                if (meta.get("annotations") or {}).get(ann):
                    names[meta["annotations"][ann]] = who
    return ips, names


def _kube_uid8(ctx):
    for n in items(ctx.data.get("namespaces")):
        if n["metadata"]["name"] == "kube-system":
            return (n["metadata"].get("uid") or "")[:8]
    return ""


def _gcp_routes_and_edge(rep, ctx, target, cluster):
    net = ctx.data.get("gcp_net") or {}
    ntarget = {"project": net.get("project") or target["project"]}
    net_name, region = net.get("name"), net.get("region") or target.get("region")
    nats0 = ctx.data.get("gcp_nats") or []
    routers0 = sorted({(n["router"], n["region"]) for n in nats0})[:3]
    map_pre = [pf(gcloud, ["compute", "routers", "get-nat-mapping-info", r[0], "--region", r[1]], ntarget, 90) for r in routers0]   # NAT mapping reads start now too
    addr_f = pf(gcloud, ["compute", "addresses", "list"], target, 90)                 # routes (cached), addresses and forwarding rules are read together
    fr_f = pf(gcloud, ["compute", "forwarding-rules", "list"], target, 120)
    bs_f = pf(gcloud, ["compute", "backend-services", "list"], target, 120)
    pf(_gcp_routes, ctx, target, ntarget)
    rep.sub("Google Cloud routing: routes of the virtual private cloud network",
            "the custom routes (default route, VPN, peering) that decide where node and pod traffic leaves the network; the automatic subnet routes are hidden.", ["VPC", "VPC peering"])
    routes, err = _gcp_routes(ctx, target, ntarget)
    if err or not isinstance(routes, list):
        rep.add(f"  routes unavailable: {_first_line(err or 'no data', 90)}")
        _net_status(rep, ctx, "routes", "Not available", f"routes could not be read: {_first_line(err or 'no data', 80)}", "Needs compute.routes.list (roles/compute.viewer).")
    else:
        mine = [r for r in routes if not net_name or _url_name(r.get("network")) == net_name]
        rows = []
        has_default = False
        for r in sorted(mine, key=lambda r: r.get("priority", 1000)):
            hop = next((f"{k[7:]}: {_url_name(r[k]) if k != 'nextHopIp' else r[k]}" for k in
                        ("nextHopGateway", "nextHopIp", "nextHopInstance", "nextHopIlb", "nextHopVpnTunnel", "nextHopPeering", "nextHopNetwork") if r.get(k)), "-")
            if r.get("destRange") == "0.0.0.0/0":
                if r.get("nextHopGateway") or r.get("nextHopIp") or r.get("nextHopInstance") or r.get("nextHopIlb") or r.get("nextHopVpnTunnel"):
                    has_default = True
                if r.get("nextHopIp") or r.get("nextHopInstance") or r.get("nextHopIlb") or r.get("nextHopVpnTunnel"):
                    ctx.find("INFO", f"Route {r.get('name')}: the default route 0.0.0.0/0 goes to {hop} (a firewall / appliance / VPN) - make sure it allows what GKE needs (Google APIs, registries)")
            if r.get("nextHopNetwork") and not r.get("nextHopPeering"):
                continue                                  # plain subnet routes: not interesting
            rows.append([r.get("name"), r.get("destRange"), hop, r.get("priority"), ",".join(r.get("tags") or []) or "-"])
        if rows:
            rep.table(["ROUTE", "DESTINATION", "NEXT HOP", "PRIORITY", "TAGS"], rows, limit=25, maxw=50,
                      about=f"the custom routes of the virtual private cloud network {net_name}: where each destination range is sent (gateway, VPN tunnel, appliance, peering) and its priority; lower number wins.")
        if mine and not has_default:
            ctx.find("HIGH", f"VPC {net_name} has no default route (0.0.0.0/0): nodes can't reach the internet, registries or Google APIs except through Private Google Access")
            _net_status(rep, ctx, "routes", "Problem", f"network {net_name} has no default route (0.0.0.0/0)",
                        "Nodes cannot reach the internet or registries except through Private Google Access. Add a default route to the internet gateway or an appliance.")
        elif any(r.get("destRange") == "0.0.0.0/0" and (r.get("nextHopIp") or r.get("nextHopInstance") or r.get("nextHopIlb") or r.get("nextHopVpnTunnel")) for r in mine):
            _net_status(rep, ctx, "routes", "Warning", "the default route 0.0.0.0/0 goes to an appliance, VPN or internal load balancer instead of the internet gateway",
                        "Make sure that device allows what GKE needs (Google APIs, registries) and does not drop large packets.")
        else:
            _net_status(rep, ctx, "routes", "OK", f"{len(mine)} route(s) on {net_name}; a default route exists", "No action needed.")
    # static IPs
    addrs, err = addr_f.result()
    if not err and isinstance(addrs, list):
        arows, unused = [], 0
        for a in addrs:
            reg = _url_region(a.get("region")) if a.get("region") else "global"
            if reg not in ("global", region):
                continue
            used_by = ", ".join(_url_name(u) for u in a.get("users") or []) or "-"
            if a.get("status") == "RESERVED" and a.get("addressType", "EXTERNAL") == "EXTERNAL":
                unused += 1
            arows.append([a.get("name"), a.get("address"), a.get("addressType", "EXTERNAL"), a.get("status"), a.get("purpose") or "-", reg, used_by[:60]])
        rep.sub("Google Cloud routing: static IP addresses",
                f"the reserved IP addresses of the project in {region} and global scope; an unused reserved address costs money and may be the one an Ingress was meant to use.")
        rep.table(["NAME", "ADDRESS", "TYPE", "STATUS", "PURPOSE", "SCOPE", "USED BY"], arows, limit=25,
                  about="each reserved IP address with its type, whether it is in use (RESERVED = attached to nothing) and which forwarding rule uses it.")
        if unused:
            ctx.find("INFO", f"{unused} reserved external static IP(s) are not attached to anything (they cost money and may be what a Service/Ingress was supposed to use)")
            _net_status(rep, ctx, "staticips", "Warning", f"{unused} reserved external IP address(es) are not attached to anything",
                        "Release them if unused, or attach them to the Service / Ingress that should use them.")
        elif arows:
            _net_status(rep, ctx, "staticips", "OK", f"{len(arows)} reserved IP address(es), all in use", "No action needed.")
        else:
            _net_status(rep, ctx, "staticips", "OK", "no reserved IP addresses exist in this scope", "No action needed.")
    else:
        rep.sub("Google Cloud routing: static IP addresses", "the reserved IP addresses of the project.")
        _net_status(rep, ctx, "staticips", "Not available", f"addresses could not be read: {_first_line(err or 'no data', 80)}", "Needs compute.addresses.list.")
    # forwarding rules + backend services
    lbs = []
    ips, names = _k8s_lb_hints(ctx)
    frs, err = fr_f.result()
    matched_names = set()
    rep.sub("Load balancers and ingress: load balancer forwarding rules of this cluster",
            "the Google Cloud forwarding rules (the load balancer front ends) that belong to this cluster's Services and Ingresses, matched by IP address, annotation or description.",
            ["Load balancer", "Ingress"])
    if err or not isinstance(frs, list):
        rep.add(f"  forwarding rules / load balancers unavailable: {_first_line(err or 'no data', 90)}")
        _net_status(rep, ctx, "forwarding", "Not available", f"forwarding rules could not be read: {_first_line(err or 'no data', 80)}", "Needs compute.forwardingRules.list.")
        _net_status(rep, ctx, "lbhealth", "Not available", "the load balancers of this cluster could not be listed, so backend health is unknown", "Needs compute.forwardingRules.list and compute.backendServices.list.")
    else:
        rows = []
        for r in frs:
            desc = r.get("description") or ""
            m = re.search(r'kubernetes\.io/(?:service|ingress)-name"\s*:\s*"([^"]+)"', desc)
            who = ips.get(r.get("IPAddress")) or names.get(r.get("name")) or (m.group(1) if m else None)
            if not who:
                continue
            tgt = r.get("target") or ""
            scheme = r.get("loadBalancingScheme") or "-"
            http = "targetHttp" in tgt or "TargetHttp" in tgt
            kind = ("https_internal" if "INTERNAL" in scheme else "https") if http else ("l3_internal" if "INTERNAL" in scheme else "l3_external")
            reg = _url_region(r.get("region")) if r.get("region") else "global"
            lbs.append({"name": r.get("name"), "scheme": scheme, "kind": kind, "ip": r.get("IPAddress"), "region": reg, "who": who})
            if r.get("backendService"):
                matched_names.add(_url_name(r["backendService"]))
            rows.append([r.get("name"), scheme, r.get("IPAddress"), f"{r.get('IPProtocol', '-')}:{r.get('portRange') or ','.join(r.get('ports') or []) or '-'}",
                         _url_name(tgt or r.get("backendService")) or "-", who, reg])
        rep.add(f"  Load balancer forwarding rules belonging to this cluster's Services / Ingresses ({len(rows)} of {len(frs)} in the project):")
        if rows:
            rep.table(["FORWARDING RULE", "SCHEME", "IP", "PROTOCOL:PORTS", "TARGET / BACKEND", "KUBERNETES OBJECT", "SCOPE"], rows, maxw=48,
                      about="the load balancer front ends of this cluster: scheme (external / internal), IP address, protocol and ports, what they forward to and the Service or Ingress that owns them.")
            _net_status(rep, ctx, "forwarding", "OK", f"{len(rows)} forwarding rule(s) belong to this cluster", "No action needed.")
        else:
            rep.add("  (none matched: no Service / Ingress has a load balancer IP, or the forwarding rules are in another project)")
            _net_status(rep, ctx, "forwarding", "OK", "no forwarding rule matches a Service / Ingress of this cluster (none uses a Google Cloud load balancer, or they are in another project)", "No action needed unless you expect a load balancer.")
        _gcp_backend_health(rep, ctx, target, cluster, matched_names, bs_f)
    _gcp_nat_mapping(rep, ctx, target, ntarget, map_pre)
    return lbs, [dict(n, id=n["name"]) for n in (ctx.data.get("gcp_nats") or [])]


def _gcp_backend_health(rep, ctx, target, cluster, wanted_names, pre=None):
    rep.sub("Load balancers and ingress: load balancer backend health (Google Cloud)",
            "for every backend service of this cluster, how many backend endpoints Google Cloud's health checks currently call healthy; unhealthy backends receive no traffic.",
            ["Load balancer", "NEG", "Health check ranges", "Ingress"])
    bss, err = pre.result() if pre is not None else gcloud(["compute", "backend-services", "list"], target, 120)
    if err or not isinstance(bss, list):
        rep.add(f"  backend services unavailable: {_first_line(err or 'no data', 90)}")
        _net_status(rep, ctx, "lbhealth", "Not available", f"backend services could not be read: {_first_line(err or 'no data', 80)}", "Needs compute.backendServices.list and get-health permission.")
        return
    migs, _e = _gcp_migs(ctx, target)
    mig_names = {_url_name(u) for p in cluster.get("nodePools") or [] for u in p.get("instanceGroupUrls") or []}
    uid8 = _kube_uid8(ctx)
    mine = []
    for b in bss:
        groups = [_url_name(x.get("group")) for x in b.get("backends") or []]
        if b.get("name") in wanted_names or any(g in mig_names for g in groups) or (uid8 and uid8 in (b.get("name") or "")) \
                or any(uid8 and g.startswith("k8s1-" + uid8) for g in groups):
            mine.append(b)
    if not mine:
        rep.add("  no backend services of this cluster found (no GKE Ingress / NEG / backend-service based load balancers).")
        _net_status(rep, ctx, "lbhealth", "OK", "no backend service belongs to this cluster (no Ingress, NEG or backend-service based load balancer)", "No action needed.")
        return
    rows, lb_states = [], []

    def get_health(b):
        scope = ["--region", _url_region(b.get("region"))] if b.get("region") else ["--global"]
        return gcloud(["compute", "backend-services", "get-health", b["name"]] + scope, target, 90)
    health_calls = [pf(get_health, b) for b in mine[:8]]          # one health read per backend service, all at once
    for b, call in zip(mine[:8], health_calls):
        health, err = call.result()
        healthy = unhealthy = 0
        if not err and isinstance(health, list):
            for h in health:
                for hs in ((h.get("status") or {}).get("healthStatus")) or []:
                    if (hs.get("healthState") or "").upper() == "HEALTHY":
                        healthy += 1
                    else:
                        unhealthy += 1
        m = re.search(r'kubernetes\.io/[a-z]+-name"\s*:\s*"([^"]+)"', b.get("description") or "")
        note = "ok"
        lb_states.append("Not available" if err else ("Problem" if unhealthy and not healthy else ("Warning" if unhealthy else "OK")))
        if err:
            note = f"health unavailable: {_first_line(err, 60)}"
        elif unhealthy and not healthy:
            note = "ALL BACKENDS UNHEALTHY"
            ctx.find("HIGH", f"Load balancer backend service {b['name']}: all {unhealthy} backend endpoint(s) are UNHEALTHY"
                             + (f" ({m.group(1)})" if m else "") + " - check pod readiness and that the firewall allows the Google health check ranges 130.211.0.0/22 and 35.191.0.0/16")
        elif unhealthy:
            note = f"{unhealthy} unhealthy"
            ctx.find("MED", f"Load balancer backend service {b['name']}: {unhealthy} of {healthy + unhealthy} backend endpoint(s) UNHEALTHY" + (f" ({m.group(1)})" if m else ""))
        rows.append([b["name"], b.get("loadBalancingScheme"), b.get("protocol"), len(b.get("backends") or []), healthy, unhealthy, m.group(1) if m else "-", note])
    rep.add(f"  Backend services of this cluster ({len(mine)}), with `gcloud compute backend-services get-health`:")
    rep.table(["BACKEND SERVICE", "SCHEME", "PROTOCOL", "BACKENDS", "HEALTHY", "UNHEALTHY", "KUBERNETES OBJECT", "NOTE"], rows, maxw=50,
              about="each backend service of this cluster with the number of backend groups and the endpoints Google Cloud's health checks call healthy or unhealthy.")
    st = _combine(lb_states)
    if st == "Problem":
        _net_status(rep, ctx, "lbhealth", "Problem", f"{lb_states.count('Problem')} backend service(s) have ALL endpoints unhealthy",
                    "No traffic reaches those pods. Check pod readiness, that the health check path returns 200, and that the firewall allows 130.211.0.0/22 and 35.191.0.0/16.")
    elif st == "Warning":
        _net_status(rep, ctx, "lbhealth", "Warning", f"{lb_states.count('Warning')} backend service(s) have some unhealthy endpoints",
                    "Some pods fail the health check: look at their readiness and the health check path and port in the BackendConfig.")
    elif st == "Not available":
        _net_status(rep, ctx, "lbhealth", "Not available", "backend health could not be read for some backend services", "Needs compute.backendServices.getHealth.")
    else:
        _net_status(rep, ctx, "lbhealth", "OK", f"all endpoints of {len(mine)} backend service(s) are healthy", "No action needed.")


def _gcp_nat_mapping(rep, ctx, target, ntarget, pre=None):
    """Which NAT IP and how many NAT ports each node got (`gcloud compute routers get-nat-mapping-info`)."""
    nats = ctx.data.get("gcp_nats") or []
    if not nats:
        return
    idents = node_idents(ctx)
    by_vm = {i["vm_name"].lower(): n for n, i in idents.items() if i["vm_name"] != "-"}
    rows, ports, egress = [], {}, set()
    routers = sorted({(n["router"], n["region"]) for n in nats})[:3]
    map_calls = pre if pre is not None and len(pre) == len(routers) else [pf(gcloud, ["compute", "routers", "get-nat-mapping-info", r[0], "--region", r[1]], ntarget, 90) for r in routers]
    for router, call in zip(routers, map_calls):
        data, err = call.result()
        if err or not isinstance(data, list):
            rep.add(f"  NAT mapping for router {router[0]} unavailable: {_first_line(err or 'no data', 90)}")
            continue
        for v in data:
            node = by_vm.get((v.get("instanceName") or "").lower())
            if not node:
                continue
            total, ranges = 0, []
            for m in v.get("interfaceNatMappings") or []:
                total += int(m.get("numTotalNatPorts") or 0)
                ranges += m.get("natIpPortRanges") or []
            for r in ranges:
                egress.add(str(r).split(":")[0])
            ports[v.get("instanceName")] = total
            rows.append([node_tag(ctx, node), router[0], ", ".join(ranges[:2]) + (" ..." if len(ranges) > 2 else ""), total])
    ctx.data["gcp_nat_ports"] = ports
    if rows:
        rep.sub("Connection tracking (conntrack) and port exhaustion: Cloud NAT (network address translation) addresses and ports given to each node",
                "which public address and port range every node uses when it connects to the internet through Cloud NAT; each connection to the same destination uses one port.", ["NAT"])
        rep.add(f"  Cloud NAT egress IPs used by the nodes: {', '.join(sorted(egress)) or '-'}  (allow-list these at external services)")
        rep.table(["NODE", "ROUTER", "PUBLIC ADDRESS AND PORT RANGES", "PORTS ALLOCATED"], rows, maxw=60,
                  about="for each node: the Cloud NAT router, the public address and port ranges it was given and how many NAT ports that is.")


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _one_of(values):
    return "one_of(" + ",".join(f'"{v}"' for v in values) + ")"


def _traffic_prefetch(ctx, target, nats):
    """Start the Cloud Monitoring queries that do not depend on the load balancers (node bytes, Cloud NAT): (node_calls, nat_calls)."""
    idents = node_idents(ctx)
    by_vm = {i["vm_name"].lower(): n for n, i in idents.items() if i["vm_name"] != "-"}
    node_calls = []
    for chunk in _chunks(sorted(by_vm), 40):
        flt = 'resource.type="gce_instance" AND metric.label.instance_name=' + _one_of(chunk)
        node_calls.append((pf(_gcp_series, ctx, target, "compute.googleapis.com/instance/network/received_bytes_count", flt, "ALIGN_SUM", "REDUCE_SUM", ["metric.label.instance_name"]),
                           pf(_gcp_series, ctx, target, "compute.googleapis.com/instance/network/sent_bytes_count", flt, "ALIGN_SUM", "REDUCE_SUM", ["metric.label.instance_name"])))
    nat_calls = None
    any_vm_id = any(i.get("id") for i in ((ctx.data.get("vm_info") or {}).get((idents[k]["vm_name"] or "").lower(), {}) for k in idents))
    if nats:
        gw0 = _one_of(sorted({n["name"] for n in nats}))
        nat_calls = (pf(_gcp_series, ctx, target, "router.googleapis.com/nat/dropped_sent_packets_count", f"resource.label.gateway_name={gw0}", "ALIGN_SUM", "REDUCE_SUM",
                        ["resource.label.gateway_name", "metric.label.reason"]),
                     pf(_gcp_series, ctx, target, "router.googleapis.com/nat/nat_allocation_failed", f"resource.label.gateway_name={gw0}", "ALIGN_MAX", "REDUCE_MAX",
                        ["resource.label.gateway_name"]),
                     pf(_gcp_series, ctx, target, "router.googleapis.com/nat/allocated_ports", "", "ALIGN_MAX", "REDUCE_MAX", ["resource.label.instance_id"]) if any_vm_id else None,
                     pf(_gcp_series, ctx, target, "router.googleapis.com/nat/port_usage", "", "ALIGN_MAX", "REDUCE_MAX", ["resource.label.instance_id"]) if any_vm_id else None)
    return node_calls, nat_calls


def _gcp_traffic(rep, ctx, target, lbs, nats, early=None):
    mins = ctx.minutes
    rep.sub(f"Traffic over the selected window (last {mins} min, from Cloud Monitoring metrics)",
            "how many bytes each node received and sent, and how the load balancers performed (requests, server errors, latency) minute by minute over the window.", ["5xx", "p95 latency", "Load balancer"])
    idents = node_idents(ctx)
    by_vm = {i["vm_name"].lower(): n for n, i in idents.items() if i["vm_name"] != "-"}
    series_rows, rows, totals = [], [], {"in": defaultdict(float), "out": defaultdict(float)}
    first_err = None
    pin, pout = {}, {}
    # every Cloud Monitoring query of this block runs together (they are independent); the results are read below in the old order
    node_calls, nat_calls = early if early is not None else _traffic_prefetch(ctx, target, nats)
    lb_calls = {}
    for lb in lbs[:8]:
        rule = _one_of([lb["name"]])
        flt_r = f"resource.label.forwarding_rule_name={rule}"
        if lb["kind"].startswith("https"):
            pre = "loadbalancing.googleapis.com/https/" + ("internal/" if lb["kind"] == "https_internal" else "")
            lb_calls[lb["name"]] = [
                pf(_gcp_series, ctx, target, pre + "request_count", flt_r, "ALIGN_SUM", "REDUCE_SUM", ["resource.label.forwarding_rule_name", "metric.label.response_code_class"]),
                pf(_gcp_series, ctx, target, pre + ("backend_latencies" if lb["kind"] == "https" else "total_latencies"), flt_r, "ALIGN_PERCENTILE_95", "REDUCE_MAX", ["resource.label.forwarding_rule_name"])]
        else:
            pre = "loadbalancing.googleapis.com/l3/" + ("internal/" if lb["kind"] == "l3_internal" else "external/")
            lb_calls[lb["name"]] = [pf(_gcp_series, ctx, target, pre + metric, flt_r, "ALIGN_SUM", "REDUCE_SUM", ["resource.label.forwarding_rule_name"])
                                    for metric in ("ingress_bytes_count", "egress_bytes_count")]
    for call_a, call_b in node_calls:
        a, err1 = call_a.result()
        b, err2 = call_b.result()
        first_err = first_err or err1 or err2
        pin.update(a or {})
        pout.update(b or {})
    for vm_name in sorted(set(pin) | set(pout)):
        node = by_vm.get(vm_name.lower())
        if not node:
            continue
        lin, lout = _line("in", pin.get(vm_name, []), "Bps", 60), _line("out", pout.get(vm_name, []), "Bps", 60)
        for t, v in pin.get(vm_name, []):
            totals["in"][t] += v / 60
        for t, v in pout.get(vm_name, []):
            totals["out"][t] += v / 60
        series_rows.append({"n": node, "s": f"{idents[node]['instance_id']} | {idents[node]['zone']} | {idents[node]['type']}", "lines": [lin, lout]})
        rows.append([node_tag(ctx, node), _fmt_rate(lin["avg"]), _fmt_rate(lin["max"]), _fmt_bytes(lin["sum"]),
                     _fmt_rate(lout["avg"]), _fmt_rate(lout["max"]), _fmt_bytes(lout["sum"])])
    if not rows:
        rep.add("  Node traffic: no Cloud Monitoring data returned" + (f" ({_first_line(first_err, 100)})" if first_err else "")
                + ". Needs roles/monitoring.viewer on the project.")
    else:
        keys = sorted(set(totals["in"]) | set(totals["out"]))
        allin = _line("in", [(k, totals["in"].get(k, 0.0)) for k in keys], "Bps", None, total=False)
        allout = _line("out", [(k, totals["out"].get(k, 0.0)) for k in keys], "Bps", None, total=False)
        series_rows.insert(0, {"n": "ALL NODES (sum)", "s": f"{len(rows)} nodes", "lines": [allin, allout]})
        rep.add(f"  All nodes together: in avg {_fmt_rate(allin['avg'])} peak {_fmt_rate(allin['max'])}; out avg {_fmt_rate(allout['avg'])} peak {_fmt_rate(allout['max'])}")
        rep.add("  Per node over the window (Compute Engine 'network/received_bytes_count' / 'sent_bytes_count', 1-minute points; avg/peak are rates, TOTAL is bytes in the window):")
        rep.table(["NODE", "BYTES RECEIVED PER SECOND (AVERAGE)", "BYTES RECEIVED PER SECOND (PEAK)", "BYTES RECEIVED (TOTAL IN WINDOW)",
                   "BYTES SENT PER SECOND (AVERAGE)", "BYTES SENT PER SECOND (PEAK)", "BYTES SENT (TOTAL IN WINDOW)"], rows, maxw=60,
                  about="per node VM over the window: average and peak rate and total bytes received and sent (Compute Engine network metrics, 1-minute points).")
        busiest = max(series_rows[1:], key=lambda r: (r["lines"][1]["max"] or 0) + (r["lines"][0]["max"] or 0))
        ctx.find("INFO", f"Busiest node on the network in the window: {busiest['n']} (peak in {_fmt_rate(busiest['lines'][0]['max'])}, out {_fmt_rate(busiest['lines'][1]['max'])})")
    rep.series(f"Node network traffic, last {mins} min (bytes per second)", series_rows,
               "Compute Engine metrics 'received_bytes_count' / 'sent_bytes_count' per node VM (1-minute points).",
               about="one small chart per node virtual machine: the bytes received and sent every second over the selected window, with the average, the peak and the total; hover a point to see its time and value.")

    # ---- load balancers
    lb_series, lb_rows, errs = [], [], []
    for lb in lbs[:8]:
        got = {}
        if lb["kind"].startswith("https"):
            req, e1 = lb_calls[lb["name"]][0].result()
            lat, e2 = lb_calls[lb["name"]][1].result()
            errs += [x for x in (e1, e2) if x]
            for key, pts in (req or {}).items():
                got["req_" + (key.split("|")[1] if "|" in key else "all")] = pts
            if lat:
                got["lat"] = next(iter(lat.values()))
        else:
            for (key, metric), call in zip((("in", "ingress_bytes_count"), ("out", "egress_bytes_count")), lb_calls[lb["name"]]):
                s, e = call.result()
                if e:
                    errs.append(e)
                elif s:
                    got[key] = next(iter(s.values()))
        if not got:
            lb_rows.append([lb["name"], lb["scheme"], "no Cloud Monitoring data" + (f" ({_first_line(errs[-1], 60)})" if errs else ""), "", "", "", ""])
            continue
        tot = lambda k: sum(v for _, v in got.get(k, []))
        reqs = sum(tot(k) for k in got if k.startswith("req_"))
        e5 = tot("req_5xx")
        pct = 100 * e5 / reqs if reqs else 0
        lat_max = max((v for _, v in got.get("lat", [])), default=None)
        lb_rows.append([lb["name"], lb["scheme"], f"{reqs:.0f}" if reqs else "-", f"{e5:.0f}", f"{pct:.1f}%" if reqs else "-",
                        f"{lat_max:.0f} ms" if lat_max is not None else "-",
                        (_fmt_bytes(tot("in")) + " / " + _fmt_bytes(tot("out"))) if "in" in got or "out" in got else "-"])
        if reqs and pct >= 1:
            ctx.find("HIGH" if pct >= 5 else "MED", f"Load balancer {lb['name']} ({lb['who']}): {pct:.1f}% of {reqs:.0f} requests were 5xx in the window")
        lines = []
        for cls in ("2xx", "3xx", "4xx", "5xx"):
            if got.get("req_" + cls):
                lines.append(_line(f"{cls} requests/min", got["req_" + cls], "count"))
        if got.get("lat"):
            lines.append(_line("p95 latency ms", got["lat"], "count", None, total=False))
        for k, label in (("in", "ingress bytes/min"), ("out", "egress bytes/min")):
            if got.get(k):
                lines.append(_line(label, got[k], "B"))
        lb_series.append({"n": lb["name"], "s": f"{lb['scheme']} | {lb['who']}", "lines": lines})
    if lb_rows:
        rep.add("")
        rep.add("  Load balancer traffic in the window (HTTP(S) load balancers: requests by response class and 95th percentile latency; network load balancers: bytes):")
        rep.table(["FORWARDING RULE", "SCHEME", "REQUESTS", "SERVER ERROR RESPONSES (HTTP 5xx)", "SERVER ERROR SHARE", "95TH PERCENTILE LATENCY (WORST MINUTE)", "BYTES RECEIVED / SENT"], lb_rows, maxw=48,
                  about="per load balancer front end over the window: number of requests, how many (and what share) were server errors, the slowest minute's 95th percentile latency and the bytes.")
        rep.series(f"Load balancer traffic, last {mins} min", lb_series, "Per-minute values from Cloud Monitoring (loadbalancing.googleapis.com).",
                   about="one small chart per load balancer forwarding rule: requests, client and server error responses and bytes over the selected window, from Cloud Monitoring; hover a point to see its time and value.")

    # ---- Cloud NAT
    rep.sub("Connection tracking (conntrack) and port exhaustion: Cloud NAT (network address translation) port exhaustion and dropped packets",
            f"whether the Cloud NAT gateways ran out of ports or addresses in the last {mins} min (dropped packets, allocation failures) and how close each node is to its allocated ports.",
            ["NAT", "OUT_OF_RESOURCES"])
    nat_states, nat_notes = [], []
    if not nats:
        _net_status(rep, ctx, "nat", "Not available", "no Cloud NAT gateway was found on this network and region (or the routers could not be read), so there is no NAT port usage to check",
                    "If private nodes need internet access, see the private cluster routing block; otherwise nodes with external IP addresses do not use NAT.")
        return
    drop, e1 = nat_calls[0].result()
    fail, e2 = nat_calls[1].result()
    rep.add("")
    if drop is None and fail is None:
        rep.add(f"  Cloud NAT metrics unavailable: {_first_line(e1 or e2 or 'no data', 100)}")
        nat_states.append("Not available")
        nat_notes.append("NAT metrics unreadable: " + _first_line(e1 or e2 or "no data", 60))
    else:
        nat_rows, nat_series = [], []
        for nat in nats[:6]:
            d_by = {k.split("|")[1] if "|" in k else "?": pts for k, pts in (drop or {}).items() if k.split("|")[0] == nat["name"]}
            all_drop = sum(v for pts in d_by.values() for _, v in pts)
            oor = sum(v for _, v in d_by.get("OUT_OF_RESOURCES", []))
            f_pts = (fail or {}).get(nat["name"], [])
            failed = any(v >= 1 for _, v in f_pts)
            nat_rows.append([nat["name"], nat["router"], f"{all_drop:.0f}", f"{oor:.0f}", "YES" if failed else "no"])
            nat_states.append("Problem" if (failed or oor > 0) else ("Warning" if all_drop > 0 else "OK"))
            if failed or oor > 0 or all_drop > 0:
                nat_notes.append(f"{nat['name']}: " + ("allocation failed; " if failed else "") + f"{all_drop:.0f} dropped" + (f" ({oor:.0f} out of ports)" if oor else ""))
            if failed:
                ctx.find("HIGH", f"Cloud NAT {nat['name']} (router {nat['router']}) reported NAT allocation FAILED in the window - it ran out of NAT IPs / ports; add NAT IPs or raise min ports per VM")
            if oor > 0:
                ctx.find("HIGH", f"Cloud NAT {nat['name']}: {oor:.0f} packets dropped OUT_OF_RESOURCES (NAT port exhaustion) in the window")
            elif all_drop > 0:
                ctx.find("MED", f"Cloud NAT {nat['name']}: {all_drop:.0f} dropped packets in the window")
            lines = []
            merged = sorted((p for pts in d_by.values() for p in pts), key=lambda x: x[0])
            if merged:
                lines.append(_line("dropped packets/min", merged, "count"))
            if f_pts:
                lines.append(_line("allocation failed (1 = yes)", f_pts, "count", None, total=False))
            nat_series.append({"n": nat["name"], "s": f"Cloud NAT | router {nat['router']}", "lines": lines})
        rep.add("  Cloud NAT in the window (dropped packets; ALLOCATION FAILED = the gateway could not allocate NAT IP addresses or ports):")
        rep.table(["CLOUD NAT GATEWAY", "ROUTER", "DROPPED PACKETS", "DROPPED: OUT OF RESOURCES (NO FREE PORTS)", "ALLOCATION FAILED"], nat_rows,
                  about="per Cloud NAT gateway: packets dropped in the window, how many of them because no NAT port or address was left, and whether the gateway reported an allocation failure.")
        rep.series(f"Cloud NAT (network address translation), last {mins} min", nat_series, "router.googleapis.com/nat/dropped_sent_packets_count and nat_allocation_failed.",
                   about="one small chart per Cloud NAT gateway: packets dropped and address allocation failures over the selected window, from Cloud Monitoring; any value above zero means connections were refused.")
    # per node NAT port use (approximate: peak port usage vs ports allocated)
    ids = {i["id"]: n for n, i in ((k, (ctx.data.get("vm_info") or {}).get((idents[k]["vm_name"] or "").lower(), {})) for k in idents) if i.get("id")}
    if ids:
        alloc, e3 = nat_calls[2].result()
        used, e4 = nat_calls[3].result()
        prow = []
        for iid, node in ids.items():
            a = max((v for _, v in (alloc or {}).get(iid, [])), default=None)
            u = max((v for _, v in (used or {}).get(iid, [])), default=None)
            if a and u is not None:
                pct = 100 * u / a
                prow.append([node_tag(ctx, node), f"{a:.0f}", f"{u:.0f}", f"{pct:.0f}%"])
                if pct >= 80:
                    nat_states.append("Problem" if pct >= 95 else "Warning")
                    nat_notes.append(f"{node_tag(ctx, node)} used {pct:.0f}% of its NAT ports")
                    ctx.find("HIGH" if pct >= 95 else "MED", f"Node {node_tag(ctx, node)}: Cloud NAT port usage peaked at {pct:.0f}% of its allocated ports - risk of dropped outbound connections")
        if prow:
            rep.add("")
            rep.add("  Cloud NAT ports per node VM (peak usage vs allocated, approximate):")
            rep.table(["NODE", "PORTS ALLOCATED", "PEAK PORTS IN USE", "PERCENT USED"], sorted(prow, key=lambda r: -float(r[3][:-1]))[:15],
                      about="per node VM: the NAT ports it was allocated, the most it used at the same time in the window and the percentage; near 100% new outbound connections fail.")
    st = _combine(nat_states)
    if st == "OK":
        _net_status(rep, ctx, "nat", "OK", f"no dropped packets, no allocation failure and no node above 80% of its NAT ports in the last {mins} min", "No action needed.")
    elif st == "Not available":
        _net_status(rep, ctx, "nat", "Not available", "; ".join(nat_notes) or "no NAT data", "Needs roles/monitoring.viewer on the project.")
    else:
        _net_status(rep, ctx, "nat", st, "; ".join(nat_notes)[:230],
                    "Outbound connections are failing or close to failing. Add NAT IP addresses, raise the minimum ports per VM or turn on dynamic port allocation, and reduce short-lived outbound connections.", "Cloud NAT port exhaustion")


def _net_gcp(rep, ctx, target, cluster):
    lbs, nats = [], []
    early = None
    try:
        early = _traffic_prefetch(ctx, target, [dict(n, id=n["name"]) for n in (ctx.data.get("gcp_nats") or [])])      # these do not need the load balancers
    except Exception:
        early = None
    try:
        lbs, nats = _gcp_routes_and_edge(rep, ctx, target, cluster)
    except Exception as exc:
        rep.add(f"[!] routing / load balancer step failed: {exc}")
        _net_status(rep, ctx, "lbhealth", "Not available", f"the routing / load balancer step failed: {str(exc)[:80]}", "Run again; if it repeats, check gcloud access.")
    if ctx.cancel is not None and ctx.cancel.is_set():
        return
    try:
        _gcp_traffic(rep, ctx, target, lbs, nats, early)
    except Exception as exc:
        rep.add(f"[!] traffic step failed: {exc}")


def section_network_details(rep, ctx, label):
    rep.section(f"10. NETWORK AND TRAFFIC - DATAPLANE, DOMAIN NAME SYSTEM, SERVICES, INGRESS, ROUTING, TRAFFIC (last {ctx.minutes} min)")
    ctx.data["net_checks"] = {}
    target, cluster = ctx.data.get("gcp_target"), ctx.data.get("gcp_cluster")
    gcp_ok = bool(GCP_OPTS["enabled"] and target and cluster)

    def edge(rep, ctx):
        rep.add("")
        if gcp_ok:
            _net_gcp(rep, ctx, target, cluster)
        else:
            rep.sub("Google Cloud routing, load balancers and traffic over the window",
                    "routes, static IP addresses, load balancer forwarding rules and backend health, Cloud NAT and the traffic of the selected window from Cloud Monitoring.")
            rep.add("GOOGLE CLOUD NETWORK AND TRAFFIC: skipped - the Google Cloud section is off or could not read the cluster. "
                    "(Routing, load balancers, Cloud NAT and the traffic over the window need Google Cloud access: run `gcloud auth login`.)")
            _net_status(rep, ctx, "lbhealth", "Not available", "load balancer backend health needs Google Cloud access, which is off or failed",
                        "Run `gcloud auth login`, turn Google Cloud details on and run again.")
            _net_status(rep, ctx, "nat", "Not available", "Cloud NAT metrics need Google Cloud access, which is off or failed", "Run `gcloud auth login` and run again.")

    mtu_step = lambda r, c: _net_mtu(r, c, target)
    route_step = lambda r, c: _net_private_routing(r, c, target)
    mtu_step.__name__, route_step.__name__ = "_net_mtu", "_net_private_routing"
    traffic_step, cni_logs_step = _net_pod_traffic, _net_cni_logs
    steps = [
        _net_intro, _net_cluster_settings,
        _net_cni_health, _net_ip_exhaustion, _net_cni_logs, _net_stuck_pods, _net_events, _net_init_order,
        _net_node_conditions, _net_pod_traffic, mtu_step, lambda r, c: _net_throttling(r, c, target),
        _net_kube_proxy, _net_kube_proxy_logs, _net_svc_no_endpoints, _net_service_inventory,
        _net_dns, _net_dns_logs, _net_nodelocal_dns, _net_ndots,
        _net_policies, _net_policy_engine, _net_firewalls, route_step,
        _net_ingresses, _net_controllers, _net_backend_annotations, _net_tls,
        _net_conntrack, lambda r, c: _net_observability(r, c, target), _net_capture_guidance,
        _net_apiserver, _net_webhooks, _net_etcd,
        edge, _net_checklist, _net_full_glossary,
    ]
    vpc_ready = threading.Event()          # the private-routing block reads the VPC that the MTU block fetched

    def block(fn, name, before=None, after=None):
        def run(r):
            try:
                if after is not None:
                    after.wait(600)
                if ctx.cancel is not None and ctx.cancel.is_set():
                    return
                fn(r, ctx)
            except Exception as exc:
                r.add(f"[!] {name} failed: {exc}")
            finally:
                if before is not None:
                    before.set()
        return run
    names = [getattr(fn, "__name__", "network step") for fn in steps]
    mtu_i, route_i = steps.index(mtu_step), steps.index(route_step)
    parallel_part = steps[:-2]             # the checklist and the glossary are built from everything above them
    blocks = [block(fn, names[i], before=vpc_ready if i == mtu_i else None, after=vpc_ready if i == route_i else None)
              for i, fn in enumerate(parallel_part)]
    slow = [i for i, fn in enumerate(parallel_part) if fn in (edge, traffic_step, cni_logs_step)]
    run_blocks(rep, ctx, blocks, first=slow)
    for fn in steps[-2:]:
        if ctx.cancel is not None and ctx.cancel.is_set():
            return
        try:
            fn(rep, ctx)
        except Exception as exc:
            rep.add(f"[!] {getattr(fn, '__name__', 'network step')} failed: {exc}")


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
        rep.sub("Horizontal pod autoscalers with issues", "autoscalers that have reached their maximum number of pods or cannot scale; a workload at its maximum cannot grow when the load rises.", ["Horizontal pod autoscaler"])
        rep.table(["HPA", "SUPPORT DL", "REPLICAS", "ISSUE"], rows,
                  about="One row per horizontal pod autoscaler with a problem: its namespace and name, the support team, the replicas now, desired and maximum (current/desired/maximum) and the issue.")
        ctx.find("MED", f"{len(rows)} HPA(s) at max or unable to scale" + support_suffix(ctx, {r[0].split("/")[0] for r in rows}))
    else:
        rep.add("Horizontal pod autoscalers: no issues found (or none defined).")

    pvcs = [[f"{p['metadata']['namespace']}/{p['metadata']['name']}", support_of(ctx, p["metadata"]["namespace"]) or "-",
             p.get("status", {}).get("phase", "?"),
             p.get("spec", {}).get("storageClassName", "-"), age(parse_ts(p["metadata"].get("creationTimestamp")), ctx.now)]
            for p in items(ctx.data.get("pvc")) if p.get("status", {}).get("phase") != "Bound"]
    pvs = [[p["metadata"]["name"], "-", p.get("status", {}).get("phase", "?"), "-", "-"]
           for p in items(ctx.data.get("pv")) if p.get("status", {}).get("phase") in ("Failed",)]
    if pvcs or pvs:
        rep.sub("Storage problems: persistent volume claims not Bound and persistent volumes Failed", "storage requests that no volume satisfies and volumes that failed; pods waiting for such storage cannot start.",
                ["Persistent volume claim", "Persistent volume", "StorageClass"])
        rep.table(["NAME", "SUPPORT DL", "PHASE", "STORAGECLASS", "AGE"], pvcs + pvs,
                  about="One row per persistent volume claim that is not Bound and per persistent volume that Failed: namespace and name, the support team, its phase, the storage class and its age.")
        for r in pvcs:
            ctx.ns_issue(r[0].split("/")[0], f"PVC {r[0].split('/', 1)[1]} is {r[2]}")
        ctx.find("HIGH", f"{len(pvcs)} PVC(s) not Bound, {len(pvs)} PV(s) Failed" + support_suffix(ctx, {r[0].split("/")[0] for r in pvcs}))
    else:
        rep.add("Storage: all persistent volume claims are Bound.")

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
        rep.sub("Services with problems", "Services that have no ready endpoints (no pod behind them can answer) or a LoadBalancer that has no external address yet.", ["LoadBalancer", "ClusterIP"])
        rep.table(["SERVICE", "SUPPORT DL", "SERVICE TYPE", "ISSUE"], svc_rows,
                  about="One row per Service with a problem: its namespace and name, the support team, the Service type and what is wrong.")
        ctx.find("MED", f"{len(svc_rows)} Service(s) with no endpoints / pending LoadBalancer" + support_suffix(ctx, {r[0].split("/")[0] for r in svc_rows}))
        for r in svc_rows:
            ctx.ns_issue(r[0].split("/")[0], f"service {r[0].split('/', 1)[1]}: {r[3]}")
    else:
        rep.add("Services: all selector services have ready endpoints; no pending LoadBalancers.")

    term = [n["metadata"]["name"] for n in items(ctx.data.get("namespaces")) if n.get("status", {}).get("phase") == "Terminating"]
    if term:
        rep.sub("Namespaces stuck Terminating", "namespaces whose deletion was requested but has not finished; something inside them is blocking it.", ["Terminating", "Namespace"])
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
        node_rows.append({"name": name, "id": ident["instance_id"], "zone": ident["zone"], "type": ident["type"], "vm": ident["vm_name"],
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
    rep.section("4. RESOURCE UTILIZATION - PROCESSOR AND MEMORY BY NAMESPACE")
    data = build_utilization(ctx)
    if not data["namespaces"] and not data["nodes"]:
        rep.add("No node or pod data.")
        return
    c = data["cluster"]
    cpu_p, mem_p = _pct(c["cu"], c["ca"]), _pct(c["mu"], c["ma"])
    rep.add(f"Cluster processor (CPU): {_cores(c['cu']) if c['cu'] is not None else 'n/a'} used of {c['ca']:.1f} cores allocatable ({_fp(cpu_p)}); "
            f"{c['cr']:.1f} cores requested ({_fp(_pct(c['cr'], c['ca']))})")
    rep.add(f"Cluster memory        : {fmt_gib(c['mu']) if c['mu'] is not None else 'n/a'} used of {fmt_gib(c['ma'])} allocatable ({_fp(mem_p)}); "
            f"{fmt_gib(c['mr'])} requested ({_fp(_pct(c['mr'], c['ma']))})")
    if not data["hasUsage"]:
        rep.add("Live usage is not available (needs the kubelet stats permission or metrics-server), so this section shows what pods REQUEST and are LIMITED to.")

    rep.util(data, about="an interactive dashboard of processor and memory by namespace and by pod: use compared with requests and limits and with the capacity of the cluster; bars are green below 75 percent, amber from 75 and red from 90 percent.")       # the interactive dashboard comes first in the HTML; the table below is the detailed list

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
    rep.sub("Namespaces ranked by memory", "the namespaces from the biggest memory user to the smallest, with processor and memory use, requests and limits, their share of what the whole cluster can offer, and how many of their pods use 90 percent or more of a limit.",
            ["Request", "Limit", "Allocatable"])
    rep.table(["NAMESPACE", "SUPPORT DL", "PODS", "CPU use", "CPU req", "CPU lim", "CPU %cl", "MEM use", "MEM req", "MEM lim", "MEM %cl", "HIGH PODS"], rows,
              about="One row per namespace: its pods, processor and memory used now, requested and limited, its share of the cluster's allocatable processor and memory (percent of cluster), and the number of pods near a limit.")

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
    for label, key in (("processor (CPU)", "cpu"), ("memory", "mem")):
        ranked = sorted(((k, u) for k, u in usage.items() if u.get(key) is not None), key=lambda kv: -kv[1][key])[:10]
        rows = []
        for (ns, name), u in ranked:
            res = _pod_resources(pods[(ns, name)]) if (ns, name) in pods else {"cpu_lim": 0, "mem_lim": 0}
            lim = res["cpu_lim"] if key == "cpu" else res["mem_lim"]
            rows.append([ns, support_of(ctx, ns) or "-", name, _cores(u["cpu"]) if u.get("cpu") is not None else "n/a", _mi(u["mem"]) if u.get("mem") is not None else "n/a",
                         _mi(u["disk"]) if u.get("disk") else "-", _fp(_pct(u[key], lim)) if lim else "no limit"])
        rep.sub(f"Top 10 pods by {label}", f"the ten pods that use the most {label} right now, with their other usage and how close the {label} is to the pod's limit.", ["Limit"])
        rep.table(["NAMESPACE", "SUPPORT DL", "POD", "CPU", "MEMORY", "DISK", f"% of {label} limit"], rows,
                  about=f"The ten pods with the highest {label} use now: namespace, support team, pod, processor, memory and disk used, and the percentage of the {label} limit in use (no limit means it can use all the node has).")


CORE_ADDON_PREFIXES = ("kube-dns", "kube-dns-autoscaler", "konnectivity-agent", "gke-metadata-server", "netd", "anetd", "cilium", "calico",
                       "ip-masq-agent", "kube-proxy", "fluentbit-gke", "gke-metrics-agent", "pdcsi-node", "gcsfusecsi-node", "filestorecsi-node",
                       "event-exporter", "l7-default-backend", "metrics-server", "cluster-autoscaler", "node-local-dns", "stackdriver-metadata-agent",
                       "nvidia-gpu-device-plugin", "antrea", "keda", "ingress-nginx", "nginx", "external-dns", "cert-manager")
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
       3. core add-ons                (kube-dns, netd / anetd, gke-metadata-server, CSI drivers, autoscalers ...)
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

    rep.sub(f"Overview of {len(rows)} log stream(s)", "how many lines each collected log has, how many look like errors or warnings and the last line, so you can pick the logs worth opening.")
    rep.table(["POD", "SUPPORT DL", "CONTAINER", "LOG", "WHY COLLECTED", "LINES", "ERRORS", "WARNINGS", "LAST LINE"], rows, maxw=90,
              about="One row per container log that was read: the pod, the support team, the container, whether it is the current or the previous run, why it was collected, and the number of lines, error-like lines and warnings with the last line.")
    errs_total = sum(r[6] for r in rows)
    rep.add(f"Total: {sum(r[5] for r in rows)} line(s), {errs_total} error-like.")
    if blocks:
        rep.sub("Log excerpts, one per container", f"the log lines of each container from the last {ctx.minutes} minutes (the HTML keeps them all, the text report the error-like lines and the last lines); error-like lines are red and warnings orange. Open one to read it.")
    for title, entries, text_entries in blocks:
        rep.log(title, entries, text_entries)
    if ctx.cancel is not None and ctx.cancel.is_set():
        rep.add("(log collection was stopped early)")


def section_timeline(rep, ctx):
    rep.section(f"14. TIMELINE - what happened in the last {ctx.minutes} min (oldest first)")
    if not ctx.timeline:
        rep.add("Nothing notable recorded in this window.")
        return
    entries = sorted(dict.fromkeys(ctx.timeline), key=lambda x: x[0])     # no duplicates, ties stay in report order
    skipped = max(0, len(entries) - MAX_TIMELINE)
    if skipped:
        rep.add(f"({skipped} older entries not shown)")
    rep.timeline(entries[-MAX_TIMELINE:], about=f"everything notable that happened in the last {ctx.minutes} minutes in time order (UTC), each entry with its kind (event, node, operation, rollout ...) so you can see what came first.")


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
        lines += ["What the severity tags mean: " + "  ".join(f"[{k}] {n} = {m}" for k, (n, m) in zip(("CRIT", "HIGH", "MED", "INFO"), ((r[0], r[1]) for r in _SEVERITY_LEGEND)))]
    guard = getattr(ctx, "guard", None)
    if guard is not None:
        statement, blocked = guard_summary_lines(guard)
        lines += ["", "READ-ONLY GUARANTEE: " + statement]
        for tool, cmd_text, reason in blocked:
            lines.append(f"  BLOCKED {tool}: {cmd_text}  ({reason})")
    plan = getattr(ctx, "plan", None)
    if plan and plan["skipped"]:
        lines.append("Sections not collected (skipped by choice): " + ", ".join(SECTION_BY_ID[i]["title"] for i in plan["skipped"]) + ". Findings and counts cover the collected sections only.")
    if plan and plan["silent"]:
        lines.append("Note: " + ", ".join(SECTION_BY_ID[i]["title"] + (" (" + ", ".join(parts) + " only)" if parts else "") for i, parts in plan["silent"].items())
                     + " was collected silently, because a selected section needs it. It is not shown and its findings are not counted.")
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
.subh{margin:22px 0 2px;font-size:15px;border-top:1px dashed var(--line);padding-top:12px}
.about{color:var(--muted);font-size:12.5px;margin:2px 0 6px}
.shows{background:var(--infobg);color:var(--text);border-left:4px solid var(--accent);border-radius:8px;padding:9px 13px;margin:12px 0 6px;font-size:13px}
.shows b{color:var(--info)}.shows .how{margin-top:5px;color:var(--muted)}.shows .how b{color:var(--muted)}
th[title]{text-decoration:underline dotted var(--muted);text-underline-offset:3px}
.check{border:1px solid var(--line);border-left-width:6px;border-radius:8px;padding:8px 12px;margin:8px 0;background:var(--card)}
.check .stpill{font-weight:700;font-size:12px;border-radius:10px;padding:1px 10px;white-space:nowrap;margin-right:8px}
.check .nx{margin-top:4px;color:var(--text)}.check .nx b{color:var(--muted);font-weight:600}
.st-ok{border-left-color:var(--good)}.st-ok .stpill{background:var(--goodbg);color:var(--good)}
.st-warning{border-left-color:#e8a317}.st-warning .stpill{background:var(--medbg);color:var(--med)}
.st-problem{border-left-color:var(--crit)}.st-problem .stpill{background:var(--critbg);color:var(--crit)}
.st-notavailable{border-left-color:var(--muted)}.st-notavailable .stpill{background:var(--code);color:var(--muted)}
.gloss{margin:8px 0}.gloss>.gtitle{font-weight:650;font-size:13px;margin-bottom:2px}
.gloss table.data th{cursor:default}
footer{padding:10px 18px 30px;color:var(--muted);font-size:12px;text-align:center}
@media(max-width:900px){.layout{grid-template-columns:1fr}nav{position:static;max-height:none}}
@media print{header,nav,.tbtools,.toolbar{display:none}.layout{display:block}details.sec{break-inside:avoid}}
"""

_HTML_JS = r"""
(function(){
const $=(s,r)=>(r||document).querySelector(s), $$=(s,r)=>Array.from((r||document).querySelectorAll(s));
const root=document.documentElement;
try{const t=localStorage.getItem('gke-theme');if(t)root.setAttribute('data-theme',t);else if(matchMedia('(prefers-color-scheme: dark)').matches)root.setAttribute('data-theme','dark');}catch(e){}
$('#theme').onclick=()=>{const d=root.getAttribute('data-theme')==='dark'?'light':'dark';root.setAttribute('data-theme',d);try{localStorage.setItem('gke-theme',d)}catch(e){}};
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
const BAD=/^(NotReady|CrashLoopBackOff|Error|Failed|Evicted|ImagePullBackOff|ErrImagePull|OOMKilled|DEGRADED|CREATE_FAILED|FAILED|PROBLEM|impaired|CRIT|Unknown|MISSING|FAILING|TERMINATED|STOPPING|SUSPENDED|UNHEALTHY|ALL BACKENDS|NOT STABLE|RUNNING_WITH_ERROR|CANNOT HOLD)/i;
const WARN=/^(Warning|Pending|Terminating|Ready,SchedulingDisabled|SchedulingDisabled|UPDATING|CREATING|RECONCILING|PROVISIONING|STAGING|REPAIRING|low IPs|VERY LOW|HIGH|AT MAX|insufficient|NEW node|nearly full|filling up)/i;
const GOOD=/^(Ready|Running|ACTIVE|OK|ok|Bound|Succeeded|Completed|available)$/;
$$('table.data').forEach(tb=>{
 const heads=$$('th',tb);const rows=$$('tbody tr',tb);
 rows.forEach(tr=>$$('td',tr).forEach((td,i)=>{
  const t=td.textContent.trim(),h=(heads[i]?heads[i].textContent:'');
  if(tb.id!=='findings'){
   if(BAD.test(t)||/MISSING|NOT READY|DEGRADED|FAILING|PROBLEM|impaired|OPEN TO THE INTERNET|NOT a registered/.test(t))td.classList.add('bad');
   else if(WARN.test(t)||/HIGH REQUESTS|AT MAX|low IPs|SWAP in use|MEM \d+% of limit/.test(t))td.classList.add('warn');
   else if(GOOD.test(t))td.classList.add('good');
   const p=t.match(/\((\d+)%\)\s*$/)||(/CPU|PROCESSOR|MEM|DISK|IMAGE|SWAP|REQUEST|USED|req/i.test(h)&&t.match(/^(\d+)%$/));
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

def _brand_css():
    """The report's cloud branding: accent colour, header band with the logo, soft cloud shapes, section icons. Built from the BRAND_ constants."""
    return ("""
:root{--accent:%(p)s;--brand:%(p)s;--brand2:%(a)s;--bandbg:linear-gradient(100deg,%(soft)s 0%%,#ffffff 55%%,%(soft)s 100%%)}
[data-theme=dark]{--accent:#8ab4f8;--brand:#8ab4f8;--brand2:#81c995;--bandbg:linear-gradient(100deg,#1a2740 0%%,#171e28 55%%,#1a2740 100%%)}
.band{position:relative;overflow:hidden;background:var(--bandbg);border-bottom:4px solid transparent;border-image:linear-gradient(90deg,%(c0)s 0 25%%,%(c1)s 25%% 50%%,%(c2)s 50%% 75%%,%(c3)s 75%% 100%%) 1;padding:14px 22px;display:flex;align-items:center;gap:16px;min-height:84px}
.band .logo{flex:0 0 auto;filter:drop-shadow(0 1px 2px rgba(60,64,67,.25))}
.band .bt{position:relative;z-index:2;min-width:0}.band h2{margin:0;font-size:22px;font-weight:650;color:%(dark)s;letter-spacing:.1px}[data-theme=dark] .band h2{color:#e8eaed}
.band .bs{color:var(--muted);font-size:13px;margin-top:3px}
.band .cloud{position:absolute;z-index:1;opacity:.55;pointer-events:none}[data-theme=dark] .band .cloud{opacity:.12}
.band .cloud.c1{right:-30px;top:-18px;width:190px}.band .cloud.c2{right:170px;bottom:-26px;width:130px;opacity:.35}.band .cloud.c3{right:380px;top:6px;width:80px;opacity:.28}
.sec>summary .ico,nav a .ico{display:inline-block;width:1.45em;text-align:center;margin-right:2px}
nav a{border-left:3px solid transparent}nav a.on{border-left-color:var(--brand);background:var(--code)}
details.sec{border-top:3px solid var(--brand)}details.sec.skipped{border-top-color:var(--line);opacity:.72}details.sec.skipped>summary{font-weight:500;color:var(--muted)}
nav a.skipnav{color:var(--muted)}.badge.b-skip{background:var(--code);color:var(--muted)}
button.primary,.toolbar button:hover{border-color:var(--brand);color:var(--brand)}
.timing{margin-top:6px}.logo{vertical-align:middle}
footer .foot-logo{vertical-align:middle;margin-right:8px}
""" % {"p": BRAND_PRIMARY, "a": BRAND_ACCENT, "soft": BRAND_SOFT, "dark": BRAND_DARK, "c0": BRAND_COLORS[0], "c1": BRAND_COLORS[1], "c2": BRAND_COLORS[2], "c3": BRAND_COLORS[3]})


def _icon_css():
    """Section icons as CSS (::before on the heading and on the table-of-contents link), one rule per registry section."""
    rules = ['#summary>summary>span:first-child::before,nav a[href="#summary"]>span:first-child::before{content:"\\1F4CB  "}',
             '#steps>summary>span:first-child::before,nav a[href="#steps"]>span:first-child::before{content:"\\1F552  "}']
    for sec in SECTIONS:
        code = "".join("\\%X" % ord(ch) for ch in sec["icon"])
        sel = '#s%d>summary>span:first-child::before,nav a[href="#s%d"]>span:first-child::before' % (sec["num"], sec["num"])
        rules.append(sel + '{content:"' + code + '  "}')
    return "\n".join(rules)


def _band_html(title, subtitle):
    """The header band: the logo, the product title, a subtitle and three soft cloud shapes (decoration only)."""
    cloud = ('<svg class="cloud %s" viewBox="0 0 100 64" aria-hidden="true"><g fill="%s"><circle cx="27" cy="42" r="14"/><circle cx="46" cy="28" r="19"/>'
             '<circle cx="68" cy="38" r="16"/><rect x="27" y="40" width="41" height="15"/></g></svg>')
    return ('<div class="band">%s<div class="bt"><h2>%s</h2><div class="bs">%s</div></div>%s%s%s</div>'
            % (logo_svg(78), _html.escape(title), subtitle, cloud % ("c1", BRAND_SOFT), cloud % ("c2", BRAND_SOFT), cloud % ("c3", BRAND_SOFT)))


_SEV_ORDER = {"CRIT": 0, "HIGH": 1, "MED": 2, "INFO": 3}
_SEV_NAME = {"CRIT": "Critical", "HIGH": "High", "MED": "Medium", "INFO": "Information"}   # shown in the HTML (the text report keeps [CRIT] tags)


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
if(!D.hasUsage)root.appendChild(el('div','note','<b>Live processor (CPU) and memory usage is not available</b> (it needs the kubelet stats permission or metrics-server). Everything below shows what pods <b>request</b> and are <b>limited</b> to instead of what they use right now.'));

// ---- cluster tiles
function tile(label,use,total,fmt,req){
  const p=pct(use,total),rp=pct(req,total),t=el('div','tile');
  t.innerHTML='<div class="tl">'+label+'</div><div class="tv">'+(use==null?'n/a':fmt(use))+' <small>of '+fmt(total)+' allocatable ('+fPct(p)+')</small></div>'
   +'<div class="gauge"><i class="g '+sev(p)+'" style="width:'+Math.min(100,p||0)+'%"></i>'+(rp!=null?'<i class="m" style="left:'+Math.min(100,rp)+'%" title="requested"></i>':'')+'</div>'
   +'<div class="ts">requested '+fmt(req)+' ('+fPct(rp)+') &nbsp;|&nbsp; the tick marks the requested amount</div>';
  return t;}
const tiles=el('div','tiles');
tiles.appendChild(tile('CLUSTER PROCESSOR (CPU)',C.cu,C.ca,fCpu,C.cr));
tiles.appendChild(tile('CLUSTER MEMORY',C.mu,C.ma,fMem,C.mr));
(function(){const t=el('div','tile'),p=pct(C.pods,C.mp);
  t.innerHTML='<div class="tl">PODS (running + pending) vs node capacity</div><div class="tv">'+C.pods+' <small>of '+C.mp+' ('+fPct(p)+')</small></div><div class="gauge"><i class="g '+sev(p)+'" style="width:'+Math.min(100,p||0)+'%"></i></div>';tiles.appendChild(t);})();
root.appendChild(tiles);

// ---- who uses the cluster: stacked share by namespace
function stacked(title,key,reqKey,fmt,total){
  const useK=D.hasUsage?key:reqKey;let arr=D.namespaces.filter(n=>n[useK]).sort((a,b)=>b[useK]-a[useK]);
  const sum=arr.reduce((s,n)=>s+n[useK],0);if(!sum)return;
  const top=arr.slice(0,10),rest=arr.slice(10).reduce((s,n)=>s+n[useK],0);
  const wrap=el('div');wrap.appendChild(el('h3',null,title+(D.hasUsage?' (used)':' (requested)')));wrap.appendChild(el('p','about','What this block shows: one bar split by namespace; the wider a segment, the bigger the share of that namespace in what pods '+(D.hasUsage?'use':'request')+' (the ten biggest namespaces, the rest as others). Hover a segment for the exact value.'));
  const bar=el('div','stack'),leg=el('div','legend');
  top.forEach(n=>{const s=el('span');s.style.width=(100*n[useK]/sum)+'%';s.style.background=nsColor(n.name);s.title=n.name+': '+fmt(n[useK])+' ('+Math.round(100*n[useK]/sum)+'% of what pods use; '+fPct(pct(n[useK],total))+' of allocatable)';bar.appendChild(s);
    leg.appendChild(el('span',null,'<b style="background:'+nsColor(n.name)+'"></b>'+esc(n.name)+' '+fmt(n[useK])+' ('+Math.round(100*n[useK]/sum)+'%)'));});
  if(rest){const s=el('span');s.style.width=(100*rest/sum)+'%';s.style.background='#98a2b3';s.title='other namespaces: '+fmt(rest);bar.appendChild(s);leg.appendChild(el('span',null,'<b style="background:#98a2b3"></b>others '+fmt(rest)));}
  wrap.appendChild(bar);wrap.appendChild(leg);root.appendChild(wrap);}
stacked('Processor (CPU) share by namespace','cu','cr',fCpu,C.ca);
stacked('Memory share by namespace','mu','mr',fMem,C.ma);

// ---- nodes
root.appendChild(el('h3',null,'Nodes (sorted by the most loaded)'));root.appendChild(el('p','about','What this block shows: one card per node, the most loaded first: processor, memory, disk and pod count against what the node offers; the tick marks what pods requested. A bar is green below 75 percent, amber from 75 and red from 90 percent.'));
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
    +'<div class="nid"><b>'+esc(n.id||'-')+'</b> &middot; '+esc(n.zone||'-')+' &middot; '+esc(n.type||'-')+(n.vm&&n.vm!=='-'&&n.vm!==n.name?' &middot; virtual machine: '+esc(n.vm):'')+'</div>';
  c.appendChild(nrow('Processor (CPU)',n.cu,n.ca,fCpu,n.cr));c.appendChild(nrow('Memory',n.mu,n.ma,fMem,n.mr));
  if(n.du!=null)c.appendChild(nrow('Disk',n.du,n.dc,fMem));
  c.appendChild(nrow('Pods',n.pods,n.mp,x=>String(Math.round(x))));
  if(n.su)c.appendChild(el('div','nv','<span class="pill c">swap in use '+fMem(n.su)+'</span>'));
  grid.appendChild(c);});
root.appendChild(grid);

// ---- namespace explorer
root.appendChild(el('h3',null,'By namespace (click a namespace to see its pods)'));root.appendChild(el('p','about','What this block shows: the namespaces ranked by processor, memory or disk, used or requested, against their limits and the capacity of the cluster; choose the metric, sort or filter, and click a namespace to list its pods.'));
const S={metric:'mem',mode:D.hasUsage?'use':'req',sort:'use',high:false,q:''};
const KEY={cpu:{use:'cu',req:'cr',lim:'cl',nolim:'ncl',fmt:fCpu,tot:C.ca,name:'Processor (CPU)'},mem:{use:'mu',req:'mr',lim:'ml',nolim:'nml',fmt:fMem,tot:C.ma,name:'Memory'},disk:{use:'du',req:null,lim:null,fmt:fMem,tot:null,name:'Disk'}};
const ctl=el('div','ctl');
ctl.innerHTML='<span class="seg" id="u-metric"><button data-v="cpu">Processor (CPU)</button><button data-v="mem" class="on">Memory</button><button data-v="disk">Disk</button></span>'
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
root.appendChild(el('h3',null,'Top consumers (pods)'));root.appendChild(el('p','about','What this block shows: the pods that use the most, the pods closest to their limit and the pods with the largest requests, for processor and for memory.'));
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
  toplist('Top processor (CPU) users',p=>p.cu,fCpu,p=>sev(pct(p.cu,p.cl)),p=>p.cl?fPct(pct(p.cu,p.cl))+' of limit':'no limit');
  toplist('Top memory',p=>p.mu,fMem,p=>sev(pct(p.mu,p.ml)),p=>p.ml?fPct(pct(p.mu,p.ml))+' of limit':'no limit');
  toplist('Closest to memory limit (risk of an out-of-memory (OOM) kill)',p=>pct(p.mu,p.ml),fPct,p=>sev(pct(p.mu,p.ml)),p=>fMem(p.mu)+' / '+fMem(p.ml));
  toplist('Closest to processor (CPU) limit (throttling)',p=>pct(p.cu,p.cl),fPct,p=>sev(pct(p.cu,p.cl)),p=>fCpu(p.cu)+' / '+fCpu(p.cl));
  toplist('Using more memory than requested',p=>(p.mu!=null&&p.mr)?pct(p.mu,p.mr):null,fPct,null,p=>fMem(p.mu)+' vs requested '+fMem(p.mr));
}else{
  toplist('Largest memory requests',p=>p.mr,fMem,null,null);toplist('Largest processor (CPU) requests',p=>p.cr,fCpu,null,null);
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



# 'What each column means': shown as a tooltip (title attribute) on the column header when the name alone is not obvious.
COLUMN_HELP = {
    "NODE POOL": "A group of nodes with the same machine type and settings that is resized and upgraded together.",
    "MACHINE TYPE": "The Compute Engine machine type (processor count and memory size) of the node.",
    "NODES": "Nodes that are ready, and how many the pool wants (target size) and may grow to (autoscaler limits).",
    "DISK / IMAGE": "Boot disk size and type, and the operating system image of the nodes.",
    "ZONES": "The Google Cloud zones the nodes of the pool run in.",
    "MAXIMUM PODS PER NODE": "The most pods that may run on one node; it decides how large a block of the pod address range every node reserves.",
    "PRIORITY": "Rules: the lower the number, the earlier it is evaluated. Machines: standard, spot or preemptible (spot and preemptible can be stopped by Google at any time).",
    "STATUS": "The current state reported by the system; anything other than OK, Ready, Running or RUNNING needs a look.",
    "HEALTH": "OK, or the reason the pool needs attention.",
    "RANGE": "The name of a subnet address range: the node range, the pod range or the service range.",
    "ADDRESS RANGE": "The range of IP addresses written in CIDR notation, for example 10.4.0.0/14 (see the glossary).",
    "ADDRESSES": "How many IP addresses the range contains in total.",
    "USED BY THIS CLUSTER": "How many addresses this cluster has reserved or in use out of the range.",
    "FREE (ESTIMATED)": "Addresses still free. It is an estimate because other machines in the same subnet are not counted.",
    "NEEDED AT MAX SIZE": "How many addresses the cluster would need if every pool grew to its autoscaler maximum.",
    "DIRECTION": "INGRESS = traffic coming in to the machines, EGRESS = traffic going out.",
    "ACTION": "ALLOW lets the traffic through, DENY blocks it.",
    "PROTOCOL:PORTS": "The network protocol and port numbers the rule applies to, for example tcp:443.",
    "SOURCE OR DESTINATION": "Where the traffic comes from (ingress rules) or goes to (egress rules): address ranges or tags.",
    "TARGET": "Which machines the rule applies to (network tags or service accounts); (all) means every machine.",
    "RULE": "The name of the firewall rule.",
    "NOTE": "An observation about the row; empty or ok means nothing special.",
    "ROUTER": "The Cloud Router that hosts the Cloud NAT gateway.",
    "IP ADDRESS ALLOCATION": "AUTO_ONLY = Google picks the NAT IP addresses, MANUAL_ONLY = fixed addresses you reserved.",
    "SOURCE RANGES": "Which subnet ranges are allowed to use the gateway.",
    "MINIMUM PORTS PER VIRTUAL MACHINE": "Network address translation ports every virtual machine is given at least; each outgoing connection uses one.",
    "DYNAMIC PORTS": "Whether a machine that needs more ports can be given more automatically.",
    "SERVICE ACCOUNT": "The non-human identity the nodes (or Google's own agent) run as.",
    "NODE POOLS": "The node pools that run as this service account.",
    "KIND": "The type of the object, or the type of account.",
    "ROLE": "The permission role granted to the account on the project.",
    "ADD-ON": "A GKE feature that can be switched on or off for the cluster.",
    "STATE": "Whether the item is enabled, running or healthy.",
    "INSTANCE IDENTIFIER": "The Compute Engine instance identifier: the number that identifies the virtual machine behind the node.",
    "INSTANCE GROUP": "The managed instance group (the group of identical virtual machines) behind a node pool in one zone.",
    "TARGET SIZE": "The number of machines the group is told to keep.",
    "ACTIONS IN PROGRESS": "What the group is doing right now, for example creating or deleting machines.",
    "AUTOHEALING": "Whether the group replaces machines that fail their health check.",
    "OPERATION": "The kind of GKE operation, for example UPGRADE_NODES or REPAIR_CLUSTER.",
    "PRINCIPAL": "The user or service account that made the call.",
    "METHOD": "The API method that was called.",
    "CODE": "The result code of the call; 7 = permission denied, 16 = not authenticated.",
    "COUNT": "How many times it happened in the window.",
    "OCCURRENCES": "How many times the event happened in the window.",
    "COMPUTE ENGINE INSTANCE": "The name of the virtual machine in Compute Engine.",
    "PROVIDER IDENTIFIER": "The text gce://project/zone/instance that links the node to its virtual machine.",
    "CAPACITY": "Whether the machine is standard or spot, and its size.",
    "PODS": "Number of pods; on node tables it is running pods out of the maximum the node allows.",
    "PROCESSOR (CPU) USED / ALLOCATABLE": "Processor in use now compared with what is left for pods after the system share (allocatable); cores or millicores.",
    "MEMORY USED / ALLOCATABLE": "Memory in use now (working set) compared with the allocatable memory of the node.",
    "DISK USED / TOTAL": "Space used on the node's root disk out of its total size.",
    "IMAGE FILESYSTEM USED / TOTAL": "Space used on the disk area that holds container images out of its total size.",
    "SWAP": "Swap space in use; any value above zero means the node ran short of memory.",
    "ROLES": "The role label of the node (control-plane or worker).",
    "VERSION": "The Kubernetes version the node runs.",
    "AGE": "How long ago the object was created.",
    "PROCESSOR (CPU) REQUESTED": "The sum of the processor requests of all pods on the node compared with the allocatable processor.",
    "MEMORY REQUESTED": "The sum of the memory requests of all pods on the node compared with the allocatable memory.",
    "EPHEMERAL STORAGE REQUESTED / ALLOCATABLE": "Temporary disk space requested by pods compared with what the node can give.",
    "FINDINGS": "Problems found for the node; none means nothing special.",
    "SUPPORT DISTRIBUTION LIST": "The e-mail group of the team that supports the namespace (from the namespace label).",
    "RESTARTS": "How many times the containers have been restarted; a high number means they keep crashing.",
    "PROCESSOR (CPU) USE": "Processor used right now.",
    "PROCESSOR (CPU) USED": "Processor used right now (cores, or millicores where 1000 millicores is one core).",
    "PROCESSOR (CPU) REQUEST": "Processor the pod asked for; the scheduler reserves it.",
    "PROCESSOR (CPU) LIMIT": "The most processor the pod may use; above it the pod is slowed down.",
    "MEMORY USED": "Memory used right now (working set).",
    "MEMORY REQUEST": "Memory the pod asked for; the scheduler reserves it.",
    "MEMORY LIMIT": "The most memory the pod may use; above it the container is killed (out-of-memory kill).",
    "DISK USED": "Disk space used by the pod's writable layer and logs.",
    "NOTES": "Observations about the row; empty means nothing special.",
    "RUNNING": "Pods that are running right now.",
    "PENDING": "Pods that are waiting to start.",
    "FAILED": "Pods that ended with an error.",
    "COMPLETED": "Pods of finished jobs.",
    "CONFIGURED": "Pods the owners (Deployments, StatefulSets, DaemonSets) want, plus standalone pods.",
    "POD QUOTA": "Pods used out of the pod limit of the namespace's resource quota.",
    "DESIRED": "The number of copies the workload is configured to run.",
    "READY": "Copies that are ready to serve, out of the number wanted.",
    "AVAILABLE": "Copies that have been ready long enough to count as available.",
    "HORIZONTAL POD AUTOSCALER MINIMUM-MAXIMUM REPLICAS": "The fewest and the most pod copies the autoscaler may run.",
    "QUOTA": "The name of the resource quota object.",
    "RESOURCE": "Which resource the quota limits, for example pods or memory.",
    "USED / LIMIT": "How much of the quota is used out of the limit.",
    "WHY": "The most likely reason in plain words.",
    "WHEN": "The time (UTC) of the event.",
    "REASON": "The short reason code Kubernetes gave.",
    "OBJECT": "The Kubernetes object the event is about.",
    "MESSAGE": "The text of the event or log entry.",
    "REPLICA SET": "The ReplicaSet (the copy-keeping object behind a Deployment).",
    "CREATED": "When the object was created.",
    "FAILED PODS": "How many pods of the job failed.",
    "ISSUE": "What is wrong.",
    "HORIZONTAL POD AUTOSCALER": "The name of the autoscaler object.",
    "REPLICAS": "Current copies out of the maximum the autoscaler allows.",
    "PHASE": "The state of the storage object (Bound = in use, Pending = waiting, Failed = broken).",
    "STORAGE CLASS": "The kind of storage that was requested, for example standard or SSD.",
    "PROCESSOR (CPU) % OF CLUSTER": "This namespace's share of the processor the whole cluster can offer.",
    "MEMORY % OF CLUSTER": "This namespace's share of the memory the whole cluster can offer.",
    "HIGH PODS": "Pods that use 90 percent or more of their limit.",
    "CONTAINER": "The container inside the pod that wrote the log.",
    "LOG": "Which log: the current one or the previous run of the container.",
    "WHY COLLECTED": "Why this pod's log was read: unhealthy, warning events or core add-on.",
    "LINES": "How many log lines were read.",
    "ERRORS": "How many lines look like errors.",
    "ERROR COUNT": "How many lines look like errors.",
    "WARNINGS": "How many lines look like warnings.",
    "LAST LINE": "The final log line, or the latest error line.",
    "DETAIL": "More information about the row.",
    "TERM": "The short form or technical word.",
    "FULL NAME": "The term written out in full.",
    "PLAIN-LANGUAGE MEANING": "What the term means, in everyday words.",
    "SEVERITY": "How serious the finding is: Critical, High, Medium or Information (see the legend).",
    "FINDING": "What the tool found.",
    "WHERE IN THE REPORT": "The section of this report that has the details; click to jump there.",
    "WHAT": "What was found for those namespaces.",
    "NAMESPACES": "The namespaces with problems that this team supports.",
    "NUMBER OF ISSUES": "How many findings belong to these namespaces.",
    "STEP": "The collection step.",
    "RESULT": "Whether the step finished (done), failed or was skipped.",
    "TIME": "How long the step took in seconds, or the time of the entry.",
    "MEANING": "What the value means.",
    "STATE / RESULT": "What the value means.",
}
COLUMN_HELP.update({k: v for k, v in {
    "% OF PROCESSOR (CPU) LIMIT": "How much of the pod's processor limit it uses right now; 'no limit' means it may use everything the node has.",
    "% OF MEMORY LIMIT": "How much of the pod's memory limit it uses right now; close to 100 percent risks an out-of-memory kill.",
    "95TH PERCENTILE LATENCY (WORST MINUTE)": "In the slowest minute, 95 out of 100 requests were faster than this time.",
    "ADDRESS": "The IP address (or the address range) of the item.",
    "ALLOCATION FAILED": "How often the gateway could not give a connection a port or address.",
    "BACKEND HEALTH (AS REPORTED BY GOOGLE KUBERNETES ENGINE)": "Whether Google Cloud says the pods behind the ingress are healthy.",
    "BACKEND SERVICE": "The Google Cloud object that groups the pods or machines a load balancer sends traffic to.",
    "BACKEND TIMEOUT (SECONDS)": "How long the load balancer waits for a backend before it gives up.",
    "BACKENDCONFIG": "The GKE BackendConfig object that tunes the load balancer for a Service.",
    "BACKENDCONFIG REFERENCED": "The BackendConfig that the Service points to; MISSING means it does not exist.",
    "BACKENDS": "How many backends (network endpoint groups or instance groups) the backend service has.",
    "BREAKDOWN": "The kinds of error lines found, with counts.",
    "BYTES RECEIVED (SINCE BOOT)": "Total bytes the node's network card received since the node started.",
    "BYTES TRANSMITTED (SINCE BOOT)": "Total bytes the node's network card sent since the node started.",
    "BYTES RECEIVED (SINCE POD START)": "Total bytes the pod received since it started.",
    "BYTES TRANSMITTED (SINCE POD START)": "Total bytes the pod sent since it started.",
    "BYTES RECEIVED (SINCE PODS STARTED)": "Total bytes the pods of the namespace received since they started.",
    "BYTES TRANSMITTED (SINCE PODS STARTED)": "Total bytes the pods of the namespace sent since they started.",
    "BYTES RECEIVED (TOTAL IN WINDOW)": "All bytes received during the selected window.",
    "BYTES SENT (TOTAL IN WINDOW)": "All bytes sent during the selected window.",
    "BYTES RECEIVED PER SECOND (AVERAGE)": "Average receive rate in the window.",
    "BYTES RECEIVED PER SECOND (PEAK)": "Highest receive rate of any minute in the window.",
    "BYTES SENT PER SECOND (AVERAGE)": "Average send rate in the window.",
    "BYTES SENT PER SECOND (PEAK)": "Highest send rate of any minute in the window.",
    "BYTES RECEIVED / SENT": "Bytes received and sent by the load balancer in the window.",
    "RECEIVE RATE NOW": "Bytes per second received during the short live sample taken by this tool.",
    "TRANSMIT RATE NOW": "Bytes per second sent during the short live sample taken by this tool.",
    "RECEIVE ERRORS": "Packets the network card received with errors; any value above zero is worth a look.",
    "TRANSMIT ERRORS": "Packets the network card could not send; any value above zero is worth a look.",
    "INTERFACE ERRORS": "Receive and transmit errors on the pod's network interface.",
    "CERTIFICATE NAME": "The name of the certificate or of the secret that holds it.",
    "CERTIFICATE SOURCE": "Where the certificate comes from: a Kubernetes secret or a Google-managed certificate.",
    "CERTIFICATE STATUS": "The state of the certificate (Active, Provisioning, Failed ...).",
    "EXPIRES ON": "The date the certificate stops being valid; renew it before.",
    "CHECK": "The name of the check.",
    "CLASS": "The ingress class: which controller handles the Ingress.",
    "CLOUD NETWORK ADDRESS TRANSLATION GATEWAY": "The Cloud NAT gateway (it gives machines without external IP addresses access to the internet).",
    "NETWORK ADDRESS TRANSLATION GATEWAY": "The Cloud NAT gateway (it gives machines without external IP addresses access to the internet).",
    "NETWORK ADDRESS TRANSLATION IP ADDRESSES": "The public IP addresses the gateway uses for outgoing connections.",
    "CLUSTER IP ADDRESS": "The stable virtual IP address of the Service inside the cluster.",
    "CLUSTER IP ADDRESS SERVICES": "How many Services of type ClusterIP the namespace has.",
    "COMPONENT": "The networking add-on or agent that was checked.",
    "CONFIGURATION": "The name of the webhook configuration object.",
    "CONNECTION DRAINING (SECONDS)": "How long the load balancer lets running requests finish when a backend is removed.",
    "CONNECTIONS TRACKED": "Connections the node currently keeps in its connection tracking table.",
    "TABLE LIMIT": "The most connections the tracking table can hold; when it is full new connections are dropped.",
    "CONTENT DELIVERY NETWORK": "Whether Google's content delivery network caching is on for the backend.",
    "CONTROLLER": "The ingress controller (the software that turns Ingress objects into a load balancer).",
    "COUNTER": "The name of the API server counter.",
    "VALUE SINCE THE KUBERNETES API SERVER STARTED": "The counter value since the API server last started.",
    "DEFAULT DENY POLICY PRESENT": "Whether the namespace has a network policy that blocks all traffic that is not explicitly allowed.",
    "NUMBER OF POLICIES": "How many network policy objects the namespace has.",
    "DESTINATION": "The address range a route sends traffic to.",
    "NEXT HOP": "Where the route sends the traffic next (gateway, instance or peering).",
    "DETAIL / ERROR": "Extra information or the error message.",
    "DROPPED PACKETS": "Packets the gateway threw away.",
    "DROPPED: OUT OF RESOURCES (NO FREE PORTS)": "Packets dropped because the gateway had no free port left.",
    "ENDPOINT-INDEPENDENT MAPPING": "Whether the gateway reuses the same port for one machine's connections to different destinations.",
    "EFFECTIVE SETTING": "The ndots value that pods really get, from their DNS settings.",
    "PODS IN SAMPLE": "How many pods of the sampled pods use this setting.",
    "SHARE OF SAMPLE": "Their share of the sampled pods.",
    "TOP NAMESPACES": "The namespaces with the most pods in this group.",
    "ENCRYPTED WITH TRANSPORT LAYER SECURITY": "Whether the Ingress terminates HTTPS with a certificate.",
    "ERROR-LIKE LINES": "Log lines that contain words such as error, failed, refused or timeout.",
    "LATEST ERROR LINE": "The newest line that looks like an error.",
    "LOG LINES READ": "How many log lines were read for the component.",
    "EVIDENCE FOUND": "The data the check is based on.",
    "WHAT TO DO NEXT": "The suggested next step.",
    "EXAMPLE COMMAND (NOT RUN BY THIS TOOL)": "A command you can run yourself; this tool only reads and never runs it.",
    "EXPOSURE": "Whether it is reachable from the internet (external) or only inside the network (internal).",
    "EXTERNAL ADDRESS": "The public or load balancer IP address.",
    "EXTERNAL IP ADDRESS": "The public IP address of the machine, if any.",
    "INTERNAL IP ADDRESS": "The private IP address of the machine in the virtual private cloud network.",
    "FAILURE POLICY": "What the API server does when the webhook does not answer: Fail rejects the request, Ignore lets it through.",
    "FEATURE": "The network observability feature.",
    "FIREWALL CHECK (NODE PORT RANGE 30000-32767)": "Whether a firewall rule opens the Service's node port.",
    "FORWARDING RULE": "The Google Cloud load balancer front end (an IP address and port) that clients connect to.",
    "FREE ADDRESSES": "Addresses of the range that are still free.",
    "TOTAL ADDRESSES": "All addresses in the range.",
    "HEALTH CHECK PATH": "The URL path the load balancer calls to see whether a backend is healthy.",
    "HEALTHY": "Backends that pass the load balancer health check.",
    "UNHEALTHY": "Backends that fail the load balancer health check.",
    "HOSTS": "The host names the Ingress answers for.",
    "HTTP 502": "Number of 502 Bad Gateway responses found in the logs.",
    "HTTP 503": "Number of 503 Service Unavailable responses found in the logs.",
    "HTTP 504": "Number of 504 Gateway Timeout responses found in the logs.",
    "IMAGE": "The container image (and version) the component runs.",
    "INGRESS": "The name of the Ingress object.",
    "ITEM": "The setting or fact that was checked.",
    "VALUE": "What was found.",
    "LIKELY CAUSE": "The most probable reason.",
    "LOAD BALANCER KIND": "The kind of Google Cloud load balancer behind the Service.",
    "LOGGING": "Whether (and what) the gateway writes to Cloud Logging.",
    "MEMORY PRESSURE": "True when the node is running out of memory.",
    "DISK PRESSURE": "True when the node is running out of disk space.",
    "PROCESS PRESSURE": "True when the node has too many processes.",
    "NETWORK UNAVAILABLE": "True when the node's pod network is not set up.",
    "MOST RESTARTS ON ONE POD": "The highest restart count of any single pod of the component.",
    "TOTAL RESTARTS": "All restarts of the component's pods together.",
    "NAMESPACE AND POD": "The namespace and name of the pod.",
    "NAMESPACE OR CLASS": "The namespace of the controller, or its ingress class.",
    "NETWORK ENDPOINT GROUP ANNOTATION": "The cloud.google.com/neg annotation: whether the Service uses network endpoint groups (container-native load balancing).",
    "NETWORK ENDPOINT GROUP ZONES": "The zones in which network endpoint groups exist for the Service.",
    "NODE PORTS": "The port opened on every node for the Service.",
    "NOT READY ENDPOINTS": "Pods behind the Service that are not ready to get traffic.",
    "READY ENDPOINTS": "Pods behind the Service that are ready to get traffic.",
    "NUMBER OF PATHS": "How many URL paths the Ingress routes.",
    "OF WHICH HEADLESS (NO VIRTUAL IP ADDRESS)": "Services without a virtual IP address that answer DNS with the pod addresses directly.",
    "PEAK PORTS IN USE": "The highest number of ports in use in the window.",
    "PORTS ALLOCATED": "Ports the gateway has reserved for the node.",
    "PERCENT USED": "How much of the capacity is used.",
    "POD RANGE": "The subnet range from which pods get their IP addresses.",
    "PODS HOLDING AN ADDRESS": "Pods that currently use an address of the range.",
    "PODS NOT READY": "Pods of the component that are not ready.",
    "PODS READY": "Pods of the component that are ready, out of the total.",
    "PORTS (PORT[:NODE PORT])": "The Service ports, with the node port after a colon if there is one.",
    "PROTOCOL": "The network protocol, for example HTTPS or TCP.",
    "PUBLIC ADDRESS AND PORT RANGES": "The public IP address and port range the gateway gave to the node.",
    "PURPOSE": "What the reserved address is used for.",
    "RANGE KIND": "Node range, pod range or service range.",
    "REQUESTS": "Number of requests the load balancer served in the window.",
    "ROUTE": "The name of the route.",
    "SANDBOX EVENT MESSAGE": "The message of the FailedCreatePodSandBox event.",
    "SCHEME": "EXTERNAL = internet-facing load balancer, INTERNAL = only inside the network.",
    "SCOPE": "Global or the region the item belongs to.",
    "SECURITY POLICY": "The Cloud Armor security policy attached to the backend, if any.",
    "SERVER ERROR RESPONSES (HTTP 5XX)": "Number of responses with a status from 500 to 599.",
    "SERVER ERROR SHARE": "The share of all responses that were server errors.",
    "SERVICE OR URL": "The service (or web address) the webhook calls.",
    "SERVICE TYPE": "ClusterIP, NodePort or LoadBalancer.",
    "SETTING": "The setting that was checked.",
    "TAGS": "The network tags a route applies to.",
    "TARGET / BACKEND": "What the forwarding rule sends traffic to.",
    "TIMEOUT (SECONDS)": "How long the API server waits for the webhook.",
    "TYPE": "The type of the item (for example the Service type or the address type).",
    "USED BY": "What currently uses the item.",
    "WEBHOOK": "The name of the webhook.",
    "WHAT IT DOES": "What the component is for.",
    "WHAT IT GIVES YOU": "What you can see or do with the feature.",
    "WHAT WAS FOUND": "The result of the check.",
    "WHERE IT RUNS OR WHAT IT NEEDS": "Where the method runs and what it needs.",
    "WHY IT MATTERS": "Why the setting is important.",
    "ZONE": "The Google Cloud zone (data center area) the item is in.",
    "START": "When the operation started (UTC).",
    "DISK": "Disk space used by the pod.",
    "MEMORY": "Memory used by the pod now (working set).",
    "PROCESSOR (CPU)": "Processor used by the pod now (cores).",
    "KUBERNETES VERSION": "The Kubernetes version of the node pool.",
    "KUBERNETES OBJECT": "The Kubernetes Service or Ingress that created the item.",
    "JOB": "The name of the job.",
    "DEPLOYMENT": "The Deployment the item belongs to.",
}.items()})
_SEVERITY_LEGEND = [
    ["Critical", "Something is broken or about to break and needs action now, for example a node that is not Ready or a cluster in an error state."],
    ["High", "A likely cause of an outage or a serious risk; look at it today."],
    ["Medium", "A problem that reduces reliability or can become serious; plan a fix."],
    ["Information", "A fact worth knowing or a recommendation; no action needed unless it surprises you."],
]
_STATE_LEGEND = [
    ["OK", "The check was done and the result is healthy."],
    ["Warning", "Worth a look: not broken yet, or only partly wrong."],
    ["Problem", "A likely cause of the trouble you are investigating; act on it."],
    ["Not available", "The check could NOT be done (the reason is shown); this is never a pass."],
]
_SEVERITY_LEGEND_ABOUT = "the four severity levels used for every finding in this report and what each one means."
_STATE_LEGEND_ABOUT = "the four result labels of the network checks and what each one means."
_SUMMARY_SHOWS = ("The result of the whole run on one screen: how many problems were found at each severity, every finding in one list with a link to the section that has the details, "
                  "the teams to contact, and the read-only guarantee of this tool. It is built from all collected sections (last {m} minutes for events, logs and traffic; the rest is the situation now). "
                  "The colours follow the severity: red = Critical, orange = High, yellow = Medium, blue = Information, green = nothing found.",
                  "Click a severity card to hide or show its findings, click a finding's section name to jump to it, and use the search box at the top to search the whole report.")
_STEPS_SHOWS = ("The collection steps this run executed, whether each finished, failed or was skipped, and how long it took. Several steps run at the same time, so the times add up to more than the total run time. "
                "It is a record of this run, not of the cluster.",
                "A failed step explains missing data in the section of the same name; run again or check the login and permissions.")
_GLOSSARY_SHOWS = ("Every short form and technical word used anywhere in this report, written out in full and explained in plain language, once, in alphabetical order, plus the meaning of the severity levels and result labels. "
                   "The same terms are also explained in a small table right before the block that uses them.",
                   "Use the filter box above the table to find a term.")
_INDEX_SHOWS = {
    "clusters": ("One row per cluster of this run: whether it was collected, how many findings it has at each severity, how long it took, which sections were collected, the top finding and a link to its full report. "
                 "Every cluster was collected with the same time window.",
                 "Open the report of a cluster with 'open report'; sort the table by a severity column to see the worst clusters first."),
    "allfindings": ("Every finding of every cluster in one list, worst first, with a link to the section of that cluster's report that has the details. The cards count the findings of all clusters together.",
                    "Click a severity card to hide or show it, or type in the search box to find one finding."),
}


def _shows_box(shows, how=None, title="What this section shows"):
    """The explanation box under a section heading."""
    return ('<div class="shows"><div><b>%s:</b> %s</div>%s</div>'
            % (_html.escape(title), _html.escape(shows), ('<div class="how"><b>How to use it:</b> %s</div>' % _html.escape(how)) if how else ""))


def _th(h):
    tip = COLUMN_HELP.get(str(h).upper())
    return '<th title="%s">%s</th>' % (_html.escape(tip, quote=True), _html.escape(str(h))) if tip else "<th>%s</th>" % _html.escape(str(h))


def _legend_html():
    """The severity and result-label legends (one small table each, with their 'What this table shows' line)."""
    out = []
    for title, rows, about in (("Legend: severity of a finding", _SEVERITY_LEGEND, _SEVERITY_LEGEND_ABOUT), ("Legend: result of a network check", _STATE_LEGEND, _STATE_LEGEND_ABOUT)):
        out.append('<div class="gloss"><div class="gtitle">%s</div>%s</div>' % (_html.escape(title), _html_table(["TERM", "MEANING"], rows, about)))
    return "".join(out)


def complete_glossary(rep):
    """[[term, full name, plain-language meaning]] of every glossary term printed anywhere in the report, de-duplicated and sorted."""
    found = {}
    for sec in rep.sections:
        for blk in sec["blocks"]:
            if blk[0] == "glossary":
                for term, full, meaning in blk[3]:
                    found.setdefault(term, (full, meaning))
    return [[t, found[t][0], found[t][1]] for t in sorted(found, key=lambda x: x.lower())]


def glossary_text_lines(rows):
    """The complete glossary (and the legends) at the end of the text report."""
    def table(headers, data, widths):
        out = ["  ".join(h.ljust(w) for h, w in zip(headers, widths)).rstrip()]
        for r in data:
            out.append("  ".join(str(c)[:w].ljust(w) if i < len(r) - 1 else str(c) for i, (c, w) in enumerate(zip(r, widths))).rstrip())
        return out
    lines = ["", "=" * 78, "COMPLETE GLOSSARY - what the short terms and technical words in this report mean (all sections)", "=" * 78,
             "What this section shows: " + _GLOSSARY_SHOWS[0],
             "", "Legend: severity of a finding", "  What this table shows: " + _SEVERITY_LEGEND_ABOUT]
    lines += ["  " + x for x in table(["TERM", "MEANING"], _SEVERITY_LEGEND, [12, 100])]
    lines += ["", "Legend: result of a network check", "  What this table shows: " + _STATE_LEGEND_ABOUT]
    lines += ["  " + x for x in table(["TERM", "MEANING"], _STATE_LEGEND, [14, 100])]
    lines += ["", "Glossary: what these terms mean (complete list, all sections, sorted)",
              "  What this table shows: every short term and technical word used in this report, spelled out and explained in plain language."]
    lines += ["  " + x for x in table(["TERM", "FULL NAME", "PLAIN-LANGUAGE MEANING"], rows, [24, 44, 100])]
    return lines



def _html_table(headers, rows, about=None):
    esc = _html.escape
    head = "".join(_th(_full_header(h)) for h in headers)
    body = "".join("<tr>" + "".join("<td>%s</td>" % esc(str(c)) for c in r) + "</tr>" for r in rows)
    if not about:
        _missing_about("table", tuple(headers))
        about = _default_about(headers)
    return ((('<p class="about">What this table shows: %s</p>' % esc(about)) if about else "")
            + '<div class="tablewrap"><div class="tbtools"><input class="tfilter" type="search" placeholder="Filter rows...">'
            '<span class="tcount"></span><button class="csv" type="button">CSV</button></div>'
            '<div class="tscroll"><table class="data"><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div></div>' % (head, body))


_ACRONYMS = ("GCP", "GKE", "CPU", "IAM", "VM", "VMs", "HPA", "PVC", "DNS", "API", "VPC", "NAT", "NEG", "GCE")


def _short_title(title):
    """'3. NODES - STATUS, CPU, ...' -> 'Nodes'; 'CLUSTER OVERVIEW - prod-gke' -> 'Cluster overview'."""
    t = re.sub(r"^\d+\.\s*", "", title).split(" (")[0].split(" - ")[0].strip()
    if t.isupper():
        t = t.capitalize()
        for word in _ACRONYMS:
            t = re.sub(r"\b%s\b" % word, word, t, flags=re.I)
        t = re.sub(r"\bgoogle kubernetes engine\b", "Google Kubernetes Engine", t, flags=re.I)
    return t


def render_html(label, ctx, rep, steps_log, raw_text, timing=None):
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
    reg = {f"s{x['num']}": x for x in SECTIONS}            # report section id -> registry entry (icon, full title, description)

    def nav_title(sec):
        r = reg.get(sec["id"])
        return r["title"] if r else _short_title(sec["title"])

    def icon_of(sec):
        return ""                       # the icons are drawn by the CSS rules of _icon_css(), keyed on the section id (the headings keep their plain markup)
    nav = ['<a href="#summary" class="jump"><span>Health summary</span></a>']
    for s in rep.sections:
        if s["id"] == "s0" or not s["blocks"]:
            continue
        if s.get("skipped"):
            nav.append('<a href="#%s" class="jump skipnav"><span>%s%s</span><span class="badge b-skip">skipped</span></a>' % (s["id"], icon_of(s), esc(nav_title(s))))
            continue
        worst = next((sv for sv in ("CRIT", "HIGH", "MED", "INFO") if per_section[s["id"]][sv]), None)
        badge = f'<span class="badge b-{worst.lower()}">{sum(per_section[s["id"]].values())}</span>' if worst else ""
        nav.append('<a href="#%s" class="jump"><span>%s%s</span>%s</a>' % (s["id"], icon_of(s), esc(nav_title(s)), badge))
    nav.append('<a href="#steps" class="jump"><span>Collection steps and timing</span></a>')
    nav.append('<a href="#glossary" class="jump"><span>Glossary: all terms</span></a>')

    # --- summary
    cards = []
    for sev, cls in (("CRIT", "c-crit"), ("HIGH", "c-high"), ("MED", "c-med"), ("INFO", "c-info")):
        cards.append(f'<div class="sevcard {cls}" data-sev="{sev}" title="click to show/hide"><b>{counts[sev]}</b>{_SEV_NAME[sev]}</div>')
    rows = []
    for sev, text, sid in findings:
        where = '<a class="jump" href="#%s">%s</a>' % (sid, esc(reg[sid]["title"] if sid in reg else _short_title(sec_titles.get(sid, ""))))
        rows.append('<tr data-sev="%s"><td><span class="sevtag badge b-%s">%s</span></td><td>%s</td><td>%s</td></tr>'
                    % (sev, sev.lower(), _SEV_NAME[sev], esc(text), where))
    if findings:
        summary_html = (f'<p class="about">What this block shows: the number of findings at each severity; click a card to hide or show that severity in the table below.</p><div class="cards">{"".join(cards)}</div><p class="about">What this table shows: every problem and notable finding the tool detected, worst first; click a severity card to hide or show it.</p><div class="tablewrap"><div class="tbtools"><span class="tcount"></span></div>'
                        f'<div class="tscroll"><table class="data" id="findings"><thead><tr>{_th("SEVERITY")}{_th("FINDING")}{_th("WHERE IN THE REPORT")}</tr></thead>'
                        f'<tbody>{"".join(rows)}</tbody></table></div></div>')
        # the findings table has no per-table filter/CSV; give it the same hooks, hidden
        summary_html = summary_html.replace('<span class="tcount"></span>', '<span class="tcount"></span><input class="tfilter" style="display:none"><button class="csv" style="display:none">CSV</button>')
    else:
        summary_html = '<p class="about">What this block shows: the overall result of the run.</p><div class="cards"><div class="sevcard c-ok"><b>OK</b>No problems detected in the collected data</div></div>'

    guard = getattr(ctx, "guard", None)
    guard_text = ""
    if guard is not None:
        guard_text, blocked = guard_summary_lines(guard)
        summary_html += ('<p class="about">What this block shows: the proof that this run only read data: how many read commands were made and whether the guard had to block anything (it should say none).</p>'
                         '<div class="check st-%s"><span class="stpill">Read-only guarantee</span>%s%s</div>'
                         % ("problem" if blocked else "ok", esc(guard_text),
                            ("<ul>" + "".join(f"<li>BLOCKED {esc(t)}: {esc(c)} ({esc(r)})</li>" for t, c, r in blocked) + "</ul>") if blocked else ""))
    plan = getattr(ctx, "plan", None)
    if plan and plan["skipped"]:
        summary_html += ('<p class="about"><b>Sections not collected (skipped by choice):</b> ' + esc(", ".join(SECTION_BY_ID[i]["title"] for i in plan["skipped"]))
                         + '. The counts and findings above cover the collected sections only.</p>')
    if plan and plan["silent"]:
        summary_html += ('<p class="about"><b>Collected silently, not shown:</b> ' + esc(", ".join(SECTION_BY_ID[i]["title"] + (" (" + ", ".join(parts) + " only)" if parts else "")
                                                                                      for i, parts in plan["silent"].items()))
                         + ' - a selected section needs this data; its findings are not counted.</p>')
    contacts = contact_rows(ctx)
    if contacts:
        summary_html += ('<h3 style="margin:18px 0 6px;font-size:14px">Teams to contact - namespaces with problems, grouped by support distribution list ('
                         + esc(SUPPORT_LABEL) + ')</h3>' + _html_table(["SUPPORT DL", "NAMESPACES", "NUMBER OF ISSUES", "WHAT"], contacts,
                         "which team (support distribution list) to contact for the namespaces that have problems, and what was found there."))

    summary_html += _legend_html()

    # --- sections
    def render_block(block):
        kind = block[0]
        if kind == "lines":
            lines = block[1]
            if not any(l.strip() for l in lines):
                return ""
            spans = "".join(f'<span class="ln {_line_class(l)}">{esc(l)}</span>\n' for l in lines)
            return f'<pre class="lines">{spans}</pre>'
        if kind in ("table", "glossary"):
            if kind == "glossary":
                _, gtitle, headers, trs, about = block
            else:
                _, headers, trs, *rest = block
                gtitle, about = None, (rest[0] if rest else None)
            head = "".join(_th(h) for h in headers)
            body = "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in trs)
            intro = (f'<div class="gtitle">{esc(gtitle)}</div>' if gtitle else "") + (f'<p class="about">What this table shows: {esc(about)}</p>' if about else "")
            html_table = ('<div class="tablewrap"><div class="tbtools"><input class="tfilter" type="search" placeholder="Filter rows...">'
                          '<span class="tcount"></span><button class="csv" type="button">CSV</button></div>'
                          f'<div class="tscroll"><table class="data"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div></div>')
            return f'<div class="gloss">{intro}{html_table}</div>' if gtitle else intro + html_table
        if kind == "sub":
            _, title, about = block
            return f'<h3 class="subh">{esc(title)}</h3>' + (f'<p class="about">What this block shows: {esc(about)}</p>' if about else "")
        if kind == "status":
            _, state, evidence, meaning = block
            cls = "st-" + re.sub(r"[^a-z]", "", state.lower())
            return (f'<div class="check {cls}"><span class="stpill">{esc(state)}</span>{esc(evidence)}'
                    f'<div class="nx"><b>What this means / what to do next:</b> {esc(meaning)}</div></div>')
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
        if kind == "intro":
            return _shows_box(block[1], block[2])
        if kind == "util":
            return (f'<p class="about">What this block shows: {esc(block[2])}</p>' if len(block) > 2 and block[2] else "") + _util_block_html(block[1])
        if kind == "series":
            return (f'<p class="about">What this block shows: {esc(block[4])}</p>' if len(block) > 4 and block[4] else "") + _series_block_html(block[1], block[2], block[3])
        if kind == "timeline":
            entries = block[1]
            tl_about = f'<p class="about">What this block shows: {esc(block[2])}</p>' if len(block) > 2 and block[2] else ""
            kinds = []
            items = []
            for ts, text in entries:
                k = (re.match(r"^([A-Za-z()]+)", text) or [None, "OTHER"])[1].upper()
                if k not in kinds:
                    kinds.append(k)
                items.append(f'<li data-kind="{esc(k)}"><time>{esc(ts)}Z</time><span class="kind">{esc(k)}</span>{esc(text)}</li>')
            chips = "".join(f'<span class="chip on" data-kind="{esc(k)}">{esc(k)}</span>' for k in kinds)
            return f'{tl_about}<div class="tlfilters">{chips}</div><ul class="timeline">{"".join(items)}</ul>'
        return ""

    sections_html = []
    for s in rep.sections:
        if s["id"] == "s0" or not s["blocks"]:
            continue
        worst = next((sv for sv in ("CRIT", "HIGH", "MED", "INFO") if per_section[s["id"]][sv]), None)
        badge = f'<span class="badge b-{worst.lower()}">{sum(per_section[s["id"]].values())} finding(s)</span>' if worst else ""
        if s.get("skipped"):
            badge = '<span class="badge b-skip">skipped by choice</span>'
        body = "".join(render_block(b) for b in s["blocks"])
        sections_html.append(f'<details class="sec{" skipped" if s.get("skipped") else ""}" id="{s["id"]}" open><summary><span>{icon_of(s)}{esc(s["title"])}</span>{badge}</summary><div class="secbody">{body}</div></details>')

    run_rows = "".join(f"<tr><td>{esc(t)}</td><td>{esc(st)}</td><td>{esc(sec)}</td></tr>" for t, st, sec in steps_log)
    steps_html = (f'<details class="sec" id="steps"><summary><span>Collection steps and timing</span></summary><div class="secbody">{_shows_box(*_STEPS_SHOWS)}'
                  f'<p class="about">What this table shows: the collection steps that ran, whether each finished, and how long it took.</p>'
                  f'<div class="tablewrap"><div class="tbtools"><span class="tcount"></span><input class="tfilter" style="display:none"><button class="csv" style="display:none">CSV</button></div>'
                  f'<div class="tscroll"><table class="data"><thead><tr>{_th("STEP")}{_th("RESULT")}{_th("TIME")}</tr></thead><tbody>{run_rows}</tbody></table></div></div></div></details>')
    gl_rows = complete_glossary(rep)
    glossary_html = ('<details class="sec" id="glossary"><summary><span>Glossary: what these terms mean (all sections)</span><span class="badge b-info">%d terms</span></summary><div class="secbody">%s%s'
                     '<h3 class="subh">Complete glossary of the terms used in this report</h3><p class="about">What this block shows: every short term and technical word used anywhere in this report, written out in full and explained once, in alphabetical order.</p>%s</div></details>'
                     % (len(gl_rows), _shows_box(*_GLOSSARY_SHOWS), _legend_html(),
                        _html_table(["TERM", "FULL NAME", "PLAIN-LANGUAGE MEANING"], gl_rows, "all terms of all sections of this report, de-duplicated and sorted by term: the term, its full name and what it means in plain language.")))

    meta = [f"Context: {ctx.meta.get('context', '?')}", f"Server: {ctx.meta.get('server') or ('not collected (overview skipped)' if plan and 'overview' in plan['skipped'] else '?')}",
            f"Window: last {ctx.minutes} min", f"Generated: {ctx.now:%Y-%m-%d %H:%M:%S} UTC"]
    if timing:
        speed = timing["sum"] / timing["total"] if timing["total"] > 0 else 1.0
        meta.append(f"Collected in {timing['total']:.1f}s ({timing['workers']} worker(s)" + (f", {speed:.1f}x faster than one after another" if timing["workers"] > 1 and speed >= 1.05 else "") + ")")
    n_sec = len(plan["selected"]) if plan else len(SECTIONS)
    band = _band_html(PRODUCT_TITLE, f"{esc(label)} &nbsp;|&nbsp; last {ctx.minutes} min &nbsp;|&nbsp; {n_sec} of {len(SECTIONS)} sections collected &nbsp;|&nbsp; {esc(CLOUD_NAME)}")
    raw = raw_text.replace("</script", "<\\/script")
    foot = logo_svg(34).replace('class="logo"', 'class="logo foot-logo"')
    timing_foot = ""
    if timing:
        timing_foot = ('<div class="timing">Timing: collected in %.1fs with %d worker(s); the steps add up to %.1fs. '
                       % (timing["total"], timing["workers"], timing["sum"])
                       + esc("  ".join(f"{t}: {sec}" for t, sec in timing["steps"] if sec not in ("-", ""))) + "</div>")
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>GKE debug - {esc(label)}</title><style>{_HTML_CSS}{_UTIL_CSS}{_SERIES_CSS}{_brand_css()}{_icon_css()}</style></head><body>
{band}
<header><h1>GKE debug report - {esc(label)}</h1><div class="meta">{esc("  |  ".join(meta))}</div>
<div class="toolbar"><input id="q" type="search" placeholder="Search everything ( press / )"><span id="hits" class="small"></span>
<button id="expand" type="button">Expand all</button><button id="collapse" type="button">Collapse all</button>
<button id="theme" type="button">Dark / light</button><button id="print" type="button">Print</button><button id="dl" type="button">Download .txt</button></div></header>
<div class="layout"><nav>{"".join(nav)}</nav><main>
<details class="sec" id="summary" open><summary><span>Health summary</span><span class="badge b-info">{len(findings)} finding(s)</span></summary><div class="secbody">{_shows_box(_SUMMARY_SHOWS[0].replace("{m}", str(ctx.minutes)), _SUMMARY_SHOWS[1])}{summary_html}</div></details>
{"".join(sections_html)}{steps_html}{glossary_html}</main></div>
<footer>{foot}Generated by gke_debug.py - read-only data. Pod logs may contain sensitive information.<div>{esc(guard_text)}</div>{timing_foot}</footer>
<script type="text/plain" id="rawtext">{raw}</script><script>{_HTML_JS}</script><script>{_UTIL_JS}</script><script>{_SERIES_JS}</script></body></html>"""
    return page


# ---------------------------------------------------------------------------
# Orchestration (step by step, with progress + cancel)
# ---------------------------------------------------------------------------

_STEP_TITLES = {
    "overview": "Cluster overview", "data": "Collect cluster data", "gcp": "GCP GKE details", "nodes": "Nodes: CPU / memory / disk / swap",
    "utilization": "Resource utilization by namespace", "nodepods": "Pods on each node", "namespaces": "Namespaces: pods used vs configured",
    "pods": "Unhealthy pods", "events": "Events", "workloads": "Workloads", "netdetail": "Network & traffic in the window",
    "network": "Autoscaling, storage, network", "top": "Top consumers", "logs": "Pod logs", "timeline": "Timeline"}


def run_steps(options=None):
    """The ordered collection steps that run for `options`: (key, title, optional_option_name_or_None). Without a section choice
    that is every step; unticked sections are left out (the data step is always there: every section reads what it fetches)."""
    opt_of = {"gcp": "gcp", "logs": "logs"}
    plan = resolve_sections(options)
    on = {SECTION_BY_ID[i]["step"] for i in plan["selected"]} | {SECTION_BY_ID[i]["step"] for i in plan["silent"]}
    return [(k, _STEP_TITLES[k], opt_of.get(k)) for k in STEP_ORDER if k == "data" or k in on or not options]


def _commit_order(selected_steps):
    """The report order of what is committed: collected steps and the 'skipped by choice' stubs, each in its place."""
    order = []
    for key in STEP_ORDER:
        if key == "data" or key in selected_steps:
            order.append(key)
        else:
            order.append(("skip", key))
    if ("skip", "overview") in order:                      # the data lines would otherwise land inside the skipped overview
        order.remove(("skip", "overview"))
        order.insert(order.index("data") + 1, ("skip", "overview"))
    return order


def run_debug(label, minutes, emit, progress=None, cancel=None, options=None, on_finding=None):
    """Collect everything for the CURRENT kubectl context and write the .txt and the
    interactive .html report. `progress(key, status, seconds)` is called for every step
    (status: running / done / failed / skipped); `cancel` is a threading.Event - when it is
    set the pending steps are cancelled and a partial report is still written.
    The collection runs as tasks in parallel (options['workers'], default PARALLEL_WORKERS; 1 = one after another): every task writes into its
    own sub-report and the results are merged in the fixed report order, so the report is the same whatever finishes first.
    options['sections'] / ['skip_sections'] choose the report sections (see SECTIONS); unticked ones are never collected.
    Returns the path of the HTML report."""
    options = {"gcp": GCP_OPTS["enabled"], "logs": True, "all_logs": False, "log_namespaces": "", **(options or {})}
    GCP_OPTS["enabled"] = bool(options["gcp"])
    plan = resolve_sections(options)
    workers = int(options.get("workers") or PARALLEL_WORKERS)
    ctx = Ctx(minutes)
    rep = Report(emit)
    rep.minutes = minutes
    ctx.report, ctx.on_finding, ctx.cancel = rep, on_finding, cancel
    ctx.plan = plan
    g0 = GUARD.snapshot()
    if KUBE_CONTEXT:
        ctx.meta["context"] = KUBE_CONTEXT          # the report header knows the context even when the overview section is not collected
    note = progress or (lambda *a, **k: None)
    selected_steps = {SECTION_BY_ID[i]["step"] for i in plan["selected"]}
    silent_steps = {SECTION_BY_ID[i]["step"]: parts for i, parts in plan["silent"].items()}
    need_usage = bool(set(plan["selected"]) & USAGE_SECTIONS)
    runs = [k for k in STEP_ORDER if k == "data" or k in selected_steps or k in silent_steps]
    num_of = {s["step"]: s["num"] for s in SECTIONS}
    gcp_only = silent_steps.get("gcp") if "gcp" not in selected_steps else None

    runners = {
        "overview": lambda r: section_overview(r, ctx, label),
        "data": lambda r: load_data(ctx, r, usage=need_usage),
        "gcp": lambda r: section_gcp(r, ctx, label, only=gcp_only),
        "nodes": lambda r: section_nodes(r, ctx),
        "utilization": lambda r: section_utilization(r, ctx),
        "nodepods": lambda r: section_node_pods(r, ctx),
        "namespaces": lambda r: section_namespaces(r, ctx),
        "pods": lambda r: section_pods(r, ctx),
        "events": lambda r: section_events(r, ctx),
        "workloads": lambda r: section_workloads(r, ctx),
        "netdetail": lambda r: section_network_details(r, ctx, label),
        "network": lambda r: section_scaling_storage_network(r, ctx),
        "top": lambda r: section_top(r, ctx),
        "logs": lambda r: section_logs(r, ctx, options),
        "timeline": lambda r: section_timeline(r, ctx),
    }
    deps = {k: [] for k in runs}
    for sec in SECTIONS:
        if sec["step"] in deps and sec["step"] != "overview":
            deps[sec["step"]] = ["data"] + [SECTION_BY_ID[n]["step"] for n, _p in sec["needs"] if SECTION_BY_ID[n]["step"] in deps] \
                + [SECTION_BY_ID[n]["step"] for n in sec.get("after", []) if SECTION_BY_ID[n]["step"] in deps]
    if "timeline" in deps:
        deps["timeline"] = [k for k in runs if k != "timeline"]
    weight = {s["step"]: s["weight"] for s in SECTIONS}
    weight["data"] = 20

    results, fatal = {}, []
    t_start = time.time()

    def execute(key):
        silent = key not in selected_steps and key != "data"
        title = _STEP_TITLES[key]
        if silent:
            kid = rep.child(buffered=True)
            kid.emit = lambda *_a: None
        else:
            kid = rep.child(buffered=run.parallel(), section_id=f"s{num_of[key]}" if key in num_of else None)
        sink = _Sink(silent=silent)
        note(key, "running", None)
        t0 = time.time()
        status, err = "done", None
        saved = (getattr(_TLS, "rep", None), getattr(_TLS, "sink", None))
        _TLS.rep, _TLS.sink = kid, sink
        try:
            runners[key](kid)
        except Exception as exc:  # one broken step must not lose the rest
            err, status = exc, "failed"
            kid.add(f"[!] step '{title}' failed: {exc}")
        finally:
            _TLS.rep, _TLS.sink = saved
        secs = time.time() - t0
        results[key] = {"status": status, "secs": secs, "rep": kid, "sink": sink, "silent": silent, "err": err}
        note(key, status, secs)
        run.task_done()
        if key == "data" and status == "failed":     # nothing else can work without the cluster data
            fatal.append(err)

    steps_log = []
    stopped = {"flag": False}
    order = _commit_order(selected_steps)
    state = {"i": 0}

    def commit_ready():
        """Merge finished tasks into the report, strictly in report order."""
        while state["i"] < len(order):
            item = order[state["i"]]
            if isinstance(item, tuple):                              # a section that was not ticked
                sec = SECTION_BY_STEP[item[1]]
                rep.skipped(sec["num"], sec["title"])
                note(item[1], "skipped", None)
                steps_log.append((_STEP_TITLES[item[1]], "skipped (not selected)", "-"))
            else:
                res = results.get(item)
                if res is None:
                    return
                if res["status"] == "skipped":                       # Stop: never started
                    if not stopped["flag"]:
                        rep.add("")
                        rep.add("Stopped by user - the remaining steps were skipped. The report below is partial.")
                    stopped["flag"] = True
                    note(item, "skipped", None)
                    steps_log.append((_STEP_TITLES[item], "skipped (stopped)", "-"))
                else:
                    rep.merge(res["rep"])
                    ctx.commit(res["sink"])
                    steps_log.append((_STEP_TITLES[item], res["status"], f"{res['secs']:.1f}s"))
            state["i"] += 1

    def tidy_silent():
        for key in runs:
            res = results.get(key)
            if res and res["silent"] and not res.get("logged"):
                res["logged"] = True
                steps_log.append((_STEP_TITLES[key] + " (collected silently, not shown)", res["status"], f"{res['secs']:.1f}s"))

    run = _Run(workers, cancel, options.get("task_progress"))
    with _limits(run):
        run.task_added(len(runs))
        if not run.parallel():
            for key in runs:
                if run.stopped() or fatal:
                    results[key] = {"status": "skipped", "secs": 0, "rep": None, "sink": None, "silent": False, "err": None}
                else:
                    execute(key)
                commit_ready()
        else:
            pool = run.pool("section")
            pending, running = list(runs), {}
            while pending or running:
                if (run.stopped() or fatal) and pending:
                    for key in pending:                               # Stop: what has not started is cancelled at once
                        results[key] = {"status": "skipped", "secs": 0, "rep": None, "sink": None, "silent": False, "err": None}
                    pending = []
                    commit_ready()
                ready = [k for k in pending if all(d in results for d in deps[k])
                         and (k != "timeline" or all(d in results and (results[d]["silent"] or results[d]["status"] == "skipped" or order.index(d) < state["i"])
                                                   for d in deps[k]))]
                for key in sorted(ready, key=lambda k: -weight.get(k, 0)):
                    pending.remove(key)
                    running[pool.submit(execute, key)] = key
                if not running:
                    if pending and not ready:                          # nothing can start (a dependency was skipped): finish them as skipped
                        for key in pending:
                            results[key] = {"status": "skipped", "secs": 0, "rep": None, "sink": None, "silent": False, "err": None}
                        pending = []
                        commit_ready()
                    continue
                done_set, _ = wait(list(running), timeout=0.1, return_when=FIRST_COMPLETED)
                for fut in done_set:
                    running.pop(fut)
                    try:
                        fut.result()
                    except Exception as exc:                          # execute() never raises; this is a safety net
                        fatal.append(exc)
                commit_ready()
        commit_ready()
    tidy_silent()
    if fatal:
        raise fatal[0]
    total = time.time() - t_start
    timing = {"total": total, "workers": run.workers, "sum": sum(r["secs"] for r in results.values()),
              "steps": [(t, s) for t, _st, s in steps_log if s not in ("-", "")], "calls": dict(run.calls), "peak": dict(run.peak)}
    ctx.timing = timing

    ctx.guard = GUARD.since(g0)                     # (read calls made, [blocked attempts]) during this run
    if ctx.guard[1]:                                # a blocked command is a bug of the tool: say it loudly
        ctx.find("CRIT", f"READ-ONLY GUARD blocked {len(ctx.guard[1])} command(s) the tool tried to run (nothing was executed): "
                         + "; ".join(f"{t} {c[:60]}" for t, c, _r in ctx.guard[1][:3]))
    summary = build_summary(ctx, label)
    rep.add("")
    for line in summary:
        rep.add(line)
    raw_text = "\n".join(summary + [""] + rep.lines[: len(rep.lines) - len(summary) - 1] + glossary_text_lines(complete_glossary(rep)))

    note("report", "running", None)
    os.makedirs(REPORT_DIR, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", label)[:60]
    base0 = os.path.abspath(os.path.join(REPORT_DIR, f"gke_debug_{safe}_{datetime.now():%Y%m%d_%H%M%S}"))
    base, n = base0, 1
    while os.path.exists(base + ".html") or os.path.exists(base + ".txt"):   # never overwrite an earlier report
        n += 1
        base = f"{base0}_{n}"
    with open(base + ".txt", "w", encoding="utf-8") as f:
        f.write(raw_text)
    with open(base + ".html", "w", encoding="utf-8") as f:
        f.write(render_html(label, ctx, rep, steps_log, raw_text, timing))
    note("report", "done", None)
    emit("")
    emit(f"Report saved to: {base}.txt")
    emit(f"Interactive HTML report: {base}.html")
    for line in timing_lines(timing):
        emit(line)
    result = ReportPath(base + ".html")
    result.txt = base + ".txt"
    result.counts = Counter(f[0] for f in ctx.findings_full)
    result.findings = list(ctx.findings_full)
    result.section_titles = {sec["id"]: sec["title"] for sec in rep.sections}
    result.partial = stopped["flag"]
    result.meta = dict(ctx.meta)
    result.plan = plan
    result.timing = timing
    return result


def find_context(label):
    """Pick the kubectl context that belongs to the selected cluster. Returns
    (context_name_or_None, [candidates], current_context). Matching looks at the context
    name and at the cluster it points to (GKE contexts are gke_<project>_<location>_<cluster>, or
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
            if text.startswith("gke_"):
                tail = text.split("_")[-1]
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
    # The gcloud listing (CLI method, or the custom login with 'all clusters') knows every cluster's project / location
    tgt = CLI_TARGETS.get(str(cluster_number)) if (cli or CLI_TARGETS) else None
    ctx_label = label
    if not skip_login:
        note("login", "running", None)
        t0 = time.time()
        use_cli = cli or (tgt is not None and not tgt.get("exe_number"))     # a cluster that is not in the gkelogin menu cannot use the exe
        if use_cli:
            if cli:
                emit(f"Logging in to cluster {cluster_number} ({label}) with the Google Cloud CLI ...")
            else:
                emit(f"Cluster {cluster_number} ({label}) is not in the gkelogin menu - logging in with the Google Cloud CLI "
                     "(gcloud container clusters get-credentials) instead ...")
            try:
                ctx = cli_login(cluster_number, label, emit)
            except RuntimeError:
                note("login", "failed", time.time() - t0)
                raise
            context = context or ctx
            tgt = CLI_TARGETS.get(str(cluster_number))
        else:
            exe_no = tgt["exe_number"] if tgt else cluster_number
            emit(f"Logging in to cluster {cluster_number} ({label}) with gkelogin" + (f" (menu number {exe_no})" if tgt else "") + " ...")
            if not gkelogin(exe_no):
                note("login", "failed", time.time() - t0)
                raise RuntimeError(f"gkelogin failed for cluster {cluster_number}")
            if tgt:
                ctx_label = tgt["name"]
        emit("Login OK.")
        note("login", "done", time.time() - t0)
    else:
        note("login", "skipped", None)
        if tgt:
            ctx_label = tgt["name"]
    saved = {k: GCP_OPTS[k] for k in ("cluster", "location", "project")}
    if tgt and tgt.get("project"):   # the listing already knows the cluster: hand it to the existing GCP project step so nothing is guessed
        GCP_OPTS.update(cluster=tgt["name"], location=tgt["location"], project=tgt["project"])
    try:
        note("context", "running", None)
        t0 = time.time()
        select_context(ctx_label, emit, forced=context)
        note("context", "done", time.time() - t0)
        plan = resolve_sections(options)
        if (options or {}).get("gcp", GCP_OPTS["enabled"]) and GCP_OPTS["enabled"] and ("gcp" in plan["selected"] or "gcp" in plan["silent"]):
            note("profile", "running", None)
            t0 = time.time()
            try:
                select_gcp_project(label, emit, preferred=GCP_OPTS["project"])
            except Exception as exc:  # never block the kubectl data because of GCP project trouble
                emit(f"WARNING: could not choose a GCP project: {exc}")
            note("profile", "done", time.time() - t0)
        else:
            note("profile", "skipped", None)
        return run_debug(label, minutes, emit, progress, cancel, options, on_finding)
    finally:
        GCP_OPTS.update(saved)


class ReportPath(str):
    """The path of a cluster's HTML report. It also carries what the run found, which the
    multi-cluster summary page is built from."""
    txt = None
    counts = None
    findings = None
    section_titles = None
    partial = False
    meta = None
    plan = None          # {"selected": [...], "skipped": [...], "silent": {...}}: which sections this report holds
    timing = None        # {"total": s, "workers": n, "sum": s, "steps": [...]}


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
    """Run the whole debug for several clusters, ONE AFTER ANOTHER (gkelogin, the kubectl context
    and the GCP project are shared state, so they must not overlap). Every cluster gets its own
    .txt and .html report; with more than one cluster a summary page linking them is written too.
    A failing cluster is recorded and the next one still runs. `cancel` stops after the current step
    of the current cluster, and the remaining clusters are marked 'not run'.
    Returns {"items": [per-cluster dicts], "index": path of the summary page or None}."""
    notify = on_cluster or (lambda *a, **k: None)
    n = len(selected)
    if n > 1 and context:
        emit("NOTE: --context applies to a single cluster and is ignored when several are selected.")
    entries = []
    expired_run = None
    for i, (number, label) in enumerate(selected, start=1):
        entry = {"number": number, "label": label, "status": "not run", "html": None, "txt": None,
                 "counts": Counter(), "findings": [], "titles": {}, "secs": None, "error": None, "plan": None}
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
        if expired_run and LOGIN_OPTS["method"] == "cli":          # the account's sign-in ended: do not ask for it again for every cluster
            entry.update(status="credentials expired", error=f"Credentials for {expired_run} expired - sign in again.", secs=0)
            emit(f"Cluster {label}: skipped - credentials for {expired_run} expired. Sign in again and run it again.")
            notify(i, n, label, entry["status"], entry)
            continue
        try:
            result = login_and_debug(number, label, minutes, emit, skip_login, context if n == 1 else None,
                                     progress, cancel, options, finding_cb)
            entry.update(status="stopped (partial)" if getattr(result, "partial", False) else "ok",
                         html=str(result), txt=getattr(result, "txt", None), counts=getattr(result, "counts", Counter()),
                         findings=getattr(result, "findings", []), titles=getattr(result, "section_titles", {}), plan=getattr(result, "plan", None))
        except Exception as exc:
            acct_now = LOGIN_OPTS.get("account") or AUTH_STATE.get("last")
            if is_expired_error(str(exc)) or (acct_now and acct_now in AUTH_STATE["expired"] and "not logged in" in str(exc)):
                expired_run = acct_now or "the account"
                mark_expired(acct_now, str(exc))
                entry.update(status="credentials expired", error=f"Credentials for {expired_run} expired - sign in again.")
                emit(f"ERROR on cluster {label}: credentials for {expired_run} expired - sign in again.")
            else:
                entry.update(status="failed", error=mask_tokens(exc))
                emit(f"ERROR on cluster {label}: {mask_tokens(exc)}")
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
    base0 = os.path.abspath(os.path.join(REPORT_DIR, f"gke_debug_summary_{datetime.now():%Y%m%d_%H%M%S}"))
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
        plan = en.get("plan")
        if plan:        # a cluster may have been collected with only some sections: say which, so a missing finding is not read as 'all fine'
            sections_cell = f'{len(plan["selected"])} of {len(SECTIONS)}' + (f' (skipped: {", ".join(SECTION_BY_ID[i]["title"] for i in plan["skipped"])})' if plan["skipped"] else "")
        else:
            sections_cell = "-"
        rows.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                    % (esc(en["label"]), esc(shown), c["CRIT"], c["HIGH"], c["MED"], c["INFO"],
                       ("%.0fs" % en["secs"]) if en["secs"] else "-", esc(sections_cell), esc(top), report))
        for sev, text, sid in ordered:
            where = ('<a href="%s#%s">%s</a>' % (esc(link), esc(sid), esc(_short_title(en["titles"].get(sid, sid))))) if link else "-"
            finding_rows.append('<tr data-sev="%s"><td><span class="sevtag badge b-%s">%s</span></td><td>%s</td><td>%s</td><td>%s</td></tr>'
                                % (sev, sev.lower(), _SEV_NAME[sev], esc(en["label"]), esc(text), where))
        raw.append(f"== {en['label']}: {shown}  CRIT {c['CRIT']}  HIGH {c['HIGH']}  MED {c['MED']}  INFO {c['INFO']}")
        raw += [f"   [{sev}] {text}" for sev, text, _ in ordered]
        if en["error"]:
            raw.append(f"   ERROR: {en['error']}")
    cards = "".join('<div class="sevcard %s" data-sev="%s" title="click to show/hide"><b>%d</b>%s</div>' % (cls, sev, total[sev], _SEV_NAME[sev])
                    for sev, cls in (("CRIT", "c-crit"), ("HIGH", "c-high"), ("MED", "c-med"), ("INFO", "c-info")))
    stamp = f"{datetime.now():%Y-%m-%d %H:%M:%S}"
    clusters_about = "one row per cluster of this run: its result, the number of findings at each severity, the time it took, the sections collected, its top finding and a link to its full report."
    findings_about = "every finding of every cluster, worst first, with the cluster it belongs to and a link to the section of that cluster's report."
    clusters_table = ('<p class="about">What this table shows: ' + esc(clusters_about) + '</p><div class="tablewrap"><div class="tbtools"><input class="tfilter" type="search" placeholder="Filter clusters...">'
                      '<span class="tcount"></span><button class="csv" type="button">CSV</button></div><div class="tscroll">'
                      '<table class="data"><thead><tr><th title="The cluster name.">Cluster</th><th title="OK = collected, FAILED = the collection did not finish.">Status</th><th title="Number of Critical findings.">Critical</th><th title="Number of High findings.">High</th><th title="Number of Medium findings.">Medium</th><th title="Number of Information findings.">Information</th>'
                      '<th title="Seconds the collection of this cluster took.">Time</th><th title="How many of the report sections were collected for this cluster.">Sections collected</th><th title="The most serious finding of the cluster.">Top finding</th><th title="Links to the full report of the cluster.">Report</th></tr></thead><tbody>%s</tbody></table></div></div>' % "".join(rows))
    findings_table = ('<p class="about">What this table shows: ' + esc(findings_about) + '</p><div class="tablewrap"><div class="tbtools"><span class="tcount"></span><input class="tfilter" style="display:none">'
                      '<button class="csv" style="display:none">CSV</button></div><div class="tscroll"><table class="data" id="findings"><thead>'
                      '<tr><th>Severity</th><th>Cluster</th><th>Finding</th><th>Where</th></tr></thead><tbody>%s</tbody></table></div></div>'
                      % ("".join(finding_rows) or '<tr data-sev="INFO"><td></td><td></td><td>No findings</td><td></td></tr>'))
    raw_text = "\n".join([f"GKE DEBUG - {len(entries)} clusters, last {minutes} min, {stamp}", ""] + raw).replace("</script", "<\\/script")
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>GKE debug - {len(entries)} clusters</title><style>{_HTML_CSS}{_brand_css()}</style></head><body>
{_band_html(PRODUCT_TITLE, f"summary of {len(entries)} clusters &nbsp;|&nbsp; last {minutes} min &nbsp;|&nbsp; {esc(CLOUD_NAME)}")}
<header><h1>GKE debug - summary of {len(entries)} clusters</h1><div class="meta">Window: last {minutes} min  |  Generated: {stamp}</div>
<div class="toolbar"><input id="q" type="search" placeholder="Search everything ( press / )"><span id="hits" class="small"></span>
<button id="expand" type="button">Expand all</button><button id="collapse" type="button">Collapse all</button>
<button id="theme" type="button">Dark / light</button><button id="print" type="button">Print</button><button id="dl" type="button">Download .txt</button></div></header>
<div class="layout"><nav><a href="#clusters" class="jump">Clusters</a><a href="#allfindings" class="jump">All findings</a></nav><main>
<details class="sec" id="clusters" open><summary><span>Clusters</span><span class="badge b-info">{len(entries)}</span></summary><div class="secbody">{_shows_box(*_INDEX_SHOWS["clusters"])}{clusters_table}</div></details>
<details class="sec" id="allfindings" open><summary><span>All findings (every cluster)</span><span class="badge b-info">{sum(total.values())}</span></summary><div class="secbody">{_shows_box(*_INDEX_SHOWS["allfindings"])}<p class="about">What this block shows: the number of findings of all clusters together at each severity; click a card to hide or show it.</p><div class="cards">{cards}</div>{findings_table}{_legend_html()}</div></details>
</main></div><footer>Generated by gke_debug.py - read-only data. The per-cluster reports are the files linked above (keep them in the same folder).</footer>
<script type="text/plain" id="rawtext">{raw_text}</script><script>{_HTML_JS}</script></body></html>"""


# ---------------------------------------------------------------------------
# GUI: live, interactive collection
# ---------------------------------------------------------------------------

_GUI = {}   # widgets of the running window (used by the tests)
SECTION_CHOICE = {"sections": None, "skip_sections": None}     # the command-line section choice (--sections / --only-networking ...); the window starts with it


def run_gui(default_minutes, skip_login=False, context=None):
    import pathlib
    import tkinter as tk
    import webbrowser
    from tkinter import ttk, scrolledtext

    if _SESSION.get("signin") in SIGNIN_METHOD_LABELS:
        LOGIN_OPTS["signin"] = _SESSION["signin"]            # the sign-in method chosen earlier in this session
    root = tk.Tk()
    root.title(PRODUCT_TITLE)
    root.geometry("1180x840")          # comfortable at 1100 x 700 and above; everything resizes with the window
    root.minsize(980, 620)
    msgs = queue.Queue()
    state = {"busy": False, "cancel": None, "html": None, "t0": None, "finished": 0, "total": 1,
             "counts": Counter(), "clusters": {}, "reports": {}, "n": 1,
             "auth": {"state": "unchecked"}, "checking": False, "signing": False, "auth_for": None, "recheck": None,
             "accounts": [], "acct_by_id": {}, "acct_chosen": set(), "acct_loading": False, "accounts_loaded": False,
             "acct_locked": False, "cl_locked": False, "pre": False, "acct_status": {}, "acct_rows": [], "acct_map": {}, "acct_gen": 0,
             "crows": [], "by_key": {}, "cchosen": set(), "listing": False, "list_cancel": None, "listed": set(),
             "list_done": 0, "list_total": 0, "rebuild": False, "said": [], "menu": {}, "src_user": False, "src_fallback": False}
    ICON = {"pending": "o", "running": ">>", "done": "OK", "failed": "FAILED", "skipped": "-"}
    COLORS = {"ok": "#067647", "err": "#c00000", "warn": "#9a7d0a", "info": "#1f4e79", "dim": "#777777"}
    NEED_SIGNIN = True      # the list of projects itself needs the sign-in (AWS profiles are read from ~/.aws)
    PER_ACCOUNT = False      # the sign-in belongs to the chosen project (AWS profile)
    ACCOUNT0 = GCP_OPTS["project"]   # the project given on the command line, if any
    try:                    # the magnifier needs a Tk that can show characters above U+FFFF
        tk.Label(root, text="\U0001F50D").destroy()
        SEARCH_ICON = "\U0001F50D"
    except Exception:
        SEARCH_ICON = "Find:"

    try:                    # emoji (above U+FFFF) need a Tk that can show them; otherwise the plain symbols of SYMBOL_FALLBACK are used
        tk.Label(root, text="\U0001F5A5").destroy()
        WIDE = True
    except Exception:
        WIDE = False

    def sym(ch):
        return ch if (WIDE or ord(ch[0]) <= 0xFFFF) else SYMBOL_FALLBACK.get(ch, "\u25CF")

    FONT = "Segoe UI"
    PAGE, CARD, LINE, MUTED, STRIPE, SOFTCLOUD = "#EEF3FB", "#FFFFFF", "#DADCE0", "#5F6368", "#F3F7FE", "#E1EAFB"
    root.configure(bg=PAGE)
    style = ttk.Style(root)
    try:
        style.theme_use("clam")       # the one built-in theme that lets every colour be set
    except tk.TclError:
        pass
    style.configure(".", font=(FONT, 10), background=CARD, foreground="#202124")
    style.configure("Page.TFrame", background=PAGE)
    style.configure("Bar.TLabel", background=PAGE)
    style.configure("Bar.TCheckbutton", background=PAGE)
    style.configure("Muted.TLabel", foreground=MUTED, font=(FONT, 9))
    style.configure("TLabelframe", background=CARD, bordercolor=LINE, relief="solid", borderwidth=1)
    style.configure("TLabelframe.Label", background=CARD, foreground=BRAND_DARK, font=(FONT, 10, "bold"))
    style.configure("TButton", padding=(10, 5), background="#F1F3F4", bordercolor=LINE, focuscolor=CARD)
    style.map("TButton", background=[("active", BRAND_SOFT), ("disabled", "#F1F3F4")], foreground=[("disabled", "#9AA0A6")])
    for name, base, hover in (("Accent", BRAND_PRIMARY, "#3367D6"), ("Run", BRAND_ACCENT, "#1E8E3E"), ("Stop", BRAND_RED, "#C5221F")):
        style.configure(name + ".TButton", background=base, foreground="#FFFFFF", font=(FONT, 10, "bold"), bordercolor=base, padding=(14, 6))
        style.map(name + ".TButton", background=[("active", hover), ("disabled", "#CFD3D8")], foreground=[("disabled", "#FFFFFF")])
    style.configure("Treeview", rowheight=24, background=CARD, fieldbackground=CARD, bordercolor=LINE)
    style.configure("Treeview.Heading", font=(FONT, 10, "bold"), background=BRAND_SOFT, foreground=BRAND_DARK, relief="flat")
    try:                    # Tk 8.6.9 ignores the colours of tagged tree rows unless the default state map is filtered like this
        def fixed_map(option):
            return [e for e in style.map("Treeview", query_opt=option) if e[:2] != ("!disabled", "!selected")]
        style.map("Treeview", foreground=fixed_map("foreground") + [("selected", "#FFFFFF")], background=fixed_map("background") + [("selected", BRAND_PRIMARY)])
    except Exception:
        pass
    style.configure("TNotebook", background=PAGE, borderwidth=0)
    style.configure("TNotebook.Tab", padding=(16, 7), font=(FONT, 10, "bold"), background="#DCE6F8", foreground=BRAND_DARK)
    style.map("TNotebook.Tab", background=[("selected", CARD)], foreground=[("selected", BRAND_PRIMARY)])
    style.configure("Horizontal.TProgressbar", troughcolor=LINE, background=BRAND_ACCENT, bordercolor=LINE, thickness=10)
    style.configure("Sec.TCheckbutton", font=(FONT, 10, "bold"))
    S_ICO = {"login": sym("\U0001F510"), "cloud": "\u2601", "globe": sym("\U0001F310"), "helm": "\u2388", "run": "\u25B6", "stop": "\u23F9", "ok": "\u2714", "bad": "\u2716",
             "warn": "\u26A0", "doc": sym("\U0001F4C4"), "refresh": sym("\U0001F504"), "node": sym("\U0001F5A5"), "find": sym("\U0001F50D"), "check": "\u2611"}
    STATUS_MARK = {"pending": "\u25CB", "running": "\u25B6", "done": "\u2714", "failed": "\u2716", "skipped": "\u2013"}
    STEP_ICON = {"login": S_ICO["login"], "context": S_ICO["helm"], "profile": S_ICO["cloud"], "data": S_ICO["find"], "report": S_ICO["doc"]}
    STEP_ICON.update({sec["step"]: sym(sec["icon"]) for sec in SECTIONS})

    def card(parent, text, color, padding=6):
        """A card: a titled panel with a coloured stripe on its left edge. Returns (outer frame to pack / add, inner frame to fill)."""
        outer = tk.Frame(parent, bg=color)
        inner = ttk.LabelFrame(outer, text=text, padding=padding)
        inner.pack(fill="both", expand=True, padx=(4, 0))
        return outer, inner

    def autowrap(label, pad=18):
        """A label whose text wraps at its own width (so the window resizes cleanly)."""
        label.bind("<Configure>", lambda e: label.configure(wraplength=max(160, e.width - pad)))
        return label

    # ---- banner: the Google Cloud logo (drawn from canvas shapes), the title, a subtitle, soft decorative clouds
    banner = tk.Canvas(root, height=92, highlightthickness=0, bg=BRAND_HEADER_BG)
    banner.pack(fill="x")
    banner_sub = tk.StringVar(value="")

    def draw_banner(_event=None):
        w = max(banner.winfo_width(), 700)
        banner.delete("all")
        for cx, cy, k in ((w - 110, 26, 1.3), (w - 330, 62, .85), (w - 520, 18, .6), (w - 720, 58, .5)):       # decoration only: soft, low contrast, behind the text
            for dx, dy, r in ((-20, 8, 14), (0, 0, 19), (22, 4, 16)):
                banner.create_oval(cx + (dx - r) * k, cy + (dy - r) * k, cx + (dx + r) * k, cy + (dy + r) * k, fill=SOFTCLOUD, outline="", tags="deco")
            banner.create_rectangle(cx - 20 * k, cy + 4 * k, cx + 22 * k, cy + 20 * k, fill=SOFTCLOUD, outline="", tags="deco")
        seg = w / 4.0
        for i, col in enumerate(BRAND_COLORS):
            banner.create_rectangle(i * seg, 88, (i + 1) * seg, 92, fill=col, outline="", tags="bar")
        draw_logo(banner, 18, 12, size=104, bg="#FFFFFF")
        banner.create_text(140, 34, anchor="w", text=PRODUCT_TITLE, font=(FONT, 20, "bold"), fill=BRAND_DARK, tags="title")
        banner.create_text(142, 64, anchor="w", text=banner_sub.get(), font=(FONT, 10), fill=MUTED, tags="subtitle")
    banner.bind("<Configure>", draw_banner)
    banner_sub.trace_add("write", lambda *_: draw_banner())

    # ---- layout: the status bar and the action bar are packed first (bottom), the notebook takes the rest
    statusbar = ttk.Frame(root, style="Page.TFrame", padding=(10, 5))
    statusbar.pack(side="bottom", fill="x")
    actionbar = ttk.Frame(root, style="Page.TFrame")
    actionbar.pack(side="bottom", fill="x")
    nb = ttk.Notebook(root)
    nb.pack(fill="both", expand=True, padx=8, pady=(8, 0))
    tab1 = ttk.Frame(nb, style="Page.TFrame", padding=2)
    tab2 = ttk.Frame(nb, style="Page.TFrame", padding=2)
    tab3 = ttk.Frame(nb, style="Page.TFrame", padding=2)
    nb.add(tab1, text=f" {S_ICO['login']}  1  Sign in and choose clusters ")
    nb.add(tab2, text=f" {S_ICO['check']}  2  What to collect ")
    nb.add(tab3, text=f" {S_ICO['run']}  3  Live run ")

    def numeric(k):
        return int(k) if str(k).isdigit() else 10**9

    def count_of(n, noun):
        return f"{n} {noun}" + ("" if n == 1 else "s")

    # ---- guide: steps 1 - 4 (login method, sign in, choose project, choose clusters) with a message line
    # the page scrolls when the window is too small for steps 1-4 plus the sign-in details (small screens)
    guide_canvas = tk.Canvas(tab1, highlightthickness=0, bg=PAGE)
    guide_scroll = ttk.Scrollbar(tab1, orient="vertical", command=guide_canvas.yview)
    guide_canvas.configure(yscrollcommand=guide_scroll.set)
    guide_scroll.pack(side="right", fill="y")
    guide_canvas.pack(side="left", fill="both", expand=True)
    guide = ttk.Frame(guide_canvas, style="Page.TFrame", padding=(6, 4, 6, 0))
    guide_win = guide_canvas.create_window(0, 0, window=guide, anchor="nw")

    def fit_guide(_event=None):
        w, h = max(1, guide_canvas.winfo_width()), max(1, guide_canvas.winfo_height())
        need = guide.winfo_reqheight()
        guide_canvas.itemconfigure(guide_win, width=w, height=max(h, need))
        guide_canvas.configure(scrollregion=(0, 0, w, max(h, need)))
        if need <= h:
            guide_scroll.pack_forget()
        elif not guide_scroll.winfo_ismapped():
            guide_scroll.pack(side="right", fill="y", before=guide_canvas)
    guide_canvas.bind("<Configure>", fit_guide)
    guide.bind("<Configure>", fit_guide)

    def guide_wheel(event):
        under = root.winfo_containing(event.x_root, event.y_root)
        if under is None or under.winfo_class() in ("Treeview", "Text", "Listbox", "TCombobox", "Entry", "TEntry") or not str(under).startswith(str(tab1)):
            return
        if guide_scroll.winfo_ismapped():
            guide_canvas.yview_scroll(int(-event.delta / 120), "units")
    root.bind_all("<MouseWheel>", guide_wheel, add="+")

    def scroll_to(widget):
        """Bring a widget of the guide into view (the sign-in details when the link appears)."""
        try:
            root.update_idletasks()
            need = max(1, guide.winfo_reqheight())
            if guide_scroll.winfo_ismapped():
                guide_canvas.yview_moveto(max(0.0, min(1.0, (widget.winfo_y() - 8) / need)))
        except Exception:
            pass
    row12 = ttk.Frame(guide, style="Page.TFrame")
    row12.pack(fill="x")
    s1_o, s1 = card(row12, f"{S_ICO['login']}  Step 1 - Login method", BRAND_COLORS[0])
    s1_o.pack(side="left", fill="y")
    method_combo = ttk.Combobox(s1, width=30, state="readonly", values=[LOGIN_LABELS["exe"], LOGIN_LABELS["cli"]])
    method_combo.set(LOGIN_LABELS[LOGIN_OPTS["method"]])
    method_combo.pack(anchor="w")
    method_info = tk.StringVar(value="")
    ttk.Label(s1, textvariable=method_info, wraplength=330, justify="left", style="Muted.TLabel").pack(anchor="w", pady=(4, 0))
    ttk.Label(s1, text="Read-only guarantee: this tool only reads. It installs, creates, changes and deletes nothing on the cluster or in the cloud.",
              style="Muted.TLabel", wraplength=330, justify="left", foreground=BRAND_ACCENT).pack(anchor="w", pady=(4, 0))
    src_var = tk.StringVar(value="menu")     # which clusters the list (step 4) shows (custom login only): the gkelogin menu (instant) or gcloud, on demand
    ttk.Label(s1, text="Cluster list:").pack(anchor="w", pady=(6, 0))
    src_all_rb = ttk.Radiobutton(s1, text="Collect clusters with gcloud from selected projects (on demand)", value="all", variable=src_var)
    src_all_rb.pack(anchor="w")
    src_menu_rb = ttk.Radiobutton(s1, text="Clusters from the gkelogin menu (instant)", value="menu", variable=src_var)
    src_menu_rb.pack(anchor="w")
    s2_o, s2 = card(row12, f"{S_ICO['cloud']}  Step 2 - Sign in", BRAND_COLORS[1])
    s2_o.pack(side="left", fill="both", expand=True, padx=(8, 0))
    s2a = ttk.Frame(s2)
    s2a.pack(fill="x")
    ttk.Label(s2a, text="Status:").pack(side="left")
    auth_badge = tk.Label(s2a, text="Not checked", fg="white", bg=COLORS["dim"], padx=10, pady=2, font=("Segoe UI", 9, "bold"))
    auth_badge.pack(side="left", padx=6)
    signin_btn = ttk.Button(s2a, text="Sign in")
    signin_btn.pack(side="left", padx=(8, 4))
    check_btn = ttk.Button(s2a, text="Check status")
    check_btn.pack(side="left")
    s2m = ttk.Frame(s2)
    s2m.pack(fill="x", pady=(2, 0))
    ttk.Label(s2m, text="Sign-in method:").pack(side="left")
    signin_var = tk.StringVar(value=SIGNIN_METHOD_LABELS[LOGIN_OPTS.get("signin") or "manual"])
    signin_combo = ttk.Combobox(s2m, width=44, state="readonly", textvariable=signin_var, values=[SIGNIN_METHOD_LABELS[k] for k in ("manual", "captured", "console")])
    signin_combo.pack(side="left", padx=6)
    device_var = tk.BooleanVar(value=LOGIN_OPTS["device_code"])
    s2d = ttk.Frame(s2)
    s2d.pack(fill="x", pady=(2, 0))
    device_chk = ttk.Checkbutton(s2d, text="Use device code / no-browser sign-in (default)", variable=device_var)
    device_chk.pack(side="left")
    autowrap(ttk.Label(s2d, style="Muted.TLabel", justify="left", wraplength=300,
                       text="No browser opens here: you get a link, open it on any device, sign in, and paste the code Google shows."), 8).pack(side="left", fill="x", expand=True, padx=(8, 0))
    s2b = ttk.Frame(s2)
    s2b.pack(fill="x", pady=(2, 0))
    ttk.Label(s2b, text="Account:").pack(side="left")
    acct_combo = ttk.Combobox(s2b, width=38, state="normal", values=[])      # type to filter the list (type-ahead) when there are many accounts
    acct_combo.pack(side="left", padx=4)
    use_acct_btn = ttk.Button(s2b, text="Use this account")
    use_acct_btn.pack(side="left")
    acct_chip = tk.Label(s2b, text="", fg="white", bg=COLORS["dim"], padx=8, pady=1, font=("Segoe UI", 9, "bold"))
    acct_chip.pack(side="left", padx=6)
    s2c = ttk.Frame(s2)
    s2c.pack(fill="x", pady=(2, 0))
    diff_btn = ttk.Button(s2c, text="Sign in with a different account")
    diff_btn.pack(side="left")
    ttk.Label(s2c, text=" hint:").pack(side="left")
    hint_var = tk.StringVar(value="")
    hint_entry = ttk.Entry(s2c, textvariable=hint_var, width=18)
    hint_entry.pack(side="left", padx=2)
    check_all_btn = ttk.Button(s2c, text="Check all accounts / Re-check")
    check_all_btn.pack(side="left", padx=(6, 0))
    sd_toggle_btn = ttk.Button(s2c, text="Hide sign-in details")
    st_wrap = ttk.Frame(s2)
    st_wrap.pack(fill="x", pady=(4, 0))
    acct_st_tree = ttk.Treeview(st_wrap, columns=("status",), show="tree headings", selectmode="browse", height=2)
    acct_st_tree.heading("#0", text="Account (all accounts gcloud knows)")
    acct_st_tree.heading("status", text="Status")
    acct_st_tree.column("#0", width=270)
    acct_st_tree.column("status", width=210)
    acct_st_sc = ttk.Scrollbar(st_wrap, orient="vertical", command=acct_st_tree.yview)
    acct_st_tree.configure(yscrollcommand=acct_st_sc.set)
    acct_st_sc.pack(side="right", fill="y")
    acct_st_tree.pack(side="left", fill="x", expand=True)
    for _tag, _col in (("active", "#067647"), ("expired", "#c00000"), ("signed_out", "#777777"), ("unknown", "#777777"), ("checking", "#1f4e79")):
        acct_st_tree.tag_configure(_tag, foreground=_col)
    auth_msg = tk.StringVar(value="")
    autowrap(ttk.Label(s2, textvariable=auth_msg, wraplength=520, justify="left")).pack(fill="x", pady=(4, 0))
    # ---- sign-in details panel (device code): the instructions, the full link, the code box; shown while / after a sign-in attempt.
    # It is a full-width row between the steps 1-2 and the steps 3-4 (packed there by sd_show), so the link and the code box are never cut off.
    sd = ttk.LabelFrame(guide, text="Sign-in details (device code)", padding=6)
    sd_right = ttk.Frame(sd)
    sd_right.pack(side="right", fill="y", padx=(12, 0))
    sd_left = ttk.Frame(sd)
    sd_left.pack(side="left", fill="both", expand=True)
    sd_steps = ttk.Label(sd_left, text=chr(10).join(SIGNIN_STEPS), justify="left", wraplength=640)
    autowrap(sd_steps, 8).pack(anchor="w", fill="x")
    sd_url_wrap = ttk.Frame(sd_left)
    sd_url_wrap.pack(fill="x", pady=(4, 0))
    sd_url = tk.Text(sd_url_wrap, height=3, wrap="char", font=("Consolas", 9), fg="#1a0dab", relief="solid", borderwidth=1, cursor="arrow", width=40)
    sd_url_sc = ttk.Scrollbar(sd_url_wrap, orient="vertical", command=sd_url.yview)
    sd_url.configure(yscrollcommand=sd_url_sc.set, state="disabled")
    sd_url_sc.pack(side="right", fill="y")
    sd_url.pack(side="left", fill="x", expand=True)
    sd_url.tag_configure("link", foreground="#1a0dab", underline=True)
    sd_btns = ttk.Frame(sd_left)
    sd_btns.pack(fill="x", pady=(4, 0))
    sd_open_btn = ttk.Button(sd_btns, text="Open in browser")
    sd_open_btn.pack(side="left")
    sd_copy_btn = ttk.Button(sd_btns, text="Copy URL")
    sd_copy_btn.pack(side="left", padx=4)
    ttk.Label(sd_right, text="Paste the verification code here:").pack(anchor="w")
    sd_code_row = ttk.Frame(sd_right)
    sd_code_row.pack(fill="x", pady=(2, 0))
    code_var = tk.StringVar(value="")
    sd_code = ttk.Entry(sd_code_row, textvariable=code_var, width=24, show="•")
    sd_code.pack(side="left")
    sd_submit_btn = ttk.Button(sd_code_row, text="Submit code")
    sd_submit_btn.pack(side="left", padx=(4, 0))
    sd_status_row = ttk.Frame(sd_right)
    sd_status_row.pack(fill="x", pady=(8, 0))
    sd_chip = tk.Label(sd_status_row, text="Idle", fg="white", bg=COLORS["dim"], padx=8, pady=1, font=("Segoe UI", 9, "bold"))
    sd_chip.pack(side="left")
    sd_cancel_btn = ttk.Button(sd_status_row, text="Cancel sign-in")
    sd_cancel_btn.pack(side="right")
    sd_count = tk.StringVar(value="")
    ttk.Label(sd_right, textvariable=sd_count, width=44).pack(anchor="w", pady=(4, 0))
    sd_result = tk.StringVar(value="")
    sd_result_lbl = ttk.Label(sd_right, textvariable=sd_result, justify="left", wraplength=360)
    sd_result_lbl.pack(anchor="w", fill="x", pady=(2, 0))
    sd_raw_hdr = ttk.Frame(sd_left)
    sd_raw_hdr.pack(fill="x", pady=(6, 0))
    ttk.Label(sd_raw_hdr, text="Raw output from gcloud", font=("Segoe UI", 9, "bold")).pack(side="left")
    sd_raw_toggle = ttk.Button(sd_raw_hdr, text="Hide raw output")
    sd_raw_toggle.pack(side="left", padx=6)
    sd_cmd_var = tk.StringVar(value="")
    sd_cmd_lbl = ttk.Label(sd_left, textvariable=sd_cmd_var, font=("Consolas", 9))
    sd_cmd_lbl.pack(anchor="w")
    sd_raw = tk.Text(sd_left, height=5, wrap="char", font=("Consolas", 8), relief="solid", borderwidth=1, state="disabled")
    sd_raw.pack(fill="x", pady=(2, 0))
    LOGIN_OPTS["gui"] = True        # no console of our own: interactive logins get a window

    guide_msg = tk.Label(guide, text="", anchor="w", justify="left", font=("Segoe UI", 10, "bold"), padx=8, pady=4,
                         bg="#eef3f8", fg=COLORS["info"])
    guide_msg.pack(fill="x", pady=(6, 0))
    guide_msg.bind("<Configure>", lambda e: guide_msg.configure(wraplength=max(300, e.width - 20)))

    def say(text, kind="info"):
        """The message line under steps 1-2: what happened and what to do next."""
        guide_msg.configure(text=text, fg=COLORS.get(kind, COLORS["info"]))
        state["said"] = (state["said"] + [text])[-60:]       # the last messages (the line above shows only the newest)

    def set_badge(text, kind):
        auth_badge.configure(text=text, bg=COLORS.get(kind, COLORS["dim"]))

    def search_box(parent, var, width=30):
        box = ttk.Frame(parent)
        ttk.Label(box, text=SEARCH_ICON).pack(side="left")
        entry = ttk.Entry(box, textvariable=var, width=width)
        entry.pack(side="left", fill="x", expand=True, padx=4)
        clear = ttk.Button(box, text="✕", width=3, command=lambda: var.set(""))
        clear.pack(side="left")
        return box, entry, clear

    row34 = ttk.Frame(guide, style="Page.TFrame")
    row34.pack(fill="both", expand=True, pady=(6, 0))
    # ---- step 3: the projects (searchable list)
    s3_o, s3 = card(row34, f"{S_ICO['globe']}  Step 3 - Choose project", BRAND_COLORS[2])
    s3_o.pack(side="left", fill="both", expand=True)
    acct_filter = tk.StringVar(value="")
    box3, acct_search, acct_search_x = search_box(s3, acct_filter)
    box3.pack(fill="x")
    acct_count = tk.StringVar(value="")
    ttk.Label(s3, textvariable=acct_count).pack(anchor="w")
    acct_sel_var = tk.StringVar(value="0 of 0 selected")
    ttk.Label(s3, textvariable=acct_sel_var, font=("Segoe UI", 9, "bold"), foreground=BRAND_ACCENT).pack(anchor="w")
    scope_var = tk.StringVar(value="sel" if LOGIN_OPTS["method"] == "cli" else "all")
    scope_row = ttk.Frame(s3)
    scope_row.pack(fill="x")
    scope_all_rb = ttk.Radiobutton(scope_row, text="", value="all", variable=scope_var)
    scope_all_rb.pack(side="left")
    scope_sel_rb = ttk.Radiobutton(scope_row, text="", value="sel", variable=scope_var)
    scope_sel_rb.pack(side="left", padx=(12, 0))
    a_wrap = ttk.Frame(s3)
    a_wrap.pack(fill="both", expand=True)
    acct_tree = ttk.Treeview(a_wrap, columns=("code", "info"), show="tree headings", selectmode="extended", height=5)
    acct_tree.heading("#0", text="Project")
    acct_tree.heading("code", text="Project id")
    acct_tree.heading("info", text="State")
    acct_tree.column("#0", width=200)
    acct_tree.column("code", width=190)
    acct_tree.column("info", width=90)
    a_scroll = ttk.Scrollbar(a_wrap, orient="vertical", command=acct_tree.yview)
    acct_tree.configure(yscrollcommand=a_scroll.set)
    a_scroll.pack(side="right", fill="y")
    acct_tree.pack(side="left", fill="both", expand=True)
    acct_tree.tag_configure("hint", foreground="#888888")
    acct_tree.tag_configure("odd", background=STRIPE)
    a_btns = ttk.Frame(s3)
    a_btns.pack(fill="x", pady=(4, 0))
    acct_all_btn = ttk.Button(a_btns, text="Select all (shown)")
    acct_all_btn.pack(side="left")
    acct_clear_btn = ttk.Button(a_btns, text="Clear")
    acct_clear_btn.pack(side="left", padx=4)
    acct_reload_btn = ttk.Button(a_btns, text="Reload projects")
    acct_reload_btn.pack(side="left")
    acct_status = tk.StringVar(value="")
    autowrap(ttk.Label(s3, textvariable=acct_status, wraplength=460, justify="left")).pack(fill="x", pady=(4, 0))

    # ---- step 4: the clusters (searchable multi-select list)
    s4_o, s4 = card(row34, f"{S_ICO['helm']}  Step 4 - Choose clusters  (Ctrl/Shift-click for several)", BRAND_COLORS[3])
    s4_o.pack(side="left", fill="both", expand=True, padx=(8, 0))
    filter_var = tk.StringVar(value="")
    box4, cl_search, cl_search_x = search_box(s4, filter_var)
    box4.pack(fill="x")
    cl_count = tk.StringVar(value="")
    ttk.Label(s4, textvariable=cl_count).pack(anchor="w")
    c_wrap = ttk.Frame(s4)
    c_wrap.pack(fill="both", expand=True)
    cluster_tree = ttk.Treeview(c_wrap, columns=("where", "acct"), show="tree headings", selectmode="extended", height=5)
    cluster_tree.heading("#0", text="Cluster")
    cluster_tree.heading("where", text="Location")
    cluster_tree.heading("acct", text="Project")
    cluster_tree.column("#0", width=230)
    cluster_tree.column("where", width=150)
    cluster_tree.column("acct", width=150)
    c_scroll = ttk.Scrollbar(c_wrap, orient="vertical", command=cluster_tree.yview)
    cluster_tree.configure(yscrollcommand=c_scroll.set)
    c_scroll.pack(side="right", fill="y")
    cluster_tree.pack(side="left", fill="both", expand=True)
    cluster_tree.tag_configure("hint", foreground="#888888")
    cluster_tree.tag_configure("odd", background=STRIPE)
    c_btns = ttk.Frame(s4)
    c_btns.pack(fill="x", pady=(4, 0))
    select_all_btn = ttk.Button(c_btns, text="Select all (shown)")
    select_all_btn.pack(side="left")
    clear_btn = ttk.Button(c_btns, text="Clear")
    clear_btn.pack(side="left", padx=4)
    refresh_btn = ttk.Button(c_btns, text="Reload clusters")
    refresh_btn.pack(side="left")
    refresh_sel_btn = ttk.Button(c_btns, text="Refresh selected")
    manual_var = tk.StringVar(value="")
    ttk.Label(c_btns, text="  or type numbers:").pack(side="left")
    manual_entry = ttk.Entry(c_btns, textvariable=manual_var, width=16)
    manual_entry.pack(side="left", padx=4)
    ttk.Label(c_btns, text="e.g. 1,3,5  2-4  all").pack(side="left")
    sel_text = tk.StringVar(value="Selected: none")
    autowrap(ttk.Label(s4, textvariable=sel_text, wraplength=460, justify="left")).pack(fill="x", pady=(4, 0))
    cl_status = tk.StringVar(value="")
    autowrap(ttk.Label(s4, textvariable=cl_status, wraplength=460, justify="left")).pack(fill="x")
    list_bar = ttk.Progressbar(s4, mode="determinate", length=300)
    list_bar.pack(fill="x", pady=(2, 0))
    acct_widgets = [acct_search, acct_search_x, scope_all_rb, scope_sel_rb, acct_all_btn, acct_clear_btn]
    cl_widgets = [cl_search, cl_search_x, select_all_btn, clear_btn, manual_entry]

    # ---- action bar: window, run / stop, run options
    top = ttk.Frame(actionbar, style="Page.TFrame", padding=(10, 8, 10, 2))
    top.pack(fill="x")
    ttk.Label(top, text="Last (minutes):", style="Bar.TLabel").pack(side="left")
    minutes_var = tk.StringVar(value=str(default_minutes))
    ttk.Spinbox(top, from_=1, to=1440, width=6, textvariable=minutes_var).pack(side="left", padx=(4, 0))
    gcp_var = tk.BooleanVar(value=GCP_OPTS["enabled"])           # the 'Google Kubernetes Engine cluster' and 'Pod logs' ticks of the 'What to collect' card
    logs_var = tk.BooleanVar(value=True)
    open_var = tk.BooleanVar(value=True)
    alllogs_var = tk.BooleanVar(value=False)
    ns_var = tk.StringVar(value="")
    workers_var = tk.StringVar(value=str(PARALLEL_WORKERS))
    HARD_GCP_OFF = not GCP_OPTS["enabled"]                        # --no-gcp: no gcloud call at all, whatever is ticked
    run_btn = ttk.Button(top, text=f"{S_ICO['run']}  Login & Debug selected cluster(s)", style="Run.TButton")
    run_btn.pack(side="left", padx=(16, 4))
    stop_btn = ttk.Button(top, text=f"{S_ICO['stop']}  Stop", state="disabled", style="Stop.TButton")
    stop_btn.pack(side="left")
    chip_sections = tk.Label(top, text="", bg=BRAND_SOFT, fg=BRAND_DARK, font=(FONT, 9, "bold"), padx=10, pady=3)
    chip_sections.pack(side="left", padx=(16, 0))

    top2 = ttk.Frame(actionbar, style="Page.TFrame", padding=(10, 0, 10, 6))
    top2.pack(fill="x")
    ttk.Checkbutton(top2, text="Logs of ALL pods", variable=alllogs_var, style="Bar.TCheckbutton").pack(side="left")
    ttk.Label(top2, text="only namespaces (comma separated, blank = all):", style="Bar.TLabel").pack(side="left", padx=(10, 2))
    ttk.Entry(top2, textvariable=ns_var, width=22).pack(side="left")
    ttk.Label(top2, text="Parallel workers:", style="Bar.TLabel").pack(side="left", padx=(14, 2))
    ttk.Spinbox(top2, from_=1, to=32, width=4, textvariable=workers_var).pack(side="left")
    ttk.Checkbutton(top2, text="Open report when done", variable=open_var, style="Bar.TCheckbutton").pack(side="left", padx=(14, 0))

    # ---- tab 2: WHAT TO COLLECT - one tick box per report section (the registry SECTIONS drives it)
    sec_vars = {sec["id"]: (gcp_var if sec["id"] == "gcp" else logs_var if sec["id"] == "logs" else tk.BooleanVar(value=True)) for sec in SECTIONS}
    sec_count = tk.StringVar(value="")
    sec_note = tk.StringVar(value="")
    collect_o, collect = card(tab2, f"{S_ICO['check']}  What to collect - untick what you do not need; unticked sections are never collected", BRAND_COLORS[0], 8)
    collect_o.pack(fill="both", expand=True, padx=6, pady=(4, 0))
    chead = ttk.Frame(collect)
    chead.pack(fill="x")
    sec_count_lbl = tk.Label(chead, textvariable=sec_count, bg=BRAND_SOFT, fg=BRAND_DARK, font=(FONT, 10, "bold"), padx=12, pady=4)
    sec_count_lbl.pack(side="left")
    qbtns = ttk.Frame(chead)
    qbtns.pack(side="left", padx=(14, 0))
    sec_buttons = {}
    for key, label in (("all", "Select all"), ("none", "Clear all"), ("net", "Only networking"), ("nonet", "Everything except networking")):
        sec_buttons[key] = ttk.Button(qbtns, text=label)
        sec_buttons[key].pack(side="left", padx=(0, 6))
    autowrap(ttk.Label(collect, textvariable=sec_note, style="Muted.TLabel", justify="left", wraplength=800), 8).pack(fill="x", pady=(6, 2))
    cscroll_wrap = ttk.Frame(collect)
    cscroll_wrap.pack(fill="both", expand=True)
    ccanvas = tk.Canvas(cscroll_wrap, highlightthickness=0, bg=CARD)
    cbar = ttk.Scrollbar(cscroll_wrap, orient="vertical", command=ccanvas.yview)
    ccanvas.configure(yscrollcommand=cbar.set)
    cbar.pack(side="right", fill="y")
    ccanvas.pack(side="left", fill="both", expand=True)
    cgrid = ttk.Frame(ccanvas)
    cwin = ccanvas.create_window((0, 0), window=cgrid, anchor="nw")
    cgrid.bind("<Configure>", lambda e: ccanvas.configure(scrollregion=ccanvas.bbox("all")))
    ccanvas.bind("<Configure>", lambda e: ccanvas.itemconfigure(cwin, width=e.width))
    cgrid.grid_columnconfigure(0, weight=1, uniform="seccol")
    cgrid.grid_columnconfigure(1, weight=1, uniform="seccol")
    per_col = (len(SECTIONS) + 1) // 2
    sec_checks, sec_descs = {}, {}
    for i, sec in enumerate(SECTIONS):
        r, c = i % per_col, i // per_col
        cell = ttk.Frame(cgrid)
        cell.grid(row=r, column=c, sticky="nsew", padx=(4, 14), pady=(2, 3))
        cb = ttk.Checkbutton(cell, text=f"{sym(sec['icon'])}  {sec['num']}. {sec['title']}", variable=sec_vars[sec["id"]], style="Sec.TCheckbutton")
        cb.pack(anchor="w")
        desc = ttk.Label(cell, text=sec["desc"], style="Muted.TLabel", justify="left", wraplength=440)
        desc.pack(fill="x", padx=(26, 0))
        autowrap(desc, 40)
        sec_checks[sec["id"]], sec_descs[sec["id"]] = cb, desc

        def wheel(e, _c=ccanvas):
            _c.yview_scroll(-1 if (getattr(e, "delta", 0) > 0 or getattr(e, "num", 0) == 4) else 1, "units")
        for w_ in (cb, desc, cell):
            w_.bind("<MouseWheel>", wheel)
    ccanvas.bind("<MouseWheel>", lambda e: ccanvas.yview_scroll(-1 if e.delta > 0 else 1, "units"))
    autowrap(ttk.Label(collect, text="The cluster data (every kubectl read) is always collected: every section works from it. A section that needs data of an unticked one "
                                     "(for example Network and traffic needs Google Cloud cluster details) reads it silently and does not show it.",
                       style="Muted.TLabel", justify="left", wraplength=800), 8).pack(fill="x", pady=(4, 0))

    def selected_ids():
        return [sec["id"] for sec in SECTIONS if sec_vars[sec["id"]].get()]

    def section_plan():
        return resolve_sections({"sections": selected_ids(), "gcp": not HARD_GCP_OFF})

    def set_sections(ids):
        for sid, var in sec_vars.items():
            var.set(sid in ids)

    # ---- tab 3: steps + clusters-in-run (left); live findings + live log (right)
    body = ttk.PanedWindow(tab3, orient="horizontal")
    body.pack(fill="both", expand=True, pady=(4, 0))
    left = ttk.Frame(body, style="Page.TFrame", width=420)
    body.add(left, weight=0)
    steps_o, steps_box = card(left, f"{S_ICO['helm']}  Collection steps (current cluster)", BRAND_COLORS[0], 4)
    steps_o.pack(fill="both", expand=True)
    steps = ttk.Treeview(steps_box, columns=("status", "time", "mark"), displaycolumns=("mark", "status", "time"), height=9, show="tree headings", selectmode="none")
    steps.heading("#0", text="Step")
    steps.heading("status", text="Status")
    steps.heading("time", text="Time")
    steps.heading("mark", text="")
    steps.column("#0", width=230)
    steps.column("status", width=72, anchor="center")
    steps.column("time", width=52, anchor="e")
    steps.column("mark", width=60, anchor="center", stretch=False)
    steps_scroll = ttk.Scrollbar(steps_box, orient="vertical", command=steps.yview)
    steps.configure(yscrollcommand=steps_scroll.set)
    steps_scroll.pack(side="right", fill="y")
    steps.pack(fill="both", expand=True)
    for tag, color in (("running", "#1f4e79"), ("done", "#067647"), ("failed", "#c00000"), ("skipped", "#888888")):
        steps.tag_configure(tag, foreground=color)
    steps.tag_configure("running", font=(FONT, 10, "bold"))
    steps.tag_configure("odd", background=STRIPE)

    run_o, run_box = card(left, f"{S_ICO['cloud']}  Clusters in this run (double-click a finished one to open its report)", BRAND_COLORS[1], 4)
    run_o.pack(fill="x", pady=(6, 0))
    run_tree = ttk.Treeview(run_box, columns=("status", "crit", "high"), height=3, show="tree headings", selectmode="browse")
    run_tree.heading("#0", text="Cluster")
    run_tree.heading("status", text="Status")
    run_tree.heading("crit", text="CRIT")
    run_tree.heading("high", text="HIGH")
    run_tree.column("#0", width=190)
    run_tree.column("status", width=100, anchor="center")
    run_tree.column("crit", width=46, anchor="center")
    run_tree.column("high", width=46, anchor="center")
    run_tree.pack(fill="x")
    for tag, color in (("running", "#1f4e79"), ("ok", "#067647"), ("failed", "#c00000"), ("partial", "#9a7d0a"), ("notrun", "#888888")):
        run_tree.tag_configure(tag, foreground=color)

    right = ttk.PanedWindow(body, orient="vertical")
    body.add(right, weight=1)
    find_o, find_box = card(right, f"{S_ICO['warn']}  Findings (live - updates while collecting)", BRAND_COLORS[2], 4)
    right.add(find_o, weight=1)
    counters = ttk.Frame(find_box)
    counters.pack(fill="x")
    counter_vars = {}
    for sev, color in (("CRIT", "#c00000"), ("HIGH", "#d35400"), ("MED", "#9a7d0a"), ("INFO", "#1f6feb")):
        counter_vars[sev] = tk.StringVar(value=f"{sev} 0")
        tk.Label(counters, text=sym(SEVERITY_DOT[sev]), bg=CARD, fg=color).pack(side="left")
        tk.Label(counters, textvariable=counter_vars[sev], fg=color, bg=CARD, font=(FONT, 10, "bold")).pack(side="left", padx=(2, 16))
    findings = ttk.Treeview(find_box, columns=("sev", "text", "dot"), displaycolumns=("dot", "sev", "text"), show="headings", height=6)
    findings.heading("dot", text="")
    findings.heading("sev", text="Sev")
    findings.heading("text", text="Finding")
    findings.column("dot", width=34, anchor="center", stretch=False)
    findings.column("sev", width=54, anchor="center", stretch=False)
    findings.column("text", width=420)
    fs = ttk.Scrollbar(find_box, orient="vertical", command=findings.yview)
    findings.configure(yscrollcommand=fs.set)
    fs.pack(side="right", fill="y")
    findings.pack(fill="both", expand=True)
    for tag, color in (("CRIT", "#c00000"), ("HIGH", "#d35400"), ("MED", "#9a7d0a"), ("INFO", "#1f6feb")):
        findings.tag_configure(tag, foreground=color)
    findings.tag_configure("odd", background=STRIPE)

    log_o, log_box = card(right, f"{S_ICO['doc']}  Live log", BRAND_COLORS[3], 4)
    right.add(log_o, weight=2)
    text = scrolledtext.ScrolledText(log_box, font=("Consolas", 9), wrap="none", relief="flat", borderwidth=0, background="#FFFFFF")
    text.pack(fill="both", expand=True)
    for tag, color in (("CRIT", "#c00000"), ("HIGH", "#d35400"), ("MED", "#9a7d0a"), ("HEAD", "#1f4e79"), ("ERR", "#c00000"), ("WARN", "#d35400")):
        text.tag_configure(tag, foreground=color)
    text.tag_configure("HEAD", font=("Consolas", 9, "bold"))

    # ---- status bar: progress + status + tasks done + elapsed + actions
    bottom = statusbar
    folder_btn = ttk.Button(bottom, text=f"{S_ICO['doc']}  Open reports folder")
    folder_btn.pack(side="right")
    open_btn = ttk.Button(bottom, text=f"{S_ICO['globe']}  Open HTML report", state="disabled", style="Accent.TButton")
    open_btn.pack(side="right", padx=6)
    progress_bar = ttk.Progressbar(bottom, mode="determinate", length=170)
    progress_bar.pack(side="left")
    elapsed = tk.StringVar(value="")
    ttk.Label(bottom, textvariable=elapsed, style="Bar.TLabel", width=13).pack(side="left", padx=(8, 0))
    tasks_var = tk.StringVar(value="")
    ttk.Label(bottom, textvariable=tasks_var, style="Bar.TLabel", foreground=BRAND_DARK, width=28).pack(side="left", padx=(6, 0))
    status = tk.StringVar(value="Ready.")
    ttk.Label(bottom, textvariable=status, style="Bar.TLabel", anchor="w").pack(side="left", padx=(8, 8), fill="x", expand=True)

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

    def set_state(widget, enabled):
        widget.state(["!disabled"] if enabled else ["disabled"])

    def is_cli():
        return LOGIN_OPTS["method"] == "cli"

    # ---- search + selection helpers (both lists work the same: live filter, selection kept, Showing X of Y)
    def terms(var):
        return var.get().strip().lower().split()

    def hit(hay, ts):
        return all(t in hay for t in ts)

    def prepare(r):
        r["hay"] = " ".join(str(x) for x in (r.get("number") or "", r.get("label") or "", r["name"], r.get("where") or "",
                                              r.get("account_name") or "", r.get("account") or "")).lower()
        return r

    def index_rows():
        state["by_key"] = {r["key"]: r for r in state["crows"]}

    def effective_accounts():
        """The projects the cluster listing covers: the selected ones, or all usable ones ('All projects')."""
        if scope_var.get() == "sel":
            return [a for a in state["accounts"] if a["id"] in state["acct_chosen"]]
        found = [a for a in state["accounts"] if a.get("usable", True)]
        return found

    def signin_account():
        """The account the sign-in belongs to (the AWS profile; the other clouds sign in once)."""
        chosen = [a["id"] for a in state["accounts"] if a["id"] in state["acct_chosen"]]
        return chosen[0] if scope_var.get() == "sel" and chosen else None

    def update_scope_labels():
        usable = len([a for a in state["accounts"] if a.get("usable", True)])
        if is_cli():
            scope_all_rb.configure(text=f"All projects ({usable})")
            scope_sel_rb.configure(text=f"Only the selected projects ({len(state['acct_chosen'])})")
        else:
            scope_all_rb.configure(text="(auto) - picked after login")
            scope_sel_rb.configure(text=f"Use the selected project ({len(state['acct_chosen'])})")

    # ---- step 3 list
    def rebuild_account_list():
        acct_tree.delete(*acct_tree.get_children())
        if state["acct_locked"]:
            acct_tree.insert("", "end", iid="__hint__", text="Sign in first (step 2)", tags=("hint",))
            acct_count.set("")
            return
        ts = terms(acct_filter)
        shown = [a for a in state["accounts"] if hit(a["hay"], ts)]
        for a in shown:
            acct_tree.insert("", "end", iid=a["id"], text=a["name"], values=(a["code"], a["info"]), tags=(("odd",) if len(acct_tree.get_children()) % 2 else ()))
        acct_tree.selection_set([a["id"] for a in shown if a["id"] in state["acct_chosen"]])
        acct_count.set(f"Showing {len(shown)} of {len(state['accounts'])}")
        acct_sel_var.set(f"{len(state['acct_chosen'])} of {len(state['accounts'])} selected")

    def on_acct_select(_event=None):
        if state["acct_locked"]:
            return
        visible = set(acct_tree.get_children())
        state["acct_chosen"] = (state["acct_chosen"] - visible) | (set(acct_tree.selection()) & visible)
        if state["acct_chosen"] and scope_var.get() == "all":
            scope_var.set("sel")
        on_scope_change()

    def on_scope_radio():
        if scope_var.get() == "all":
            state["acct_chosen"] = set()               # 'All' is explicit: no leftover selection
            rebuild_account_list()
        on_scope_change()

    def acct_select_all():
        state["acct_chosen"] |= {i for i in acct_tree.get_children() if i != "__hint__"}
        if state["acct_chosen"]:
            scope_var.set("sel")
        rebuild_account_list()
        on_scope_change()

    def acct_clear():
        state["acct_chosen"] = set()
        scope_var.set("sel" if is_cli() else "all")
        rebuild_account_list()
        on_scope_change()

    def on_scope_change():
        _SESSION["projects"] = sorted(state["acct_chosen"])            # remembered for the session
        update_scope_labels()
        acct_sel_var.set(f"{len(state['acct_chosen'])} of {len(state['accounts'])} selected")

        sync_login_opts()
        rebuild_cluster_list()
        cluster_hint()
        if PER_ACCOUNT and is_cli():
            if state["recheck"]:
                root.after_cancel(state["recheck"])
            state["recheck"] = root.after(500, recheck_account)

    def recheck_account():
        state["recheck"] = None
        if signin_account() != state["auth_for"]:
            check_status()

    # ---- step 4 list
    def scoped_rows():
        rows = state["crows"]
        if is_cli() and scope_var.get() == "sel":
            rows = [r for r in rows if r.get("account") in state["acct_chosen"]]
        return rows

    def rebuild_cluster_list():
        cluster_tree.delete(*cluster_tree.get_children())
        if state["cl_locked"]:
            cluster_tree.insert("", "end", iid="__hint__", text="Sign in first (step 2)", tags=("hint",))
            cl_count.set("")
            update_selected_label()
            return
        rows = scoped_rows()
        ts = terms(filter_var)
        shown = [r for r in rows if hit(r["hay"], ts)]
        for r in shown:
            cluster_tree.insert("", "end", iid=r["key"], text=(f"{r['number']} - {r['name']}" if r.get("number") else r["name"]),
                                values=(r.get("where") or "", r.get("account_name") or ""), tags=(("odd",) if len(cluster_tree.get_children()) % 2 else ()))
        if not rows and on_demand() and not state["listing"]:
            cluster_tree.insert("", "end", iid="__hint__", text=COLLECT_HINT, tags=("hint",))
        cluster_tree.selection_set([r["key"] for r in shown if r["key"] in state["cchosen"]])
        cl_count.set(f"Showing {len(shown)} of {len(rows)}")
        update_selected_label()

    def rebuild_soon():
        """Listing adds clusters in many small batches: redraw at most every 200 ms."""
        if not state["rebuild"]:
            state["rebuild"] = True

            def go():
                state["rebuild"] = False
                rebuild_cluster_list()
            root.after(200, go)

    def on_tree_select(_event=None):
        if state["cl_locked"]:
            return
        visible = set(cluster_tree.get_children())
        state["cchosen"] = (state["cchosen"] - visible) | (set(cluster_tree.selection()) & visible)
        update_selected_label()

    def typed_numbers():
        return [n for n, _ in parse_cluster_selection(manual_var.get(), state["clusters"])] if manual_var.get().strip() else []

    def chosen_clusters():
        """[(number, label)] from the list selection plus typed numbers, in numeric order."""
        numbers = {state["by_key"][k]["number"] for k in state["cchosen"] if k in state["by_key"] and state["by_key"][k].get("number")}
        numbers |= set(typed_numbers())
        order = sorted(numbers, key=numeric)
        return [(n, state["clusters"].get(n) or f"cluster-{n}") for n in order]

    def update_selected_label():
        sel = chosen_clusters()
        sel_text.set("Selected: none" if not sel else f"Selected {len(sel)}: " + ", ".join(f"{n} {l}" for n, l in sel)[:230])

    def select_all():
        state["cchosen"] |= {i for i in cluster_tree.get_children() if i != "__hint__"}
        cluster_tree.selection_set([i for i in cluster_tree.get_children() if i in state["cchosen"]])
        update_selected_label()

    def clear_selection():
        state["cchosen"] = set()
        manual_var.set("")
        cluster_tree.selection_remove(cluster_tree.selection())
        update_selected_label()

    def set_rows(rows):
        state["crows"] = rows
        index_rows()
        rebuild_cluster_list()

    COLLECT_HINT = "Select one or more projects above, then press 'Collect clusters from the selected projects'."

    def on_demand():
        """True when the cluster list is filled by the 'Collect clusters' button (Cloud CLI login, or the custom login's gcloud source)."""
        return is_cli() or src_var.get() == "all"

    def selected_projects():
        """The projects a collection covers: the ones selected in step 3 ('All projects' only when the user chose that option explicitly)."""
        if is_cli():
            return effective_accounts()
        return [a for a in state["accounts"] if a["id"] in state["acct_chosen"]]

    def update_collect_button():
        if on_demand():
            refresh_btn.configure(text="Collect clusters from selected projects", style="Accent.TButton")
            if not refresh_sel_btn.winfo_ismapped():
                refresh_sel_btn.pack(side="left", padx=(4, 0), after=refresh_btn)
        else:
            refresh_btn.configure(text="Reload clusters", style="TButton")
            refresh_sel_btn.pack_forget()

    def cluster_hint():
        """The line under the cluster list: where the list stands."""
        if state["cl_locked"]:
            cl_status.set("Sign in first (step 2).")
        elif state["listing"]:
            secs = int(time.time() - state.get("list_t0", time.time()))
            cl_status.set(f"Listing clusters: {state['list_done']}/{state['list_total']} projects ... ({count_of(len(state['crows']), 'cluster')} so far)   elapsed {secs // 60}:{secs % 60:02d}")
        elif on_demand() and not state["listed"] and not state["crows"]:
            cl_status.set(COLLECT_HINT)
        elif on_demand():
            todo = [a for a in selected_projects() if a["id"] not in state["listed"]]
            if todo:
                cl_status.set(f"{len(todo)} of the selected projects are not collected yet - press 'Collect clusters from selected projects'.")

    # ---- sign-in state: badge, message, what is unlocked
    def refresh_banner(*_):
        try:
            mins = max(1, int(minutes_var.get()))
        except ValueError:
            mins = default_minutes
        who = (("credentials expired" if state["auth"]["state"] == "expired" else "not signed in") if state["auth"]["state"] != "ok" else (state["auth"].get("who") or "signed in")) if is_cli() else "custom login (gkelogin)"
        projects = count_of(len(state["accounts"]), "project") if state["accounts_loaded"] else "projects not loaded yet"
        banner_sub.set(f"Last {mins} min   \u2022   {CLOUD_NAME} account: {who}   \u2022   {projects}   \u2022   read-only")

    def set_collect_state(enabled):
        for w in list(sec_checks.values()) + list(sec_buttons.values()):
            set_state(w, enabled)

    def update_controls():
        cli = is_cli()
        busy, listing = state["busy"], state["listing"]
        auth_ok = state["auth"]["state"] == "ok"
        working = state["signing"] or state["checking"]
        idle = not busy and not listing and not working
        set_state(signin_btn, cli and idle)
        set_state(check_btn, cli and idle)
        set_state(device_chk, cli and not working)
        signin_combo.configure(state="readonly" if (cli and not working and not busy and not listing) else "disabled")
        for w in (acct_combo, use_acct_btn, diff_btn, hint_entry, check_all_btn):
            set_state(w, cli and idle)
        for rb in (src_all_rb, src_menu_rb):
            set_state(rb, not cli and not busy and not listing)
        state["acct_locked"] = cli and NEED_SIGNIN and not auth_ok
        state["cl_locked"] = cli and not auth_ok
        for w in acct_widgets:
            set_state(w, not state["acct_locked"])
        for w in cl_widgets:
            set_state(w, not state["cl_locked"])
        set_state(acct_all_btn, not state["acct_locked"])
        set_state(acct_reload_btn, not state["acct_locked"] and not busy and not state["acct_loading"])
        set_state(refresh_btn, not state["cl_locked"] and not busy and not listing)
        set_state(refresh_sel_btn, not state["cl_locked"] and not busy and not listing)
        update_collect_button()
        set_state(select_all_btn, not state["cl_locked"])
        set_state(clear_btn, not state["cl_locked"])
        acct_tree.configure(selectmode="none" if state["acct_locked"] else "extended")
        cluster_tree.configure(selectmode="none" if state["cl_locked"] else "extended")
        method_combo.configure(state="disabled" if (busy or listing or working) else "readonly")
        run_btn.state(["disabled"] if (busy or listing) else ["!disabled"])
        stop_btn.state(["!disabled"] if (busy or listing) else ["disabled"])
        s3.configure(text=f"{S_ICO['globe']}  Step 3 - Choose project" + ("   [locked - sign in first]" if state["acct_locked"] else
                                                                           (f"   [{len(state['accounts'])} loaded]" if state["accounts"] else "")))
        s4.configure(text=f"{S_ICO['helm']}  Step 4 - Choose clusters  (Ctrl/Shift-click for several)"
                          + ("   [locked - sign in first]" if state["cl_locked"] else ""))
        if state.get("locks") != (state["acct_locked"], state["cl_locked"]):
            state["locks"] = (state["acct_locked"], state["cl_locked"])
            rebuild_account_list()
            rebuild_cluster_list()
        set_collect_state(not busy)                 # the 'What to collect' panel is locked while a run is going
        refresh_banner()
        cluster_hint()

    def apply_method_ui():
        if is_cli():
            method_info.set("Uses the Google Cloud CLI (gcloud): sign in in step 2, then the projects and clusters are read from Google Cloud.")
        else:
            method_info.set("Uses gkelogin.exe: it signs in when you press Run. The cluster list (step 4) is every cluster you can access with gcloud "
                            "(default) or only the gkelogin menu; a cluster that is not in the gkelogin menu is logged in with gcloud.")
        update_scope_labels()
        if not is_cli():
            state["auth"] = {"state": "unchecked"}
            man_hide()
            set_badge("Handled by gkelogin", "dim")
            if not state["acct_chosen"]:
                scope_var.set("all")
            auth_msg.set("Uses gkelogin.exe - it signs in when you press Run. The sign-in buttons are only used with the Cloud CLI method.")
            say("Custom login: choose the project if you want to force one (otherwise it is picked after login), select the clusters in step 4 and press "
                "'Login & Debug'.", "info")
        else:
            set_badge("Not checked", "dim")
            auth_msg.set("")
            if not state["acct_chosen"]:
                scope_var.set("sel")

    def apply_auth(res, source):
        if source == "manual_auto":           # the window's own check while it waits for a manual sign-in: only a success matters
            man["probing"] = False
            if (res["state"] != "ok" or not man["active"] or not is_cli() or state["busy"] or state["checking"] or state["signing"] or state["listing"]):
                return
            if state["auth"]["state"] == "ok" and state["auth"].get("who") == res.get("who"):
                man_signed_in(res)
                return
            source = "manual_signed"
        state["checking"] = state["signing"] = False
        if not is_cli():                      # the method was switched while this was running
            update_controls()
            return
        manual_src = source in ("manual_verify", "manual_signed")
        if manual_src and res["state"] == "ok":
            source = "check" if (state["auth"]["state"] == "ok" and state["auth"].get("who") == res.get("who")) else "signin_new"
        elif source == "manual_verify":
            fix = signin_failure_help(res, man_account())
            if res.get("accounts") is not None:
                fill_account_combo(res)
            if res.get("expired") and res.get("who"):
                show_expired(res["who"], "verify")                 # the red banner + the commands under it
            else:
                state["auth"] = res
                set_badge("gcloud not installed" if res.get("state") == "no_cli" else "Not signed in", "err")
                auth_msg.set(fix.splitlines()[0])
                say("Not signed in yet: " + fix.splitlines()[0].replace("Verification failed: ", ""), "err")
            man_open(force=True, quiet=True)
            man_failed(fix)
            update_controls()
            return
        was_ok = state["auth"]["state"] == "ok"
        state["auth"] = res
        st = res["state"]
        if res.get("accounts") is not None:
            fill_account_combo(res)
        if source in ("signin_new", "switch") and st == "ok":
            reset_lists()
            if source == "signin_new":
                state["signin_note"] = res.get("who")
                sd_result.set(f"Signed in as {res.get('who')} - reading the projects ...")
                sd_result_lbl.configure(foreground=COLORS["ok"])
        if st == "ok" and source in ("check", "already", "signin_new", "signin_ok", "switch"):
            signin_btn.configure(style="TButton")
            AUTH_STATE["expired"].discard(res.get("who"))
            check_accounts_async(res.get("who"))
        if st == "ok" and man["shown"]:
            man_signed_in(res)
        if st == "ok":
            who = res.get("who") or "?"
            set_badge(f"Signed in as {who}", "ok")
            auth_msg.set("You are signed in. Next: step 3 and step 4.")
            lead = {"signin_ok": "Login OK - ", "signin_new": "Login OK - ", "already": "Already signed in - ", "switch": "Now using this account - "}.get(source, "")
            say(f"{lead}signed in as {who}. Next: choose the project in step 3 (or keep 'All projects') and the clusters in step 4.", "ok")
        elif st == "no_cli":
            set_badge("gcloud not installed", "err")
            auth_msg.set(res.get("detail") or "")
            say(f"{res.get('detail')} {res.get('hint')}", "err")
            if LOGIN_OPTS["signin"] == "manual":
                man_open(quiet=True, force=True)
        else:
            set_badge("Not signed in", "err")
            auth_msg.set(f"Reason: {res.get('detail') or 'unknown'}\nNext: {res.get('hint')}")
            if LOGIN_OPTS["signin"] == "manual" and source != "signin_fail":
                man_open(quiet=True)
            if source == "signin_fail":
                say("Sign-in failed or was cancelled (" + (res.get("detail") or "no details") + "). Press 'Sign in' to try again, or run 'gcloud auth login' in a "
                    "terminal and then press 'Check status'.", "err")
                if sd.winfo_ismapped() and not sd_result.get():
                    sd_result.set(res.get("detail") or "The sign-in failed.")
                    sd_result_lbl.configure(foreground=COLORS["err"])
            else:
                say("Not signed in. " + (res.get("hint") or ""), "err")
        if st == "ok" and state["accounts_loaded"] and not state["accounts"]:
            say(acct_status.get(), "warn")
        update_controls()
        if st == "ok" and (not was_ok or source in ("signin_ok", "signin_new", "switch", "already")):
            if not state["accounts_loaded"] or not state["accounts"]:
                load_accounts_async()
            else:
                maybe_auto_list()

    def check_status(_event=None, source="check"):
        if not is_cli() or state["checking"] or state["signing"] or state["busy"] or state["listing"]:
            return
        sync_login_opts()
        acct = signin_account()
        state.update(checking=True, auth_for=acct)
        set_badge("Checking...", "info")
        auth_msg.set("Checking whether gcloud is signed in ...")
        say("Checking the sign-in ...", "info")
        update_controls()

        def work():
            try:
                res = verify_signin_follow(acct) if source == "manual_verify" else login_status(acct)
            except Exception as exc:
                res = {"state": "not_signed_in", "who": None, "detail": str(exc), "hint": "Press 'Sign in'."}
            msgs.put(("auth", res, source))
        threading.Thread(target=work, daemon=True).start()

    def sign_in(_event=None, force=False, hint=None, method=None):
        if not is_cli() or state["checking"] or state["signing"] or state["busy"] or state["listing"]:
            return
        if state["auth"]["state"] == "expired":            # renew the expired account: a fresh sign-in for exactly that account
            force = True
            hint = hint or state["auth"].get("who")
        sync_login_opts()
        explicit = method is not None
        method = method or LOGIN_OPTS.get("signin") or "manual"
        if method == "manual":                              # the default: show the commands, the user runs them (the window verifies read-only)
            man_open(force=force, account=hint)
            return
        acct = signin_account()
        state.update(signing=True, auth_for=acct)
        set_badge("Signing in...", "info")
        auth_msg.set("Running the sign-in ...")
        captured = method == "captured" and (LOGIN_OPTS["device_code"] or explicit)
        if captured:
            panel_reset()
        update_controls()

        def work():
            try:
                pre = login_status(acct)
                if pre["state"] != "not_signed_in" and not force:
                    msgs.put(("auth", pre, "already" if pre["state"] == "ok" else "check"))
                    return
                if captured:
                    msgs.put(("signin_begin", hint, force))
                    return
                if method == "console" or force:
                    flags = ["--no-launch-browser"] if (method == "console" and LOGIN_OPTS["device_code"]) else []
                    msgs.put(("say", "A console window opened - complete the sign-in there (open the link, paste the code). This window continues when you are done.", "info"))
                    cmd = [shutil.which("gcloud") or "gcloud", "auth", "login"] + flags + (["--account", hint] if hint else [])
                    rc = _run_interactive(cmd, lambda l: msgs.put(("line", l)))
                    finish_signin_thread(rc == 0, "" if rc == 0 else "The sign-in failed or was cancelled.", True)
                    return
                msgs.put(("say", "Login window opened - complete the sign-in in the console window / browser that just opened (a code may be shown "
                                 "in the console). This window continues when you are done.", "info"))
                ok = cli_sign_in(lambda l: msgs.put(("line", l)), acct)
                msgs.put(("auth", login_status(acct), "signin_ok" if ok else "signin_fail"))
            except Exception as exc:
                msgs.put(("auth", {"state": "not_signed_in", "who": None, "detail": str(exc), "hint": "Press 'Sign in' to try again."}, "signin_fail"))
        threading.Thread(target=work, daemon=True).start()

    # ---- manual sign-in (the DEFAULT method): the exact commands, each with a Copy button; the window waits and notices the sign-in by itself
    man_panel = ttk.LabelFrame(guide, text="Sign in - run a command yourself", padding=8)
    man = state["man"] = {"shown": False, "active": False, "t0": None, "after": None, "probing": False, "account": None, "fallback": False, "auto": False,
                          "different": False}
    man_form = tk.StringVar(value="device")
    man_instr_var = tk.StringVar(value=MANUAL_INSTRUCTIONS)
    ttk.Label(man_panel, textvariable=man_instr_var, wraplength=900, justify="left", font=("Segoe UI", 10, "bold")).pack(fill="x")
    man_cli_var = tk.StringVar(value="")
    man_cli_lbl = tk.Label(man_panel, textvariable=man_cli_var, anchor="w", justify="left", font=("Segoe UI", 10, "bold"), fg=COLORS["ok"], bg=CARD)
    man_cli_lbl.pack(fill="x", pady=(4, 0))
    man_inst = tk.Label(man_panel, text="Install the Google Cloud CLI first: " + GCLOUD_INSTALL_HINT, anchor="w", justify="left", wraplength=900,
                        fg=COLORS["err"], bg=CARD, font=("Segoe UI", 9))
    man_msg_var = tk.StringVar(value="")
    man_msg_lbl = tk.Label(man_panel, textvariable=man_msg_var, anchor="w", justify="left", wraplength=900, font=("Segoe UI", 10, "bold"), fg=COLORS["warn"], bg=CARD)
    man_msg_lbl.pack(fill="x", pady=(2, 0))
    man_fb = ttk.Frame(man_panel)              # shown after an automatic switch from the captured sign-in
    man_retry_btn = ttk.Button(man_fb, text="Retry")
    man_retry_btn.pack(side="left")
    man_console_btn = ttk.Button(man_fb, text="Run in a console window instead")
    man_console_btn.pack(side="left", padx=(6, 0))
    man_copycmd_btn = ttk.Button(man_fb, text="Copy command")
    man_copycmd_btn.pack(side="left", padx=(6, 0))
    man_cmd_vars, man_copy_btns, man_entries, man_rows = {}, {}, {}, {}
    for _it in manual_commands("someone@example.com", True):
        _row = ttk.Frame(man_panel)
        man_rows[_it["key"]] = _row
        if _it["key"] not in ("device-account", "plugin"):      # the 1b variant (account hint) and the plugin helper are shown only when relevant
            _row.pack(fill="x", pady=(4, 0))
        _top = ttk.Frame(_row)
        _top.pack(fill="x")
        if _it["key"] in _TERMINAL_FORMS:
            ttk.Radiobutton(_top, text=f"{_it['n']}.", variable=man_form, value=_it["key"], width=4).pack(side="left")
        else:
            ttk.Label(_top, text=f"{_it['n']}.", width=5).pack(side="left", padx=(18, 0))
        _note = ttk.Label(_top, text=_it["note"], style="Desc.TLabel", wraplength=860, justify="left")
        _note.pack(side="left", fill="x", expand=True)
        _line = ttk.Frame(_row)
        _line.pack(fill="x", padx=(40, 0))
        man_cmd_vars[_it["key"]] = tk.StringVar(value=_it["cmd"])
        man_entries[_it["key"]] = ttk.Entry(_line, textvariable=man_cmd_vars[_it["key"]], state="readonly",
                                            font=("Consolas", 12, "bold") if _it["key"] == "device" else ("Consolas", 10))
        man_entries[_it["key"]].pack(side="left", fill="x", expand=True)
        man_copy_btns[_it["key"]] = ttk.Button(_line, text="Copy", style="Accent.TButton" if _it["key"] == "device" else "TButton")
        man_copy_btns[_it["key"]].pack(side="left", padx=(6, 0))
    man_steps_lbl = ttk.Label(man_panel, text=MANUAL_STEPS, wraplength=900, justify="left", font=("Segoe UI", 10))
    man_steps_lbl.pack(fill="x", pady=(8, 0))
    ttk.Label(man_panel, text="The round button in front of 1 - 2 chooses which command 'Open a terminal for me' runs. The other commands are only shown here as text. "
                              "This tool never installs anything and never runs these commands itself.", style="Desc.TLabel", wraplength=900, justify="left").pack(fill="x", pady=(4, 0))
    man_act = ttk.Frame(man_panel)
    man_act.pack(fill="x", pady=(6, 0))
    man_chip = tk.Label(man_act, text="Waiting for you", fg="white", bg=COLORS["dim"], padx=10, pady=2, font=("Segoe UI", 9, "bold"))
    man_chip.pack(side="left")
    man_status_var = tk.StringVar(value="")
    ttk.Label(man_act, textvariable=man_status_var, font=("Segoe UI", 10)).pack(side="left", padx=8)
    man_stop_btn = ttk.Button(man_act, text="Stop waiting", state="disabled")
    man_stop_btn.pack(side="right")
    man_verify_btn = ttk.Button(man_act, text="I have signed in - Verify", style="Accent.TButton")
    man_verify_btn.pack(side="right", padx=(0, 6))
    man_term_btn = ttk.Button(man_act, text="Open a terminal for me")
    man_term_btn.pack(side="right", padx=(0, 6))
    man_result_var = tk.StringVar(value="")
    man_result_lbl = tk.Label(man_panel, textvariable=man_result_var, anchor="w", justify="left", wraplength=900, font=("Segoe UI", 10, "bold"), fg=COLORS["info"], bg=CARD)
    man_result_lbl.pack(fill="x", pady=(4, 0))

    def man_account():
        """The account the account-specific command (1b) and 'Open a terminal for me' use: the 'hint' box, else the expired account the banner is about."""
        a = hint_var.get().strip()
        return a if EMAIL_RE.match(a) else (man.get("account") if man.get("account") and EMAIL_RE.match(man["account"]) else None)

    def man_refresh():
        """Show / hide the account variant (1b) and the plugin helper (4b) and fill their text."""
        items = {i["key"]: i for i in manual_commands(man_account(), None)}
        for key in ("device-account", "plugin"):
            row = man_rows[key]
            if key in items:
                man_cmd_vars[key].set(items[key]["cmd"])
                if not row.winfo_ismapped():
                    anchor = man_rows["device"] if key == "device-account" else man_rows["adc"]
                    row.pack(fill="x", pady=(4, 0), after=anchor)
            else:
                row.pack_forget()
                if man_form.get() == key:
                    man_form.set("device")

    def man_cli_check():
        """The 'Google Cloud CLI installed?' line: PATH lookup now, `gcloud --version` (first line) in the background; install hint as TEXT when gcloud is missing."""
        text, found = gcloud_cli_line()
        man_cli_var.set(text)
        man_cli_lbl.configure(fg=COLORS["ok"] if found else COLORS["err"])
        if found:
            man_inst.pack_forget()

            def work():
                try:
                    ver = gcloud_version()
                except Exception:
                    ver = None
                msgs.put(("man", "cliinfo", ver))
            threading.Thread(target=work, daemon=True).start()
        else:
            man_inst.pack(fill="x", pady=(2, 0), after=man_cli_lbl)

    def man_cliinfo(ver):
        if ver and shutil.which("gcloud"):
            man_cli_var.set(f"Google Cloud CLI installed: {ver}")

    def man_show():
        if not man["shown"]:
            man["shown"] = True
            man_panel.pack(fill="x", pady=(6, 0), after=guide_msg)
            root.after(60, lambda: scroll_to(man_panel))

    def man_chip_set(text, kind):
        man_chip.configure(text=text, bg=COLORS.get(kind, COLORS["dim"]))

    def man_fallback(on):
        man["fallback"] = bool(on)
        if on:
            man_fb.pack(fill="x", pady=(4, 0), after=man_msg_lbl)
        else:
            man_fb.pack_forget()

    def man_open(reason=None, force=False, account=None, quiet=False, fallback=False, auto=False):
        """Show the manual sign-in panel and start waiting (checks every MANUAL_POLL_SECONDS whether the sign-in happened). Idempotent while waiting."""
        if not is_cli():
            return
        if state["auth"]["state"] == "ok" and not force and not reason and not man["active"]:
            say(f"Already signed in as {state['auth'].get('who') or '?'}. Use 'Sign in with a different account' to sign in with another account.", "ok")
            return
        if account:
            man["account"] = account
        man_show()
        man_refresh()
        man_cli_check()
        man["auto"] = man["auto"] or auto
        if fallback:
            man_fallback(True)
        if man["active"]:
            if reason:
                man_msg_var.set(reason)
            return
        man_msg_var.set(reason or "")
        man_form.set("device" if device_var.get() else "browser")
        man_result_var.set("")
        man_result_lbl.configure(fg=COLORS["info"])
        if man.get("after"):
            try:
                root.after_cancel(man["after"])
            except Exception:
                pass
        man.update(active=True, t0=time.time(), probing=False)
        man_stop_btn.state(["!disabled"])
        man_chip_set("Waiting for you", "info")
        man_status_var.set("Waiting for you to sign in...")
        man["after"] = root.after(int(MANUAL_POLL_SECONDS * 1000), man_tick)
        if not quiet:
            say("Run the first command of step 2 in your own terminal (open the URL it prints, sign in, paste the code back), then press 'I have signed in - Verify'. "
                "This window also notices the sign-in by itself.", "info")

    def man_tick():
        man["after"] = None
        if not man["active"]:
            return
        if time.time() - man["t0"] > MANUAL_POLL_CAP:
            man_stop(f"Stopped waiting after {MANUAL_POLL_CAP // 60} minutes. Press 'I have signed in - Verify' when you have signed in.")
            return
        if not man["probing"] and not (state["busy"] or state["checking"] or state["signing"] or state["listing"]):
            man["probing"] = True

            def work():
                try:
                    res = verify_signin_follow(None)
                except Exception as exc:
                    res = {"state": "not_signed_in", "who": None, "detail": str(exc), "hint": ""}
                msgs.put(("auth", res, "manual_auto"))
            threading.Thread(target=work, daemon=True).start()
        man["after"] = root.after(int(MANUAL_POLL_SECONDS * 1000), man_tick)

    def man_stop(message=None):
        man["active"] = False
        if man.get("after"):
            try:
                root.after_cancel(man["after"])
            except Exception:
                pass
            man["after"] = None
        man_stop_btn.state(["disabled"])
        if message != "":
            man_chip_set("Stopped", "dim")
            man_status_var.set(message or "Stopped waiting. Press 'I have signed in - Verify' when you have signed in.")

    def man_hide():
        man_stop("")
        man_fallback(False)
        man["auto"] = False
        if man["shown"]:
            man["shown"] = False
            man_panel.pack_forget()

    def man_verify():
        if state["checking"] or state["signing"] or state["busy"] or state["listing"]:
            man_result_var.set("Please wait for the current action to finish, then press Verify again.")
            return
        man_result_var.set("Verifying (read-only): gcloud auth list, the token check and gcloud projects list ...")
        man_result_lbl.configure(fg=COLORS["info"])
        man_chip_set("Verifying", "info")
        check_status(source="manual_verify")

    def man_signed_in(res):
        man_stop("")
        man_fallback(False)
        n_ = res.get("n_projects")
        man_chip_set("Signed in", "ok")
        man_status_var.set("Signed in.")
        man_msg_var.set("")
        man_result_var.set(f"Signed in as {res.get('who') or '?'}" + (f" - {count_of(n_, 'project')}" if n_ is not None else ""))
        man_result_lbl.configure(fg=COLORS["ok"])

    def man_failed(text):
        man_chip_set("Waiting for you" if man["active"] else "Not signed in", "info" if man["active"] else "err")
        man_result_var.set(text)
        man_result_lbl.configure(fg=COLORS["err"])

    def man_copy(key):
        cmd = man_cmd_vars[key].get()
        root.clipboard_clear()
        root.clipboard_append(cmd)
        status.set("Copied: " + cmd)

    def man_terminal():
        """'Open a terminal for me': a visible PowerShell window with the chosen gcloud auth login form (user-initiated, local-only; nothing else is ever started this way)."""
        form = man_form.get()
        ok, info = open_terminal_signin(form if form in _TERMINAL_FORMS else "device", man_account())
        if ok:
            if not man["active"]:
                man_open(force=True, quiet=True)
            man_result_var.set(f"A PowerShell window opened and runs: {info}   Complete the sign-in there, then press 'I have signed in - Verify'.")
            man_result_lbl.configure(fg=COLORS["info"])
        else:
            man_result_var.set("Could not open a terminal: " + info)
            man_result_lbl.configure(fg=COLORS["err"])

    def after_signing(fn, tries=30):
        """Run fn() once the current sign-in process has been cancelled and the window is idle again."""
        if state["signing"] and tries > 0:
            root.after(200, lambda: after_signing(fn, tries - 1))
        else:
            fn()

    def man_retry():
        sess = state.get("session")
        if sess and not sess.done.is_set():
            sess.cancel()
        man_fallback(False)
        after_signing(lambda: sign_in(force=True, hint=man_account(), method="captured"))

    def man_console():
        sess = state.get("session")
        if sess and not sess.done.is_set():
            sess.cancel()
        man_fallback(False)
        after_signing(lambda: sign_in(force=True, hint=man_account(), method="console"))

    def man_copy_command():
        cmd = man_cmd_vars["device-account" if man_account() and man_form.get() == "device-account" else "device"].get()
        root.clipboard_clear()
        root.clipboard_append(cmd)
        status.set("Copied: " + cmd)

    def on_signin_method(_event=None):
        LOGIN_OPTS["signin"] = _SESSION["signin"] = SIGNIN_KEYS.get(signin_var.get(), "manual")
        if LOGIN_OPTS["signin"] == "manual":
            say("Sign-in method: you run the command yourself. The commands are shown in step 2.", "info")
            if is_cli() and state["auth"]["state"] != "ok":
                man_open()
        else:
            man_hide()
            say(f"Sign-in method: {SIGNIN_METHOD_LABELS[LOGIN_OPTS['signin']]}. Press 'Sign in' to start it.", "info")
        update_controls()

    for _k, _b in man_copy_btns.items():
        _b.configure(command=lambda k=_k: man_copy(k))
    man_verify_btn.configure(command=man_verify)
    man_term_btn.configure(command=man_terminal)
    man_stop_btn.configure(command=man_stop)
    man_retry_btn.configure(command=man_retry)
    man_console_btn.configure(command=man_console)
    man_copycmd_btn.configure(command=man_copy_command)
    signin_combo.bind("<<ComboboxSelected>>", on_signin_method)

    # ---- device-code sign-in panel + account switching
    def sd_chip_set(text, kind):
        sd_chip.configure(text=text, bg=COLORS.get(kind, COLORS["dim"]))

    def sd_set_url(text):
        sd_url.configure(state="normal")
        sd_url.delete("1.0", "end")
        if text:
            sd_url.insert("1.0", text, "link")
        sd_url.configure(state="disabled")          # read-only, still selectable / copyable

    def sd_show():
        if not sd.winfo_ismapped():
            sd.pack(fill="x", pady=(6, 0), before=row34)
            root.after(50, lambda: scroll_to(sd))
        sd_toggle_btn.configure(text="Hide sign-in details")
        if not sd_toggle_btn.winfo_ismapped():
            sd_toggle_btn.pack(side="left", padx=(6, 0))

    def sd_toggle():
        if sd.winfo_ismapped():
            sd.pack_forget()
            sd_toggle_btn.configure(text="Show sign-in details")
        else:
            sd_show()

    def sd_raw_set(text):
        sd_raw.configure(state="normal")
        sd_raw.delete("1.0", "end")
        if text:
            sd_raw.insert("1.0", text)
        sd_raw.configure(state="disabled")

    def sd_raw_add(text):
        sd_raw.configure(state="normal")
        sd_raw.insert("end", text)
        sd_raw.see("end")
        sd_raw.configure(state="disabled")

    def sd_raw_toggle_cmd():
        if sd_raw.winfo_ismapped():
            sd_raw.pack_forget()
            sd_raw_toggle.configure(text="Show raw output")
        else:
            sd_raw.pack(fill="x", pady=(2, 0))
            sd_raw_toggle.configure(text="Hide raw output")

    def panel_reset():
        """A new attempt: clear the previous link, code and result."""
        state["session"] = None
        state["signin_url"] = None
        state["signin_note"] = None
        sd_show()
        sd_steps.configure(text="\n".join(SIGNIN_STEPS))
        sd_set_url("")
        code_var.set("")
        sd_count.set("")
        sd_result.set("")
        sd_result_lbl.configure(foreground="")
        sd_raw_set("")
        sd_cmd_var.set("")
        sd_chip_set("Starting...", "info")
        for w in (sd_open_btn, sd_copy_btn, sd_submit_btn, sd_code, sd_cancel_btn):
            set_state(w, False)

    def open_signin_url(_event=None):
        if state.get("signin_url"):
            webbrowser.open(state["signin_url"])

    def copy_signin_url():
        if state.get("signin_url"):
            root.clipboard_clear()
            root.clipboard_append(state["signin_url"])
            sd_count.set("Link copied.")

    sd_url.tag_bind("link", "<Double-Button-1>", open_signin_url)

    def begin_device_signin(hint, force):
        """(main thread) start gcloud auth login --no-launch-browser with captured output."""
        def on_event(kind, data=None):
            msgs.put(("signin", kind, data))
        sess = SignInSession(hint, on_event)
        err = sess.start()
        if err:
            sd_chip_set("Failed", "err")
            sd_result.set(err)
            sd_result_lbl.configure(foreground=COLORS["err"])
            finish_signin_thread(False, err, force)
            return
        state["session"] = sess
        state["signin_force"] = force
        set_state(sd_cancel_btn, True)
        sd_chip_set("Starting...", "info")
        sd_cmd_var.set("Command: " + sess.command_text())
        write("Running: " + sess.command_text())
        say("Sign-in started - the link and the code box appear in step 2 (Sign-in details).", "info")

    def on_signin_event(kind, data):
        sess = state.get("session")
        if kind == "url":
            state["signin_url"] = data
            sd_set_url(data)
            for w in (sd_open_btn, sd_copy_btn, sd_submit_btn, sd_code):
                set_state(w, True)
            sd_chip_set("Waiting for you", "warn")
            if man["fallback"] or man["auto"]:         # the link arrived after all (late): back to the captured view
                man_hide()
                sd_result.set("")
            for l in SIGNIN_STEPS:
                write(l)
            write("Sign-in link: " + data)
            scroll_to(sd)
            say("Open the link in step 2 (Sign-in details), sign in, then paste the verification code and press 'Submit code'.", "info")
        elif kind == "line":
            write(data)
        elif kind == "raw":
            sd_raw_add(data)
        elif kind == "tick":
            if sess and not sess.done.is_set():
                if sess.code_sent:
                    sd_count.set("checking the code ...")
                elif state.get("signin_url"):
                    sd_count.set("waiting for you to sign in... %02d:%02d" % (data // 60, data % 60))
                else:
                    sd_count.set("waiting for gcloud to print the link ... %02d:%02d" % (data // 60, data % 60))
        elif kind == "nourl":
            if not state.get("signin_url"):
                sd_chip_set("No URL received", "err")
                sd_result.set("No URL received from gcloud yet. The raw output is shown below - if it contains a link that starts with https://, open it. "
                              "Otherwise use the commands in the panel below (run one in your own terminal).")
                sd_result_lbl.configure(foreground=COLORS["err"])
                if not sd_raw.get("1.0", "end").strip():
                    sd_raw_set("(gcloud printed nothing)")
                man_open(reason=NO_URL_REASON, force=True, quiet=True, fallback=True, auto=True)
                man_status_var.set("No URL received from gcloud yet")
        elif kind == "finished":
            ok = bool(data.get("ok"))
            sd_count.set("")
            for w in (sd_submit_btn, sd_code, sd_cancel_btn):
                set_state(w, False)
            code_var.set("")
            if ok:
                sd_chip_set("Signed in", "ok")
            else:
                sd_chip_set("Cancelled" if data.get("cancelled") else ("Timed out" if data.get("expired") else "Failed"), "warn" if data.get("cancelled") else "err")
                msg_ = data.get("error") or "The sign-in did not finish."
                if not data.get("cancelled") and not data.get("expired"):
                    tail_ = [l for l in (data.get("last_lines") or []) if l.strip()][-4:]
                    msg_ += f"  Exit code {data.get('rc')}." + (" Last output: " + " | ".join(l.strip()[:160] for l in tail_) + "." if tail_ else "")
                    msg_ += " Likely causes: " + " ".join(data.get("causes") or [])
                sd_result.set(msg_)
                sd_result_lbl.configure(foreground=COLORS["err"])
                if not data.get("cancelled") and not state.get("signin_url") and not man["fallback"]:
                    man_open(reason=FAILED_REASON, force=True, quiet=True, fallback=True, auto=True)
            finish_signin_thread(ok, data.get("error") or "", state.get("signin_force", False))

    def submit_code(_event=None):
        sess = state.get("session")
        code = code_var.get().strip()
        code_var.set("")
        if not sess or not code:
            return
        if sess.submit(code):
            sd_chip_set("Checking the code", "info")
            sd_count.set("checking the code ...")
            set_state(sd_submit_btn, False)
            set_state(sd_code, False)
        else:
            sd_result.set("The sign-in has already ended - the code was not sent. Press 'Sign in' to start again.")

    def cancel_signin():
        sess = state.get("session")
        if sess:
            sess.cancel()
            sd_chip_set("Cancelling...", "warn")

    def finish_signin_thread(ok, err, force):
        """After the sign-in process ended: re-read the accounts (and pin the newly signed-in one), then tell the window."""
        def work():
            try:
                if ok:
                    rows, _e = gcloud_accounts()
                    new = next((r["account"] for r in rows if r["active"]), None)
                    if new:
                        pin_account(new)
                res = login_status(None)
            except Exception as exc:
                res = {"state": "not_signed_in", "who": None, "detail": str(exc), "hint": "Press 'Sign in' to try again.", "accounts": [], "active": None}
            if ok and res["state"] == "ok":
                msgs.put(("auth", res, "signin_new"))
            elif res["state"] == "ok":
                msgs.put(("auth", res, "check"))
            else:
                res = dict(res, detail=err or res.get("detail"))
                msgs.put(("auth", res, "signin_fail"))
        threading.Thread(target=work, daemon=True).start()

    def reset_lists():
        """The account changed: the project and cluster lists belong to the old one - clear them (they are read again)."""
        state.update(accounts=[], acct_by_id={}, acct_chosen=set(), accounts_loaded=False, crows=[], by_key={}, cchosen=set(), listed=set(), clusters={})
        CLI_TARGETS.clear()
        scope_var.set("sel" if is_cli() else "all")
        _SESSION.pop("projects", None)
        manual_var.set("")
        update_scope_labels()
        rebuild_account_list()
        rebuild_cluster_list()

    CHIP = {"active": ("Active", "ok"), "expired": ("Credentials expired - sign in again", "err"), "signed_out": ("Not signed in", "dim"),
            "unknown": ("Unknown", "dim"), "checking": ("Checking ...", "info")}

    def acct_status_of(email):
        return state["acct_status"].get(email) or {"state": "checking", "detail": ""}

    def acct_label(r, pin):
        st = acct_status_of(r["account"])
        return (r["account"] + ("  (service account)" if r["account"].endswith(".gserviceaccount.com") else "") + ("  (active)" if r["active"] else "")
                + ("  - used by this tool" if pin == r["account"] else "") + "  [" + CHIP[st["state"]][0].split(" - ")[0] + "]")

    def render_accounts():
        """The account dropdown (every account gcloud knows, status in the text), the coloured status list and the chip of the chosen one."""
        rows = state.get("acct_rows") or []
        pin = LOGIN_OPTS.get("account")
        who = state["auth"].get("who")
        state["acct_map"] = {acct_label(r, pin): r["account"] for r in rows}
        state["acct_all"] = list(state["acct_map"])
        flt = state.get("acct_typed", "").strip().lower()
        shown = [l for l in state["acct_all"] if not flt or flt in l.lower()]
        acct_combo.configure(values=shown or state["acct_all"])
        if not flt:
            acct_combo.set(next((l for l, a in state["acct_map"].items() if a == who), state["acct_all"][0] if state["acct_all"] else ""))
        acct_st_tree.delete(*acct_st_tree.get_children())
        for r in rows:
            st = acct_status_of(r["account"])
            text = CHIP[st["state"]][0] + (f" ({st['detail']})" if st["state"] == "unknown" and st.get("detail") else "")
            acct_st_tree.insert("", "end", iid=r["account"], text=r["account"] + ("   (active)" if r["active"] else "") + ("   > in use" if who == r["account"] else ""),
                                values=(text,), tags=(st["state"],))
        cur = who or (state["acct_map"].get(acct_combo.get()))
        if cur:
            st = acct_status_of(cur)
            acct_chip.configure(text=CHIP[st["state"]][0].split(" - ")[0].replace("Credentials expired", "Expired"), bg=COLORS.get(CHIP[st["state"]][1], COLORS["dim"]))
        else:
            acct_chip.configure(text="", bg=COLORS["dim"])

    def on_acct_typed(_event=None):
        text = acct_combo.get()
        if text in state.get("acct_map", {}):
            return
        state["acct_typed"] = text
        render_accounts()

    def fill_account_combo(res):
        state["acct_rows"] = res.get("accounts") or []
        state["acct_typed"] = ""
        state["acct_status"] = {k: v for k, v in state["acct_status"].items() if any(r["account"] == k for r in state["acct_rows"])}
        for r in state["acct_rows"]:
            if r["account"] in AUTH_STATE["expired"]:
                state["acct_status"][r["account"]] = {"state": "expired", "detail": ""}
        render_accounts()

    def check_accounts_async(first=None):
        """Status of every listed account in the background (at most 4 gcloud calls at once; the chosen account first)."""
        rows = list(state.get("acct_rows") or [])
        if not rows:
            return
        emails = [r["account"] for r in rows]
        if first in emails:
            emails.remove(first)
            emails.insert(0, first)
        state["acct_gen"] = gen = state.get("acct_gen", 0) + 1
        for e in emails:
            state["acct_status"][e] = {"state": "checking", "detail": ""}
        render_accounts()

        def work():
            check_accounts(emails, rows, on_result=lambda a, r: msgs.put(("acctst", gen, a, r)))
        threading.Thread(target=work, daemon=True).start()

    def on_acct_status(gen, email, res):
        if gen != state.get("acct_gen"):
            return
        state["acct_status"][email] = res
        render_accounts()
        if res["state"] == "expired" and email == state["auth"].get("who"):
            show_expired(email)
        elif res["state"] == "active" and email == state["auth"].get("who") and state["auth"]["state"] == "expired":
            state["auth"] = dict(state["auth"], state="ok")
            apply_auth(state["auth"], "check")

    def show_expired(email, reason=""):
        """The account in use has no valid credentials any more: red banner, 'Sign in' highlighted, steps 3 / 4 locked until it is renewed."""
        if state["auth"]["state"] == "expired" and state["auth"].get("who") == email:
            return
        state["acct_status"][email] = {"state": "expired", "detail": ""}
        state["auth"] = dict(state["auth"], state="expired", who=email, detail=f"Credentials for {email} expired.",
                             hint="Press Sign in to renew.", accounts=state.get("acct_rows") or [])
        set_badge("Credentials expired", "err")
        auth_msg.set(f"Credentials for {email} expired. Press Sign in to renew." + (f"  ({reason})" if reason else ""))
        say(f"Credentials for {email} expired. Press Sign in to renew.", "err")
        try:
            signin_btn.configure(style="Run.TButton")
        except Exception:
            pass
        man_open(reason=f"Credentials for {email} expired. Run one of these commands in your own terminal (or press Sign in), then press Verify.",
                 force=True, quiet=True, account=email)
        render_accounts()
        update_controls()

    def on_expiry_event(email, reason):
        if email in {r["account"] for r in (state.get("acct_rows") or [])}:
            state["acct_status"][email] = {"state": "expired", "detail": ""}
        who = state["auth"].get("who") or LOGIN_OPTS.get("account")
        if email == who or (not state["auth"].get("who") and state["auth"]["state"] == "ok"):
            show_expired(email, reason)
        else:
            render_accounts()

    def check_all():
        if not is_cli() or state["checking"] or state["signing"]:
            return
        if not state.get("acct_rows"):
            check_status(source="check")
            return
        check_accounts_async(state["auth"].get("who"))

    def on_acct_selected(_event=None):
        use_account()

    def use_account(_event=None):
        email = (state.get("acct_map") or {}).get(acct_combo.get())
        if not email or not is_cli() or state["busy"] or state["listing"] or state["checking"] or state["signing"]:
            return
        pin_account(email)                    # nothing is written to gcloud: --account EMAIL is added to every call
        state["auth"] = dict(state["auth"], state="unchecked", who=None)
        signin_btn.configure(style="TButton")
        reset_lists()
        say(f"Using {email}: every gcloud call now carries --account {email}. Re-reading the projects and clusters ...", "info")
        check_status(source="switch")

    def different_account(_event=None):
        sign_in(force=True, hint=hint_var.get().strip() or None)

    sd_toggle_btn.configure(command=sd_toggle)
    sd_raw_toggle.configure(command=sd_raw_toggle_cmd)
    sd_open_btn.configure(command=open_signin_url)
    sd_copy_btn.configure(command=copy_signin_url)
    sd_submit_btn.configure(command=submit_code)
    sd_code.bind("<Return>", submit_code)
    sd_cancel_btn.configure(command=cancel_signin)
    use_acct_btn.configure(command=use_account)
    check_all_btn.configure(command=check_all)
    acct_combo.bind("<<ComboboxSelected>>", on_acct_selected)
    acct_combo.bind("<KeyRelease>", on_acct_typed)
    diff_btn.configure(command=different_account)
    for _w in (sd_open_btn, sd_copy_btn, sd_submit_btn, sd_code, sd_cancel_btn):
        set_state(_w, False)


    # ---- step 3 loading (projects are read once and cached for the session; 'Reload projects' refreshes)
    def load_accounts_async(force=False):
        if state["acct_loading"] or (state["accounts_loaded"] and not force):
            return
        state["acct_loading"] = True
        acct_status.set("Loading projects ...")
        update_controls()

        def work():
            try:
                accts, err = load_accounts()
            except Exception as exc:
                accts, err = [], str(exc)
            for a in accts:
                a["hay"] = f"{a['name']} {a['code']} {a['info']}".lower()
            msgs.put(("accounts", accts, err))
        threading.Thread(target=work, daemon=True).start()

    def on_accounts(accts, err):
        state["acct_loading"] = False
        state["accounts_loaded"] = True
        state["accounts"] = accts
        state["acct_by_id"] = {a["id"]: a for a in accts}
        state["acct_chosen"] &= set(state["acct_by_id"])
        if ACCOUNT0 and not state["pre"]:
            state["pre"] = True
            if ACCOUNT0 in state["acct_by_id"]:
                state["acct_chosen"] = {ACCOUNT0}
                scope_var.set("sel")
        if not state["acct_chosen"] and _SESSION.get("projects"):         # the projects selected earlier in this session
            state["acct_chosen"] = set(_SESSION["projects"]) & set(state["acct_by_id"])
            if state["acct_chosen"]:
                scope_var.set("sel")
        update_scope_labels()
        update_controls()
        rebuild_account_list()
        rebuild_cluster_list()
        state["acct_err"] = err
        if state.get("signin_note"):
            who_, state["signin_note"] = state["signin_note"], None
            sd_result.set(f"Signed in as {who_} - {count_of(len(accts), 'project')}" + ("" if accts else " (none visible to this account)") + ".")
            sd_result_lbl.configure(foreground=COLORS["ok"] if accts else COLORS["warn"])
        if not accts:
            acct_status.set("No projects found. Your Google account can see no project (or the list could not be read). Fix: press 'Sign in' and use another account, ask for the Viewer role on a project, or run 'gcloud config set project ID'; then press 'Reload projects'." + (f" (details: {err})" if err else ""))
            if state["auth"]["state"] in ("ok", "unchecked") or not is_cli():
                say(acct_status.get(), "warn" if is_cli() else "info")
            return
        usable = len([a for a in accts if a.get("usable", True)])
        acct_status.set(f"{count_of(len(accts), 'project')} loaded" + (f" ({usable} usable)" if usable != len(accts) else "") + ".")
        if is_cli() and state["auth"]["state"] == "ok":
            say(f"{count_of(len(accts), 'project')} loaded. Step 3: select one or more projects (Ctrl/Shift-click, or 'Select all (shown)'), "
                "then press 'Collect clusters from selected projects' in step 4. Nothing is searched until you press it.", "ok")

    # ---- step 4 loading (clusters are listed per project in parallel; the list grows while it runs)
    def maybe_auto_list():
        """Clusters are collected only when the user presses the button (searching every project is slow): nothing happens here."""
        return

    def load_clusters(_event=None, refresh=False):
        if state["busy"]:
            status.set("Wait for the current run to finish (or press Stop) before reloading the cluster list.")
            return
        if state["listing"]:
            return
        sync_login_opts()
        if not is_cli():
            if src_var.get() == "all" and not shutil.which("gcloud"):
                src_var.set("menu")
                say("The Google Cloud CLI (gcloud) is not installed, so only the clusters from the gkelogin menu can be listed. "
                    "Install gcloud and choose 'Collect clusters with gcloud' to see more.", "warn")
            if src_var.get() == "all":
                collect(refresh)
                return
            CLI_TARGETS.clear()
            state["listing"] = True
            cl_status.set("Loading the cluster list from gkelogin ...")
            update_controls()
            threading.Thread(target=lambda: msgs.put(("clusters", list_clusters())), daemon=True).start()
            return
        if state["auth"]["state"] != "ok":
            say("Sign in first (step 2): press 'Sign in', or 'Check status' if you already signed in.", "warn")
            return
        collect(refresh)

    def refresh_selected(_event=None):
        load_clusters(refresh=True)

    def list_tick():
        if state["listing"]:
            cluster_hint()
            root.after(1000, list_tick)

    def collect(refresh=False):
        """'Collect clusters from selected projects': ONLY the selected projects are searched (parallel `gcloud container clusters list --project P`);
        projects already collected are skipped unless refresh=True ('Refresh selected')."""
        accts = selected_projects()
        if not accts:
            say(COLLECT_HINT, "warn")
            cl_status.set(COLLECT_HINT)
            return
        todo = [a for a in accts if refresh or a["id"] not in state["listed"]]
        if not todo and not is_cli():
            pass                       # custom login: nothing new to search, but the gkelogin menu is read again and merged (no cluster call)
        elif not todo:
            say(f"The selected project(s) are already collected ({count_of(len(state['crows']), 'cluster')} in the list). Press 'Refresh selected' to read them again.", "info")
            cluster_hint()
            return
        if len(todo) > CONFIRM_OVER and not confirm_many(len(todo)):
            say("Cluster collection cancelled - select fewer projects, or confirm to continue.", "info")
            return
        ids = {a["id"] for a in todo}
        state["crows"] = [r for r in state["crows"] if r.get("account") not in ids]       # these are listed again
        state["listed"] -= ids
        index_rows()
        cancel = threading.Event()
        state.update(listing=True, list_cancel=cancel, list_done=0, list_total=len(todo), list_t0=time.time())
        list_bar.configure(maximum=max(1, len(todo)), value=0)
        simple = [{"id": a["id"], "name": a["name"]} for a in todo]
        say(f"Collecting clusters from {count_of(len(todo), 'selected project')} ... the list below fills in as results arrive (Stop cancels).", "info")
        update_controls()
        root.after(1000, list_tick)

        def work():
            try:
                menu = None
                if not is_cli():
                    menu = list_clusters()
                    msgs.put(("menu", dict(menu)))
                    res = login_status()
                    if res["state"] != "ok":
                        msgs.put(("srcfallback", res.get("detail") or "gcloud is not signed in", dict(menu)))
                        return
                found, failed = (scan_clusters(simple, lambda l: msgs.put(("line", l)), lambda d, t: msgs.put(("lprog", d, t)), cancel,
                                               lambda batch: msgs.put(("cbatch", list(batch))), inventory=True) if simple else ([], 0))
                msgs.put(("cdone", found, failed, cancel.is_set(), [a["id"] for a in simple]))
            except Exception as exc:
                msgs.put(("cerr", str(exc), [a["id"] for a in simple]))
        threading.Thread(target=work, daemon=True).start()

    def show_menu_clusters(menu, note=None):
        """The list is the gkelogin menu only (numbers = the menu numbers)."""
        CLI_TARGETS.clear()
        state["listing"] = False
        state["clusters"] = dict(menu)
        set_rows([prepare({"key": k, "number": k, "label": v, "name": v, "where": "", "account": None, "account_name": ""})
                  for k, v in sorted(menu.items(), key=lambda kv: numeric(kv[0]))])
        update_controls()
        if menu:
            cl_status.set(f"{len(menu)} cluster(s) from gkelogin." if not note else f"{len(menu)} cluster(s) from the gkelogin menu only.")
            say(note or f"{len(menu)} cluster(s) found. Select one or more in step 4, then press 'Login & Debug'.", "warn" if note else "ok")
            status.set(f"{len(menu)} cluster(s) found. Select one or more, then press Login & Debug.")
        else:
            cl_status.set("No clusters.")
            say((note + " " if note else "") + "Could not read the cluster list from gkelogin - type the cluster number(s) in the box in step 4, or check gkelogin.exe / clusters.json.", "warn")
            status.set("Could not read the cluster list from gkelogin - type the cluster number(s) in the box.")

    def on_source_change(_event=None):
        if state["busy"] or state["listing"]:
            src_var.set("menu" if src_var.get() == "all" else "all")
            status.set("Wait for the current run / listing to finish (or press Stop) before changing the cluster list.")
            return
        state["src_user"] = True
        state.update(crows=[], by_key={}, cchosen=set(), clusters={}, listed=set(), menu={}, src_fallback=False)
        manual_var.set("")
        update_collect_button()
        rebuild_cluster_list()
        cluster_hint()
        if src_var.get() == "menu":
            load_clusters()              # the gkelogin menu: instant, no cloud call

    def on_clusters_done(found, failed, cancelled, ids):
        state["listing"] = False
        scanned = set(ids)
        rows = [r for r in state["crows"] if r.get("account") not in scanned]
        rows += [prepare(c) for c in found]
        seen, uniq = set(), []
        for r in rows:
            if r["key"] not in seen:                  # the same cluster can be reached through two profiles
                seen.add(r["key"])
                uniq.append(r)
        rows = [r for r in uniq if not r.get("exe_only")]
        if not is_cli():                       # custom login: the clusters that are in the gkelogin menu keep using it (the others use gcloud)
            for r in rows:
                r.pop("exe_number", None)
            rows = merge_menu(rows, state["menu"])
            state["src_fallback"] = False
        if not cancelled:
            state["listed"] |= scanned
        aidx = {a["id"]: i for i, a in enumerate(state["accounts"])}
        rows.sort(key=lambda r: aidx.get(r.get("account"), len(aidx)))             # stable: the listed order stays inside a project
        clusters = register_clusters(rows, len(state["listed"] | scanned) > 1)
        for i, r in enumerate(rows, start=1):
            r["number"], r["label"] = str(i), clusters[str(i)]
            prepare(r)
        state["clusters"] = clusters
        set_rows(rows)
        list_bar.configure(value=state["list_total"])
        n = len(rows)
        skipped = f" {failed} lookup(s) failed - see the log." if failed else ""
        summary = scan_summary([r for r in rows if not r.get("exe_only")])
        if cancelled:
            cl_status.set(f"Listing stopped - {count_of(n, 'cluster')} so far.")
            say(f"Listing stopped - {count_of(n, 'cluster')} found so far. Press 'Reload clusters' to list again.", "warn")
        elif not n:
            cl_status.set("No clusters found.")
            say("No GKE clusters found in the selected project(s). Check that the Kubernetes Engine API is enabled and that you have access, or click 'All projects'." + skipped, "warn")
        else:
            cl_status.set(summary + ".")
            say(f"{summary}. Step 4: search / select the clusters, then press 'Login & Debug'."
                + ("  Clusters that are not in the gkelogin menu are logged in with gcloud." if not is_cli() else ""), "ok" if not failed else "warn")
        update_controls()

    # ---- run options, steps list
    def plan(options=None):
        """The rows of the steps list: login, context, project, then the collection steps of the ticked sections (and the ones read silently)."""
        pl = section_plan()
        options = options if options is not None else {"sections": selected_ids(), "gcp": not HARD_GCP_OFF}
        silent_steps = {SECTION_BY_ID[i]["step"] for i in pl["silent"]}
        rows = [("login", "Login (Cloud CLI: gcloud)" if is_cli() else "Login (gkelogin)"), ("context", "Select kubectl context"), ("profile", "Select GCP project")]
        rows += [(k, t + (" (read silently)" if k in silent_steps else "")) for k, t, _ in run_steps(options)] + [("report", "Write HTML report")]
        return rows

    def step_values(key, st, secs=None):
        return (ICON.get(st, st), f"{secs:.1f}s" if secs else "", f"{STEP_ICON.get(key, '')} {STATUS_MARK.get(st, '')}".strip())

    def step_tags(key, st):
        idx = list(steps.get_children()).index(key) if steps.exists(key) else 0
        return (st,) + (("odd",) if idx % 2 else ())

    def reset_steps():
        steps.delete(*steps.get_children())
        rows = plan()
        for key, title in rows:
            steps.insert("", "end", iid=key, text=title, values=step_values(key, "pending"), tags=(("odd",) if len(steps.get_children()) % 2 else ()))
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
        """Read the login method and the chosen project (and region) into the options a run (or a cluster listing) uses."""
        LOGIN_OPTS["method"] = "cli" if method_combo.get() == LOGIN_LABELS["cli"] else "exe"
        LOGIN_OPTS["device_code"] = device_var.get()
        LOGIN_OPTS["signin"] = SIGNIN_KEYS.get(signin_var.get(), LOGIN_OPTS.get("signin") or "manual")
        LOGIN_OPTS["all_clusters"] = False        # the window has its own 'Cluster list' choice (list_clusters() then means the gkelogin menu)
        picked = state["acct_chosen"] if scope_var.get() == "sel" else set()
        if state["pre"] or not ACCOUNT0:      # keep the command-line value until the list has been read
            GCP_OPTS["project"] = next(iter(picked)) if len(picked) == 1 else None

    def on_method_change(_event=None):
        if state["busy"] or state["listing"]:
            method_combo.set(LOGIN_LABELS[LOGIN_OPTS["method"]])
            status.set("Wait for the current run / listing to finish (or press Stop) before changing the login method.")
            return
        state.update(crows=[], by_key={}, cchosen=set(), clusters={}, listed=set(), checking=False, signing=False, menu={}, src_fallback=False)
        CLI_TARGETS.clear()
        manual_var.set("")
        sync_login_opts()
        apply_method_ui()
        update_controls()
        rebuild_cluster_list()
        reset_steps()
        if is_cli():
            check_status()
        else:
            load_clusters()
        if not (is_cli() and NEED_SIGNIN):
            load_accounts_async()

    def start():
        if state["busy"]:
            return
        if state["listing"]:
            status.set("Wait for the cluster listing to finish (or press Stop).")
            return
        sync_login_opts()
        if is_cli() and state["auth"]["state"] != "ok":
            say("Sign in first (step 2): press 'Sign in', or 'Check status' if you already signed in.", "warn")
            status.set("Not signed in - see step 2.")
            return
        selected = chosen_clusters()
        if not selected:
            status.set("Select at least one cluster in the list (or type numbers like 1,3).")
            say("Select at least one cluster in step 4 (or type numbers like 1,3), then press 'Login & Debug'.", "warn")
            return
        try:
            minutes = max(1, int(minutes_var.get()))
        except ValueError:
            status.set("Minutes must be a number.")
            return
        ids = selected_ids()
        if not ids:
            status.set("Select at least one section to collect (tab 2 - What to collect).")
            say("Nothing is selected to collect. Tick at least one section in tab 2 (What to collect), or press 'Select all'.", "warn")
            nb.select(tab2)
            return
        try:
            workers = max(1, int(workers_var.get()))
        except ValueError:
            workers = PARALLEL_WORKERS
        options = {"gcp": not HARD_GCP_OFF, "logs": True, "all_logs": alllogs_var.get(), "log_namespaces": ns_var.get(),
                   "sections": ids, "workers": workers, "task_progress": lambda d, t: msgs.put(("tasks", d, t))}
        state.update(busy=True, cancel=threading.Event(), html=None, t0=time.time(), n=len(selected))
        update_controls()
        open_btn.state(["disabled"])
        tasks_var.set("")
        nb.select(tab3)
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
        if state["busy"] and state["cancel"] is not None:
            state["cancel"].set()
            stop_btn.state(["disabled"])
            status.set("Stopping after the current step ... a partial report is saved, remaining clusters are skipped.")
        elif state["listing"] and state["list_cancel"] is not None:
            state["list_cancel"].set()
            stop_btn.state(["disabled"])
            cl_status.set("Stopping the cluster listing ...")

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
        update_controls()
        status.set(ok_text)

    RUN_TAG = {"running": "running", "ok": "ok", "failed": "failed", "credentials expired": "failed", "stopped (partial)": "partial"}

    def poll():
        try:
            while True:
                try:
                    exp_acct, exp_reason = EXPIRY_EVENTS.get_nowait()
                except queue.Empty:
                    break
                on_expiry_event(exp_acct, exp_reason)
        except Exception:
            pass
        try:
            while True:
                msg = msgs.get_nowait()
                kind = msg[0]
                if kind == "line":
                    write(msg[1])
                elif kind == "say":
                    say(msg[1], msg[2])
                elif kind == "auth":
                    apply_auth(msg[1], msg[2])
                elif kind == "acctst":
                    on_acct_status(msg[1], msg[2], msg[3])
                elif kind == "man":
                    man_cliinfo(msg[2])
                elif kind == "signin_begin":
                    begin_device_signin(msg[1], msg[2])
                elif kind == "signin":
                    on_signin_event(msg[1], msg[2])
                elif kind == "accounts":
                    on_accounts(msg[1], msg[2])
                elif kind == "lprog":
                    state["list_done"], state["list_total"] = msg[1], msg[2]
                    list_bar.configure(maximum=max(1, msg[2]), value=msg[1])
                    cluster_hint()
                    status.set(f"Listing clusters: {msg[1]}/{msg[2]} projects ...")
                elif kind == "cbatch":
                    for c in msg[1]:
                        if c["key"] not in state["by_key"]:
                            state["crows"].append(prepare(c))
                            state["by_key"][c["key"]] = c
                    rebuild_soon()
                elif kind == "lstart":
                    state["list_total"] = msg[1]
                    list_bar.configure(maximum=max(1, msg[1]), value=0)
                    cl_status.set(f"Listing clusters in {count_of(msg[1], 'project')} ...")
                elif kind == "menu":
                    state["menu"] = dict(msg[1])
                elif kind == "srcfallback":
                    state["src_fallback"] = True
                    src_var.set("menu")
                    show_menu_clusters(msg[2], f"Google Cloud CLI (gcloud) is not usable ({msg[1]}), so only the clusters from the gkelogin menu are shown. "
                                               "Sign in with 'gcloud auth login', then press 'Reload clusters' to list every cluster you can access.")
                elif kind == "cdone":
                    on_clusters_done(msg[1], msg[2], msg[3], msg[4])
                    status.set("Cluster listing done." if not msg[3] else "Cluster listing stopped.")
                elif kind == "cerr":
                    state["listing"] = False
                    cl_status.set("Listing failed.")
                    say(f"Could not list the clusters: {msg[1]}", "err")
                    update_controls()
                elif kind == "step":
                    _, key, st, secs = msg
                    if steps.exists(key):
                        steps.item(key, values=step_values(key, st, secs), tags=step_tags(key, st))
                    if st in ("done", "skipped", "failed") and steps.exists(key):
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
                    findings.insert("", "end", values=(sev, t, sym(SEVERITY_DOT.get(sev, "\U0001F535"))),
                                    tags=(sev,) + (("odd",) if len(findings.get_children()) % 2 else ()))
                    findings.yview_moveto(1.0)
                elif kind == "tasks":
                    tasks_var.set(f"{msg[1]} of {msg[2]} collection tasks done")
                elif kind == "clusters":          # the custom login's own cluster list (the gkelogin menu only)
                    show_menu_clusters(msg[1])
                elif kind == "done":
                    results = msg[1]
                    ok = [r for r in results["items"] if r["html"]]
                    state["html"] = results["index"] or (ok[0]["html"] if ok else None)
                    if not is_cli():
                        load_accounts_async(force=True)          # gkelogin may have created / refreshed the projects
                        pass
                    progress_bar.configure(value=state["total"])
                    if state["html"]:
                        open_btn.state(["!disabled"])
                    bad = [r["label"] for r in results["items"] if r["status"] in ("failed", "credentials expired")]
                    finish(f"Done: {len(ok)} of {len(results['items'])} cluster(s) reported"
                           + (f" ({len(bad)} failed: {', '.join(bad)})" if bad else "")
                           + ". Click 'Open HTML report'." if state["html"] else "Finished, but no report could be written - see the log.")
                    if state["html"] and open_var.get():
                        open_report()
                    if not is_cli() and state["src_fallback"]:      # gkelogin may have signed gcloud in: try the full list again
                        load_clusters()
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
    refresh_sel_btn.configure(command=refresh_selected)
    open_btn.configure(command=open_report)
    folder_btn.configure(command=open_folder)
    select_all_btn.configure(command=select_all)
    clear_btn.configure(command=clear_selection)
    signin_btn.configure(command=sign_in)
    check_btn.configure(command=check_status)
    acct_all_btn.configure(command=acct_select_all)
    acct_clear_btn.configure(command=acct_clear)
    acct_reload_btn.configure(command=lambda: load_accounts_async(force=True))
    scope_all_rb.configure(command=on_scope_radio)
    scope_sel_rb.configure(command=on_scope_radio)
    cluster_tree.bind("<<TreeviewSelect>>", on_tree_select)
    acct_tree.bind("<<TreeviewSelect>>", on_acct_select)
    run_tree.bind("<Double-1>", open_cluster_report)
    filter_var.trace_add("write", lambda *_: rebuild_cluster_list())
    acct_filter.trace_add("write", lambda *_: rebuild_account_list())
    manual_var.trace_add("write", lambda *_: update_selected_label())
    method_combo.bind("<<ComboboxSelected>>", on_method_change)
    device_var.trace_add("write", lambda *_: sync_login_opts())
    src_all_rb.configure(command=on_source_change)
    src_menu_rb.configure(command=on_source_change)

    # ---- 'What to collect': quick buttons, counter, note, remembered selection (the tick variables live as long as the window)
    def refresh_sections(*_):
        ids = selected_ids()
        n = len(ids)
        sec_count.set(f"{n} of {len(SECTIONS)} sections selected")
        chip_sections.configure(text=f"{n} of {len(SECTIONS)} sections")
        pl = section_plan()
        bits = []
        if pl["silent"]:
            def needed_by(sid):
                return ", ".join(SECTION_BY_ID[x]["title"] for x in ids if any(nn == sid for nn, _p in SECTION_BY_ID[x]["needs"]))
            bits.append("Read silently and not shown: " + "; ".join(f"{SECTION_BY_ID[sid]['title']} (needed by {needed_by(sid)})" for sid in pl["silent"]) + ".")
        if HARD_GCP_OFF:
            bits.append("Google Cloud lookups are switched off (--no-gcp): no gcloud call is made.")
        if n == 0:
            bits.append("Nothing is selected - tick at least one section before you start.")
        sec_note.set(" ".join(bits))
        if not state["busy"]:
            reset_steps()

    sec_buttons["all"].configure(command=lambda: set_sections({s_["id"] for s_ in SECTIONS}))
    sec_buttons["none"].configure(command=lambda: set_sections(set()))
    sec_buttons["net"].configure(command=lambda: set_sections(set(NETWORKING_IDS)))
    sec_buttons["nonet"].configure(command=lambda: set_sections({s_["id"] for s_ in SECTIONS} - set(NETWORKING_IDS)))
    if SECTION_CHOICE["sections"] is not None or SECTION_CHOICE["skip_sections"]:        # the command line chose sections (--sections / --only-networking ...)
        set_sections(set(resolve_sections({"sections": SECTION_CHOICE["sections"], "skip_sections": SECTION_CHOICE["skip_sections"]})["selected"]))
    for var in sec_vars.values():
        var.trace_add("write", refresh_sections)
    minutes_var.trace_add("write", refresh_banner)
    reset_steps()
    refresh_sections()
    apply_method_ui()
    update_controls()
    _GUI.update(banner=banner, banner_sub=banner_sub, notebook=nb, tabs=(tab1, tab2, tab3), sec_vars=sec_vars, sec_checks=sec_checks, sec_descs=sec_descs,
                sec_buttons=sec_buttons, sec_count=sec_count, sec_note=sec_note, selected_sections=selected_ids, set_sections=set_sections,
                workers_var=workers_var, tasks_var=tasks_var, minutes_var=minutes_var, chip_sections=chip_sections, collect_card=collect, wide=WIDE,
                style=style, sym=sym, steps_plan=plan, section_plan=section_plan, statusbar=statusbar, actionbar=actionbar, logo_draw=draw_logo)
    _GUI.update(root=root, cluster_tree=cluster_tree, run_btn=run_btn, stop_btn=stop_btn, open_btn=open_btn, steps=steps,
                findings=findings, text=text, status=status, state=state, counter_vars=counter_vars,
                open_var=open_var, gcp_var=gcp_var, logs_var=logs_var, progress=progress_bar,
                alllogs_var=alllogs_var, ns_var=ns_var, load_profiles=lambda: load_accounts_async(force=True),
                select_all_btn=select_all_btn, clear_btn=clear_btn, filter_var=filter_var, manual_var=manual_var,
                run_tree=run_tree, sel_text=sel_text, chosen_clusters=chosen_clusters,
                method_combo=method_combo, device_var=device_var, on_method_change=on_method_change,
                acct_tree=acct_tree, acct_filter=acct_filter, acct_count=acct_count, cl_count=cl_count, scope_var=scope_var,
                acct_all_btn=acct_all_btn, acct_clear_btn=acct_clear_btn, acct_reload_btn=acct_reload_btn, refresh_btn=refresh_btn, refresh_sel_btn=refresh_sel_btn,
                on_scope_change=on_scope_change, acct_sel_var=acct_sel_var, selected_projects=selected_projects, collect=collect, rebuild_cluster_list=rebuild_cluster_list,
                signin_btn=signin_btn, check_btn=check_btn, auth_badge=auth_badge, auth_msg=auth_msg, guide_msg=guide_msg,
                acct_status=acct_status, cl_status=cl_status, method_info=method_info, search_icon=SEARCH_ICON,
                s3=s3, s4=s4, list_bar=list_bar, acct_search=acct_search, cl_search=cl_search, scope_all_rb=scope_all_rb,
                scope_sel_rb=scope_sel_rb, check_status=check_status, src_var=src_var, src_all_rb=src_all_rb, src_menu_rb=src_menu_rb,
                on_source_change=on_source_change, sign_in=sign_in, load_clusters=load_clusters,
                acct_combo=acct_combo, use_acct_btn=use_acct_btn, diff_btn=diff_btn, hint_var=hint_var, sd=sd, sd_url=sd_url, sd_chip=sd_chip,
                sd_count=sd_count, sd_result=sd_result, sd_code=sd_code, code_var=code_var, sd_submit_btn=sd_submit_btn, sd_cancel_btn=sd_cancel_btn,
                sd_open_btn=sd_open_btn, sd_copy_btn=sd_copy_btn, sd_raw=sd_raw, use_account=use_account, different_account=different_account,
                acct_chip=acct_chip, acct_st_tree=acct_st_tree, check_all_btn=check_all_btn, check_all=check_all, show_expired=show_expired,
                render_accounts=render_accounts, on_acct_typed=on_acct_typed, sd_steps=sd_steps,
                signin_var=signin_var, signin_combo=signin_combo, on_signin_method=on_signin_method, man=man, man_panel=man_panel, man_open=man_open,
                man_hide=man_hide, man_verify_btn=man_verify_btn, man_term_btn=man_term_btn, man_stop_btn=man_stop_btn, man_chip=man_chip,
                man_status_var=man_status_var, man_result_var=man_result_var, man_msg_var=man_msg_var, man_cli_var=man_cli_var, man_cli_lbl=man_cli_lbl,
                man_inst=man_inst, man_form=man_form, man_cmd_vars=man_cmd_vars, man_copy_btns=man_copy_btns, man_rows=man_rows, man_fb=man_fb,
                man_retry_btn=man_retry_btn, man_console_btn=man_console_btn, man_copycmd_btn=man_copycmd_btn, man_instr_var=man_instr_var,
                man_steps_lbl=man_steps_lbl, sd_raw_toggle=sd_raw_toggle, sd_cmd_var=sd_cmd_var, sd_toggle=sd_toggle, man_tick=man_tick,
                man_refresh=man_refresh, man_cli_check=man_cli_check)
    if is_cli():
        check_status()
    else:
        load_clusters()
    if not (is_cli() and NEED_SIGNIN):
        load_accounts_async()
    poll()
    root.mainloop()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli_progress(key, status, secs):
    if status != "running":
        with _EMIT_LOCK:
            print(f"  [{key}] {status}" + (f" ({secs:.1f}s)" if secs else ""), flush=True)


def print_sections(selected=None):
    """--list-sections: every report section with its id, what it shows, what it needs, and whether this run would collect it."""
    print("Report sections (use the id with --sections / --skip-sections; --only-networking / --no-networking switch the networking group):")
    for sec in SECTIONS:
        needs = "; ".join(f"{SECTION_BY_ID[n]['title']}" + (f" ({', '.join(parts)} only)" if parts else "") for n, parts in sec["needs"])
        mark = "" if selected is None else ("[x] " if sec["id"] in selected else "[ ] ")
        print(f"  {mark}{sec['num']:>2}. {sec['id']:<12} {sec['title']}")
        print(f"        {sec['desc']}" + (f"  Networking group." if sec.get("group") == "networking" else ""))
        if needs:
            print(f"        needs (collected silently when not selected): {needs}")
    print("The cluster data (every kubectl read) is always collected: every section works from it.")


def main():
    global LOOKBACK_MINUTES, GKELOGIN_EXE, LOG_TAIL_LINES, SUPPORT_LABEL, TRAFFIC_SAMPLE_SECONDS, PARALLEL_WORKERS
    parser = argparse.ArgumentParser(description="GKE debugger: log in, then show what happened in the last N minutes")
    parser.add_argument("--minutes", type=int, default=None, help=f"time window in minutes (default {LOOKBACK_MINUTES})")
    parser.add_argument("--cluster", help="cluster number(s) or name to log in to and debug (no GUI): 3 | 1,3,5 | 2-4 | all | my-cluster. "
                                          "Several clusters run one after another and get a combined summary page")
    parser.add_argument("--name", help="label for the report when ONE cluster is given with --cluster (default: from the cluster list)")
    parser.add_argument("--list", action="store_true", help="print the clusters (name, location, project with --all-clusters / --login-method cli; "
                                                            "otherwise what gkelogin offers) and exit")
    parser.add_argument("--all-clusters", action="store_true",
                        help="list / select among EVERY cluster the signed-in gcloud user can access (all projects; one Cloud Asset Inventory search or parallel "
                             "`gcloud container clusters list`). With the exe login a cluster that is not in the gkelogin menu is logged in with "
                             "`gcloud container clusters get-credentials`. Works with --list and --cluster N|name|all")
    parser.add_argument("--skip-login", action="store_true", help="don't run gkelogin; use the current kubectl context")
    parser.add_argument("--gkelogin", help="path to gkelogin.exe")
    parser.add_argument("--no-gui", action="store_true", help="never open the GUI")
    parser.add_argument("--login-method", choices=["exe", "cli"], default="exe",
                        help="how to log in: exe = the custom gkelogin.exe (default); cli = the standard Google Cloud CLI (gcloud) - then --list / --cluster use the cluster list read from gcloud")
    parser.add_argument("--device-code", "--no-launch-browser", dest="device_code", action="store_true", default=True,
                        help="DEFAULT. With --login-method cli: sign in without a browser pop-up (gcloud auth login --no-launch-browser): "
                             "you get a link, sign in on any device, and paste the verification code back")
    parser.add_argument("--no-device-code", dest="device_code", action="store_false",
                        help="use the normal browser sign-in (gcloud auth login) instead of the device code flow")
    parser.add_argument("--signin-method", choices=["manual", "captured", "console"], default=None,
                        help="how to sign in with --login-method cli: manual (default) = the commands are printed, you run one in your own terminal and press Enter "
                             "(this tool then verifies read-only); captured = gcloud auth login --no-launch-browser runs here, you get the link and paste the code; "
                             "console = gcloud auth login in its own console window")
    parser.add_argument("--list-accounts", action="store_true", help="list the accounts gcloud knows (active one marked) with their status (Active / credentials expired) and exit; read-only")
    parser.add_argument("--account", default=None, help="use this signed-in Google account for every gcloud call (--account EMAIL); nothing is written to gcloud's configuration")
    parser.add_argument("--gke-cluster", help="GKE cluster name for the GCP checks (default: found from the kubectl context / API endpoint)")
    parser.add_argument("--location", help="zone or region of the GKE cluster (use with --gke-cluster)")
    parser.add_argument("--project", help="GCP project id to use; with --list / --all-clusters / --cluster NAME also a comma list (a,b,c) or 'all' = the projects whose "
                                          "clusters are collected (nothing else is searched). Default: the currently configured gcloud project")
    parser.add_argument("--context", help="kubectl context to use (default: matched from the selected cluster)")
    parser.add_argument("--list-projects", action="store_true", help="list the GCP projects `gcloud` can see and exit")
    parser.add_argument("--open", action="store_true", help="open the HTML report in your browser when done")
    parser.add_argument("--no-logs", action="store_true", help="skip pulling pod logs")
    parser.add_argument("--logs-all", action="store_true", help="also read logs of ALL running pods (capped), not just unhealthy / warning / core add-on pods")
    parser.add_argument("--log-namespaces", default="", help="with --logs-all: only these namespaces (comma separated)")
    parser.add_argument("--log-lines", type=int, default=None, help=f"max log lines per container (default {LOG_TAIL_LINES})")
    parser.add_argument("--traffic-sample", type=int, default=None,
                        help=f"seconds to sample live pod/node traffic from the kubelet (default {TRAFFIC_SAMPLE_SECONDS}, 0 = skip)")
    parser.add_argument("--no-gcp", action="store_true", help="skip the gcloud / GCP sections (kubectl data only; alias of unticking the cluster infrastructure section, with no gcloud call at all)")
    parser.add_argument("--workers", type=int, default=None, help=f"collection tasks that run in parallel after the login (default {PARALLEL_WORKERS}; 1 = one after another, the old order)")
    parser.add_argument("--sections", default=None, help="collect ONLY these report sections (comma separated ids, see --list-sections), e.g. nodes,pods,networking")
    parser.add_argument("--skip-sections", default=None, help="do not collect these report sections (comma separated ids), e.g. logs,timeline")
    parser.add_argument("--only-networking", action="store_true", help="collect only the Network and traffic section (the cloud details it needs are read silently)")
    parser.add_argument("--no-networking", action="store_true", help="collect everything except the Network and traffic section")
    parser.add_argument("--list-sections", action="store_true", help="print the report sections and exit")
    parser.add_argument("--support-label", default=None, help=f"namespace label that names the team to contact (default {SUPPORT_LABEL})")
    args = parser.parse_args()

    if args.workers is not None:
        if args.workers < 1:
            parser.error("--workers must be 1 or more")
        PARALLEL_WORKERS = args.workers
    chosen = skipped = None
    try:
        if args.sections is not None:
            chosen = parse_section_list(args.sections)
        if args.skip_sections is not None:
            skipped = parse_section_list(args.skip_sections)
    except ValueError as exc:
        parser.error(str(exc))
    if args.only_networking and (args.no_networking or args.sections is not None):
        parser.error("--only-networking cannot be combined with --no-networking or --sections")
    if args.only_networking:
        chosen = list(NETWORKING_IDS)
    if args.no_networking:
        skipped = list(dict.fromkeys((skipped or []) + NETWORKING_IDS))
    SECTION_CHOICE["sections"], SECTION_CHOICE["skip_sections"] = chosen, skipped
    if args.list_sections:
        print_sections(resolve_sections({"sections": chosen, "skip_sections": skipped, "logs": not args.no_logs, "gcp": not args.no_gcp})["selected"])
        return
    if not resolve_sections({"sections": chosen, "skip_sections": skipped, "logs": not args.no_logs, "gcp": not args.no_gcp})["selected"]:
        parser.error("no section is selected - nothing to collect (see --list-sections)")

    if args.minutes:
        LOOKBACK_MINUTES = args.minutes
    if args.log_lines:
        LOG_TAIL_LINES = args.log_lines
    if args.support_label:
        SUPPORT_LABEL = args.support_label
    if args.traffic_sample is not None:
        TRAFFIC_SAMPLE_SECONDS = max(0, args.traffic_sample)
    _scope, _single = parse_project_scope(args.project)
    GCP_OPTS.update(cluster=args.gke_cluster, location=args.location, project=_single, enabled=not args.no_gcp, scope=_scope)
    if args.gkelogin:
        GKELOGIN_EXE = args.gkelogin
    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass
    signin = args.signin_method or ("console" if not args.device_code else default_signin_method())
    LOGIN_OPTS.update(method=args.login_method, device_code=args.device_code, all_clusters=args.all_clusters, account=args.account or None, signin=signin)

    if args.list_accounts:
        print_accounts()
        return

    if args.list_projects:
        found = list_gcp_projects()
        print("GCP projects (gcloud projects list):")
        for pid, info in sorted(found.items(), key=lambda kv: kv[1].get("name") or kv[0]):
            print("  " + describe_project(pid, info))
        if not found:
            print("  (none - run `gcloud auth login`)")
        return

    if args.list:
        clusters = list_selected_clusters()
        from_gcloud = bool(CLI_TARGETS)
        if not clusters:
            print("No clusters found (see the messages above)." if (LOGIN_OPTS["method"] == "cli" or LOGIN_OPTS["all_clusters"]) else
                  "No clusters found (could not parse the gkelogin menu). Create clusters.json: {\"1\": \"name\", ...}")
        for k, v in sorted(clusters.items(), key=lambda kv: int(kv[0])):
            print(f"{k} - {v}" + (f"   [{describe_cluster(k)}]" if k in CLI_TARGETS else ""))
        if from_gcloud:
            print(scan_summary([c for c in CLI_TARGETS.values() if not c.get("exe_only")]))
            if LOGIN_OPTS["method"] == "exe":
                print("Clusters that are not in the gkelogin menu are logged in with `gcloud container clusters get-credentials`.")
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
            progress=_cli_progress, options={"gcp": not args.no_gcp, "logs": not args.no_logs,
                                             "all_logs": args.logs_all, "log_namespaces": args.log_namespaces,
                                             "sections": chosen, "skip_sections": skipped, "workers": PARALLEL_WORKERS})
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
        if any(r["status"] in ("failed", "credentials expired") for r in results["items"]):
            sys.exit(1)
        return

    run_gui(LOOKBACK_MINUTES, args.skip_login, args.context)


if __name__ == "__main__":
    main()

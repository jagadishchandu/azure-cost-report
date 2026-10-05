#!/usr/bin/env python3
"""
EKS Debugger - uses the AWS credentials that already exist in ~/.aws (no sign-in in this tool), connects to the clusters you choose and collects the
basic debugging picture of what happened (and what is happening) in the last N minutes.

    python eks_debug.py                       # GUI: choose AWS profile(s), regions, clusters
    python eks_debug.py --minutes 60          # GUI, default window 60 minutes
    python eks_debug.py --list-accounts       # the profiles of ~/.aws with their credentials status (Active / Expiring soon / Expired / Not configured / Unknown)
    python eks_debug.py --list --profile dev,prod --region us-east-1,eu-west-1   # collect the clusters of THESE profiles x regions (nothing is scanned unless you name the scope)
    python eks_debug.py --profile dev --region us-east-1 --cluster 1             # debug cluster #1 of that list (no GUI)
    python eks_debug.py --profile dev,prod --region all --cluster 1,3,5          # SEVERAL clusters, one after another (also: 2-4, all, a cluster name)
    python eks_debug.py --connect ekslogin --list        # the clusters the ekslogin.exe menu offers; --cluster 3 then runs ekslogin for entry 3 first
    python eks_debug.py --cluster 3 --skip-login         # already connected: don't run ekslogin / write the kubeconfig entry; use the current kubectl context
    python eks_debug.py --cluster 3 --context my-ctx     # force a specific kubectl context
    python eks_debug.py --list-profiles                  # show the profiles found in ~/.aws

Credentials: this tool NEVER signs in. It uses what the aws CLI itself would use: ~/.aws/config and ~/.aws/credentials (honouring AWS_CONFIG_FILE /
AWS_SHARED_CREDENTIALS_FILE, AWS_PROFILE and the AWS_ACCESS_KEY_ID ... variables). Whatever your company's own process puts there (SSO cache, credentials
file, credential_process, assumed roles) just works. A profile whose credentials are expired or missing is reported with a "renew them outside this tool"
message (the window shows it in red, the command line exits with 1). The only login the tool can start is YOUR OWN ekslogin.exe, when you choose
'Log in with ekslogin first' (--connect ekslogin); ~/.aws is re-read afterwards, because ekslogin may refresh or create profiles.

By default the tool writes the kubeconfig entry of each selected cluster with `aws eks update-kubeconfig --name N --region R --profile P --alias A` (local
kubeconfig only), switches kubectl to it (`kubectl config use-context`) and pins every call to that context with --context, so a different 'current'
context can never send the checks to the wrong cluster.

Several clusters
    In the window select as many clusters as you like (Ctrl/Shift-click, 'Select all', filter box, or
    type numbers like 1,3,5). They run ONE AFTER ANOTHER - each with its own kubeconfig entry, kubectl context,
    AWS profile and its own .html/.txt report - and a summary page links them all. A cluster that fails
    is recorded and the next one still runs (credentials that expire in the middle: that cluster is marked 'credentials expired'). Stop skips the clusters not yet started.

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
    * Network & traffic: one status block (OK / Warning / Problem / Not available + what to do next) per check -
      network plugin health, IP exhaustion and per-instance limits, plugin logs, stuck pods, node conditions, MTU,
      kube-proxy, Services without endpoints, DNS, network policies, cloud firewalls (security groups + network
      ACLs), ingress / load balancers / certificates, conntrack and NAT port exhaustion, observability, packet
      capture guidance, API server throttling, webhooks, etcd - plus (AWS) VPC routing / NAT / endpoints / network
      interfaces and the TRAFFIC of the selected window from CloudWatch, a 10-row traffic issue checklist and
      glossaries. Headings and column headers are written in full words.
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

Requirements: Python 3.9+ (tkinter ships with it), kubectl and the AWS CLI (aws) on PATH, working credentials in ~/.aws (ekslogin.exe only if you use it).
"""

import argparse
import copy
import json
import math
import os
import random
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutureTimeout
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
PARALLEL_WORKERS = 8         # collection tasks that run at the same time after the kubeconfig step (--workers N; 1 = one after another, exactly as before)
KUBECTL_CONCURRENCY = 6      # at most this many kubectl calls at the same moment (keeps the API server calm)
AWS_CONCURRENCY = 4          # at most this many `aws` calls at the same moment (AWS throttles read APIs)
AWS_RETRIES = 4              # tries for an aws call that was answered with a throttling error (waits 0.5 s, 1 s, 2 s ... between the tries)

# ---------------------------------------------------------------------------
# Branding (data-driven: the sibling tools only change these constants and the drawing below)
# ---------------------------------------------------------------------------

CLOUD_NAME = "Amazon Web Services"
CLOUD_SHORT = "AWS"
PRODUCT_NAME = "Amazon Elastic Kubernetes Service (EKS) Debugger"
REPORT_TITLE_PREFIX = "EKS debug"
BRAND_PRIMARY = "#232F3E"          # AWS dark navy: header band, title text, table heads
BRAND_ACCENT = "#FF9900"           # AWS orange: buttons, stripes, highlights, the smile arrow of the logo
BRAND_DARK = "#161E2B"             # darker navy (gradient end, pressed buttons)
BRAND_PALE = "#FFF4DE"             # pale orange panels
BRAND_LINK = "#146EB4"             # AWS blue for links and the Kubernetes helm
BRAND_TEXT = "#16191F"
BRAND_PALETTE = [BRAND_PRIMARY, BRAND_ACCENT, BRAND_LINK]
LOGO_BOX = (100, 64)               # width, height of the coordinate box LOGO_SHAPES draws in


def _smile_points():
    """The orange 'smile' of the logo: a thin crescent from the left to the right below the cloud."""
    outer, inner = [], []
    for i in range(17):
        t = i / 16.0
        s = math.sin(math.pi * t)
        x, yo = 14 + 66 * t, 45 + 10 * s
        outer.append((round(x, 1), round(yo, 1)))
        inner.append((round(x, 1), round(yo - 1.2 - 5.2 * (s ** 0.8), 1)))
    return tuple(outer + inner[::-1])


def LOGO_SHAPES():
    """The logo as simple shapes in a 100 x 64 box: ('oval'|'rect'|'poly', coordinates, colour). A stylised white cloud with an orange
    smile ending in an arrow (generic artwork drawn from primitives, not a copy of any trademarked file). Used by the Tk banner AND the
    HTML report (inline SVG)."""
    cloud = "#FFFFFF"
    return [("oval", (4, 15, 32, 39), cloud), ("oval", (15, 1, 51, 35), cloud), ("oval", (37, 8, 69, 38), cloud), ("oval", (58, 15, 92, 39), cloud),
            ("rect", (18, 20, 76, 39), cloud), ("poly", _smile_points(), BRAND_ACCENT), ("poly", ((70, 42), (94, 32), (83, 56)), BRAND_ACCENT)]


def draw_logo(canvas, x=0, y=0, scale=1.0, tag="logo"):
    """Draw LOGO_SHAPES on a tkinter Canvas with its top-left corner at (x, y)."""
    for kind, c, colour in LOGO_SHAPES():
        if kind == "poly":
            pts = [v for px, py in c for v in (x + px * scale, y + py * scale)]
            canvas.create_polygon(*pts, fill=colour, outline=colour, tags=tag)
        else:
            x0, y0, x1, y1 = c
            make = canvas.create_oval if kind == "oval" else canvas.create_rectangle
            make(x + x0 * scale, y + y0 * scale, x + x1 * scale, y + y1 * scale, fill=colour, outline=colour, tags=tag)


def logo_svg(width=64):
    """The same logo as inline SVG (HTML report header)."""
    w, h = LOGO_BOX
    parts = []
    for kind, c, colour in LOGO_SHAPES():
        if kind == "poly":
            parts.append('<polygon points="%s" fill="%s"/>' % (" ".join(f"{px},{py}" for px, py in c), colour))
        elif kind == "oval":
            x0, y0, x1, y1 = c
            parts.append('<ellipse cx="%s" cy="%s" rx="%s" ry="%s" fill="%s"/>' % ((x0 + x1) / 2, (y0 + y1) / 2, (x1 - x0) / 2, (y1 - y0) / 2, colour))
        else:
            x0, y0, x1, y1 = c
            parts.append('<rect x="%s" y="%s" width="%s" height="%s" fill="%s"/>' % (x0, y0, x1 - x0, y1 - y0, colour))
    return ('<svg class="logo" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img" aria-label="%s logo">%s</svg>'
            % (w, h, width, round(width * h / w), CLOUD_NAME, "".join(parts)))


def _helm_parts(cx, cy, r):
    """The Kubernetes helm (ship's wheel: ring, hub and 7 spokes) as primitives: ('ring', cx, cy, r, width) / ('line', x0, y0, x1, y1, width) / ('disc', cx, cy, r, 0)."""
    parts = [("ring", cx, cy, r, max(1.5, r * 0.16))]
    for i in range(7):
        a = 2 * math.pi * i / 7 - math.pi / 2
        parts.append(("line", cx + 0.25 * r * math.cos(a), cy + 0.25 * r * math.sin(a), cx + 1.18 * r * math.cos(a), cy + 1.18 * r * math.sin(a), max(1.5, r * 0.2)))
    parts.append(("disc", cx, cy, r * 0.3, 0))
    return parts


def draw_helm(canvas, cx, cy, r, colour="#FFFFFF", tag="helm"):
    """Draw the Kubernetes helm symbol on a tkinter Canvas."""
    for p in _helm_parts(cx, cy, r):
        if p[0] == "ring":
            canvas.create_oval(p[1] - p[3], p[2] - p[3], p[1] + p[3], p[2] + p[3], outline=colour, width=p[4], tags=tag)
        elif p[0] == "line":
            canvas.create_line(p[1], p[2], p[3], p[4], fill=colour, width=p[5], capstyle="round", tags=tag)
        else:
            canvas.create_oval(p[1] - p[3], p[2] - p[3], p[1] + p[3], p[2] + p[3], fill=colour, outline=colour, tags=tag)


def helm_svg(size=34, colour="#FFFFFF"):
    """The helm as inline SVG."""
    parts = []
    for p in _helm_parts(16, 16, 14):
        if p[0] == "ring":
            parts.append('<circle cx="%.1f" cy="%.1f" r="%.1f" fill="none" stroke="%s" stroke-width="%.1f"/>' % (p[1], p[2], p[3], colour, p[4]))
        elif p[0] == "line":
            parts.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" stroke-width="%.1f" stroke-linecap="round"/>' % (p[1], p[2], p[3], p[4], colour, p[5]))
        else:
            parts.append('<circle cx="%.1f" cy="%.1f" r="%.1f" fill="%s"/>' % (p[1], p[2], p[3], colour))
    return '<svg class="helm" xmlns="http://www.w3.org/2000/svg" viewBox="-4 -4 40 40" width="%d" height="%d" role="img" aria-label="Kubernetes">%s</svg>' % (size, size, "".join(parts))


# icon name -> (symbol, plain fallback used when the font cannot draw the symbol)
ICONS = {
    "cloud": ("☁", "~"), "key": ("\U0001F510", "*"), "search": ("\U0001F50D", "?"), "helm": ("⎈", "K"), "run": ("▶", ">"),
    "stop": ("⏹", "[]"), "ok": ("✔", "OK"), "fail": ("✖", "X"), "warn": ("⚠", "!"), "node": ("\U0001F5A5", "N"),
    "net": ("\U0001F310", "@"), "report": ("\U0001F4C4", "R"), "reload": ("\U0001F504", "o"), "chart": ("\U0001F4CA", "#"), "pod": ("\U0001F4E6", "P"),
    "bell": ("\U0001F514", "E"), "gear": ("⚙", "W"), "scale": ("\U0001F4C8", "A"), "trophy": ("\U0001F3C6", "T"), "log": ("\U0001F4DC", "L"),
    "clock": ("\U0001F552", "t"), "folder": ("\U0001F4C1", "/"), "layers": ("\U0001F9E9", "+"), "pending": ("○", "o"), "skip": ("➖", "-"),
    "crit": ("\U0001F534", "C"), "high": ("\U0001F7E0", "H"), "med": ("\U0001F7E1", "M"), "info": ("\U0001F535", "I"),
    "speed": ("⚡", "~"), "list": ("\U0001F4CB", "="),
}
_ICON_PLAIN = [False]


def icon(name):
    """The symbol for `name`, or its plain-text fallback when the GUI found the font cannot draw it."""
    sym, fallback = ICONS.get(name, ("", ""))
    return fallback if _ICON_PLAIN[0] else sym


# ---------------------------------------------------------------------------
# The report sections: ONE registry that drives the collection steps, the scheduler, the report, the table of contents,
# the command line (--sections ...) and the GUI check boxes.
#   id        short name (also the step key)            title      full words, shown everywhere
#   step      title of the live step in the GUI list    needs      sections whose DATA this one reads (collected silently when not selected)
#   resources kubectl objects this section reads        usage      needs the live CPU / memory / disk numbers (kubelet + metrics-server)
#   calls     makes slow calls of its own (kubectl / aws) beyond the shared cluster data: these run ahead in parallel
#   locked    always collected (the report identity)
#   shows / how / terms   (SECTION_TEXT, merged below) the 'What this section shows' / 'How to use it' text and the glossary terms of the section
# ---------------------------------------------------------------------------

SECTIONS = [
    {"id": "overview", "title": "Cluster overview", "step": "Cluster overview", "icon": "helm", "locked": True, "needs": (), "resources": (), "usage": False, "calls": True,
     "desc": "cluster name, kubectl context, Kubernetes versions and API server readiness (always collected)"},
    {"id": "aws", "title": "Amazon EKS control plane and AWS infrastructure", "step": "AWS EKS details", "icon": "cloud", "needs": (), "resources": ("nodes",),
     "usage": False, "calls": True,
     "desc": "aws: cluster state, API endpoint, VPC subnets and security groups, IAM roles, node groups, add-ons, EC2 status, control-plane logs"},
    {"id": "nodes", "title": "Nodes: processor, memory, disk and swap", "step": "Nodes: CPU / memory / disk / swap", "icon": "node", "needs": (),
     "resources": ("nodes", "pods"), "usage": True, "calls": False, "desc": "every node with its EC2 server, status, pressure, CPU / memory / disk / swap and pod slots"},
    {"id": "utilization", "title": "Resource utilization by namespace", "step": "Resource utilization by namespace", "icon": "chart", "needs": (),
     "resources": ("nodes", "pods"), "usage": True, "calls": False, "desc": "the interactive dashboard of requests, limits and real usage per namespace, workload and node"},
    {"id": "nodepods", "title": "Pods on each node", "step": "Pods on each node", "icon": "layers", "needs": (), "resources": ("pods",), "usage": True, "calls": False,
     "desc": "which pods run on which node, with their usage"},
    {"id": "namespaces", "title": "Namespaces: pods used versus configured", "step": "Namespaces: pods used vs configured", "icon": "folder", "needs": (),
     "resources": ("hpa", "namespaces", "nodes", "pods", "replicasets", "resourcequotas"), "usage": True, "calls": False,
     "desc": "pods and resources per namespace against quotas, with the support team of each namespace"},
    {"id": "pods", "title": "Unhealthy pods", "step": "Unhealthy pods", "icon": "pod", "needs": (), "resources": ("pods",), "usage": False, "calls": False,
     "desc": "crash loops, pending pods, image pull errors, restarts, OOM kills"},
    {"id": "events", "title": "Warning events", "step": "Events", "icon": "bell", "needs": (), "resources": ("events",), "usage": False, "calls": False,
     "desc": "the Kubernetes Warning events of the window"},
    {"id": "workloads", "title": "Workloads", "step": "Workloads", "icon": "gear", "needs": (),
     "resources": ("daemonsets", "deployments", "jobs", "replicasets", "statefulsets"), "usage": False, "calls": False,
     "desc": "deployments, stateful sets, daemon sets and jobs that are not at their wanted size"},
    {"id": "network", "title": "Network and traffic", "step": "Network & traffic in the window", "icon": "net", "needs": ("aws",),
     "resources": ("daemonsets", "deployments", "endpoints", "events", "ingresses", "networkpolicies", "nodes", "pods", "services"), "usage": True, "calls": True,
     "desc": "CNI, IP exhaustion, DNS, services, policies, firewalls, ingress and load balancers, NAT, throttling, traffic of the window and the checklist"},
    {"id": "scaling", "title": "Autoscaling, storage and services", "step": "Autoscaling, storage, network", "icon": "scale", "needs": (),
     "resources": ("endpoints", "hpa", "namespaces", "pv", "pvc", "services"), "usage": False, "calls": False,
     "desc": "horizontal pod autoscalers, volumes and claims, services without endpoints"},
    {"id": "top", "title": "Top consumers", "step": "Top consumers", "icon": "trophy", "needs": (), "resources": ("pods",), "usage": True, "calls": False,
     "desc": "the pods and nodes using the most CPU, memory and disk"},
    {"id": "logs", "title": "Pod logs", "step": "Pod logs", "icon": "log", "needs": ("pods",), "resources": ("events", "pods"), "usage": False, "calls": True,
     "desc": "logs of unhealthy pods, pods with warnings and core add-ons (all pods on request)"},
    {"id": "timeline", "title": "Timeline", "step": "Timeline", "icon": "clock", "needs": (), "resources": (), "usage": False, "calls": False,
     "desc": "everything that happened in the window, oldest first"},
]

# What every section shows, in plain words: drives the "What this section shows" box of the HTML report and the intro lines of the text
# report (so both always agree). shows = what data, where it comes from, which time window; how = how to read colours / what to click;
# terms = the glossary terms the section uses (the complete glossary at the end of the report is built from them).
# {minutes} is replaced by the length of the time window.
SECTION_TEXT = {
    "overview": {
        "shows": "Which cluster this report is about: the kubectl context (the saved connection to the cluster), the client and server Kubernetes versions, the "
                 "time window and whether the cluster's front door (the Kubernetes API server) answers its readiness check. It is read with kubectl at the moment "
                 "the report is made.",
        "how": "A line marked FAILING or NOT REACHABLE (shown in amber or red) means the API server has a problem: look at it first.",
        "terms": ("kubectl", "API server", "readyz")},
    "aws": {
        "shows": "What Amazon Web Services (AWS) itself says about this cluster: cluster state and endpoint, subnets and firewalls (security groups), identity and "
                 "access roles, node groups, add-ons, Fargate, the status of the cloud servers behind the nodes, and error lines of the managed control plane from "
                 "Amazon CloudWatch for the last {minutes} minutes. It is read with read-only AWS command line calls; nothing is changed.",
        "how": "ACTIVE and OK are healthy; DEGRADED, MISSING, FAILING, impaired or VERY LOW (red or amber) need attention. Click a column header to sort a table, "
               "or type in its filter box.",
        "terms": ("AWS", "EKS", "CloudWatch", "IAM")},
    "nodes": {
        "shows": "Every worker node (a virtual server that runs pods): which cloud server it is, whether it is Ready, how much processor, memory, disk and swap it "
                 "uses compared with what it can offer to pods (allocatable), and how much the pods on it have requested. Usage is a live reading from the "
                 "kubelet (the agent on each node) or from the metrics server at the moment of the report; requests come from the pod definitions.",
        "how": f"Percent bars turn amber from {UTIL_WARN}% and red from {UTIL_CRIT}%. A status other than Ready, or text in the Findings column, means the node needs attention.",
        "terms": ("EC2", "kubelet", "CPU")},
    "utilization": {
        "shows": "How much processor (CPU) and memory the cluster and each namespace use, have requested and are limited to. Use is the live reading now, a request is "
                 "what a pod asked the scheduler to reserve, and a limit is the most a pod may use. It is a snapshot of the moment the report was made, built from kubectl "
                 "and the kubelet; no history is stored.",
        "how": f"Bars turn amber from {UTIL_WARN}% and red from {UTIL_CRIT}% of a limit. In the dashboard click a namespace to see its pods, and use the buttons to switch "
               "between processor, memory and disk, and between used and requested.",
        "terms": ("CPU", "Request", "Limit")},
    "nodepods": {
        "shows": "For each node, the pods that run (or wait to run) on it with their status, restarts and live usage compared with their requests and limits. It comes "
                 "from kubectl and shows which pods share a node and which one is under pressure.",
        "how": "Statuses in red (CrashLoopBackOff, ImagePullBackOff, Error...) are failing pods. The Notes column appears when a pod is above 90% of a limit.",
        "terms": ("CPU", "Request", "Limit", "DL")},
    "namespaces": {
        "shows": "Per namespace (a named group of objects inside the cluster): how many pods exist and run, how many are configured by their workloads, the quotas that "
                 "limit them, the resources they use and which support team to contact. It is read with kubectl at the moment of the report.",
        "how": "Rows with missing pods, or a quota that is nearly full, need attention. Click a column header to sort.",
        "terms": ("DL", "Quota", "HPA", "CPU", "Request")},
    "pods": {
        "shows": "The pods that are not healthy right now: crash loops, pods that cannot start or be scheduled, image pull errors, pods that are not ready, restarts and "
                 "out-of-memory kills during the last {minutes} minutes. It comes from the pod status that kubectl reads.",
        "how": "The most serious problems are listed first. The Why column quotes what Kubernetes itself reports.",
        "terms": ("OOMKilled", "CrashLoopBackOff", "ImagePullBackOff", "DL")},
    "events": {
        "shows": "The Kubernetes events of the last {minutes} minutes: Warning events (something went wrong, such as BackOff or FailedScheduling) with how often they "
                 "happened, plus a few notable Normal events such as scaling, kills and node changes. Kubernetes keeps events for about one hour only.",
        "how": "Newest first. The Reason is Kubernetes' own short cause name and the Message explains it.",
        "terms": ("DL",)},
    "workloads": {
        "shows": "Deployments, stateful sets and daemon sets (the objects that keep pods running) that are not at their wanted size, new rollouts of the last {minutes} "
                 "minutes, jobs that failed in that time and the core add-ons in kube-system. It is read with kubectl at the moment of the report.",
        "how": "Ready 1/3 means one of three wanted pods is ready. DEGRADED means fewer pods than wanted are ready.",
        "terms": ("Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "DL")},
    "network": {
        "shows": "Everything that decides whether traffic reaches and leaves pods: the pod network plug-in, free IP addresses, node conditions, kube-proxy and Service "
                 "routing, name lookups (DNS), network policies and firewalls, load balancers and ingress, the traffic of the last {minutes} minutes from Amazon "
                 "CloudWatch, connection-table limits and the health of the control plane. Every check is read-only and ends in a status with advice.",
        "how": "Each check has a coloured stripe: green OK, amber Warning, red Problem, grey Not available (the data could not be read). Start with the 'Traffic issue "
               "checklist' near the end of this section.",
        "terms": ("CNI", "DNS", "VPC")},
    "scaling": {
        "shows": "Autoscaling, storage and Service problems: horizontal pod autoscalers that are at their maximum or cannot scale, storage claims that are not bound, "
                 "Services without ready pods or without an external address, and namespaces stuck terminating. It is read with kubectl at the moment of the report.",
        "how": "Only problems are listed; if a block says everything is fine, nothing needs attention there.",
        "terms": ("HPA", "PVC", "PV", "DL")},
    "top": {
        "shows": "The ten pods that use the most processor and the ten that use the most memory right now, with their share of their limit. These are live readings from "
                 "the kubelet or the metrics server at the moment of the report, not history.",
        "how": "A percentage near 100% of the limit means the pod is close to being throttled (processor) or killed (memory).",
        "terms": ("CPU", "Limit", "DL")},
    "logs": {
        "shows": "Recent log lines (the last {minutes} minutes, at most 200 lines per container) of unhealthy pods, pods named in Warning events and core add-ons "
                 "(every pod only on request), read with kubectl logs. Logs can contain sensitive information.",
        "how": "Error-like lines are red and warning lines amber. Click a log heading to open it, tick 'errors only' to hide the rest, and use Copy to copy what is visible.",
        "terms": ("kubectl", "DL")},
    "timeline": {
        "shows": "One list, oldest first, of everything the other sections noticed during the last {minutes} minutes: events, container terminations, node changes, "
                 "rollouts, failed jobs and control-plane errors. Times are in UTC (Coordinated Universal Time).",
        "how": "Click a kind chip (POD, EVENT, NODE ...) to hide or show that kind of entry.",
        "terms": ("UTC",)},
}
for _s in SECTIONS:
    _s.update(SECTION_TEXT[_s["id"]])

# The parts of the HTML report that are not collection sections (they have no checkbox) get the same kind of box.
REPORT_PARTS = {
    "summary": {
        "title": "Health summary",
        "shows": "The result of the whole run in one place: how many problems were found at each severity, every finding with the report section it came from and, for "
                 "namespaces with problems, which support team to contact. The counters and findings cover only the sections that were collected.",
        "how": "Click a severity card to hide or show that severity. The search box at the top filters every table at once, and a section name in the findings table "
               "jumps to that section."},
    "steps": {
        "title": "Collection steps",
        "shows": "Which collection steps ran, whether each one finished, failed or was skipped, and how long it took. It tells you where the time went and which part "
                 "failed if something is missing from the report.",
        "how": "Click the Time column header to find the slowest step."},
    "timing": {
        "title": "Timing summary: how long this run took",
        "shows": "The total run time, whether the collection ran in parallel (several tasks at the same moment) or one after another, how many read-only calls were made "
                 "and the time of every step.",
        "how": "Compare the total with the sum of the steps: the difference is the time saved by running steps in parallel."},
    "readonly": {
        "title": "Read-only guarantee",
        "shows": "The proof that this report only read information: how many read commands were run, and which attempted changes (if any) the built-in guard refused. "
                 "Nothing is installed, created, changed or deleted on the cluster or in the cloud account.",
        "how": "The line should end with '0 blocked'. A blocked command is listed with its reason, and also appears as a Critical finding."},
    "glossary": {
        "title": "Glossary: all terms used in this report",
        "shows": "Every abbreviation and technical term used anywhere in this report, spelled out and explained in plain language, sorted alphabetically. The same terms "
                 "are also explained just before the table that first uses them.",
        "how": "Use the filter box above the table to find a term."},
    "clusters": {
        "title": "Clusters",
        "shows": "One row per cluster of this run: whether its report was written, how many findings of each severity it has, how long it took, how many report sections "
                 "were collected, its most serious finding and a link to its full report.",
        "how": "Click a column header to sort, or use the filter box. Open a cluster's full report with the link in the last column."},
    "allfindings": {
        "title": "All findings (every cluster)",
        "shows": "Every finding of every cluster in one table, with the cluster and the report section it came from. The cards above the table count the findings per severity.",
        "how": "Click a severity card to hide or show that severity; a section link opens the matching section of that cluster's report."},
}

SECTION_BY_ID = {s["id"]: s for s in SECTIONS}
SECTION_ALIASES = {"networking": "network", "net": "network", "traffic": "network", "autoscaling": "scaling", "storage": "scaling", "node": "nodes",
                   "namespace": "namespaces", "ns": "namespaces", "pod": "pods", "event": "events", "log": "logs", "cluster": "overview",
                   "utilisation": "utilization", "workload": "workloads", "eks": "aws", "cloud": "aws", "consumers": "top", "netdetail": "network"}
BASE_RESOURCES = ("nodes", "pods", "namespaces")        # always read: the cluster cannot be reported without them, and the support teams come from the namespaces
SECTION_PRESETS = {
    "all": [s["id"] for s in SECTIONS],
    "only_networking": ["overview", "network"],
    "no_networking": [s["id"] for s in SECTIONS if s["id"] != "network"],
}
LEGACY_SWITCHES = {"aws": "aws", "logs": "logs"}        # the older --no-aws / --no-logs switches (and their GUI boxes) are aliases of these sections


def parse_section_list(text):
    """'aws,nodes,networking' -> ['aws', 'nodes', 'network'] (ids or aliases). Raises ValueError naming the unknown ones."""
    out, bad = [], []
    for part in re.split(r"[,\s;]+", str(text or "").strip().lower()):
        if not part:
            continue
        sid = SECTION_ALIASES.get(part, part)
        if sid in SECTION_BY_ID:
            if sid not in out:
                out.append(sid)
        else:
            bad.append(part)
    if bad:
        raise ValueError("unknown section(s): " + ", ".join(bad) + ". Valid ids: " + ", ".join(SECTION_BY_ID))
    return out


def plan_sections(options=None):
    """What a run collects. Returns {'collect': ids shown in the report, 'hidden': {id: [who needs it]} collected silently, 'skipped': {id: why},
    'live': ids actually read, 'resources': kubectl objects to read, 'usage': read live usage?}.
    Options: 'sections' (None = all), 'aws' / 'logs' (the older switches: False unticks that section)."""
    options = options or {}
    wanted = options.get("sections")
    chosen = {s["id"] for s in SECTIONS} if wanted is None else set(wanted)
    why = {}
    for opt, sid in LEGACY_SWITCHES.items():
        if not options.get(opt, True):
            chosen.discard(sid)
            why[sid] = f"turned off (--no-{opt} / '{SECTION_BY_ID[sid]['title']}' unticked)"
    chosen |= {s["id"] for s in SECTIONS if s.get("locked")}
    hidden = {}
    for sid in [s["id"] for s in SECTIONS]:
        if sid in chosen:
            for dep in SECTION_BY_ID[sid]["needs"]:
                if dep not in chosen and dep not in why:        # a section the user switched off explicitly (--no-aws) is not read behind their back
                    hidden.setdefault(dep, []).append(sid)
    live = chosen | set(hidden)
    resources = set(BASE_RESOURCES)
    for sid in live:
        resources |= set(SECTION_BY_ID[sid]["resources"])
    skipped = {sid: why.get(sid, "not selected") for sid in SECTION_BY_ID if sid not in live}
    return {"collect": chosen, "hidden": hidden, "skipped": skipped, "live": live, "resources": resources, "usage": any(SECTION_BY_ID[s]["usage"] for s in live)}


def section_titles_of(ids):
    return ", ".join(SECTION_BY_ID[i]["title"] for i in ids)


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
# ekslogin.exe (the user's own login program - only used when 'Log in with ekslogin first' / --connect ekslogin is chosen)
# ---------------------------------------------------------------------------

def ekslogin(cluster_number):
    # ekslogin.exe prints a menu and waits on stdin for the cluster number(s).
    # LOCAL-ONLY exception of the read-only guarantee: the login program the user chose to use (it writes the local kubeconfig, nothing else).
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
    """{'1': 'cluster-name', ...} for the list: by default - or with ekslogin and the source 'Collect clusters ...' - every cluster `aws` can see
    (see list_clusters_cli); otherwise the ekslogin menu (exe_menu_clusters)."""
    if _lists_via_cli():
        return list_clusters_cli(emit or print)
    return exe_menu_clusters(refresh=True)


EXE_MENU = {"loaded": False, "clusters": {}}


def exe_menu_clusters(refresh=False):
    """{'1': 'cluster-name', ...} offered by the custom login. Order of preference: the CLUSTERS dict, clusters.json next to this script,
    then the menu that ekslogin.exe prints when it is given no selection (stdin closed). Cached: pass refresh=True to read it again."""
    if EXE_MENU["loaded"] and not refresh:
        return dict(EXE_MENU["clusters"])
    EXE_MENU["clusters"] = _read_exe_menu()
    EXE_MENU["loaded"] = True
    return dict(EXE_MENU["clusters"])


def _read_exe_menu():
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
# How to connect to a cluster: your existing AWS credentials from ~/.aws (default - this tool NEVER signs in) or ekslogin.exe first
# ---------------------------------------------------------------------------

LOGIN_OPTS = {"method": "cli",        # "cli" = use the credentials that already exist in ~/.aws (default) | "exe" = log in with ekslogin.exe first, then re-read ~/.aws
              "source": None,         # where the cluster LIST comes from: "all" = every cluster aws can see (selected profiles x regions), "menu" = the ekslogin menu (None = menu with ekslogin)
              "all_clusters": False}  # --all-clusters: list every accessible cluster
SOURCE_LABELS = {"all": "Collect clusters with the aws CLI from selected profiles (on demand)", "menu": "Clusters from the ekslogin menu (instant)"}
LARGE_SCOPE = 20            # profile x region pairs: above this the window asks before it starts to collect clusters
LOGIN_LABELS = {"cli": "Use my existing AWS credentials from ~/.aws (no sign-in)", "exe": "Log in with ekslogin first"}   # the GUI combobox values
CLI_TARGETS = {}            # cluster number (str) -> what the CLI listing found; fed into AWS_OPTS before a run


def _lists_via_cli():
    """True when the cluster list is read with the aws CLI: the default (existing credentials) method, or ekslogin with the source 'all'."""
    return LOGIN_OPTS["method"] == "cli" or LOGIN_OPTS.get("source") == "all"


def ekslogin_available():
    """True when the user's own ekslogin.exe can be found (next to this script, the path given with --ekslogin, or on PATH)."""
    try:
        return os.path.isfile(EKSLOGIN_EXE) or bool(shutil.which(EKSLOGIN_EXE))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Credentials status of the AWS profiles in ~/.aws: Active / Expiring soon / Expired / Not configured / Unknown.
# This tool never signs in. It only READS: the local SSO token cache (~/.aws/sso/cache: only startUrl and expiresAt are used - never a token or a
# secret) and `aws sts get-caller-identity --profile P` (read-only, on the allow-list), 4 at a time.
# ---------------------------------------------------------------------------

EXPIRING_SECONDS = 30 * 60      # less than this left = "Expiring soon"
STATUS_WORKERS = 4              # `aws sts get-caller-identity` calls at the same moment when checking profiles
LAZY_STATUS_LIMIT = 40          # up to this many profiles are all checked in the background; above it: the selected ones first + 'Re-check all'
BLOCKED_STATES = ("expired", "not_configured")      # a profile in one of these states is skipped (collect) / marked 'credentials expired' (run)
ENV_LABEL = "Environment / default credentials"     # the pseudo-entry: what the aws CLI uses without --profile (AWS_* variables, AWS_PROFILE, [default])
ENV_KEY = "__env__"
_EXPIRED_PATTERNS = ("expiredtoken", "token has expired", "has expired or is otherwise invalid", "expired or is otherwise invalid", "access token has expired",
                     "refresh token", "invalidclienttokenid", "unable to locate credentials", "security token included in the request is expired",
                     "token is expired", "credentials have expired", "credentials expired")


def is_expired_error(err):
    """True when an aws error text says the credentials / SSO session expired (or are missing / invalid): ExpiredToken, 'Token has expired',
    'The SSO session associated with this profile has expired or is otherwise invalid', refresh token, InvalidClientTokenId, 'Unable to locate credentials'."""
    low = str(err or "").lower()
    return any(p in low for p in _EXPIRED_PATTERNS) or ("sso" in low and "expired" in low)


def renew_command(profile):
    return "aws sso login" + (f" --profile {profile}" if profile else "")


def renew_message(profile, with_ekslogin=None):
    """The red message for a profile whose credentials are expired or missing. The tool does not sign in: the text only says what to do."""
    name = profile or ENV_LABEL
    msg = (f"Credentials for profile {name} are expired or missing. This tool does not sign in. Renew them the way your company does it "
           f"(for example your company's login tool, or `{renew_command(profile)}` in your own terminal), then press Re-check.")
    if (ekslogin_available() if with_ekslogin is None else with_ekslogin):
        msg += " Or choose 'Log in with ekslogin first' under 'How to connect to a cluster'."
    return msg


class CredentialsExpired(RuntimeError):
    """The credentials of an AWS profile are expired or missing (the run continues with the other clusters; the profile is shown as Expired)."""

    def __init__(self, profile, message=None):
        self.profile = profile
        super().__init__(message or renew_message(profile))


def _parse_expiry(text):
    t = str(text or "").strip().replace("UTC", "Z")
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    t = re.sub(r"\.(\d+)", lambda m: "." + (m.group(1) + "000000")[:6], t, count=1)
    try:
        dt = datetime.fromisoformat(t)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def sso_cache_dir():
    return os.path.join(os.path.dirname(aws_config_paths()[0]), "sso", "cache")


def read_sso_cache():
    """{start_url: latest expiresAt (aware datetime)} from the SSO token cache. Local file read; only startUrl and expiresAt are kept - the access / refresh
    tokens in those files are never stored, shown or logged."""
    out = {}
    folder = sso_cache_dir()
    try:
        names = [n for n in os.listdir(folder) if n.endswith(".json")]
    except OSError:
        return out
    for n in names:
        try:
            with open(os.path.join(folder, n), encoding="utf-8") as f:
                d = json.load(f)
            if not isinstance(d, dict) or not d.get("startUrl") or "accessToken" not in d:
                continue                                     # client registrations etc. have no access token
            exp = _parse_expiry(d.get("expiresAt"))
            if exp and (d["startUrl"] not in out or exp > out[d["startUrl"]]):
                out[d["startUrl"]] = exp
        except Exception:
            continue
    return out


def _lookup_name(name, profiles):
    """The profile whose settings apply: the given one, or what the aws CLI itself would use (AWS_PROFILE, else [default])."""
    return name or os.environ.get("AWS_PROFILE") or ("default" if "default" in profiles else None)


def _sso_chain(name, profiles):
    """(is_sso, start_url) of a profile, following source_profile for role profiles."""
    seen, n = set(), name
    while n in profiles and n not in seen:
        seen.add(n)
        info = profiles[n]
        if info.get("sso"):
            return True, info.get("start_url")
        if not info.get("source"):
            break
        n = info["source"]
    return False, None


def _expiry_of(name, profiles, cache):
    """(is_sso, expiry or None): the SSO token expiry from the local cache, else the expiration written next to temporary credentials (non-secret
    keys such as aws_session_expiration / expiration in the credentials file, or AWS_CREDENTIAL_EXPIRATION for the environment)."""
    key = _lookup_name(name, profiles)
    is_sso, start = _sso_chain(key, profiles)
    exp = cache.get(start) if (is_sso and start) else None
    if exp is None:
        exp = (profiles.get(key) or {}).get("expires_at")
    if exp is None and not name and not os.environ.get("AWS_PROFILE") and os.environ.get("AWS_CREDENTIAL_EXPIRATION"):
        exp = _parse_expiry(os.environ.get("AWS_CREDENTIAL_EXPIRATION"))
    return is_sso, exp


def sso_expiry_of(name):
    """The expiry (aware datetime) behind a profile, or None when it cannot be told. Local file read; no token is read into a message."""
    try:
        return _expiry_of(name, list_aws_profiles(), read_sso_cache())[1]
    except Exception:
        return None


def fmt_left(seconds):
    s = int(max(0, seconds))
    if s < 60:
        return "under 1m"
    h, m = s // 3600, (s % 3600) // 60
    return f"{h}h {m:02d}m" if h else f"{m}m"


def fmt_time(dt):
    try:
        return dt.astimezone().strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(dt)


def classify_status(name, err, profiles=None, cache=None, now=None):
    """Status of one profile from the result of `sts get-caller-identity` (err = None when it worked) and the local expiry information.
    {"profile", "state": active | expiring | expired | not_configured | unknown, "left" (seconds or None), "expires_at", "detail", "sso"}."""
    profiles = list_aws_profiles() if profiles is None else profiles
    cache = read_sso_cache() if cache is None else cache
    now = now or datetime.now(timezone.utc)
    is_sso, exp = _expiry_of(name, profiles, cache)
    left = (exp - now).total_seconds() if exp else None
    st = {"profile": name, "state": "unknown", "left": None, "expires_at": exp, "detail": "", "sso": is_sso}
    if err is None:
        st["state"] = "active"
        if left is not None and left > 0:
            st["left"] = left
            if left < EXPIRING_SECONDS:
                st["state"] = "expiring"
        return st
    low = str(err).lower()
    st["detail"] = _first_line(err, 160)
    if "could not be found" in low or ("not found" in low and "profile" in low):
        st.update(state="not_configured", detail="profile not found in ~/.aws")
    elif ("does not exist" in low and "sso" in low) or "error loading sso token" in low or "unable to locate credentials" in low or "no credentials" in low:
        if is_sso and left is not None and left <= 0:
            st["state"] = "expired"
        else:
            st["state"] = "not_configured"
    elif is_expired_error(err):
        st["state"] = "expired"
    return st


def status_text(st):
    """The words shown in the status chip of a profile."""
    s = st.get("state")
    if s == "active":
        return "Active" + (f" - {fmt_left(st['left'])} left" if st.get("left") else "")
    if s == "expiring":
        return f"Expiring soon - {fmt_left(st.get('left') or 0)} left"
    if s == "expired":
        return "Credentials expired - renew them outside this tool"
    if s == "not_configured":
        return "Not configured" + (f": {_first_line(st['detail'], 60)}" if st.get("detail") else "")
    if s == "unchecked":
        return "Checking ..."
    return "Unknown" + (f": {_first_line(st['detail'], 70)}" if st.get("detail") else "")


def local_status(name, profiles=None, cache=None, now=None):
    """Quick status from the local expiry information only (no aws call): shown until the check with `sts get-caller-identity` has answered."""
    profiles = list_aws_profiles() if profiles is None else profiles
    cache = read_sso_cache() if cache is None else cache
    now = now or datetime.now(timezone.utc)
    is_sso, exp = _expiry_of(name, profiles, cache)
    st = {"profile": name, "state": "unchecked", "left": None, "expires_at": exp, "detail": "", "sso": is_sso, "provisional": True}
    if exp is not None:
        left = (exp - now).total_seconds()
        st.update(state="expired" if left <= 0 else ("expiring" if left < EXPIRING_SECONDS else "active"), left=left if left > 0 else None)
    return st


def check_accounts(names, on_result=None, cancel=None, workers=STATUS_WORKERS):
    """Check the credentials of each profile with `aws sts get-caller-identity --profile P` (read-only), `workers` at a time. A name of None means
    'no --profile' (the environment / default credentials). on_result(name, status) is called as each answer arrives (from a worker thread).
    Returns {name: status}. Each status also has 'arn' and 'account' when the credentials work."""
    names = list(names)
    profiles, cache = list_aws_profiles(), read_sso_cache()
    out = {}

    def one(n):
        if cancel is not None and cancel.is_set():
            return n, None
        ident, err = aws_cli(["sts", "get-caller-identity"], {"profile": n}, timeout=30)
        st = classify_status(n, err, profiles, cache)
        if not err and isinstance(ident, dict):
            st["arn"], st["account"] = ident.get("Arn"), ident.get("Account")
        return n, st
    if not names:
        return out
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(names)))) as pool:
        futures = [pool.submit(one, n) for n in names]
        for fut in as_completed(futures):
            n, st = fut.result()
            if st is not None:
                out[n] = st
                if on_result:
                    on_result(n, st)
    return out


def print_accounts(emit=print):
    """--list-accounts: every profile with its credentials status."""
    rows, _err = load_accounts()
    if not rows:
        emit("No AWS profiles found in ~/.aws (config / credentials) and no AWS_* credentials in the environment.")
        return {}
    emit(f"AWS profiles ({len(rows)}) - checking the credentials of each (read-only: aws sts get-caller-identity) ...")
    profiles = list_aws_profiles()
    ids = [r["id"] for r in rows]
    res = check_accounts(ids) if shutil.which("aws") else {i: local_status(i, profiles) for i in ids}
    for r in rows:
        st = res.get(r["id"]) or {"state": "unknown", "detail": "not checked"}
        emit(f"  {r['name']:<34} {status_text(st):<52} {r['info']}")
        if st.get("state") in BLOCKED_STATES:
            emit("      " + renew_message(r["id"]))
    return res


def _run_captured(cmd, timeout=120):
    """Run a non-interactive command. Returns (ok, first line of its output or error). Only allow-listed commands (check_local_command)."""
    why = check_local_command(cmd)
    if why:
        return False, _ro_block(os.path.basename(str(cmd[0])), cmd[1:], why)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f"timed out after {timeout}s"
    except Exception as exc:
        return False, str(exc)
    text = (proc.stdout if proc.returncode == 0 else (proc.stderr or proc.stdout)) or ""
    return proc.returncode == 0, _first_line(text, 200) if text.strip() else ""


def list_selected_clusters(emit=print):
    """The cluster list of the chosen connection method / source (ekslogin.exe menu / clusters.json, or the aws CLI)."""
    return list_clusters(emit)


def env_credentials_present():
    """True when the environment names credentials for the aws CLI (the values are never read into a message)."""
    return any(os.environ.get(k) for k in ("AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_SESSION_TOKEN", "AWS_WEB_IDENTITY_TOKEN_FILE"))


def role_text(info):
    """Role / SSO information of a profile, where it is known (never a secret)."""
    info = info or {}
    parts = []
    if info.get("role_name"):
        parts.append("role " + info["role_name"])
    elif info.get("role_arn"):
        parts.append("role " + info["role_arn"].rsplit("/", 1)[-1])
    if info.get("sso"):
        parts.append("SSO" + (f" {info['start_url']}" if info.get("start_url") else ""))
    if info.get("source") and not info.get("sso"):
        parts.append(f"via {info['source']}")
    return ", ".join(parts) or ("keys / credential_process" if not info.get("role_arn") else "")


def load_accounts():
    """Every AWS profile of ~/.aws (no cap) plus - when the environment names credentials (AWS_ACCESS_KEY_ID ..., AWS_PROFILE) or there is no profile
    at all - the pseudo-entry 'Environment / default credentials' (id None = no --profile). ([{id, key, name, code, info, role, region, usable}], error_or_None)."""
    try:
        profiles = list_aws_profiles()
    except Exception as exc:
        return [], str(exc)
    rows = [{"id": n, "key": n, "name": n, "code": i.get("account") or "-", "role": role_text(i), "region": i.get("region") or "-",
             "info": ("SSO" if i.get("sso") else ("role" if i.get("role_arn") else "keys")) + ", " + (i.get("region") or "no region"),
             "usable": True} for n, i in profiles.items()]
    rows.sort(key=lambda a: a["name"].lower())
    if env_credentials_present() or not rows:
        cur = profiles.get(os.environ.get("AWS_PROFILE") or "default") or {}
        rows.insert(0, {"id": None, "key": ENV_KEY, "name": ENV_LABEL, "code": cur.get("account") or "-", "role": role_text(cur) or "AWS_* variables / default chain",
                        "region": os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or cur.get("region") or "-",
                        "info": "environment, " + (os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or cur.get("region") or "no region"),
                        "usable": True})
    return rows, None


PREFLIGHT = {"blocked": [], "usable": []}       # the last command-line check of the profiles in scope (see split_usable)


def split_usable(accounts, emit=print):
    """Check the credentials of the profiles in `accounts` ([{id, name}]) and split them: (usable, blocked). A profile that is expired / not configured
    is NOT used: the renew message is printed for it (this tool does not sign in). Unknown (could not be checked) counts as usable."""
    res = check_accounts([a["id"] for a in accounts])
    usable, blocked = [], []
    for a in accounts:
        st = res.get(a["id"]) or {"state": "unknown"}
        if st["state"] in BLOCKED_STATES:
            blocked.append(a)
            emit(renew_message(a["id"]))
        else:
            usable.append(a)
    PREFLIGHT.update(blocked=[a["id"] for a in blocked], usable=[a["id"] for a in usable])
    return usable, blocked



LIST_WORKERS = 16            # parallel `aws eks list-clusters` calls (one per profile + region)
ALL_REGIONS = ("all", "*")
FALLBACK_REGIONS = ("us-east-1", "us-east-2", "us-west-1", "us-west-2", "af-south-1", "ap-east-1", "ap-south-1", "ap-south-2", "ap-southeast-1",
                    "ap-southeast-2", "ap-southeast-3", "ap-southeast-4", "ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ca-central-1",
                    "eu-central-1", "eu-central-2", "eu-west-1", "eu-west-2", "eu-west-3", "eu-north-1", "eu-south-1", "eu-south-2", "il-central-1",
                    "me-south-1", "me-central-1", "sa-east-1")      # used for 'all regions' when `aws ec2 describe-regions` fails for every profile


def all_regions(profile):
    """Every enabled region of the account (`aws ec2 describe-regions`): (sorted names, error_or_None)."""
    data, err = aws_cli(["ec2", "describe-regions"], {"profile": profile, "region": "us-east-1"}, timeout=60)
    names = [r.get("RegionName") for r in (data.get("Regions") or []) if isinstance(r, dict)] if isinstance(data, dict) else []
    return sorted(n for n in names if n), err


def _all_regions_for(accounts):
    """Region names for 'all': describe-regions of the first profile that answers (up to 3 tried), else the built-in list."""
    for a in (accounts or [{"id": None}])[:3]:
        names, _err = all_regions(a["id"])
        if names:
            return names
    return list(FALLBACK_REGIONS)


def _explicit_regions(regions=None):
    text = AWS_OPTS.get("region") if regions is None else regions
    return [r.strip() for r in (text or "").split(",") if r.strip()]


def _plan_lookups(accounts, explicit, profiles):
    """[(profile, region)] to look at, and the profiles that have no region: --region / the Region(s) box (comma separated; 'all' =
    every enabled region), else each profile's own region, else AWS_REGION / AWS_DEFAULT_REGION."""
    if any(r.lower() in ALL_REGIONS for r in explicit):
        explicit = _all_regions_for(accounts) + [r for r in explicit if r.lower() not in ALL_REGIONS]
    lookups, noregion = [], []
    for a in accounts:
        p = a["id"]
        info = profiles.get(p or os.environ.get("AWS_PROFILE") or "default") or {}
        rs = explicit or [r for r in (info.get("region") or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),) if r]
        if not rs:
            noregion.append(p or "default")
        lookups += [(p, r) for r in rs]
    return lookups, noregion


def cli_regions(profile, emit):
    """Regions to list EKS clusters in for one profile (see _plan_lookups)."""
    lookups, _ = _plan_lookups([{"id": profile}], _explicit_regions(), list_aws_profiles())
    if not lookups:
        emit("No AWS region known - pass --region (e.g. --region us-east-1, or several: us-east-1,eu-west-1) or set a region in the profile.")
    return [r for _, r in lookups]


def scan_clusters(accounts, emit=print, progress=None, cancel=None, on_batch=None, regions=None, stats=None, lookups=None):
    """EKS clusters for the given profiles ([{id, name}], no limit): returns (clusters, failed_count), de-duplicated (same account +
    region + name) and in profile / region order. One `aws eks list-clusters` per profile + region, LIST_WORKERS in parallel.
    progress(done, total) counts those lookups; on_batch(new_clusters) gets clusters as they arrive. A lookup that fails is logged and
    skipped. `stats` (a dict) receives: lookups, failures [(profile, region, error)], profiles (all looked at), failed_profiles (every one of
    their lookups failed) - see scan_summary."""
    note = progress or (lambda *a: None)
    if stats is None:
        stats = {}
    profiles = list_aws_profiles()
    if lookups is None:
        lookups, noregion = _plan_lookups(accounts, _explicit_regions(regions), profiles)
    else:                                            # the caller picked the (profile, region) scopes (the window skips the ones it already collected)
        lookups, noregion = [tuple(x) for x in lookups], []
    total = len(lookups)
    stats.update(lookups=total, failures=[], profiles=[], failed_profiles=[], noregion=list(noregion), attempted=list(lookups), ok_scopes=[], last_scope=None)
    if not lookups:
        emit("No AWS region known - pass --region (e.g. --region us-east-1, or several: us-east-1,eu-west-1) or set a region in the profile.")
        return [], 0
    if noregion and len(accounts) > 1:
        emit(f"  {len(noregion)} profile(s) skipped - no region (type a region in the Region(s) box or set one in the profile): " + ", ".join(noregion[:5]))

    def stopped():
        return cancel is not None and cancel.is_set()

    def one(i):
        profile, region = lookups[i]
        if stopped():
            return i, None, "cancelled"
        data, err = aws_cli(["eks", "list-clusters"], {"region": region, "profile": profile}, timeout=90)
        if err:
            return i, None, err
        names = data.get("clusters") if isinstance(data, dict) else None
        rows = []
        for n in names or []:
            acct = (profiles.get(profile or os.environ.get("AWS_PROFILE") or "default") or {}).get("account")
            rows.append({"name": n, "region": region, "profile": profile, "account_id": acct, "where": region, "account": profile,
                         "account_name": profile or ENV_LABEL, "key": f"{acct or profile}/{region}/{n}".lower()})
        return i, rows, None
    results, failed, done, seen = {}, [], 0, set()
    with ThreadPoolExecutor(max_workers=max(1, min(LIST_WORKERS, total))) as pool:
        futures = [pool.submit(one, i) for i in range(total)]
        for fut in as_completed(futures):
            if fut.cancelled():
                continue
            i, rows, err = fut.result()
            if err == "cancelled":
                continue
            done += 1
            stats["last_scope"] = lookups[i]
            if rows is None:
                failed.append(i)
                stats["failures"].append((lookups[i][0], lookups[i][1], err))
                if total <= 5 or len(failed) <= 5:
                    p, r = lookups[i]
                    emit(f"  region {r}" + (f" (profile {p})" if len(accounts) > 1 and p else "") + f": could not list EKS clusters: {_first_line(err, 120)}")
            else:
                results[i] = rows
                new = [c for c in rows if c["key"] not in seen]
                seen.update(c["key"] for c in new)
                if new and on_batch:
                    on_batch(new)
            note(done, total)
            if stopped():
                for f in futures:
                    f.cancel()
    if len(failed) > 5:
        emit(f"  ... {len(failed)} of {total} profile/region lookups failed (expired credentials, no access, or EKS not available there).")
    stats["ok_scopes"] = [lookups[i] for i in sorted(results)]
    found, seen = [], set()
    for i in sorted(results):                       # profile / region order, whatever order the answers arrived in
        for c in results[i]:
            if c["key"] not in seen:
                seen.add(c["key"])
                found.append(c)
    per_profile = defaultdict(lambda: [0, 0])       # profile -> [lookups, failed lookups]
    for i, (profile, _region) in enumerate(lookups):
        per_profile[profile][0] += 1
        per_profile[profile][1] += 1 if i in failed else 0
    stats["profiles"] = [p or ENV_LABEL for p in per_profile]
    stats["failed_profiles"] = [p or ENV_LABEL for p, (n, f) in per_profile.items() if n and f == n and not stopped()]
    stats["expired_profiles"] = sorted({(p or os.environ.get("AWS_PROFILE") or "default") for p, _r, e in stats["failures"] if is_expired_error(e)})
    if stats["expired_profiles"]:
        emit("  Credentials expired for: " + ", ".join(stats["expired_profiles"]) + " - renew them outside this tool, press Re-check and collect again.")
    return found, len(failed)


def scan_summary(found, stats):
    """'Found 57 clusters in 12 accounts, 3 accounts failed: a, b, c' (+ the number of single region lookups that failed and were skipped)."""
    stats = stats or {}
    accounts = {c.get("account_id") or c.get("profile") or "default" for c in found}
    n = len(found)
    msg = f"Found {n} cluster{'s' if n != 1 else ''} in {len(accounts)} account{'s' if len(accounts) != 1 else ''}"
    fp = stats.get("failed_profiles") or []
    if fp:
        msg += f", {len(fp)} account{'s' if len(fp) != 1 else ''} failed: " + ", ".join(fp[:8]) + (" ..." if len(fp) > 8 else "")
    partial = len(stats.get("failures") or []) - sum(1 for f in (stats.get("failures") or []) if (f[0] or ENV_LABEL) in fp)
    if partial > 0:
        msg += f"; {partial} region lookup{'s' if partial != 1 else ''} failed and {'were' if partial != 1 else 'was'} skipped"
    if stats.get("expired_profiles"):
        msg += "; credentials expired for " + ", ".join(stats["expired_profiles"][:6]) + (" ..." if len(stats["expired_profiles"]) > 6 else "") + " - renew them outside this tool"
    checked = len(stats.get("profiles") or [])
    if checked and stats.get("lookups"):
        msg += f" ({checked} account{'s' if checked != 1 else ''} / profile{'s' if checked != 1 else ''}, {stats['lookups']} region lookups)"
    return msg + "."


def _menu_number(c, menu, by_name):
    """The ekslogin menu number of cluster `c` ({name, region, account_id}), or None when the menu does not offer it. A name that exists in
    several accounts / regions only matches a menu entry that also names the region or account (otherwise the wrong cluster could be logged in)."""
    name, region, acct = c["name"].lower(), (c.get("region") or "").lower(), str(c.get("account_id") or "")
    hits = []
    for num, label in (menu or {}).items():
        low = str(label).lower()
        if low == name or name in [t for t in re.split(r"[\s()/,:\[\]]+", low) if t]:
            hits.append((num, low))
    if not hits:
        return None
    if by_name[c["name"]] == 1 and len(hits) == 1:
        return hits[0][0]
    named = [num for num, low in hits if (region and region in low) or (acct and acct in low)]
    return named[0] if len(named) == 1 else None


def register_clusters(found, multi=False, menu=None):
    """Number the clusters 1..N in the given order, fill CLI_TARGETS and return {'1': 'name (region[/profile])'}.
    With the custom login (ekslogin) every cluster also gets `exe_number` (its number in the ekslogin menu) when the menu offers it - such a
    cluster is logged in with ekslogin, any other with `aws eks update-kubeconfig` - and `via` (what the window shows)."""
    CLI_TARGETS.clear()
    by_name = Counter(c["name"] for c in found)
    by_region = Counter((c["name"], c["region"]) for c in found)
    if menu is None and LOGIN_OPTS["method"] == "exe" and found:
        menu = exe_menu_clusters()
    clusters = {}
    for i, c in enumerate(found, start=1):
        alias = c["name"] if by_name[c["name"]] == 1 else (f"{c['name']}-{c['region']}" if by_region[(c["name"], c["region"])] == 1
                                                          else f"{c['name']}-{c['region']}-{c.get('profile') or 'default'}")
        exe_number = _menu_number(c, menu, by_name) if LOGIN_OPTS["method"] == "exe" else None
        via = "aws" if LOGIN_OPTS["method"] == "cli" else (f"ekslogin #{exe_number}" if exe_number else "aws (not in ekslogin menu)")
        CLI_TARGETS[str(i)] = dict(c, alias=alias, exe_number=exe_number, via=via)
        clusters[str(i)] = f"{c['name']} ({c['region']}" + (f"/{c['profile']}" if multi and c.get("profile") else "") + ")"
    return clusters


def _console_progress(emit, total_hint=None):
    """A progress callback that logs about every 10% (for the command line)."""
    last = {"n": -1}

    def note(done, total):
        step = max(1, total // 10)
        if done == total or done // step != last["n"]:
            last["n"] = done // step
            emit(f"  listing clusters: {done}/{total} profile/region lookups done")
    return note


def parse_profile_scope(text):
    """--profile NAME | a,b,c | all  ->  None (not given), "all", or a list of names."""
    t = (text or "").strip()
    if not t:
        return None
    if t.lower() == "all":
        return "all"
    names = [x.strip() for x in t.split(",") if x.strip()]
    return names or None


def scope_description(accounts, regions):
    names = [a["name"] or ENV_LABEL for a in accounts]
    ex = _explicit_regions(regions)
    return (f"{len(names)} profile{'s' if len(names) != 1 else ''} ({', '.join(names[:6])}{' ...' if len(names) > 6 else ''}) x "
            + ("every enabled region" if any(r.lower() in ALL_REGIONS for r in ex) else (", ".join(ex) if ex else "each profile's own region")))


def list_clusters_cli(emit=print, accounts=None, progress=None, cancel=None, on_batch=None, regions=None):
    """{'1': 'name (region)', ...} from `aws eks list-clusters` (read-only), numbered in the listed order. Fills CLI_TARGETS.
    From the command line it uses the profile(s) of --profile (a, a,b,c or all) - or the credentials the aws CLI would use anyway - after checking that their
    credentials work (a profile that is expired / not configured is skipped with the renew message; this tool never signs in), x the regions of --region
    (`all` via `aws ec2 describe-regions`, built-in list as fallback), in parallel, de-duplicated, failures logged and skipped.
    The window passes the profiles it has selected (`accounts`, [{id, name}]). No limit on profiles or regions."""
    CLI_TARGETS.clear()
    all_mode = bool(LOGIN_OPTS.get("all_clusters"))
    if accounts is None:
        PREFLIGHT.update(blocked=[], usable=[])
        if not shutil.which("aws"):
            emit("AWS CLI (aws) was not found on PATH - install it from https://aws.amazon.com/cli/ (this tool only uses the credentials that already exist in ~/.aws).")
            return {}
        profile = AWS_OPTS.get("profile")
        accounts = [{"id": profile, "name": profile}]
        scope = AWS_OPTS.get("profile_scope")                    # --profile a,b,c | all (nothing is scanned unless it is asked for)
        if scope == "all":
            rows, err = load_accounts()
            if rows:
                accounts = [{"id": a["id"], "name": a["name"]} for a in rows]
            elif err:
                emit(f"WARNING: could not read the profiles in ~/.aws ({err}); using the current credentials only.")
        elif isinstance(scope, list) and len(scope) > 1:
            accounts = [{"id": n, "name": n} for n in scope]
        elif all_mode and scope is None:
            emit("NOTE: --all-clusters no longer scans every profile. Only the selected / default profile is searched; use --profile a,b,c or --profile all "
                 "(and --region r1,r2 or --region all) to search more.")
        accounts, blocked = split_usable(accounts, emit)
        if blocked and accounts:
            emit("NOTE: " + ", ".join(a["name"] or ENV_LABEL for a in blocked) + " skipped (credentials expired or missing); the other profiles are searched.")
        if not accounts:
            return {}
        emit("Searching for EKS clusters in: " + scope_description(accounts, regions) + ".")
    if LOGIN_OPTS["method"] == "exe":
        exe_menu_clusters()                                      # read the ekslogin menu once, to see which clusters it offers
    stats = {}
    found, failed = scan_clusters(accounts, emit, progress or _console_progress(emit), cancel, on_batch, regions, stats=stats)
    emit(scan_summary(found, stats))
    if not found:
        regs = sorted({r for _, r in _plan_lookups(accounts, _explicit_regions(regions), list_aws_profiles())[0]})
        if regs:
            emit("No EKS clusters found in " + ", ".join(regs[:10]) + (" ..." if len(regs) > 10 else "") + " - check the profile and region.")
        return {}
    clusters = register_clusters(found, len(accounts) > 1)
    if LOGIN_OPTS["method"] == "exe":
        n_menu = sum(1 for t in CLI_TARGETS.values() if t.get("exe_number"))
        emit(f"{n_menu} of them are in the ekslogin menu (logged in with ekslogin); the other {len(found) - n_menu} are connected with aws eks update-kubeconfig.")
    return clusters


def cli_login(number, label, emit):
    """Connect to cluster `number` with the credentials that already exist in ~/.aws (no sign-in): check they work (`aws sts get-caller-identity`), then
    `aws eks update-kubeconfig`. Returns the kubectl context name. Raises CredentialsExpired when the profile's credentials are expired / missing,
    RuntimeError when anything else fails."""
    if not CLI_TARGETS:
        list_clusters_cli(emit)
    tgt = CLI_TARGETS.get(str(number))
    if not tgt:
        raise RuntimeError(f"cluster {number} is not in the AWS CLI cluster list - run --list to see the numbers")
    profile = tgt.get("profile") or AWS_OPTS.get("profile")
    ident, err = aws_cli(["sts", "get-caller-identity"], {"profile": profile}, timeout=30)
    if err:
        if classify_status(profile, err)["state"] in BLOCKED_STATES:
            raise CredentialsExpired(profile)
        raise RuntimeError(f"the AWS credentials of {profile or ENV_LABEL} do not work: {_first_line(err, 120)}")
    emit(f"Using your existing AWS credentials ({profile or ENV_LABEL}): {ident.get('Arn') if isinstance(ident, dict) else '?'}")
    # LOCAL-ONLY exception of the read-only guarantee: `aws eks update-kubeconfig` writes the user's local kubeconfig, never the cluster.
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
# READ-ONLY GUARANTEE (enforced, not just promised)
#
# Every command this tool starts goes through an ALLOW-LIST first. A command that is not on the list is REFUSED: no process is started, the
# attempt is recorded (and shown in the report with a CRIT finding), and the caller gets the usual "failed" value. Nothing is installed, nothing
# is created / changed / deleted on the cluster or in the cloud account, nothing is run inside a pod or on a node.
#   kubectl : get (incl. get --raw GET paths), logs, top, version, api-resources, api-versions, cluster-info, explain, auth can-i,
#             config get-contexts | current-context | view   |   LOCAL-ONLY: config use-context (switches the local kubeconfig's current context)
#   aws     : exactly the (service, command) pairs of READ_ONLY_CLOUD_COMMANDS below (describe / list / get / filter-log-events ...)
#             LOCAL-ONLY exceptions: `aws eks update-kubeconfig` (writes the LOCAL kubeconfig only) and the user's own login program ekslogin.exe
#             (only when the user chose 'Log in with ekslogin first'). The tool has NO sign-in of its own.
# No code path installs a tool (no pip / package manager / download / `aws configure`); install hints are printed as text only.
# ---------------------------------------------------------------------------

KUBECTL_READ_VERBS = {"get", "logs", "top", "version", "api-resources", "api-versions", "cluster-info", "explain"}
KUBECTL_CONFIG_READ = {"get-contexts", "current-context", "view"}
KUBECTL_LOCAL_ONLY = {("config", "use-context")}                   # local-only: changes the current context in the user's own kubeconfig
KUBECTL_WRITE_FLAGS = ("-f", "--filename", "--data", "--data-binary", "--data-raw", "-X", "--request", "--patch", "--patch-file", "--from-file", "--overwrite",
                       "--force", "--grace-period", "-k", "--kustomize", "--field-manager", "--server-side", "--dry-run")
READ_ONLY_CLOUD_COMMANDS = {         # aws service -> the read-only commands the tool uses (anything else is refused)
    "sts": {"get-caller-identity"},
    "eks": {"describe-addon", "describe-cluster", "describe-fargate-profile", "describe-nodegroup", "list-access-entries", "list-addons",
            "list-clusters", "list-fargate-profiles", "list-nodegroups"},
    "ec2": {"describe-flow-logs", "describe-instance-status", "describe-instance-types", "describe-instances", "describe-nat-gateways",
            "describe-network-acls", "describe-network-interfaces", "describe-regions", "describe-route-tables", "describe-security-groups",
            "describe-subnets", "describe-traffic-mirror-sessions", "describe-vpc-endpoints", "describe-vpcs"},
    "elbv2": {"describe-load-balancers", "describe-target-groups", "describe-target-health"},
    "elb": {"describe-instance-health", "describe-load-balancers"},
    "iam": {"list-attached-role-policies"},
    "cloudwatch": {"get-metric-statistics"},
    "logs": {"filter-log-events"},
}
LOCAL_ONLY_CLOUD_COMMANDS = {("eks", "update-kubeconfig")}     # local-only: the local kubeconfig; never touches the cluster
_RO = {"lock": threading.Lock(), "reads": 0, "blocked": []}


def _ro_block(tool, args, why):
    text = f"{tool} {' '.join(str(a) for a in args)}".strip()
    with _RO["lock"]:
        _RO["blocked"].append((text[:200], why))
    return f"blocked: read-only mode - {why}"


def check_kubectl(args):
    """None when `kubectl args` is allowed, else the reason (the command is then NOT run)."""
    a = [str(x) for x in (args or [])]
    if a and a[0] == "--context":
        a = a[2:]
    if not a:
        return "'' is not allowed"
    verb = a[0]
    if verb == "config":
        sub = a[1] if len(a) > 1 else ""
        if sub in KUBECTL_CONFIG_READ or ("config", sub) in KUBECTL_LOCAL_ONLY:
            return None
        return f"'config {sub}' is not allowed"
    if verb == "auth":
        return None if a[1:2] == ["can-i"] else f"'auth {a[1] if len(a) > 1 else ''}' is not allowed"
    if verb not in KUBECTL_READ_VERBS:
        return f"'{verb}' is not allowed"
    bad = [x for x in a[1:] if x in KUBECTL_WRITE_FLAGS or x.startswith(("--data=", "--filename=", "--patch=", "--request="))]
    if bad:
        return f"'{verb}' with {bad[0]} is not allowed"
    if verb == "get" and "--raw" in a:
        raw = a[a.index("--raw") + 1:]
        if len(raw) != 1 or not raw[0].startswith("/") or len(a) != 3:
            return "'get --raw' is only allowed as a plain GET of one path"
    return None


def check_aws(args):
    """None when `aws args` (service, command first) is a listed read-only command, else the reason."""
    a = [str(x) for x in (args or [])]
    service, cmd = (a + ["", ""])[:2]
    if cmd in READ_ONLY_CLOUD_COMMANDS.get(service, ()):
        return None
    return f"'{service} {cmd}' is not allowed"


def check_local_command(cmd):
    """Guard for the helper that starts local-only commands: `aws eks update-kubeconfig`, ekslogin.exe (the user's own login program), or a read-only kubectl / aws call."""
    c = [str(x) for x in cmd]
    tool = os.path.splitext(os.path.basename(c[0]))[0].lower() if c else ""
    if tool == "aws":
        pair = tuple(c[1:3])
        if pair in LOCAL_ONLY_CLOUD_COMMANDS:
            return None
        return check_aws(c[1:])
    if tool == "kubectl":
        return check_kubectl(c[1:])
    if c and os.path.basename(c[0]).lower() == os.path.basename(EKSLOGIN_EXE).lower():
        return None                                           # local-only: the user's own login program
    return f"'{tool or c[:1]}' is not allowed"


def read_only_status(since=None):
    """(read calls, [blocked attempts]) - since a snapshot taken with read_only_status()."""
    with _RO["lock"]:
        r, b = _RO["reads"], list(_RO["blocked"])
    if since:
        return r - since[0], b[len(since[1]):]
    return r, b


def read_only_lines(since):
    reads, blocked = read_only_status(since)
    lines = ["READ-ONLY GUARANTEE",
             f"  This tool only reads. It does not install, create, change or delete anything on the cluster or in the cloud account. {reads} read call(s), {len(blocked)} blocked.",
             "  Every command goes through an allow-list (kubectl get / logs / top / version; aws describe / list / get); anything else is refused before it starts."]
    for text, why in blocked:
        lines.append(f"  BLOCKED: {text}  ({why})")
    return lines


# ---------------------------------------------------------------------------
# kubectl helpers
# ---------------------------------------------------------------------------

KUBE_CONTEXT = None   # set by select_context(): every kubectl call is pinned to this context


def kubectl(args, timeout=KUBECTL_TIMEOUT):
    """Run kubectl (pinned to KUBE_CONTEXT once one is selected). Returns (ok, text). Never raises. READ-ONLY: see check_kubectl()."""
    why = check_kubectl(args)
    if why:
        return False, _ro_block("kubectl", args, why)          # refused: no process is started
    with _RO["lock"]:
        _RO["reads"] += 1
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
    why = check_kubectl(args)
    if why:
        return None, _ro_block("kubectl", args, why)
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

# Column headers are written in full words. Callers may still pass the short form: it is expanded here, so the text report
# and the HTML always agree.
HEADER_FULL = {
    "AZ": "Availability zone", "CIDR": "IP address range", "FREE IPs": "Free IP addresses", "POD IPs": "Pod IP addresses",
    "PUBLIC IP": "Public IP address", "INTERNAL IP": "Internal IP address", "PRIVATE IPs": "Private IP addresses",
    "DIR": "Direction", "PROTO": "Protocol", "SOURCE / DESTINATION": "Source or destination",
    "INSTANCE / AMI": "Instance type / machine image", "INSTANCE-ID": "Instance identifier", "EC2 NAME": "Server name tag",
    "PROVIDER-ID": "Cloud provider identifier", "NODEGROUP": "Node group", "SYSTEM CHECK": "System status check",
    "INSTANCE CHECK": "Instance status check", "CPU used/alloc": "CPU used / allocatable", "MEMORY used/alloc": "Memory used / allocatable",
    "DISK used/total": "Disk used / total", "IMAGEFS": "Container image storage", "SWAP": "Swap memory",
    "CPUreq": "CPU requested", "MEMreq": "Memory requested", "EPHEMERAL": "Temporary storage", "RST": "Restarts",
    "CPU use": "CPU used", "CPU req": "CPU requested", "CPU lim": "CPU limit", "MEM use": "Memory used", "MEM req": "Memory requested",
    "MEM lim": "Memory limit", "DISK use": "Disk used", "CPU %cl": "CPU percent of cluster", "MEM %cl": "Memory percent of cluster",
    "SUPPORT DL": "Support team contact", "HPA": "Horizontal Pod Autoscaler", "HPA min-max": "Autoscaler minimum-maximum replicas",
    "STORAGECLASS": "Storage class", "USED / LIMIT": "Used / limit", "NAMESPACES": "Namespaces", "WHAT": "Problems found",
    "NAMESPACE/POD": "Namespace / pod", "NAMESPACE/NAME": "Namespace / name", "ADD-ON": "Add-on", "ERROR-LIKE": "Error-like lines",
    "RX TOTAL": "Bytes received (total since start)", "TX TOTAL": "Bytes transmitted (total since start)",
    "RX NOW": "Bytes received per second (now)", "TX NOW": "Bytes transmitted per second (now)", "ERRORS": "Network errors",
    "ENIs": "Elastic network interfaces", "PODS (VPC IP)": "Pods using a VPC IP address", "LB KIND": "Load balancer kind",
    "PORTS (port[:nodePort])": "Ports (service port : node port)", "TLS": "Encryption (Transport Layer Security)",
    "DEFAULT-DENY": "Default-deny policy present", "POLICIES": "Network policies", "IN avg": "Bytes in: average per second",
    "IN peak": "Bytes in: peak per second", "IN total": "Bytes in: total", "OUT avg": "Bytes out: average per second",
    "OUT peak": "Bytes out: peak per second", "OUT total": "Bytes out: total", "2xx": "Successful responses (HTTP status 200-299)",
    "4xx": "Client errors (HTTP status 400-499)", "5xx (error rate)": "Server errors (HTTP status 500-599) and error rate",
    "DNS NAME": "Load balancer domain name", "PORT ALLOC ERRORS": "Port allocation errors", "NAT GATEWAY": "Network address translation gateway",
    "BYTES OUT": "Bytes sent out", "BYTES IN": "Bytes received", "TARGET LATENCY": "Target response time",
    "LOG LINES": "Log lines", "WHY COLLECTED": "Why collected", "SECURITY GROUPS": "Security groups", "SUBNETS": "Subnets",
    "NS": "Namespace", "SVC": "Service", "LB": "Load balancer", "NLB/ALB": "Network or application load balancer",
    # the processor / memory columns: the full word is in the header itself, the short form only in brackets
    "CPU used / allocatable": "Processor (CPU) used / allocatable", "CPU used": "Processor (CPU) used", "CPU requested": "Processor (CPU) requested",
    "CPU limit": "Processor (CPU) limit", "CPU percent of cluster": "Processor (CPU) percent of cluster", "CPU": "Processor (CPU) used",
    "HIGH PODS": "Pods near their limit", "RESTARTS": "Restarts", "ROUTE TABLE": "Route table", "DEFAULT ROUTE": "Default route",
    "PODS (VPC IP)": "Pods using an IP address of the Virtual Private Cloud (VPC)", "ERROR-LIKE": "Error-like log lines",
    "% of CPU limit": "Percent of the processor (CPU) limit", "% of memory limit": "Percent of the memory limit",
    "SUPPORT DL": "Support team contact", "Setting found in pod specs": "Setting found in pod specifications",
    "HTTP 502 responses": "Hypertext Transfer Protocol (HTTP) 502 responses", "HTTP 503 responses": "Hypertext Transfer Protocol (HTTP) 503 responses",
    "HTTP 504 responses": "Hypertext Transfer Protocol (HTTP) 504 responses",
    "aws-node state": "Network agent (aws-node) state", "aws-node restarts": "Network agent (aws-node) restarts",
    "kube-proxy state": "Service proxy (kube-proxy) state", "kube-proxy restarts": "Service proxy (kube-proxy) restarts",
    "PROFILE": "Fargate profile", "REPLICASET": "ReplicaSet", "USER": "User", "VERB": "Action", "RESOURCE": "Resource", "CODE": "Status code", "COUNT": "Count",
    "ZONE": "Zone", "TYPE": "Type", "VERSION": "Version", "ROLES": "Roles",
}

# "What each column means": the tooltip (title attribute) of a column header whose name alone is not obvious. Keys are the full-word headers.
COLUMN_HELP = {
    "Node": "The worker node: a virtual server that runs pods.",
    "Instance identifier": "The Amazon Web Services identifier of the server (Elastic Compute Cloud instance) behind the node, read from the node's provider identifier.",
    "Server name tag": "The Name tag of the cloud server, read from AWS when the AWS section is on.",
    "Zone": "The availability zone: the isolated data-centre location of the server, for example us-east-1a.",
    "Availability zone": "The isolated data-centre location inside the AWS region, for example us-east-1a.",
    "Capacity": "How the server is bought: ON_DEMAND (normal price) or SPOT (cheaper, but AWS can take it away).",
    "Node group": "The AWS-managed group of identical nodes this node belongs to.",
    "Internal IP address": "The private address of the node inside the network.",
    "Cloud provider identifier": "The full identifier Kubernetes stores for the node: cloud, zone and server identifier.",
    "Status": "The state of the object. For nodes: Ready accepts pods, NotReady does not, SchedulingDisabled means no new pods are placed on it (cordoned).",
    "Pods": "Number of pods. In the node table: running pods out of the maximum the node allows.",
    "Processor (CPU) used / allocatable": "Live processor use compared with the processor the node can give to pods (allocatable).",
    "Memory used / allocatable": "Live memory use compared with the memory the node can give to pods (allocatable).",
    "Disk used / total": "Used and total size of the node's root disk.",
    "Container image storage": "Space used by container images on the node (the imagefs file system): used / total.",
    "Swap memory": "Disk space the operating system uses as extra memory. Any use is a sign of memory pressure.",
    "Roles": "The Kubernetes role labels of the node, for example worker or control-plane.",
    "Version": "The Kubernetes (kubelet) version running on the node, or the version of the add-on.",
    "Age": "Time since the object was created.",
    "Processor (CPU) requested": "The processor that pods asked the scheduler to reserve. In node tables it is a percent of what the node can give (allocatable).",
    "Memory requested": "The memory that pods asked the scheduler to reserve. In node tables it is a percent of what the node can give (allocatable).",
    "Temporary storage": "Ephemeral storage: scratch disk space pods use, requested / allocatable.",
    "Findings": "What the tool flagged for this row; empty means nothing was found.",
    "Restarts": "How many times the containers of the pod restarted (a high number means a crash loop).",
    "Support team contact": "The team to contact for this namespace: the value of the 'elvh-app-support-dl' namespace label. A dash means the label is not set.",
    "Processor (CPU) used": "The processor in use right now, in cores (1.0 = one full core) or millicores (500m = half a core). n/a means no live reading.",
    "Processor (CPU) limit": "The most processor the pods may use together. 'partial/none' means at least one pod has no limit.",
    "Memory used": "The memory in use right now (the working set). n/a means no live reading.",
    "Memory limit": "The most memory the pods may use together. 'partial/none' means at least one pod has no limit; a pod above its limit is killed.",
    "Processor (CPU) percent of cluster": "Share of the processor of the whole cluster (used if a live reading exists, otherwise requested).",
    "Memory percent of cluster": "Share of the memory of the whole cluster (used if a live reading exists, otherwise requested).",
    "Pods near their limit": "Pods at 90 percent or more of their memory or processor limit.",
    "Disk used": "Temporary (ephemeral) disk space used by the pod or namespace.",
    "Configured": "Pods that the workloads (deployments, stateful sets, daemon sets) and standalone pods say should exist.",
    "Pod quota": "Pods used / the pod limit of the namespace's resource quota.",
    "Desired": "How many pods the workload is configured to run.",
    "Ready": "Ready out of wanted: ready containers of a pod, or ready replicas of a workload.",
    "Available": "Pods that have been ready long enough to count as available.",
    "Running": "Pods in the Running phase.",
    "Autoscaler minimum-maximum replicas": "The smallest and largest number of pods the horizontal pod autoscaler may run, and the current number.",
    "Used / limit": "What is used, then the limit set by the quota, and the percent used in brackets.",
    "Quota": "The name of the resource quota object.",
    "Why": "The reason Kubernetes reports.",
    "Reason": "Kubernetes' own short name for the cause, for example BackOff or FailedScheduling.",
    "Object": "The Kubernetes object the event is about.",
    "Count": "How many times it happened.",
    "Occurrences": "How many times events with this reason happened in the window.",
    "Horizontal pod autoscaler": "The object that adds or removes pods automatically when load changes.",
    "Replicas": "Current / desired / maximum number of pods of the autoscaler.",
    "Storage class": "The kind of disk the claim asks for.",
    "Phase": "The stage of the object: for a claim, Bound is healthy and Pending is waiting for a disk.",
    "Error-like log lines": "Log lines that contain words such as error, exception, fatal, refused or timeout.",
    "Warnings": "Log lines that contain the word warning.",
    "Why collected": "Why this pod's log was read: unhealthy, named in Warning events or a core add-on.",
    "Log": "current = the running container; previous = the container before its last restart.",
    "System status check": "The AWS check of the physical host of the server; impaired means a hardware or network problem on the AWS side.",
    "Instance status check": "The AWS check of the server's own operating system and network; impaired means the server itself is not responding.",
    "State": "The state reported by AWS.",
    "Health": "The health issues AWS reports, or OK.",
    "Nodes": "Ready nodes out of the wanted number (minimum and maximum in brackets).",
    "Direction": "in = traffic coming in to the resource, out = traffic going out.",
    "Protocol": "The network protocol: tcp, udp, icmp or all.",
    "Ports": "The port number or range the rule allows.",
    "Source or destination": "Where the traffic may come from (inbound) or go to (outbound): an address range, another security group or self.",
    "Status code": "The HTTP status of the request: 401 means not authenticated, 403 means not allowed.",
    "Action": "What the caller tried to do (get, list, create ...).",
    "Fargate profile": "A rule that sends pods of the listed namespaces to serverless Fargate capacity.",
    "Free IP addresses": "How many unused IP addresses are left in the subnet; pods and nodes need one each.",
    "IP address range": "The range of addresses of the subnet or network, written like 10.0.0.0/16.",
    "Result": "The outcome of the check or step.",
    "Time": "Time taken, or the time of the entry.",
    "Where": "The report section the finding comes from.",
    "Report section": "The report section the finding comes from; click it to jump there.",
    "Severity": "How serious the finding is: Critical, High, Medium or Information (see the legend).",
    "Finding": "What the tool found.",
    "Term": "The abbreviation or technical term as used in the report.",
    "Full name": "The term written out in full.",
    "Plain-language meaning": "What it means in everyday words.",
    "Time taken": "How long the cluster's run took.",
    "Sections collected": "How many of the report sections were collected for the cluster.",
    "Top finding": "The most serious finding of the cluster.",
    "Meaning": "What the word means.",
    "Short form in the text report": "How the severity is written in the .txt report, for example [CRIT].",
    "Critical": "Number of Critical findings of the cluster (broken now, act immediately).",
    "High": "Number of High findings of the cluster (a real problem).",
    "Medium": "Number of Medium findings of the cluster (needs attention, not urgent).",
    "Information": "Number of Information findings of the cluster (good to know).",
    "Cluster": "The cluster the row is about.",
    "Report": "Links to the cluster's full interactive report (.html) and its text report (.txt).",
}


def header_help(h):
    """The 'What each column means' tooltip of a (full-word) column header, or ''."""
    return COLUMN_HELP.get(str(h), "")


# The words used for severities and statuses, explained once in the Health summary and in the glossary.
SEVERITY_LEGEND = [
    ("Critical", "CRIT", "Something is broken now and needs action immediately (for example a node is NotReady or the cluster's front door is failing)."),
    ("High", "HIGH", "A real problem that is likely to hurt users or will soon become critical (for example crash loops or a degraded node group)."),
    ("Medium", "MED", "Something that needs attention but is not urgent (for example a nearly full quota or a few Warning events)."),
    ("Information", "INFO", "A fact worth knowing; no action is needed unless it surprises you."),
]
STATUS_LEGEND = [
    ("OK", "The check ran and found nothing wrong."),
    ("Warning", "The check found something that may become a problem or may be deliberate; read the advice."),
    ("Problem", "The check found something that is wrong and probably explains symptoms; act on the advice."),
    ("Not available", "The data could not be read (missing permission, feature off or no data). It does NOT mean the check passed."),
]


def full_header(h):
    """The column header in full words (explicit map first, then ALL-CAPS -> 'Sentence case')."""
    h = str(h)
    if h in HEADER_FULL:
        h = HEADER_FULL[h]
        return HEADER_FULL.get(h, h)          # a second step: 'CPU %cl' -> 'CPU percent of cluster' -> 'Processor (CPU) percent of cluster'
    if h and h.upper() == h and any(c.isalpha() for c in h):
        return h[0] + h[1:].lower()
    return h


# What the unavoidable technical terms mean. rep.glossary([...]) prints the ones a block uses just before that block, and the
# complete list (GLOSSARY) closes the network section.
GLOSSARY = {
    "CNI": ("Container Network Interface", "the plugin that gives every pod its network address and connects it to the network (on EKS: the Amazon VPC CNI, the aws-node pods)."),
    "VPC CNI": ("Amazon Virtual Private Cloud Container Network Interface", "the AWS plugin that gives pods real IP addresses from your VPC subnets."),
    "IPAMD": ("IP Address Management Daemon (L-IPAMD)", "the part of the VPC CNI on each node that keeps a pool of free IP addresses for new pods."),
    "DNS": ("Domain Name System", "turns names such as my-service.default.svc into IP addresses; inside a cluster it is answered by CoreDNS."),
    "CoreDNS": ("CoreDNS", "the DNS server that runs as pods in kube-system and answers every name lookup of the cluster."),
    "NodeLocal DNSCache": ("NodeLocal DNS Cache", "a small DNS cache on every node that lowers the load on CoreDNS and speeds up lookups."),
    "ndots": ("resolver option ndots", "how many dots a name needs before it is tried as-is; with the Kubernetes default of 5, external names are first tried with several cluster suffixes, which creates extra DNS queries."),
    "MTU": ("Maximum Transmission Unit", "the biggest network packet (in bytes) that can be sent without being split; a mismatch causes stalls on large transfers."),
    "NAT": ("Network Address Translation", "lets private addresses reach the internet through a shared public address (a NAT gateway)."),
    "SNAT": ("Source Network Address Translation", "replaces the pod address by the node address when traffic leaves the VPC; too many connections can run out of ports."),
    "VPC": ("Virtual Private Cloud", "your private network in AWS that holds the nodes, subnets and routes."),
    "ENI": ("Elastic Network Interface", "a virtual network card of an EC2 server; each one carries a limited number of IP addresses, which limits the pods per node."),
    "EC2": ("Elastic Compute Cloud", "the AWS virtual servers that the nodes run on."),
    "kube-proxy": ("kube-proxy", "the node component that forwards traffic sent to a Service address to one of the pods behind it."),
    "iptables": ("iptables mode", "kube-proxy mode that uses Linux packet-filter rules; simple, but slows down with very many Services."),
    "IPVS": ("IP Virtual Server", "kube-proxy mode that uses the Linux load balancer in the kernel; scales better with many Services."),
    "conntrack": ("connection tracking table", "the kernel table that remembers every open connection; when it is full, new connections are dropped."),
    "NetworkPolicy": ("Kubernetes NetworkPolicy", "a rule that says which pods may talk to which other pods or addresses."),
    "ClusterIP": ("ClusterIP Service", "a Service reachable only from inside the cluster, on a stable virtual address."),
    "NodePort": ("NodePort Service", "a Service that is also opened on one port (30000-32767 by default) on every node."),
    "LoadBalancer": ("LoadBalancer Service", "a Service that asks the cloud to create an external load balancer for it."),
    "ALB": ("Application Load Balancer", "the AWS load balancer that understands web (HTTP) requests; used by most Ingress objects."),
    "NLB": ("Network Load Balancer", "the AWS load balancer that forwards raw TCP/UDP connections; used by LoadBalancer Services."),
    "Ingress": ("Kubernetes Ingress", "a rule that routes web requests from outside the cluster to Services, by host name and path."),
    "TLS": ("Transport Layer Security", "the encryption (HTTPS) and the certificates behind it."),
    "Security group": ("Security group", "the AWS firewall attached to servers and network interfaces; it decides which traffic is allowed in and out."),
    "Network ACL": ("Network access control list", "an AWS firewall at subnet level; unlike security groups it has explicit allow AND deny rules and is checked for both directions."),
    "Endpoints": ("Service endpoints", "the list of ready pods behind a Service; with none, the Service cannot answer."),
    "Webhook": ("admission webhook", "an outside service the API server calls before it accepts a change; if it is down and set to 'Fail', changes are rejected."),
    "API server": ("Kubernetes API server", "the front door of the cluster that every tool and node talks to; it can throttle (reject with HTTP 429) when overloaded."),
    "etcd": ("etcd", "the database in which the cluster keeps its state; on EKS it is managed by AWS and not directly visible."),
    "Flow Logs": ("VPC Flow Logs", "an AWS record of the traffic accepted or rejected in the VPC, used to see who talked to whom."),
    "Container Insights": ("Amazon CloudWatch Container Insights", "AWS monitoring of cluster, node and pod metrics and logs."),
    "Network Flow Monitor": ("Amazon CloudWatch Network Flow Monitor", "AWS monitoring of network flows between pods, nodes and services (Container Network Observability)."),
    "DaemonSet": ("DaemonSet", "a workload that runs exactly one pod on every node, such as aws-node or kube-proxy."),
    "kubelet": ("kubelet", "the agent on each node that starts pods and reports the node's health and counters."),
    "CIDR": ("Classless Inter-Domain Routing block", "an address range written like 10.0.0.0/16."),
    "AWS": ("Amazon Web Services", "the cloud provider that hosts this cluster; the report reads its information with the read-only AWS command line."),
    "EKS": ("Amazon Elastic Kubernetes Service", "the AWS service that runs the Kubernetes control plane for you."),
    "CloudWatch": ("Amazon CloudWatch", "the AWS service that stores metrics (numbers over time) and logs; the traffic and control-plane log numbers come from it."),
    "IAM": ("Identity and Access Management", "the AWS system of users, roles and permissions that decides who may do what."),
    "ARN": ("Amazon Resource Name", "the unique text identifier of any AWS object, such as arn:aws:iam::123456789012:role/name."),
    "IRSA": ("IAM Roles for Service Accounts", "a way to give a pod its own AWS permissions instead of using the node's role."),
    "AMI": ("Amazon Machine Image", "the template (operating system image) a cloud server is started from."),
    "Fargate": ("AWS Fargate", "serverless capacity: pods run without you managing the servers underneath."),
    "CPU": ("Central processing unit (processor)", "the computing power of a node or pod, counted in cores (1.0 = one core) or millicores (500m = half a core)."),
    "Request": ("Resource request", "the amount of processor or memory a pod asks the scheduler to reserve for it."),
    "Limit": ("Resource limit", "the most processor or memory a pod may use; above the memory limit the pod is killed, above the processor limit it is slowed down."),
    "OOMKilled": ("Out of memory, killed", "the container used more memory than its limit, so the system stopped it."),
    "CrashLoopBackOff": ("Crash loop back-off", "the container keeps starting and crashing, and Kubernetes waits longer between tries."),
    "ImagePullBackOff": ("Image pull back-off", "Kubernetes cannot download the container image (wrong name, missing permission or registry down) and retries more slowly each time."),
    "HPA": ("Horizontal Pod Autoscaler", "the Kubernetes object that adds or removes pods automatically when the load changes."),
    "PVC": ("Persistent volume claim", "a pod's request for a disk; Bound means it got one."),
    "PV": ("Persistent volume", "an actual disk made available to the cluster."),
    "Quota": ("ResourceQuota", "a limit on the pods, processor, memory or storage a namespace may use together."),
    "Deployment": ("Deployment", "the workload type that keeps a number of identical pods running and rolls out new versions."),
    "StatefulSet": ("StatefulSet", "a workload for pods that need a stable name and their own disk, such as databases."),
    "ReplicaSet": ("ReplicaSet", "the object a Deployment creates for one version of its pods; a new one appears with every rollout."),
    "DL": ("Distribution list (support team contact)", "the e-mail group of the team that owns a namespace, read from the namespace label named in the report."),
    "kubectl": ("Kubernetes command line tool", "the program this report uses (read-only) to ask the cluster for information."),
    "readyz": ("Readiness endpoint", "the API server address that reports whether every internal check of the API server passes."),
    "UTC": ("Coordinated Universal Time", "the world time standard; all times in this report are UTC."),
    "IP": ("Internet Protocol address", "the numeric address of a server or pod on the network, for example 10.0.1.25."),
    "HTTP": ("Hypertext Transfer Protocol", "the protocol of web requests; status 5xx means a server error and 4xx a client error."),
    "AZ": ("Availability zone", "one isolated data-centre location inside an AWS region, for example us-east-1a."),
    "CSV": ("Comma-separated values", "a plain text table format that opens in a spreadsheet; the CSV button above a table saves its visible rows."),
    "aws-node": ("Amazon VPC CNI agent (aws-node)", "the DaemonSet that runs the Amazon VPC CNI on every node and hands out pod IP addresses."),
    "aws-auth": ("aws-auth ConfigMap", "the Kubernetes ConfigMap in kube-system that maps AWS IAM roles to Kubernetes users; a node whose role is not in it cannot join the cluster (newer clusters can use EKS access entries instead)."),
    "ConfigMap": ("ConfigMap", "a Kubernetes object that stores settings as text."),
    "SSM": ("AWS Systems Manager Session Manager", "an AWS tool that opens a shell on a server without opening a network port; used here only in the commands you can run yourself."),
    "SSH": ("Secure Shell", "an encrypted remote login to a server."),
    "API": ("Application programming interface", "the way programs talk to a service; the Kubernetes API and the AWS API are what kubectl and the AWS tools call."),
    "Provider ID": ("Cloud provider identifier", "the text Kubernetes stores for a node that names its cloud, zone and server, for example aws:///us-east-1a/i-0123456789abcdef0."),
}


def _locked(fn):
    """Report methods are called from several threads (run-ahead tasks, the GUI): one at a time."""
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return fn(self, *args, **kwargs)
    wrapper.__name__, wrapper.__doc__ = fn.__name__, fn.__doc__
    return wrapper


class Report:
    """Collects the report as text lines (streamed to `emit`) AND as structured sections /
    blocks (tables, logs, timeline) that the interactive HTML report is built from. Thread-safe."""

    def __init__(self, emit):
        self._lock = threading.RLock()
        self.lines = []
        self.emit = emit
        self.sections = [{"id": "s0", "title": "Run log", "blocks": []}]
        self.current = self.sections[0]
        self.terms = []             # glossary terms printed by this report, in order: the complete glossary at the end is built from them
        self.unexplained = []       # tables / blocks written without an `about` text: the tests make a non-empty list FAIL (nothing is hidden)

    @_locked
    def skip(self, title, reason=None, sid=None):
        """A section the user did not tick: ONE line in the text report, and an entry (no body) in the HTML table of contents."""
        self._text("")
        self._text(f"Skipped by choice: {title}" + (f"  ({reason})" if reason else ""))
        self.sections.append({"id": f"s{len(self.sections)}", "title": title, "blocks": [], "skipped": True, "reason": reason, "sid": sid})

    @_locked
    def _text(self, line):
        self.lines.append(line)
        self.emit(line)

    @_locked
    def _block(self, kind, *payload):
        blocks = self.current["blocks"]
        if kind == "lines":
            if blocks and blocks[-1][0] == "lines":
                blocks[-1][1].append(payload[0])
            else:
                blocks.append(("lines", [payload[0]]))
        else:
            blocks.append((kind, *payload))

    @_locked
    def add(self, text=""):
        for line in str(text).splitlines() or [""]:
            self._text(line)
            self._block("lines", line)

    @_locked
    def section(self, title, sid=None, minutes=None):
        """A numbered section. With `sid` (a key of SECTIONS) the 'What this section shows' / 'How to use it' text of the registry follows the heading."""
        self._text("")
        self._text("=" * 78)
        self._text(title)
        self._text("=" * 78)
        self.current = {"id": f"s{len(self.sections)}", "title": title, "blocks": []}
        info = SECTION_BY_ID.get(sid)
        if info:
            fill = lambda t: t.replace("{minutes}", str(minutes if minutes is not None else "selected"))
            self.current.update(sid=sid, shows=fill(info["shows"]), how=fill(info.get("how", "")))
            self._text("What this section shows: " + self.current["shows"])
            if self.current["how"]:
                self._text("How to use it: " + self.current["how"])
            self._use_terms(info.get("terms", ()))
        self.sections.append(self.current)

    @_locked
    def _use_terms(self, terms):
        for t in terms:
            if t in GLOSSARY and t not in self.terms:
                self.terms.append(t)

    @_locked
    def table(self, headers, rows, limit=MAX_ROWS, maxw=58, what=None, kind="", about=None, title=None, terms=None):
        """A table in the text report and in the HTML. Column headers are written out in full words (see HEADER_FULL).
        `about` (alias `what`) = one plain sentence 'What this table shows: ...' shown above the table (text and HTML); a table without it is
        recorded in self.unexplained. `title` = a heading printed above the table; `terms` = glossary terms printed before it."""
        what = about or what
        if not rows:
            return
        if terms:
            self.glossary(list(terms))
        if title:
            self._text("")
            self._text(f"--- {title} ---")
            self._block("head", title, None)
        headers = [full_header(h) for h in headers]
        rows = [["-" if c is None else str(c) for c in r] for r in rows]
        shown = rows[:limit]
        widths = [min(maxw, max([len(h)] + [len(r[i]) for r in shown])) for i, h in enumerate(headers)]

        def cell(text, w):
            return text if len(text) <= w else text[: w - 1] + "~"

        if what:
            self._text("  What this table shows: " + what)
        else:
            self.unexplained.append(("table", title or ", ".join(headers[:4])))
        self._text("  ".join(h.ljust(w) for h, w in zip(headers, widths)))
        for r in shown:
            self._text("  ".join(cell(c, w).ljust(w) for c, w in zip(r, widths)).rstrip())
        if len(rows) > limit:
            self._text(f"... and {len(rows) - limit} more")
        self._block("table", list(headers), rows, what, kind)   # the HTML report keeps ALL rows

    @_locked
    def subhead(self, title, what=None, about=None, terms=None):
        """A heading for one block of the report, with a one-line plain explanation under it (`about`, alias `what`; recorded in
        self.unexplained when missing). `terms` = glossary terms printed before the heading."""
        what = about or what
        if terms:
            self.glossary(list(terms))
        self._text("")
        self._text(f"--- {title} ---")
        if what:
            self._text("  What this block shows: " + what)
        else:
            self.unexplained.append(("block", title))
        self._block("head", title, what)

    @_locked
    def check(self, title, status, why, advice, what=None):
        """One named check with a status (OK / Warning / Problem / Not available), the reason, and what to do next."""
        self.subhead(title, what)
        self._text(f"  Status: {status}")
        self._text(f"  Why: {why}")
        self._text(f"  What this means / what to do next: {advice}")
        self._block("check", title, status, why, advice)

    @_locked
    def glossary(self, terms, title="Glossary: what these terms mean"):
        """The terms (keys of GLOSSARY) used by the block that follows: Term | Full name | Plain-language meaning."""
        self._use_terms(terms)
        rows = [[t, GLOSSARY[t][0], GLOSSARY[t][1]] for t in terms if t in GLOSSARY]
        if self.current.get("sid") != "network":          # a term explained once in a section is not repeated in the same section
            seen = self.current.setdefault("_gl", set())
            rows = [r for r in rows if r[0] not in seen]
            seen.update(r[0] for r in rows)
        if not rows:
            return
        self._text("")
        self._text(title)
        self._block("head", title, None)
        self.table(["Term", "Full name", "Plain-language meaning"], rows, limit=200, maxw=170,
                   what="the abbreviations and technical terms used just below, written out and explained.", kind="glossary")

    @_locked
    def log(self, title, entries, text_entries=None):
        """entries: [(text, kind)] with kind '' | 'warn' | 'err'. The HTML keeps all of them;
        the text report prints text_entries (a shortened version) when given. The caller puts a `subhead` with an explanation before the first log."""
        def norm(k):
            return "err" if k is True else ("" if k in (False, None) else k)
        entries = [(t, norm(k)) for t, k in entries]
        shown = entries if text_entries is None else [(t, norm(k)) for t, k in text_entries]
        self._text(f"[{title}]")
        for text, kind in shown:
            self._text(("  ERR> " if kind == "err" else "      ") + text)
        self._block("log", title, entries)

    @_locked
    def util(self, data, about=None):
        """Structured utilization data: rendered as the interactive dashboard in the HTML only (its parts explain themselves in the page)."""
        if not about:
            self.unexplained.append(("block", "utilization dashboard"))
        self._block("util", data, about)

    @_locked
    def series(self, title, rows, note="", about=None):
        """Time series (sparkline charts) - rendered in the HTML only; the numbers are printed as tables by the caller."""
        if rows:
            if not about:
                self.unexplained.append(("block", title))
            self._block("series", title, rows, note, about)

    @_locked
    def timeline(self, entries, about=None):
        """entries: [(datetime, text)]"""
        if about:
            self._text("  What this block shows: " + about)
        else:
            self.unexplained.append(("block", "timeline"))
        for ts, text in entries:
            self._text(f"{ts:%H:%M:%S}Z  {text}")
        self._block("timeline", [(f"{ts:%H:%M:%S}", text) for ts, text in entries], about)


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
        self.resources = None     # kubectl objects to read (None = all); see plan_sections()
        self.usage = True         # read the live CPU / memory / disk numbers?
        self.warm = None          # a _Warm on the scratch context of the run-ahead tasks, None on the real one
        self._lock = threading.RLock()

    def find(self, severity, text):
        with self._lock:
            self.findings.append((severity, text))
            section_id = self.report.current["id"] if self.report else "s0"
            self.findings_full.append((severity, text, section_id))
            cb = self.on_finding
        if cb:
            try:
                cb(severity, text)
            except Exception:
                pass

    def ns_issue(self, ns, text):
        with self._lock:
            if ns and text not in self.ns_issues[ns]:
                self.ns_issues[ns].append(text)

    def happened(self, ts, text):
        if ts and ts >= self.since:
            with self._lock:
                self.timeline.append((ts, text))


# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Parallel collection (after the login)
#
# Design (the lower-risk one): the report is still written by the ordinary, sequential section code, in the fixed order, so it is the same
# report whatever the number of workers.  What runs in parallel is the slow part - the read-only kubectl / aws calls:
#   * every kubectl / aws call goes through a thread-safe CACHE keyed by the command (+ kubectl context / aws region + profile) with
#     in-flight de-duplication, two semaphores (KUBECTL_CONCURRENCY, AWS_CONCURRENCY) and a back-off retry for AWS throttling errors;
#   * "run ahead" tasks execute the call-making sections (aws, network, logs ...) on a scratch copy of the context, concurrently, with the
#     calls inside a section fanned out (per node group, per load balancer, per metric ...), so by the time the report reaches a section
#     its calls are finished or in flight;
#   * the real section code then reads everything from the cache (a hit costs nothing).
# --workers 1 installs none of this: the calls run one after another exactly as before.
# ---------------------------------------------------------------------------

_TLS = threading.local()
_RT = None            # the active _Runtime while a parallel run_debug is in progress
_THROTTLE = re.compile(r"throttl|RequestLimitExceeded|Rate exceeded|TooManyRequests|Too Many Requests|SlowDown|RequestThrottled", re.I)
_CANCELLED = {"k": (False, "cancelled"), "a": (None, "cancelled")}


def _set_warm(flag):
    _TLS.warm = flag


def _pool(n):
    """A thread pool whose threads inherit the 'ahead of the report' flag of the thread that creates it."""
    return ThreadPoolExecutor(max_workers=n, initializer=_set_warm, initargs=(bool(getattr(_TLS, "warm", False)),))


class _Entry:
    __slots__ = ("ev", "res", "exc")

    def __init__(self):
        self.ev, self.res, self.exc = threading.Event(), None, None


class _Warm:
    """What the 'run ahead' tasks share: events that tell which data is ready."""

    def __init__(self, cancel):
        self.cancel = cancel
        self.events = {n: threading.Event() for n in ("data", "aws_basics", "aws_net", "lbs")}

    def set(self, name):
        self.events[name].set()

    def wait(self, name, limit=900):
        t0, ev = time.time(), self.events[name]
        while not ev.wait(0.2):
            if (self.cancel is not None and self.cancel.is_set()) or time.time() - t0 > limit:
                return False
        return True


class _Runtime:
    """The cache, the semaphores, the thread pools and the counters of ONE parallel run."""

    def __init__(self, workers, cancel):
        self.workers, self.cancel = workers, cancel
        self.lock = threading.Lock()
        self.entries = {}
        self.sem = {"k": threading.Semaphore(KUBECTL_CONCURRENCY), "a": threading.Semaphore(AWS_CONCURRENCY)}
        self.active, self.peak = {"k": 0, "a": 0}, {"k": 0, "a": 0}
        self.executed = self.hits = self.throttled = self.sched = self.done = 0
        self.work_secs = 0.0
        self.misses = []          # keys that were first requested by the report itself (not run ahead): should stay near zero
        self.on_tasks = None
        self._last = 0.0
        self.orig = {}
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="collect", initializer=_set_warm, initargs=(True,))
        self.leaf = ThreadPoolExecutor(max_workers=max(4, workers * 2), thread_name_prefix="call", initializer=_set_warm, initargs=(True,))

    def cancelled(self):
        return self.cancel is not None and self.cancel.is_set()

    def _tick(self, force=False):
        cb, now = self.on_tasks, time.time()
        if cb and (force or now - self._last >= 0.15):
            self._last = now
            try:
                cb(self.done, self.sched)
            except Exception:
                pass

    def _execute(self, kind, fn):
        sem = self.sem[kind]
        while not sem.acquire(timeout=0.2):
            if self.cancelled():
                return _CANCELLED[kind]
        t0 = time.time()
        try:
            if self.cancelled():
                return _CANCELLED[kind]
            with self.lock:
                self.active[kind] += 1
                self.peak[kind] = max(self.peak[kind], self.active[kind])
            try:
                tries = AWS_RETRIES if kind == "a" else 1
                res = None
                for attempt in range(tries):
                    res = fn()
                    if (kind == "a" and attempt < tries - 1 and isinstance(res, tuple) and len(res) > 1 and res[1]
                            and _THROTTLE.search(str(res[1])) and not self.cancelled()):
                        with self.lock:
                            self.throttled += 1
                        time.sleep(min(8.0, 0.5 * (2 ** attempt)) + random.random() * 0.25)     # AWS is throttling: back off, then try again
                        continue
                    break
                return res
            finally:
                with self.lock:
                    self.active[kind] -= 1
                    self.executed += 1
                    self.work_secs += time.time() - t0
        finally:
            sem.release()

    def call(self, kind, key, fn):
        """Run (or reuse) one read-only call. Never raises for a failed command (that comes back as the usual error value)."""
        if self.cancelled():
            return _CANCELLED[kind]
        if getattr(_TLS, "fresh", False):          # a deliberate second reading (live traffic sample): never from the cache
            return self._execute(kind, fn)
        counted = getattr(_TLS, "counted", False)
        with self.lock:
            entry = self.entries.get(key)
            owner = entry is None
            if owner:
                entry = self.entries[key] = _Entry()
                if not counted:
                    self.sched += 1
            else:
                self.hits += 1
        if owner:
            if not getattr(_TLS, "warm", False):
                self.misses.append(key)
            try:
                entry.res = self._execute(kind, fn)
            except BaseException as exc:
                entry.exc = exc
            entry.ev.set()
            if not counted:
                with self.lock:
                    self.done += 1
                self._tick()
        else:
            while not entry.ev.wait(0.2):
                if self.cancelled():
                    return _CANCELLED[kind]
        if entry.exc is not None:
            raise entry.exc
        return copy.deepcopy(entry.res) if kind == "a" else entry.res

    def hint(self, kind, key, fn):
        """Start a call in the background so that it is finished (or in flight) when the report needs it."""
        if self.cancelled():
            return
        with self.lock:
            if key in self.entries:
                return
            self.sched += 1
        try:
            self.leaf.submit(self._hint_job, kind, key, fn)
        except RuntimeError:           # the pools were shut down (Stop)
            with self.lock:
                self.done += 1

    def _hint_job(self, kind, key, fn):
        _TLS.counted = True
        try:
            self.call(kind, key, fn)
        except BaseException:
            pass
        finally:
            _TLS.counted = False
            with self.lock:
                self.done += 1
            self._tick()

    def memo(self, key, fn):
        """Compute fn() once for `key` (the first caller runs it, the others wait for the result)."""
        with self.lock:
            entry = self.entries.get(("memo", key))
            owner = entry is None
            if owner:
                entry = self.entries[("memo", key)] = _Entry()
        if owner:
            try:
                entry.res = fn()
            except BaseException as exc:
                entry.exc = exc
            entry.ev.set()
        else:
            while not entry.ev.wait(0.2):
                if self.cancelled():
                    return None
        if entry.exc is not None:
            raise entry.exc
        return entry.res

    def shutdown(self):
        for p in (self.pool, self.leaf):
            try:
                p.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass


def _key_k(args):
    return ("k", KUBE_CONTEXT, tuple(args))


def _key_a(args, target):
    t = target or {}
    return ("a", tuple(args), t.get("region"), t.get("profile"))


def _install_runtime(rt):
    """Put the cache in front of kubectl() and aws_cli() (whatever they are right now, including a test double). Returns what to restore."""
    global kubectl, aws_cli, _RT
    orig_k, orig_a = kubectl, aws_cli
    rt.orig = {"k": orig_k, "a": orig_a}

    def cached_kubectl(args, timeout=KUBECTL_TIMEOUT):
        why = check_kubectl(args)
        if why:
            return False, _ro_block("kubectl", args, why)
        return rt.call("k", _key_k(args), lambda: orig_k(args, timeout))

    def cached_aws_cli(args, target, timeout=60):
        why = check_aws(args)
        if why:
            return None, _ro_block("aws", args, why)
        return rt.call("a", _key_a(args, target), lambda: orig_a(args, target, timeout))
    kubectl, aws_cli, _RT = cached_kubectl, cached_aws_cli, rt
    return orig_k, orig_a


def _uninstall_runtime(saved):
    global kubectl, aws_cli, _RT
    kubectl, aws_cli = saved
    _RT = None


def _warm_on(ctx):
    return _RT is not None and getattr(ctx, "warm", None) is not None


def hint_k(ctx, args, timeout=KUBECTL_TIMEOUT):
    """(run-ahead tasks only) start `kubectl args` in the background."""
    rt = _RT
    if rt is not None and getattr(ctx, "warm", None) is not None and not check_kubectl(args):
        rt.hint("k", _key_k(args), lambda: rt.orig["k"](list(args), timeout))


def hint_a(ctx, args, target, timeout=60):
    """(run-ahead tasks only) start `aws args` in the background."""
    rt = _RT
    if rt is not None and getattr(ctx, "warm", None) is not None and target and not check_aws(args):
        rt.hint("a", _key_a(args, target), lambda: rt.orig["a"](list(args), target, timeout))


def warm_wait(ctx, name):
    w = getattr(ctx, "warm", None)
    return w.wait(name) if w is not None else True


def _fresh_kubectl(args, timeout=KUBECTL_TIMEOUT):
    """kubectl that is never answered from the cache (a deliberate second reading)."""
    _TLS.fresh = True
    try:
        return kubectl(args, timeout)
    finally:
        _TLS.fresh = False


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
    wanted = {name: args for name, args in RESOURCES.items() if ctx.resources is None or name in ctx.resources}     # unticked sections are never read
    with _pool(6) as pool:
        futures = {name: pool.submit(kjson, args) for name, args in wanted.items()}
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
    if ctx.usage:
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
    """Run `aws ...` (read-only). Returns (parsed_json_or_text_or_None, error_or_None). READ-ONLY: see check_aws()."""
    why = check_aws(args)
    if why:
        return None, _ro_block("aws", args, why)               # refused: no process is started
    with _RO["lock"]:
        _RO["reads"] += 1
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
# AWS profiles from ~/.aws (re-read after ekslogin, which may create / refresh them)
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
    profiles, sessions = {}, {}
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
                    if section.startswith("sso-session "):          # [sso-session x]: remember its start URL (an identity hint)
                        sessions[section[len("sso-session "):].strip()] = parser.get(section, "sso_start_url", fallback=None)
                    continue            # [sso-session x], [services x], ...
            else:
                name = section
            get = lambda key: parser.get(section, key, fallback=None)
            info = profiles.setdefault(name, {"region": None, "account": None, "role_arn": None, "sso": False, "source": None, "files": [], "sso_key": None,
                                                         "role_name": None, "start_url": None, "expires_at": None})
            info["files"].append("config" if is_config else "credentials")
            info["region"] = info["region"] or get("region")
            role = get("role_arn")
            if role:
                info["role_arn"] = role
            account = get("sso_account_id") or (AWS_ACCOUNT_IN_ARN.match(role).group(1) if role and AWS_ACCOUNT_IN_ARN.match(role) else None)
            info["account"] = info["account"] or account
            info["sso"] = info["sso"] or bool(get("sso_session") or get("sso_start_url"))
            info["sso_key"] = info["sso_key"] or get("sso_session") or get("sso_start_url")
            info["source"] = info["source"] or get("source_profile")
            info["role_name"] = info["role_name"] or get("sso_role_name")
            info["start_url"] = info["start_url"] or get("sso_start_url")
            if not info["expires_at"]:                      # temporary credentials written next to the keys carry a non-secret expiry (the keys / tokens are never read)
                for _k in ("aws_session_expiration", "aws_expiration", "expiration", "x_security_token_expires"):
                    if get(_k):
                        info["expires_at"] = _parse_expiry(get(_k))
                        break
    for info in profiles.values():
        if not info["start_url"] and info["sso_key"] in sessions:
            info["start_url"] = sessions[info["sso_key"]]
    return profiles


def describe_profile(name, info):
    kind = "SSO" if info.get("sso") else ("role" if info.get("role_arn") else "keys")
    return f"{name}  ({info.get('account') or 'account ?'}, {info.get('region') or 'no region'}, {kind})"


def profile_hint(info):
    """A short identity hint for the account selector: SSO start URL / account id / role where they are known."""
    info = info or {}
    parts = ["SSO" if info.get("sso") else ("role" if info.get("role_arn") else "keys")]
    if info.get("account"):
        parts.append(f"account {info['account']}")
    if info.get("role_name"):
        parts.append(f"role {info['role_name']}")
    elif info.get("role_arn"):
        parts.append("role " + info["role_arn"].rsplit("/", 1)[-1])
    if info.get("start_url"):
        parts.append(info["start_url"])
    return ", ".join(parts)


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
    """Read the profiles in ~/.aws (again after ekslogin), rank them for the selected cluster, and use the
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
        hint = "  -> credentials expired or missing: renew them outside this tool, then try again" if is_expired_error(err) else ""
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


def _cp_log_args(name, since_ms, audit=False):
    """The two CloudWatch control-plane queries (errors; 401/403 denials of the audit log) - shared by the report and the run-ahead hints."""
    group = f"/aws/eks/{name}/cluster"
    if not audit:
        return ["logs", "filter-log-events", "--log-group-name", group, "--start-time", since_ms,
                "--filter-pattern", "?error ?Error ?ERROR ?forbidden ?Unauthorized ?denied ?failed ?timeout", "--max-items", "300"]
    return ["logs", "filter-log-events", "--log-group-name", group, "--start-time", since_ms,
            "--log-stream-name-prefix", "kube-apiserver-audit",
            "--filter-pattern", "{ ($.responseStatus.code = 401) || ($.responseStatus.code = 403) }",
            "--max-items", "300"]


def _warm_aws(ctx, target, name):
    """(run-ahead tasks only) Start the AWS calls of the section as soon as their inputs are known - they are independent of each other -
    and publish what the network checks wait for. The report itself then finds every answer in the cache."""
    w = ctx.warm
    try:
        hint_a(ctx, ["sts", "get-caller-identity"], target, 30)
        hint_a(ctx, ["eks", "describe-cluster", "--name", name], target)
        _ident, err = aws_cli(["sts", "get-caller-identity"], target, timeout=30)
        desc, derr = aws_cli(["eks", "describe-cluster", "--name", name], target)
        c = (desc or {}).get("cluster") if not (err or derr) else None
        if not c:
            return
        ctx.data["aws_target"], ctx.data["aws_cluster"] = target, c
        w.set("aws_basics")
        vpc = c.get("resourcesVpcConfig", {})
        subnet_ids = vpc.get("subnetIds") or []
        sg_ids = list(dict.fromkeys(([vpc["clusterSecurityGroupId"]] if vpc.get("clusterSecurityGroupId") else []) + (vpc.get("securityGroupIds") or [])))
        enabled = set()
        for entry in (c.get("logging") or {}).get("clusterLogging", []) or []:
            if entry.get("enabled"):
                enabled |= set(entry.get("types") or [])
        if subnet_ids:
            hint_a(ctx, ["ec2", "describe-subnets", "--subnet-ids", *subnet_ids], target)
        if sg_ids:
            hint_a(ctx, ["ec2", "describe-security-groups", "--group-ids", *sg_ids], target)
        if c.get("roleArn"):
            hint_a(ctx, ["iam", "list-attached-role-policies", "--role-name", c["roleArn"].split("/")[-1]], target)
        hint_a(ctx, ["eks", "list-nodegroups", "--cluster-name", name], target)
        hint_a(ctx, ["eks", "list-addons", "--cluster-name", name], target)
        hint_a(ctx, ["eks", "list-fargate-profiles", "--cluster-name", name], target)
        hint_k(ctx, ["get", "configmap", "aws-auth", "-n", "kube-system", "-o", "json"])
        if enabled:
            since_ms = str(int(ctx.since.timestamp() * 1000))
            hint_a(ctx, _cp_log_args(name, since_ms), target, 90)
            if "audit" in enabled:
                hint_a(ctx, _cp_log_args(name, since_ms, audit=True), target, 90)
        # what the network checks need: subnets and security groups
        if subnet_ids:
            data, e = aws_cli(["ec2", "describe-subnets", "--subnet-ids", *subnet_ids], target)
            if not e:
                ctx.data["aws_subnets"] = data.get("Subnets", [])
        if sg_ids:
            data, e = aws_cli(["ec2", "describe-security-groups", "--group-ids", *sg_ids], target)
            if not e:
                ctx.data["aws_sgs"] = data.get("SecurityGroups", [])
        w.set("aws_net")
        # the EC2 side needs the node list
        if warm_wait(ctx, "data"):
            ids = {}
            for n in items(ctx.data.get("nodes")):
                m = re.search(r"/(i-[0-9a-f]+)$", n.get("spec", {}).get("providerID", ""))
                if m:
                    ids[m.group(1)] = n["metadata"]["name"]
            if ids:
                hint_a(ctx, ["ec2", "describe-instances", "--instance-ids", *list(ids)[:50]], target)
                hint_a(ctx, ["ec2", "describe-instance-status", "--include-all-instances", "--instance-ids", *list(ids)[:50]], target)
        # node groups -> their roles; add-ons; Fargate profiles; the access entries when aws-auth cannot be read
        ngs, e = aws_cli(["eks", "list-nodegroups", "--cluster-name", name], target)
        roles = set()
        if not e:
            groups = (ngs or {}).get("nodegroups", [])
            for g in groups:
                hint_a(ctx, ["eks", "describe-nodegroup", "--cluster-name", name, "--nodegroup-name", g], target)
            for g in groups:
                d, e = aws_cli(["eks", "describe-nodegroup", "--cluster-name", name, "--nodegroup-name", g], target)
                if not e and (d or {}).get("nodegroup", {}).get("nodeRole"):
                    role = d["nodegroup"]["nodeRole"]
                    if role not in roles:
                        roles.add(role)
                        hint_a(ctx, ["iam", "list-attached-role-policies", "--role-name", role.split("/")[-1]], target)
        ok, _out = kubectl(["get", "configmap", "aws-auth", "-n", "kube-system", "-o", "json"])
        if not ok:
            hint_a(ctx, ["eks", "list-access-entries", "--cluster-name", name], target)
        adds, e = aws_cli(["eks", "list-addons", "--cluster-name", name], target)
        if not e:
            for a in (adds or {}).get("addons", []):
                hint_a(ctx, ["eks", "describe-addon", "--cluster-name", name, "--addon-name", a], target)
        fps, e = aws_cli(["eks", "list-fargate-profiles", "--cluster-name", name], target)
        pnames = (fps or {}).get("fargateProfileNames", []) if not e else []
        for p in pnames:
            hint_a(ctx, ["eks", "describe-fargate-profile", "--cluster-name", name, "--fargate-profile-name", p], target)
        if pnames:
            hint_k(ctx, ["get", "namespace", "aws-observability"])
            hint_k(ctx, ["get", "configmap", "aws-logging", "-n", "aws-observability"])
    finally:
        w.set("aws_basics")
        w.set("aws_net")


def section_aws(rep, ctx, label):
    rep.section("2. AWS EKS CONTROL PLANE & INFRASTRUCTURE (Amazon Web Services / Amazon Elastic Kubernetes Service: cluster, network, identity and access roles, node groups, logging)",
                "aws", ctx.minutes)
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
    if ctx.warm is not None:
        _warm_aws(ctx, target, name)

    ident, err = aws_cli(["sts", "get-caller-identity"], target, timeout=30)
    if err:
        rep.add(f"AWS credentials: NOT WORKING ({_first_line(err)})")
        if "sso" in err.lower():
            rep.add("  " + renew_message(target["profile"], with_ekslogin=False))
        ctx.find("HIGH", "AWS CLI credentials are not valid - AWS-side checks skipped")
        return
    rep.add(f"Signed in as: {ident.get('Arn')}  (account {ident.get('Account')})")
    ctx.meta["identity"] = ident.get("Arn")

    desc, err = aws_cli(["eks", "describe-cluster", "--name", name], target)
    if err:
        rep.add(f"eks describe-cluster FAILED: {_first_line(err)}")
        ctx.find("MED", f"Could not describe the EKS cluster in AWS ({_first_line(err, 90)})")
        return
    c = desc.get("cluster", {})
    ctx.data["aws_target"], ctx.data["aws_cluster"] = target, c
    rep.subhead("Cluster state, endpoint and certificate",
                about="the cluster as AWS reports it: status, Kubernetes version, creation time, API server address and who may reach it, the service address range, "
                      "the certificate authority and whether your kubeconfig still matches the cluster.", terms=("EKS", "API server", "CIDR"))
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
    rep.subhead("Control-plane logging (Amazon CloudWatch log group /aws/eks/%s/cluster)" % name,
                about="which control-plane log types (api, audit, authenticator, controllerManager, scheduler) are switched on; a type that is off leaves nothing to read "
                      "when the control plane misbehaves.", terms=("CloudWatch",))
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
    rep.subhead("Network: virtual private cloud, subnets and security groups",
                about="the private network (Virtual Private Cloud) of the cluster, the free addresses left in each subnet and the firewall rules (security groups) "
                      "that protect the nodes and the control plane.", terms=("VPC", "Security group", "CIDR"))
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
            rep.table(["SUBNET", "AZ", "CIDR", "FREE IPs", "STATE", "NOTE"], rows,
                      about="one row per subnet of the cluster: its availability zone, its address range, how many addresses are still free (pods and nodes need one each) "
                            "and the state AWS reports; a note is added when the free addresses run low.")
    if sg_ids:
        data, err = aws_cli(["ec2", "describe-security-groups", "--group-ids", *sg_ids], target)
        if err:
            rep.add(f"  Security groups: {', '.join(sg_ids)}   (rules unavailable: {_first_line(err, 90)})")
            return
        ctx.data["aws_sgs"] = data.get("SecurityGroups", [])
        for sg in data.get("SecurityGroups", []):
            gid = sg["GroupId"]
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
            rep.table(["DIR", "PROTO", "PORTS", "SOURCE / DESTINATION"], rows, limit=20, title=f"Security group {gid} ({sg.get('GroupName')})",
                      about=f"the traffic rules of security group {gid}: for every rule the direction (in or out), the protocol, the ports and where the traffic may "
                            "come from or go to.")
            if not rows:
                rep.add(f"  Security group {gid} ({sg.get('GroupName')}): no rules")
            if gid == vpc.get("clusterSecurityGroupId") and not self_ref:
                ctx.find("MED", f"Cluster security group {gid} has no self-referencing inbound rule - nodes may not reach the API/each other")
            if not any(r[0] == "out" for r in rows):
                ctx.find("HIGH", f"Security group {gid} has no outbound rules - nodes can't reach the API, ECR or the internet")


def _aws_iam_and_compute(rep, ctx, target, c, name):
    rep.subhead("Identity and Access Management (IAM) roles and permissions",
                about="the AWS role of the cluster and whether it carries the managed permission policy that EKS needs (read with the AWS IAM service).", terms=("IAM", "ARN"))
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
    rep.subhead("MANAGED NODEGROUPS: node groups managed by Amazon Web Services for this cluster",
                about="the groups of worker nodes that AWS creates and replaces for you: their state, how many nodes are ready out of the wanted number and what AWS reports as health.",
                terms=("AMI",))
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
        rep.table(["NODEGROUP", "STATUS", "NODES", "INSTANCE / AMI", "HEALTH"], rows, maxw=90,
                  about="one row per managed node group: its status, ready nodes out of the wanted number (minimum and maximum in brackets), the server size and "
                        "machine image, and the health issues AWS reports.")

    rep.subhead("Node roles and the aws-auth mapping",
                about="whether the IAM role of every node group has the permissions nodes need, and whether that role is allowed to join the cluster (aws-auth ConfigMap or EKS access entries).",
                terms=("IAM", "aws-auth"))
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
    rep.subhead("Amazon Elastic Kubernetes Service (EKS) add-ons",
                about="the software add-ons that AWS installs and updates for the cluster (pod network plug-in, name lookups, service proxy ...), with their status, version and health.",
                terms=("VPC CNI", "CoreDNS", "kube-proxy"))
    names, err = aws_cli(["eks", "list-addons", "--cluster-name", name], target)
    if err:
        rep.add(f"  could not list add-ons: {_first_line(err, 100)}")
    else:
        ctx.data["aws_addons"] = list(names.get("addons", []))
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
            rep.table(["ADD-ON", "STATUS", "VERSION", "HEALTH"], rows, maxw=90,
                      about="one row per EKS-managed add-on: its status, installed version and the health issues AWS reports (OK when there are none).")
        else:
            rep.add("  no EKS-managed add-ons installed")

    # Fargate
    profiles, err = aws_cli(["eks", "list-fargate-profiles", "--cluster-name", name], target)
    names = (profiles or {}).get("fargateProfileNames", []) if not err else []
    if names:
        rep.subhead("Fargate profiles",
                    about="the rules that send pods of chosen namespaces to serverless Fargate capacity, plus whether Fargate pod logs are shipped.", terms=("Fargate",))
        rows = []
        for p in names:
            d, err = aws_cli(["eks", "describe-fargate-profile", "--cluster-name", name, "--fargate-profile-name", p], target)
            fp = (d or {}).get("fargateProfile", {}) if not err else {}
            sel = ", ".join(f"{s.get('namespace')}" + (f"[{','.join(s.get('labels', {}))}]" if s.get("labels") else "") for s in fp.get("selectors", []))
            if fp and fp.get("status") != "ACTIVE":
                ctx.find("HIGH", f"Fargate profile {p} status is {fp.get('status')}")
            rows.append([p, fp.get("status", "?"), sel or "-"])
        rep.table(["PROFILE", "STATUS", "NAMESPACES"], rows,
                  about="one row per Fargate profile: its status and the namespaces (with optional labels) whose pods it sends to Fargate.")
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
    rep.subhead("Elastic Compute Cloud (EC2) status of the nodes",
                about="the health AWS reports for the cloud server behind every node: its state and the two AWS status checks (the host and the server's own system).",
                terms=("EC2",))
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
    rep.table(["NODE", "INSTANCE-ID", "EC2 NAME", "STATE", "SYSTEM CHECK", "INSTANCE CHECK", "RESULT"], unhealthy or rows[:5],
              about="the servers with a problem (or, when all are healthy, the first five): node, server identifier, name tag, state and the two AWS status checks.")


def _aws_cp_logs(rep, ctx, target, name, enabled):
    group = f"/aws/eks/{name}/cluster"
    since_ms = str(int(ctx.since.timestamp() * 1000))
    rep.subhead(f"Control-plane log errors (last {ctx.minutes} minutes, from Amazon CloudWatch log group {group})",
                about="error-like lines of the managed Kubernetes control plane (API server, authenticator, controller manager ...) inside the time window, read from "
                      "Amazon CloudWatch Logs; they show problems you cannot see on the nodes.", terms=("CloudWatch", "API server"))
    data, err = aws_cli(_cp_log_args(name, since_ms), target, timeout=90)
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
        data, err = aws_cli(_cp_log_args(name, since_ms, audit=True), target, timeout=90)
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
        rep.subhead("Denied requests (status 401 and 403) in the audit log",
                    about="requests that the API server refused during the window because the caller was not authenticated (401) or not allowed (403), from the audit log.")
        if denied:
            rep.add(f"  API requests DENIED (401/403) in the window: {sum(denied.values())} (top callers)")
            rep.table(["USER", "VERB", "RESOURCE", "CODE", "COUNT"], [[u, v, r, c, n] for (u, v, r, c), n in denied.most_common(10)],
                      about="the callers with the most refused requests: who asked, what action, on which resource, the status code and how many times.")
            ctx.find("MED", f"{sum(denied.values())} API request(s) denied (401/403) in window, e.g. {denied.most_common(1)[0][0][0]}")
        else:
            rep.add("  no 401/403 denials in the audit log for the window.")


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------

def section_overview(rep, ctx, label):
    rep.section(f"1. CLUSTER OVERVIEW - {label}", "overview", ctx.minutes)
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
    top_jobs = None
    with _pool(8) as pool:
        if _RT is not None:       # parallel run: metrics-server is read while the kubelets answer
            top_jobs = (pool.submit(kubectl, ["top", "nodes", "--no-headers"]), pool.submit(kubectl, ["top", "pods", "-A", "--no-headers"]))
        for name, data, err in pool.map(one, names):
            if data:
                stats[name] = data
            elif err and not first_err:
                first_err = err.splitlines()[0][:140] if err else "unknown error"
        if top_jobs:
            top_jobs = (top_jobs[0].result(), top_jobs[1].result())
    ctx.data["node_stats"] = stats
    ctx.data["stats_error"] = first_err

    top_nodes, top_pods, top_err = {}, {}, None
    ok, out = top_jobs[0] if top_jobs else kubectl(["top", "nodes", "--no-headers"])
    if ok:
        for line in out.splitlines():
            p = line.split()
            if len(p) >= 5:  # NAME CPU(cores) CPU% MEMORY(bytes) MEMORY%
                top_nodes[p[0]] = {"cpu": parse_cpu(p[1]), "mem": parse_mem(p[3])}
    else:
        top_err = out.splitlines()[0][:140] if out else "unknown error"
    ok, out = top_jobs[1] if top_jobs else kubectl(["top", "pods", "-A", "--no-headers"])
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
    rep.section("3. NODES - STATUS, PROCESSOR (CPU), MEMORY, DISK, SWAP", "nodes", ctx.minutes)
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
    rep.table(["NODE", "INSTANCE-ID", "EC2 NAME", "ZONE", "TYPE", "CAPACITY", "NODEGROUP", "INTERNAL IP", "STATUS", "PROVIDER-ID"], inv_rows, maxw=64,
              title="Node inventory", terms=("EC2", "Provider ID"),
              about="one row per worker node: its name, the actual cloud server (Elastic Compute Cloud instance) it runs on, zone, size, how it is paid for, node group, "
                    "internal IP address and status. The instance identifier comes from the node's provider identifier; the server name tag is read from AWS when the AWS section is on.")
    rep.table(["NODE", "INSTANCE-ID", "STATUS", "PODS", "CPU used/alloc", "MEMORY used/alloc", "DISK used/total", "IMAGEFS", "SWAP"],
              usage_rows, maxw=40, title="Live usage per node", terms=("kubelet",),
              about="one row per node: pods running out of the maximum the node allows, and the live processor, memory, disk, container-image storage and swap use "
                    "compared with what the node can offer. n/a means no live reading (it needs the kubelet statistics permission or the metrics server).")
    rep.table(["NODE", "INSTANCE-ID", "ROLES", "TYPE", "ZONE", "VERSION", "AGE", "CPUreq", "MEMreq", "EPHEMERAL", "FINDINGS"],
              info_rows, maxw=110, title="Scheduling view: what the pods have requested", terms=("Request",),
              about="one row per node: its role, size, zone, kubelet version and age, and how much of its processor, memory and temporary storage the pods have "
                    "requested (a high percent leaves little room for new pods), with the problems the tool flagged.")
    notready = [(n["metadata"]["name"], re.search(r"/(i-[0-9a-f]+)$", n.get("spec", {}).get("providerID", "") or ""))
                for n in nodes
                if not any(c["type"] == "Ready" and c["status"] == "True" for c in n.get("status", {}).get("conditions", []))]
    if notready:
        rep.subhead("Node logs of NotReady nodes", terms=("SSM", "SSH"),
                    about="node logs cannot be read through kubectl, so these are the commands to run yourself (they open a session on the server, outside this tool).")
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
    rep.section("5. PODS ON EACH NODE - PROCESSOR (CPU), MEMORY, DISK per pod", "nodepods", ctx.minutes)
    pods = items(ctx.data.get("pods"))
    usage = ctx.data.get("pod_usage") or {}
    by_node = defaultdict(list)
    for p in pods:
        if p.get("spec", {}).get("nodeName") and p.get("status", {}).get("phase") in ("Running", "Pending"):
            by_node[p["spec"]["nodeName"]].append(p)
    if not by_node:
        rep.add("No pods are scheduled on nodes.")
        return
    rep.add("Used = live usage now; Requested and Limit = what the pod asked for and the most it may use. Memory used is the working set.")
    first_node = True
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
               + (f" | server name {ident['ec2_name']}" if ident.get("ec2_name", "-") != "-" else "") + "]") if ident else ""
        rep.table(["POD", "SUPPORT DL", "STATUS", "RST", "CPU use", "CPU req", "CPU lim", "MEM use", "MEM req", "MEM lim", "DISK use", "NOTES"],
                  [r[1] for r in rows], limit=MAX_NODE_PODS, maxw=60, title=f"Node {node}{who}  -  {len(by_node[node])} pod(s) ({running} running)",
                  terms=("Request", "Limit", "DL") if first_node else None,
                  about=f"the pods on node {node}, the biggest memory user first (at most {MAX_NODE_PODS} in the text report): status, restarts, live usage compared with "
                        "the requested and the limit values, and a note when a pod is above 90% of a limit.")
        first_node = False


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
    rep.section("6. NAMESPACES - PODS USED VERSUS CONFIGURED", "namespaces", ctx.minutes)
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
    rep.table(["NAMESPACE", f"SUPPORT DL", "PODS", "RUNNING"], owners, maxw=70, title="Who to contact for each namespace", terms=("DL",),
              about=f"one row per namespace: the support team to contact (the value of the namespace label '{SUPPORT_LABEL}', the same as kubectl get namespaces -l "
                    f"{SUPPORT_LABEL}), how many pods it has and how many run. NOT SET means the namespace has no such label.")
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
    rep.table(["NAMESPACE", "SUPPORT DL", "PODS", "RUNNING", "PENDING", "FAILED", "COMPLETED", "CONFIGURED", "STATUS", "POD QUOTA", "NOTES"], rows, maxw=60,
              title="Pods used versus configured per namespace", terms=("Deployment", "StatefulSet", "DaemonSet", "Quota"),
              about="one row per namespace: its pods by phase, how many pods its workloads are configured to run (the desired replicas of Deployments, StatefulSets and "
                    "DaemonSets plus standalone pods), whether that many run, and the pods used out of the pod limit of its resource quota, if it has one.")

    res_rows = []
    for n, d in sorted(ns.items(), key=lambda kv: -kv[1]["mem_use"]):
        if not d["total"]:
            continue
        res_rows.append([n, support_of(ctx, n) or "-", d["total"], _cores(d["cpu_use"]) if d["has_use"] else "n/a", _cores(d["cpu_req"]) if d["cpu_req"] else "-",
                         _mi(d["mem_use"]) if d["has_use"] else "n/a", _mi(d["mem_req"]) if d["mem_req"] else "-",
                         _mi(d["disk_use"]) if d["has_use"] and d["disk_use"] else "n/a", d["restarts"]])
    rep.table(["NAMESPACE", "SUPPORT DL", "PODS", "CPU use", "CPU req", "MEM use", "MEM req", "DISK use", "RESTARTS"], res_rows,
              title="Resources used by each namespace's pods", terms=("Request",),
              about="one row per namespace (largest memory user first): its pods, the processor and memory they use now and have requested, the temporary disk they use "
                    "and the total restarts. n/a means no live reading.")

    wl_rows.sort(key=lambda r: (r[8] == "OK", r[0], r[2]))
    if wl_rows:
        rep.table(["NAMESPACE", "SUPPORT DL", "KIND", "NAME", "DESIRED", "READY", "AVAILABLE", "RUNNING", "HPA min-max", "STATUS"],
                  [[r[0], support_of(ctx, r[0]) or "-"] + r[1:] for r in wl_rows], title=f"Workloads behind those pods ({len(wl_rows)})", terms=("HPA",),
                  about="one row per workload (Deployment, StatefulSet or DaemonSet): how many pods are desired (configured), ready, available and running now, "
                        "the autoscaler range if one is attached, and whether any pod is not ready.")

    quota_rows = []
    for n in sorted(quotas):
        for qname, key, used, hard in quotas[n]:
            pct = _pct(used, hard)
            if pct is not None and pct >= 75:
                ctx.find("HIGH" if pct >= 90 else "MED", f"Namespace {n}{support_suffix(ctx, [n])}: quota '{qname}' {key} is {pct:.0f}% used ({_fmt_qty(key, used)}/{_fmt_qty(key, hard)})")
                ctx.ns_issue(n, f"quota '{qname}' {key} is {pct:.0f}% used")
            quota_rows.append([n, support_of(ctx, n) or "-", qname, key, f"{_fmt_qty(key, used)}/{_fmt_qty(key, hard)} ({_fp(pct)})"])
    if quota_rows:
        rep.table(["NAMESPACE", "SUPPORT DL", "QUOTA", "RESOURCE", "USED / LIMIT"], quota_rows, title="Resource quotas (used / limit)",
                  about="the limits set on namespaces: for each quota the resource it limits, how much is used and the limit, with the percent used in brackets "
                        "(a quota at 100% blocks new pods or objects).")


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
    rep.section("7. UNHEALTHY PODS", "pods", ctx.minutes)
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
    rep.table(["NAMESPACE/POD", "SUPPORT DL", "STATUS", "READY", "RESTARTS", "NODE", "AGE", "WHY"],
              [[f"{a['ns']}/{a['name']}", support_of(ctx, a["ns"]) or "-", a["status"], a["ready"], a["restarts"], node_tag(ctx, a["node"]), a["age"],
                "; ".join(dict.fromkeys(a["problems"]))[:200]] for a in bad], maxw=110,
              title=f"Pods with problems ({len(bad)})", terms=("CrashLoopBackOff", "ImagePullBackOff", "OOMKilled", "DL"),
              about="one row per unhealthy pod, the most serious first: its status, how many containers are ready, restarts, the node it runs on, its age and "
                    "the reason Kubernetes reports.")
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
    rep.section(f"8. EVENTS (last {ctx.minutes} minutes)", "events", ctx.minutes)
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
        rep.table(["REASON", "OCCURRENCES"], [[r, c] for r, c in count.most_common()], title="Warning events by reason",
                  about="how often each kind of Warning event (grouped by Kubernetes' reason name) happened in the window, the most frequent first.")
        rows = []
        for t, e in sorted(warnings, key=lambda x: x[0], reverse=True)[:MAX_EVENTS]:
            obj = e.get("involvedObject") or e.get("regarding") or {}
            obj_name = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else obj.get("name", "?")
            rows.append([age(t, ctx.now) + " ago", e.get("reason", "?"),
                         f"{obj.get('kind', '?')} {obj.get('namespace', '')}/{obj_name}".replace(" /", " "),
                         support_of(ctx, obj.get("namespace")) or "-",
                         ((e.get("series") or {}).get("count") or e.get("count") or 1),
                         (e.get("message") or e.get("note") or "").replace("\n", " ")[:140]])
        rep.table(["WHEN", "REASON", "OBJECT", "SUPPORT DL", "COUNT", "MESSAGE"], rows, limit=MAX_EVENTS, title=f"Latest Warning events (up to {MAX_EVENTS})", terms=("DL",),
                  about="the newest Warning events: when, the reason, the object they are about, the team to contact, how many times it repeated and Kubernetes' message.")
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
        rows = []
        for t, e in sorted(notable, key=lambda x: x[0], reverse=True)[:25]:
            obj = e.get("involvedObject") or {}
            shown = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else obj.get("name", "?")
            rows.append([age(t, ctx.now) + " ago", e.get("reason"), f"{obj.get('kind', '?')} {shown}",
                         support_of(ctx, obj.get("namespace")) or "-",
                         (e.get("message") or "").replace("\n", " ")[:110]])
            ctx.happened(t, f"EVENT(normal) {e.get('reason')} {obj.get('kind', '?')} {shown}: "
                            f"{(e.get('message') or '')[:90]}")
        rep.table(["WHEN", "REASON", "OBJECT", "SUPPORT DL", "MESSAGE"], rows, limit=25, title="Notable Normal events (scaling, kills, node changes)",
                  about="Normal (non-warning) events that often explain a change: containers killed, scaling, nodes joining or leaving, with their object and message.")


def section_workloads(rep, ctx):
    rep.section("9. WORKLOADS", "workloads", ctx.minutes)
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
        for r in rows:
            ctx.ns_issue(r[1].split("/")[0], f"{r[0]} {r[1].split('/', 1)[1]} not fully ready ({r[2]})")
        rep.table(["KIND", "NAMESPACE/NAME", "SUPPORT DL", "READY", "ISSUE"],
                  [[r[0], r[1], support_of(ctx, r[1].split("/")[0]) or "-", r[2], r[3]] for r in rows],
                  title=f"Workloads that are not fully ready ({len(rows)})", terms=("Deployment", "StatefulSet", "DaemonSet", "DL"),
                  about="the Deployments, StatefulSets and DaemonSets that have fewer ready pods than wanted: ready out of wanted and what Kubernetes reports as the issue.")
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
        rep.table(["DEPLOYMENT", "SUPPORT DL", "REPLICASET", "READY", "CREATED"], recent, title=f"Recent rollouts and scale changes (new ReplicaSets in the last {ctx.minutes} minutes)",
                  terms=("ReplicaSet",),
                  about="Deployments that created a new ReplicaSet in the window (a new version was rolled out or the scale changed): the new ReplicaSet, how many of its pods are ready and when it appeared.")
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
        rep.table(["JOB", "SUPPORT DL", "FAILED PODS", "REASON", "WHEN"], failed, title="Jobs that failed in the window",
                  about="the jobs (one-off tasks) that ended in failure during the window: how many pods failed, the reason Kubernetes reports and when it happened.")
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
    rep.table(["KIND", "NAME", "READY", "STATE"], core, title="Core add-ons (kube-system)",
              about="the Deployments and DaemonSets in kube-system, where the cluster's own services run (name lookups, network agent, service proxy ...): ready pods out of wanted and "
                    "whether the add-on is OK or DEGRADED.")


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


# ---------------------------------------------------------------------------
# Network section: status vocabulary + helpers shared by the checks
# ---------------------------------------------------------------------------

ST_OK, ST_WARN, ST_BAD, ST_NA = "OK", "Warning", "Problem", "Not available"
_ST_RANK = {ST_OK: 0, ST_NA: 1, ST_WARN: 2, ST_BAD: 3}
NET_CHECK_ROWS = [      # the final 'Traffic issue checklist': (key, text shown in the Check column)
    ("pods", "Pod reachability evidence"),
    ("cni", "Container Network Interface logs and health"),
    ("nodes", "Node health"),
    ("proxy", "kube-proxy and Service routing"),
    ("dns", "Domain Name System (DNS)"),
    ("policy", "Network policies"),
    ("firewall", "Cloud firewalls (security groups and network access control lists)"),
    ("lb", "Load balancer health checks"),
    ("capture", "Packet capture availability"),
    ("observe", "Provider observability tools"),
]
NODE_PORT_RANGE = (30000, 32767)


def _note(ctx, key, status, evidence, nxt, supplement=False):
    """Remember one check result for the 'Traffic issue checklist' (several checks can feed one row). A supplementary check only
    changes the row when it found a Warning or Problem; its 'Not available' never hides the main checks' result."""
    if key:
        ctx.data.setdefault("net_checks", {}).setdefault(key, []).append((status, evidence, nxt, supplement))


def _net_check(rep, ctx, key, title, status, why, advice, what, find=None, sev=None, terms=(), supplement=False):
    """Print one named check block (glossary of its terms first, then heading, status, why, what to do) and record it."""
    if terms:
        rep.glossary(list(terms))
    rep.check(title, status, why, advice, what)
    _note(ctx, key, status, why, advice, supplement)
    if find and status in (ST_WARN, ST_BAD):
        ctx.find(sev or ("HIGH" if status == ST_BAD else "MED"), find)


def _cap(text):
    return text[:1].upper() + text[1:]


def _worst(statuses, default=ST_NA):
    statuses = [s for s in statuses if s]
    return max(statuses, key=lambda s: _ST_RANK[s]) if statuses else default


def _ds_get(ctx, name, ns="kube-system"):
    """A DaemonSet from the data already collected, else one read-only `kubectl get`."""
    for d in items(ctx.data.get("daemonsets")):
        if d["metadata"]["namespace"] == ns and d["metadata"]["name"] == name:
            return d
    obj = _kobj(["get", "daemonset", name, "-n", ns])
    return obj if isinstance(obj, dict) and (obj.get("metadata") or {}).get("name") == name else None


def _ds_find(ctx, *needles):
    """DaemonSets (any namespace) whose name contains one of the needles."""
    return [d for d in items(ctx.data.get("daemonsets")) if any(n in d["metadata"]["name"] for n in needles)]


def _pod_ready(p):
    sts = p.get("status", {}).get("containerStatuses") or []
    return p.get("status", {}).get("phase") == "Running" and bool(sts) and all(c.get("ready") for c in sts)


def _pod_restarts(p):
    return sum(c.get("restartCount", 0) for c in (p.get("status", {}).get("containerStatuses") or []))


def _pods_where(ctx, ns=None, prefixes=(), labels=None):
    out = []
    for p in items(ctx.data.get("pods")):
        meta = p["metadata"]
        if ns and meta["namespace"] != ns:
            continue
        lab = meta.get("labels") or {}
        if (prefixes and any(meta["name"].startswith(x) for x in prefixes)) or (labels and any(lab.get(k) in v for k, v in labels.items())):
            out.append(p)
    return out


def _net_log_lines(ctx, selector, ns="kube-system", tail=800):
    """(ok, lines, error) of the logs of every pod matching the label selector in the window - read once, shared by the checks."""
    cache = ctx.data.setdefault("_net_logs", {})
    key = (ns, selector)
    if key not in cache:
        ok, out = kubectl(["logs", "-n", ns, "-l", selector, "--all-containers", "--prefix", f"--since={ctx.minutes}m",
                           f"--tail={tail}", "--max-log-requests=20"], timeout=90)
        cache[key] = (ok, out.splitlines() if ok else [], "" if ok else (out.splitlines()[0][:100] if out else "unknown error"))
    return cache[key]


def _log_counts(lines, patterns):
    counts = {k: sum(1 for l in lines if re.search(p, l)) for k, p in patterns.items()}
    latest = {k: next((l for l in reversed(lines) if re.search(p, l)), "") for k, p in patterns.items()}
    return counts, latest


def _cni_env(ds):
    spec = ds.get("spec", {}).get("template", {}).get("spec", {})
    conts = spec.get("containers", [])
    cont = next((c for c in conts if c.get("name") == "aws-node"), {})
    env = {e["name"]: e.get("value", "(from secret/configmap)") for e in cont.get("env", []) if "name" in e}
    return cont, env, [c.get("name") for c in conts]


def _is_true(v):
    return str(v).strip().lower() == "true"


def _kube_proxy_mode(ctx):
    if "_kp_mode" not in ctx.data:
        mode = None
        cm = _kobj(["get", "configmap", "kube-proxy-config", "-n", "kube-system"])
        if cm:
            m = re.search(r"^\s*mode:\s*\"?([\w-]*)\"?", (cm.get("data") or {}).get("config", ""), re.M)
            mode = (m.group(1) if m else "") or "iptables (default)"
        ctx.data["_kp_mode"] = mode
    return ctx.data["_kp_mode"]


def _corefile(ctx):
    if "_corefile" not in ctx.data:
        ctx.data["_corefile"] = ((_kobj(["get", "configmap", "coredns", "-n", "kube-system"]) or {}).get("data") or {}).get("Corefile", "")
    return ctx.data["_corefile"]


# ---------------------------------------------------------------------------
# Network section, part 1: cluster-wide network settings
# ---------------------------------------------------------------------------

def _net_cluster_settings(rep, ctx):
    rep.subhead("Cluster network settings", "the address ranges, the DNS service address and the networking components of this cluster, as the cluster reports them.")
    cluster = ctx.data.get("aws_cluster") or {}
    net = cluster.get("kubernetesNetworkConfig", {})
    services = {(s["metadata"]["namespace"], s["metadata"]["name"]): s for s in items(ctx.data.get("services"))}
    dns_svc = services.get(("kube-system", "kube-dns"), {})
    k8s_svc = services.get(("default", "kubernetes"), {})
    rep.add(f"  Service IP address range : {net.get('serviceIpv4Cidr') or '(AWS section off) kubernetes service IP ' + (k8s_svc.get('spec', {}).get('clusterIP') or '?')}"
            f"   IP version: {net.get('ipFamily', '-')}")
    rep.add(f"  Domain Name System service: IP address {dns_svc.get('spec', {}).get('clusterIP', '?')}  ports "
            f"{','.join(str(p.get('port')) + '/' + p.get('protocol', '') for p in dns_svc.get('spec', {}).get('ports', [])) or '?'}")
    pod_cidrs = sorted({c for n in items(ctx.data.get("nodes")) for c in (n.get("spec", {}).get("podCIDRs") or [n.get("spec", {}).get("podCIDR")]) if c})
    rep.add(f"  Pod IP address ranges (set on nodes): {', '.join(pod_cidrs[:6]) + (' ...' if len(pod_cidrs) > 6 else '') if pod_cidrs else 'none set - pods take IP addresses straight from the VPC subnets (Amazon VPC CNI)'}")
    mode = _kube_proxy_mode(ctx)
    rep.add(f"  kube-proxy mode          : {mode or 'unknown (kube-proxy-config not readable)'}")
    core = next((d for d in items(ctx.data.get("deployments")) if d["metadata"]["namespace"] == "kube-system" and d["metadata"]["name"] == "coredns"), None)
    if core:
        rep.add(f"  CoreDNS                  : {core.get('status', {}).get('readyReplicas', 0)}/{core.get('spec', {}).get('replicas', '?')} ready")
    forwards = re.findall(r"forward\s+\.\s+(\S+)", _corefile(ctx))
    if forwards:
        rep.add(f"  CoreDNS upstream servers : {', '.join(forwards)}")
    ds = _ds_get(ctx, "aws-node")
    if ds:
        cont, _env, _names = _cni_env(ds)
        st = ds.get("status", {})
        rep.add(f"  Network plugin           : Amazon VPC CNI  image {cont.get('image', '?').split('/')[-1]}  ready {st.get('numberReady', 0)}/{st.get('desiredNumberScheduled', 0)}")
    else:
        rep.add("  Network plugin           : aws-node (Amazon VPC CNI) not found - this cluster may use another network plugin")
    rep.add("  (The checks that decide whether this is healthy follow below, one block each.)")


# ---------------------------------------------------------------------------
# Network section, part 2: pod-level networking
# ---------------------------------------------------------------------------

def _chk_cni(rep, ctx):
    terms = ("CNI", "VPC CNI", "VPC", "ENI", "IPAMD", "SNAT", "CIDR")
    title = "Container Network Interface plugin health (Amazon VPC CNI)"
    what = "whether the aws-node pods that give pods their IP addresses are running on every node, which version, and their IP address settings."
    ds = _ds_get(ctx, "aws-node")
    if not ds:
        _net_check(rep, ctx, "cni", title, ST_NA, "the aws-node DaemonSet was not found in kube-system - this cluster may use another network plugin (for example Calico or Cilium).",
                   "Find out which network plugin is installed (kubectl get daemonset -n kube-system); the checks below that depend on aws-node cannot run.", what, terms=terms)
        ctx.find("INFO", "aws-node (Amazon VPC CNI) DaemonSet not found - another CNI may be in use")
        return None
    cont, env, cnames = _cni_env(ds)
    st = ds.get("status", {})
    ready, desired = st.get("numberReady", 0), st.get("desiredNumberScheduled", 0)
    version = (cont.get("image", "?").split("/")[-1].split(":")[-1]) if ":" in cont.get("image", "") else "?"
    flags = []     # (setting, level, text)
    pd = _is_true(env.get("ENABLE_PREFIX_DELEGATION"))
    if env.get("MINIMUM_IP_TARGET") and not env.get("WARM_IP_TARGET"):
        flags.append(("MINIMUM_IP_TARGET", ST_WARN, "set, but has no effect unless WARM_IP_TARGET is also set"))
    if env.get("WARM_IP_TARGET") and env.get("WARM_ENI_TARGET"):
        flags.append(("WARM_ENI_TARGET", ST_WARN, "set together with WARM_IP_TARGET; the plugin then ignores it"))
    try:
        if env.get("WARM_IP_TARGET") and int(env["WARM_IP_TARGET"]) >= 30:
            flags.append(("WARM_IP_TARGET", ST_WARN, "very high: every node keeps many unused addresses, which drains the subnets"))
        if not pd and env.get("WARM_ENI_TARGET") and int(env["WARM_ENI_TARGET"]) >= 2:
            flags.append(("WARM_ENI_TARGET", ST_WARN, "2 or more spare network interfaces per node waste many addresses"))
    except ValueError:
        pass
    if _is_true(env.get("AWS_VPC_K8S_CNI_CUSTOM_NETWORK_CFG")) and not (env.get("ENI_CONFIG_LABEL_DEF") or env.get("ENI_CONFIG_ANNOTATION_DEF")):
        flags.append(("AWS_VPC_K8S_CNI_CUSTOM_NETWORK_CFG", ST_WARN, "custom networking is on but no ENIConfig selector (ENI_CONFIG_LABEL_DEF) is set: new pods may get no address"))
    if _is_true(env.get("ENABLE_POD_ENI")):
        flags.append(("ENABLE_POD_ENI", ST_OK, "security groups for pods are on: those pods use trunk/branch network interfaces, check the per-instance limits"))
    if pd:
        flags.append(("ENABLE_PREFIX_DELEGATION", ST_OK, "prefix delegation is on: far more pods per node, but the subnet needs free /28 blocks"))
    flag_by = {}
    for k, lvl, txt in flags:
        flag_by.setdefault(k, (lvl, txt))
    rows = []
    for k, d, why in CNI_SETTINGS:
        shown = env.get(k, f"{d} (default)" if d != "-" else "not set")
        lvl, txt = flag_by.get(k, (None, ""))
        rows.append([k, shown, why, (("Warning: " if lvl == ST_WARN else "Note: ") + txt) if txt else "OK"])
    status = ST_OK
    reasons = [f"aws-node is ready on {ready} of {desired} node(s); version {version}"]
    if desired and ready < desired:
        status = ST_BAD
        reasons.append(f"{desired - ready} node(s) have no ready aws-node: pods on them cannot get addresses")
    bad_flags = [f"{k}: {t}" for k, lvl, t in flags if lvl == ST_WARN]
    if bad_flags:
        status = _worst([status, ST_WARN])
        reasons.append("settings to review: " + "; ".join(bad_flags))
    if status == ST_OK:
        advice = "Nothing to do: the network plugin is running everywhere and its settings look consistent."
    elif status == ST_BAD:
        advice = "Look at the aws-node pods that are not ready (kubectl describe pod -n kube-system -l k8s-app=aws-node) and at their logs; fix the node role permissions (AmazonEKS_CNI_Policy) or free IP addresses if that is the cause."
    else:
        advice = "Review the flagged settings in the table below; change them in the vpc-cni add-on configuration, not by editing the DaemonSet."
    _net_check(rep, ctx, "cni", title, status, "; ".join(reasons) + ".", advice, what, terms=terms,
               find=(f"aws-node (Amazon VPC CNI) is ready on only {ready} of {desired} nodes" if status == ST_BAD else
                     (f"VPC CNI setting(s) to review: {bad_flags[0]}" if status == ST_WARN else None)))
    rep.table(["Setting", "Value", "What it does", "Assessment"], rows, maxw=90,
              what="the Amazon VPC CNI settings that decide how many IP addresses and pods a node can hold, with a note where a value looks wrong.")
    return env


def _chk_ip_limits(rep, ctx, env):
    title = "IP address exhaustion and per-instance network limits"
    what = "whether the subnets still have free IP addresses for new pods, and whether each node is near the number of pods its network interfaces allow."
    terms = ("ENI", "CIDR", "EC2")
    pods = items(ctx.data.get("pods"))
    all_on, vpc_on = Counter(), Counter()
    for p in pods:
        n = p.get("spec", {}).get("nodeName")
        if n and p.get("status", {}).get("phase") in ("Running", "Pending"):
            all_on[n] += 1
            if not p.get("spec", {}).get("hostNetwork"):
                vpc_on[n] += 1
    parts, why, advice = [], [], []
    subnets = ctx.data.get("aws_subnets") or []
    subnet_rows = []
    if subnets:
        host_net = sum(1 for p in pods if p.get("spec", {}).get("hostNetwork"))
        with_ip = [p for p in pods if p.get("status", {}).get("podIP") and not p.get("spec", {}).get("hostNetwork")
                   and p.get("status", {}).get("phase") in ("Running", "Pending")]
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
        lowest = None
        for sn, net in nets:
            top = ", ".join(f"{n} ({c})" for n, c in by_ns[sn["SubnetId"]].most_common(3)) or "-"
            free = sn.get("AvailableIpAddressCount")
            subnet_rows.append([sn["SubnetId"], sn.get("AvailabilityZone"), sn["CidrBlock"], free, used[sn["SubnetId"]], top])
            if isinstance(free, int) and (lowest is None or free < lowest[1]):
                lowest = (sn["SubnetId"], free)
        sst = ST_OK
        if lowest and lowest[1] < 10:
            sst = ST_BAD
        elif lowest and lowest[1] < LOW_SUBNET_IPS:
            sst = ST_WARN
        parts.append(sst)
        why.append(f"{len(with_ip)} pod(s) hold a VPC IP address and {host_net} use the node network; the emptiest subnet ({lowest[0]}) has {lowest[1]} free IP address(es)" if lowest
                   else f"{len(with_ip)} pod(s) hold a VPC IP address")
        if sst != ST_OK:
            advice.append("add subnets or a secondary address range (custom networking), or turn on prefix delegation, before new pods fail with 'no available IP'")
    else:
        why.append("the free IP addresses per subnet need the AWS section (not available here)")

    node_rows, over = [], []
    types = sorted({(n.get("metadata", {}).get("labels") or {}).get("node.kubernetes.io/instance-type") or (n.get("metadata", {}).get("labels") or {}).get("beta.kubernetes.io/instance-type")
                    for n in items(ctx.data.get("nodes"))} - {None})
    limits, lerr = {}, None
    target = ctx.data.get("aws_target") if AWS_OPTS["enabled"] else None
    if target and types:
        data, lerr = aws_cli(["ec2", "describe-instance-types", "--instance-types", *types[:100], "--query",
                              "InstanceTypes[].{t:InstanceType,e:NetworkInfo.MaximumNetworkInterfaces,i:NetworkInfo.Ipv4AddressesPerInterface}"], target, timeout=60)
        for d in (data if isinstance(data, list) else []):
            if isinstance(d, dict) and d.get("t"):
                limits[d["t"]] = (d.get("e"), d.get("i"))
    elif not target:
        lerr = "needs the AWS section"
    pd = _is_true((env or {}).get("ENABLE_PREFIX_DELEGATION"))
    nst = []
    for n in sorted(items(ctx.data.get("nodes")), key=lambda x: x["metadata"]["name"]):
        name = n["metadata"]["name"]
        lab = n["metadata"].get("labels") or {}
        typ = lab.get("node.kubernetes.io/instance-type") or lab.get("beta.kubernetes.io/instance-type") or "-"
        try:
            kube_max = int((n.get("status", {}).get("allocatable") or {}).get("pods", ""))
        except ValueError:
            kube_max = None
        enis, ips = limits.get(typ, (None, None))
        formula = None
        if enis and ips:
            formula = enis * (ips - 1) * (16 if pd else 1) + 2
            if pd:
                formula = min(formula, 250)
        note, level = "", ST_OK
        total = all_on[name]
        if kube_max and total >= kube_max:
            note, level = "node is FULL (pods = limit set in kubelet)", ST_BAD
        elif kube_max and total >= 0.9 * kube_max:
            note, level = "over 90% of the pod limit", ST_WARN
        if formula and vpc_on[name] >= formula:
            note, level = "no spare IP addresses from the network interfaces", ST_BAD
        elif formula and kube_max and kube_max > formula and not pd:
            note, level = (note + "; " if note else "") + "kubelet allows more pods than the network interfaces can address", _worst([level, ST_WARN])
        nst.append(level)
        if level != ST_OK:
            over.append(f"{node_tag(ctx, name)}: {note}")
        node_rows.append([node_tag(ctx, name), typ, enis if enis else "n/a", ips if ips else "n/a", formula if formula else "n/a",
                          kube_max if kube_max is not None else "n/a", total, note or "OK"])
    if node_rows:
        nstat = _worst(nst, ST_OK)
        parts.append(nstat)
        why.append(f"{len(node_rows)} node(s) checked against their pod limits" + (f"; {len(over)} near or at the limit" if over else "")
                   + ("" if limits else f" (network interface limits per instance type unavailable: {_first_line(lerr or 'unknown', 70)})"))
        if over:
            advice.append("nodes at their pod limit cannot start more pods: add nodes, use a larger instance type, or turn on prefix delegation")
    status = _worst(parts, ST_NA)
    if status == ST_NA:
        why = ["no subnet or node data could be read."]
        advice = ["Run again with the AWS section enabled."]
    _net_check(rep, ctx, "cni", title, status, "; ".join(why) + ".", _cap("; ".join(advice) + ".") if advice else "Nothing to do: enough free IP addresses and pod capacity.",
               what, terms=terms,
               find=(f"Node(s) at their pod / IP address limit: {over[0]}" if over and status in (ST_WARN, ST_BAD) else None),
               sev="HIGH" if status == ST_BAD else "MED", supplement=True)
    if subnet_rows:
        rep.table(["Subnet", "Availability zone", "IP address range", "Free IP addresses", "Pod IP addresses", "Top namespaces"], subnet_rows, maxw=70,
                  what="for each subnet of the cluster: how many IP addresses are still free and how many pods use it right now.")
    if node_rows:
        rep.table(["Node", "Instance type", "Maximum network interfaces", "Internet Protocol version 4 (IPv4) addresses per interface", "Pod limit from network interfaces",
                   "Pod limit set in kubelet", "Pods on node", "Note"], node_rows, maxw=60,
                  what="for each node: how many pods its network interfaces can address, the maximum its kubelet allows, and how many it runs now.")


CNI_PATTERNS = {
    "failed to assign an IP address": r"failed to assign|unable to assign|no available IP|InsufficientFreeAddressesInSubnet|IP address.*(exhaust|not available)",
    "network interface (ENI) problems": r"(?i)\beni\b.*(fail|error|limit)|AttachmentLimitExceeded|PrivateIpAddressLimitExceeded",
    "cloud API throttling (RequestLimitExceeded)": r"RequestLimitExceeded|Throttling|throttl|[Rr]ate exceeded",
    "other error lines": r"\"level\":\"error\"|\berror\b|ERROR",
}


def _chk_cni_logs(rep, ctx):
    title = "Container Network Interface daemon logs (L-IPAMD)"
    what = "what the aws-node pods logged in the selected window: failures to give a pod an IP address, network interface problems and cloud API throttling."
    ok, lines, err = _net_log_lines(ctx, "k8s-app=aws-node")
    if not ok:
        _net_check(rep, ctx, "cni", title, ST_NA, f"the aws-node logs could not be read ({err}).",
                   "Grant 'get pods/log' on kube-system, or read them yourself: kubectl logs -n kube-system -l k8s-app=aws-node --all-containers.", what, terms=("CNI", "IPAMD", "ENI"))
        return
    counts, latest = _log_counts(lines, CNI_PATTERNS)
    ctx.data["net_throttle"] = counts["cloud API throttling (RequestLimitExceeded)"]
    status = ST_OK
    bad = counts["failed to assign an IP address"] + counts["network interface (ENI) problems"]
    if bad:
        status = ST_BAD
    elif counts["cloud API throttling (RequestLimitExceeded)"] or counts["other error lines"]:
        status = ST_WARN
    if status == ST_OK:
        why, advice = f"{len(lines)} log line(s) read, no failures to assign IP addresses, no network interface errors, no throttling.", "Nothing to do."
    elif status == ST_BAD:
        why = (f"{counts['failed to assign an IP address']} 'failed to assign an IP address' and {counts['network interface (ENI) problems']} network interface error line(s) "
               f"in {len(lines)} log lines.")
        advice = "Pods are failing to get addresses: check the free IP addresses in the subnets (block above), the per-instance limits, and the node role permission AmazonEKS_CNI_Policy."
    else:
        why = f"{counts['cloud API throttling (RequestLimitExceeded)']} throttling line(s) and {counts['other error lines']} other error line(s) in {len(lines)} log lines."
        advice = "Read the latest error in the table; throttling means AWS is slowing the plugin's calls: lower WARM_* settings or spread node start-ups."
    _net_check(rep, ctx, "cni", title, status, why, advice, what, terms=("CNI", "IPAMD", "ENI"),
               find=(f"aws-node logged {counts['failed to assign an IP address']} 'no free IP' errors in the window - subnet or network interface IP exhaustion" if status == ST_BAD else None))
    rep.table(["Pattern", "Matching log lines", "Latest example"], [[k, counts[k], (latest[k] or "-")[:140]] for k in CNI_PATTERNS], maxw=90,
              what="how many log lines of the aws-node pods matched each kind of problem in the window, with the most recent example.")


def _chk_stuck_pods(rep, ctx):
    title = "Pods stuck creating (ContainerCreating / FailedCreatePodSandBox)"
    what = "pods that were scheduled but never got their network set up, and the 'sandbox' / network events that explain why."
    pods = items(ctx.data.get("pods"))
    events = {}
    sandbox_events = []
    for e in items(ctx.data.get("events")):
        t = _event_time(e)
        reason = e.get("reason", "")
        text = (e.get("message") or e.get("note") or "")
        if e.get("type") == "Warning" and t and t >= ctx.since and (reason in ("FailedCreatePodSandBox", "NetworkNotReady", "FailedCreatePodContainer")
                                                                    or re.search(r"failed to assign|network plugin|CNI|no available IP", text, re.I)):
            obj = e.get("involvedObject") or e.get("regarding") or {}
            events[(obj.get("namespace"), obj.get("name"))] = text.replace("\n", " ")
            sandbox_events.append((t, reason, obj, text))
    stuck = []
    for p in pods:
        if p.get("status", {}).get("phase") != "Pending" or not p.get("spec", {}).get("nodeName"):
            continue
        created = parse_ts(p["metadata"].get("creationTimestamp"))
        if created and (ctx.now - created).total_seconds() >= 120:
            key = (p["metadata"]["namespace"], p["metadata"]["name"])
            stuck.append([f"{key[0]}/{key[1]}", support_of(ctx, key[0]) or "-", node_tag(ctx, p["spec"]["nodeName"]), age(created, ctx.now), (events.get(key) or "no network event recorded")[:130],
                          (ctx.now - created).total_seconds()])
    running = sum(1 for p in pods if p.get("status", {}).get("phase") == "Running")
    with_ip = sum(1 for p in pods if p.get("status", {}).get("phase") == "Running" and (p.get("status", {}).get("podIP") or p.get("spec", {}).get("hostNetwork")))
    status = ST_OK
    if sandbox_events or any(r[-1] >= 300 for r in stuck):
        status = ST_BAD
    elif stuck:
        status = ST_WARN
    ev_txt = f"{len(sandbox_events)} sandbox / network start-up warning event(s) in the window"
    why = f"{len(stuck)} pod(s) waiting in ContainerCreating for over 2 minutes; {ev_txt}; {with_ip} of {running} running pods have an IP address."
    advice = {ST_OK: "Nothing to do: no pod is stuck waiting for its network.",
              ST_WARN: "Wait a few minutes and run again; if they stay, describe the pod (kubectl describe pod) and read the Container Network Interface logs block above.",
              ST_BAD: "Pods cannot start because their network is not set up: the usual causes are no free IP addresses, a failing aws-node pod, or a missing node role permission. See the blocks above."}[status]
    _net_check(rep, ctx, "pods", title, status, why, advice, what, terms=("CNI",),
               find=(f"{len(stuck)} pod(s) stuck in ContainerCreating / {len(sandbox_events)} sandbox network failure event(s) - pods cannot get their network" if status != ST_OK else None))
    if stuck:
        rep.table(["Pod", "Support team contact", "Node", "Waiting for", "Latest network event"], [r[:-1] for r in stuck[:30]], maxw=80,
                  what="pods that were placed on a node but are still waiting for their containers and network to be created.")
        for r in stuck:
            ctx.ns_issue(r[0].split("/")[0], f"pod {r[0].split('/', 1)[1]} stuck creating")
    if sandbox_events:
        rows = []
        for t, reason, obj, text in sorted(sandbox_events, key=lambda x: x[0], reverse=True)[:15]:
            rows.append([age(t, ctx.now) + " ago", reason, f"{obj.get('kind', '?')} {(obj.get('namespace') + '/') if obj.get('namespace') else ''}{obj.get('name', '?')}", text.replace("\n", " ")[:130]])
        rep.table(["When", "Reason", "Object", "Message"], rows, maxw=80,
                  what="the newest warning events about pod network start-up (sandbox creation, network plugin not ready, IP address assignment).")


def _chk_startup_order(rep, ctx):
    title = "Network start-up order (aws-node and kube-proxy readiness)"
    what = "whether aws-node (gives pods addresses) and kube-proxy (routes Service traffic) are ready on every node, and whether they keep restarting."
    cni = _pods_where(ctx, "kube-system", prefixes=("aws-node-",), labels={"k8s-app": ["aws-node"]})
    prox = _pods_where(ctx, "kube-system", prefixes=("kube-proxy-",), labels={"k8s-app": ["kube-proxy"]})
    nodes_unavail = [n["metadata"]["name"] for n in items(ctx.data.get("nodes"))
                     if any(c.get("type") == "NetworkUnavailable" and c.get("status") == "True" for c in n.get("status", {}).get("conditions", []))]
    if not cni and not prox:
        _net_check(rep, ctx, "cni", title, ST_NA, "no aws-node or kube-proxy pods were found in kube-system.",
                   "If the cluster uses another network plugin, check its own pods; otherwise confirm you may list pods in kube-system.", what, terms=("CNI", "kube-proxy", "DaemonSet"))
        return
    per_node = defaultdict(dict)
    for kind, group in (("aws-node", cni), ("kube-proxy", prox)):
        for p in group:
            per_node[p.get("spec", {}).get("nodeName") or "(unscheduled)"][kind] = p
    rows, bad, warn = [], [], []
    for node in sorted(per_node):
        cells = [node_tag(ctx, node)]
        note = []
        for kind in ("aws-node", "kube-proxy"):
            p = per_node[node].get(kind)
            if not p:
                cells += ["missing", "-"]
                note.append(f"no {kind} pod")
                bad.append(f"{node}: no {kind} pod")
                continue
            ready, rst = _pod_ready(p), _pod_restarts(p)
            cells += ["ready" if ready else "NOT READY", rst]
            if not ready:
                bad.append(f"{node}: {kind} not ready")
                note.append(f"{kind} not ready")
            elif rst > 0:
                warn.append(f"{node}: {kind} restarted {rst}x")
                note.append(f"{kind} restarted {rst}x")
        if per_node[node].get("kube-proxy") and per_node[node].get("aws-node") and _pod_ready(per_node[node]["kube-proxy"]) and not _pod_ready(per_node[node]["aws-node"]):
            note.append("kube-proxy is ready before aws-node: new pods on this node get no network yet")
        cells.append("; ".join(note) or "OK")
        rows.append(cells)
    for n in nodes_unavail:
        bad.append(f"{n}: NetworkUnavailable condition is True")
    status = ST_BAD if bad else (ST_WARN if warn else ST_OK)
    why = (f"{len(per_node)} node(s) checked; " + (f"{len(bad)} not ready: {bad[0]}" if bad else (f"{len(warn)} restart(s) seen: {warn[0]}" if warn else "both components are ready everywhere with no restarts")) + ".")
    advice = {ST_OK: "Nothing to do.",
              ST_WARN: "Restarts can mean crashes at start-up: read the previous container logs of the restarting pod (kubectl logs --previous).",
              ST_BAD: "A node whose aws-node or kube-proxy is not ready cannot run pods with a working network: describe that pod, check the node's conditions and the logs."}[status]
    _net_check(rep, ctx, "cni", title, status, why, advice, what, terms=("CNI", "kube-proxy", "DaemonSet"),
               find=(f"Network start-up: {bad[0]}" + (f" (+{len(bad) - 1} more)" if len(bad) > 1 else "") if status == ST_BAD else None))
    shown = [r for r in rows if r[-1] != "OK"] or rows[:5]
    rep.table(["Node", "aws-node state", "aws-node restarts", "kube-proxy state", "kube-proxy restarts", "Note"], shown, maxw=70,
              what="per node, the state and restart count of the two network pods (only nodes with a problem are listed; if all are fine, five examples are shown).")


# ---------------------------------------------------------------------------
# Network section, part 3: node networking
# ---------------------------------------------------------------------------

def _chk_node_conditions(rep, ctx):
    title = "Node conditions (NotReady and pressure)"
    what = "whether any node is NotReady or reports memory, disk, process-count or network problems, which also break pod networking."
    rows, bad, warn = [], [], []
    nodes = items(ctx.data.get("nodes"))
    for n in sorted(nodes, key=lambda x: x["metadata"]["name"]):
        conds = {c.get("type"): c for c in n.get("status", {}).get("conditions", [])}
        ready = conds.get("Ready", {}).get("status")
        probs = []
        if ready != "True":
            probs.append(f"NotReady ({conds.get('Ready', {}).get('reason', 'unknown')})")
        for t in ("MemoryPressure", "DiskPressure", "PIDPressure"):
            if conds.get(t, {}).get("status") == "True":
                probs.append(t)
        if conds.get("NetworkUnavailable", {}).get("status") == "True":
            probs.append("NetworkUnavailable")
        if probs:
            rows.append([node_tag(ctx, n["metadata"]["name"]), "Ready" if ready == "True" else "NotReady", ", ".join(probs)])
            (bad if (ready != "True" or "NetworkUnavailable" in probs) else warn).append(f"{n['metadata']['name']}: {', '.join(probs)}")
    if not nodes:
        _net_check(rep, ctx, "nodes", title, ST_NA, "the node list could not be read.", "Check that your login may list nodes (kubectl get nodes).", what)
        return
    status = ST_BAD if bad else (ST_WARN if warn else ST_OK)
    why = f"{len(nodes)} node(s) checked; " + (f"{len(bad)} NotReady or without network ({bad[0]})" if bad else (f"{len(warn)} with pressure ({warn[0]})" if warn else "all Ready, no pressure conditions")) + "."
    advice = {ST_OK: "Nothing to do.", ST_WARN: "Pressure makes the kubelet evict pods and can stall networking: free memory, disk or process slots on those nodes (see the Nodes section).",
              ST_BAD: "NotReady nodes drop their pods' traffic: check the node in the EC2 console / status checks and the kubelet, and replace it if it does not come back."}[status]
    _net_check(rep, ctx, "nodes", title, status, why, advice, what)
    if rows:
        rep.table(["Node", "Ready state", "Conditions that are not normal"], rows, maxw=80,
                  what="only the nodes that are NotReady or report a pressure / network condition.")


def _net_totals(iface_stats):
    """(bytes received, bytes transmitted, receive errors, transmit errors) from a kubelet 'network' stanza (interfaces list or flat)."""
    if not iface_stats:
        return None
    ifaces = iface_stats.get("interfaces")
    if ifaces:
        return (sum(i.get("rxBytes", 0) for i in ifaces), sum(i.get("txBytes", 0) for i in ifaces),
                sum(i.get("rxErrors", 0) for i in ifaces), sum(i.get("txErrors", 0) for i in ifaces))
    if "rxBytes" in iface_stats:
        return (iface_stats.get("rxBytes", 0), iface_stats.get("txBytes", 0), iface_stats.get("rxErrors", 0), iface_stats.get("txErrors", 0))
    return None


def _traffic_sample(ctx, nodes):
    """Wait TRAFFIC_SAMPLE_SECONDS, then read every node's kubelet counters again: ({node: stats}, seconds the sample took).
    In a parallel run the sample starts as soon as the cluster data is there and runs next to everything else (one reading, shared)."""
    rt = _RT

    def job():
        t0 = time.time()
        if rt is None:
            time.sleep(TRAFFIC_SAMPLE_SECONDS)
        else:                                  # small steps so that Stop does not have to wait for the whole sample
            steps = max(1, int(math.ceil(TRAFFIC_SAMPLE_SECONDS / 0.25)))
            for _ in range(steps):
                if ctx.cancel is not None and ctx.cancel.is_set():
                    break
                time.sleep(TRAFFIC_SAMPLE_SECONDS / steps)

        def one(node):
            ok, out = _fresh_kubectl(["get", "--raw", f"/api/v1/nodes/{node}/proxy/stats/summary"], timeout=60)
            try:
                return node, json.loads(out) if ok else None
            except json.JSONDecodeError:
                return node, None
        with _pool(8) as pool:
            fresh = {n: d for n, d in pool.map(one, nodes) if d}
        return fresh, max(1.0, time.time() - t0)
    if rt is None:
        return job()
    return rt.memo(("traffic", tuple(nodes)), job) or ({}, 1.0)


def _net_pod_traffic(rep, ctx):
    title = "Node and pod network counters (from each node's kubelet)"
    what = "how many bytes each node, pod and namespace received and sent, and whether the network interfaces counted errors."
    stats = ctx.data.get("node_stats") or {}
    if not stats:
        _net_check(rep, ctx, "nodes", title, ST_NA, "the kubelet statistics could not be read (they need the 'nodes/proxy' permission), so interface errors and dropped packets are not visible.",
                   "Ask for 'get nodes/proxy' access, or look at the node's network interface counters yourself; the traffic over the window comes from Amazon CloudWatch further below.",
                   what, terms=("kubelet",), supplement=True)
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
    rates_node, rates_pod = {}, {}
    if TRAFFIC_SAMPLE_SECONDS > 0:
        rep.emit(f"  sampling live traffic for {TRAFFIC_SAMPLE_SECONDS}s ...")
        fresh, dt = _traffic_sample(ctx, list(stats))
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
        rows.append([node_tag(ctx, node), _fmt_bytes(t[0]), _fmt_bytes(t[1]), _fmt_rate(r[0]) if r else "-", _fmt_rate(r[1]) if r else "-", t[2] + t[3]])
        if t[2] + t[3] > 0:
            err_nodes.append((node, t))
            ctx.find("MED", f"Node {node_tag(ctx, node)} has {t[2] + t[3]} network errors since boot (receive {t[2]}, transmit {t[3]})")
    if not rows:
        status, why, advice = ST_NA, "the kubelet answered but reported no network counters.", "Counters need a kubelet that exposes network statistics."
    elif err_nodes:
        status = ST_WARN
        why = f"{len(err_nodes)} node(s) counted network errors since they booted (for example {node_tag(ctx, err_nodes[0][0])}: {err_nodes[0][1][2] + err_nodes[0][1][3]})."
        advice = "Errors can mean a faulty network interface, an MTU mismatch or an overloaded instance; check the instance's network performance in CloudWatch. The kubelet reports error counters only, not dropped packets."
    else:
        status, why = ST_OK, f"{len(rows)} node(s) read, no interface errors counted since boot."
        advice = "Nothing to do. (The kubelet reports error counters only, not dropped packets.)"
    _net_check(rep, ctx, "nodes", title, status, why, advice, what, terms=("kubelet",), supplement=True)
    rep.table(["Node", "RX TOTAL", "TX TOTAL", "RX NOW", "TX NOW", "ERRORS"], rows, maxw=60,
              what="per node: bytes received and transmitted since the node started, the live rate during the sample, and interface errors.")

    prow = []
    for (ns, name), (t, node) in pod_a.items():
        r = rates_pod.get((ns, name))
        prow.append(((r[0] + r[1]) if r else 0, t[0] + t[1], [f"{ns}/{name}", support_of(ctx, ns) or "-", node_tag(ctx, node), _fmt_bytes(t[0]), _fmt_bytes(t[1]),
                                                           _fmt_rate(r[0]) if r else "-", _fmt_rate(r[1]) if r else "-", t[2] + t[3]]))
    prow.sort(key=lambda x: (-x[0], -x[1]))
    rep.table(["Pod", "SUPPORT DL", "Node", "RX TOTAL", "TX TOTAL", "RX NOW", "TX NOW", "ERRORS"], [x[2] for x in prow[:15]], maxw=60,
              what="the 15 pods with the most network traffic (totals since the pod started - not the selected window - and the live rate).")
    ns_tot = defaultdict(lambda: [0, 0])
    for (ns, _name), (t, _node) in pod_a.items():
        ns_tot[ns][0] += t[0]
        ns_tot[ns][1] += t[1]
    rep.table(["Namespace", "SUPPORT DL", "RX TOTAL", "TX TOTAL"],
              [[n, support_of(ctx, n) or "-", _fmt_bytes(v[0]), _fmt_bytes(v[1])] for n, v in sorted(ns_tot.items(), key=lambda kv: -(kv[1][0] + kv[1][1]))[:15]],
              what="bytes received and transmitted per namespace (totals since its pods started).")


def _chk_mtu(rep, ctx, env):
    title = "Maximum Transmission Unit (MTU) hints"
    what = "the packet size the network plugin is configured for; a mismatch between pods and network interfaces makes large transfers stall."
    terms = ("MTU", "CNI", "ENI")
    if env is None:
        _net_check(rep, ctx, None, title, ST_NA, "the aws-node settings were not available, and the real interface MTU cannot be read without node access (this tool never connects to nodes).",
                   "On a node you may use, run 'ip link' and compare the MTU of eth0 and the pod interfaces (9001 for jumbo frames inside the VPC, 1500 across the internet or VPN).", what, terms=terms)
        return
    eni_mtu, pod_mtu = env.get("AWS_VPC_ENI_MTU"), env.get("POD_MTU")
    labels = {k: v for n in items(ctx.data.get("nodes")) for k, v in (n["metadata"].get("labels") or {}).items() if "mtu" in k.lower()}
    if not eni_mtu and not pod_mtu and not labels:
        _net_check(rep, ctx, None, title, ST_NA,
                   "no MTU is set in the aws-node settings or node labels (the plugin then uses its defaults, normally 9001 on the interfaces), and the real value cannot be read without node access.",
                   "If large downloads or uploads hang while small requests work, compare the interface MTU on a node you may use ('ip link') with the pods' MTU, or set AWS_VPC_ENI_MTU / POD_MTU in the vpc-cni add-on.", what, terms=terms)
        return
    problems = []
    try:
        if eni_mtu and pod_mtu and int(pod_mtu) > int(eni_mtu):
            problems.append(f"POD_MTU {pod_mtu} is bigger than AWS_VPC_ENI_MTU {eni_mtu}")
        for k, v in (("AWS_VPC_ENI_MTU", eni_mtu), ("POD_MTU", pod_mtu)):
            if v and int(v) < 1280:
                problems.append(f"{k} {v} is below the IPv6 minimum of 1280")
    except ValueError:
        problems.append("a MTU value is not a number")
    shown = ", ".join(f"{k}={v}" for k, v in (("AWS_VPC_ENI_MTU", eni_mtu), ("POD_MTU", pod_mtu)) if v) or ", ".join(f"{k}={v}" for k, v in list(labels.items())[:3])
    status = ST_WARN if problems else ST_OK
    _net_check(rep, ctx, None, title, status, (f"the plugin is configured with {shown}" + ("; " + "; ".join(problems) if problems else "")) + ". The real interface value is not readable without node access.",
               "Fix the MTU settings in the vpc-cni add-on." if problems else "Nothing to do unless large transfers stall: then compare with 'ip link' on a node you may use.", what, terms=terms,
               find=(f"MTU settings look inconsistent: {problems[0]}" if problems else None))


def _chk_throttling(rep, ctx):
    title = "Cloud API throttling evidence"
    what = "signs that AWS is rate-limiting the calls the network plugin and controllers make (RequestLimitExceeded), which delays IP address and load balancer changes."
    terms = ("API",)
    n_logs = ctx.data.get("net_throttle")
    n_events = sum(1 for e in items(ctx.data.get("events")) if (_event_time(e) or ctx.since) >= ctx.since
                   and re.search(r"RequestLimitExceeded|Throttling|rate exceeded", (e.get("message") or e.get("note") or ""), re.I))
    if n_logs is None and not n_events:
        _net_check(rep, ctx, "cni", title, ST_NA, "the aws-node logs could not be read and no throttling events were found.",
                   "Read the aws-node logs yourself (kubectl logs -n kube-system -l k8s-app=aws-node) and search for RequestLimitExceeded.", what, supplement=True, terms=terms)
        return
    total = (n_logs or 0) + n_events
    status = ST_OK if not total else (ST_BAD if total >= 20 else ST_WARN)
    _net_check(rep, ctx, "cni", title, status,
               f"{n_logs if n_logs is not None else 'unknown'} throttling line(s) in the aws-node logs and {n_events} throttling event(s) in the window.",
               "Nothing to do." if status == ST_OK else "Reduce how often the account calls the EC2 API (fewer parallel node start-ups, lower WARM_* targets) or ask AWS Support for a higher limit.", what,
               find=(f"Cloud API throttling (RequestLimitExceeded) seen {total} time(s) in the window" if status != ST_OK else None), supplement=True, terms=terms)


# ---------------------------------------------------------------------------
# Network section, part 4: kube-proxy and Service routing
# ---------------------------------------------------------------------------

def _sg_port_verdict(ctx, port):
    """How the cluster's security groups treat inbound TCP `port`: text, or None when the rules are unknown."""
    sgs = ctx.data.get("aws_sgs")
    if not sgs:
        return None
    hits = []
    for sg in sgs:
        for perm in sg.get("IpPermissions", []):
            proto = perm.get("IpProtocol")
            lo, hi = perm.get("FromPort"), perm.get("ToPort")
            if proto not in ("-1", "tcp", "6"):
                continue
            if proto != "-1" and not (lo is not None and hi is not None and lo <= port <= hi):
                continue
            _proto, _ports, src = _fmt_perm(perm, sg["GroupId"])
            hits.append(f"{', '.join(x for x in src if x) or '-'} ({sg['GroupId']})")
    return ("allowed from " + "; ".join(dict.fromkeys(hits))[:120]) if hits else "no inbound rule found for this port"


def _chk_kube_proxy(rep, ctx):
    title = "kube-proxy health, mode and logs"
    what = "whether kube-proxy (which routes traffic sent to Service addresses) runs on every node, in which mode, and what it logged."
    terms = ("kube-proxy", "iptables", "IPVS", "DaemonSet")
    ds = _ds_get(ctx, "kube-proxy")
    mode = _kube_proxy_mode(ctx)
    if not ds:
        _net_check(rep, ctx, "proxy", title, ST_NA, "the kube-proxy DaemonSet was not found in kube-system (some clusters replace it, for example Cilium in kube-proxy replacement mode).",
                   "Confirm which component routes Service traffic in this cluster.", what, terms=terms)
        return
    st = ds.get("status", {})
    ready, desired = st.get("numberReady", 0), st.get("desiredNumberScheduled", 0)
    ok, lines, err = _net_log_lines(ctx, "k8s-app=kube-proxy")
    n_services = len(items(ctx.data.get("services")))
    pats = {"error lines": r"\bE\d{4} |\berror\b|ERROR|\bfailed\b", "rule sync problems": r"iptables-restore|ipvs.*(fail|error)|sync.*(fail|error)|Failed to execute iptables",
            "conntrack messages": r"conntrack"}
    counts, latest = _log_counts(lines, pats) if ok else ({k: 0 for k in pats}, {k: "" for k in pats})
    problems, notes = [], []
    if desired and ready < desired:
        problems.append(f"kube-proxy is ready on only {ready} of {desired} node(s)")
    if ok and (counts["error lines"] or counts["rule sync problems"]):
        notes.append(f"{counts['error lines']} error line(s), {counts['rule sync problems']} rule-sync problem line(s) in the logs")
    if (mode or "").startswith("iptables") and n_services >= 1000:
        notes.append(f"iptables mode with {n_services} Services: rule updates become slow, IPVS mode scales better")
    why = f"kube-proxy is ready on {ready} of {desired} node(s); mode {mode or 'unknown'}; {n_services} Service(s)."
    if problems:
        status = ST_BAD
    elif notes:
        status = ST_WARN
    elif not ok:
        status = ST_NA
        why += f" The logs could not be read ({err}), so errors are unknown."
    else:
        status = ST_OK
    if notes:
        why += " " + "; ".join(notes) + "."
    advice = {ST_OK: "Nothing to do.", ST_NA: "Read the logs yourself (kubectl logs -n kube-system -l k8s-app=kube-proxy) or ask for 'get pods/log' access.",
              ST_WARN: "Read the latest error in the table and consider switching kube-proxy to IPVS mode if the cluster has many Services.",
              ST_BAD: "Traffic to Service addresses fails on nodes without a ready kube-proxy: describe those pods and check their logs; restart or replace the node if it stays broken."}[status]
    _net_check(rep, ctx, "proxy", title, status, why, advice, what, terms=terms,
               find=(f"kube-proxy is ready on only {ready} of {desired} nodes - Service traffic fails there" if status == ST_BAD else
                     (f"kube-proxy: {notes[0]}" if status == ST_WARN else None)))
    if ok:
        rep.table(["Pattern", "Matching log lines", "Latest example"], [[k, counts[k], (latest[k] or "-")[:140]] for k in pats], maxw=90,
                  what="how many kube-proxy log lines in the window matched each kind of problem, with the most recent example.")


def _chk_services(rep, ctx):
    title = "Services: no ready endpoints, node ports and external exposure"
    what = "Services that have no pod behind them, the node ports that outside traffic would use, and the Services exposed outside the cluster."
    terms = ("ClusterIP", "NodePort", "LoadBalancer", "Endpoints", "Security group", "NLB")
    services = items(ctx.data.get("services"))
    by_type = Counter(s.get("spec", {}).get("type", "ClusterIP") for s in services)
    if not services:
        _net_check(rep, ctx, "proxy", title, ST_NA, "the Service list could not be read.", "Check that your login may list Services in all namespaces.", what, terms=terms)
        return
    endpoints = {(e["metadata"]["namespace"], e["metadata"]["name"]): e for e in items(ctx.data.get("endpoints"))}
    no_ep, rows, public, ports = [], [], [], {}
    for s in services:
        spec, meta = s.get("spec", {}), s["metadata"]
        stype = spec.get("type", "ClusterIP")
        key = (meta["namespace"], meta["name"])
        if spec.get("selector") and key in endpoints and key[1] != "kubernetes" and stype != "ExternalName":
            if not any(sub.get("addresses") for sub in (endpoints[key].get("subsets") or [])):
                no_ep.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], stype, "no ready pod behind this Service"])
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
        kind = ann.get("service.beta.kubernetes.io/aws-load-balancer-type") or ("classic or network load balancer" if stype == "LoadBalancer" else "-")
        pstr = ", ".join(f"{p.get('port')}" + (f":{p['nodePort']}" if p.get("nodePort") else "") + "/" + p.get("protocol", "TCP") for p in spec.get("ports", [])[:4])
        for p in spec.get("ports", []):
            if p.get("nodePort") and p.get("protocol", "TCP") == "TCP":
                ports.setdefault(p["nodePort"], []).append(f"{meta['namespace']}/{meta['name']}")
        rows.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], stype, scheme, kind, address[:60], pstr])
    if public:
        ctx.find("INFO", f"{len(public)} LoadBalancer Service(s) are internet-facing (no internal annotation): "
                         + ", ".join(public[:6]) + (" ..." if len(public) > 6 else "")
                         + support_suffix(ctx, {x.split("/")[0] for x in public}))
    pending = [r for r in rows if r[3] == "LoadBalancer" and r[6] == "-"]
    status = ST_WARN if (no_ep or pending) else ST_OK
    why = (f"{len(services)} Service(s): " + ", ".join(f"{v} {k}" for k, v in by_type.most_common()) + f"; {len(no_ep)} without a ready endpoint"
           + (f"; {len(pending)} LoadBalancer Service(s) without an external address yet" if pending else "")
           + (f"; {len(public)} internet-facing" if public else "") + ".")
    advice = ("Nothing to do." if status == ST_OK else
              "A Service without ready endpoints answers nothing: check that its selector matches running, ready pods (kubectl get pods --show-labels). A LoadBalancer without an address means the cloud load balancer was not created: describe the Service for events.")
    _net_check(rep, ctx, "proxy", title, status, why, advice, what, terms=terms)
    for r in no_ep:
        ctx.ns_issue(r[0], f"service {r[2]}: no ready endpoints")
    if no_ep:
        rep.table(["Namespace", "Support team contact", "Service", "Type", "Problem"], no_ep, maxw=70,
                  what="Services that select pods but have no ready pod behind them right now.")
    if rows:
        rep.table(["Namespace", "Support team contact", "Service", "Type", "Exposure", "Load balancer kind", "External address", "Ports (service port : node port)"], rows, maxw=64,
                  what="every Service that is reachable from outside its own cluster IP: NodePort, LoadBalancer and ExternalName Services, with the address and ports.")
    if ports:
        lo, hi = NODE_PORT_RANGE
        prow = []
        for port in sorted(ports)[:25]:
            verdict = _sg_port_verdict(ctx, port)
            prow.append([port, ", ".join(ports[port])[:60], verdict or "unknown - needs the AWS section"])
        rng = _sg_port_verdict(ctx, lo)
        rep.add(f"  Node port range {lo}-{hi}: security group check for the lowest port: {rng or 'unknown - needs the AWS section'}")
        rep.table(["Node port", "Used by", "Cloud firewall (security group) inbound rule"], prow, maxw=80,
                  what="the node ports in use and whether an inbound security group rule covers them (traffic from a load balancer to the nodes needs it).")


# ---------------------------------------------------------------------------
# Network section, part 5: Domain Name System
# ---------------------------------------------------------------------------

def _chk_dns(rep, ctx):
    title = "Domain Name System (DNS): CoreDNS health, configuration and logs"
    what = "whether the cluster's name server pods are healthy, how they are configured, what they logged, and how pods are set to look names up."
    terms = ("DNS", "CoreDNS", "NodeLocal DNSCache", "ndots", "DaemonSet")
    pods = _pods_where(ctx, "kube-system", prefixes=("coredns-", "kube-dns-"), labels={"k8s-app": ["kube-dns"]})
    core = next((d for d in items(ctx.data.get("deployments")) if d["metadata"]["namespace"] == "kube-system" and d["metadata"]["name"] in ("coredns", "kube-dns")), None)
    if not pods and not core:
        _net_check(rep, ctx, "dns", title, ST_NA, "no CoreDNS / kube-dns pods or Deployment were found in kube-system.",
                   "Check which DNS provider the cluster uses; without it pods cannot resolve Service names.", what, terms=terms)
        return
    desired = (core or {}).get("spec", {}).get("replicas", len(pods))
    ready_n = sum(1 for p in pods if _pod_ready(p))
    restarts = sum(_pod_restarts(p) for p in pods)
    corefile = _corefile(ctx)
    plugin = lambda name: bool(re.search(r"^\s*%s\b" % re.escape(name), corefile, re.M))
    cache_m = re.search(r"^\s*cache\s+(\d+)", corefile, re.M)
    forwards = re.findall(r"forward\s+\.\s+(.+)", corefile)
    nodelocal = _ds_find(ctx, "node-local-dns", "nodelocaldns")
    n_nodes, n_pods = len(items(ctx.data.get("nodes"))), len(items(ctx.data.get("pods")))
    large = n_nodes >= 50 or n_pods >= 1000
    ok, lines, err = _net_log_lines(ctx, "k8s-app=kube-dns")
    pats = {"SERVFAIL (server failure)": r"SERVFAIL", "REFUSED": r"REFUSED", "timeouts": r"i/o timeout|timed out", "NXDOMAIN (name does not exist)": r"NXDOMAIN",
            "other error lines": r"\[ERROR\]|plugin/errors"}
    counts, latest = _log_counts(lines, pats) if ok else ({k: 0 for k in pats}, {k: "" for k in pats})
    notes = []
    if corefile and not cache_m:
        notes.append("the Corefile has no 'cache' plugin: every lookup goes to the upstream server")
    if ok and counts["timeouts"] + counts["SERVFAIL (server failure)"] >= 10:
        notes.append(f"{counts['timeouts']} timeouts and {counts['SERVFAIL (server failure)']} SERVFAIL in the logs")
        ctx.find("MED", f"CoreDNS (DNS): {counts['timeouts']} timeouts and {counts['SERVFAIL (server failure)']} SERVFAIL in the window - DNS problems likely")
    if restarts >= 3:
        notes.append(f"CoreDNS pods restarted {restarts} time(s)")
    if desired and ready_n < desired:
        status = ST_BAD
    elif notes:
        status = ST_WARN
    elif not ok or not corefile:
        status = ST_NA
    else:
        status = ST_OK
    why = f"{ready_n} of {desired} CoreDNS pod(s) ready, {restarts} restart(s); upstream {', '.join(f.strip() for f in forwards) or 'unknown'}; cache {(cache_m.group(1) + ' s') if cache_m else 'not set'}; NodeLocal DNSCache {'present' if nodelocal else 'not found'}."
    if desired and ready_n < desired:
        why += " Not all CoreDNS pods are ready."
    if notes:
        why += " " + "; ".join(notes) + "."
    if status == ST_NA:
        why += f" Could not read: {'the CoreDNS logs (' + err + ')' if not ok else 'the Corefile'}."
    advice = {ST_OK: "Nothing to do.", ST_NA: "Read the missing data yourself (kubectl logs -n kube-system -l k8s-app=kube-dns, kubectl get configmap coredns -n kube-system -o yaml).",
              ST_WARN: "Read the latest error below; add the cache plugin or more CoreDNS replicas if lookups time out under load.",
              ST_BAD: "Name lookups fail or are slow while CoreDNS pods are not ready: describe those pods and check node capacity and the aws-node health above."}[status]
    if large and not nodelocal:
        advice += f" The cluster is large ({n_nodes} nodes, {n_pods} pods): consider NodeLocal DNSCache to cut DNS latency and load on CoreDNS."
        ctx.find("INFO", f"Large cluster ({n_nodes} nodes, {n_pods} pods) without NodeLocal DNSCache - consider it to reduce DNS load and latency")
    _net_check(rep, ctx, "dns", title, status, why, advice, what, terms=terms,
               find=(f"CoreDNS: only {ready_n} of {desired} pods ready - DNS lookups fail or are slow" if status == ST_BAD else None))
    plugins = [("kubernetes", "answers Service and pod names"), ("forward", "sends other names to an upstream server"), ("cache", "remembers answers"),
               ("loop", "stops forwarding loops"), ("ready", "readiness endpoint used by the Kubernetes probe"), ("health", "liveness endpoint"),
               ("errors", "logs errors"), ("log", "logs EVERY query (very verbose)"), ("autopath", "shortens the search-domain lookups"), ("prometheus", "exposes metrics")]
    if corefile:
        rep.table(["Corefile plugin", "Present", "Meaning"], [[n, "yes" if plugin(n) else "no", m] for n, m in plugins], maxw=70,
                  what="the CoreDNS configuration (Corefile) in short: which plugins are switched on.")
    prow = []
    for p in sorted(pods, key=lambda x: x["metadata"]["name"]):
        prow.append([p["metadata"]["name"], node_tag(ctx, p.get("spec", {}).get("nodeName")), "ready" if _pod_ready(p) else "NOT READY", _pod_restarts(p),
                     age(parse_ts(p["metadata"].get("creationTimestamp")), ctx.now)])
    if prow:
        rep.table(["Pod", "Node", "State", "Restarts", "Age"], prow, maxw=60, what="the CoreDNS pods: where they run, whether they are ready, and how often they restarted.")
    if ok:
        rep.table(["Pattern", "Matching log lines", "Latest example"], [[k, counts[k], (latest[k] or "-")[:140]] for k in pats], maxw=90,
                  what="how many CoreDNS log lines in the window matched each kind of DNS problem, with the most recent example.")
    # how the pods themselves are told to resolve names
    explicit, default_n, host_dns, host_net = Counter(), 0, 0, 0
    for p in items(ctx.data.get("pods")):
        if p.get("status", {}).get("phase") not in ("Running", "Pending"):
            continue
        spec = p.get("spec", {})
        pol = spec.get("dnsPolicy") or "ClusterFirst"
        nd = next((o.get("value") for o in (spec.get("dnsConfig") or {}).get("options", []) if o.get("name") == "ndots"), None)
        if pol == "Default" or (spec.get("hostNetwork") and pol != "ClusterFirstWithHostNet"):
            host_dns += 1
            host_net += 1 if spec.get("hostNetwork") else 0
        elif nd is not None:
            explicit[str(nd)] += 1
        else:
            default_n += 1
    drows = [[f"ndots:{v} set in the pod spec", c, "this lookup depth was chosen by the owner of the pod"] for v, c in sorted(explicit.items())]
    drows.append(["ndots:5 (Kubernetes default, nothing set)", default_n,
                  "a name with fewer than 5 dots is first tried with each cluster search suffix, so one external lookup can cost up to 5 extra queries"])
    drows.append(["pod uses the node's DNS settings (policy Default or host network)", host_dns, "these pods do not use CoreDNS"])
    rep.table(["Setting found in pod specs", "Pods", "What it means"], drows, maxw=100,
              what="how the running pods are configured to look up names (read from the pod specs; the search list itself is not read because that would need a command inside the pod).")
    ctx.data["net_ndots_default"] = default_n


# ---------------------------------------------------------------------------
# Network section, part 6: network policies and cloud firewalls
# ---------------------------------------------------------------------------

def _chk_policies(rep, ctx, env):
    title = "Network policies and the policy engine"
    what = "which namespaces have NetworkPolicies (and whether any denies all traffic by default), and whether something in the cluster actually enforces them."
    terms = ("NetworkPolicy", "CNI", "VPC CNI")
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
    engines = []
    ds = _ds_get(ctx, "aws-node")
    if ds:
        _cont, e2, cnames = _cni_env(ds)
        if "aws-eks-nodeagent" in cnames or _is_true(e2.get("ENABLE_NETWORK_POLICY")) or (e2.get("NETWORK_POLICY_ENFORCING_MODE") and e2.get("NETWORK_POLICY_ENFORCING_MODE") != "disabled"):
            engines.append("Amazon VPC CNI network policy agent")
    for name, label in (("calico-node", "Calico"), ("cilium", "Cilium"), ("antrea-agent", "Antrea"), ("kube-router", "kube-router"), ("weave-net", "Weave Net")):
        if _ds_find(ctx, name):
            engines.append(label)
    open_ns = [n for n in pod_ns if not per_ns[n]["n"] and n != "kube-system"]
    if pols and not engines:
        status = ST_WARN
        why = f"{len(pols)} NetworkPolicy object(s) exist in {len(per_ns)} namespace(s) but no policy engine was found, so they are probably NOT enforced."
        advice = "Turn on enforcement (for the Amazon VPC CNI: enableNetworkPolicy in the vpc-cni add-on configuration) or install a policy engine such as Calico or Cilium."
    elif not pols:
        status = ST_OK
        why = "no NetworkPolicy exists: nothing in the cluster restricts pod-to-pod traffic." + (f" Policy engine found: {', '.join(engines)}." if engines else "")
        advice = "Nothing blocks traffic, which is fine for troubleshooting; for security consider a default-deny policy per application namespace."
    else:
        status = ST_OK
        why = f"{len(pols)} NetworkPolicy object(s) in {len(per_ns)} namespace(s); enforced by: {', '.join(engines)}; {len(open_ns)} namespace(s) with pods have none."
        advice = "If traffic between two pods is refused, read the policies of both namespaces in the table: a default-deny policy needs an explicit allow rule."
    _net_check(rep, ctx, "policy", title, status, why, advice, what, terms=terms)
    if open_ns:
        ctx.find("INFO", f"{len(open_ns)} namespace(s) with pods have no NetworkPolicy (all pod-to-pod traffic allowed unless a mesh/CNI restricts it): "
                         + ", ".join(sorted(open_ns)[:8]) + (" ..." if len(open_ns) > 8 else ""))
    prow = [[n, support_of(ctx, n) or "-", pod_ns[n], per_ns[n]["n"],
             ("ingress " if per_ns[n]["deny_in"] else "") + ("egress" if per_ns[n]["deny_out"] else "") or ("none" if not per_ns[n]["n"] else "no")]
            for n in sorted(set(pod_ns) | set(per_ns))]
    rep.table(["Namespace", "Support team contact", "Pods", "Network policies", "Default-deny policy present"], prow,
              what="for each namespace: how many pods run, how many NetworkPolicies exist, and whether one of them denies all incoming or outgoing traffic by default.")


def _acl_allows(entries, egress, proto, port, ip):
    """First-match evaluation of a network ACL for one probe packet: True = allowed."""
    for e in sorted((x for x in entries if bool(x.get("Egress")) == egress), key=lambda x: x.get("RuleNumber", 32767)):
        p = str(e.get("Protocol"))
        if p not in ("-1", proto):
            continue
        pr = e.get("PortRange")
        if p != "-1" and pr and not (pr.get("From", 0) <= port <= pr.get("To", 65535)):
            continue
        cidr = e.get("CidrBlock")
        try:
            if cidr and ipaddress.ip_address(ip) not in ipaddress.ip_network(cidr, strict=False):
                continue
        except ValueError:
            continue
        return e.get("RuleAction") == "allow"
    return False


def _chk_firewalls(rep, ctx):
    title = "Cloud firewalls: security groups and network access control lists"
    what = "the AWS firewall rules around the nodes: the security group rules and the subnet-level network access control lists, and whether they block normal cluster traffic."
    terms = ("Security group", "Network ACL", "CIDR", "NodePort")
    target, cluster = ctx.data.get("aws_target"), ctx.data.get("aws_cluster")
    if not (AWS_OPTS["enabled"] and target and cluster):
        _net_check(rep, ctx, "firewall", title, ST_NA, "the AWS section is off or could not describe the cluster, so the security groups and network ACLs are unknown.",
                   "Run again with AWS details on, or look at the security groups and network ACLs of the cluster subnets in the EC2 console.", what, terms=terms)
        return
    sgs = ctx.data.get("aws_sgs") or []
    subnets = ctx.data.get("aws_subnets") or []
    subnet_ids = [s["SubnetId"] for s in subnets] or ((cluster.get("resourcesVpcConfig") or {}).get("subnetIds") or [])
    acls, err = (None, "no subnets known")
    if subnet_ids:
        acls, err = aws_cli(["ec2", "describe-network-acls", "--filters", "Name=association.subnet-id,Values=" + ",".join(subnet_ids[:50])], target)
    probe_ip = "10.0.0.5"
    for sn in subnets:
        try:
            probe_ip = str(next(ipaddress.ip_network(sn["CidrBlock"]).hosts()))
            break
        except (KeyError, ValueError, StopIteration):
            pass
    acl_rows, blocked, denies, acl_list = [], [], [], []
    if not err and isinstance(acls, dict):
        for acl in acls.get("NetworkAcls", []):
            acl_list.append(acl)
            entries = acl.get("Entries", [])
            subs = ", ".join(a["SubnetId"] for a in acl.get("Associations", []) if a.get("SubnetId"))
            for e in sorted(entries, key=lambda x: (bool(x.get("Egress")), x.get("RuleNumber", 0))):
                pr = e.get("PortRange")
                proto = {"-1": "all", "6": "TCP", "17": "UDP", "1": "ICMP"}.get(str(e.get("Protocol")), str(e.get("Protocol")))
                acl_rows.append([acl["NetworkAclId"] + (" (default)" if acl.get("IsDefault") else ""), subs[:40], "outbound" if e.get("Egress") else "inbound",
                                 "default (last)" if e.get("RuleNumber") == 32767 else e.get("RuleNumber"), e.get("RuleAction"), proto,
                                 f"{pr.get('From')}-{pr.get('To')}" if pr else "all", e.get("CidrBlock") or e.get("Ipv6CidrBlock") or "-"])
                if e.get("RuleAction") == "deny" and e.get("RuleNumber") != 32767:
                    denies.append(f"{acl['NetworkAclId']} rule {e.get('RuleNumber')}")
            for egress in (False, True):
                for proto, port, name in (("6", 443, "HTTPS 443"), ("6", 32768, "return traffic on a high port"), ("17", 53, "DNS 53")):
                    if not _acl_allows(entries, egress, proto, port, probe_ip):
                        blocked.append(f"{acl['NetworkAclId']} blocks {'outbound' if egress else 'inbound'} {name}")
    parts = []
    why = []
    if not err:
        if blocked:
            parts.append(ST_BAD)
            why.append(f"{len(acl_list)} network ACL(s): " + blocked[0] + (f" (+{len(blocked) - 1} more)" if len(blocked) > 1 else ""))
        elif denies:
            parts.append(ST_WARN)
            why.append(f"{len(acl_list)} network ACL(s) with explicit deny rules ({', '.join(denies[:3])}); the probes for HTTPS, DNS and return traffic still pass")
        else:
            parts.append(ST_OK)
            why.append(f"{len(acl_list)} network ACL(s) on the cluster subnets allow HTTPS, DNS and return traffic")
    else:
        why.append(f"the network ACLs could not be read ({_first_line(err, 80)})")
    if sgs:
        why.append(f"{len(sgs)} security group(s) read (rules below; their checks are also in the AWS section)")
        parts.append(ST_OK)
    else:
        why.append("security group rules were not available")
    status = _worst(parts, ST_NA)
    if err and sgs and status == ST_OK:
        status = ST_NA
    advice = {ST_OK: "Nothing to do.", ST_NA: "Read the missing firewall data yourself (ec2 describe-network-acls) - 'Not available' means it could not be checked, not that it is fine.",
              ST_WARN: "Make sure the deny rules do not cover node, pod or load balancer traffic; network ACLs are checked in order of rule number.",
              ST_BAD: "A network ACL that blocks return traffic or DNS breaks connections between nodes and the internet: add allow rules for ephemeral ports 1024-65535 in both directions."}[status]
    _net_check(rep, ctx, "firewall", title, status, "; ".join(why) + ".", advice, what, terms=terms,
               find=(f"Network ACL problem: {blocked[0]}" if status == ST_BAD and blocked else None))
    if sgs:
        rows = []
        for sg in sgs:
            for direction, perms in (("inbound", sg.get("IpPermissions", [])), ("outbound", sg.get("IpPermissionsEgress", []))):
                for perm in perms:
                    proto, ports, src = _fmt_perm(perm, sg["GroupId"])
                    rows.append([sg["GroupId"], sg.get("GroupName", ""), direction, proto, ports, ", ".join(x for x in src if x) or "-"])
        rep.table(["Security group", "Name", "Direction", "Protocol", "Ports", "Source or destination"], rows, limit=40, maxw=60,
                  what="the rules of the cluster security group and the additional security groups: which traffic is allowed in and out.")
    if acl_rows:
        rep.table(["Network access control list", "Subnets", "Direction", "Rule number", "Action", "Protocol", "Ports", "Address range"], acl_rows, limit=60, maxw=50,
                  what="the network access control list rules of the cluster subnets, in the order they are checked (lowest rule number first).")


# ---------------------------------------------------------------------------
# Network section, part 7: load balancers and ingress
# ---------------------------------------------------------------------------

def _der_item(b, i):
    tag = b[i]
    n = b[i + 1]
    j = i + 2
    if n & 0x80:
        k = n & 0x7F
        n = int.from_bytes(b[j:j + k], "big")
        j += k
    return tag, j, j + n


def cert_not_after(pem):
    """Expiry date (UTC datetime) of the first certificate in a PEM / DER blob, read with a small built-in parser; None if unreadable."""
    import base64
    try:
        text = pem.decode("ascii", "ignore") if isinstance(pem, bytes) else pem
        m = re.search(r"-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----", text, re.S)
        der = base64.b64decode(m.group(1)) if m else (pem if isinstance(pem, bytes) else b"")
        _t, s, _e = _der_item(der, 0)           # Certificate
        _t, s, _e = _der_item(der, s)           # TBSCertificate
        i = s
        t, a, b = _der_item(der, i)
        if t == 0xA0:                           # optional version [0]
            i = b
        for _ in range(3):                      # serialNumber, signature, issuer
            _t, a, b = _der_item(der, i)
            i = b
        _t, vs, _ve = _der_item(der, i)         # validity SEQUENCE
        t1, a1, b1 = _der_item(der, vs)         # notBefore
        t2, a2, b2 = _der_item(der, b1)         # notAfter
        raw = der[a2:b2].decode("ascii")
        fmt = "%y%m%d%H%M%SZ" if t2 == 0x17 else "%Y%m%d%H%M%SZ"
        return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
    except Exception:
        return None


ING_NAMES = ("ingress-nginx", "ingress", "aws-load-balancer-controller", "traefik", "haproxy", "contour", "kong", "emissary", "ambassador", "istio-ingressgateway")


def _lb_collect(ctx, target, vpc_id):
    """Cloud load balancers of the VPC with their target health (read-only). No printing."""
    res = {"found": [], "rows": [], "err": None, "healthy": 0, "unhealthy": 0, "bad_names": []}
    v2, err = aws_cli(["elbv2", "describe-load-balancers"], target, timeout=90)
    if err:
        res["err"] = err
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
                res["bad_names"].append(lb["LoadBalancerName"])
            res["healthy"] += healthy
            res["unhealthy"] += unhealthy
            res["rows"].append([lb["LoadBalancerName"], lb.get("Type"), lb.get("Scheme"), (lb.get("State") or {}).get("Code"), healthy, unhealthy, lb.get("DNSName", "")[:50], note])
            res["found"].append(("alb" if lb.get("Type") == "application" else "nlb", suffix, lb["LoadBalancerName"], lb.get("DNSName", "")))
    classic, cerr = aws_cli(["elb", "describe-load-balancers"], target, timeout=90)
    if not cerr:
        for lb in [x for x in classic.get("LoadBalancerDescriptions", []) if x.get("VPCId") == vpc_id][:12]:
            ih, herr = aws_cli(["elb", "describe-instance-health", "--load-balancer-name", lb["LoadBalancerName"]], target)
            states = Counter(s.get("State") for s in ([] if herr else ih.get("InstanceStates", [])))
            note = ""
            if states.get("OutOfService"):
                note = f"{states['OutOfService']} OUT OF SERVICE"
                ctx.find("MED", f"Classic load balancer {lb['LoadBalancerName']}: {states['OutOfService']} instance(s) OutOfService")
                res["bad_names"].append(lb["LoadBalancerName"])
            res["healthy"] += states.get("InService", 0)
            res["unhealthy"] += states.get("OutOfService", 0)
            res["rows"].append([lb["LoadBalancerName"], "classic", lb.get("Scheme"), "active", states.get("InService", 0), states.get("OutOfService", 0), lb.get("DNSName", "")[:50], note])
            res["found"].append(("classic", lb["LoadBalancerName"], lb["LoadBalancerName"], lb.get("DNSName", "")))
    if err and cerr:
        res["err"] = err
    elif err and not res["rows"]:
        res["err"] = err
    return res


def _chk_ingress_lb(rep, ctx):
    title = "Load balancers and ingress: controllers, backends, certificates and cloud health checks"
    what = "whether the ingress controller pods are healthy and free of 502/503/504 errors, whether each ingress points to a Service with ready pods, when its TLS certificates expire, and what the cloud load balancers' health checks say."
    terms = ("Ingress", "ALB", "NLB", "TLS", "Endpoints", "LoadBalancer")
    ings = items(ctx.data.get("ingresses"))
    pods = items(ctx.data.get("pods"))
    ctrl = []
    for p in pods:
        meta = p["metadata"]
        lab = meta.get("labels") or {}
        nm = (lab.get("app.kubernetes.io/name") or lab.get("app") or meta["name"]).lower()
        if "admission" in meta["name"] or p.get("status", {}).get("phase") == "Succeeded":
            continue
        if any(x in nm for x in ING_NAMES) or any(x in meta["name"].lower() for x in ("ingress-nginx", "load-balancer-controller", "traefik", "ingress-controller")):
            ctrl.append(p)
    # ---- logs of up to 4 controller pods
    log_rows, five, errs, unreadable = [], 0, 0, 0
    for p in ctrl[:4]:
        hint_k(ctx, ["logs", "-n", p["metadata"]["namespace"], p["metadata"]["name"], "--all-containers", f"--since={ctx.minutes}m", "--tail=800"], 60)
    for p in ctrl[:4]:
        meta = p["metadata"]
        ok, out = kubectl(["logs", "-n", meta["namespace"], meta["name"], "--all-containers", f"--since={ctx.minutes}m", "--tail=800"], timeout=60)
        if not ok:
            unreadable += 1
            log_rows.append([f"{meta['namespace']}/{meta['name']}", "-", "-", "-", "-", "-", "unreadable: " + (out.splitlines()[0][:60] if out else "?")])
            continue
        lines = out.splitlines()
        codes = Counter((m.group(1) or m.group(2)) for l in lines for m in [re.search(r'HTTP/\d(?:\.\d)?"\s+(50[234])\b|\b(?:status|code)[=: ]+(50[234])\b', l)] if m)
        n_err = sum(1 for l in lines if re.search(r"no live upstreams|upstream timed out|connect\(\) failed|\berror\b|\"level\":\"error\"|Failed", l, re.I))
        last = next((l for l in reversed(lines) if re.search(r"no live upstreams|upstream timed out|connect\(\) failed|error|failed", l, re.I)), "")
        five += sum(codes.values())
        errs += n_err
        log_rows.append([f"{meta['namespace']}/{meta['name']}", len(lines), codes.get("502", 0), codes.get("503", 0), codes.get("504", 0), n_err, last[:110] or "-"])
    # ---- backends of every ingress
    endpoints = {(e["metadata"]["namespace"], e["metadata"]["name"]): e for e in items(ctx.data.get("endpoints"))}
    services = {(s["metadata"]["namespace"], s["metadata"]["name"]) for s in items(ctx.data.get("services"))}
    brow, bad_back, no_ready = [], 0, 0
    tls_refs = []
    for i in ings:
        meta, spec = i["metadata"], i.get("spec", {})
        specs = []
        for r in spec.get("rules", []) or []:
            for path in (r.get("http") or {}).get("paths", []):
                bk = path.get("backend") or {}
                svc = (bk.get("service") or {}).get("name") or bk.get("serviceName")
                specs.append((r.get("host") or "*", path.get("path") or "/", svc))
        db = spec.get("defaultBackend") or {}
        if (db.get("service") or {}).get("name") or db.get("serviceName"):
            specs.append(("*", "(default)", (db.get("service") or {}).get("name") or db.get("serviceName")))
        for host, path, svc in specs:
            if not svc or svc == "use-annotation":
                continue
            key = (meta["namespace"], svc)
            if key not in services:
                state, bad_back = "SERVICE MISSING", bad_back + 1
            else:
                n = sum(len(sub.get("addresses") or []) for sub in ((endpoints.get(key) or {}).get("subsets") or []))
                state = f"{n} ready endpoint(s)"
                if n == 0:
                    state, no_ready = "NO READY ENDPOINTS", no_ready + 1
            brow.append([meta["namespace"], meta["name"], host, path, svc, state])
        for t in spec.get("tls", []) or []:
            if t.get("secretName"):
                tls_refs.append((meta["namespace"], meta["name"], t["secretName"], ", ".join(t.get("hosts") or []) or "-"))
    # ---- certificates
    crow, cert_bad, cert_warn, cert_na = [], 0, 0, 0
    seen = set()
    for ns, ing, secret, hosts in tls_refs[:30]:
        hint_k(ctx, ["get", "secret", secret, "-n", ns, "-o", "json"])
    for ns, ing, secret, hosts in tls_refs[:30]:
        if (ns, secret) in seen:
            continue
        seen.add((ns, secret))
        data, err = kjson(["get", "secret", secret, "-n", ns])
        if err or not data:
            if err and re.search(r"forbidden", err, re.I):
                cert_na += 1
                crow.append([ns, ing, secret, hosts[:40], "-", "-", "not readable: no permission to read Secrets"])
            else:
                cert_bad += 1
                crow.append([ns, ing, secret, hosts[:40], "-", "-", "SECRET NOT FOUND - the ingress references a missing certificate"])
            continue
        import base64
        crt = (data.get("data") or {}).get("tls.crt")
        try:
            raw = base64.b64decode(crt) if crt else b""
        except Exception:
            raw = b""
        exp = cert_not_after(raw) if raw else None
        if not exp:
            cert_na += 1
            crow.append([ns, ing, secret, hosts[:40], "-", "-", "certificate not readable (no tls.crt or unknown format)"])
            continue
        days = (exp - ctx.now).days
        res = "EXPIRED" if days < 0 else ("expires within 14 days" if days < 14 else ("expires within 30 days" if days < 30 else "OK"))
        cert_bad += res == "EXPIRED"
        cert_warn += res.startswith("expires")
        crow.append([ns, ing, secret, hosts[:40], exp.strftime("%Y-%m-%d"), days, res])
    # ---- cloud side
    target, cluster = ctx.data.get("aws_target"), ctx.data.get("aws_cluster")
    vpc_id = ((cluster or {}).get("resourcesVpcConfig") or {}).get("vpcId")
    lbres = None
    if AWS_OPTS["enabled"] and target and vpc_id:
        lbres = _lb_collect(ctx, target, vpc_id)
        ctx.data["net_lbs"] = lbres["found"]
    # ---- verdict
    bad, warn, na = [], [], []
    if ctrl:
        notready = [p for p in ctrl if not _pod_ready(p)]
        if notready:
            bad.append(f"{len(notready)} of {len(ctrl)} ingress controller pod(s) not ready")
        if five:
            warn.append(f"{five} HTTP 502/503/504 response(s) in the controller logs")
        if errs and not five:
            warn.append(f"{errs} error line(s) in the controller logs")
        if unreadable:
            na.append(f"logs of {unreadable} controller pod(s) unreadable")
    elif ings:
        bad.append("Ingress objects exist but no ingress controller pod was found")
    if bad_back:
        bad.append(f"{bad_back} ingress backend(s) point to a missing Service")
    if no_ready:
        warn.append(f"{no_ready} ingress backend(s) have no ready endpoint")
    if cert_bad:
        bad.append(f"{cert_bad} TLS certificate problem(s) (expired or missing)")
    if cert_warn:
        warn.append(f"{cert_warn} TLS certificate(s) expire within 30 days")
    if cert_na:
        na.append(f"{cert_na} certificate(s) not readable")
    if lbres is not None:
        if lbres["err"] and not lbres["rows"]:
            na.append(f"cloud load balancer health unavailable ({_first_line(lbres['err'], 60)})")
        elif lbres["unhealthy"]:
            (bad if not lbres["healthy"] else warn).append(f"{lbres['unhealthy']} unhealthy cloud load balancer target(s) in {', '.join(lbres['bad_names'][:3])}")
    else:
        na.append("cloud load balancer health needs the AWS section")
    if not ctrl and not ings and not (lbres and lbres["rows"]):
        status = ST_NA
        why = "no ingress controller, Ingress object or cloud load balancer was found" + (" (" + "; ".join(na) + ")" if na else "") + "."
        advice = "Nothing to check here if this cluster is not exposed through load balancers."
    else:
        status = ST_BAD if bad else (ST_WARN if warn else (ST_NA if na else ST_OK))
        parts = [f"{len(ctrl)} ingress controller pod(s)", f"{len(ings)} Ingress object(s)", f"{len(brow)} backend(s) checked", f"{len(crow)} certificate(s) checked"]
        if lbres is not None and lbres["rows"]:
            parts.append(f"{len(lbres['rows'])} cloud load balancer(s) with {lbres['unhealthy']} unhealthy target(s)")
        why = "; ".join(parts) + ". " + "; ".join(bad + warn + na) + ("" if (bad or warn or na) else "No problem found.")
        advice = {ST_OK: "Nothing to do.", ST_NA: "Part of the data could not be read (see above); read it yourself before assuming the path is healthy.",
                  ST_WARN: "Read the tables below: a 503 usually means no ready backend pod, a 504 a slow backend, a 502 a backend that closed the connection; renew certificates before they expire.",
                  ST_BAD: "Fix the first problem listed above: an ingress without controller, backend Service, valid certificate or healthy targets cannot serve traffic."}[status]
    _net_check(rep, ctx, "lb", title, status, why, advice, what, terms=terms,
               find=(f"Ingress / load balancer problem: {(bad or warn)[0]}" if status in (ST_WARN, ST_BAD) and (bad or warn) else None))
    ing_rows = []
    for i in ings:
        meta, spec = i["metadata"], i.get("spec", {})
        lb = (i.get("status", {}).get("loadBalancer") or {}).get("ingress") or []
        address = (lb[0].get("hostname") or lb[0].get("ip")) if lb else ""
        hosts = sorted({r.get("host") or "*" for r in spec.get("rules", []) or []})
        paths = sum(len((r.get("http") or {}).get("paths", [])) for r in spec.get("rules", []) or [])
        klass = spec.get("ingressClassName") or (meta.get("annotations") or {}).get("kubernetes.io/ingress.class") or "-"
        ing_rows.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], klass, ", ".join(hosts)[:60], address[:60] or "NO ADDRESS",
                         "yes" if spec.get("tls") else "no", paths])
        if not address:
            ctx.find("MED", f"Ingress {meta['namespace']}/{meta['name']} has no load balancer address" + support_suffix(ctx, [meta["namespace"]]))
            ctx.ns_issue(meta["namespace"], f"ingress {meta['name']} has no address")
    if ing_rows:
        rep.table(["Namespace", "Support team contact", "Ingress", "Class", "Hosts", "Address", "TLS", "Paths"], ing_rows, maxw=64,
                  what="every Ingress object: which hosts it serves, the load balancer address it received, whether it uses TLS and how many paths it routes.")
    if ctrl:
        rep.table(["Controller pod", "Namespace", "State", "Restarts", "Node"],
                  [[p["metadata"]["name"], p["metadata"]["namespace"], "ready" if _pod_ready(p) else "NOT READY", _pod_restarts(p), node_tag(ctx, p.get("spec", {}).get("nodeName"))] for p in ctrl[:20]],
                  maxw=60, what="the ingress controller pods (the programs that turn Ingress objects into load balancer rules): state, restarts and node.")
    if log_rows:
        rep.table(["Controller pod", "Log lines", "HTTP 502 responses", "HTTP 503 responses", "HTTP 504 responses", "Error lines", "Latest error"], log_rows, maxw=70,
                  what="what the ingress controllers logged in the window: server error responses (502 bad gateway, 503 unavailable, 504 gateway timeout) and error lines.")
    if brow:
        rep.table(["Namespace", "Ingress", "Host", "Path", "Backend Service", "Backend state"], brow, maxw=60,
                  what="where each ingress rule sends its traffic and whether that Service has ready pods behind it.")
    if crow:
        rep.table(["Namespace", "Ingress", "Secret", "Hosts", "Expires on", "Days left", "Result"], crow, maxw=60,
                  what="the TLS certificate each ingress uses (read from the Kubernetes Secret) and when it expires; only the public certificate is read, never the private key.")
    if lbres is not None and lbres["rows"]:
        rep.table(["Load balancer", "Type", "Scheme", "State", "Healthy targets", "Unhealthy targets", "Domain name", "Note"], lbres["rows"], maxw=56,
                  what="the cloud load balancers in this VPC and the health-check result of their targets (the nodes or pods behind them).")


def _net_warning_events(rep, ctx):
    title = "Network-related warning events"
    what = "Kubernetes warning events in the window that mention networking (addresses, sandboxes, DNS, routes, load balancers, security groups)."
    events = []
    for e in items(ctx.data.get("events")):
        t = _event_time(e)
        text = f"{e.get('reason', '')} {e.get('message') or e.get('note') or ''}"
        if e.get("type") == "Warning" and t and t >= ctx.since and NET_EVENT_PATTERN.search(text):
            events.append((t, e))
    if not events:
        _net_check(rep, ctx, "pods", title, ST_OK, f"no network-related Warning event in the last {ctx.minutes} minutes.", "Nothing to do.", what)
        return
    rows = []
    for t, e in sorted(events, key=lambda x: x[0], reverse=True)[:30]:
        obj = e.get("involvedObject") or e.get("regarding") or {}
        name = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else f"{(obj.get('namespace') + '/') if obj.get('namespace') else ''}{obj.get('name', '?')}"
        rows.append([age(t, ctx.now) + " ago", e.get("reason", "?"), f"{obj.get('kind', '?')} {name}", support_of(ctx, obj.get("namespace")) or "-",
                     (e.get("series") or {}).get("count") or e.get("count") or 1, (e.get("message") or e.get("note") or "").replace("\n", " ")[:130]])
        ctx.ns_issue(obj.get("namespace"), f"network warning: {e.get('reason', '?')}")
    _net_check(rep, ctx, "pods", title, ST_WARN, f"{len(events)} network-related Warning event(s) in the last {ctx.minutes} minutes (newest: {rows[0][1]}).",
               "Read the newest messages below: they usually name the pod, node or address that failed.", what,
               find=f"{len(events)} network-related Warning event(s) in the window (e.g. {rows[0][1]})")
    rep.table(["When", "Reason", "Object", "Support team contact", "Count", "Message"], rows, maxw=70,
              what="the newest network-related warning events with the object they concern and the message Kubernetes recorded.")


# ---------------------------------------------------------------------------
# Network section, part 9: connection tracking and port exhaustion
# ---------------------------------------------------------------------------

def _exporter_args(p):
    """The kubectl call that reads the metrics of one node-exporter pod through the API server proxy."""
    meta = p["metadata"]
    port = 9100
    for c in p.get("spec", {}).get("containers", []):
        for cp in c.get("ports", []) or []:
            if cp.get("name") in ("metrics", "http-metrics") or cp.get("containerPort") == 9100:
                port = cp.get("containerPort", port)
    return ["get", "--raw", f"/api/v1/namespaces/{meta['namespace']}/pods/{meta['name']}:{port}/proxy/metrics"]


def _chk_conntrack(rep, ctx, env):
    title = "Connection tracking (conntrack) and port exhaustion"
    what = "whether the nodes' connection tracking tables are filling up, and whether the NAT gateways ran out of ports (both make new connections fail while old ones keep working)."
    terms = ("conntrack", "NAT", "SNAT", "kubelet")
    exporters = []
    for p in items(ctx.data.get("pods")):
        lab = p["metadata"].get("labels") or {}
        nm = (lab.get("app.kubernetes.io/name") or lab.get("app") or p["metadata"]["name"]).lower()
        if "node-exporter" in nm and p.get("status", {}).get("phase") == "Running":
            exporters.append(p)
    rows, worst_pct, unreadable = [], None, 0
    for p in exporters[:8]:
        hint_k(ctx, _exporter_args(p), 60)
    for p in exporters[:8]:
        ok, out = kubectl(_exporter_args(p), timeout=60)
        if not ok:
            unreadable += 1
            continue
        e_m = re.search(r"^node_nf_conntrack_entries\s+([0-9.eE+]+)", out, re.M)
        l_m = re.search(r"^node_nf_conntrack_entries_limit\s+([0-9.eE+]+)", out, re.M)
        if e_m and l_m and float(l_m.group(1)) > 0:
            entries, limit = float(e_m.group(1)), float(l_m.group(1))
            pct = 100.0 * entries / limit
            worst_pct = pct if worst_pct is None else max(worst_pct, pct)
            rows.append([node_tag(ctx, p.get("spec", {}).get("nodeName")), f"{entries:.0f}", f"{limit:.0f}", f"{pct:.1f}%"])
    parts, why, advice = [], [], []
    if rows:
        cst = ST_BAD if worst_pct >= 90 else (ST_WARN if worst_pct >= 70 else ST_OK)
        parts.append(cst)
        why.append(f"connection tracking is at most {worst_pct:.0f}% full on {len(rows)} node(s)")
        if cst != ST_OK:
            advice.append("raise net.netfilter.nf_conntrack_max (node tuning) or reduce short-lived connections")
    else:
        why.append("connection tracking usage is not available: " + ("node-exporter pods exist but their metrics could not be read through the API proxy" if exporters
                   else "no node-exporter pods were found, so it needs node-exporter or node access (this tool never connects to nodes)"))
    nat = ctx.data.get("net_nat")
    if nat:
        nst = ST_BAD if nat["port_errors"] > 0 else (ST_WARN if nat["drops"] > 0 else ST_OK)
        parts.append(nst)
        why.append(f"{nat['gateways']} NAT gateway(s): {nat['port_errors']:.0f} port allocation error(s), {nat['drops']:.0f} dropped packet(s) in the window")
        if nst == ST_BAD:
            advice.append("NAT port exhaustion: add NAT gateways (one per subnet/zone), use VPC endpoints for AWS services, or reduce parallel connections to one destination")
    else:
        why.append("NAT port exhaustion needs the AWS section and Amazon CloudWatch access (not available here)")
    snat = (env or {}).get("AWS_VPC_K8S_CNI_EXTERNALSNAT")
    if env is not None:
        why.append(f"VPC CNI SNAT setting: AWS_VPC_K8S_CNI_EXTERNALSNAT={snat if snat is not None else 'false (default: pod traffic leaving the VPC shares the node address)'}")
    status = _worst(parts, ST_NA)
    _net_check(rep, ctx, None, title, status, "; ".join(why) + ".",
               ("; ".join(advice) + ".").capitalize() if advice else
               ("Nothing to do." if status == ST_OK else "Install node-exporter (metric node_nf_conntrack_entries) or check 'conntrack -S' on a node you may use."), what, terms=terms,
               find=(f"Node connection tracking table up to {worst_pct:.0f}% full - new connections may be dropped" if rows and worst_pct >= 70 else None))
    if rows:
        rep.table(["Node", "Tracked connections", "Table limit", "Percent used"], rows, maxw=40,
                  what="how many connections each node tracks compared with the size of its connection tracking table (read from node-exporter through the Kubernetes API proxy).")


# ---------------------------------------------------------------------------
# Network section, part 10: observability and packet capture
# ---------------------------------------------------------------------------

def _chk_observability(rep, ctx):
    title = "Network observability on this cluster"
    what = "which AWS tools that record network traffic are switched on: Container Insights, Network Flow Monitor and VPC Flow Logs."
    terms = ("Container Insights", "Network Flow Monitor", "Flow Logs", "VPC")
    target, cluster = ctx.data.get("aws_target"), ctx.data.get("aws_cluster")
    aws_on = bool(AWS_OPTS["enabled"] and target and cluster)
    addons = ctx.data.get("aws_addons")
    vpc_id = ((cluster or {}).get("resourcesVpcConfig") or {}).get("vpcId")
    feats = []
    ci_ds = _ds_find(ctx, "cloudwatch-agent")
    ci_on = bool(ci_ds) or bool(addons and "amazon-cloudwatch-observability" in addons)
    feats.append(["Container Insights (metrics and logs of the cluster)", "Enabled" if ci_on else "Not found",
                  ("add-on amazon-cloudwatch-observability or the cloudwatch-agent DaemonSet" if ci_on else "no CloudWatch agent DaemonSet or add-on"),
                  "EKS add-on amazon-cloudwatch-observability (changes your account: you run it)"])
    nfm_ds = _ds_find(ctx, "network-flow-monitor")
    nfm_on = bool(nfm_ds) or bool(addons and any("network-flow-monitor" in a for a in addons))
    feats.append(["Container Network Observability (CloudWatch Network Flow Monitor)", "Enabled" if nfm_on else "Not found",
                  "agent add-on / DaemonSet present" if nfm_on else "no Network Flow Monitor agent add-on or DaemonSet",
                  "EKS add-on aws-network-flow-monitor-agent, then create a monitor in CloudWatch (changes your account: you run it)"])
    flow_on, flow_err, flow_n = False, None, 0
    if aws_on and vpc_id:
        hint_a(ctx, ["ec2", "describe-flow-logs", "--filter", f"Name=resource-id,Values={vpc_id}"], target)
    if aws_on:
        hint_a(ctx, ["ec2", "describe-traffic-mirror-sessions"], target)
    if aws_on and vpc_id:
        data, flow_err = aws_cli(["ec2", "describe-flow-logs", "--filter", f"Name=resource-id,Values={vpc_id}"], target)
        if not flow_err and isinstance(data, dict):
            logs = [f for f in data.get("FlowLogs", []) if f.get("FlowLogStatus", "ACTIVE") == "ACTIVE"]
            flow_n, flow_on = len(logs), bool(logs)
    feats.append(["VPC Flow Logs (accepted / rejected traffic of the VPC)", "Enabled" if flow_on else ("Unknown" if (flow_err or not aws_on) else "Not found"),
                  (f"{flow_n} active flow log(s) on {vpc_id}" if flow_on else (f"could not be read: {_first_line(flow_err, 70)}" if flow_err else ("needs the AWS section" if not aws_on else f"no active flow log on {vpc_id}"))),
                  "aws ec2 create-flow-logs for the VPC (changes your account: you run it)"])
    enabled = [f[0] for f in feats if f[1] == "Enabled"]
    unknown = [f[0] for f in feats if f[1] == "Unknown"]
    if enabled:
        status = ST_OK
        why = f"{len(enabled)} of 3 network observability feature(s) enabled; " + ("not determinable: " + "; ".join(u.split(" (")[0] for u in unknown) if unknown else "all others confirmed absent") + "."
        advice = "Use them to see which pods or addresses talk to each other and which traffic is rejected."
    elif aws_on and not unknown:
        status = ST_WARN
        why = "none of Container Insights, Network Flow Monitor or VPC Flow Logs is enabled."
        advice = "Without them, past network problems cannot be investigated afterwards: enable at least VPC Flow Logs and Container Insights."
    else:
        status = ST_NA
        why = "the AWS section or its permissions were not available, so the observability features could not be determined."
        advice = "Check the CloudWatch console and the VPC's Flow Logs tab yourself."
    _net_check(rep, ctx, "observe", title, status, why, advice, what, terms=terms,
               find=("No network observability (Container Insights, Network Flow Monitor, VPC Flow Logs) is enabled" if status == ST_WARN else None), sev="INFO")
    rep.table(["Feature", "State", "Evidence", "How to switch it on"], feats, maxw=80,
              what="each AWS network observability feature, whether it is switched on for this cluster, and how to enable it (text only: this tool never changes anything).")

    # ---- packet capture: guidance only
    mirror_n, mirror_err = None, None
    if aws_on:
        mdata, mirror_err = aws_cli(["ec2", "describe-traffic-mirror-sessions"], target)
        if not mirror_err and isinstance(mdata, dict):
            mirror_n = len(mdata.get("TrafficMirrorSessions", []))
    if mirror_n:
        status, why = ST_OK, f"{mirror_n} VPC Traffic Mirroring session(s) exist, so packets can be captured by a collector."
    else:
        status = ST_NA
        why = ("no VPC Traffic Mirroring session exists" if mirror_n == 0 else "Traffic Mirroring could not be checked" + (f" ({_first_line(mirror_err, 60)})" if mirror_err else "")) \
              + "; this tool is read-only and never captures packets, so no capture was taken."
    _net_check(rep, ctx, "capture", "Packet capture options (guidance only)", status, why,
               "If counters and logs do not explain a problem, capture packets yourself with one of the tools below, with the approval of the cluster owner.",
               "which ways exist to capture packets for a deeper investigation. Nothing in this table is run by this tool.", terms=("VPC", "kubelet"))
    rep.table(["Tool or command", "What it gives you", "Changes anything?", "Needs"],
              [["kubectl get --raw /api/v1/nodes/<node>/proxy/stats/summary", "read-only byte and error counters per node and pod (already used above)", "no", "permission nodes/proxy"],
               ["aws ec2 describe-flow-logs / CloudWatch Logs Insights on the flow log group", "which source and destination addresses were accepted or rejected", "no (to read)", "VPC Flow Logs enabled"],
               ["aws ec2 describe-traffic-mirror-sessions", "shows whether VPC Traffic Mirroring already copies packets to a collector", "no", "AWS read access"],
               ["VPC Traffic Mirroring to a packet analyser", "full packets from a network interface, without touching the node", "yes: you create the session", "AWS write access, a collector"],
               ["tcpdump on a node (through Systems Manager Session Manager)", "full packets on the node's interfaces", "no change to the cluster, but runs a command on the node", "node access (this tool never does it)"],
               ["kubectl debug node/<node> with a network tools image", "a temporary pod on the node to run tcpdump or ping", "yes: creates a temporary pod", "permission to create pods"]],
              maxw=70, what="the usual ways to capture or inspect packets, whether each changes anything, and what it needs. Guidance only: this tool runs none of them.")


# ---------------------------------------------------------------------------
# Network section, part 11: control plane / API server
# ---------------------------------------------------------------------------

_CP_TERMS = ("API server", "Webhook", "etcd")


def _chk_apiserver_throttling(rep, ctx):
    terms = _CP_TERMS
    title = "API server request throttling"
    what = "whether the Kubernetes API server rejected requests because it was overloaded (HTTP 429 and priority-and-fairness rejections); nodes and controllers then slow down."
    ok, out = kubectl(["get", "--raw", "/metrics"], timeout=90)
    if not ok:
        _net_check(rep, ctx, None, title, ST_NA, "the API server metrics could not be read (" + (out.splitlines()[0][:90] if out else "no answer") + ").",
                   "Ask for permission to read the /metrics endpoint, or look at the 'apiserver_request_total' metric in your monitoring.", what, terms=terms)
    else:
        total = rej = n429 = 0.0
        for line in out.splitlines():
            if line.startswith("apiserver_request_total{"):
                try:
                    v = float(line.rsplit(" ", 1)[1])
                except (ValueError, IndexError):
                    continue
                total += v
                if 'code="429"' in line:
                    n429 += v
            elif line.startswith("apiserver_flowcontrol_rejected_requests_total{"):
                try:
                    rej += float(line.rsplit(" ", 1)[1])
                except (ValueError, IndexError):
                    pass
        ratio = (100.0 * n429 / total) if total else 0.0
        status = ST_BAD if ratio >= 5 else (ST_WARN if (ratio >= 0.5 or rej > 0) else ST_OK)
        if not total and not rej:
            status = ST_NA
        _net_check(rep, ctx, None, title, status,
                   (f"since the API server started: {total:.0f} requests, {n429:.0f} answered with HTTP 429 (too many requests, {ratio:.2f}%), {rej:.0f} rejected by priority and fairness."
                    if status != ST_NA else "the /metrics output did not contain the apiserver_request_total counters, so throttling cannot be judged."),
                   "Nothing to do." if status == ST_OK else "Find the client that sends too many requests (audit logs) and slow it down, or reduce the number of controllers / watchers; these are totals since start, so compare two runs to see whether it is still happening.",
                   what, terms=terms, find=(f"API server throttling: {n429:.0f} HTTP 429 responses ({ratio:.2f}% of requests) since it started" if status != ST_OK else None))


def _chk_webhooks(rep, ctx):
    terms = _CP_TERMS
    title = "Admission webhooks (failure policy, targets, recent failures)"
    what = "the webhooks the API server calls before accepting changes: if one is set to 'Fail' and its service is down or slow, changes (and pod creation) are rejected."
    configs = []
    for _kind, res in (("Validating", "validatingwebhookconfigurations"), ("Mutating", "mutatingwebhookconfigurations")):
        hint_k(ctx, ["get", res, "-o", "json"])
    for kind, res in (("Validating", "validatingwebhookconfigurations"), ("Mutating", "mutatingwebhookconfigurations")):
        data, err = kjson(["get", res])
        if err:
            configs.append((kind, None, err))
        else:
            configs.append((kind, items(data), None))
    if all(c[1] is None for c in configs):
        _net_check(rep, ctx, None, title, ST_NA, "the webhook configurations could not be read (" + _first_line(configs[0][2], 80) + ").",
                   "Ask for permission to list validatingwebhookconfigurations and mutatingwebhookconfigurations.", what, terms=terms)
    else:
        endpoints = {(e["metadata"]["namespace"], e["metadata"]["name"]): e for e in items(ctx.data.get("endpoints"))}
        services = {(s["metadata"]["namespace"], s["metadata"]["name"]) for s in items(ctx.data.get("services"))}
        rows, bad, warn = [], [], []
        for kind, cfgs, _e in configs:
            for cfg in cfgs or []:
                for wh in cfg.get("webhooks", []) or []:
                    cc = wh.get("clientConfig") or {}
                    policy = wh.get("failurePolicy") or "Fail"
                    timeout = wh.get("timeoutSeconds", 10)
                    svc = cc.get("service")
                    if svc:
                        key = (svc.get("namespace"), svc.get("name"))
                        if key not in services:
                            state = "SERVICE MISSING"
                        else:
                            n = sum(len(sub.get("addresses") or []) for sub in ((endpoints.get(key) or {}).get("subsets") or []))
                            state = f"{n} ready endpoint(s)" if n else "NO READY ENDPOINTS"
                        target = f"{key[0]}/{key[1]}"
                    else:
                        state, target = "external address", (cc.get("url") or "-")[:50]
                    if state in ("SERVICE MISSING", "NO READY ENDPOINTS"):
                        (bad if policy == "Fail" else warn).append(f"{wh.get('name')} ({policy})")
                    rows.append([kind, cfg["metadata"]["name"], wh.get("name", "-"), policy, timeout, target, state])
        fail_events = [e for e in items(ctx.data.get("events")) if (_event_time(e) or ctx.since) >= ctx.since
                       and re.search(r"failed calling webhook|webhook .* (timeout|timed out|denied)", (e.get("message") or e.get("note") or ""), re.I)]
        if bad:
            status = ST_BAD
        elif warn or fail_events:
            status = ST_WARN
        else:
            status = ST_OK
        why = (f"{len(rows)} webhook(s) listed; " + (f"{len(bad)} set to Fail with no working target ({bad[0]})" if bad else "none set to Fail with a missing target")
               + f"; {len(fail_events)} 'failed calling webhook' event(s) in the window.")
        advice = {ST_OK: "Nothing to do.", ST_WARN: "Read the events: a webhook that times out slows or blocks changes; check its service and set a short timeout or failurePolicy Ignore for non-critical ones.",
                  ST_BAD: "A webhook set to Fail whose service has no ready pod rejects every matching change: restore its pods, or temporarily set failurePolicy to Ignore."}[status]
        _net_check(rep, ctx, None, title, status, why, advice, what, terms=terms,
                   find=(f"Admission webhook problem: {bad[0]} has no working target" if bad else (f"{len(fail_events)} 'failed calling webhook' event(s) in the window" if fail_events else None)))
        if rows:
            rep.table(["Kind", "Configuration", "Webhook", "Failure policy", "Timeout (seconds)", "Target", "Target state"], rows, maxw=50,
                      what="every admission webhook with what happens when it fails, how long the API server waits, where it points and whether that target has ready pods.")


def _chk_etcd(rep, ctx):
    terms = _CP_TERMS
    title = "etcd health"
    what = "whether the cluster database answers its readiness probe (on EKS it is managed by AWS and only this probe is visible)."
    ok, out = kubectl(["get", "--raw", "/readyz/etcd"], timeout=30)
    if ok and out.strip().lower() == "ok":
        _net_check(rep, ctx, None, title, ST_OK, "the API server's etcd readiness probe answered 'ok'.", "Nothing to do.", what, terms=terms)
    elif ok:
        _net_check(rep, ctx, None, title, ST_BAD, f"the etcd readiness probe answered: {out.strip()[:100]}.", "Open an AWS Support case: the managed control plane is unhealthy.", what, terms=terms,
                   find="etcd readiness probe is not ok")
    else:
        _net_check(rep, ctx, None, title, ST_NA, "etcd is not exposed (managed by AWS) and its readiness probe could not be read (" + (out.splitlines()[0][:80] if out else "no answer") + ").",
                   "Nothing to do from inside the cluster; AWS monitors the managed etcd. Check the cluster health in the EKS console.", what, terms=terms)


# ---------------------------------------------------------------------------
# Network section: the final checklist and the complete glossary
# ---------------------------------------------------------------------------

def _net_checklist(rep, ctx):
    rep.glossary(["CNI", "kube-proxy", "DNS", "NetworkPolicy", "Security group", "Network ACL", "Flow Logs", "Network Flow Monitor"])
    rep.subhead("Traffic issue checklist", "the ten standard questions for a traffic problem, answered from the data collected in this report. 'Not available' means the data could not be read, never that the check passed.")
    notes = ctx.data.get("net_checks", {})
    rows = []
    for key, label in NET_CHECK_ROWS:
        entries = notes.get(key) or []
        use = [e for e in entries if not e[3]] + [e for e in entries if e[3] and e[0] in (ST_WARN, ST_BAD)] or entries
        if not use:
            rows.append([label, ST_NA, "this check did not run or found nothing to evaluate in this report", "Run again with the AWS section and pod logs enabled."])
            continue
        status = _worst([e[0] for e in use], ST_NA)
        evidence = " | ".join(dict.fromkeys(e[1].rstrip(".")[:230] for e in use))
        worst = [e for e in use if e[0] == status] or use
        nxt = "No action needed." if status == ST_OK else " ".join(dict.fromkeys(e[2] for e in worst))[:300]
        rows.append([label, status, evidence[:520], nxt])
    rep.table(["Check", "Result", "Evidence found", "What to do next"], rows, limit=20, maxw=160,
              what="one row per standard check with its result (OK / Warning / Problem / Not available), the evidence behind it and the next step.")
    bad = [r[0] for r in rows if r[1] == ST_BAD]
    warn = [r[0] for r in rows if r[1] == ST_WARN]
    na = [r[0] for r in rows if r[1] == ST_NA]
    rep.add(f"  Checklist summary: {sum(1 for r in rows if r[1] == ST_OK)} OK, {len(warn)} Warning, {len(bad)} Problem, {len(na)} Not available.")
    nat = ctx.data.get("net_nat") or {}
    if ctx.data.get("net_ndots_default") and nat.get("out", 0) >= 2 ** 30:
        ctx.find("INFO", f"{ctx.data['net_ndots_default']} pod(s) use the default ndots:5 while the NAT gateways carried {_fmt_bytes(nat['out'])} to the internet - extra DNS queries per external lookup; "
                         "consider ndots:2 or fully qualified names (trailing dot) for chatty workloads")
    rep.glossary(sorted(GLOSSARY, key=str.lower), title="Glossary: complete list of the terms used in this network section")


# --- AWS: VPC, routing, NAT, endpoints, ENIs, load balancers -------------------------------------------

def _aws_vpc_routing(rep, ctx, target, cluster):
    vpc_id = (cluster.get("resourcesVpcConfig") or {}).get("vpcId")
    rep.glossary(["VPC", "NAT", "CIDR"])
    rep.subhead("Virtual Private Cloud addressing, routing, network address translation and endpoints",
                f"the address ranges, route tables, NAT gateways and VPC endpoints of {vpc_id}: how the nodes reach the internet and AWS services.")
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
            rep.table(["SUBNET", "AZ", "CIDR", "FREE IPs", "TYPE", "DEFAULT ROUTE", "ROUTE TABLE"], subnet_rows, maxw=40,
                      what="each cluster subnet with its free addresses and where its default route (traffic to the internet) points: internet gateway = public, NAT gateway = private, none = isolated.")
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
        rep.table(["NAT GATEWAY", "STATE", "SUBNET", "PUBLIC IP"], rows,
                  what="the NAT gateways that let private subnets reach the internet, their state and their public address.")
    eps, err = aws_cli(["ec2", "describe-vpc-endpoints", "--filters", f"Name=vpc-id,Values={vpc_id}"], target)
    if err:
        rep.add(f"  VPC endpoints unavailable: {_first_line(err, 100)}")
    else:
        rows = [[e["ServiceName"].split(".", 3)[-1], e.get("VpcEndpointType"), e.get("State")] for e in eps.get("VpcEndpoints", [])]
        have = " ".join(r[0] for r in rows)
        rep.add(f"  VPC endpoints ({len(rows)}):" if rows else "  VPC endpoints: none")
        rep.table(["SERVICE", "TYPE", "STATE"], rows,
                  what="the VPC endpoints that let nodes reach AWS services privately, without a NAT gateway or internet route.")
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
    rep.glossary(["ENI", "EC2", "Security group"])
    rep.subhead("Elastic network interfaces and IP addresses of the nodes",
                "how many network interfaces and IP addresses each node has in AWS, compared with the pods that need one.")
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
    rep.table(["NODE", "ENIs", "PRIVATE IPs", "PODS (VPC IP)", "SUBNETS", "SECURITY GROUPS", "NOTE"], rows, maxw=64,
              what="per node: network interfaces and IP addresses attached in AWS versus the pods that hold a VPC address (more pods than addresses means new pods will fail).")


# --- CloudWatch: TRAFFIC in the selected window --------------------------------------------------------

def _cw_args(ctx, namespace, metric, dims, stat, period):
    return ["cloudwatch", "get-metric-statistics", "--namespace", namespace, "--metric-name", metric,
            "--dimensions", *[f"Name={k},Value={v}" for k, v in dims.items()],
            "--start-time", _iso(ctx.since), "--end-time", _iso(ctx.now), "--period", str(period), "--statistics", stat]


def _lb_cw_jobs(kind, ident):
    """(CloudWatch namespace, dimensions, [(key, metric, statistic)]) of one load balancer."""
    if kind == "alb":
        return "AWS/ApplicationELB", {"LoadBalancer": ident}, [
            ("req", "RequestCount", "Sum"), ("2xx", "HTTPCode_Target_2XX_Count", "Sum"), ("4xx", "HTTPCode_Target_4XX_Count", "Sum"),
            ("5xx", "HTTPCode_Target_5XX_Count", "Sum"), ("elb5xx", "HTTPCode_ELB_5XX_Count", "Sum"),
            ("lat", "TargetResponseTime", "Average"), ("bytes", "ProcessedBytes", "Sum")]
    if kind == "nlb":
        return "AWS/NetworkELB", {"LoadBalancer": ident}, [
            ("flows", "NewFlowCount", "Sum"), ("active", "ActiveFlowCount", "Average"), ("bytes", "ProcessedBytes", "Sum"),
            ("rst", "TCP_Target_Reset_Count", "Sum")]
    return "AWS/ELB", {"LoadBalancerName": ident}, [
        ("req", "RequestCount", "Sum"), ("2xx", "HTTPCode_Backend_2XX", "Sum"), ("4xx", "HTTPCode_Backend_4XX", "Sum"),
        ("5xx", "HTTPCode_Backend_5XX", "Sum"), ("elb5xx", "HTTPCode_ELB_5XX", "Sum"), ("lat", "Latency", "Average"), ("bytes", "EstimatedProcessedBytes", "Sum")]


NAT_CW_JOBS = (("out", "BytesOutToDestination"), ("in", "BytesInFromDestination"), ("drop", "PacketsDropCount"), ("port", "ErrorPortAllocation"))


def _cw_points(ctx, target, namespace, metric, dims, stat, period):
    """[(datetime, value)] from CloudWatch for the selected window, or (None, error)."""
    args = _cw_args(ctx, namespace, metric, dims, stat, period)
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
    rep.glossary(["EC2", "ALB", "NLB", "NAT"])
    rep.subhead(f"Traffic in the selected window (last {mins} minutes, from Amazon CloudWatch)",
                "how many bytes and requests the nodes, load balancers and NAT gateways carried in the window, with a chart per resource below.")

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
        with _pool(8) as pool:
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
            rep.table(["NODE", "IN avg", "IN peak", "IN total", "OUT avg", "OUT peak", "OUT total"], rows, maxw=60,
                      what="per node: average and peak data rate and the total bytes that came in and went out in the window (Amazon EC2 NetworkIn / NetworkOut).")
            busiest = max(series_rows[1:] or series_rows, key=lambda r: (r["lines"][1]["max"] or 0) + (r["lines"][0]["max"] or 0))
            ctx.find("INFO", f"Busiest node on the network in the window: {busiest['n']} (peak in {_fmt_rate(busiest['lines'][0]['max'])}, out {_fmt_rate(busiest['lines'][1]['max'])})")
    rep.series(f"Node network traffic, last {mins} minutes (bytes per second)", series_rows,
               "EC2 NetworkIn / NetworkOut. Basic monitoring gives one point per 5 minutes, so short windows show few points.",
               about="a small line chart per node of the data that came in and went out of the node during the window; each card also shows the average, the peak and the total.")

    period = 60 if mins <= 360 else 300
    # ---- load balancers
    lb_series, lb_rows = [], []
    for kind, ident, name, dns in lbs[:10]:
        ns_, dim, jobs = _lb_cw_jobs(kind, ident)
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
            lines.append(_line("successful (2xx)", got["2xx"], "count"))
        if got.get("4xx"):
            lines.append(_line("client errors (4xx)", got["4xx"], "count"))
        if got.get("5xx"):
            lines.append(_line("server errors (5xx)", got["5xx"], "count"))
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
        rep.table(["LOAD BALANCER", "TYPE", "REQUESTS", "2xx", "4xx", "5xx (error rate)", "TARGET LATENCY", "BYTES"], lb_rows, maxw=40, terms=("HTTP", "ALB", "NLB"),
                  what="per load balancer: requests, successful / client-error / server-error responses, how long the targets took to answer and the bytes processed in the window.")
        rep.series(f"Load balancer traffic, last {mins} minutes", lb_series, "Per-minute counts from CloudWatch (ALB: RequestCount and HTTP codes, NLB: flows, classic: backend codes).",
                   about="a small line chart per load balancer of the requests, the responses by status class, the response time and the bytes handled in each period of the window.")

    # ---- NAT gateways
    nat_series, nat_rows = [], []
    nat_tot = {"out": 0.0, "drops": 0.0, "port_errors": 0.0, "gateways": 0}
    for nat in nat_ids[:6]:
        got = {}
        for key, metric in NAT_CW_JOBS:
            pts, err = _cw_points(ctx, target, "AWS/NATGateway", metric, {"NatGatewayId": nat}, "Sum", period)
            if pts is not None:
                got[key] = pts
        if not got:
            continue
        tot = lambda k: sum(v for _, v in got.get(k, []))
        nat_rows.append([nat, _fmt_bytes(tot("out")), _fmt_bytes(tot("in")), f"{tot('drop'):.0f}", f"{tot('port'):.0f}"])
        nat_tot["out"] += tot("out")
        nat_tot["drops"] += tot("drop")
        nat_tot["port_errors"] += tot("port")
        nat_tot["gateways"] += 1
        if tot("port") > 0:
            ctx.find("HIGH", f"NAT gateway {nat}: {tot('port'):.0f} port allocation errors in the window (port exhaustion - too many connections to one destination)")
        elif tot("drop") > 0:
            ctx.find("MED", f"NAT gateway {nat}: {tot('drop'):.0f} dropped packets in the window")
        nat_series.append({"n": nat, "s": "NAT gateway", "lines": [_line("out to internet", got.get("out", []), "Bps", period), _line("in from internet", got.get("in", []), "Bps", period)]})
    if nat_rows:
        rep.add("")
        rep.add("  NAT gateway traffic in the window (egress to the internet from private subnets):")
        ctx.data["net_nat"] = nat_tot
        rep.table(["NAT GATEWAY", "BYTES OUT", "BYTES IN", "DROPPED PACKETS", "PORT ALLOC ERRORS"], nat_rows,
                  what="per NAT gateway: bytes sent to and received from the internet, dropped packets and port allocation errors (which mean the gateway ran out of ports).")
        rep.series(f"Network address translation (NAT) gateway traffic, last {mins} minutes (bytes per second)", nat_series, "",
                   about="a small line chart per network address translation gateway of the data sent out to the internet and received back from it in each period of the window.")


def _net_aws(rep, ctx, target, cluster):
    vpc_id = (cluster.get("resourcesVpcConfig") or {}).get("vpcId")
    if not vpc_id:
        rep.add("  (no VPC id from the cluster description)")
        return
    nat_ids, lbs = [], ctx.data.get("net_lbs") or []
    try:
        nat_ids = _aws_vpc_routing(rep, ctx, target, cluster) or []
    except Exception as exc:
        rep.add(f"[!] VPC routing step failed: {exc}")
    try:
        _aws_node_enis(rep, ctx, target)
    except Exception as exc:
        rep.add(f"[!] node network interface step failed: {exc}")
    if ctx.cancel is not None and ctx.cancel.is_set():
        return
    try:
        _aws_traffic(rep, ctx, target, nat_ids, lbs)
    except Exception as exc:
        rep.add(f"[!] traffic step failed: {exc}")


def _warm_lb(ctx):
    """(run-ahead only) Start the load balancer calls (list, target groups, target health) and publish the list of load balancers."""
    try:
        if not warm_wait(ctx, "aws_basics"):
            return
        target, cluster = ctx.data.get("aws_target"), ctx.data.get("aws_cluster")
        vpc_id = ((cluster or {}).get("resourcesVpcConfig") or {}).get("vpcId")
        if not (AWS_OPTS["enabled"] and target and vpc_id):
            return
        hint_a(ctx, ["elbv2", "describe-load-balancers"], target, 90)
        hint_a(ctx, ["elb", "describe-load-balancers"], target, 90)
        found = []
        v2, err = aws_cli(["elbv2", "describe-load-balancers"], target, timeout=90)
        if not err:
            lbs = [x for x in v2.get("LoadBalancers", []) if x.get("VpcId") == vpc_id][:12]
            for lb in lbs:
                hint_a(ctx, ["elbv2", "describe-target-groups", "--load-balancer-arn", lb["LoadBalancerArn"]], target)
            for lb in lbs:
                arn = lb["LoadBalancerArn"]
                tgs, terr = aws_cli(["elbv2", "describe-target-groups", "--load-balancer-arn", arn], target)
                for tg in ([] if terr else tgs.get("TargetGroups", [])):
                    hint_a(ctx, ["elbv2", "describe-target-health", "--target-group-arn", tg["TargetGroupArn"]], target)
                found.append(("alb" if lb.get("Type") == "application" else "nlb", arn.split("loadbalancer/", 1)[-1], lb["LoadBalancerName"], lb.get("DNSName", "")))
        classic, cerr = aws_cli(["elb", "describe-load-balancers"], target, timeout=90)
        if not cerr:
            cl = [x for x in classic.get("LoadBalancerDescriptions", []) if x.get("VPCId") == vpc_id][:12]
            for lb in cl:
                hint_a(ctx, ["elb", "describe-instance-health", "--load-balancer-name", lb["LoadBalancerName"]], target)
                found.append(("classic", lb["LoadBalancerName"], lb["LoadBalancerName"], lb.get("DNSName", "")))
        ctx.data["net_lbs"] = found
    finally:
        ctx.warm.set("lbs")


def _warm_cw(ctx):
    """(run-ahead only) Start the VPC reads and every CloudWatch traffic query of the network section (hundreds of small calls)."""
    if not warm_wait(ctx, "aws_basics"):
        return
    target, cluster = ctx.data.get("aws_target"), ctx.data.get("aws_cluster")
    vpc_id = ((cluster or {}).get("resourcesVpcConfig") or {}).get("vpcId")
    if not (AWS_OPTS["enabled"] and target and cluster and vpc_id):
        return
    mins = ctx.minutes
    nat_args = ["ec2", "describe-nat-gateways", "--filter", f"Name=vpc-id,Values={vpc_id}"]
    hint_a(ctx, nat_args, target)
    hint_a(ctx, ["ec2", "describe-vpcs", "--vpc-ids", vpc_id], target)
    hint_a(ctx, ["ec2", "describe-route-tables", "--filters", f"Name=vpc-id,Values={vpc_id}"], target)
    hint_a(ctx, ["ec2", "describe-vpc-endpoints", "--filters", f"Name=vpc-id,Values={vpc_id}"], target)
    idents = node_idents(ctx)
    ids = {i["instance_id"]: n for n, i in idents.items() if i["instance_id"].startswith("i-")}
    if ids:
        hint_a(ctx, ["ec2", "describe-network-interfaces", "--filters", "Name=attachment.instance-id,Values=" + ",".join(list(ids)[:50])], target)
        for iid in list(ids)[:30]:
            for metric in ("NetworkIn", "NetworkOut"):
                hint_a(ctx, _cw_args(ctx, "AWS/EC2", metric, {"InstanceId": iid}, "Sum", 300), target, 60)
    period = 60 if mins <= 360 else 300
    nats, err = aws_cli(nat_args, target)
    if not err:
        nat_ids = [g["NatGatewayId"] for g in (nats or {}).get("NatGateways", []) if g.get("State") != "deleted"]
        for nat in nat_ids[:6]:
            for _key, metric in NAT_CW_JOBS:
                hint_a(ctx, _cw_args(ctx, "AWS/NATGateway", metric, {"NatGatewayId": nat}, "Sum", period), target, 60)
    if warm_wait(ctx, "lbs"):
        for kind, ident, _name, _dns in (ctx.data.get("net_lbs") or [])[:10]:
            ns_, dim, jobs = _lb_cw_jobs(kind, ident)
            for _key, metric, stat in jobs:
                hint_a(ctx, _cw_args(ctx, ns_, metric, dim, stat, period), target, 60)


def _warm_network(rep, ctx):
    """(run-ahead tasks only) The network section on the scratch context: every check at once instead of one after another. The
    checks that need another check's answer wait for it (the plugin settings; the AWS cluster description; the load balancer list)."""
    warm_wait(ctx, "data")
    pool = _pool(18)
    futs = {}

    def submit(name, fn, *args, env=False, waits=()):
        def job():
            for ev in waits:
                warm_wait(ctx, ev)
            call_args = args
            if env:
                try:
                    call_args = (futs["cni"].result(),)
                except Exception:
                    call_args = (None,)
            if ctx.cancel is not None and ctx.cancel.is_set():
                return None
            try:
                return fn(rep, ctx, *call_args)
            except Exception:
                return None
        futs[name] = pool.submit(job)

    def net_aws_task(rep_, ctx_):
        if not warm_wait(ctx_, "aws_basics"):
            return
        target, cluster = ctx_.data.get("aws_target"), ctx_.data.get("aws_cluster")
        if AWS_OPTS["enabled"] and target and cluster:
            warm_wait(ctx_, "lbs")
            _net_aws(rep_, ctx_, target, cluster)
    submit("cni", _chk_cni)
    submit("lb", lambda r, c: _warm_lb(c))
    submit("cw", lambda r, c: _warm_cw(c))
    submit("settings", _net_cluster_settings)
    submit("ip", _chk_ip_limits, env=True, waits=("aws_net",))
    submit("cnilogs", _chk_cni_logs)
    submit("stuck", _chk_stuck_pods)
    submit("startup", _chk_startup_order)
    submit("events", _net_warning_events)
    submit("nodecond", _chk_node_conditions)
    submit("traffic", _net_pod_traffic)
    submit("mtu", _chk_mtu, env=True)
    submit("throttle", _chk_throttling)
    submit("proxy", _chk_kube_proxy)
    submit("services", _chk_services)
    submit("dns", _chk_dns)
    submit("policies", _chk_policies, env=True)
    submit("firewalls", _chk_firewalls, waits=("aws_net",))
    submit("ingress", _chk_ingress_lb, waits=("aws_basics",))
    submit("netaws", net_aws_task)
    submit("conntrack", _chk_conntrack, env=True)
    submit("observe", _chk_observability, waits=("aws_basics",))
    submit("api", _chk_apiserver_throttling)
    submit("webhooks", _chk_webhooks)
    submit("etcd", _chk_etcd)
    for f in list(futs.values()):
        try:
            f.result()
        except Exception:
            pass
    pool.shutdown(wait=True)


def section_network_details(rep, ctx, label):
    if ctx.warm is not None:
        _warm_network(rep, ctx)
        return
    rep.section(f"10. NETWORK & TRAFFIC - POD NETWORKING, NODES, SERVICE ROUTING, DOMAIN NAME SYSTEM, FIREWALLS, LOAD BALANCERS, TRAFFIC (last {ctx.minutes} minutes)", "network", ctx.minutes)
    rep.add("How to read this section: every check has a status - OK, Warning, Problem or Not available (with the reason) - and a 'What this means / what to do next' line.")
    rep.add("The 'Traffic issue checklist' near the end sums them up. Everything is read-only: no command runs inside a pod or on a node, and no packet is captured.")
    state = {}

    def run(fn, *args):
        if ctx.cancel is not None and ctx.cancel.is_set():
            return None
        try:
            return fn(rep, ctx, *args)
        except Exception as exc:
            rep.add(f"[!] {fn.__name__} failed: {exc}")
            return None

    run(_net_cluster_settings)
    rep.subhead("Part 1 - Pod-level networking", "can pods get an address, start, and reach the network?")
    state["env"] = run(_chk_cni)
    run(_chk_ip_limits, state["env"])
    run(_chk_cni_logs)
    run(_chk_stuck_pods)
    run(_chk_startup_order)
    run(_net_warning_events)
    rep.subhead("Part 2 - Node networking", "are the nodes themselves healthy enough to carry traffic?")
    run(_chk_node_conditions)
    run(_net_pod_traffic)
    run(_chk_mtu, state["env"])
    run(_chk_throttling)
    rep.subhead("Part 3 - kube-proxy and Service routing", "is traffic sent to a Service address forwarded to a ready pod?")
    run(_chk_kube_proxy)
    run(_chk_services)
    rep.subhead("Part 4 - Domain Name System", "can pods look up names?")
    run(_chk_dns)
    rep.subhead("Part 5 - Network policies and cloud firewalls", "is traffic blocked on purpose (policies) or by a firewall rule?")
    run(_chk_policies, state["env"])
    run(_chk_firewalls)
    rep.subhead("Part 6 - Load balancers and ingress", "does traffic from outside reach the pods, and are the health checks green?")
    run(_chk_ingress_lb)
    target, cluster = ctx.data.get("aws_target"), ctx.data.get("aws_cluster")
    rep.subhead("Part 7 - Cloud network and traffic (Amazon Web Services)", "addresses, routes, NAT, network interfaces and the traffic of the selected window as AWS sees it.")
    if AWS_OPTS["enabled"] and target and cluster:
        run(_net_aws, target, cluster)
    else:
        rep.add("AWS NETWORK AND TRAFFIC: skipped - the AWS section is off or could not describe the cluster. "
                "(VPC routing, NAT, load balancers and the traffic over the window need AWS access.)")
    rep.subhead("Part 8 - Connection tracking and port exhaustion", "do new connections fail because a table or a port range is full?")
    run(_chk_conntrack, state["env"])
    rep.subhead("Part 9 - Observability and packet capture", "which tools can show past traffic, and how to capture packets safely.")
    run(_chk_observability)
    rep.subhead("Part 10 - Control plane and Kubernetes application programming interface (API) server", "is the Kubernetes API (which nodes and controllers depend on) throttling or blocked by a webhook?")
    run(_chk_apiserver_throttling)
    run(_chk_webhooks)
    run(_chk_etcd)
    rep.subhead("Part 11 - Summary", "the checklist of all checks above and the glossary of terms.")
    run(_net_checklist)


def section_scaling_storage_network(rep, ctx):
    rep.section("11. AUTOSCALING, STORAGE, NETWORKING", "scaling", ctx.minutes)
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
        rep.table(["HPA", "SUPPORT DL", "REPLICAS", "ISSUE"], rows, title="Horizontal pod autoscalers with problems", terms=("HPA", "DL"),
                  about="the horizontal pod autoscalers that are at their maximum or cannot scale: current / desired / maximum number of pods and the problem.")
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
        rep.table(["NAME", "SUPPORT DL", "PHASE", "STORAGECLASS", "AGE"], pvcs + pvs, title="Storage problems (claims not Bound, volumes Failed)", terms=("PVC", "PV"),
                  about="the persistent volume claims that are not Bound (no disk assigned yet) and the persistent volumes in the Failed phase, with the storage class and age.")
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
        rep.table(["SERVICE", "SUPPORT DL", "TYPE", "ISSUE"], svc_rows, title="Services with problems", terms=("Endpoints", "LoadBalancer"),
                  about="the Services that cannot work yet: no ready pods behind them (no endpoints), or a LoadBalancer that has no external address.")
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
    rep.section("4. RESOURCE UTILIZATION - PROCESSOR (CPU) & MEMORY BY NAMESPACE", "utilization", ctx.minutes)
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

    rep.util(data, about="an interactive dashboard of processor and memory use: cluster gauges, who uses the cluster, a card per node, an explorer per namespace and "
                         "the top consumers (it explains each of its parts); the table below it is the detailed list.")      # the dashboard comes first in the HTML

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
    rep.table(["NAMESPACE", "SUPPORT DL", "PODS", "CPU use", "CPU req", "CPU lim", "CPU %cl", "MEM use", "MEM req", "MEM lim", "MEM %cl", "HIGH PODS"], rows,
              title="Namespaces ranked by memory", terms=("CPU", "Request", "Limit", "DL"),
              about="one row per namespace, the biggest memory user first: its pods, processor and memory used now, requested and limited, its percent share of what the "
                    "whole cluster can give (allocatable), and how many pods are at 90% or more of a limit.")

    for metric, key, fmt in (("CPU", "cu", lambda v: f"{v:.2f} cores"), ("memory", "mu", _mi)):
        top = max((x for x in data["namespaces"] if x[key]), key=lambda x: x[key], default=None)
        total = c[key]
        if top and total:
            ctx.find("INFO", f"Top {metric} consumer: namespace {top['name']} ({fmt(top[key])}, "
                             f"{100 * top[key] / total:.0f}% of the cluster's {metric} use)")


def section_top(rep, ctx):
    rep.section("12. TOP RESOURCE CONSUMERS (live)", "top", ctx.minutes)
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
        shown = "processor (CPU)" if label == "CPU" else label
        if not rows:
            rep.add(f"No pod has a live {shown} reading.")
            continue
        rep.table(["NAMESPACE", "SUPPORT DL", "POD", "CPU", "MEMORY", "DISK", f"% of {label} limit"], rows, title=f"Top 10 pods by {shown}",
                  terms=("CPU", "Limit", "DL") if label == "CPU" else None,
                  about=f"the ten pods that use the most {shown} right now: their processor, memory and disk use and the percent of their {shown} limit that this is "
                        "(no limit means the pod may use everything the node has).")


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
    rep.section(f"13. LOGS (last {ctx.minutes} minutes) - {scope}", "logs", ctx.minutes)
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
            f"(up to {LOG_TAIL_LINES} lines each, last {ctx.minutes} minutes) ...")

    results = [None] * len(jobs)

    def work(i):
        if ctx.cancel is not None and ctx.cancel.is_set():
            return i, None, "cancelled"
        j = jobs[i]
        lines, err = _container_logs(j[0], j[1], j[2], ctx.minutes, previous=j[3])
        return i, lines, err

    finished = 0
    with _pool(LOG_WORKERS) as pool:
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

    rep.table(["POD", "SUPPORT DL", "CONTAINER", "LOG", "WHY COLLECTED", "LINES", "ERROR-LIKE", "WARNINGS", "LAST LINE"], rows, maxw=90,
              title=f"Overview of {len(rows)} log stream(s)", terms=("DL",),
              about="one row per container log that was read: the pod and container, whether it is the current or the previous (before a restart) log, why it was read, "
                    "how many lines it has, how many look like errors or warnings and its last line.")
    errs_total = sum(r[6] for r in rows)
    rep.add(f"Total: {sum(r[5] for r in rows)} line(s), {errs_total} error-like.")
    if blocks:
        rep.subhead("Log lines of each container",
                    about="the log lines themselves, one collapsible entry per container (errors in red, warnings in amber); the text report shows only the error lines and the last few lines.")
    for title, entries, text_entries in blocks:
        rep.add("")
        rep.log(title, entries, text_entries)
    if ctx.cancel is not None and ctx.cancel.is_set():
        rep.add("(log collection was stopped early)")


def section_timeline(rep, ctx):
    rep.section(f"14. TIMELINE - what happened in the last {ctx.minutes} minutes (oldest first)", "timeline", ctx.minutes)
    if not ctx.timeline:
        rep.add("Nothing notable recorded in this window.")
        return
    entries = sorted(set(ctx.timeline), key=lambda x: x[0])
    skipped = max(0, len(entries) - MAX_TIMELINE)
    if skipped:
        rep.add(f"({skipped} older entries not shown)")
    rep.timeline(entries[-MAX_TIMELINE:], about="every notable thing the other sections saw in the window, oldest first, with its time (UTC) and kind; use the kind chips to filter.")


def build_summary(ctx, label, skipped=None):
    order = {"CRIT": 0, "HIGH": 1, "MED": 2, "INFO": 3}
    lines = ["=" * 78, f"HEALTH SUMMARY - {label}  (last {ctx.minutes} minutes, {ctx.now:%Y-%m-%d %H:%M:%S} Coordinated Universal Time)", "=" * 78]
    lines.append("Read-only: this tool only reads - nothing is installed, created, changed or deleted on the cluster or in the cloud account.")
    lines.append("What this section shows: " + REPORT_PARTS["summary"]["shows"])
    if skipped:
        lines.append("Skipped by choice (not part of this summary): " + ", ".join(skipped))
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
    lines += ["", "What the severity levels mean:"]
    lines += _plain_table(["Severity", "Short form in the text report", "Meaning"], [[w, c, m] for w, c, m in SEVERITY_LEGEND],
                          "the four severity levels used for findings and what each one means.")
    lines += ["", "What the status words mean:"]
    lines += _plain_table(["Status", "Meaning"], [[w, m] for w, m in STATUS_LEGEND],
                          "the four status words used by the checks (for example in the network section) and what each one means.")
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
h3.bh{margin:20px 0 4px;font-size:15px;padding-top:10px;border-top:1px dashed var(--line)}
p.what{margin:2px 0 8px;color:var(--muted);font-size:12.5px}
.tablewrap p.what{margin:0;padding:6px 10px;background:var(--code);border-bottom:1px solid var(--line)}
.chk{border:1px solid var(--line);border-left-width:6px;border-radius:8px;padding:6px 12px;margin:6px 0 10px;background:var(--card)}
.chk p{margin:4px 0}.stat{display:inline-block;font-weight:700;font-size:12px;border-radius:5px;padding:1px 8px;margin-top:4px}
.chk.st-ok{border-left-color:var(--good)}.chk.st-warn{border-left-color:#e8a317}.chk.st-bad{border-left-color:var(--crit)}.chk.st-na{border-left-color:var(--muted)}
.stat.st-ok{background:var(--goodbg);color:var(--good)}.stat.st-warn{background:var(--medbg);color:var(--med)}.stat.st-bad{background:var(--critbg);color:var(--crit)}.stat.st-na{background:var(--code);color:var(--muted)}
.tablewrap.glossary{border-style:dashed}
.shows{background:var(--pale,var(--code));border:1px solid var(--line);border-left:5px solid var(--brand,var(--accent));border-radius:10px;padding:8px 14px;margin:12px 0 4px;font-size:13px}
.shows p{margin:3px 0}.shows .how{color:var(--muted)}
.sevcard small{display:block;font-weight:400;font-size:11px;margin-top:2px}
th[title]{text-decoration:underline dotted var(--muted);text-underline-offset:3px}
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
const BAD=/^(Problem|NotReady|CrashLoopBackOff|Error|Failed|Evicted|ImagePullBackOff|ErrImagePull|OOMKilled|DEGRADED|CREATE_FAILED|FAILED|PROBLEM|impaired|CRIT|Unknown|MISSING|FAILING)/i;
const WARN=/^(Warning|Pending|Terminating|Ready,SchedulingDisabled|SchedulingDisabled|UPDATING|CREATING|low IPs|VERY LOW|HIGH|AT MAX|insufficient|NEW node)/i;
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
.nrow{display:grid;grid-template-columns:96px 1fr;gap:8px;align-items:center;margin:5px 0;font-size:12px}
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
const what=t=>el('p','what','What this block shows: '+esc(t));
if(!D.hasUsage)root.appendChild(el('div','note','<b>Live processor (CPU) and memory usage is not available</b> (it needs the kubelet stats permission or metrics-server). Everything below shows what pods <b>request</b> and are <b>limited</b> to instead of what they use right now.'));

// ---- cluster tiles
function tile(label,use,total,fmt,req){
  const p=pct(use,total),rp=pct(req,total),t=el('div','tile');
  t.innerHTML='<div class="tl">'+label+'</div><div class="tv">'+(use==null?'n/a':fmt(use))+' <small>of '+fmt(total)+' allocatable ('+fPct(p)+')</small></div>'
   +'<div class="gauge"><i class="g '+sev(p)+'" style="width:'+Math.min(100,p||0)+'%"></i>'+(rp!=null?'<i class="m" style="left:'+Math.min(100,rp)+'%" title="requested"></i>':'')+'</div>'
   +'<div class="ts">requested '+fmt(req)+' ('+fPct(rp)+') &nbsp;|&nbsp; the tick marks the requested amount</div>';
  return t;}
root.appendChild(what('the cluster as a whole: processor, memory and pod places used now compared with what all nodes can give to pods (allocatable); the tick on a gauge marks the amount that pods have requested.'));
const tiles=el('div','tiles');
tiles.appendChild(tile('Cluster processor (CPU)',C.cu,C.ca,fCpu,C.cr));
tiles.appendChild(tile('Cluster memory',C.mu,C.ma,fMem,C.mr));
(function(){const t=el('div','tile'),p=pct(C.pods,C.mp);
  t.innerHTML='<div class="tl">Pods (running and pending) versus node capacity</div><div class="tv">'+C.pods+' <small>of '+C.mp+' ('+fPct(p)+')</small></div><div class="gauge"><i class="g '+sev(p)+'" style="width:'+Math.min(100,p||0)+'%"></i></div>';tiles.appendChild(t);})();
root.appendChild(tiles);

// ---- who uses the cluster: stacked share by namespace
function stacked(title,key,reqKey,fmt,total){
  const useK=D.hasUsage?key:reqKey;let arr=D.namespaces.filter(n=>n[useK]).sort((a,b)=>b[useK]-a[useK]);
  const sum=arr.reduce((s,n)=>s+n[useK],0);if(!sum)return;
  const top=arr.slice(0,10),rest=arr.slice(10).reduce((s,n)=>s+n[useK],0);
  const wrap=el('div');wrap.appendChild(el('h3',null,title+(D.hasUsage?' (used)':' (requested)')));
  wrap.appendChild(what('who uses the cluster: one coloured slice per namespace (the ten biggest, the rest grouped as others); hover a slice for the exact numbers.'));
  const bar=el('div','stack'),leg=el('div','legend');
  top.forEach(n=>{const s=el('span');s.style.width=(100*n[useK]/sum)+'%';s.style.background=nsColor(n.name);s.title=n.name+': '+fmt(n[useK])+' ('+Math.round(100*n[useK]/sum)+'% of what pods use; '+fPct(pct(n[useK],total))+' of allocatable)';bar.appendChild(s);
    leg.appendChild(el('span',null,'<b style="background:'+nsColor(n.name)+'"></b>'+esc(n.name)+' '+fmt(n[useK])+' ('+Math.round(100*n[useK]/sum)+'%)'));});
  if(rest){const s=el('span');s.style.width=(100*rest/sum)+'%';s.style.background='#98a2b3';s.title='other namespaces: '+fmt(rest);bar.appendChild(s);leg.appendChild(el('span',null,'<b style="background:#98a2b3"></b>others '+fmt(rest)));}
  wrap.appendChild(bar);wrap.appendChild(leg);root.appendChild(wrap);}
stacked('Processor (CPU) share by namespace','cu','cr',fCpu,C.ca);
stacked('Memory share by namespace','mu','mr',fMem,C.ma);

// ---- nodes
root.appendChild(el('h3',null,'Nodes (sorted by the most loaded)'));
root.appendChild(what('one card per node, the most loaded first: live use, request and percent of what the node can give for processor, memory, disk and pods; a card turns amber from '+WARN+'% and red from '+CRIT+'%.'));
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
    +'<div class="nid"><b>'+esc(n.id||'-')+'</b> &middot; '+esc(n.zone||'-')+' &middot; '+esc(n.type||'-')+(n.ec2&&n.ec2!=='-'?' &middot; server name tag: '+esc(n.ec2):'')+'</div>';
  c.appendChild(nrow('Processor (CPU)',n.cu,n.ca,fCpu,n.cr));c.appendChild(nrow('Memory',n.mu,n.ma,fMem,n.mr));
  if(n.du!=null)c.appendChild(nrow('Disk',n.du,n.dc,fMem));
  c.appendChild(nrow('Pods',n.pods,n.mp,x=>String(Math.round(x))));
  if(n.su)c.appendChild(el('div','nv','<span class="pill c">swap in use '+fMem(n.su)+'</span>'));
  grid.appendChild(c);});
root.appendChild(grid);

// ---- namespace explorer
root.appendChild(el('h3',null,'By namespace (click a namespace to see its pods)'));
root.appendChild(what('every namespace with a bar of its use (or request); the ticks mark the request and the limit. Click a namespace to list its pods; use the buttons, the sort box and the filter above the list.'));
const S={metric:'mem',mode:D.hasUsage?'use':'req',sort:'use',high:false,q:''};
const KEY={cpu:{use:'cu',req:'cr',lim:'cl',nolim:'ncl',fmt:fCpu,tot:C.ca,name:'Processor (CPU)'},mem:{use:'mu',req:'mr',lim:'ml',nolim:'nml',fmt:fMem,tot:C.ma,name:'Memory'},disk:{use:'du',req:null,lim:null,fmt:fMem,tot:null,name:'Disk'}};
const ctl=el('div','ctl');
ctl.innerHTML='<span class="seg" id="u-metric"><button data-v="cpu">Processor (CPU)</button><button data-v="mem" class="on">Memory</button><button data-v="disk">Disk</button></span>'
 +'<span class="seg" id="u-mode"><button data-v="use"'+(D.hasUsage?' class="on"':' disabled title="no live usage"')+'>Used</button><button data-v="req"'+(D.hasUsage?'':' class="on"')+'>Requested</button></span>'
 +'<label>Sort <select id="u-sort"><option value="use">highest value</option><option value="limpct">closest to limit (worst pod)</option><option value="share">share of cluster</option><option value="rs">most restarts</option><option value="name">name</option></select></label>'
 +'<label><input type="checkbox" id="u-high"> only namespaces with a pod at '+WARN+'%+ of its limit</label><input type="search" id="u-q" placeholder="Filter namespace or support team contact...">';
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
root.appendChild(what('the pods at the top of each ranking (twelve at most); the bar is relative to the biggest value and turns amber or red near the limit.'));
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
  toplist('Top processor (CPU)',p=>p.cu,fCpu,p=>sev(pct(p.cu,p.cl)),p=>p.cl?fPct(pct(p.cu,p.cl))+' of limit':'no limit');
  toplist('Top memory',p=>p.mu,fMem,p=>sev(pct(p.mu,p.ml)),p=>p.ml?fPct(pct(p.mu,p.ml))+' of limit':'no limit');
  toplist('Closest to the memory limit (out-of-memory risk)',p=>pct(p.mu,p.ml),fPct,p=>sev(pct(p.mu,p.ml)),p=>fMem(p.mu)+' / '+fMem(p.ml));
  toplist('Closest to the processor (CPU) limit (throttling)',p=>pct(p.cu,p.cl),fPct,p=>sev(pct(p.cu,p.cl)),p=>fCpu(p.cu)+' / '+fCpu(p.cl));
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
.sline{display:grid;grid-template-columns:120px 1fr;gap:8px;align-items:center;margin:4px 0}
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


def _series_block_html(title, rows, note, about=None):
    payload = json.dumps({"rows": rows}, separators=(",", ":")).replace("</", "<\\/")
    return ('<div class="series"><h3>%s</h3>%s%s<div class="sgrid"></div><script type="application/json">%s</script></div>'
            % (_html.escape(title), _what(about, "block"), ('<div class="snote">%s</div>' % _html.escape(note)) if note else "", payload))


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


_BRAND_CSS = r"""
:root{--navy:@NAVY@;--dark:@DARK@;--brand:@ACCENT@;--pale:@PALE@;--accent:#B25E00;--link:@LINK@}
[data-theme=dark]{--accent:#FFB84D;--pale:#2b2417;--link:#6cb4ee}
.brandband{position:relative;overflow:hidden;background:linear-gradient(105deg,var(--navy) 0%,var(--dark) 100%);color:#fff;padding:14px 22px;display:flex;align-items:center;gap:18px;border-bottom:4px solid var(--brand)}
.brandband .bb-logo{display:flex;align-items:center;gap:12px;flex:none;position:relative;z-index:1}
.brandband .bb-text{position:relative;z-index:1;min-width:0}
.bb-title{font-size:20px;font-weight:700;letter-spacing:.2px;line-height:1.2}
.bb-sub{color:#cfd8e3;font-size:12.5px;margin-top:4px}
.bb-pill{display:inline-block;border-radius:999px;padding:1px 10px;margin:2px 6px 0 0;font-size:11.5px;background:rgba(255,255,255,.13);color:#fff}
.bb-pill b{color:var(--brand)}
.bb-cloud{position:absolute;pointer-events:none;opacity:.07;z-index:0}
header h1{color:var(--navy)}[data-theme=dark] header h1{color:#fff}
header{border-top:0}
.chip.on{background:var(--brand);border-color:var(--brand);color:#232F3E;font-weight:600}
ul.timeline li:before{background:var(--brand)}
details.sec{border-left:5px solid var(--brand)}
details.sec>summary{color:var(--navy)}[data-theme=dark] details.sec>summary{color:#fff}
table.data th{color:var(--navy)}[data-theme=dark] table.data th{color:#e6eaf0}
.ln.sub,h3.bh{color:var(--accent)}
button.on,button:hover{border-color:var(--brand);color:var(--accent)}
.ico{display:inline-block;min-width:1.5em;text-align:center;margin-right:3px}
details.sec>summary>span[data-ico]:before{content:attr(data-ico);display:inline-block;min-width:1.5em;text-align:center;margin-right:4px}
nav .skiprow{display:flex;justify-content:space-between;gap:6px;padding:6px 8px;font-size:13px;color:var(--muted);font-style:italic}
nav .skiprow small{font-size:10.5px;white-space:nowrap}
.skipline{background:var(--card);border:1px dashed var(--line);border-left:5px solid var(--line);border-radius:10px;padding:8px 14px;margin-bottom:10px;color:var(--muted);font-size:13px}
.secnote{background:var(--pale);border:1px solid var(--line);border-left:5px solid var(--brand);border-radius:10px;padding:8px 14px;margin:0 0 12px;font-size:13px}
.foottiming{max-width:900px;margin:10px auto 0;text-align:left}
.foottiming summary{cursor:pointer;color:var(--accent);font-weight:600}
.foottiming pre{background:var(--code);border-radius:8px;padding:8px 12px;overflow:auto;font:12px/1.5 Consolas,"Cascadia Mono",monospace;color:var(--text)}
footer{border-top:3px solid var(--brand)}
@media(max-width:700px){.brandband{flex-wrap:wrap}.bb-title{font-size:17px}}
"""


def _brand_css():
    return (_BRAND_CSS.replace("@NAVY@", BRAND_PRIMARY).replace("@DARK@", BRAND_DARK).replace("@ACCENT@", BRAND_ACCENT)
            .replace("@PALE@", BRAND_PALE).replace("@LINK@", BRAND_LINK))


def _ico_attr(name):
    """data-ico="..." for a <span>: the icon is drawn by CSS, so the text of the heading is untouched."""
    sym = ICONS.get(name, ("", ""))[0]
    return f'data-ico="{sym}"' if sym else ""


def _icon_html(name):
    """The emoji of a registry icon for the HTML report (browsers draw them; the Tk fallback does not apply here)."""
    sym = ICONS.get(name, ("", ""))[0]
    return f'<span class="ico" aria-hidden="true">{sym}</span>' if sym else ""


def _cloud_deco():
    """Soft decorative clouds behind the band text (inline SVG, no images)."""
    cloud = ('<g fill="#fff"><ellipse cx="50" cy="55" rx="40" ry="22"/><ellipse cx="85" cy="38" rx="34" ry="28"/>'
             '<ellipse cx="120" cy="56" rx="38" ry="21"/><rect x="50" y="52" width="70" height="25"/></g>')
    return ('<svg class="bb-cloud" style="right:-30px;top:-30px" viewBox="0 0 160 90" width="300" aria-hidden="true">%s</svg>'
            '<svg class="bb-cloud" style="right:260px;top:26px;opacity:.05" viewBox="0 0 160 90" width="170" aria-hidden="true">%s</svg>'
            '<svg class="bb-cloud" style="left:38%%;top:-18px;opacity:.04" viewBox="0 0 160 90" width="130" aria-hidden="true">%s</svg>' % (cloud, cloud, cloud))


def _brand_band(subtitle_html, pills=()):
    """The header band of the HTML pages: logo + helm + product name, subtitle, pills."""
    pill_html = "".join(f'<span class="bb-pill">{p}</span>' for p in pills)
    return ('<div class="brandband">%s<div class="bb-logo">%s%s</div><div class="bb-text"><div class="bb-title">%s</div>'
            '<div class="bb-sub">%s</div><div>%s</div></div></div>'
            % (_cloud_deco(), logo_svg(84), helm_svg(40, "#FFFFFF"), _html.escape(PRODUCT_NAME), subtitle_html, pill_html))


def icon_html_text(name):
    return ICONS.get(name, ("", ""))[0]


def _who(arn):
    """'arn:aws:sts::123456789012:assumed-role/Admin/me' -> 'Admin/me'."""
    return re.sub(r"^arn:[^:]*:(?:sts|iam)::\d+:(?:assumed-role|user|role)/", "", str(arn))


def _sec_label(sec):
    """The full-word title of a report section: from the registry when it is one of its sections, else its own heading."""
    sid = sec.get("sid")
    return SECTION_BY_ID[sid]["title"] if sid in SECTION_BY_ID else _short_title(sec.get("title", ""))


def _th(h):
    """A column header cell, with the 'What each column means' tooltip when the header name alone is not obvious."""
    full = full_header(h)
    tip = header_help(full)
    return '<th title="%s">%s</th>' % (_html.escape(tip, quote=True), _html.escape(full)) if tip else "<th>%s</th>" % _html.escape(full)


def _what(about, kind="table"):
    return ('<p class="what">What this %s shows: %s</p>' % (kind, _html.escape(about))) if about else ""


def _shows_box(shows, how=""):
    """The 'What this section shows' box under a section heading."""
    return ('<div class="shows"><p><b>What this section shows:</b> %s</p>%s</div>'
            % (_html.escape(shows), ('<p class="how"><b>How to use it:</b> %s</p>' % _html.escape(how)) if how else ""))


SEV_WORDS = {"CRIT": "Critical", "HIGH": "High", "MED": "Medium", "INFO": "Information"}


def _html_table(headers, rows, about=None, kind=""):
    esc = _html.escape
    head = "".join(_th(h) for h in headers)
    body = "".join("<tr>" + "".join("<td>%s</td>" % esc(str(c)) for c in r) + "</tr>" for r in rows)
    return ('<div class="tablewrap%s">%s<div class="tbtools"><input class="tfilter" type="search" placeholder="Filter rows...">'
            '<span class="tcount"></span><button class="csv" type="button">CSV</button></div>'
            '<div class="tscroll"><table class="data"><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div></div>' % (" glossary" if kind == "glossary" else "", _what(about), head, body))


def _legend_html():
    """The legend of the severity levels and of the check statuses (Health summary and glossary)."""
    sev = _html_table(["Severity", "Short form in the text report", "Meaning"], [[w, c, m] for w, c, m in SEVERITY_LEGEND],
                      about="the four severity levels used for findings and what each one means.")
    sta = _html_table(["Status", "Meaning"], [[w, m] for w, m in STATUS_LEGEND],
                      about="the four status words used by the checks (for example in the network section) and what each one means.", kind="")
    return ('<h3 class="bh">What the severity levels mean</h3>' + sev + '<h3 class="bh">What the status words mean</h3>' + sta)


def _glossary_rows(rep):
    """The complete glossary: every term printed by this report (de-duplicated, sorted)."""
    return [[t, GLOSSARY[t][0], GLOSSARY[t][1]] for t in sorted(set(rep.terms), key=str.lower) if t in GLOSSARY]


GLOSSARY_ABOUT = "every abbreviation and technical term used in this report, spelled out and explained in plain words, sorted alphabetically."


def _plain_table(headers, rows, about, maxw=170):
    """A table as text lines (same layout as the report's own tables)."""
    tmp = Report(lambda line: None)
    tmp.table(headers, rows, limit=1000, maxw=maxw, about=about)
    return tmp.lines


_ACRONYMS = ("AWS", "EKS", "CPU", "IAM", "EC2", "HPA", "PVC", "DNS", "API", "SSM")


def _short_title(title):
    """'3. NODES - STATUS, CPU, ...' -> 'Nodes'; 'CLUSTER OVERVIEW - prod-eks' -> 'Cluster overview'."""
    t = re.sub(r"^\d+\.\s*", "", title).split(" (")[0].split(" - ")[0].strip()
    if t.isupper():
        t = t.capitalize()
        for word in _ACRONYMS:
            t = re.sub(r"\b%s\b" % word, word, t, flags=re.I)
    return t


def render_html(label, ctx, rep, steps_log, raw_text, timing=None, plan=None, readonly=None, glossary=None):
    """The whole report as ONE self-contained interactive HTML page (collapsible sections,
    sortable/filterable tables, severity filters, global search, timeline filters, dark mode)."""
    esc = _html.escape
    findings = sorted(ctx.findings_full, key=lambda f: _SEV_ORDER.get(f[0], 9))
    counts = Counter(f[0] for f in findings)
    sec_titles = {s["id"]: _sec_label(s) for s in rep.sections}
    per_section = defaultdict(Counter)
    for sev, _, sid in findings:
        per_section[sid][sev] += 1

    # --- navigation
    nav = ['<a href="#summary" class="jump"><span>%sHealth summary</span></a>' % _icon_html("chart")]
    for s in rep.sections:
        if s["id"] == "s0":
            continue
        ico = _icon_html(SECTION_BY_ID[s["sid"]]["icon"]) if s.get("sid") in SECTION_BY_ID else ""
        if s.get("skipped"):                      # a section the user did not tick: still in the table of contents, greyed out
            nav.append('<span class="skiprow" title="Skipped by choice"><span>%s%s</span><small>skipped by choice</small></span>' % (ico, esc(_sec_label(s))))
            continue
        if not s["blocks"]:
            continue
        worst = next((sv for sv in ("CRIT", "HIGH", "MED", "INFO") if per_section[s["id"]][sv]), None)
        badge = f'<span class="badge b-{worst.lower()}">{sum(per_section[s["id"]].values())}</span>' if worst else ""
        nav.append('<a href="#%s" class="jump"><span>%s%s</span>%s</a>' % (s["id"], ico, esc(_sec_label(s)), badge))

    # extra table-of-contents entries for the parts that are not collection sections
    nav.append('<a href="#steps" class="jump"><span>%sCollection steps</span></a>' % _icon_html("list"))
    if timing:
        nav.append('<a href="#timing" class="jump"><span>%sTiming summary</span></a>' % _icon_html("speed"))
    if readonly:
        nav.append('<a href="#readonly" class="jump"><span>%sRead-only guarantee</span></a>' % _icon_html("key"))
    gl_rows = glossary if glossary is not None else _glossary_rows(rep)
    nav.append('<a href="#glossary" class="jump"><span>%sGlossary: all terms</span></a>' % _icon_html("search"))

    # --- summary
    cards = []
    for sev, cls in (("CRIT", "c-crit"), ("HIGH", "c-high"), ("MED", "c-med"), ("INFO", "c-info")):
        meaning = next(m for w, c, m in SEVERITY_LEGEND if c == sev)
        cards.append('<div class="sevcard %s" data-sev="%s" title="%s"><b>%d</b>%s</div>'
                     % (cls, sev, esc("%s: %s (click to show or hide)" % (SEV_WORDS[sev], meaning), quote=True), counts[sev], SEV_WORDS[sev]))
    rows = []
    for sev, text, sid in findings:
        where = '<a class="jump" href="#%s">%s</a>' % (sid, esc(sec_titles.get(sid, "")))
        rows.append('<tr data-sev="%s"><td><span class="sevtag badge b-%s">%s</span></td><td>%s</td><td>%s</td></tr>'
                    % (sev, sev.lower(), SEV_WORDS[sev], esc(text), where))
    if findings:
        summary_html = (f'<div class="cards">{"".join(cards)}</div><div class="tablewrap">'
                        + _what("every problem the tool found, one row per finding: how serious it is, what was found and the report section it came from.")
                        + f'<div class="tbtools"><span class="tcount"></span></div>'
                        f'<div class="tscroll"><table class="data" id="findings"><thead><tr>{_th("Severity")}{_th("Finding")}{_th("Report section")}</tr></thead>'
                        f'<tbody>{"".join(rows)}</tbody></table></div></div>')
        # the findings table has no per-table filter/CSV; give it the same hooks, hidden
        summary_html = summary_html.replace('<span class="tcount"></span>', '<span class="tcount"></span><input class="tfilter" style="display:none"><button class="csv" style="display:none">CSV</button>')
    else:
        summary_html = '<div class="cards"><div class="sevcard c-ok"><b>OK</b>No problems detected in the collected data</div></div>'

    skipped_secs = [x for x in rep.sections if x.get("skipped")]
    if skipped_secs:
        summary_html = ('<div class="secnote">%s <b>%d</b> section(s) skipped by choice: %s. The counters and findings below cover only the sections that were collected.</div>'
                        % (_icon_html("skip"), len(skipped_secs), ", ".join(esc(_sec_label(x)) for x in skipped_secs))) + summary_html
    contacts = contact_rows(ctx)
    if contacts:
        summary_html += ('<h3 class="bh">Teams to contact - namespaces with problems, grouped by support team contact (namespace label ' + esc(SUPPORT_LABEL) + ')</h3>'
                         + _html_table(["SUPPORT DL", "NAMESPACES", "ISSUES", "WHAT"], contacts,
                                       about="for every support team, the namespaces that have problems, how many issues they have and what the issues are, so you know whom to call."))
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
        if kind == "table":
            _, headers, trs, *extra = block
            what = extra[0] if extra else None
            tkind = extra[1] if len(extra) > 1 else ""
            head = "".join(_th(h) for h in headers)
            body = "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in trs)
            note = _what(what)
            return (f'<div class="tablewrap{" glossary" if tkind == "glossary" else ""}">{note}'
                    '<div class="tbtools"><input class="tfilter" type="search" placeholder="Filter rows...">'
                    '<span class="tcount"></span><button class="csv" type="button">CSV</button></div>'
                    f'<div class="tscroll"><table class="data"><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div></div>')
        if kind == "head":
            _, title, what = block
            return f'<h3 class="bh">{esc(title)}</h3>' + _what(what, "block")
        if kind == "check":
            _, title, status, why, advice = block
            cls = {"OK": "ok", "Warning": "warn", "Problem": "bad"}.get(status, "na")
            return (f'<div class="chk st-{cls}" data-status="{esc(status)}"><span class="stat st-{cls}">Status: {esc(status)}</span>'
                    f'<p><b>Why:</b> {esc(why)}</p><p><b>What this means / what to do next:</b> {esc(advice)}</p></div>')
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
            return _what(block[2] if len(block) > 2 else None, "block") + _util_block_html(block[1])
        if kind == "series":
            return _series_block_html(block[1], block[2], block[3], block[4] if len(block) > 4 else None)
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
            return _what(block[2] if len(block) > 2 else None, "block") + f'<div class="tlfilters">{chips}</div><ul class="timeline">{"".join(items)}</ul>'
        return ""

    sections_html = []
    for s in rep.sections:
        if s["id"] == "s0":
            continue
        ico = _ico_attr(SECTION_BY_ID[s["sid"]]["icon"]) if s.get("sid") in SECTION_BY_ID else ""
        if s.get("skipped"):
            sections_html.append(f'<div class="skipline" id="{s["id"]}">{_icon_html("skip")}Skipped by choice: {esc(_sec_label(s))}'
                                 + (f' <small>({esc(s["reason"])})</small>' if s.get("reason") else "") + "</div>")
            continue
        if not s["blocks"]:
            continue
        worst = next((sv for sv in ("CRIT", "HIGH", "MED", "INFO") if per_section[s["id"]][sv]), None)
        badge = f'<span class="badge b-{worst.lower()}">{sum(per_section[s["id"]].values())} finding(s)</span>' if worst else ""
        body = "".join(render_block(b) for b in s["blocks"])
        box = _shows_box(s["shows"], s.get("how", "")) if s.get("shows") else ""
        sections_html.append(f'<details class="sec" id="{s["id"]}" open><summary><span {ico}>{esc(s["title"])}</span>{badge}</summary><div class="secbody">{box}{body}</div></details>')

    def part(pid, icon_name, inner, is_open=False):
        info = REPORT_PARTS[pid]
        return (f'<details class="sec" id="{pid}"{" open" if is_open else ""}><summary><span {_ico_attr(icon_name)}>{esc(info["title"])}</span></summary>'
                f'<div class="secbody">{_shows_box(info["shows"], info["how"])}{inner}</div></details>')

    steps_html = part("steps", "list", _html_table(["Step", "Result", "Time taken"], [[t, st, sec] for t, st, sec in steps_log],
                                                   about="one row per collection step: whether it finished, failed or was skipped, and how long it took."))
    timing_part = part("timing", "speed", render_block(("lines", list(timing)))) if timing else ""
    readonly_part = part("readonly", "key", render_block(("lines", list(readonly)))) if readonly else ""
    glossary_part = part("glossary", "search", _html_table(["Term", "Full name", "Plain-language meaning"], gl_rows, about=GLOSSARY_ABOUT, kind="glossary") + _legend_html())

    meta = [f"Context: {ctx.meta.get('context', '?')}", f"Server: {ctx.meta.get('server', '?')}",
            f"Window: last {ctx.minutes} min", f"Generated: {ctx.now:%Y-%m-%d %H:%M:%S} UTC"]
    raw = raw_text.replace("</script", "<\\/script")
    n_all, n_got = len(SECTIONS), len(plan["collect"]) if plan else len([x for x in rep.sections if x.get("sid") and not x.get("skipped")])
    pills = [f"{icon_html_text('cloud')} <b>{esc(CLOUD_NAME)}</b>", f"{icon_html_text('helm')} cluster <b>{esc(label)}</b>", f"{icon_html_text('clock')} last <b>{ctx.minutes}</b> min",
             f"{icon_html_text('list')} <b>{n_got}</b> of {n_all} sections"]
    if ctx.meta.get("identity"):
        pills.append(f"{icon_html_text('key')} signed in as <b>{esc(_who(ctx.meta['identity']))}</b>")
    pills.append("&#128274; <b>read-only</b>")
    band = _brand_band(esc("Read-only debugging report: what happened in the last %d minutes" % ctx.minutes), pills)
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(REPORT_TITLE_PREFIX)} - {esc(label)}</title><style>{_HTML_CSS}{_UTIL_CSS}{_SERIES_CSS}{_brand_css()}</style></head><body>
{band}
<header><h1>{esc(REPORT_TITLE_PREFIX)} report - {esc(label)}</h1><div class="meta">{esc("  |  ".join(meta))}</div>
<div class="toolbar"><input id="q" type="search" placeholder="Search everything ( press / )"><span id="hits" class="small"></span>
<button id="expand" type="button">Expand all</button><button id="collapse" type="button">Collapse all</button>
<button id="theme" type="button">Dark / light</button><button id="print" type="button">Print</button><button id="dl" type="button">Download .txt</button></div></header>
<div class="layout"><nav>{"".join(nav)}</nav><main>
<details class="sec" id="summary" open><summary><span>Health summary</span><span class="badge b-info">{len(findings)} finding(s)</span></summary><div class="secbody">{_shows_box(REPORT_PARTS["summary"]["shows"], REPORT_PARTS["summary"]["how"])}{summary_html}</div></details>
{"".join(sections_html)}{steps_html}{timing_part}{readonly_part}{glossary_part}</main></div>
<footer>Generated by eks_debug.py - read-only data. Pod logs may contain sensitive information.</footer>
<script type="text/plain" id="rawtext">{raw}</script><script>{_HTML_JS}</script><script>{_UTIL_JS}</script><script>{_SERIES_JS}</script></body></html>"""
    return page


# ---------------------------------------------------------------------------
# Orchestration (step by step, with progress + cancel)
# ---------------------------------------------------------------------------

def run_steps(options=None):
    """The ordered collection steps: (key, title, optional_option_name_or_None). Built from the SECTIONS registry; 'data' (the shared
    cluster data every section reads) comes right after the overview. `options` is accepted for compatibility: use plan_sections() to
    see which of the steps a run collects."""
    out = []
    for sec in SECTIONS:
        legacy = next((o for o, sid in LEGACY_SWITCHES.items() if sid == sec["id"]), None)
        out.append((sec["id"], sec["step"], legacy))
        if sec["id"] == "overview":
            out.append(("data", "Collect cluster data", None))
    return out


def _resolve_workers(options):
    w = (options or {}).get("workers")
    try:
        w = PARALLEL_WORKERS if w is None else int(w)
    except (TypeError, ValueError):
        w = PARALLEL_WORKERS
    return max(1, w)


def _section_runners(ctx, label, options):
    """section id -> function(report). The same functions run for the real report and (hidden) for data another section needs."""
    return {
        "overview": lambda r: section_overview(r, ctx, label),
        "aws": lambda r: section_aws(r, ctx, label),
        "nodes": lambda r: section_nodes(r, ctx),
        "utilization": lambda r: section_utilization(r, ctx),
        "nodepods": lambda r: section_node_pods(r, ctx),
        "namespaces": lambda r: section_namespaces(r, ctx),
        "pods": lambda r: section_pods(r, ctx),
        "events": lambda r: section_events(r, ctx),
        "workloads": lambda r: section_workloads(r, ctx),
        "network": lambda r: section_network_details(r, ctx, label),
        "scaling": lambda r: section_scaling_storage_network(r, ctx),
        "top": lambda r: section_top(r, ctx),
        "logs": lambda r: section_logs(r, ctx, options),
        "timeline": lambda r: section_timeline(r, ctx),
    }


def _run_hidden(runner, ctx, rep):
    """Run a section that was NOT selected because a selected one needs its data: silently, on a throw-away report. Its findings, timeline
    entries and team contacts are removed again, so the summary and the counters reflect only the sections that were collected."""
    n_find, n_full, n_time = len(ctx.findings), len(ctx.findings_full), len(ctx.timeline)
    issues = {k: list(v) for k, v in ctx.ns_issues.items()}
    cb = ctx.on_finding
    quiet = Report(lambda line: None)
    ctx.on_finding, ctx.report = None, quiet
    try:
        runner(quiet)
    finally:
        del ctx.findings[n_find:]
        del ctx.findings_full[n_full:]
        del ctx.timeline[n_time:]
        ctx.ns_issues = defaultdict(list, issues)
        ctx.on_finding, ctx.report = cb, rep


def _await_ahead(fut, cancel, limit=None):
    """Wait until a run-ahead task is finished (or Stop was pressed; or `limit` seconds passed)."""
    if fut is None:
        return
    t0 = time.time()
    while True:
        try:
            fut.result(timeout=0.2)
            return
        except FutureTimeout:
            if (cancel is not None and cancel.is_set() and limit is None) or (limit is not None and time.time() - t0 > limit):
                return
        except Exception:
            return


def _start_run_ahead(rt, ctx, label, options, plan, note):
    """Start the parallel collection: the shared cluster data (so that the report's own 'data' step finds it ready) and, on a scratch copy of the
    context, the call-making sections (aws, network, logs). Returns (scratch context, {section id: future}, {section id: seconds})."""
    cancel = ctx.cancel
    sctx = Ctx(ctx.minutes)
    sctx.now, sctx.since = ctx.now, ctx.since           # the same window: the same CloudWatch / log arguments
    sctx.cancel, sctx.resources, sctx.usage = cancel, ctx.resources, ctx.usage
    sctx.warm = _Warm(cancel)
    srep = Report(lambda line: None)
    sctx.report = srep
    if "aws" not in plan["live"]:
        sctx.warm.set("aws_basics")
        sctx.warm.set("aws_net")
    secs = {}

    def prefetch():
        _TLS.warm = True
        hints = [["config", "current-context"], ["version", "-o", "json"], ["get", "--raw", "/readyz?verbose"]]
        order = ["nodes"] + [n for n in RESOURCES if n != "nodes"]
        hints += [[*RESOURCES[n], "-o", "json"] for n in order if n in plan["resources"]]
        if plan["usage"]:
            hints += [["top", "nodes", "--no-headers"], ["top", "pods", "-A", "--no-headers"]]
        for args in hints:
            rt.hint("k", _key_k(args), lambda a=args: rt.orig["k"](list(a), KUBECTL_TIMEOUT))
        if plan["usage"]:
            data, _err = kjson(RESOURCES["nodes"])
            for n in items(data):
                args = ["get", "--raw", f"/api/v1/nodes/{n['metadata']['name']}/proxy/stats/summary"]
                rt.hint("k", _key_k(args), lambda a=args: rt.orig["k"](list(a), 60))

    tasks = {
        "aws": lambda: section_aws(srep, sctx, label),
        "network": lambda: (warm_wait(sctx, "data"), section_network_details(srep, sctx, label)),
        "logs": lambda: (warm_wait(sctx, "data"), section_pods(srep, sctx), section_logs(srep, sctx, options)),
    }

    def wrap(sid, fn):
        def job():
            _TLS.warm = True
            t0 = time.time()
            try:
                fn()
            except Exception:
                pass                         # a run-ahead task only warms the cache: the report itself runs the section and reports any failure
            secs[sid] = time.time() - t0
        return job
    rt.pool.submit(prefetch)
    futures = {}
    for sid, fn in tasks.items():
        if sid in plan["live"]:
            futures[sid] = rt.pool.submit(wrap(sid, fn))
            note(sid, "running", None)
    return sctx, futures, secs


def _timing_lines(total, workers, step_times, rt):
    """The timing summary (end of the run and report footer): total + per step."""
    mode = f"parallel, {workers} workers" if rt is not None else "one after another (--workers 1)"
    lines = ["TIMING SUMMARY", f"  Total time: {total:.1f}s  ({mode})"]
    if rt is not None and rt.work_secs > 0:
        lines.append(f"  Work done: {rt.work_secs:.0f}s of kubectl / aws call time in {total:.1f}s - about {max(1.0, rt.work_secs / max(total, 0.1)):.1f}x faster than one after another "
                     f"({rt.executed} calls run, {rt.hits} answered from the cache, at most {rt.peak['k']} kubectl and {rt.peak['a']} aws at the same time"
                     + (f", {rt.throttled} throttled call(s) retried" if rt.throttled else "") + ")")
    for title, secs in step_times:
        lines.append(f"  {title:<48} {secs:>7.1f}s")
    return lines


def run_debug(label, minutes, emit, progress=None, cancel=None, options=None, on_finding=None):
    """Collect everything for the CURRENT kubectl context and write the .txt and the
    interactive .html report. `progress(key, status, seconds)` is called for every step
    (status: running / done / failed / skipped); `cancel` is a threading.Event - when it is
    set the remaining steps are skipped and a partial report is still written.
    options: aws / logs (older switches), sections (list of section ids, None = all), workers (parallel collection tasks, 1 = sequential),
    all_logs, log_namespaces, on_tasks (callback(done, total) for 'x of y collection tasks done').
    Returns the path of the HTML report."""
    options = {"aws": AWS_OPTS["enabled"], "logs": True, "all_logs": False, "log_namespaces": "", "sections": None, "workers": None, **(options or {})}
    AWS_OPTS["enabled"] = bool(options["aws"])
    plan = plan_sections(options)
    workers = _resolve_workers(options)
    ctx = Ctx(minutes)
    ctx.resources, ctx.usage = plan["resources"], plan["usage"]
    rep = Report(emit)
    ctx.report, ctx.on_finding, ctx.cancel = rep, on_finding, cancel
    note = progress or (lambda *a, **k: None)
    steps_log, step_times = [], []
    runners = _section_runners(ctx, label, options)
    t_run = time.time()
    ro0 = read_only_status()
    rt, saved, sctx, ahead, ahead_secs = None, None, None, {}, {}
    gl_rows = []
    if workers > 1:
        rt = _Runtime(workers, cancel)
        rt.on_tasks = options.get("on_tasks")
        saved = _install_runtime(rt)
        sctx, ahead, ahead_secs = _start_run_ahead(rt, ctx, label, options, plan, note)
    stopped = False
    try:
        for key, title, opt in run_steps(options):
            if stopped or (cancel is not None and cancel.is_set()):
                if not stopped:
                    rep.add("")
                    rep.add("Stopped by user - the remaining steps were skipped. The report below is partial.")
                    if rt is not None:
                        rt.shutdown()          # pending run-ahead calls are cancelled; only the few calls already running finish
                stopped = True
                note(key, "skipped", None)
                steps_log.append((title, "skipped (stopped)", "-"))
                continue
            if key != "data" and key not in plan["live"]:
                why = plan["skipped"][key]
                rep.skip(SECTION_BY_ID[key]["title"], None if why == "not selected" else why, sid=key)
                note(key, "skipped", None)
                steps_log.append((title, "skipped (turned off)" if why != "not selected" else "skipped (not selected)", "-"))
                continue
            hidden = key != "data" and key not in plan["collect"]
            note(key, "running", None)
            t0 = time.time()
            n_secs = len(rep.sections)
            try:
                _await_ahead(ahead.get(key), cancel)
                if key == "data":
                    try:
                        load_data(ctx, rep)
                    finally:
                        if sctx is not None:
                            sctx.data.update(ctx.data)         # the run-ahead tasks read the same (never modified) cluster data
                            sctx.warm.set("data")
                elif hidden:
                    needers = ", ".join(SECTION_BY_ID[x]["title"] for x in plan["hidden"][key])
                    _run_hidden(runners[key], ctx, rep)
                    rep.skip(SECTION_BY_ID[key]["title"], f"not selected; read in the background only because {needers} need(s) its data, so it is not shown", sid=key)
                else:
                    runners[key](rep)
                status = "done"
            except Exception as exc:  # one broken step must not lose the rest
                rep.add(f"[!] step '{title}' failed: {exc}")
                status = "failed"
                if key == "data":      # nothing else can work without the cluster data
                    note(key, status, time.time() - t0)
                    steps_log.append((title, f"failed: {exc}", f"{time.time() - t0:.1f}s"))
                    raise
            for sec in rep.sections[n_secs:]:
                sec.setdefault("sid", key if key != "data" else None)
            secs = time.time() - t0
            shown = max(secs, ahead_secs.get(key, 0.0))      # the time the step's own collection took (it ran ahead, next to the others)
            note(key, status, shown)
            steps_log.append((title + (" (read in the background, not shown)" if hidden else ""), status, f"{shown:.1f}s"))
            step_times.append((title, shown))

        total = time.time() - t_run
        _reads, blocked_now = read_only_status(ro0)
        if blocked_now:
            ctx.find("CRIT", f"READ-ONLY GUARD blocked {len(blocked_now)} attempted write command(s), e.g. {blocked_now[0][0]} - nothing was changed; please report this")
        summary = build_summary(ctx, label, skipped=[SECTION_BY_ID[k]["title"] for k in plan["skipped"]] + [SECTION_BY_ID[k]["title"] for k in plan["hidden"]])
        rep._text("")
        for line in summary:
            rep._text(line)          # text report only: the HTML has its own Health summary section
        raw_text = "\n".join(summary + [""] + rep.lines[: len(rep.lines) - len(summary) - 1])
        timing = _timing_lines(total, workers, step_times, rt)
        ro_lines = read_only_lines(ro0)
        raw_text += "\n\n" + "\n".join(timing) + "\n\n" + "\n".join(ro_lines)
        gl_rows = _glossary_rows(rep)
        raw_text += "\n\n" + "\n".join(["=" * 78, REPORT_PARTS["glossary"]["title"].upper(), "=" * 78, "What this section shows: " + REPORT_PARTS["glossary"]["shows"]]
                                        + _plain_table(["Term", "Full name", "Plain-language meaning"], gl_rows, GLOSSARY_ABOUT))
    finally:
        if sctx is not None:
            for ev in sctx.warm.events.values():
                ev.set()
        if rt is not None:
            if stopped:      # tasks that are still running only get 'cancelled' answers now and end at once; let them, before the real functions are back
                for fut in ahead.values():
                    _await_ahead(fut, cancel, limit=5.0)
            _uninstall_runtime(saved)
            rt.shutdown()

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
        f.write(render_html(label, ctx, rep, steps_log, raw_text, timing=timing, plan=plan, readonly=ro_lines, glossary=gl_rows))
    note("report", "done", None)
    emit("")
    for line in timing + [""] + ro_lines:
        emit(line)
    emit("")
    emit(f"Report saved to: {base}.txt")
    emit(f"Interactive HTML report: {base}.html")
    result = ReportPath(base + ".html")
    result.txt = base + ".txt"
    result.counts = Counter(f[0] for f in ctx.findings_full)
    result.findings = list(ctx.findings_full)
    result.section_titles = {sec["id"]: _sec_label(sec) for sec in rep.sections}
    result.partial = stopped
    result.unexplained = list(rep.unexplained)      # tables / blocks written without an explanation (must stay empty: the tests check it)
    result.meta = dict(ctx.meta)
    result.sections = [SECTION_BY_ID[k]["title"] for k in SECTION_BY_ID if k in plan["collect"]]
    result.skipped = [SECTION_BY_ID[k]["title"] for k in SECTION_BY_ID if k not in plan["collect"]]
    result.secs = total
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
    listed_by_cli = _lists_via_cli()
    tgt = CLI_TARGETS.get(str(cluster_number)) if listed_by_cli else None
    # the default method always connects with aws; ekslogin only for clusters its menu offers (the others, found by the aws listing, use aws eks update-kubeconfig)
    cli = LOGIN_OPTS["method"] == "cli" or bool(tgt and LOGIN_OPTS["method"] == "exe" and not tgt.get("exe_number"))
    ctx_label = label
    if tgt and not cli:
        ctx_label = tgt["name"]
    if not skip_login:
        note("login", "running", None)
        t0 = time.time()
        if cli:
            if LOGIN_OPTS["method"] == "exe":
                emit(f"Cluster {label} is not in the ekslogin menu - connecting with `aws eks update-kubeconfig` instead.")
            emit(f"Connecting to cluster {cluster_number} ({label}) with your existing AWS credentials (aws eks update-kubeconfig; no sign-in) ...")
            try:
                ctx = cli_login(cluster_number, label, emit)
            except RuntimeError:
                note("login", "failed", time.time() - t0)
                raise
            context = context or ctx
            tgt = CLI_TARGETS.get(str(cluster_number))
        else:
            exe_number = tgt["exe_number"] if tgt else cluster_number     # the aws listing numbers differ from the ekslogin menu numbers
            emit(f"Logging in to cluster {cluster_number} ({label}) with ekslogin" + (f" (menu entry {exe_number})" if tgt else "") + " ...")
            if not ekslogin(exe_number):
                note("login", "failed", time.time() - t0)
                raise RuntimeError(f"ekslogin failed for cluster {cluster_number}")
            emit("Re-reading ~/.aws (ekslogin may have created or refreshed profiles) ...")
        emit("Connected." if cli else "Login OK.")
        note("login", "done", time.time() - t0)
    else:
        note("login", "skipped", None)
        if tgt:
            ctx_label = tgt["name"]
    saved = {k: AWS_OPTS[k] for k in ("cluster", "region", "profile")}
    if tgt:   # the CLI listing already knows the cluster: hand it to the existing AWS profile step so nothing is guessed
        AWS_OPTS.update(cluster=tgt["name"], region=tgt["region"], profile=tgt.get("profile") or AWS_OPTS["profile"])
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
    sections = None       # titles of the sections that were collected
    skipped = None        # titles of the sections that were not
    unexplained = None    # tables / blocks of the report written without a 'What this table / block shows' text (always empty)
    secs = None
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
    """Run the whole debug for several clusters, ONE AFTER ANOTHER (the kubeconfig entry, the kubectl context
    and the AWS profile are shared state, so they must not overlap). Every cluster gets its own
    .txt and .html report; with more than one cluster a summary page linking them is written too.
    A failing cluster is recorded and the next one still runs. `cancel` stops after the current step
    of the current cluster, and the remaining clusters are marked 'not run'.
    Returns {"items": [per-cluster dicts], "index": path of the summary page or None}."""
    notify = on_cluster or (lambda *a, **k: None)
    n = len(selected)
    if n > 1 and context:
        emit("NOTE: --context applies to a single cluster and is ignored when several are selected.")
    entries, expired_profiles = [], set()
    for i, (number, label) in enumerate(selected, start=1):
        entry = {"number": number, "label": label, "status": "not run", "html": None, "txt": None,
                 "counts": Counter(), "findings": [], "titles": {}, "secs": None, "error": None, "sections": None, "skipped": None, "expired_profile": None}
        entries.append(entry)
        if cancel is not None and cancel.is_set():
            entry["status"] = "not run (stopped)"
            notify(i, n, label, entry["status"], entry)
            continue
        emit("")
        emit("#" * 78)
        emit(f"# CLUSTER {i} of {n}: {label}  (#{number})")
        emit("#" * 78)
        tprofile = (CLI_TARGETS.get(str(number)) or {}).get("profile")
        if tprofile and tprofile in expired_profiles:          # this account's credentials already turned out to be expired: don't try again
            entry.update(status="credentials expired", expired_profile=tprofile,
                         error=renew_message(tprofile))
            emit(f"SKIPPED {label}: {entry['error']}")
            notify(i, n, label, entry["status"], entry)
            continue
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
                         findings=getattr(result, "findings", []), titles=getattr(result, "section_titles", {}),
                         sections=getattr(result, "sections", None), skipped=getattr(result, "skipped", None))
        except Exception as exc:
            if isinstance(exc, CredentialsExpired) or is_expired_error(str(exc)):
                prof = getattr(exc, "profile", None) or tprofile or AWS_OPTS.get("profile") or os.environ.get("AWS_PROFILE") or "default"
                expired_profiles.add(prof)
                entry.update(status="credentials expired", expired_profile=prof,
                             error=renew_message(prof))
                emit(f"ERROR on cluster {label}: credentials for profile {prof or ENV_LABEL} are expired or missing - the other clusters continue.")
                emit(renew_message(prof))
            else:
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
        got, skipped = en.get("sections"), en.get("skipped")
        sec_cell = ('<span title="%s">%d of %d</span>' % (esc("Skipped by choice: " + ", ".join(skipped) if skipped else "all sections collected"), len(got), len(got) + len(skipped))
                    if got is not None and skipped is not None else "-")
        rows.append("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                    % (esc(en["label"]), esc(shown), c["CRIT"], c["HIGH"], c["MED"], c["INFO"],
                       ("%.0fs" % en["secs"]) if en["secs"] else "-", sec_cell, esc(top), report))
        for sev, text, sid in ordered:
            where = ('<a href="%s#%s">%s</a>' % (esc(link), esc(sid), esc(_short_title(en["titles"].get(sid, sid))))) if link else "-"
            finding_rows.append('<tr data-sev="%s"><td><span class="sevtag badge b-%s">%s</span></td><td>%s</td><td>%s</td><td>%s</td></tr>'
                                % (sev, sev.lower(), SEV_WORDS[sev], esc(en["label"]), esc(text), where))
        raw.append(f"== {en['label']}: {shown}  CRIT {c['CRIT']}  HIGH {c['HIGH']}  MED {c['MED']}  INFO {c['INFO']}"
                   + (f"  [{len(en['sections'])} of {len(en['sections']) + len(en['skipped'])} sections; skipped by choice: {', '.join(en['skipped'])}]" if en.get("skipped") else ""))
        raw += [f"   [{sev}] {text}" for sev, text, _ in ordered]
        if en["error"]:
            raw.append(f"   ERROR: {en['error']}")
    cards = "".join('<div class="sevcard %s" data-sev="%s" title="%s"><b>%d</b>%s</div>'
                    % (cls, sev, esc("%s: %s (click to show or hide)" % (SEV_WORDS[sev], next(m for w, c, m in SEVERITY_LEGEND if c == sev)), quote=True), total[sev], SEV_WORDS[sev])
                    for sev, cls in (("CRIT", "c-crit"), ("HIGH", "c-high"), ("MED", "c-med"), ("INFO", "c-info")))
    stamp = f"{datetime.now():%Y-%m-%d %H:%M:%S}"
    clusters_table = ('<div class="tablewrap">%s<div class="tbtools"><input class="tfilter" type="search" placeholder="Filter clusters...">'
                      '<span class="tcount"></span><button class="csv" type="button">CSV</button></div><div class="tscroll">'
                      '<table class="data"><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div></div>'
                      % (_what("one row per cluster: whether its report was written, how many findings of each severity it has, how long it took, how many report "
                               "sections were collected, its most serious finding and a link to its full report."),
                         "".join(_th(h) for h in ("Cluster", "Status", "Critical", "High", "Medium", "Information", "Time taken", "Sections collected", "Top finding", "Report")),
                         "".join(rows)))
    findings_table = ('<div class="tablewrap">%s<div class="tbtools"><span class="tcount"></span><input class="tfilter" style="display:none">'
                      '<button class="csv" style="display:none">CSV</button></div><div class="tscroll"><table class="data" id="findings"><thead>'
                      '<tr>%s</tr></thead><tbody>%s</tbody></table></div></div>'
                      % (_what("every finding of every cluster, one row each: how serious it is, which cluster, what was found and the report section it came from."),
                         "".join(_th(h) for h in ("Severity", "Cluster", "Finding", "Report section")),
                         "".join(finding_rows) or '<tr data-sev="INFO"><td></td><td></td><td>No findings</td><td></td></tr>'))
    raw_text = "\n".join([f"EKS DEBUG - {len(entries)} clusters, last {minutes} min, {stamp}", ""] + raw).replace("</script", "<\\/script")
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(REPORT_TITLE_PREFIX)} - {len(entries)} clusters</title><style>{_HTML_CSS}{_brand_css()}</style></head><body>
{_brand_band(esc("Summary of " + str(len(entries)) + " clusters"), [icon_html_text("cloud") + " <b>" + esc(CLOUD_NAME) + "</b>", icon_html_text("clock") + " last <b>" + str(minutes) + "</b> min", icon_html_text("helm") + " <b>" + str(len(entries)) + "</b> clusters"])}
<header><h1>{esc(REPORT_TITLE_PREFIX)} - summary of {len(entries)} clusters</h1><div class="meta">Window: last {minutes} min  |  Generated: {stamp}</div>
<div class="toolbar"><input id="q" type="search" placeholder="Search everything ( press / )"><span id="hits" class="small"></span>
<button id="expand" type="button">Expand all</button><button id="collapse" type="button">Collapse all</button>
<button id="theme" type="button">Dark / light</button><button id="print" type="button">Print</button><button id="dl" type="button">Download .txt</button></div></header>
<div class="layout"><nav><a href="#clusters" class="jump"><span>{_icon_html("helm")}Clusters</span></a><a href="#allfindings" class="jump"><span>{_icon_html("bell")}All findings</span></a></nav><main>
<details class="sec" id="clusters" open><summary><span {_ico_attr("helm")}>Clusters</span><span class="badge b-info">{len(entries)}</span></summary><div class="secbody">{_shows_box(REPORT_PARTS["clusters"]["shows"], REPORT_PARTS["clusters"]["how"])}{clusters_table}</div></details>
<details class="sec" id="allfindings" open><summary><span {_ico_attr("bell")}>All findings (every cluster)</span><span class="badge b-info">{sum(total.values())}</span></summary><div class="secbody">{_shows_box(REPORT_PARTS["allfindings"]["shows"], REPORT_PARTS["allfindings"]["how"])}<div class="cards">{cards}</div>{findings_table}{_legend_html()}</div></details>
</main></div><footer>Generated by eks_debug.py - read-only data. The per-cluster reports are the files linked above (keep them in the same folder).</footer>
<script type="text/plain" id="rawtext">{raw_text}</script><script>{_HTML_JS}</script></body></html>"""


# ---------------------------------------------------------------------------
# GUI: live, interactive collection
# ---------------------------------------------------------------------------

_GUI = {}   # widgets of the running window (used by the tests)
_SESSION = {"sections": None, "profile": None, "profiles": None, "regions": None}   # the sections ticked in the window and the last chosen AWS profile: remembered for the session


def run_gui(default_minutes, skip_login=False, context=None, sections=None):
    import pathlib
    import tkinter as tk
    import webbrowser
    from tkinter import ttk, scrolledtext, messagebox

    root = tk.Tk()
    root.title(PRODUCT_NAME)
    root.geometry("1280x820")
    root.minsize(980, 620)
    msgs = queue.Queue()
    state = {"busy": False, "cancel": None, "html": None, "t0": None, "finished": 0, "total": 1, "term": set(),
             "counts": Counter(), "clusters": {}, "reports": {}, "n": 1,
             "accounts": [], "acct_by_id": {}, "acct_chosen": set(), "acct_loading": False, "accounts_loaded": False, "pre": False,
             "acct_status": {}, "status_checking": False, "status_cancel": None, "status_gen": 0, "profiles_info": {},
             "crows": [], "by_key": {}, "cchosen": set(), "listing": False, "list_cancel": None, "listed": set(),
             "list_done": 0, "list_total": 0, "rebuild": False, "said": [], "src_user": False, "fell_back": False,
             "scopes": set(), "list_scope": None, "list_t0": None}
    COLORS = {"ok": "#067647", "err": "#c00000", "warn": "#9a7d0a", "info": "#1f4e79", "dim": "#777777"}
    ACCOUNT0 = AWS_OPTS["profile"]   # the profile given on the command line, if any
    try:                    # symbols above U+FFFF (the magnifier, the severity dots ...) need a Tk that can show them
        tk.Label(root, text="\U0001F50D\U0001F534☁").destroy()
        _ICON_PLAIN[0] = float(tk.TkVersion) < 8.6
        SEARCH_ICON = "\U0001F50D"
    except Exception:
        _ICON_PLAIN[0] = True
        SEARCH_ICON = "Find:"
    # status symbols of the live steps list (plain text when the symbols cannot be drawn)
    ICON = ({"pending": "o", "running": ">>", "done": "OK", "failed": "FAILED", "skipped": "-"} if _ICON_PLAIN[0] else
            {"pending": icon("pending"), "running": icon("run"), "done": icon("ok"), "failed": icon("fail"), "skipped": icon("skip")})
    SEV_ICON = {"CRIT": icon("crit"), "HIGH": icon("high"), "MED": icon("med"), "INFO": icon("info")}

    # ---- look: AWS navy + orange, Segoe UI, flat cards with a coloured stripe
    from tkinter import font as tkfont
    try:
        FAM = "Segoe UI" if "Segoe UI" in set(tkfont.families(root)) else tkfont.nametofont("TkDefaultFont").actual("family")
    except Exception:
        FAM = "TkDefaultFont"
    BG, CARD, LINE, STRIPE_B = "#F4F6F9", "#FFFFFF", "#D5DBE3", "#F6F8FB"
    NAVY, ORANGE = BRAND_PRIMARY, BRAND_ACCENT
    root.configure(bg=BG)
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass
    style.configure(".", font=(FAM, 10), background=BG, foreground=BRAND_TEXT)
    style.configure("TFrame", background=BG)
    style.configure("TLabel", background=BG)
    style.configure("TCheckbutton", background=BG)
    style.configure("TRadiobutton", background=BG)
    style.map("TCheckbutton", background=[("active", "#FFF4DE")])
    style.map("TRadiobutton", background=[("active", "#FFF4DE")])
    style.configure("TLabelframe", background=BG, bordercolor=LINE, relief="solid", borderwidth=1)
    style.configure("TLabelframe.Label", background=BG, foreground=NAVY, font=(FAM, 10, "bold"))
    style.configure("TButton", padding=(10, 4), background="#FFFFFF", bordercolor=LINE, relief="flat")
    style.map("TButton", background=[("active", "#FFF4DE"), ("disabled", "#EEF1F5")], foreground=[("disabled", "#9AA4B2")])
    style.configure("Accent.TButton", background=ORANGE, foreground=NAVY, font=(FAM, 10, "bold"), padding=(14, 5), bordercolor="#E08700")
    style.map("Accent.TButton", background=[("pressed", "#C45500"), ("active", "#EC7211"), ("disabled", "#F1DDBD")], foreground=[("disabled", "#8A7A5E")])
    style.configure("Stop.TButton", foreground="#B42318", font=(FAM, 10, "bold"))
    style.map("Stop.TButton", foreground=[("disabled", "#9AA4B2")])
    style.configure("Treeview", rowheight=24, background=CARD, fieldbackground=CARD, bordercolor=LINE, font=(FAM, 10))
    style.configure("Treeview.Heading", background=NAVY, foreground="#FFFFFF", font=(FAM, 10, "bold"), relief="flat", padding=(6, 4))
    style.map("Treeview.Heading", background=[("active", "#2F3E52")])
    style.map("Treeview", background=[("selected", "#FFE2B0")], foreground=[("selected", NAVY)])
    style.configure("Horizontal.TProgressbar", troughcolor="#E3E7ED", background=ORANGE, lightcolor=ORANGE, darkcolor=ORANGE, bordercolor=LINE)
    style.configure("TNotebook", background=BG, borderwidth=0)
    style.configure("TNotebook.Tab", padding=(16, 7), background="#E4E8EE", foreground=NAVY, font=(FAM, 10, "bold"))
    style.map("TNotebook.Tab", background=[("selected", CARD), ("active", "#FFF4DE")], foreground=[("selected", NAVY)])
    style.configure("Footer.TFrame", background=NAVY)
    style.configure("Footer.TLabel", background=NAVY, foreground="#FFFFFF")
    style.configure("Footer.TButton", padding=(10, 3))
    style.configure("Note.TLabel", foreground="#6B7685")
    style.configure("Desc.TLabel", foreground="#6B7685", font=(FAM, 9))

    def card(parent, text, stripe=None, padding=6, **pack):
        """A card: a labelled frame with a coloured stripe on its left edge. Returns the labelled frame; `pack` places the whole card."""
        outer = tk.Frame(parent, bg=BG)
        tk.Frame(outer, width=4, bg=stripe or ORANGE).pack(side="left", fill="y")
        lf = ttk.LabelFrame(outer, text=text, padding=padding)
        lf.pack(side="left", fill="both", expand=True)
        outer.pack(**pack)
        lf.outer = outer
        return lf

    def stripe_tree(tree):
        """Alternate row colours (tags 'odd' / 'even') without touching the status tags."""
        for n, iid in enumerate(tree.get_children()):
            tags = [t for t in tree.item(iid, "tags") if t not in ("odd", "even")]
            tree.item(iid, tags=tuple(tags) + (("odd", "even")[n % 2],))

    def striped(tree):
        tree.tag_configure("odd", background=CARD)
        tree.tag_configure("even", background=STRIPE_B)
        return tree

    # ---- banner: logo, helm, product name, subtitle (window, signed-in identity, profile count)
    banner_sub = tk.StringVar(value="")
    banner = tk.Canvas(root, height=90, bg=NAVY, highlightthickness=0)
    banner.pack(fill="x")

    def draw_banner(_event=None):
        banner.delete("all")
        w = max(banner.winfo_width(), 700)
        for cx, cy, sc in ((w - 90, 34, 1.3), (w - 330, 68, 0.9), (w * 0.52, 12, 0.7), (w - 520, 22, 0.55)):      # soft decorative clouds (a shade lighter than the band)
            for ox, oy, rx, ry in ((-26, 8, 26, 14), (0, -2, 30, 22), (30, 6, 28, 15), (4, 12, 40, 10)):
                banner.create_oval(cx + (ox - rx) * sc, cy + (oy - ry) * sc, cx + (ox + rx) * sc, cy + (oy + ry) * sc, fill="#2B3A4E", outline="")
        draw_logo(banner, 16, 10, 1.05)
        draw_helm(banner, 150, 45, 20, "#FFFFFF")
        banner.create_text(190, 33, text=PRODUCT_NAME, anchor="w", fill="#FFFFFF", font=(FAM, 18, "bold"))
        banner.create_text(190, 62, text=banner_sub.get(), anchor="w", fill="#C9D3E0", font=(FAM, 10))
        banner.create_rectangle(0, 86, w, 90, fill=ORANGE, outline="")
    banner.bind("<Configure>", draw_banner)

    # ---- action bar, status bar and the tabs (the status bar is packed first so that it is always visible)
    top = ttk.Frame(root, padding=(10, 8, 10, 4))
    top.pack(fill="x")
    bottom = ttk.Frame(root, style="Footer.TFrame", padding=(10, 6))
    bottom.pack(side="bottom", fill="x")
    nb = ttk.Notebook(root)
    nb.pack(fill="both", expand=True, padx=8, pady=(2, 0))
    tab_clusters = ttk.Frame(nb, padding=(2, 6, 2, 2))
    tab_collect = ttk.Frame(nb, padding=(2, 6, 2, 2))
    tab_run = ttk.Frame(nb, padding=(2, 6, 2, 2))
    nb.add(tab_clusters, text=f"{icon('key')} 1  Choose profiles and clusters")
    nb.add(tab_collect, text=f"{icon('list')} 2  What to collect")
    nb.add(tab_run, text=f"{icon('run')} 3  Run and results")

    def numeric(k):
        return int(k) if str(k).isdigit() else 10**9

    def count_of(n, noun):
        return f"{n} {noun}" + ("" if n == 1 else "s")

    # ---- tab 1: step 1 (AWS profiles + credentials status), step 2 (regions + collect), step 3 (clusters), with a message line at the bottom
    guide = ttk.Frame(tab_clusters, padding=(2, 0, 2, 0))
    guide.pack(fill="both", expand=True)
    guide_msg = tk.Label(guide, text="", anchor="w", justify="left", font=("Segoe UI", 10, "bold"), padx=8, pady=4,
                         bg="#eef3f8", fg=COLORS["info"])
    guide_msg.pack(side="bottom", fill="x", pady=(6, 0))
    guide_msg.bind("<Configure>", lambda e: guide_msg.configure(wraplength=max(300, e.width - 20)))
    cols = ttk.Frame(guide)
    cols.pack(fill="both", expand=True)
    col_l = ttk.Frame(cols)
    col_l.pack(side="left", fill="both", expand=True)
    col_r = ttk.Frame(cols)
    col_r.pack(side="left", fill="both", expand=True, padx=(8, 0))

    def say(text, kind="info"):
        """The message line at the bottom of tab 1: what happened and what to do next."""
        guide_msg.configure(text=text, fg=COLORS.get(kind, COLORS["info"]))
        state["said"] = (state["said"] + [text])[-60:]       # the last messages (the line above shows only the newest)

    def search_box(parent, var, width=30):
        box = ttk.Frame(parent)
        ttk.Label(box, text=SEARCH_ICON).pack(side="left")
        entry = ttk.Entry(box, textvariable=var, width=width)
        entry.pack(side="left", fill="x", expand=True, padx=4)
        clear = ttk.Button(box, text="✕", width=3, command=lambda: var.set(""))
        clear.pack(side="left")
        return box, entry, clear

    # ---- step 1: the AWS profiles of ~/.aws with a credentials status chip (the parts below the list are packed first, from the bottom, so they are never clipped)
    s3 = card(col_l, f"{icon('key')} Step 1 - Choose AWS profile(s)", fill="both", expand=True)
    uses_note = ttk.Label(s3, text="Uses your existing AWS credentials from ~/.aws - no sign-in in this tool.",
                          wraplength=520, justify="left", foreground="#067647", font=(FAM, 9, "bold"))
    uses_note.pack(side="top", anchor="w", pady=(0, 4))
    acct_filter = tk.StringVar(value="")
    box3, acct_search, acct_search_x = search_box(s3, acct_filter)
    box3.pack(side="top", fill="x")
    acct_count = tk.StringVar(value="")
    acct_sel = tk.StringVar(value="0 selected")
    _cnt_row = ttk.Frame(s3)
    _cnt_row.pack(side="top", fill="x")
    ttk.Label(_cnt_row, textvariable=acct_count).pack(side="left")
    ttk.Label(_cnt_row, textvariable=acct_sel, font=(FAM, 9, "bold")).pack(side="left", padx=(12, 0))
    ro_note = ttk.Label(s3, text="Read-only: nothing is installed, created, changed or deleted on the cluster or in the cloud account. Local writes: reports "
                                 "and the kubeconfig entry / context only.",
                        wraplength=520, justify="left", foreground="#067647", font=(FAM, 9, "bold"))
    ro_note.pack(side="bottom", anchor="w", pady=(4, 0))
    # how to connect to a cluster: the existing credentials (default) or ekslogin first
    conn = ttk.Frame(s3)
    conn.pack(side="bottom", fill="x", pady=(6, 0))
    ttk.Label(conn, text="How to connect to a cluster:", font=(FAM, 10, "bold")).pack(anchor="w")
    method_combo = ttk.Combobox(conn, width=58, state="readonly", values=[LOGIN_LABELS["cli"], LOGIN_LABELS["exe"]])
    method_combo.set(LOGIN_LABELS[LOGIN_OPTS["method"]])
    method_combo.pack(anchor="w", pady=(2, 0))
    method_info = tk.StringVar(value="")
    ttk.Label(conn, textvariable=method_info, wraplength=520, justify="left", style="Desc.TLabel").pack(anchor="w", pady=(2, 0))
    # the red message for a selected profile whose credentials are expired or missing: text only (the tool never signs in, never runs the command)
    renew_box = tk.Frame(s3, bg="#FDE7E7", highlightbackground="#B42318", highlightthickness=1)
    renew_var = tk.StringVar(value="")
    renew_lbl = tk.Label(renew_box, textvariable=renew_var, anchor="w", justify="left", wraplength=380, font=(FAM, 9, "bold"), fg="#B42318", bg="#FDE7E7", padx=8, pady=4)
    renew_copy_btn = ttk.Button(renew_box, text="Copy example command")
    renew_copy_btn.pack(side="right", padx=8, pady=4)
    renew_lbl.pack(side="left", fill="x", expand=True)
    acct_status = tk.StringVar(value="")
    acct_status_lbl = ttk.Label(s3, textvariable=acct_status, wraplength=520, justify="left", style="Desc.TLabel")
    acct_status_lbl.pack(side="bottom", anchor="w", pady=(4, 0))
    a_btns = ttk.Frame(s3)
    a_btns.pack(side="bottom", fill="x", pady=(4, 0))
    acct_all_btn = ttk.Button(a_btns, text="Select all (shown)")
    acct_all_btn.pack(side="left")
    acct_clear_btn = ttk.Button(a_btns, text="Clear")
    acct_clear_btn.pack(side="left", padx=4)
    check_btn = ttk.Button(a_btns, text=f"{icon('reload')} Check credentials", style="Accent.TButton")
    check_btn.pack(side="left", padx=(8, 0))
    recheck_all_btn = ttk.Button(a_btns, text="Re-check all")
    recheck_all_btn.pack(side="left", padx=4)
    acct_reload_btn = ttk.Button(a_btns, text=f"{icon('reload')} Reload profiles")
    acct_reload_btn.pack(side="left")
    a_wrap = ttk.Frame(s3)
    a_wrap.pack(side="top", fill="both", expand=True)
    acct_tree = striped(ttk.Treeview(a_wrap, columns=("status", "account", "role", "region"), show="tree headings", selectmode="extended", height=6))
    acct_tree.heading("#0", text="Profile")
    acct_tree.heading("status", text="Credentials status")
    acct_tree.heading("account", text="Account id")
    acct_tree.heading("role", text="Role / SSO")
    acct_tree.heading("region", text="Region")
    acct_tree.column("#0", width=120)
    acct_tree.column("status", width=190)
    acct_tree.column("account", width=95)
    acct_tree.column("role", width=100)
    acct_tree.column("region", width=75)
    a_scroll = ttk.Scrollbar(a_wrap, orient="vertical", command=acct_tree.yview)
    acct_tree.configure(yscrollcommand=a_scroll.set)
    a_scroll.pack(side="right", fill="y")
    acct_tree.pack(side="left", fill="both", expand=True)
    acct_tree.tag_configure("hint", foreground="#888888")
    for _tag, _col in (("st_active", COLORS["ok"]), ("st_expiring", "#B26A00"), ("st_expired", COLORS["err"]), ("st_none", "#6B7685"), ("st_unknown", "#6B7685")):
        acct_tree.tag_configure(_tag, foreground=_col)

    # ---- step 2: regions + collect clusters (on demand)
    s2 = card(col_r, f"{icon('cloud')} Step 2 - Choose regions and collect clusters", stripe=NAVY, fill="x")
    source_var = tk.StringVar(value="all")
    src_row = ttk.Frame(s2)
    src_row.pack(fill="x")
    ttk.Label(src_row, text="Cluster list:").pack(side="left", anchor="n")
    src_col = ttk.Frame(src_row)
    src_col.pack(side="left", padx=(6, 0))
    source_all_rb = ttk.Radiobutton(src_col, text=SOURCE_LABELS["all"], value="all", variable=source_var)
    source_all_rb.pack(anchor="w")
    source_menu_rb = ttk.Radiobutton(src_col, text=SOURCE_LABELS["menu"], value="menu", variable=source_var)
    source_menu_rb.pack(anchor="w")
    region0 = AWS_OPTS["region"]
    _reg_start = _SESSION.get("regions") if _SESSION.get("regions") is not None else (region0 or "")
    region_var = tk.StringVar(value=_reg_start)
    region_row = ttk.Frame(s2)
    region_row.pack(fill="x", pady=(4, 0))
    ttk.Label(region_row, text="Region(s):").pack(side="left")
    region_entry = ttk.Entry(region_row, textvariable=region_var, width=24)
    region_entry.pack(side="left", padx=4)
    allreg_var = tk.BooleanVar(value=_reg_start.strip().lower() == "all")
    allreg_chk = ttk.Checkbutton(region_row, text="All regions", variable=allreg_var)
    allreg_chk.pack(side="left", padx=(0, 6))
    region_hint = tk.StringVar(value="comma separated; blank = each selected profile's own region")
    ttk.Label(s2, textvariable=region_hint, style="Desc.TLabel").pack(anchor="w")
    est_var = tk.StringVar(value="")
    ttk.Label(s2, textvariable=est_var, font=(FAM, 9, "bold")).pack(anchor="w", pady=(2, 0))
    collect_row = ttk.Frame(s2)
    collect_row.pack(fill="x", pady=(4, 0))
    collect_btn = ttk.Button(collect_row, text="Collect clusters from selected profiles", style="Accent.TButton")
    collect_btn.pack(side="left")
    refresh_sel_btn = ttk.Button(collect_row, text=f"{icon('reload')} Refresh selected")
    refresh_sel_btn.pack(side="left", padx=(6, 0))
    list_stop_btn = ttk.Button(collect_row, text="Stop", state="disabled")
    list_stop_btn.pack(side="left", padx=(6, 0))
    cl_status = tk.StringVar(value="")
    ttk.Label(s2, textvariable=cl_status, wraplength=520, justify="left").pack(anchor="w", pady=(4, 0))
    list_bar = ttk.Progressbar(s2, mode="determinate", length=300)
    list_bar.pack(anchor="w", pady=(2, 0))

    # ---- step 3: the clusters (searchable multi-select list)
    s4 = card(col_r, f"{icon('helm')} Step 3 - Choose clusters  (Ctrl/Shift-click for several)", fill="both", expand=True, pady=(6, 0))
    filter_var = tk.StringVar(value="")
    box4, cl_search, cl_search_x = search_box(s4, filter_var)
    box4.pack(fill="x")
    cl_count = tk.StringVar(value="")
    ttk.Label(s4, textvariable=cl_count).pack(anchor="w")
    scope_var = tk.StringVar(value="all")                   # which collected clusters are SHOWN (all / only the selected profiles); collecting always needs a selection
    scope_row = ttk.Frame(s4)
    scope_row.pack(fill="x")
    scope_all_rb = ttk.Radiobutton(scope_row, text="", value="all", variable=scope_var)
    scope_all_rb.pack(side="left")
    scope_sel_rb = ttk.Radiobutton(scope_row, text="", value="sel", variable=scope_var)
    scope_sel_rb.pack(side="left", padx=(12, 0))
    c_wrap = ttk.Frame(s4)
    c_wrap.pack(fill="both", expand=True)
    cluster_tree = striped(ttk.Treeview(c_wrap, columns=("acct", "where", "via"), show="tree headings", selectmode="extended", height=7))
    cluster_tree.heading("#0", text="Cluster")
    cluster_tree.heading("acct", text="Account / Profile")
    cluster_tree.heading("where", text="Region")
    cluster_tree.heading("via", text="Connect with")
    cluster_tree.column("#0", width=220)
    cluster_tree.column("acct", width=170)
    cluster_tree.column("where", width=100)
    cluster_tree.column("via", width=150)
    c_scroll = ttk.Scrollbar(c_wrap, orient="vertical", command=cluster_tree.yview)
    cluster_tree.configure(yscrollcommand=c_scroll.set)
    c_scroll.pack(side="right", fill="y")
    cluster_tree.pack(side="left", fill="both", expand=True)
    cluster_tree.tag_configure("hint", foreground="#888888")
    c_btns = ttk.Frame(s4)
    c_btns.pack(fill="x", pady=(4, 0))
    select_all_btn = ttk.Button(c_btns, text="Select all (shown)")
    select_all_btn.pack(side="left")
    clear_btn = ttk.Button(c_btns, text="Clear")
    clear_btn.pack(side="left", padx=4)
    refresh_btn = ttk.Button(c_btns, text=f"{icon('reload')} Reload list")
    refresh_btn.pack(side="left")
    manual_var = tk.StringVar(value="")
    ttk.Label(c_btns, text="  or type numbers:").pack(side="left")
    manual_entry = ttk.Entry(c_btns, textvariable=manual_var, width=14)
    manual_entry.pack(side="left", padx=4)
    ttk.Label(c_btns, text="e.g. 1,3,5  2-4  all", style="Desc.TLabel").pack(side="left")
    sel_text = tk.StringVar(value="Selected: none")
    ttk.Label(s4, textvariable=sel_text, wraplength=520, justify="left").pack(anchor="w", pady=(4, 0))

    # ---- action bar: window, run / stop, run state
    ttk.Label(top, text="Last (minutes):").pack(side="left")
    minutes_var = tk.StringVar(value=str(default_minutes))
    ttk.Spinbox(top, from_=1, to=1440, width=6, textvariable=minutes_var).pack(side="left", padx=(4, 0))
    initial = set(sections) if sections is not None else (set(_SESSION["sections"]) if _SESSION.get("sections") else set(SECTION_BY_ID))
    initial |= {s["id"] for s in SECTIONS if s.get("locked")}
    aws_var = tk.BooleanVar(value=AWS_OPTS["enabled"] and "aws" in initial)        # the SAME variable as the box of the 'aws' section below
    logs_var = tk.BooleanVar(value="logs" in initial)                              # ... and of the 'logs' section
    open_var = tk.BooleanVar(value=True)
    alllogs_var = tk.BooleanVar(value=False)
    ns_var = tk.StringVar(value="")
    run_btn = ttk.Button(top, text=f"{icon('run')} Login & Debug selected cluster(s)", style="Accent.TButton")
    run_btn.pack(side="left", padx=(16, 4))
    stop_btn = ttk.Button(top, text=f"{icon('stop')} Stop", state="disabled", style="Stop.TButton")
    stop_btn.pack(side="left")
    ttk.Checkbutton(top, text="Open report when done", variable=open_var).pack(side="left", padx=(16, 0))
    run_chip = tk.Label(top, text="Ready", bg=COLORS["dim"], fg="white", padx=12, pady=2, font=(FAM, 9, "bold"))
    run_chip.pack(side="right")
    sec_count = tk.StringVar(value="")
    sec_chip = tk.Label(top, textvariable=sec_count, bg="#FFF4DE", fg=NAVY, padx=10, pady=2, font=(FAM, 9, "bold"), relief="flat")
    sec_chip.pack(side="right", padx=(0, 8))

    def set_run_chip(kind, text):
        run_chip.configure(text=text, bg={"run": "#1f4e79", "ok": COLORS["ok"], "err": COLORS["err"], "warn": "#B26A00"}.get(kind, COLORS["dim"]))

    # ---- tab 2: what to collect - one box per report section (the registry drives them), quick presets, a counter
    section_vars = {}
    section_checks = {}
    section_widgets = []
    SEC_IDS = [s["id"] for s in SECTIONS]
    sec_card = card(tab_collect, f"{icon('list')} What to collect", fill="both", expand=True)
    sec_head = ttk.Frame(sec_card)
    sec_head.pack(fill="x")
    ttk.Label(sec_head, textvariable=sec_count, font=(FAM, 11, "bold"), foreground=NAVY).pack(side="left")
    sec_all_btn = ttk.Button(sec_head, text="Select all")
    sec_none_btn = ttk.Button(sec_head, text="Clear all")
    sec_net_btn = ttk.Button(sec_head, text=f"{icon('net')} Only networking")
    sec_nonet_btn = ttk.Button(sec_head, text="Everything except networking")
    for b in (sec_nonet_btn, sec_net_btn, sec_none_btn, sec_all_btn):
        b.pack(side="right", padx=(4, 0))
        section_widgets.append(b)
    ttk.Label(sec_card, text="Tick what the report should contain. Unticked sections are never collected: no command is run for them. "
                             "Sections that need another section's data read it silently (see the note below).", style="Note.TLabel", wraplength=900, justify="left").pack(fill="x", pady=(6, 4))
    sec_grid = ttk.Frame(sec_card)
    sec_grid.pack(fill="both", expand=True)
    sec_grid.columnconfigure(0, weight=1, uniform="sec")
    sec_grid.columnconfigure(1, weight=1, uniform="sec")
    rows_per_col = (len(SECTIONS) + 1) // 2
    for i, sec in enumerate(SECTIONS):
        sid = sec["id"]
        var = aws_var if sid == "aws" else (logs_var if sid == "logs" else tk.BooleanVar(value=sid in initial))
        section_vars[sid] = var
        cell = ttk.Frame(sec_grid, padding=(4, 2, 12, 5))
        cell.grid(row=i % rows_per_col, column=i // rows_per_col, sticky="nsew")
        cb = ttk.Checkbutton(cell, text=f"{icon(sec['icon'])}  {sec['title']}", variable=var)
        cb.pack(anchor="w")
        desc = ttk.Label(cell, text=sec["desc"], style="Desc.TLabel", justify="left", wraplength=420)
        desc.pack(anchor="w", padx=(24, 0))
        cell.bind("<Configure>", lambda e, d=desc: d.configure(wraplength=max(200, e.width - 48)))
        section_checks[sid] = cb
        if sec.get("locked"):
            var.set(True)
            cb.state(["disabled"])
        else:
            section_widgets.append(cb)
    sec_note = tk.StringVar(value="")
    ttk.Label(sec_card, textvariable=sec_note, style="Note.TLabel", wraplength=900, justify="left").pack(fill="x", pady=(4, 0))
    logopt = ttk.Frame(sec_card)
    logopt.pack(fill="x", pady=(8, 0))
    ttk.Label(logopt, text=f"{icon('log')} Pod log options:").pack(side="left")
    alllogs_chk = ttk.Checkbutton(logopt, text="Logs of ALL pods", variable=alllogs_var)
    alllogs_chk.pack(side="left", padx=(8, 0))
    ttk.Label(logopt, text="only namespaces (comma separated, blank = all):").pack(side="left", padx=(10, 2))
    ns_entry = ttk.Entry(logopt, textvariable=ns_var, width=26)
    ns_entry.pack(side="left")
    section_widgets += [alllogs_chk, ns_entry]

    # ---- tab 3: steps + clusters-in-run + live findings (left), live log (right)
    body = ttk.PanedWindow(tab_run, orient="horizontal")
    body.pack(fill="both", expand=True)
    left = ttk.Frame(body, width=470)
    body.add(left, weight=0)
    steps_box = card(left, f"{icon('list')} Collection steps (current cluster)", padding=4, fill="x")
    steps_wrap = ttk.Frame(steps_box)
    steps_wrap.pack(fill="x")
    steps = striped(ttk.Treeview(steps_wrap, columns=("status", "time"), height=8, show="tree headings", selectmode="none"))
    steps.heading("#0", text="Step")
    steps.heading("status", text="Status")
    steps.heading("time", text="Time")
    steps.column("#0", width=290)
    steps.column("status", width=80, anchor="center")
    steps.column("time", width=60, anchor="e")
    steps_scroll = ttk.Scrollbar(steps_wrap, orient="vertical", command=steps.yview)
    steps.configure(yscrollcommand=steps_scroll.set)
    steps_scroll.pack(side="right", fill="y")
    steps.pack(side="left", fill="x", expand=True)
    for tag, color in (("running", "#1f4e79"), ("done", "#067647"), ("failed", "#c00000"), ("skipped", "#888888")):
        steps.tag_configure(tag, foreground=color)
    steps.tag_configure("running", font=(FAM, 10, "bold"))

    run_box = card(left, f"{icon('folder')} Clusters in this run (double-click a finished one to open its report)", stripe=NAVY, padding=4, fill="x", pady=(6, 0))
    run_tree = striped(ttk.Treeview(run_box, columns=("status", "crit", "high"), height=3, show="tree headings", selectmode="browse"))
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

    find_box = card(left, f"{icon('bell')} Findings (live - updates while collecting)", stripe="#C00000", padding=4, fill="both", expand=True, pady=(6, 0))
    counters = ttk.Frame(find_box)
    counters.pack(fill="x")
    counter_vars = {}
    SEV_COLOR = {"CRIT": "#c00000", "HIGH": "#d35400", "MED": "#9a7d0a", "INFO": "#1f6feb"}

    def counter_text(sev, n):
        return f"{sev} {n}"
    for sev, color in SEV_COLOR.items():
        counter_vars[sev] = tk.StringVar(value=counter_text(sev, 0))
        tk.Label(counters, text=SEV_ICON[sev], fg=color, bg=BG, font=(FAM, 10)).pack(side="left")
        tk.Label(counters, textvariable=counter_vars[sev], fg=color, bg=BG, font=(FAM, 10, "bold")).pack(side="left", padx=(2, 14))
    findings = striped(ttk.Treeview(find_box, columns=("sev", "text"), show="headings", height=5))
    findings.heading("sev", text="Severity")
    findings.heading("text", text="Finding")
    findings.column("sev", width=84, anchor="w")
    findings.column("text", width=370)
    fs = ttk.Scrollbar(find_box, orient="vertical", command=findings.yview)
    findings.configure(yscrollcommand=fs.set)
    fs.pack(side="right", fill="y")
    findings.pack(fill="both", expand=True)
    for tag, color in SEV_COLOR.items():
        findings.tag_configure(tag, foreground=color)

    right = ttk.Frame(body)
    body.add(right, weight=1)
    text = scrolledtext.ScrolledText(right, font=("Consolas", 9), wrap="none", relief="flat", borderwidth=1, background="#FFFFFF")
    text.pack(fill="both", expand=True)
    for tag, color in (("CRIT", "#c00000"), ("HIGH", "#d35400"), ("MED", "#9a7d0a"), ("HEAD", "#1f4e79"), ("ERR", "#c00000"), ("WARN", "#d35400")):
        text.tag_configure(tag, foreground=color)
    text.tag_configure("HEAD", font=("Consolas", 9, "bold"))

    # ---- footer status bar: progress + status + collection tasks + actions
    progress_bar = ttk.Progressbar(bottom, mode="determinate", length=300)
    progress_bar.pack(side="left")
    status = tk.StringVar(value="Ready.")
    ttk.Label(bottom, textvariable=status, style="Footer.TLabel").pack(side="left", padx=10)
    tasks_var = tk.StringVar(value="")
    ttk.Label(bottom, textvariable=tasks_var, style="Footer.TLabel").pack(side="left")
    elapsed = tk.StringVar(value="")
    ttk.Label(bottom, textvariable=elapsed, style="Footer.TLabel").pack(side="left", padx=(10, 0))
    ttk.Label(bottom, text="Uses your existing AWS credentials from ~/.aws - no sign-in in this tool. Read-only: nothing is installed, created, changed or deleted", style="Footer.TLabel").pack(side="left", padx=(14, 0))
    folder_btn = ttk.Button(bottom, text=f"{icon('folder')} Open reports folder", style="Footer.TButton")
    folder_btn.pack(side="right")
    open_btn = ttk.Button(bottom, text=f"{icon('report')} Open HTML report", state="disabled", style="Footer.TButton")
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

    def set_state(widget, enabled):
        widget.state(["!disabled"] if enabled else ["disabled"])

    def is_cli():
        """True when the cluster list is read with aws (the default method, or ekslogin with the source 'Collect clusters from selected profiles')."""
        return _lists_via_cli()

    def default_source():
        return "menu"                       # instant: the ekslogin menu, no cloud calls. The aws listing only runs on demand (button)

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
        """The profiles the cluster collection covers: the selected ones (rows of load_accounts, 'id' None = environment / default credentials)."""
        return [a for a in state["accounts"] if a["key"] in state["acct_chosen"]]

    def update_scope_labels():
        usable = len([a for a in state["accounts"] if a.get("usable", True)])
        if is_cli():
            scope_all_rb.configure(text=f"All profiles ({usable})")
            scope_sel_rb.configure(text=f"Only the selected profiles ({len(state['acct_chosen'])})")
        else:
            scope_all_rb.configure(text="(auto) - picked after login")
            scope_sel_rb.configure(text=f"Use the selected profile ({len(state['acct_chosen'])})")

    # ---- step 1: profiles with a credentials status chip
    STATE_TAG = {"active": "st_active", "expiring": "st_expiring", "expired": "st_expired", "not_configured": "st_none", "unknown": "st_unknown", "unchecked": "st_none"}
    STATE_DOT = {"active": "●", "expiring": "●", "expired": "●", "not_configured": "○", "unknown": "○", "unchecked": "○"}

    def status_of(key):
        st = state["acct_status"].get(key)
        if st is None:
            a = state["acct_by_id"].get(key)
            try:
                st = local_status(a["id"] if a else key, state.get("profiles_info") or {})
            except Exception:
                st = {"profile": key, "state": "unchecked", "detail": "", "left": None, "expires_at": None}
        return st

    def chip_text(st):
        return f"{STATE_DOT.get(st['state'], '')} {status_text(st)}"

    def rebuild_account_list():
        acct_tree.delete(*acct_tree.get_children())
        ts = terms(acct_filter)
        shown = []
        for a in state["accounts"]:
            st = status_of(a["key"])
            a["hay"] = f"{a['name']} {a['code']} {a['role']} {a['region']} {status_text(st)}".lower()
            if hit(a["hay"], ts):
                shown.append((a, st))
        for n, (a, st) in enumerate(shown):
            acct_tree.insert("", "end", iid=a["key"], text=a["name"], values=(chip_text(st), a["code"], a["role"], a["region"]),
                             tags=(STATE_TAG.get(st["state"], "st_unknown"), ("odd", "even")[n % 2]))
        acct_tree.selection_set([a["key"] for a, _st in shown if a["key"] in state["acct_chosen"]])
        acct_count.set(f"Showing {len(shown)} of {len(state['accounts'])}")
        acct_sel.set(f"{len(state['acct_chosen'])} selected")

    def blocked_selected():
        """[(profile id, status)] of the SELECTED profiles whose credentials are expired or missing."""
        out = []
        for a in state["accounts"]:
            if a["key"] in state["acct_chosen"]:
                st = status_of(a["key"])
                if st["state"] in BLOCKED_STATES:
                    out.append((a["id"], st))
        return out

    def renew_text(blocked):
        lines = [renew_message(p) for p, _st in blocked[:3]]
        if len(blocked) > 3:
            lines.append(f"... and {len(blocked) - 3} more selected profile(s) with expired or missing credentials.")
        return "\n".join(lines)

    def update_renew_box():
        """The red message under the profile list: shown while a selected profile has expired / missing credentials (text only - nothing is run)."""
        blocked = blocked_selected()
        if blocked:
            renew_var.set(renew_text(blocked))
            renew_box.pack(side="bottom", fill="x", pady=(6, 0), before=acct_status_lbl)
        else:
            renew_box.pack_forget()
            renew_var.set("")

    def copy_renew_command():
        blocked = blocked_selected()
        cmd = renew_command(blocked[0][0] if blocked else None)
        root.clipboard_clear()
        root.clipboard_append(cmd)
        status.set("Copied (run it yourself, in your own terminal): " + cmd)

    def after_status_change():
        rebuild_account_list()
        update_renew_box()
        update_banner()

    def set_acct_status(key, st):
        state["acct_status"][key] = st
        after_status_change()

    def status_summary():
        c = Counter(status_of(a["key"])["state"] for a in state["accounts"])
        names = (("active", "Active"), ("expiring", "Expiring soon"), ("expired", "Expired"), ("not_configured", "Not configured"), ("unknown", "Unknown"), ("unchecked", "not checked"))
        return ", ".join(f"{c[k]} {label}" for k, label in names if c[k])

    def start_status_checks(keys):
        """Check the credentials of these profiles in the background (`aws sts get-caller-identity`, read-only, 4 at a time); the selected ones come first."""
        keys = [k for k in dict.fromkeys(keys) if k in state["acct_by_id"]]
        keys.sort(key=lambda k: 0 if k in state["acct_chosen"] else 1)
        if not keys:
            return
        if not shutil.which("aws"):
            acct_status.set("The AWS CLI (aws) was not found on PATH - the status comes from the local cache only. Install it from https://aws.amazon.com/cli/ ; this tool only uses credentials that already exist in ~/.aws.")
            return
        if state.get("status_cancel") is not None:
            state["status_cancel"].set()
        cancel = state["status_cancel"] = threading.Event()
        state["status_gen"] += 1
        gen = state["status_gen"]
        state["status_checking"] = True
        acct_status.set(f"Checking the credentials of {count_of(len(keys), 'profile')} ...")
        update_controls()
        ids = [state["acct_by_id"][k]["id"] for k in keys]

        def work():
            try:
                check_accounts(ids, lambda n, st: msgs.put(("acct_status", n or ENV_KEY, st)), cancel)
            finally:
                msgs.put(("acct_status_done", gen))
        threading.Thread(target=work, daemon=True).start()

    def check_selected(_event=None):
        keys = [a["key"] for a in state["accounts"] if a["key"] in state["acct_chosen"]]
        start_status_checks(keys or list(state["acct_by_id"]))

    def check_all_accounts(_event=None):
        start_status_checks(list(state["acct_by_id"]))

    def auto_check_accounts():
        """After the profiles are read: up to LAZY_STATUS_LIMIT profiles are all checked in the background (the selected ones first), otherwise only the selected ones."""
        if len(state["accounts"]) <= LAZY_STATUS_LIMIT:
            check_all_accounts()
        else:
            check_selected() if state["acct_chosen"] else acct_status.set("Many profiles: press 'Check credentials' for the selected ones, or 'Re-check all'.")

    def mark_expired(profile):
        """The credentials of `profile` turned out to be expired while listing / running: show it in the list and the red message."""
        key = profile or ENV_KEY
        try:
            st = classify_status(profile, "ExpiredToken: the credentials have expired", state.get("profiles_info") or {})
        except Exception:
            st = {"profile": profile, "state": "expired", "detail": "", "left": None, "expires_at": None}
        state["acct_status"][key] = st
        after_status_change()
        say(renew_message(profile), "err")

    def on_acct_select(_event=None):
        visible = set(acct_tree.get_children())
        state["acct_chosen"] = (state["acct_chosen"] - visible) | (set(acct_tree.selection()) & visible)
        if state["acct_chosen"] and scope_var.get() == "all":
            scope_var.set("sel")
        if scope_var.get() == "sel" and len(state["acct_chosen"]) == 1:
            only = next(iter(state["acct_chosen"]))
            state["active_profile"] = _SESSION["profile"] = state["acct_by_id"][only]["id"] if only in state["acct_by_id"] else only
        on_scope_change()

    def on_scope_radio():
        if scope_var.get() == "all":
            state["acct_chosen"] = set()               # 'All' is explicit: no leftover selection
            state["active_profile"] = None
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
        state["active_profile"] = None
        scope_var.set("all")
        rebuild_account_list()
        on_scope_change()

    def on_scope_change():
        _SESSION["profiles"] = sorted(state["acct_chosen"])                   # remembered for the session
        update_scope_labels()
        sync_login_opts()
        acct_count_refresh()
        rebuild_cluster_list()
        cluster_hint()
        update_renew_box()
        chosen = [k for k in state["acct_chosen"] if k not in state["acct_status"]]
        if chosen and len(state["accounts"]) > LAZY_STATUS_LIMIT:
            start_status_checks(chosen)             # many profiles: the ones you select are checked as you select them

    def acct_count_refresh():
        acct_sel.set(f"{len(state['acct_chosen'])} selected")
        update_estimate()

    # ---- step 3 list
    def scoped_rows():
        rows = state["crows"]
        if is_cli() and scope_var.get() == "sel":
            ids = {state["acct_by_id"][k]["id"] for k in state["acct_chosen"] if k in state["acct_by_id"]}
            rows = [r for r in rows if r.get("account") in ids]
        return rows

    def rebuild_cluster_list():
        cluster_tree.delete(*cluster_tree.get_children())
        rows = scoped_rows()
        ts = terms(filter_var)
        shown = [r for r in rows if hit(r["hay"], ts)]
        if is_cli() and not rows and not state["listing"]:
            cluster_tree.insert("", "end", iid="__hint__", text="Select one or more profiles in step 1, then press 'Collect clusters from selected profiles'.", tags=("hint",))
        for n, r in enumerate(shown):
            cluster_tree.insert("", "end", iid=r["key"], text=(f"{r['number']} - {r['name']}" if r.get("number") else r["name"]),
                                values=(r.get("account_name") or "", r.get("where") or "", r.get("via") or ""), tags=(("odd", "even")[n % 2],))
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

    def cluster_hint():
        """The line under the Collect button: where the list stands."""
        if state["listing"]:
            sc = state.get("list_scope")
            el = int(time.time() - (state.get("list_t0") or time.time()))
            cl_status.set(f"Listing clusters: {state['list_done']}/{state['list_total']}" + (f" ({sc[0] or ENV_LABEL}, {sc[1]})" if sc else "")
                          + f"   elapsed {el // 60:02d}:{el % 60:02d}   ({count_of(len(state['crows']), 'cluster')} so far)")
        elif is_cli() and not state["scopes"] and not state["crows"]:
            cl_status.set("Nothing is collected yet. Select profile(s) in step 1 and press 'Collect clusters from selected profiles'.")
        elif is_cli():
            new, _total, _np, _nr = scope_estimate()
            if new:
                cl_status.set((state.get("hint_note") or "") + f"{new} selected profile/region pair(s) are not collected yet - press 'Collect clusters from selected profiles'.")

    # ---- what is enabled, the banner
    def update_controls():
        cli = is_cli()
        busy, listing = state["busy"], state["listing"]
        idle = not busy and not listing
        set_state(check_btn, not busy)
        set_state(recheck_all_btn, not busy)
        set_state(acct_reload_btn, not busy and not state["acct_loading"])
        set_state(refresh_btn, not busy and not listing)
        set_state(collect_btn, cli and idle)
        set_state(refresh_sel_btn, cli and idle)
        list_stop_btn.state(["!disabled"] if listing else ["disabled"])
        set_state(allreg_chk, cli and not listing)
        set_state(region_entry, not listing)
        for rb in (source_all_rb, source_menu_rb):                  # the choice only exists with ekslogin; the default method always lists from aws
            set_state(rb, LOGIN_OPTS["method"] == "exe" and idle)
        method_combo.configure(state="disabled" if (busy or listing) else "readonly")
        run_btn.state(["disabled"] if (busy or listing) else ["!disabled"])
        stop_btn.state(["!disabled"] if (busy or listing) else ["disabled"])
        for w in section_widgets:
            set_state(w, not busy)
        update_banner()
        update_estimate()
        cluster_hint()

    def update_banner():
        """The subtitle of the banner: window, where the credentials come from, how many profiles were found."""
        try:
            mins = max(1, int(minutes_var.get()))
        except ValueError:
            mins = default_minutes
        who = ("Uses your existing AWS credentials from ~/.aws - no sign-in in this tool" if LOGIN_OPTS["method"] == "cli"
               else "ekslogin first, then ~/.aws is re-read - this tool does not sign in itself")
        n = len(state["accounts"])
        line = f"{icon('clock')} Last {mins} minutes      {icon('key')} {who}      {icon('list')} {n} profile{'' if n == 1 else 's'}      read-only: nothing is changed or installed"
        if line != banner_sub.get():
            banner_sub.set(line)
            draw_banner()

    def selected_sections():
        return [sid for sid in SEC_IDS if section_vars[sid].get()]

    def sections_changed(*_args):
        sel = selected_sections()
        sec_count.set(f"{len(sel)} of {len(SECTIONS)} sections selected")
        _SESSION["sections"] = list(sel)
        hidden = plan_sections(current_options())["hidden"]
        sec_note.set("".join(f"{icon('info')} {SECTION_BY_ID[d]['title']} is not ticked, but {section_titles_of(who)} need(s) its data: it is read silently "
                             f"and not shown in the report.   " for d, who in hidden.items()))
        if not state["busy"]:
            reset_steps()

    def set_sections(ids):
        for sid in SEC_IDS:
            if not SECTION_BY_ID[sid].get("locked"):
                section_vars[sid].set(sid in ids)

    def apply_method_ui():
        exe = LOGIN_OPTS["method"] == "exe"
        if exe:
            method_info.set("Runs your ekslogin.exe with the cluster's menu number first, then re-reads ~/.aws (ekslogin may refresh profiles). "
                            "Clusters not in its menu use aws eks update-kubeconfig.")
        else:
            method_info.set("Uses the credentials already in ~/.aws; aws eks update-kubeconfig writes the kubeconfig entry. Nothing signs in here.")
        region_hint.set("comma separated; blank = each selected profile's own region; all = every enabled region")
        update_scope_labels()
        if not is_cli():
            say("ekslogin menu: the clusters below come from ekslogin. Select them in step 3 and press 'Login & Debug'.", "info")
        else:
            say("Select the AWS profile(s) in step 1 (check their credentials status), then press 'Collect clusters from selected profiles' in step 2.", "info")

    # ---- step 1 loading (profiles are read once and cached for the session; 'Reload profiles' refreshes - also after ekslogin ran)
    def load_accounts_async(force=False):
        if state["acct_loading"] or (state["accounts_loaded"] and not force):
            return
        state["acct_loading"] = True
        acct_status.set("Reading the profiles in ~/.aws ...")
        update_controls()

        def work():
            try:
                accts, err = load_accounts()
            except Exception as exc:
                accts, err = [], str(exc)
            try:
                profs = list_aws_profiles()
            except Exception:
                profs = {}
            msgs.put(("accounts", accts, err, profs))
        threading.Thread(target=work, daemon=True).start()

    def on_accounts(accts, err, profs=None):
        state["acct_loading"] = False
        state["accounts_loaded"] = True
        state["accounts"] = accts
        state["acct_by_id"] = {a["key"]: a for a in accts}
        state["profiles_info"] = profs or {}
        state["acct_chosen"] &= set(state["acct_by_id"])
        first = ACCOUNT0
        if first and not state["pre"]:
            state["pre"] = True
            state["active_profile"] = first
            if first in state["acct_by_id"]:
                state["acct_chosen"] = {first}
                scope_var.set("sel")
        if not state["acct_chosen"] and _SESSION.get("profiles"):                     # the last selection of this session
            state["acct_chosen"] = {n for n in _SESSION["profiles"] if n in state["acct_by_id"]}
        if state["acct_chosen"]:
            scope_var.set("sel")
        if scope_var.get() == "sel" and not state["acct_chosen"]:
            scope_var.set("all")
        update_scope_labels()
        update_controls()
        rebuild_account_list()
        rebuild_cluster_list()
        update_renew_box()
        state["acct_err"] = err
        if not accts:
            acct_status.set("No AWS profiles found in ~/.aws/config / credentials and no AWS_* credentials in the environment. Set them up the way your company does "
                            "(this tool does not sign in or create profiles), then press 'Reload profiles'." + (f" (details: {err})" if err else ""))
            say(acct_status.get(), "warn")
            return
        acct_status.set(f"{count_of(len(accts), 'profile')} found.")
        auto_check_accounts()
        if is_cli():
            say(f"{count_of(len(accts), 'profile')} found. Step 1: select the profile(s) and check their credentials; step 2: press 'Collect clusters from selected profiles' "
                "(nothing is listed automatically).", "ok")

    # ---- step 2: clusters are listed per profile in parallel; the list grows while it runs
    def scope_estimate(accts=None):
        """(new pairs, all pairs, profiles, regions) the selected profiles x the Region(s) box would search, without any cloud call ('all' counts the built-in region list)."""
        accts = effective_accounts() if accts is None else accts
        text = region_var.get().strip()
        ex = _explicit_regions(text)
        profs = list_aws_profiles() if accts else {}
        if any(r.lower() in ALL_REGIONS for r in ex):
            ex = list(state.get("all_regions") or FALLBACK_REGIONS) + [r for r in ex if r.lower() not in ALL_REGIONS]
        pairs = []
        for a in accts:
            info = profs.get(a["id"] or os.environ.get("AWS_PROFILE") or "default") or {}
            rs = ex or [r for r in (info.get("region") or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),) if r]
            pairs += [(a["id"], r) for r in rs]
        new = [x for x in pairs if x not in state["scopes"]]
        return len(new), len(pairs), len(accts), len({r for _, r in pairs})

    def update_estimate():
        if not is_cli():
            est_var.set("")
            return
        new, total, nprof, nreg = scope_estimate()
        if not nprof:
            est_var.set("No profile selected: select one or more profiles in step 1.")
        else:
            est_var.set(f"{total} profile/region pair(s) selected ({count_of(nprof, 'profile')} x {count_of(nreg, 'region')})" + (f"; {new} not collected yet" if state["scopes"] else "")
                        + ("   [All regions: about " + str(len(FALLBACK_REGIONS)) + " per profile]" if "all" in region_var.get().lower() else ""))

    def region_changed(*_a):
        text = region_var.get().strip()
        _SESSION["regions"] = region_var.get()
        if allreg_var.get() != (text.lower() == "all"):
            allreg_var.set(text.lower() == "all")
        sync_login_opts()
        update_estimate()
        cluster_hint()

    def allreg_changed(*_a):
        if allreg_var.get() and region_var.get().strip().lower() != "all":
            region_var.set("all")
        elif not allreg_var.get() and region_var.get().strip().lower() == "all":
            region_var.set("")

    def collect_clusters(refresh=False):
        """The button: `aws eks list-clusters` for the SELECTED profiles x the Region(s) only - and only for profiles whose credentials are not Expired / Not
        configured (those are skipped with a note). Scopes already collected are skipped (refresh=True collects them again)."""
        if state["busy"]:
            status.set("Wait for the current run to finish (or press Stop) before collecting clusters.")
            return
        if state["listing"]:
            return
        sync_login_opts()
        chosen = effective_accounts()
        if not chosen:
            say("Select one or more profiles in step 1 first (or press 'Select all (shown)'), then press 'Collect clusters from selected profiles'.", "warn")
            return
        skipped = [a for a in chosen if status_of(a["key"])["state"] in BLOCKED_STATES]
        accts = [a for a in chosen if a not in skipped]
        if not accts:
            say(renew_text([(a["id"], None) for a in skipped]), "err")
            return
        new, total, nprof, nreg = scope_estimate(accts)
        todo = total if refresh else new
        if not todo:
            say("All selected profile/region pairs are already collected - the list below is up to date. Press 'Refresh selected' to collect them again.", "ok")
            return
        if todo > LARGE_SCOPE:
            if not messagebox.askyesno("Collect clusters", f"This will search {todo} profile/region pairs and can take several minutes. Continue?"):
                say("Collecting stopped before it started - select fewer profiles / regions.", "info")
                return
        simple = [{"id": a["id"], "name": a["name"]} for a in accts]
        regions_text = region_var.get().strip() or None
        done_before = set(state["scopes"])
        cancel = threading.Event()
        note = ("Skipped (credentials expired or missing - renew them outside this tool, then Re-check): " + ", ".join(a["name"] for a in skipped) + ".   ") if skipped else ""
        state.update(listing=True, list_cancel=cancel, list_done=0, list_total=todo, list_scope=None, list_t0=time.time(), hint_note=note)
        list_bar.configure(maximum=max(1, todo), value=0)
        say(note + f"Collecting clusters: {todo} profile/region pair(s) ... the list below fills in as results arrive (Stop cancels).", "warn" if skipped else "info")
        update_controls()
        list_tick()

        def work():
            stats = {}
            try:
                if LOGIN_OPTS["method"] == "exe":
                    exe_menu_clusters(refresh=True)         # which of the found clusters ekslogin offers (the others use aws eks update-kubeconfig)
                lookups, noregion = _plan_lookups(simple, _explicit_regions(regions_text), list_aws_profiles())
                if any(r.lower() in ALL_REGIONS for r in _explicit_regions(regions_text)):
                    msgs.put(("allreg", sorted({r for _p, r in lookups})))          # the real list of enabled regions: the estimate uses it from now on
                if not refresh:
                    lookups = [x for x in lookups if x not in done_before]
                if not lookups:
                    msgs.put(("cdone", [], 0, False, [a["id"] for a in simple], {"lookups": 0, "failures": [], "profiles": [], "failed_profiles": [], "attempted": [], "ok_scopes": [],
                                                                               "noregion": list(noregion)}))
                    return
                found, failed = scan_clusters(simple, lambda l: msgs.put(("line", l)), lambda d, t: msgs.put(("lprog", d, t, stats.get("last_scope"))), cancel,
                                              lambda batch: msgs.put(("cbatch", list(batch))), stats=stats, lookups=lookups)
                msgs.put(("cdone", found, failed, cancel.is_set(), [a["id"] for a in simple], stats))
            except Exception as exc:
                msgs.put(("cerr", str(exc), [a["id"] for a in simple]))
        threading.Thread(target=work, daemon=True).start()

    def list_tick():
        if state["listing"]:
            cluster_hint()
            root.after(1000, list_tick)

    def load_clusters(_event=None):
        if state["busy"]:
            status.set("Wait for the current run to finish (or press Stop) before reloading the cluster list.")
            return
        if state["listing"]:
            return
        sync_login_opts()
        if not is_cli():
            state["listing"] = True
            cl_status.set("Loading the cluster list from ekslogin ...")
            update_controls()
            threading.Thread(target=lambda: msgs.put(("clusters", list_clusters())), daemon=True).start()
            return
        collect_clusters(refresh=True)

    def on_clusters_done(found, failed, cancelled, ids, stats=None):
        state["listing"] = False
        stats = stats or {}
        scanned = set(ids)
        ok_scopes = set(tuple(x) for x in (stats.get("ok_scopes") or []))
        rows = [r for r in state["crows"] if (r.get("account"), r.get("region")) not in ok_scopes]       # only the scopes that were collected again are replaced
        rows += [prepare(c) for c in found]
        state["scopes"] |= ok_scopes
        seen, uniq = set(), []
        for r in rows:
            if r["key"] not in seen:                  # the same cluster can be reached through two profiles
                seen.add(r["key"])
                uniq.append(r)
        rows = uniq
        state["listed"] = {p for p, _r in state["scopes"]}
        aidx = {a["id"]: i for i, a in enumerate(state["accounts"])}
        rows.sort(key=lambda r: aidx.get(r.get("account"), len(aidx)))             # stable: the listed order stays inside a profile
        clusters = register_clusters(rows, len(state["listed"]) > 1)
        for i, r in enumerate(rows, start=1):
            r["number"], r["label"] = str(i), clusters[str(i)]
            r["via"] = CLI_TARGETS[str(i)].get("via", "")
            prepare(r)
        state["clusters"] = clusters
        set_rows(rows)
        list_bar.configure(value=state["list_total"])
        n = len(rows)
        skipped = f" {failed} lookup(s) failed - see the log." if failed else ""
        summary = scan_summary(found, stats) if stats else f"{count_of(n, 'cluster')} found in {count_of(len(scanned), 'profile')}."
        n_menu = sum(1 for t in CLI_TARGETS.values() if t.get("exe_number"))
        if LOGIN_OPTS["method"] == "exe" and n:
            summary += f" {n_menu} in the ekslogin menu, {n - n_menu} connected with aws eks update-kubeconfig."
        if stats.get("lookups") == 0 and not cancelled:
            cl_status.set("Nothing new to collect." if not stats.get("noregion") else "No region known for: " + ", ".join(stats["noregion"][:5]) + " - type a region in the Region(s) box.")
            say(cl_status.get(), "warn" if stats.get("noregion") else "info")
            update_controls()
            return
        if cancelled:
            state["hint_note"] = f"Listing stopped - {count_of(n, 'cluster')} so far. "
            cl_status.set(f"Listing stopped - {count_of(n, 'cluster')} so far.")
            say(f"Listing stopped - {count_of(n, 'cluster')} found so far. Press 'Collect clusters from selected profiles' to continue (collected profile/region pairs are kept).", "warn")
        elif not n:
            cl_status.set("No clusters found.")
            say("No EKS clusters found for the selected profile(s) and region(s). Check that the profile has working credentials and access, and type the right region(s) "
                "in the Region(s) box (or tick All regions)." + skipped, "warn")
        else:
            cl_status.set(summary + (skipped if not stats else ""))
            say(f"{summary} Step 3: search / select the clusters, then press 'Login & Debug'." + (skipped if not stats else ""),
                "ok" if not (stats or {}).get("failed_profiles") and not failed else "warn")
        for prof in (stats or {}).get("expired_profiles") or []:          # expired while listing: not a generic failure
            mark_expired(None if prof == (os.environ.get("AWS_PROFILE") or "default") and prof not in state["acct_by_id"] else prof)
        update_controls()

    # ---- run options, steps list
    def plan(options):
        rows = [("login", "Log in with ekslogin (aws eks update-kubeconfig for clusters that are not in its menu)" if LOGIN_OPTS["method"] == "exe" else
                 "Write kubeconfig entry (aws eks update-kubeconfig, existing credentials)"),
                ("context", "Select kubectl context"), ("profile", "Select AWS profile (~/.aws)")]
        rows += [(k, t) for k, t, _ in run_steps(options)] + [("report", "Write HTML report")]
        return rows

    def current_options():
        """What the boxes ask for. The two older switches only matter when --no-aws was given on the command line and the box is still unticked:
        otherwise an unticked section is simply not shown (a ticked section that needs its data still reads it silently)."""
        return {"aws": AWS_OPTS["enabled"] or aws_var.get(), "logs": True, "sections": selected_sections()}

    def reset_steps():
        steps.delete(*steps.get_children())
        rows = plan({})
        live = plan_sections(current_options())["live"]
        state["term"] = set()
        for key, title in rows:
            if key in SECTION_BY_ID and key not in live:           # not ticked: nothing will run for it
                steps.insert("", "end", iid=key, text=title, values=(ICON["skipped"], ""), tags=("skipped",))
                state["term"].add(key)
            else:
                steps.insert("", "end", iid=key, text=title, values=(ICON["pending"], ""))
        state["total"], state["finished"] = len(rows), len(state["term"])
        progress_bar.configure(maximum=len(rows), value=state["finished"])
        tasks_var.set("")
        stripe_tree(steps)

    def reset_run(selected):
        findings.delete(*findings.get_children())
        state["counts"] = Counter()
        for sev in counter_vars:
            counter_vars[sev].set(counter_text(sev, 0))
        run_tree.delete(*run_tree.get_children())
        state["reports"] = {}
        for number, label in selected:
            run_tree.insert("", "end", iid=number, text=f"{number} - {label}", values=("waiting", "", ""), tags=("notrun",))
        stripe_tree(run_tree)

    def sync_login_opts():
        """Read the connection method and the chosen profile (and region) into the options a run (or a cluster listing) uses."""
        LOGIN_OPTS["method"] = "exe" if method_combo.get() == LOGIN_LABELS["exe"] else "cli"
        LOGIN_OPTS["source"] = "all" if LOGIN_OPTS["method"] == "cli" else source_var.get()
        picked = state["acct_chosen"] if scope_var.get() == "sel" else set()
        if state["pre"] or not ACCOUNT0:      # keep the command-line value until the list has been read
            only = next(iter(picked)) if len(picked) == 1 else None
            AWS_OPTS["profile"] = state["acct_by_id"][only]["id"] if only in state["acct_by_id"] else None
        AWS_OPTS["region"] = (region_var.get().strip() or None) if is_cli() else region0

    def reset_listing():
        """The cluster list starts again after the connection method or the list source changed."""
        state.update(crows=[], by_key={}, cchosen=set(), clusters={}, listed=set(), scopes=set())
        manual_var.set("")
        sync_login_opts()
        apply_method_ui()
        update_controls()
        rebuild_cluster_list()
        reset_steps()
        if not is_cli():
            load_clusters()
        load_accounts_async()

    def on_method_change(_event=None):
        if state["busy"] or state["listing"]:
            method_combo.set(LOGIN_LABELS[LOGIN_OPTS["method"]])
            status.set("Wait for the current run / listing to finish (or press Stop) before changing how to connect.")
            return
        if method_combo.get() == LOGIN_LABELS["exe"] and not state["src_user"]:
            source_var.set(default_source())
        reset_listing()

    def on_source_change(_event=None):
        if state["busy"] or state["listing"]:
            source_var.set(LOGIN_OPTS.get("source") or "menu")
            status.set("Wait for the current run / listing to finish (or press Stop) before changing the cluster list source.")
            return
        state["src_user"] = True
        reset_listing()

    def start():
        if state["busy"]:
            return
        if state["listing"]:
            status.set("Wait for the cluster listing to finish (or press Stop).")
            return
        sync_login_opts()
        if is_cli():
            blocked = blocked_selected()
            if blocked:
                say(renew_text(blocked), "err")          # a cluster of such a profile is marked 'credentials expired'; the others still run
        selected = chosen_clusters()
        if not selected:
            status.set("Select at least one cluster in the list (or type numbers like 1,3).")
            say("Select at least one cluster in step 3 (or type numbers like 1,3), then press 'Login & Debug'.", "warn")
            return
        try:
            minutes = max(1, int(minutes_var.get()))
        except ValueError:
            status.set("Minutes must be a number.")
            return
        picked = selected_sections()
        if not [x for x in picked if not SECTION_BY_ID[x].get("locked")]:
            nb.select(tab_collect)
            status.set("Nothing selected to collect - tick at least one section in 'What to collect'.")
            say("Nothing selected to collect. Tick at least one section in the 'What to collect' tab (or press 'Select all'), then press 'Login & Debug'.", "warn")
            return
        options = {**current_options(), "all_logs": alllogs_var.get(), "log_namespaces": ns_var.get(), "sections": picked,
                   "on_tasks": lambda done, total: msgs.put(("tasks", done, total))}
        state.update(busy=True, cancel=threading.Event(), html=None, t0=time.time(), n=len(selected))
        update_controls()
        set_run_chip("run", "Running")
        nb.select(tab_run)
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
        if state["busy"] and state["cancel"] is not None:
            state["cancel"].set()
            stop_btn.state(["disabled"])
            set_run_chip("warn", "Stopping")
            status.set("Stopping: pending collection tasks are cancelled ... a partial report is saved, remaining clusters are skipped.")
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

    def finish(ok_text, kind="ok", chip="Done"):
        state["busy"] = False
        update_controls()
        status.set(ok_text)
        set_run_chip(kind, chip)

    RUN_TAG = {"running": "running", "ok": "ok", "failed": "failed", "stopped (partial)": "partial", "credentials expired": "failed"}

    def poll():
        try:
            while True:
                msg = msgs.get_nowait()
                kind = msg[0]
                if kind == "line":
                    write(msg[1])
                elif kind == "say":
                    say(msg[1], msg[2])
                elif kind == "acct_status":
                    set_acct_status(msg[1], msg[2])
                elif kind == "acct_status_done":
                    if msg[1] == state["status_gen"]:
                        state["status_checking"] = False
                        acct_status.set("Credentials checked: " + status_summary() + ".")
                        blocked = blocked_selected()
                        if blocked:
                            say(renew_text(blocked), "err")
                        update_controls()
                elif kind == "accounts":
                    on_accounts(msg[1], msg[2], msg[3] if len(msg) > 3 else None)
                elif kind == "allreg":
                    state["all_regions"] = msg[1]
                    update_estimate()
                elif kind == "lprog":
                    state["list_done"], state["list_total"] = msg[1], msg[2]
                    state["list_scope"] = msg[3] if len(msg) > 3 else None
                    list_bar.configure(maximum=max(1, msg[2]), value=msg[1])
                    cluster_hint()
                    status.set(f"Listing clusters: {msg[1]}/{msg[2]} profile/region lookups ...")
                elif kind == "cbatch":
                    for c in msg[1]:
                        if c["key"] not in state["by_key"]:
                            state["crows"].append(prepare(c))
                            state["by_key"][c["key"]] = c
                    rebuild_soon()
                elif kind == "cdone":
                    on_clusters_done(msg[1], msg[2], msg[3], msg[4], msg[5] if len(msg) > 5 else None)
                    status.set("Cluster listing done." if not msg[3] else "Cluster listing stopped.")
                elif kind == "cerr":
                    state["listing"] = False
                    cl_status.set("Listing failed.")
                    say(f"Could not list the clusters: {msg[1]}", "err")
                    update_controls()
                elif kind == "step":
                    _, key, st, secs = msg
                    if steps.exists(key):
                        pos = steps.index(key)
                        steps.item(key, values=(ICON.get(st, st), f"{secs:.1f}s" if secs else ""), tags=(st, ("odd", "even")[pos % 2]))
                    if st in ("done", "skipped", "failed"):
                        if key not in state["term"]:        # a step counts once, however often it is reported (run-ahead tasks report early)
                            state["term"].add(key)
                            state["finished"] += 1
                            progress_bar.configure(value=state["finished"])
                    elif st == "running" and steps.exists(key):
                        steps.see(key)
                        status.set(f"{steps.item(key, 'text')} ...")
                elif kind == "cluster":
                    _, i, n, label, st, entry = msg
                    number = entry["number"]
                    shown = {"ok": "OK", "failed": "FAILED", "running": "running ..."}.get(st, st)
                    if st == "credentials expired" and entry.get("expired_profile"):
                        mark_expired(entry["expired_profile"])
                    c = entry["counts"] if entry.get("counts") else Counter()
                    if run_tree.exists(number):
                        run_tree.item(number, values=(shown, c["CRIT"] if st != "running" else "", c["HIGH"] if st != "running" else ""),
                                      tags=(RUN_TAG.get(st, "notrun"), ("odd", "even")[run_tree.index(number) % 2]))
                    if st == "running":
                        reset_steps()                       # fresh checklist for this cluster
                        status.set(f"Cluster {i} of {n}: {label} ...")
                    elif entry.get("html"):
                        state["reports"][number] = entry["html"]
                elif kind == "tasks":
                    tasks_var.set(f"{icon('speed')} {msg[1]} of {msg[2]} collection tasks done")
                elif kind == "finding":
                    _, sev, t = msg
                    state["counts"][sev] += 1
                    counter_vars[sev].set(counter_text(sev, state["counts"][sev]))
                    findings.insert("", "end", values=(f"{SEV_ICON.get(sev, '')} {sev}", t), tags=(sev, ("odd", "even")[len(findings.get_children()) % 2]))
                    findings.yview_moveto(1.0)
                elif kind == "clusters":          # the custom login's own cluster list
                    state["listing"] = False
                    state["clusters"] = dict(msg[1])
                    set_rows([prepare({"key": k, "number": k, "label": v, "name": v, "where": "", "account": None, "account_name": "", "via": "ekslogin"})
                              for k, v in sorted(msg[1].items(), key=lambda kv: numeric(kv[0]))])
                    update_controls()
                    if msg[1]:
                        cl_status.set(f"{len(msg[1])} cluster(s) from ekslogin.")
                        say(f"{len(msg[1])} cluster(s) found. Select one or more in step 3, then press 'Login & Debug'.", "ok")
                        status.set(f"{len(msg[1])} cluster(s) found. Select one or more, then press Login & Debug.")
                    else:
                        cl_status.set("No clusters.")
                        say("Could not read the cluster list from ekslogin - type the cluster number(s) in the box in step 4, or check ekslogin.exe / clusters.json.", "warn")
                        status.set("Could not read the cluster list from ekslogin - type the cluster number(s) in the box.")
                elif kind == "done":
                    results = msg[1]
                    ok = [r for r in results["items"] if r["html"]]
                    state["html"] = results["index"] or (ok[0]["html"] if ok else None)
                    if LOGIN_OPTS["method"] == "exe":
                        load_accounts_async(force=True)          # ekslogin may have created / refreshed the profiles: read ~/.aws again and re-check the credentials
                    progress_bar.configure(value=state["total"])
                    if state["html"]:
                        open_btn.state(["!disabled"])
                    bad = [r["label"] for r in results["items"] if r["status"] in ("failed", "credentials expired")]
                    stopped_run = any(r["status"].startswith("stopped") or r["status"].startswith("not run") for r in results["items"])
                    finish(f"Done: {len(ok)} of {len(results['items'])} cluster(s) reported"
                           + (f" ({len(bad)} failed: {', '.join(bad)})" if bad else "")
                           + ". Click 'Open HTML report'." if state["html"] else "Finished, but no report could be written - see the log.",
                           "err" if (bad and not ok) else ("warn" if (bad or stopped_run) else "ok"), "Stopped" if stopped_run else ("Failed" if (bad and not ok) else "Done"))
                    if state["html"] and open_var.get():
                        open_report()
                elif kind == "error":
                    write(f"ERROR: {msg[1]}")
                    finish(f"ERROR: {msg[1]}", "err", "Failed")
        except queue.Empty:
            pass
        if state["busy"] and state["t0"]:
            s = int(time.time() - state["t0"])
            elapsed.set(f"elapsed {s // 60}:{s % 60:02d}")
        root.after(150, poll)

    run_btn.configure(command=start)
    stop_btn.configure(command=stop)
    sec_all_btn.configure(command=lambda: set_sections(set(SEC_IDS)))
    sec_none_btn.configure(command=lambda: set_sections(set()))
    sec_net_btn.configure(command=lambda: set_sections(set(SECTION_PRESETS["only_networking"])))
    sec_nonet_btn.configure(command=lambda: set_sections(set(SECTION_PRESETS["no_networking"])))
    for _var in section_vars.values():
        _var.trace_add("write", sections_changed)
    minutes_var.trace_add("write", lambda *_: update_banner())
    refresh_btn.configure(command=load_clusters)
    collect_btn.configure(command=lambda: collect_clusters(False))
    refresh_sel_btn.configure(command=lambda: collect_clusters(True))
    list_stop_btn.configure(command=lambda: state["list_cancel"] and state["list_cancel"].set())
    allreg_var.trace_add("write", allreg_changed)
    region_var.trace_add("write", region_changed)
    open_btn.configure(command=open_report)
    folder_btn.configure(command=open_folder)
    select_all_btn.configure(command=select_all)
    clear_btn.configure(command=clear_selection)
    check_btn.configure(command=check_selected)
    recheck_all_btn.configure(command=check_all_accounts)
    renew_copy_btn.configure(command=copy_renew_command)
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
    source_all_rb.configure(command=lambda: on_source_change())
    source_menu_rb.configure(command=lambda: on_source_change())
    state["active_profile"] = ACCOUNT0 or _SESSION.get("profile") or None
    state["src_user"] = LOGIN_OPTS.get("source") in ("all", "menu")      # given on the command line (--all-clusters) = the user's choice
    LOGIN_OPTS["source"] = "all" if LOGIN_OPTS["method"] == "cli" else (LOGIN_OPTS.get("source") or default_source())
    source_var.set(LOGIN_OPTS["source"])
    sections_changed()
    apply_method_ui()
    update_controls()
    _GUI.update(root=root, cluster_tree=cluster_tree, run_btn=run_btn, stop_btn=stop_btn, open_btn=open_btn, steps=steps,
                findings=findings, text=text, status=status, state=state, counter_vars=counter_vars,
                open_var=open_var, aws_var=aws_var, logs_var=logs_var, progress=progress_bar,
                alllogs_var=alllogs_var, ns_var=ns_var, load_profiles=lambda: load_accounts_async(force=True),
                select_all_btn=select_all_btn, clear_btn=clear_btn, filter_var=filter_var, manual_var=manual_var,
                run_tree=run_tree, sel_text=sel_text, chosen_clusters=chosen_clusters,
                method_combo=method_combo, method_info=method_info, on_method_change=on_method_change,
                acct_tree=acct_tree, acct_filter=acct_filter, acct_count=acct_count, cl_count=cl_count, scope_var=scope_var,
                acct_all_btn=acct_all_btn, acct_clear_btn=acct_clear_btn, acct_reload_btn=acct_reload_btn, refresh_btn=refresh_btn, acct_sel=acct_sel,
                collect_btn=collect_btn, refresh_sel_btn=refresh_sel_btn, list_stop_btn=list_stop_btn, allreg_var=allreg_var, allreg_chk=allreg_chk,
                est_var=est_var, collect_clusters=collect_clusters, scope_estimate=scope_estimate,
                check_btn=check_btn, recheck_all_btn=recheck_all_btn, check_selected=check_selected, check_all_accounts=check_all_accounts,
                renew_box=renew_box, renew_var=renew_var, renew_copy_btn=renew_copy_btn, update_renew_box=update_renew_box, uses_note=uses_note,
                guide_msg=guide_msg, acct_status=acct_status, cl_status=cl_status, search_icon=SEARCH_ICON,
                s2=s2, s3=s3, s4=s4, list_bar=list_bar, acct_search=acct_search, cl_search=cl_search, scope_all_rb=scope_all_rb,
                scope_sel_rb=scope_sel_rb, load_clusters=load_clusters, mark_expired=mark_expired, status_of=status_of, set_acct_status=set_acct_status,
                source_var=source_var, source_all_rb=source_all_rb, source_menu_rb=source_menu_rb, on_source_change=lambda: on_source_change(),
                region_hint=region_hint, sync_login_opts=sync_login_opts, start_status_checks=start_status_checks, status_summary=status_summary)
    _GUI.update(ro_note=ro_note, region_var=region_var, banner=banner, banner_sub=banner_sub, notebook=nb, tab_clusters=tab_clusters, tab_collect=tab_collect, tab_run=tab_run,
                section_vars=section_vars, section_checks=section_checks, sec_count=sec_count, sec_note=sec_note, sec_card=sec_card,
                sec_all_btn=sec_all_btn, sec_none_btn=sec_none_btn, sec_net_btn=sec_net_btn, sec_nonet_btn=sec_nonet_btn,
                selected_sections=selected_sections, set_sections=set_sections, run_chip=run_chip, tasks_var=tasks_var, footer=bottom,
                action_bar=top, minutes_var=minutes_var, alllogs_chk=alllogs_chk, ns_entry=ns_entry, section_widgets=section_widgets)
    if not is_cli():
        load_clusters()
    load_accounts_async()
    poll()
    root.mainloop()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def resolve_cli_sections(args):
    """The sections the command line asks for: None = all, else a list of ids. Raises ValueError with a clear message."""
    picks = [n for n, on in (("--sections", args.sections), ("--only-networking", args.only_networking), ("--no-networking", args.no_networking)) if on]
    if len(picks) > 1:
        raise ValueError(f"use only one of {', '.join(picks)}")
    wanted = None
    if args.sections:
        wanted = parse_section_list(args.sections)
        if not wanted:
            raise ValueError("--sections is empty: name at least one section (see --list-sections)")
    elif args.only_networking:
        wanted = list(SECTION_PRESETS["only_networking"])
    elif args.no_networking:
        wanted = list(SECTION_PRESETS["no_networking"])
    if args.skip_sections:
        skip = parse_section_list(args.skip_sections)
        wanted = [x for x in (wanted if wanted is not None else SECTION_PRESETS["all"]) if x not in skip]
    if wanted is not None and set(wanted) >= set(SECTION_PRESETS["all"]):
        wanted = None
    return wanted


def print_sections(out=print):
    """--list-sections: every section with its id, full title, what it shows and what it needs."""
    out("Report sections (use the ids with --sections / --skip-sections; --only-networking = overview + network; --no-networking = all but network):")
    for sec in SECTIONS:
        out(f"  {sec['id']:<12} {sec['title']}" + ("   [always collected]" if sec.get("locked") else ""))
        out(f"               {sec['desc']}")
        if sec["needs"]:
            out(f"               needs the data of: {', '.join(sec['needs'])} (read silently when it is not selected)")
    out("Aliases: " + ", ".join(f"{a}={b}" for a, b in sorted(SECTION_ALIASES.items())))
    out("The older switches --no-aws and --no-logs are aliases that untick the 'aws' and 'logs' sections.")


def _cli_progress(key, status, secs):
    if status != "running":
        print(f"  [{key}] {status}" + (f" ({secs:.1f}s)" if secs else ""), flush=True)


def main():
    global LOOKBACK_MINUTES, EKSLOGIN_EXE, LOG_TAIL_LINES, SUPPORT_LABEL, TRAFFIC_SAMPLE_SECONDS, PARALLEL_WORKERS
    parser = argparse.ArgumentParser(description="EKS debugger: uses the AWS credentials that already exist in ~/.aws (no sign-in in this tool), connects to the chosen "
                                                 "clusters and shows what happened in the last N minutes")
    parser.add_argument("--minutes", type=int, default=None, help=f"time window in minutes (default {LOOKBACK_MINUTES})")
    parser.add_argument("--cluster", help="cluster number(s) or name to debug (no GUI), numbered as in --list: 3 | 1,3,5 | 2-4 | all | my-cluster. "
                                          "Several clusters run one after another and get a combined summary page")
    parser.add_argument("--name", help="label for the report when ONE cluster is given with --cluster (default: from the cluster list)")
    parser.add_argument("--list", action="store_true", help="collect and print the clusters of --profile (a | a,b,c | all) x --region (r | r1,r2 | all): number, name, region, account / profile; "
                                                            "with --connect ekslogin and no --all-clusters: the clusters the ekslogin menu offers. Exits 1 when the credentials of every profile are expired / missing")
    parser.add_argument("--all-clusters", action="store_true",
                        help="with --connect ekslogin: list the clusters found with aws (selected profiles x regions) instead of the ekslogin menu; clusters that are not in the "
                             "menu are connected with aws eks update-kubeconfig. (With the default connection this is what --list does anyway.)")
    parser.add_argument("--skip-login", "--skip-kubeconfig", dest="skip_login", action="store_true",
                        help="don't run ekslogin and don't write the kubeconfig entry; use the current kubectl context as it is")
    parser.add_argument("--ekslogin", help="path to ekslogin.exe (only used with --connect ekslogin)")
    parser.add_argument("--no-gui", action="store_true", help="never open the GUI")
    parser.add_argument("--connect", choices=["creds", "ekslogin"], default=None,
                        help="how to connect to a cluster: creds = use the credentials that already exist in ~/.aws, no sign-in (default); ekslogin = run your own ekslogin.exe "
                             "first (the cluster number is its menu number), then ~/.aws is re-read")
    parser.add_argument("--login-method", choices=["exe", "cli"], default=None, help=argparse.SUPPRESS)      # older spelling of --connect (exe = ekslogin, cli = creds)
    parser.add_argument("--aws-cluster", help="EKS cluster name for the AWS checks (default: read from kubeconfig)")
    parser.add_argument("--region", help="AWS region for the AWS checks (default: read from kubeconfig). For --list / --all-clusters: a region, several (r1,r2) or 'all' to search")
    parser.add_argument("--profile", help="AWS profile (from ~/.aws) whose existing credentials are used (default: what the aws CLI would use - AWS_PROFILE, [default] or the AWS_* variables). "
                                          "For --list / --cluster: several profiles (a,b,c) or 'all' to search them for clusters - nothing is scanned unless you name the scope")
    parser.add_argument("--context", help="kubectl context to use (default: matched from the selected cluster)")
    parser.add_argument("--list-profiles", action="store_true", help="list the AWS profiles found in ~/.aws and exit")
    parser.add_argument("--list-accounts", action="store_true", help="list the AWS profiles of ~/.aws (plus the environment / default credentials) with their credentials status (Active / Expiring soon / Expired / Not configured / Unknown) and exit")
    parser.add_argument("--open", action="store_true", help="open the HTML report in your browser when done")
    parser.add_argument("--no-logs", action="store_true", help="skip pulling pod logs; alias for --skip-sections logs")
    parser.add_argument("--logs-all", action="store_true", help="also read logs of ALL running pods (capped), not just unhealthy / warning / core add-on pods")
    parser.add_argument("--log-namespaces", default="", help="with --logs-all: only these namespaces (comma separated)")
    parser.add_argument("--log-lines", type=int, default=None, help=f"max log lines per container (default {LOG_TAIL_LINES})")
    parser.add_argument("--traffic-sample", type=int, default=None,
                        help=f"seconds to sample live pod/node traffic from the kubelet (default {TRAFFIC_SAMPLE_SECONDS}, 0 = skip)")
    parser.add_argument("--no-aws", action="store_true", help="skip the AWS CLI section (kubectl data only); alias for --skip-sections aws")
    parser.add_argument("--workers", type=int, default=None,
                        help=f"collection tasks that run at the same time after the kubeconfig step (default {PARALLEL_WORKERS}; 1 = one after another, exactly as before). "
                             f"At most {KUBECTL_CONCURRENCY} kubectl and {AWS_CONCURRENCY} aws calls run at the same moment")
    parser.add_argument("--sections", default=None, help="collect only these report sections (comma separated ids; see --list-sections). The overview is always collected")
    parser.add_argument("--skip-sections", default=None, help="do not collect these sections (comma separated ids; see --list-sections)")
    parser.add_argument("--only-networking", action="store_true", help="collect only the network and traffic section (plus the overview; the AWS data it needs is read silently)")
    parser.add_argument("--no-networking", action="store_true", help="collect everything except the network and traffic section")
    parser.add_argument("--list-sections", action="store_true", help="list the report sections (ids, titles, what they need) and exit")
    parser.add_argument("--support-label", default=None, help=f"namespace label that names the team to contact (default {SUPPORT_LABEL})")
    args = parser.parse_args()

    if args.list_sections:
        print_sections()
        return
    try:
        wanted_sections = resolve_cli_sections(args)
    except ValueError as exc:
        parser.error(str(exc))
    if args.workers is not None:
        PARALLEL_WORKERS = max(1, args.workers)
    if args.minutes:
        LOOKBACK_MINUTES = args.minutes
    if args.log_lines:
        LOG_TAIL_LINES = args.log_lines
    if args.support_label:
        SUPPORT_LABEL = args.support_label
    if args.traffic_sample is not None:
        TRAFFIC_SAMPLE_SECONDS = max(0, args.traffic_sample)
    scope = parse_profile_scope(args.profile)
    args.profile = scope[0] if isinstance(scope, list) else (None if scope == "all" else args.profile)
    AWS_OPTS.update(cluster=args.aws_cluster, region=args.region, profile=args.profile, enabled=not args.no_aws, profile_scope=scope)
    if args.ekslogin:
        EKSLOGIN_EXE = args.ekslogin
    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass
    connect = args.connect or ({"exe": "ekslogin", "cli": "creds"}.get(args.login_method) if args.login_method else "creds")
    LOGIN_OPTS.update(method="exe" if connect == "ekslogin" else "cli", source="all" if args.all_clusters else None, all_clusters=args.all_clusters)

    if args.list_accounts:
        print_accounts()
        return

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
        clusters = list_selected_clusters(lambda l: print(l, flush=True))
        if not clusters:
            print("No clusters found (see the messages above)." if _lists_via_cli() else
                  "No clusters found (could not parse the ekslogin menu). Create clusters.json: {\"1\": \"name\", ...}")
            if PREFLIGHT["blocked"] and not PREFLIGHT["usable"]:
                sys.exit(1)
        for k, v in sorted(clusters.items(), key=lambda kv: int(kv[0])):
            t = CLI_TARGETS.get(k) if _lists_via_cli() else None
            extra = ""
            if t:
                extra = f"   [account {t.get('account_id') or '?'}, profile {t.get('profile') or '(default credentials)'}" + (f", connect: {t['via']}" if LOGIN_OPTS["method"] == "exe" else "") + "]"
            print(f"{k} - {v}{extra}")
        return

    if args.cluster or args.skip_login and args.no_gui:
        known = {} if (args.skip_login and not args.cluster) else list_selected_clusters(lambda l: print(l, flush=True))
        if not known and PREFLIGHT["blocked"] and not PREFLIGHT["usable"]:
            sys.exit(1)                 # every profile in scope has expired / missing credentials: the renew message was printed above
        selected = parse_cluster_selection(args.cluster or "0", known)
        if not selected:
            print("ERROR: no valid cluster in --cluster (use a number such as 3, 1,3,5, 2-4, all, or a cluster name from --list).", file=sys.stderr)
            sys.exit(1)
        if len(selected) == 1 and args.name:
            selected = [(selected[0][0], args.name)]
        pl = plan_sections({"sections": wanted_sections, "aws": not args.no_aws, "logs": not args.no_logs})
        print(f"Sections: collecting {len(pl['collect'])} of {len(SECTIONS)}"
              + (f" (skipped by choice: {section_titles_of([k for k in SECTION_BY_ID if k not in pl['collect']])})" if len(pl["collect"]) < len(SECTIONS) else "")
              + f".  Parallel collection: {PARALLEL_WORKERS} worker(s)" + (" (one after another)" if PARALLEL_WORKERS == 1 else "") + ".", flush=True)
        results = run_clusters(
            selected, LOOKBACK_MINUTES, lambda l: print(l, flush=True), args.skip_login, args.context,
            progress=_cli_progress, options={"aws": not args.no_aws, "logs": not args.no_logs, "all_logs": args.logs_all,
                                             "log_namespaces": args.log_namespaces, "sections": wanted_sections, "workers": PARALLEL_WORKERS})
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

    run_gui(LOOKBACK_MINUTES, args.skip_login, args.context, sections=wanted_sections)


if __name__ == "__main__":
    main()
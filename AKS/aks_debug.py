#!/usr/bin/env python3
"""
AKS Debugger - log in to an AKS cluster with akslogin.exe, then collect the basic debugging picture of what
happened (and what is happening) in the last N minutes, and save it as an interactive HTML report + a text report.

    python aks_debug.py                       # GUI: select one OR SEVERAL clusters in the list
    python aks_debug.py --minutes 60          # GUI, default window 60 minutes
    python aks_debug.py --cluster 3           # no GUI: log in to cluster #3 and collect
    python aks_debug.py --cluster 1,3,5       # several clusters, one after another (also: 2-4, or all)
    python aks_debug.py --list                # show the clusters akslogin offers
    python aks_debug.py --cluster 3 --skip-login            # already logged in: don't run akslogin
    python aks_debug.py --cluster 3 --context my-ctx        # force a specific kubectl context
    python aks_debug.py --cluster 3 --subscription <id>     # force an Azure subscription
    python aks_debug.py --list-subscriptions                # show the Azure subscriptions `az` can see

Everything is READ-ONLY (kubectl get / top / logs, and az show / list / metrics / log-analytics query).
The one opt-in exception is the "Privileged roles (PIM)" tab (4th tab; also --pim-list / --pim-activate-all): it lists the roles you hold through
Privileged Identity Management and, only after you confirm, submits SELF-ACTIVATION requests for your own eligible roles. See the README.

What is collected
    * Cluster: context, versions, API server readiness
    * Azure (az CLI): cluster state, node pools, network (VNet subnets + IP capacity, NSG, routes, outbound),
      identities + role assignments, add-ons, VM scale set instance health, control-plane diagnostic settings and
      recent errors / 401-403 denials from Log Analytics
    * Nodes: name + the actual VM (VM scale set / instance, zone, size, priority, pool), status, CPU / memory / disk,
      resource utilization dashboard by namespace, pods on each node, namespaces (pods used vs configured, quotas)
    * Network & traffic: Azure CNI / pod-IP, DNS, services / ingresses / network policies, routing, load balancers,
      NAT, and the TRAFFIC of the selected window from Azure Monitor (node in/out, load balancer, NAT gateway)
    * Who to contact: the namespace label elvh-app-support-dl next to every namespace + a Teams-to-contact list
    * Pods, events, workloads, autoscaling / storage, top consumers, pod logs (unhealthy / warning / core add-ons / all)
    * A timeline of everything that happened in the window, and a health summary

Login methods (--login-method)
    exe (default)  the custom akslogin.exe wrapper, as before.
    cli            the standard Azure CLI (`az`): signs in if needed (device code by default; --no-device-code = browser), lists the
                   clusters, and writes the kubeconfig entry for each selected cluster. Everything after the login
                   (context, cloud details, report) is the same. See the README, section "Login methods".
    e.g.  python aks_debug.py --login-method cli --subscription <id> --cluster all

Requirements: Python 3.9+, kubectl on PATH, akslogin.exe, and the Azure CLI (`az`, logged in) for the Azure parts.
"""

import argparse
import copy
import functools
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import types
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Global settings
# ---------------------------------------------------------------------------

LOOKBACK_MINUTES = 30        # <-- the time window. Override with --minutes (or the GUI box)

MAX_LOG_PODS = 20            # unhealthy pods + pods with Warning events whose logs are pulled
MAX_CORE_LOG_PODS = 12       # core add-on pods (coredns, azure-cns, kube-proxy, CSI ...) whose logs are pulled
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
LOGIN_TIMEOUT = 120          # seconds for akslogin.exe
REPORT_DIR = "reports"
INITIAL_SECTIONS = None      # sections chosen on the command line (None = all); the GUI starts with them ticked
PARALLEL_WORKERS = 8         # collection tasks that run at the same time after the login (--workers N; 1 = one after another, as before)
KUBECTL_CONCURRENCY = 6      # at most this many kubectl calls at the same moment (keeps the API server calm)
AZ_CONCURRENCY = 4           # at most this many `az` calls at the same moment (Azure throttles aggressively)

# ---------------------------------------------------------------------------
# Branding (data-driven: the sibling tools only change these constants and the drawing below)
# ---------------------------------------------------------------------------

CLOUD_NAME = "Microsoft Azure"
CLOUD_SHORT = "Azure"
PRODUCT_NAME = "Azure Kubernetes Service (AKS) Debugger"
REPORT_TITLE_PREFIX = "AKS debug"
BRAND_PRIMARY = "#0078D4"          # Azure blue
BRAND_ACCENT = "#50E6FF"           # Azure cyan
BRAND_DARK = "#005A9E"             # darker blue (hover / pressed / gradient end)
BRAND_PALE = "#EAF4FC"             # pale blue panels
BRAND_TEXT = "#1B2A3A"
BRAND_PALETTE = [BRAND_PRIMARY, BRAND_ACCENT, BRAND_DARK]     # a multi-colour brand (Google Cloud) lists all four colours here
LOGO_BOX = (100, 64)               # width, height of the coordinate box LOGO_SHAPES draws in


def LOGO_SHAPES():
    """The logo as simple shapes in a 100 x 64 box: ('oval'|'rect'|'poly', coordinates, colour). A stylised white cloud with an 'A' mark
    in the brand colours (generic artwork, not a copy of any trademarked file). Used by the Tk banner AND the HTML report (inline SVG)."""
    cloud = "#FFFFFF"
    return [("oval", (4, 30, 44, 62), cloud), ("oval", (22, 8, 66, 54), cloud), ("oval", (46, 20, 94, 62), cloud), ("rect", (24, 38, 72, 62), cloud),
            ("poly", ((41, 18), (52, 18), (66, 54), (55, 54), (47, 33), (30, 54), (24, 54)), BRAND_PRIMARY),
            ("poly", ((47, 33), (56, 54), (36, 54), (40, 46), (50, 46)), BRAND_ACCENT)]


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


# icon name -> (symbol, plain fallback used when the font cannot draw the symbol)
ICONS = {
    "cloud": ("☁", "~"), "key": ("\U0001F510", "*"), "search": ("\U0001F50D", "?"), "helm": ("⎈", "K"), "run": ("▶", ">"),
    "stop": ("⏹", "[]"), "ok": ("✔", "OK"), "fail": ("✖", "X"), "warn": ("⚠", "!"), "node": ("\U0001F5A5", "N"),
    "net": ("\U0001F310", "@"), "report": ("\U0001F4C4", "R"), "reload": ("\U0001F504", "o"), "chart": ("\U0001F4CA", "#"), "pod": ("\U0001F4E6", "P"),
    "bell": ("\U0001F514", "E"), "gear": ("⚙", "W"), "scale": ("\U0001F4C8", "A"), "trophy": ("\U0001F3C6", "T"), "log": ("\U0001F4DC", "L"),
    "clock": ("\U0001F552", "t"), "folder": ("\U0001F4C1", "/"), "layers": ("\U0001F9E9", "+"), "pending": ("○", "o"), "skip": ("➖", "-"),
    "crit": ("\U0001F534", "C"), "high": ("\U0001F7E0", "H"), "med": ("\U0001F7E1", "M"), "info": ("\U0001F535", "I"),
    "speed": ("⚡", "~"), "list": ("\U0001F4CB", "="), "shield": ("\U0001F6E1", "S"),
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
#   locked    always collected (the report identity)
# ---------------------------------------------------------------------------

SECTIONS = [
    {"id": "overview", "title": "Cluster overview", "step": "Cluster overview", "icon": "helm", "locked": True, "needs": (), "resources": (), "usage": False,
     "desc": "cluster name, kubectl context, Kubernetes versions and API server readiness (always collected)"},
    {"id": "azure", "title": "Azure cluster and infrastructure", "step": "Azure AKS details", "icon": "cloud", "needs": (), "resources": (), "usage": False,
     "desc": "az: cluster state, node pools, subnets and security groups, identities, add-ons, VM scale sets, control-plane logging"},
    {"id": "nodes", "title": "Nodes: processor, memory, disk and swap", "step": "Nodes: CPU / memory / disk / swap", "icon": "node", "needs": (),
     "resources": ("nodes", "pods"), "usage": True, "desc": "every node with its VM, status, pressure, CPU / memory / disk / swap and pod slots"},
    {"id": "utilization", "title": "Resource utilization by namespace", "step": "Resource utilization by namespace", "icon": "chart", "needs": (),
     "resources": ("nodes", "pods"), "usage": True, "desc": "the interactive dashboard of requests, limits and real usage per namespace, workload and node"},
    {"id": "nodepods", "title": "Pods on each node", "step": "Pods on each node", "icon": "layers", "needs": (), "resources": ("pods",), "usage": True,
     "desc": "which pods run on which node, with their usage"},
    {"id": "namespaces", "title": "Namespaces: pods used versus configured", "step": "Namespaces: pods used vs configured", "icon": "folder", "needs": (),
     "resources": ("daemonsets", "deployments", "hpa", "jobs", "namespaces", "nodes", "pods", "replicasets", "resourcequotas", "statefulsets"), "usage": True,
     "desc": "pods and resources per namespace against quotas, with the support team of each namespace"},
    {"id": "pods", "title": "Unhealthy pods", "step": "Unhealthy pods", "icon": "pod", "needs": (), "resources": ("pods",), "usage": False,
     "desc": "crash loops, pending pods, image pull errors, restarts, OOM kills"},
    {"id": "events", "title": "Warning events", "step": "Events", "icon": "bell", "needs": (), "resources": ("events",), "usage": False,
     "desc": "the Kubernetes Warning events of the window"},
    {"id": "workloads", "title": "Workloads", "step": "Workloads", "icon": "gear", "needs": (),
     "resources": ("daemonsets", "deployments", "jobs", "replicasets", "statefulsets"), "usage": False,
     "desc": "deployments, stateful sets, daemon sets and jobs that are not at their wanted size"},
    {"id": "network", "title": "Network and traffic", "step": "Network & traffic in the window", "icon": "net", "needs": ("azure",),
     "resources": ("daemonsets", "deployments", "endpoints", "events", "ingresses", "networkpolicies", "nodes", "pods", "services"), "usage": True,
     "desc": "CNI, IP exhaustion, DNS, services, policies, ingress, load balancers, NAT, throttling, traffic of the window and the checklist"},
    {"id": "scaling", "title": "Autoscaling and storage", "step": "Autoscaling, storage, network", "icon": "scale", "needs": (),
     "resources": ("endpoints", "hpa", "namespaces", "pv", "pvc", "services"), "usage": False,
     "desc": "horizontal pod autoscalers, volumes and claims, services without endpoints"},
    {"id": "top", "title": "Top consumers", "step": "Top consumers", "icon": "trophy", "needs": (), "resources": ("pods",), "usage": True,
     "desc": "the pods and nodes using the most CPU, memory and disk"},
    {"id": "logs", "title": "Pod logs", "step": "Pod logs", "icon": "log", "needs": ("pods",), "resources": ("events", "pods"), "usage": False,
     "desc": "logs of unhealthy pods, pods with warnings and core add-ons (all pods on request)"},
    {"id": "timeline", "title": "Timeline", "step": "Timeline", "icon": "clock", "needs": (), "resources": (), "usage": False,
     "desc": "everything that happened in the window, oldest first"},
]
# What every section shows (plain language, for the HTML box under the heading and the intro lines of the text report): the data it holds,
# where it comes from, the time window it covers and how to read the colours. "howto" is the short usage hint.
_SECTION_TEXT = {
    "overview": (
        "The identity of the cluster you are looking at: the kubectl context (the saved connection to the cluster), the Kubernetes client and server versions and whether the "
        "API server (the cluster's front door) reports itself ready. The data comes from kubectl and is a snapshot taken when the report was generated, so no time window applies.",
        "Start here: when the server is not reachable or the readiness check lists failing parts, the other sections can be incomplete. Failing values are shown in red."),
    "azure": (
        "The cluster as Azure sees it, read with the Azure command-line tool (az): cluster and node pool state, subnets with their free IP addresses, network security group rules, "
        "identities and their roles, add-ons, virtual machine scale set instances and control-plane logging. It is a snapshot of the current configuration; only the control-plane log tables cover the report window. "
        "Green (OK) means healthy; red or amber words such as PROBLEM, low IPs or CANNOT HOLD mean attention is needed.",
        "Needs az to be signed in with at least Reader access; when it is not, the section says what is missing. Click a column header to sort a table and type in its filter box to narrow the rows."),
    "nodes": (
        "Every worker node with its status, how much processor, memory, disk and swap space it uses compared with what it can offer, and which Azure virtual machine it runs on. "
        "The usage comes from each node's kubelet (the agent on the node), or from the metrics server when the kubelet cannot be read, as a snapshot during the run. "
        "Red cells mean NotReady or a failing condition, amber cells a warning; the small bars show how full a resource is (amber from 75 percent, red from 90 percent).",
        "Sort the usage table by a column to find the busiest node; the Findings column of the scheduling table says why a node was flagged."),
    "utilization": (
        "An interactive dashboard of processor and memory use by namespace, workload and node: what pods request, what they are limited to and what they really use. "
        "Requests and limits come from kubectl and live usage from the node kubelets, as a snapshot during the run. Bars turn amber at 75 percent and red at 90 percent of a limit. Units: m = millicores (1000 m = one processor core), Mi / Gi = mebibytes / gibibytes.",
        "Switch between processor, memory and disk and between used and requested, sort or filter the namespace list, and click a namespace to see its pods."),
    "nodepods": (
        "For every node, the pods running on it with their processor, memory and disk use next to what they requested and may use at most. "
        "The data comes from kubectl and the node kubelets as a snapshot during the run, largest memory users first. A note such as 'Memory 95% of limit' marks a pod that is close to being stopped for running out of memory.",
        "Use a table's filter box to find a pod or a namespace and click a usage column header to sort by it."),
    "namespaces": (
        "For each namespace: how many pods run compared with how many its workloads are configured to run, its resource quota, the resources its pods use and the support team to contact. "
        "The data comes from kubectl as a snapshot (not limited to the report window). Status OK means every configured pod runs; 'missing' (red) or a nearly full quota (amber) needs attention.",
        "Use the first table to find who to contact; sort the second table by the Status column to bring namespaces with missing pods to the top."),
    "pods": (
        "The pods that are not healthy: crash loops, pending (unschedulable) pods, image pull failures, evictions, restarts and out-of-memory kills. "
        "The data comes from kubectl pod status; a restart counts when it happened inside the report window. Red status words need action, amber ones mean the pod is still waiting.",
        "The Why column explains each problem in words; sort by Container restarts to find the most unstable pod."),
    "events": (
        "The Kubernetes Warning events of the report window (plus notable normal ones such as scaling), counted by reason and listed newest first. "
        "The data comes from kubectl; Kubernetes keeps events for about one hour only. A high count for one reason usually points at the real problem.",
        "Read the reason table first to see what is repeating, then the detailed list for the object and message."),
    "workloads": (
        "Deployments, stateful sets and daemon sets that are not at their wanted size, new rollouts and failed jobs of the report window, and the health of the core add-ons in the kube-system namespace. "
        "The data comes from kubectl. A ready count lower than the wanted count, or the word DEGRADED, is shown in red.",
        "When a table is missing, nothing was found for it and the line above says so."),
    "network": (
        "Whether the cluster network works: how pods get addresses, the name lookup service (DNS), services and ingress, network policies and firewalls, load balancers and outbound connections, and the traffic of the report window. "
        "It reads Kubernetes objects, pod logs, Azure resources and Azure Monitor metrics and never runs anything inside pods. "
        "Each check shows OK (green), Warning (amber), Problem (red) or Not available (grey: the data could not be read, never read that as OK).",
        "Start with the checklist near the end of this section, then open the check cards it points to; every card says what it means and what to do next."),
    "scaling": (
        "Horizontal pod autoscalers that are stuck or at their maximum, persistent volume claims and volumes that are not bound, services without ready endpoints or without an external address, and namespaces stuck terminating. "
        "The data comes from kubectl as a snapshot. A line saying 'no issues' means nothing was found, so no table is shown.",
        "Open the table of the problem you are chasing and use the support team column to know who owns it."),
    "top": (
        "The ten pods using the most processor and the ten using the most memory right now, with their share of the limit they were given. "
        "The data comes from the node kubelets (or the metrics server) as a snapshot during the run. A share close to 100 percent (amber or red bar) means the pod is about to be slowed down or stopped.",
        "Compare the share-of-limit column with the limit itself: 'no limit' means the pod can take everything the node has."),
    "logs": (
        "The recent log lines of unhealthy pods, pods named in warning events and core add-ons (all pods only when requested), read with kubectl logs for the report window. "
        "Lines with error words are red and lines with warnings amber. Logs can contain sensitive information, so share the report with care.",
        "Use the overview table to pick a container, open its log below, and tick 'errors only' to hide everything else."),
    "timeline": (
        "Everything notable that happened inside the report window in time order, oldest first: pod restarts, node changes, warning events, rollouts, failed jobs and control-plane log entries. "
        "It is built from the data of the other sections (kubectl and Azure), so it only covers the sections that were collected.",
        "Click the coloured kind chips to show or hide a kind of entry."),
}
for _s in SECTIONS:
    _s["shows"], _s["howto"] = _SECTION_TEXT[_s["id"]]
SECTION_BY_ID = {s["id"]: s for s in SECTIONS}


def section_text(sid):
    """(what the section shows, how to use it) for a registry section or one of the always-present parts; None for an unknown id."""
    if sid in SECTION_BY_ID:
        return SECTION_BY_ID[sid]["shows"], SECTION_BY_ID[sid]["howto"]
    return EXTRA_SECTIONS.get(sid)

# Parts of the report that are not selectable sections (always present), with their explanation.
EXTRA_SECTIONS = {
    "summary": ("Every problem found in this run, ranked by how serious it is, and the support teams to contact. The findings come from all collected sections and cover the report window.",
                "Click a severity card to hide or show that level, and use the Where links to jump to the section behind a finding."),
    "steps": ("The collection steps of this run (one per section plus the shared cluster data), whether each finished, failed or was skipped, and how long it took.",
              "A failed step is not fatal: the other sections are still reported."),
    "readonly": ("The proof that this tool only reads: how many read-only commands ran, how many were refused by the safety guard and the rules it enforces. A refused command would be listed in red.",
                 "Nothing needs to be done when it says 0 blocked."),
    "timing": ("How long the run took in total and for each step, and how much the parallel collection saved compared with running one step after another.",
               "Compare the total with the sum of the step times to see how much ran in parallel."),
    "glossary": ("The meaning of every technical term and abbreviation used in this report, with its full name in plain language, sorted alphabetically, plus what the severity and status words mean.",
                 "Every table that uses such a term also has a small glossary right before it."),
    "clusters": ("One row per selected cluster: whether its run succeeded, how many findings of each severity it has, which sections were collected, how long it took and a link to its full report.",
                 "Click a column header to sort, use the filter box to find a cluster and open the report link for the details."),
    "allfindings": ("Every finding of every cluster in one list, ranked by severity, with a link to the section of the cluster report that explains it.",
                    "Click a severity card to hide or show that level and use the filter box to search for text."),
}

# What the severity levels and the status words mean (shown once in the Health summary and once in the glossary).
LEGEND_SEVERITY = [
    ("Critical", "Something is broken now and needs action immediately (for example the API server is not ready or a node is NotReady)."),
    ("High", "A serious problem that is likely to hurt users soon or already does (for example crash looping pods or a subnet that cannot hold the maximum size)."),
    ("Medium", "Something that needs attention but is not urgent (for example a nearly full quota or warning events)."),
    ("Information", "A good-to-know observation or a recommendation; nothing is broken."),
]
LEGEND_STATUS = [
    ("OK", "The check ran and found nothing wrong."),
    ("Warning", "The check found something that may become a problem or deserves a look."),
    ("Problem", "The check found something that is wrong."),
    ("Not available", "The data for the check could not be read (for example no permission or the feature is switched off); this is NOT the same as OK."),
]

# Tooltips for column headers whose name alone is not obvious (header text in lower case -> what the column means).
COLUMN_HELP = {
    "severity": "How serious the finding is: Critical, High, Medium or Information (see the legend).",
    "finding": "What was detected, in one sentence.",
    "where": "The section of this report that shows the details; click to jump there.",
    "status": "The current state of the object (for example Ready, NotReady, Running, Pending).",
    "result": "The outcome of the check: OK, Warning, Problem or Not available.",
    "mode": "Whether a node pool is a system pool (runs cluster add-ons) or a user pool (runs your workloads).",
    "priority": "Regular virtual machines, or spot machines that Azure can take away at any time. In a security rule: the evaluation order, lower numbers first.",
    "node pool": "A group of nodes with the same virtual machine size and settings (an Azure virtual machine scale set).",
    "nodes": "Ready nodes out of the wanted number of nodes, with the autoscaler minimum and maximum when the autoscaler is on.",
    "availability zones": "The Azure data centre zones the pool spreads its nodes over.",
    "availability zone": "The Azure data centre zone the node runs in.",
    "maximum pods per node": "The most pods the pool allows on one node; with classic Azure networking every pod takes an address from the subnet.",
    "address prefix": "The range of IP addresses of the subnet, written as address/length.",
    "usable ip addresses": "Addresses in the subnet that can be handed out (Azure keeps 5 for itself).",
    "ip addresses used": "Addresses already taken by network interfaces in the subnet.",
    "ip addresses free": "An estimate: usable addresses minus those taken.",
    "ip addresses needed at maximum size": "Nodes at the pool maximum times the addresses each node reserves; compare with the free addresses.",
    "network security group": "Azure's firewall rules attached to a subnet or a network interface.",
    "route table": "The Azure route table (custom routes) attached to the subnet, if any.",
    "direction": "Inbound = traffic coming into the subnet; Outbound = traffic leaving it.",
    "access": "Allow lets the traffic through; Deny blocks it.",
    "source": "Where the traffic comes from (an address range, a tag such as Internet, or * for anything).",
    "destination port": "The port the rule applies to (* means all ports).",
    "scope": "Where a role applies: the Azure resource, resource group or subscription it is granted on.",
    "role": "The Azure permission set that is granted (for example Network Contributor, AcrPull).",
    "power state": "Whether the virtual machine is running, stopped or deallocated.",
    "provisioning state": "Whether Azure finished creating or updating the object (Succeeded is good).",
    "verb": "The kind of request made to the API server (get, list, create, delete ...).",
    "response code": "The result the API server answered: 401 = not authenticated, 403 = not allowed.",
    "virtual machine scale set / instance": "The Azure virtual machine scale set that runs the node and the number of the machine inside it.",
    "capacity type": "Regular (on demand) or spot (cheap, can be taken away) capacity.",
    "cloud provider identifier": "The unique Azure resource path of the machine, taken from spec.providerID of the node.",
    "pods (running / maximum)": "Running pods on the node out of the maximum the node allows (pending pods in brackets).",
    "processor (cpu) used / allocatable": "Live processor use compared with the processor the node offers to pods (cores).",
    "memory used / allocatable": "Live memory use compared with the memory the node offers to pods.",
    "disk used / total": "Space used on the node's root disk compared with its size.",
    "container image storage used / total": "Space used by downloaded container images compared with the size of that file system.",
    "swap space used / total": "Swap space in use compared with its size; swap in use is a sign of memory pressure.",
    "kubelet version": "The version of the kubelet (the node agent), which normally equals the Kubernetes version of the node.",
    "processor (cpu) requested": "The sum of what pods asked to reserve (not what they use); the scheduler uses it to place pods.",
    "memory requested": "The sum of the memory that pods asked to reserve.",
    "temporary storage requested / allocatable": "Ephemeral storage that pods asked to reserve compared with what the node offers.",
    "findings": "Why this node was flagged (pressure, high usage, recent changes), empty when nothing was found.",
    "support team (distribution list)": "The e-mail list of the team that owns the namespace, read from the namespace label.",
    "container restarts": "How often the containers of the pod were restarted since it started; many restarts mean a crash loop.",
    "processor (cpu) used": "Live processor use in cores (1.0 = one full core, 0.1 = 100 millicores).",
    "processor (cpu) limit": "The most processor the pod may use before it is slowed down (throttled).",
    "memory used": "Live memory use (the working set that cannot be reclaimed easily).",
    "memory limit": "The most memory the pod may use; above it the pod is stopped (out-of-memory kill).",
    "disk used": "Temporary (ephemeral) disk space used by the pod: logs, writable layer and empty directories.",
    "notes": "Extra remarks, for example a pod close to its limit.",
    "pods in total": "All pods of the namespace, whatever their phase.",
    "configured (desired) pods": "The number of pods the namespace's workloads should have: the wanted replicas plus single pods.",
    "pod quota used / limit": "Pods used against the pod limit of the namespace's resource quota, if it has one.",
    "horizontal pod autoscaler minimum - maximum": "The smallest and largest number of replicas the autoscaler may set, with the current count in brackets.",
    "desired": "The number of pods the workload should run.",
    "ready": "How many of the pods or containers are ready to serve traffic.",
    "available": "Pods that have been ready long enough to count as available.",
    "used / limit": "How much of a quota is used compared with the limit.",
    "ready containers": "Containers of the pod that are ready, out of all of its containers.",
    "why": "The reason the pod was flagged, in words.",
    "occurrences": "How many times the event happened in the window.",
    "count": "How many times the same event or request was repeated.",
    "replicas (current / desired / maximum)": "Replicas now, the number the autoscaler wants and the maximum it may use.",
    "storage class": "The kind of storage the claim asked for.",
    "phase": "The life-cycle phase of the object (Bound is healthy for a claim).",
    "processor (cpu) used, % of cluster": "Processor used (or requested when no live usage exists) as a share of what all nodes offer.",
    "memory used, % of cluster": "Memory used (or requested when no live usage exists) as a share of what all nodes offer.",
    "pods with high usage": "Pods at 90 percent or more of their processor or memory limit.",
    "why collected": "The reason this container's log was read (unhealthy pod, warning event, core add-on, all pods).",
    "log": "Current = the running container; previous = the container before its last restart.",
    "error-like lines": "Lines that contain words such as error, fatal, exception or failed.",
    "warning lines": "Lines that contain the word warn or warning.",
    "term": "The word or abbreviation as it appears in the report.",
    "full name": "What the term stands for.",
    "plain-language meaning": "What it means, without jargon.",
    "pricing tier": "The service tier (Standard or Basic) of the resource.",
    "ready / wanted nodes": "Nodes where the component pod is ready out of the nodes where it should run.",
    "what it means": "A plain-language explanation of the value.",
    "evidence found": "What the check actually saw.",
    "what to do next": "The suggested next step.",
    "sections collected": "How many of the report sections were collected for the cluster.",
    "top finding": "The most serious finding of the cluster.",
    "report": "Link to the full report of the cluster.",
    "time": "How long the step or run took.",
    "step": "One collection step of the run.",
    "namespaces": "The namespaces that have problems.",
    "issues": "How many problems were found.",
    "what": "A short description of the problems.",
    "number of issues": "How many problems were found for this support team.",
    "issues in short": "A short list of the problems found in the namespaces of this team.",
    "time taken": "How long the collection for this cluster took.",
    "item": "The name of the value.",
    "value": "The value, as read from the cluster or from Azure.",
    "setting": "The name of the setting.",
    "meaning": "What the word means.",
    "quota": "The name of the resource quota object.",
    "resource": "The kind of resource the quota limits or the request is about.",
    "used now (share of allocatable)": "What all pods use right now and its share of what the nodes offer.",
    "allocatable (what the nodes offer to pods)": "The total the nodes can give to pods (their capacity minus what the system keeps).",
    "requested by pods (share of allocatable)": "The total that pods asked to reserve and its share of what the nodes offer.",
    "log category": "The kind of control-plane log Azure can record.",
    "state": "Whether the feature or add-on is switched on or healthy.",
    "detail": "Extra information about the state.",
    "reason": "The reason given by Kubernetes (or the reason the object was flagged).",
    "object": "The Kubernetes object the event is about: its kind, namespace and name.",
    "message": "The text of the event as written by Kubernetes.",
    "created": "How long ago the object was created.",
    "age": "How long ago the object was created.",
    "roles": "The node roles from its labels (worker nodes usually have none).",
    "replicaset": "The object a Deployment creates for each version of its pods.",
    "kind": "The type of Kubernetes object (Deployment, StatefulSet, DaemonSet ...).",
    "failed pods": "How many pods of the job failed.",
    "persistent volume claim / volume": "The storage request (claim) or volume that has a problem.",
    "horizontal pod autoscaler": "The object that adds or removes pod replicas automatically.",
    "processor (cpu) used, % of its limit": "The processor use as a share of the pod's processor limit; 'no limit' means it may take everything.",
    "memory used, % of its limit": "The memory use as a share of the pod's memory limit; 'no limit' means it may take everything.",
    "last line": "The last log line of the container.",
    "container": "The container inside the pod.",
}
SECTIONS_ORDER = [s["id"] for s in SECTIONS]
SECTION_ALIASES = {"networking": "network", "net": "network", "traffic": "network", "autoscaling": "scaling", "storage": "scaling", "node": "nodes",
                   "namespace": "namespaces", "ns": "namespaces", "pod": "pods", "event": "events", "log": "logs", "cluster": "overview",
                   "utilisation": "utilization", "workload": "workloads", "az": "azure", "consumers": "top"}
BASE_RESOURCES = ("nodes", "pods", "namespaces")        # always read: the cluster cannot be reported without them, and the support teams come from the namespaces
SECTION_PRESETS = {
    "all": [s["id"] for s in SECTIONS],
    "only_networking": ["overview", "network"],
    "no_networking": [s["id"] for s in SECTIONS if s["id"] != "network"],
}


def parse_section_list(text):
    """'azure,nodes,networking' -> ['azure', 'nodes', 'network'] (ids or aliases). Raises ValueError naming the unknown ones."""
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
    'resources': kubectl objects to read, 'usage': read live usage?}. Options: 'sections' (None = all), 'azure' / 'logs' (the older switches)."""
    options = options or {}
    wanted = options.get("sections")
    chosen = {s["id"] for s in SECTIONS} if wanted is None else set(wanted)
    why = {}
    if not options.get("azure", True):
        chosen.discard("azure")
        why["azure"] = "turned off (--no-azure / Azure details unticked)"
    if not options.get("logs", True):
        chosen.discard("logs")
        why["logs"] = "turned off (--no-logs / Pod logs unticked)"
    chosen |= {s["id"] for s in SECTIONS if s.get("locked")}
    hidden = {}
    for sid in [s["id"] for s in SECTIONS]:
        if sid in chosen:
            for dep in SECTION_BY_ID[sid]["needs"]:
                if dep not in chosen and not (dep == "azure" and not options.get("azure", True)):
                    hidden.setdefault(dep, []).append(sid)
    live = chosen | set(hidden)
    resources = set(BASE_RESOURCES)
    for sid in live:
        resources |= set(SECTION_BY_ID[sid]["resources"])
    skipped = {sid: why.get(sid, "not selected") for sid in SECTION_BY_ID if sid not in live}
    return {"collect": chosen, "hidden": hidden, "skipped": skipped, "resources": resources, "usage": any(SECTION_BY_ID[s]["usage"] for s in live)}


# Optional fixed cluster list {"1": "my-cluster-a", "2": "my-cluster-b"}. If empty, the
# list is read from clusters.json (same format) next to this script, otherwise it is
# parsed from the menu that akslogin.exe prints.
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


AKSLOGIN_EXE = os.path.join(_HERE, "akslogin.exe") if os.path.isfile(os.path.join(_HERE, "akslogin.exe")) else ".\\akslogin.exe"


# ---------------------------------------------------------------------------
# READ-ONLY GUARANTEE
#
# This tool only reads. It never installs anything, never creates / changes / deletes anything on the cluster or in the cloud
# account, and never runs a command inside a pod or node. It is enforced HERE, centrally, with allow-lists: every kubectl call goes
# through assert_read_only_kubectl(), every az call through assert_read_only_cloud(), every other process start (the sign-in, the
# local kubeconfig step) through assert_local_only_command(). Anything not on the lists is refused WITHOUT starting a process, the
# attempt is recorded in GUARD.blocked and shown in the report ("Read-only guarantee" block + a CRITICAL finding).
# ---------------------------------------------------------------------------

class ReadOnlyViolation(Exception):
    """Raised by the assert_* guards; the message is the user-visible "blocked: read-only mode - ..." text."""


READ_ONLY_KUBECTL_VERBS = ("get", "logs", "top", "version", "api-resources", "api-versions", "cluster-info", "explain")   # `get --raw` = HTTP GET only
READ_ONLY_KUBECTL_CONFIG = ("get-contexts", "current-context", "view")      # `view` without --raw (secrets stay redacted); the report uses `view --minify` and `view -o json`
LOCAL_ONLY_KUBECTL_CONFIG = ("use-context",)                                  # local-only: switches the context in the LOCAL kubeconfig file, never touches the cluster
READ_ONLY_KUBECTL_AUTH = ("can-i",)                                           # `kubectl auth can-i` only asks the API server
_KUBECTL_PRE_VERB_VALUE_FLAGS = ("--context", "-n", "--namespace", "--kubeconfig", "--request-timeout")   # global flags that may come before the verb
# Flags that run things, send a body, write or change identity: refused on every verb.
KUBECTL_FORBIDDEN_FLAGS = ("-f", "--filename", "-k", "--kustomize", "--data", "--data-binary", "--data-raw", "--patch", "--overwrite", "--force",
                           "--token", "--as", "--as-group", "--as-uid", "--username", "--password", "--client-key", "--client-certificate",
                           "--server", "-s", "--insecure-skip-tls-verify", "-i", "--stdin", "-t", "--tty", "--field-manager", "--save-config",
                           "--log-file", "--log-dir", "--follow")
KUBECTL_FORBIDDEN_RAW_SEGMENTS = ("exec", "attach", "portforward")           # `get --raw` paths that would open a session in a pod

# Every cloud command the report uses. EVERYTHING else is refused. (Only read verbs: list / show / get-upgrades / query.)
READ_ONLY_CLOUD_COMMANDS = {
    "az": (
        ("account", "list"), ("account", "show"), ("account", "get-access-token"), ("extension", "list"), ("graph", "query"),
        ("aks", "list"), ("aks", "show"), ("aks", "get-upgrades"),
        ("network", "vnet", "subnet", "show"), ("network", "nsg", "show"), ("network", "nsg", "list"),
        ("network", "public-ip", "show"), ("network", "public-ip", "list"), ("network", "route-table", "show"),
        ("network", "lb", "list"), ("network", "nat", "gateway", "list"), ("network", "watcher", "flow-log", "list"), ("network", "firewall", "list"),
        ("role", "assignment", "list"), ("ad", "signed-in-user", "show"), ("vmss", "list"), ("vmss", "list-instances"),
        ("monitor", "diagnostic-settings", "list"), ("monitor", "metrics", "list"), ("monitor", "activity-log", "list"),
        ("monitor", "log-analytics", "workspace", "show"), ("monitor", "log-analytics", "query"),
    ),
}
CLOUD_FORBIDDEN_FLAGS = ("--yes", "-y", "--no-wait", "--set", "--add", "--remove", "--force-string", "--allow-preview", "--admin")
# The ONLY commands that are not reads. All are LOCAL-ONLY (they write the local kubeconfig file / the CLI's own sign-in state, never the
# cluster or the cloud account), run with fixed argument shapes, and are checked by assert_local_only_command() / assert_read_only_kubectl().
LOCAL_ONLY_COMMANDS = (
    "az login [--use-device-code] [--tenant T] [-o none] [--only-show-errors]   (interactive user sign-in; local CLI token cache; nothing else)",
    "az aks get-credentials --resource-group --name [--subscription] --overwrite-existing   (writes the LOCAL kubeconfig file only)",
    "kubelogin convert-kubeconfig -l azurecli                      (rewrites the LOCAL kubeconfig file only)",
    "kubectl config use-context <name>                             (switches the context in the LOCAL kubeconfig file only)",
    "akslogin.exe                                                  (your organisation's sign-in tool; sign-in only, started when you choose it)",
    "powershell -NoExit -Command \"az login [--use-device-code] [--tenant T]\"   ('Open a terminal for me': a visible window YOU asked for; exactly these az login forms and nothing else)",
)
LOCAL_INFO_COMMANDS = ("az --version",)      # prints the installed version of the Azure CLI (shown in the 'Azure CLI installed?' line); changes nothing
# Dynamic extension install would put software on this machine the first time `az graph` etc. is used: switched off for every az process
# through the environment (never with `az config set`).
AZ_SAFE_ENV = {"AZURE_EXTENSION_USE_DYNAMIC_INSTALL": "no", "AZURE_EXTENSION_DYNAMIC_INSTALL_ALLOW_PREVIEW": "false"}
READ_ONLY_STATEMENT = "This tool only reads. It does not install, create, change or delete anything on the cluster or in the cloud account."


class _ReadOnlyGuard:
    """Thread-safe counters and the list of refused attempts (shared by the cache layer, the parallel workers and the live sampler)."""

    def __init__(self):
        self._lock = threading.Lock()
        self.reads = 0
        self.local = 0
        self.blocked = []        # (time, tool, command text, reason)
        self.pim = []            # PIM self-activation requests SENT this session (the one opt-in exception): dicts {time, type, name, scope, minutes}

    def read(self):
        with self._lock:
            self.reads += 1

    def local_only(self):
        with self._lock:
            self.local += 1

    def pim_record(self, entry):
        with self._lock:
            self.pim.append(dict(entry))

    def block(self, tool, cmd, reason):
        text = " ".join(str(x) for x in cmd) if isinstance(cmd, (list, tuple)) else str(cmd)
        with self._lock:
            self.blocked.append((datetime.now(timezone.utc).strftime("%H:%M:%S"), tool, text[:300], reason))

    def mark(self):
        with self._lock:
            return (self.reads, self.local, len(self.blocked))

    def since(self, mark):
        """Counts since mark(): {"reads", "local", "blocked": [...]}."""
        with self._lock:
            return {"reads": self.reads - mark[0], "local": self.local - mark[1], "blocked": list(self.blocked[mark[2]:]), "pim": list(self.pim)}

    def reset(self):
        with self._lock:
            self.reads = self.local = 0
            self.blocked = []
            self.pim = []


GUARD = _ReadOnlyGuard()


TENANT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")     # a tenant id (GUID) or domain name for `az login --tenant`


def _blocked_message(what):
    return f"blocked: read-only mode - '{what}' is not allowed"


def _flag_hit(tok, flags):
    """The forbidden flag `tok` is (or starts with, for `--flag=value` and `-fvalue`), else None."""
    if not isinstance(tok, str) or not tok.startswith("-"):
        return None
    for f in flags:
        if tok == f or (f.startswith("--") and tok.startswith(f + "=")) or (len(f) == 2 and not f.startswith("--") and tok.startswith(f) and not tok.startswith("--")):
            return f
    return None


def assert_read_only_kubectl(args):
    """Allow-list for kubectl. `args` = everything after `kubectl`. Returns "read" or "local" (local-only: config use-context);
    raises ReadOnlyViolation (message "blocked: read-only mode - '<verb>' is not allowed") for anything else. Starts no process."""
    if not isinstance(args, (list, tuple)) or not args or not all(isinstance(a, str) for a in args):
        raise ReadOnlyViolation(_blocked_message("(empty or malformed kubectl command)"))
    i, n = 0, len(args)
    while i < n and args[i].startswith("-"):                       # global flags before the verb: only a few, with their value
        name = args[i].split("=", 1)[0]
        if name not in _KUBECTL_PRE_VERB_VALUE_FLAGS:
            raise ReadOnlyViolation(_blocked_message(args[i]))
        i += 1 if "=" in args[i] else 2
    if i >= n:
        raise ReadOnlyViolation(_blocked_message("(no kubectl verb)"))
    verb, rest = args[i].lower(), list(args[i + 1:])
    for tok in rest:
        hit = _flag_hit(tok, KUBECTL_FORBIDDEN_FLAGS)
        if hit:
            raise ReadOnlyViolation(_blocked_message(f"{verb} {hit}"))
    if verb == "config":
        sub = (rest[0].lower() if rest else "")
        if sub in READ_ONLY_KUBECTL_CONFIG:
            if "--raw" in rest:                                    # would print the credentials in the kubeconfig
                raise ReadOnlyViolation(_blocked_message("config view --raw"))
            return "read"
        if sub in LOCAL_ONLY_KUBECTL_CONFIG and len(rest) == 2 and not rest[1].startswith("-"):
            return "local"
        raise ReadOnlyViolation(_blocked_message("config " + (sub or "(none)")))
    if verb == "auth":
        sub = (rest[0].lower() if rest else "")
        if sub in READ_ONLY_KUBECTL_AUTH:
            return "read"
        raise ReadOnlyViolation(_blocked_message("auth " + (sub or "(none)")))
    if verb not in READ_ONLY_KUBECTL_VERBS:
        raise ReadOnlyViolation(_blocked_message(verb))
    if "--raw" in rest or any(t.startswith("--raw=") for t in rest):
        if verb != "get":
            raise ReadOnlyViolation(_blocked_message(f"{verb} --raw"))
        paths = [t for t in rest if t.startswith("/")]
        if len(paths) != 1:
            raise ReadOnlyViolation(_blocked_message("get --raw (needs exactly one API path)"))
        if any(seg.lower() in KUBECTL_FORBIDDEN_RAW_SEGMENTS for seg in re.split(r"[/?:]", paths[0].split("?", 1)[0])):
            raise ReadOnlyViolation(_blocked_message("get --raw " + paths[0]))
    return "read"


def _cloud_words(args):
    """The leading sub-command words of a cloud CLI command (everything before the first option), lower-case."""
    words = []
    for a in args:
        if not isinstance(a, str) or a.startswith("-"):
            break
        words.append(a.lower())
    return tuple(words)


def assert_read_only_cloud(tool, args):
    """Allow-list for cloud CLI calls (`az`). `args` = everything after the executable. Only the read commands of
    READ_ONLY_CLOUD_COMMANDS pass; everything else (create / delete / update / set / extension add / config set / rest ...) raises ReadOnlyViolation."""
    if not isinstance(args, (list, tuple)) or not all(isinstance(a, str) for a in args):
        raise ReadOnlyViolation(_blocked_message("(malformed command)"))
    words = _cloud_words(args)
    if words not in READ_ONLY_CLOUD_COMMANDS.get(tool, ()):
        raise ReadOnlyViolation(_blocked_message(" ".join(words) if words else (args[0] if args else "(none)")))
    if tool == "az" and words == ("account", "get-access-token"):
        return _assert_token_expiry_args(args)          # only the expiry time may be queried - never the token itself
    for tok in args[len(words):]:
        hit = _flag_hit(tok, CLOUD_FORBIDDEN_FLAGS)
        if hit:
            raise ReadOnlyViolation(_blocked_message(" ".join(words) + " " + hit))
    return "read"


def assert_local_only_command(cmd):
    """The few LOCAL-ONLY process starts (sign-in, local kubeconfig). `cmd` = the full command line list. Raises ReadOnlyViolation for any other shape."""
    if not isinstance(cmd, (list, tuple)) or not cmd or not all(isinstance(a, str) for a in cmd):
        raise ReadOnlyViolation(_blocked_message("(malformed command)"))
    exe = os.path.splitext(os.path.basename(cmd[0]))[0].lower()
    args = list(cmd[1:])
    if os.path.normcase(os.path.normpath(cmd[0])) == os.path.normcase(os.path.normpath(AKSLOGIN_EXE)) and not args:
        return "local"
    if exe == "az" and args and args[0].lower() == "login":
        # exactly: az login [--use-device-code] [--tenant <id or domain>] [-o none] [--only-show-errors]   (no service principal, identity, password ...)
        j = 1
        while j < len(args):
            a = args[j]
            if a in ("--use-device-code", "--only-show-errors"):
                j += 1
            elif a == "--tenant" and j + 1 < len(args) and TENANT_RE.match(args[j + 1]):
                j += 2
            elif a in ("-o", "--output") and j + 1 < len(args) and args[j + 1].lower() == "none":
                j += 2
            else:
                raise ReadOnlyViolation(_blocked_message("az login " + a))
        return "local"
    if exe == "az" and [a.lower() for a in args[:2]] == ["aks", "get-credentials"]:
        j, rest = 0, args[2:]
        while j < len(rest):
            if rest[j] in ("--resource-group", "--name", "--subscription") and j + 1 < len(rest):
                j += 2
            elif rest[j] == "--overwrite-existing":
                j += 1
            else:
                raise ReadOnlyViolation(_blocked_message("az aks get-credentials " + rest[j]))
        return "local"
    if exe == "kubelogin" and args == ["convert-kubeconfig", "-l", "azurecli"]:
        return "local"
    raise ReadOnlyViolation(_blocked_message(" ".join([exe, *args[:3]]).strip()))


def assert_local_info(cmd):
    """The one local information command (`az --version`: the first line is shown in the 'Azure CLI installed?' line). Raises ReadOnlyViolation otherwise."""
    if not isinstance(cmd, (list, tuple)) or len(cmd) != 2 or not all(isinstance(a, str) for a in cmd):
        raise ReadOnlyViolation(_blocked_message("(malformed command)"))
    if os.path.splitext(os.path.basename(cmd[0]))[0].lower() == "az" and cmd[1] == "--version":
        return "read"
    raise ReadOnlyViolation(_blocked_message(" ".join(cmd[:3])))


def _az_env():
    """Environment for every az process: no dynamic extension install, ever."""
    env = dict(os.environ)
    env.update(AZ_SAFE_ENV)
    return env


def guard_lines(info):
    """The text of the 'Read-only guarantee' block for a run. info = GUARD.since(mark)."""
    nb = len(info["blocked"])
    lines = [READ_ONLY_STATEMENT, f"Commands used: {info['reads']} read calls, {nb} blocked."]
    if info["local"]:
        lines.append(f"Local-only steps (sign-in / local kubeconfig file, not the cluster): {info['local']}.")
    if nb:
        lines.append(f"CRITICAL: {nb} command(s) that would change something were REFUSED (nothing was run):")
        lines += [f"  {when}  {tool}  {cmd}   -> {why}" for when, tool, cmd, why in info["blocked"]]
    else:
        lines.append("Allowed: kubectl get / logs / top / version / config (read) / api-resources / api-versions / cluster-info / explain / auth can-i; "
                     "az list / show / get-upgrades / query / metrics list / log-analytics query only.")
    lines.append(PIM_EXCEPTION_STATEMENT)
    pim = info.get("pim") or []
    if pim:
        lines.append(f"Privileged roles (PIM) activations requested in this session: {len(pim)} (your own eligible roles, after you confirmed; none came from this report run):")
        lines += [f"  {x['time']}  {x['type']}  {x['name']}  ({x['scope']})  for {pim_duration_text(x['minutes'])}" for x in pim]
    else:
        lines.append("Privileged roles (PIM) activations requested in this session: 0.")
    return lines


# ---------------------------------------------------------------------------
# Login (akslogin, same shape as akslogin)
# ---------------------------------------------------------------------------

def akslogin(cluster_number):
    # akslogin.exe prints a menu and waits on stdin for the cluster number(s).   (local-only: sign-in tool, started by the user's choice)
    try:
        assert_local_only_command([AKSLOGIN_EXE])
    except ReadOnlyViolation as v:
        GUARD.block("akslogin", [AKSLOGIN_EXE], str(v))
        return False
    GUARD.local_only()
    proc = subprocess.run(
        [AKSLOGIN_EXE],
        input=f"{cluster_number}\n",
        capture_output=True,
        text=True,
        shell=True,
    )
    if proc.returncode != 0:
        print(f"[cluster {cluster_number}] akslogin failed: {proc.stderr.strip()}")
        print(proc.stdout[-1000:])
        return False
    time.sleep(2)  # brief buffer for kubeconfig/context to settle
    return True


def exe_menu_clusters() -> dict:
    """{'1': 'cluster-name', ...} offered by the custom login. Order of preference: the CLUSTERS dict, clusters.json next to
    this script, then the menu that akslogin.exe prints when it is given no selection (stdin closed)."""
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
        assert_local_only_command([AKSLOGIN_EXE])
        GUARD.local_only()
        proc = subprocess.run([AKSLOGIN_EXE], input="", capture_output=True, text=True,
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
    - login method cli: every cluster `az` can reach (see list_clusters_cli);
    - login method exe + --all-clusters: every cluster `az` can reach, and the ones that are in the akslogin menu are
      logged in with akslogin as before (the others with `az aks get-credentials`);
    - otherwise: only the akslogin menu (CLUSTERS dict, clusters.json, or the menu akslogin.exe prints)."""
    out = emit or print
    if LOGIN_OPTS["method"] == "cli":
        return list_clusters_cli(out, all_subs=bool(LOGIN_OPTS.get("all_clusters")))
    menu = exe_menu_clusters()
    if LOGIN_OPTS.get("all_clusters"):
        return list_clusters_cli(out, menu=menu, all_subs=True)
    CLI_TARGETS.clear()
    return menu


# ---------------------------------------------------------------------------
# Login method: the standard Azure CLI (`az`) instead of akslogin.exe  (--login-method cli)
# ---------------------------------------------------------------------------

SIGNIN_METHOD_LABELS = {"manual": "I run the command myself (recommended)", "captured": "Show URL and code here (captured)", "console": "Open a console window for me"}
def default_signin_method():
    """manual unless the environment says otherwise (AKS_DEBUG_SIGNIN_METHOD=captured|console|manual; used by automated tests)."""
    v = (os.environ.get("AKS_DEBUG_SIGNIN_METHOD") or "").strip().lower()
    return v if v in SIGNIN_METHOD_LABELS else "manual"


LOGIN_OPTS = {"method": "exe", "device_code": True, "gui": False, "all_clusters": False, "tenant": None,
              "signin": default_signin_method()}   # method: "exe" (akslogin.exe, default) or "cli" (az); device_code: ON by default; signin: manual (default) | captured | console
LOGIN_LABELS = {"exe": "Custom login (akslogin)", "cli": "Cloud CLI (az)"}   # the GUI combobox values
CLI_TARGETS = {}    # cluster number (str) -> what the CLI listing found; fed into AZ_OPTS after the login

DEVICE_CODE_EXPIRY_SECONDS = 900      # a device code is valid for about 15 minutes
SIGNIN_KEEP_LINES = 200               # raw sign-in output lines kept
_URL_RE = re.compile(r"https?://[^\s\"'<>)]+", re.I)
_CODE_RE = re.compile(r"\b(?:enter|use|type|input)\s+(?:the\s+)?(?:device\s+)?code\s*[:=]?\s*([A-Z0-9][A-Z0-9-]{5,14})\b", re.I)
_CODE_FALLBACK_RE = re.compile(r"\bcode\s*[:=]?\s*([A-Z0-9]{8,10})\b")
_JSONISH_RE = re.compile(r'^(?:[\[\]{}]|"[^"]*"\s*:)')


def parse_device_code(text):
    """(url, code) from the text `az login --use-device-code` prints, e.g.
    'To sign in, use a web browser to open the page https://microsoft.com/devicelogin and enter the code ABCD1234 to authenticate.'
    Handles other wording (login.microsoft.com/device, 'use the code', 'code: X') and wrapped lines (whitespace is collapsed).
    Either value is None when it is not (yet) in the text."""
    flat = " ".join(str(text or "").split())
    m = _CODE_RE.search(flat) or _CODE_FALLBACK_RE.search(flat)
    code = m.group(1).upper() if m else None
    urls = [u.rstrip(".,;:!?]") for u in _URL_RE.findall(flat)]
    url = None
    if urls:
        url = next((u for u in urls if "device" in u.lower()), urls[0])
    return url, code


def explain_signin_error(text):
    """Plain-language reason for a failed `az login` (from its error output)."""
    t = " ".join(str(text or "").split())
    low = t.lower()
    if "aadsts50076" in low or "aadsts50079" in low or "multi-factor" in low or "multifactor" in low or "mfa" in low.split():
        return ("Your organisation requires multi-factor authentication (MFA) and it was not completed. Start the sign-in again and approve the "
                "MFA prompt in the browser.")
    if "aadsts53003" in low or "conditional access" in low or "aadsts50105" in low:
        return ("A conditional access policy blocked the sign-in (for example: unmanaged device, location or app not allowed). "
                "Try from a compliant device / the corporate network, or ask your administrator.")
    if "aadsts50020" in low or "aadsts50034" in low or "does not exist in tenant" in low:
        return ("That account does not exist in the chosen tenant (a guest account must sign in to the tenant it was invited to). "
                "Enter the right tenant in the 'Tenant' box, or leave it empty.")
    if "aadsts90002" in low or "aadsts900023" in low or "aadsts700016" in low or ("tenant" in low and ("not found" in low or "invalid" in low or "does not exist" in low)):
        return "The tenant was not found - check the tenant id / domain in the 'Tenant' box (or leave it empty) and try again."
    if "aadsts70016" in low or "expired" in low or "timed out" in low or "timeout" in low:
        return "The device code expired before you signed in (it is valid for about 15 minutes). Press 'Sign in' to get a new code."
    if "aadsts70000" in low or "declined" in low or "cancel" in low:
        return "The sign-in was declined or cancelled in the browser. Press 'Sign in' to try again."
    if "no subscriptions found" in low:
        return ("You signed in, but this account has no Azure subscription. Use another account ('Sign in with a different account') "
                "or ask for the Reader role on a subscription.")
    line = next((l.strip() for l in reversed(str(text or "").splitlines()) if l.strip()), "")
    return (line[:300] if line else "the sign-in did not finish (no details from az)")


def format_signin_box(url, code, tenant=None, minutes=15):
    """The 'Open this URL ... enter this code ...' box for the log / command line."""
    rows = ["To sign in to Azure:",
            f"  1. Open this URL in a browser:  {url or '(see the output above)'}",
            f"  2. Enter this code:             {code or '(see the output above)'}" + (f"      (tenant: {tenant})" if tenant else ""),
            f"  The code is valid for about {minutes} minutes. Waiting for you to sign in ..."]
    w = max(len(r) for r in rows) + 2
    return ["+" + "-" * w + "+"] + ["| " + r.ljust(w - 1) + "|" for r in rows] + ["+" + "-" * w + "+"]


# ---------------------------------------------------------------------------
# Signing in.  Three methods (step 2 of the window, --signin-method on the command line):
#   manual   (DEFAULT)  the tool SHOWS the exact commands (az login --use-device-code, ...); the user runs one in their own Command Prompt / PowerShell,
#                       then presses 'I have signed in - Verify' (the tool only runs the read-only account list / show / expiry checks). The window also
#                       checks every few seconds and notices the sign-in by itself. 'Open a terminal for me' starts a visible PowerShell window with the
#                       chosen `az login` form - a LOCAL-ONLY, user-initiated exception limited to exactly `az login [--use-device-code] [--tenant T]`.
#   captured            `az login --use-device-code` runs with its output captured; URL and code are parsed and shown in the window. When it fails or
#                       shows no URL the window switches to the manual commands.
#   console             the same command in its own visible console window; the tool waits and then re-checks the sign-in.
# The tool never installs anything: the install hints for the Azure CLI are TEXT only.
# ---------------------------------------------------------------------------

MANUAL_POLL_SECONDS = 5         # manual mode: how often the window checks whether the sign-in happened
MANUAL_POLL_CAP = 900           # ... and for how long (seconds) before it stops waiting
NO_URL_SECONDS = 25.0           # captured mode: no URL parsed after this long -> switch to the manual commands
MANUAL_INSTRUCTIONS = "Sign in from your own Command Prompt or PowerShell. If the Azure CLI is not installed yet, install it first, then run this command:"
MANUAL_STEPS = ("Then: open the URL az prints (https://microsoft.com/devicelogin), enter the code, approve the sign-in (MFA), return to this window and "
                "press 'I have signed in - Verify'.")
AZ_INSTALL_URL = "https://learn.microsoft.com/cli/azure/install-azure-cli"
AZ_INSTALL_WINGET = "winget install -e --id Microsoft.AzureCLI"
AZ_MISSING_TEXT = "Azure CLI (az) not found on this computer - install it first"
AZ_INSTALL_HINT = (f"Install it from {AZ_INSTALL_URL}  -  on Windows you can run:  {AZ_INSTALL_WINGET}  "
                   "(then open a NEW terminal window). This tool never installs anything.")
NO_URL_REASON = "Automatic sign-in did not show a URL. Run one of these commands in your own terminal, then press Verify."
FAILED_REASON = "The automatic sign-in did not complete. Run one of these commands in your own terminal, then press Verify."
TENANT_PLACEHOLDER = "<tenant-id>"
_TERMINAL_FORMS = ("device", "device-tenant", "browser", "tenant-device")


def manual_commands(tenant=None, account_tenant=None):
    """The numbered commands of the manual sign-in. Each: {n, key, cmd, note}. `tenant` = the value of the Tenant box (adds the 1b variant line);
    `account_tenant` = the tenant id of the selected account (used in command 3 when the box is empty). These are shown as TEXT; the tool itself
    never runs them (only 'Open a terminal for me' starts the az login forms, on the user's request)."""
    tenant = tenant if (tenant and TENANT_RE.match(tenant)) else None
    t3 = tenant or account_tenant or TENANT_PLACEHOLDER
    items = [{"n": "1", "key": "device", "cmd": "az login --use-device-code",
              "note": "Device code (recommended): prints a URL and a code - open the URL on any computer, enter the code, approve."}]
    if tenant:
        items.append({"n": "1b", "key": "device-tenant", "cmd": f"az login --use-device-code --tenant {tenant}",
                      "note": "Same, for the tenant in the Tenant box."})
    items += [{"n": "2", "key": "browser", "cmd": "az login", "note": "Normal flow: opens your browser by itself."},
              {"n": "3", "key": "tenant-device", "cmd": f"az login --tenant {t3} --use-device-code",
               "note": "For a specific tenant (e.g. a guest account or several organisations)" + ("." if t3 != TENANT_PLACEHOLDER else " - replace <tenant-id> with your tenant id or domain.")},
              {"n": "4a", "key": "list", "cmd": "az account list --output table", "note": "Verify / see your subscriptions after signing in."},
              {"n": "4b", "key": "show", "cmd": "az account show", "note": "Shows who you are signed in as."}]
    return items


_AZ_VERSION = {}       # exe path -> first line of `az --version` (only successful reads are remembered)


def az_cli_line():
    """('Azure CLI installed ...' text, found): PATH lookup only (no process); the version is read separately by az_version()."""
    exe = shutil.which("az")
    if not exe:
        return AZ_MISSING_TEXT, False
    ver = _AZ_VERSION.get(exe)
    return (f"Azure CLI installed: {ver}" if ver else f"Azure CLI installed ({exe})"), True


def az_version(timeout=60):
    """First line of `az --version` (e.g. 'azure-cli 2.60.0'), or None. The only information command the tool runs (assert_local_info)."""
    exe = shutil.which("az")
    if not exe:
        return None
    if exe in _AZ_VERSION:
        return _AZ_VERSION[exe]
    cmd = [exe, "--version"]
    try:
        assert_local_info(cmd)
    except ReadOnlyViolation as v:
        GUARD.block("az", cmd, str(v))
        return None
    GUARD.read()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, stdin=subprocess.DEVNULL, env=_az_env())
    except Exception:
        return None
    first = next((" ".join(l.split()) for l in str(getattr(proc, "stdout", "") or "").splitlines() if l.strip()), "")
    if getattr(proc, "returncode", 1) != 0 or not first:
        return None
    _AZ_VERSION[exe] = first[:120]
    return _AZ_VERSION[exe]


def manual_signin_block(tenant=None, account_tenant=None, reason=None, az_missing=None):
    """The numbered command block as text lines (command line; same content as the window's manual panel, incl. the install hint when az is missing)."""
    if az_missing is None:
        az_missing = not shutil.which("az")
    rows = []
    if reason:
        rows += [reason, ""]
    rows += [MANUAL_INSTRUCTIONS, ""]
    if az_missing:
        rows += [AZ_MISSING_TEXT, "  Install it from " + AZ_INSTALL_URL, "  On Windows you can run:  " + AZ_INSTALL_WINGET,
                 "  (then open a NEW terminal window; this tool never installs anything)", ""]
    for item in manual_commands(tenant, account_tenant):
        rows += [f"  {item['n']}. {item['cmd']}", f"        {item['note']}"]
    rows += ["", MANUAL_STEPS.replace("press 'I have signed in - Verify'.", "press Enter here.")]
    width = max(len(r) for r in rows) + 2
    return ["+" + "-" * width + "+"] + ["| " + r.ljust(width - 1) + "|" for r in rows] + ["+" + "-" * width + "+"]


def verify_signin(account=None):
    """The 'Verify' check (read-only: az account show, the expiry check via get-access-token --query expiresOn, az account list).
    Returns login_status()'s dict; on success it also has "n_subs" and "tok". A failed one has "state": "not_signed_in" and the reason in "detail"
    ("expired": True when the credentials are expired, "no_subs": True when the account sees no subscription)."""
    st = login_status(account)
    if st["state"] != "ok":
        return st
    tok = check_account_token(None, None)
    st = dict(st, tok=tok)
    if tok["state"] == "expired":
        return dict(st, state="not_signed_in", expired=True, detail=tok.get("detail") or "the credentials have expired",
                    hint="Run one of the commands shown in step 2 again.")
    rows, err = load_accounts()
    st["n_subs"] = len(rows)
    if not rows:
        return dict(st, state="not_signed_in", no_subs=True, detail="No subscriptions found" + (f" ({_first_line(err, 120)})" if err else ""),
                    hint="Sign in with an account that has an Azure subscription.")
    return st


def signin_failure_help(res, tenant=None, account_tenant=None):
    """The exact error and which command to try next, after a failed verification. `res` = the dict of verify_signin() (or just the error text)."""
    res = res if isinstance(res, dict) else {"detail": str(res or "")}
    detail = str(res.get("detail") or "unknown error")
    cmds = {i["key"]: i["cmd"] for i in manual_commands(tenant, account_tenant)}
    low = detail.lower()
    if res.get("state") == "no_cli":
        nxt = (f"{AZ_MISSING_TEXT}. Install it from {AZ_INSTALL_URL} (Windows: {AZ_INSTALL_WINGET}), open a NEW terminal window and run: {cmds['device']}")
    elif res.get("no_subs") or "no subscriptions found" in low:
        nxt = (f"You signed in, but this account has no Azure subscription. Sign in with another account or tenant ({cmds['tenant-device']}), "
               "or ask for the Reader role on a subscription.")
    elif res.get("expired") or is_expired_error(detail):
        nxt = f"The sign-in expired or was not completed. Run: {cmds['device']}"
    elif re.search(r"aadsts\d+", low) or "tenant" in low:
        why = explain_signin_error(detail)
        nxt = f"{why} Then run: {cmds['device']}" + (f"   or, for a specific tenant: {cmds['tenant-device']}" if re.search(r"aadsts(50020|50034|90002|900023|700016)|tenant", low) else "")
    else:
        nxt = f"You are not signed in yet. Run: {cmds['device']}   (or: {cmds['browser']})"
    return f"Verification failed: {_first_line(detail, 200)}\nNext: {nxt}   Then press 'I have signed in - Verify' again."


def terminal_signin_plan(form, tenant=None):
    """(Popen args, displayed command, None) or (None, None, reason): the PowerShell window of 'Open a terminal for me'. User-initiated LOCAL-ONLY
    exception: only `az login [--use-device-code] [--tenant T]`, checked by the same strict guard as every other sign-in command."""
    if form not in _TERMINAL_FORMS:
        return None, None, f"'{form}' is not one of the sign-in commands"
    flags = {"device": ["--use-device-code"], "device-tenant": ["--use-device-code", "--tenant", tenant or ""], "browser": [],
             "tenant-device": ["--tenant", tenant or "", "--use-device-code"]}[form]
    if "--tenant" in flags and (not tenant or not TENANT_RE.match(tenant)):
        return None, None, "Type a tenant id or domain in the 'Tenant' box first (command 3 needs a real tenant)."
    if not shutil.which("az"):
        return None, None, AZ_MISSING_TEXT + ". " + AZ_INSTALL_HINT
    cmd = ["az", "login", *flags]
    try:
        assert_local_only_command(cmd)
    except ReadOnlyViolation as v:
        GUARD.block("local", cmd, str(v))
        return None, None, str(v)
    line = " ".join(cmd)
    shell = shutil.which("powershell") or shutil.which("pwsh") or "powershell"
    return [shell, "-NoExit", "-Command", line], line, None


def open_terminal_signin(form, tenant=None, popen=None):
    """Start a visible PowerShell window that runs the chosen `az login` form and stays open (CREATE_NEW_CONSOLE). The tool does not wait for it.
    Returns (ok, command text or reason)."""
    args, line, why = terminal_signin_plan(form, tenant)
    if why:
        return False, why
    if os.name != "nt":
        return False, "Opening a terminal is only done on Windows - copy the command and run it in your own terminal."
    GUARD.local_only()
    try:
        (popen or subprocess.Popen)(args, creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0x10), env=_az_env())
    except Exception as exc:
        return False, f"could not open a terminal: {exc}"
    return True, line


def _use_manual_signin():
    """Command line: the default sign-in method shows the commands and waits for the user (the window has its own manual panel)."""
    return (LOGIN_OPTS.get("signin") or "manual") == "manual" and not LOGIN_OPTS["gui"]


def manual_signin_cli(emit=print, reason=None, input_fn=None, status_fn=None):
    """Command line, manual method: print the numbered command block, wait for Enter, verify (read-only), repeat on failure.
    Returns True when signed in; False on Ctrl+C / no input (stdin is not interactive)."""
    tenant = LOGIN_OPTS.get("tenant")
    for line in manual_signin_block(tenant, None, reason):
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
            n = st.get("n_subs")
            emit(f"Signed in as {st.get('who') or '?'}" + (f" (tenant {st['tenant']})" if st.get("tenant") else "")
                 + (f" - {n} subscription{'s' if n != 1 else ''}" if n is not None else ""))
            return True
        for line in signin_failure_help(st, tenant, st.get("tenant_id")).splitlines():
            emit(line)


def signin_command(device_code=None, tenant=None):
    """The one sign-in command line: az login [--use-device-code] [--tenant T] -o none  (-o none keeps the subscription JSON out of the output;
    --only-show-errors is NOT added because it can hide the device-code message)."""
    dc = LOGIN_OPTS["device_code"] if device_code is None else device_code
    tn = LOGIN_OPTS.get("tenant") if tenant is None else tenant
    cmd = [shutil.which("az") or "az", "login"]
    if dc:
        cmd.append("--use-device-code")
    if tn:
        cmd += ["--tenant", tn]
    return cmd + ["-o", "none"]


def _kill_process(proc):
    """Stop the sign-in process (and on Windows its child processes: az.cmd starts python)."""
    pid = getattr(proc, "pid", None)
    try:
        proc.terminate()
    except Exception:
        pass
    if os.name == "nt" and isinstance(pid, int) and pid > 4:
        try:
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, timeout=10)
        except Exception:
            pass
    try:
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def run_signin(cmd, emit, on_event=None, cancel=None, expiry=DEVICE_CODE_EXPIRY_SECONDS + 10, poll=0.2):
    """Run the interactive sign-in (`az login ...`, the LOCAL-ONLY exception of the read-only guard) with its output CAPTURED and streamed
    line by line (no separate console window). The verification URL and user code are parsed out of az's text and handed to
    on_event({"kind": "code", "url", "code", "expires_at"}); every output line goes to on_event({"kind": "line", "line"}) and emit().
    cancel = threading.Event: setting it terminates the process. Waits until it finishes.
    Returns {"status": "ok" | "failed" | "cancelled" | "expired" | "error", "rc", "url", "code", "lines", "error"}."""
    res = {"status": "error", "rc": None, "url": None, "code": None, "lines": [], "error": "", "started": time.time()}
    ev = on_event or (lambda e: None)
    try:
        assert_local_only_command(cmd)
    except ReadOnlyViolation as v:
        GUARD.block("local", cmd, str(v))
        emit("ERROR: " + str(v))
        res["error"] = str(v)
        return res
    GUARD.local_only()
    emit("Running: " + " ".join([os.path.splitext(os.path.basename(cmd[0]))[0], *cmd[1:]]))
    env = _az_env()
    env.update(PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
    kwargs = {"stdout": subprocess.PIPE, "stderr": subprocess.STDOUT, "stdin": subprocess.DEVNULL, "env": env,
              "text": True, "encoding": "utf-8", "errors": "replace", "bufsize": 1}
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        proc = subprocess.Popen(cmd, **kwargs)
    except Exception as exc:
        res["error"] = f"could not start {cmd[0]}: {exc}"
        emit("ERROR: " + res["error"])
        return res
    q = queue.Queue()

    def reader():
        try:
            for ln in iter(proc.stdout.readline, ""):
                q.put(ln)
        except Exception:
            pass
        q.put(None)
    threading.Thread(target=reader, daemon=True).start()
    buf, hidden, code_at = "", 0, None
    while True:
        try:
            raw = q.get(timeout=poll)
        except queue.Empty:
            raw = ""
        if raw is None:
            break
        line = raw.strip()
        if line:
            if _JSONISH_RE.match(line):
                hidden += 1                                    # the JSON subscription list az may print at the end of a login: not shown
            else:
                shown = line if len(line) <= 300 else line[:300] + " ..."
                if len(res["lines"]) < SIGNIN_KEEP_LINES:
                    res["lines"].append(shown)
                ev({"kind": "line", "line": shown})
                emit("  az: " + shown)
                buf += " " + line
                if code_at is None:
                    url, code = parse_device_code(buf)
                    res["url"], res["code"] = url or res["url"], code or res["code"]
                    if res["code"] and res["url"]:
                        code_at = time.time()
                        for bl in format_signin_box(res["url"], res["code"], LOGIN_OPTS.get("tenant")):
                            emit(bl)
                        ev({"kind": "code", "url": res["url"], "code": res["code"], "expires_at": code_at + DEVICE_CODE_EXPIRY_SECONDS})
        if cancel is not None and cancel.is_set():
            _kill_process(proc)
            res["status"] = "cancelled"
            emit("Sign-in cancelled.")
            return res
        if code_at is not None and time.time() - code_at > expiry:
            _kill_process(proc)
            res["status"] = "expired"
            res["error"] = explain_signin_error("expired")
            emit("Sign-in stopped: " + res["error"])
            return res
    try:
        res["rc"] = proc.wait(timeout=15)
    except Exception:
        _kill_process(proc)
        res["rc"] = proc.poll()
    if hidden:
        emit(f"  (az's subscription list - {hidden} line(s) - is not shown here)")
    if res["rc"] == 0:
        res["status"] = "ok"
    else:
        res["status"] = "failed"
        res["error"] = explain_signin_error("\n".join(res["lines"]))
    return res


def run_signin_console(cmd, emit, cancel=None, expiry=DEVICE_CODE_EXPIRY_SECONDS + 60, poll=0.3):
    """The 'console' sign-in method: the same `az login ...` (LOCAL-ONLY exception of the read-only guard) in its OWN visible console window; this
    function only waits for the process to end. Returns {"status": "ok" | "failed" | "cancelled" | "expired" | "error", "rc", "url", "code", "lines", "error"}."""
    res = {"status": "error", "rc": None, "url": None, "code": None, "lines": [], "error": "", "started": time.time()}
    try:
        assert_local_only_command(cmd)
    except ReadOnlyViolation as v:
        GUARD.block("local", cmd, str(v))
        emit("ERROR: " + str(v))
        res["error"] = str(v)
        return res
    GUARD.local_only()
    emit("Running in its own console window: " + " ".join([os.path.splitext(os.path.basename(cmd[0]))[0], *cmd[1:]]))
    kwargs = {"env": _az_env()}
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_CONSOLE", 0x10)
    try:
        proc = subprocess.Popen(cmd, **kwargs)
    except Exception as exc:
        res["error"] = f"could not start {cmd[0]}: {exc}"
        emit("ERROR: " + res["error"])
        return res
    while True:
        rc = proc.poll()
        if rc is not None:
            break
        if cancel is not None and cancel.is_set():
            _kill_process(proc)
            res["status"] = "cancelled"
            emit("Sign-in cancelled.")
            return res
        if time.time() - res["started"] > expiry:
            _kill_process(proc)
            res["status"] = "expired"
            res["error"] = explain_signin_error("expired")
            emit("Sign-in stopped: " + res["error"])
            return res
        time.sleep(poll)
    res["rc"] = rc
    if rc == 0:
        res["status"] = "ok"
    else:
        res["status"] = "failed"
        res["error"] = f"the sign-in console window ended with exit code {rc} - nothing was signed in"
    return res


def _run_captured(cmd, timeout=120):
    """Run a non-interactive LOCAL-ONLY command (az aks get-credentials / kubelogin convert-kubeconfig: they write the local kubeconfig file only;
    anything else is refused). Returns (ok, first line of its output or error)."""
    try:
        assert_local_only_command(cmd)
    except ReadOnlyViolation as v:
        GUARD.block("local", cmd, str(v))
        return False, str(v)
    GUARD.local_only()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, env=_az_env())
    except subprocess.TimeoutExpired:
        return False, f"timed out after {timeout}s"
    except Exception as exc:
        return False, str(exc)
    text = (proc.stdout if proc.returncode == 0 else (proc.stderr or proc.stdout)) or ""
    return proc.returncode == 0, _first_line(text, 200) if text.strip() else ""


def list_selected_clusters(emit=print):
    """The cluster list of the chosen login method (akslogin.exe menu / clusters.json, or the az CLI)."""
    return list_clusters(emit)


def login_status(account=None):
    """Read-only sign-in check for the window (`az account show`; nothing is changed, no sign-in is started).
    Returns {"state": "ok" | "not_signed_in" | "no_cli", "who", "tenant", "detail", "hint"}."""
    if not shutil.which("az"):
        return {"state": "no_cli", "who": None, "tenant": None, "detail": "The Azure CLI (az) is not installed (it was not found on PATH).",
                "hint": f"Install it from {AZ_INSTALL_URL} (Windows: {AZ_INSTALL_WINGET}), then press 'Check status'."}
    acct, err = az_cli(["account", "show"], None, 30, subscription=False)
    if err:
        return {"state": "not_signed_in", "who": None, "tenant": None, "detail": _first_line(err, 160),
                "hint": ("Run 'az login --use-device-code' in your own terminal (the commands are shown in step 2), then press 'I have signed in - Verify'."
                         if (LOGIN_OPTS.get("signin") or "manual") == "manual" else "Press 'Sign in' (runs: az login --use-device-code); the URL and code are shown in step 2.")}
    who = str(((acct or {}).get("user") or {}).get("name") or "?") if isinstance(acct, dict) else "?"
    tenant = (acct or {}).get("tenantDisplayName") or (acct or {}).get("tenantId") if isinstance(acct, dict) else None
    return {"state": "ok", "who": who, "tenant": tenant, "tenant_id": (acct or {}).get("tenantId") if isinstance(acct, dict) else None,
            "detail": "", "hint": ""}


def cli_sign_in_detailed(emit, on_event=None, cancel=None, tenant=None, device_code=None, console=None):
    """The automatic sign-in (methods 'captured' and 'console'): az login [--use-device-code] [--tenant T] -o none, output captured (see run_signin)
    or in its own console window (console=True; default: LOGIN_OPTS['signin'] == 'console'). Returns the run_signin result dict."""
    if not shutil.which("az"):
        emit(f"Azure CLI (az) was not found on PATH - install it first: {AZ_INSTALL_URL}   (Windows: {AZ_INSTALL_WINGET}), then run: az login --use-device-code")
        return {"status": "error", "rc": None, "url": None, "code": None, "lines": [], "error": "The Azure CLI (az) is not installed (not found on PATH)."}
    if console is None:
        console = (LOGIN_OPTS.get("signin") == "console")
    if console:
        return run_signin_console(signin_command(device_code, tenant), emit, cancel)
    return run_signin(signin_command(device_code, tenant), emit, on_event, cancel)


def cli_sign_in_any(emit, reason=None):
    """Command-line sign-in with the chosen method: manual (default: print the commands, wait for Enter, verify) or captured / console (az login is run).
    Returns a run_signin-style dict (status 'ok' / 'failed' / 'cancelled' ...)."""
    if _use_manual_signin():
        ok = manual_signin_cli(emit, reason=reason)
        return {"status": "ok" if ok else "cancelled", "rc": 0 if ok else None, "url": None, "code": None, "lines": [],
                "error": "" if ok else "no sign-in was verified (manual sign-in stopped)"}
    return cli_sign_in_detailed(emit)


def cli_sign_in(emit, account=None, **kw):
    """True when the sign-in finished OK (see cli_sign_in_detailed)."""
    return cli_sign_in_detailed(emit, **kw)["status"] == "ok"


def cli_ensure_az(emit):
    """True when the Azure CLI is installed and logged in (`az account show`). If not, runs `az login --use-device-code` (device-code is the
    default; --no-device-code = browser flow), prints the URL / code box, and checks again."""
    exe = shutil.which("az")
    if not exe:
        if _use_manual_signin():          # the default: print the commands (with the install hint), wait for Enter, verify
            return manual_signin_cli(emit, reason=f"{AZ_MISSING_TEXT}.")
        emit(f"Azure CLI (az) was not found on PATH - install it first: {AZ_INSTALL_URL}   (Windows: {AZ_INSTALL_WINGET}), then run: az login --use-device-code")
        return False

    def who(acct):
        return str(((acct or {}).get("user") or {}).get("name") or "?") if isinstance(acct, dict) else "?"
    acct, err = az_cli(["account", "show"], None, 30, subscription=False)
    if not err:
        tok = check_account_token(None, None)
        if tok["state"] != "expired":
            emit(f"Azure CLI is signed in as {who(acct)}")
            return True
        set_account_status(who(acct), "expired", None, tok["detail"])
        emit(f"Credentials for {who(acct)} expired - sign in again." + ("" if _use_manual_signin() else " Starting the device-code sign-in ..."))
        why = f"Credentials for {who(acct)} expired. Sign in again."
    else:
        emit(f"Azure CLI is not logged in: {_first_line(err, 110)}")
        why = "Azure CLI is not logged in."
    res = cli_sign_in_any(emit, why)
    if res["status"] != "ok":
        emit("az login failed or was cancelled" + (f" (exit code {res['rc']})" if res.get("rc") else "") + (": " + res["error"] if res.get("error") else "") + ".")
        return False
    acct, err = az_cli(["account", "show"], None, 30, subscription=False)
    if err:
        emit(f"Still not logged in after az login: {_first_line(err, 110)}")
        return False
    emit(f"Azure CLI login OK - signed in as {who(acct)}")
    return True


def load_accounts():
    """Every Azure subscription `az` can see (no cap): ([{id, name, code, info, usable}], error_or_None), sorted by name."""
    subs, err = _az_subscriptions()
    rows = [{"id": sid, "name": i.get("name") or sid, "code": sid,
             "info": " ".join(x for x in ((i.get("state") or ""), ("[default]" if i.get("default") else "")) if x),
             "usable": i.get("state") in (None, "Enabled"), "user": i.get("user"), "tenant": i.get("tenant"), "kind": i.get("kind"),
             "tenant_name": i.get("tenant_name")} for sid, i in subs.items()]
    return sorted(rows, key=lambda a: (a["name"].lower(), a["id"])), err


# ---------------------------------------------------------------------------
# Accounts known to the Azure CLI and whether their credentials are still valid (read-only)
# ---------------------------------------------------------------------------

EXPIRING_SOON_SECONDS = 30 * 60       # under 30 minutes left = 'Expiring soon'
ACCOUNT_CHECK_WORKERS = 4             # accounts checked at the same time
CRED_EXPIRED_PATTERNS = ("aadsts70043", "aadsts700082", "aadsts50173", "aadsts70008", "aadsts50132", "refresh token has expired",
                         "refresh token is expired", "please run 'az login'", 'please run "az login"', "interactive authentication is needed",
                         "failed to refresh", "token has expired", "credentials have expired", "credentials expired", "session has expired",
                         "authentication token has expired", "az login' to setup account", "run az login")
_TOKENISH_RE = re.compile(r"eyJ[A-Za-z0-9_\-]{8,}(?:\.[A-Za-z0-9_\-]+){0,2}|[A-Za-z0-9_\-+/=]{80,}")
_EXPIRES_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?")
# status -> (label shown on the chip, colour kind of the window)
ACCOUNT_STATES = {"active": "Active", "expiring": "Expiring soon", "expired": "Expired", "none": "Not signed in", "unknown": "Unknown"}

CRED = {"owner": {}, "status": {}, "events": 0, "hook": None, "current": None, "lock": threading.Lock()}
# owner: subscription id (lower) -> account (user name); status: account -> {"state", "left", "detail", "checked"}


def scrub_secrets(text):
    """Remove anything that looks like a token (JWT / long base64 string) from text before it is shown or logged."""
    return _TOKENISH_RE.sub("<hidden>", str(text or ""))


def is_expired_error(text):
    """True when an az / kubelogin error means the sign-in (refresh token / session) expired: the user has to sign in again."""
    low = " ".join(str(text or "").lower().split())
    return any(p in low for p in CRED_EXPIRED_PATTERNS)


def parse_expires_on(text, now=None):
    """Seconds left until the `expiresOn` value az printed (local time, e.g. '2026-10-04 15:03:12.000000'); None when it is not a time."""
    m = _EXPIRES_RE.search(str(text or ""))
    if not m:
        return None
    try:
        dt = datetime(*[int(x) for x in m.groups()[:6]])
    except ValueError:
        return None
    return (dt - (now or datetime.now())).total_seconds()


def token_expiry_args(subscription=None, tenant=None):
    """The ONE shape of `az account get-access-token` that is allowed: only the expiry time is queried, so the token itself never reaches this tool."""
    args = ["account", "get-access-token", "--query", "expiresOn", "-o", "tsv"]
    if tenant:
        args += ["--tenant", tenant]
    if subscription:
        args += ["--subscription", subscription]
    return args


def _assert_token_expiry_args(args):
    """Guard for `account get-access-token`: exactly --query expiresOn -o tsv [--tenant T] [--subscription S] [--only-show-errors]; any variant
    that could print the token (no --query, --query accessToken, -o json, --resource / --scope ...) raises ReadOnlyViolation."""
    rest = list(args[2:])
    seen_q = seen_o = False
    j = 0
    while j < len(rest):
        a = rest[j]
        if a == "--query" and j + 1 < len(rest) and rest[j + 1] == "expiresOn" and not seen_q:
            seen_q, j = True, j + 2
        elif a in ("-o", "--output") and j + 1 < len(rest) and rest[j + 1].lower() == "tsv" and not seen_o:
            seen_o, j = True, j + 2
        elif a in ("--tenant", "--subscription") and j + 1 < len(rest) and TENANT_RE.match(rest[j + 1]):
            j += 2
        elif a == "--only-show-errors":
            j += 1
        else:
            raise ReadOnlyViolation(_blocked_message("account get-access-token " + a + " (the token must never be printed)"))
    if not (seen_q and seen_o):
        raise ReadOnlyViolation(_blocked_message("account get-access-token without --query expiresOn -o tsv (the token must never be printed)"))
    return "read"


def set_account_status(user, state, left=None, detail=""):
    with CRED["lock"]:
        CRED["status"][user] = {"state": state, "left": left, "detail": scrub_secrets(detail)[:300], "checked": time.time()}


def status_text(st):
    """'Active (42 min left)', 'Expiring soon (12 min left)', 'Credentials expired - sign in again', 'Not signed in', 'Unknown: reason'."""
    if not st:
        return "Not checked yet"
    s, left = st.get("state"), st.get("left")
    mins = (lambda x: f"{int(x // 3600)} h {int(x % 3600 // 60)} min" if x >= 3600 else f"{max(1, int(x // 60))} min")
    if s == "active":
        return "Active" + (f" ({mins(left)} left)" if left else "")
    if s == "expiring":
        return "Expiring soon" + (f" ({mins(left)} left)" if left else "")
    if s == "expired":
        return "Credentials expired - sign in again"
    if s == "none":
        return "Not signed in"
    return "Unknown" + (f": {st.get('detail')}" if st.get("detail") else "")


def check_account_token(subscription=None, tenant=None, timeout=60, now=None):
    """Read-only check of one account's credentials: `az account get-access-token --query expiresOn -o tsv [--tenant T] [--subscription S]`
    (only the expiry time is printed). Returns {"state": active | expiring | expired | none | unknown, "left": seconds or None, "detail"}."""
    args = token_expiry_args(subscription, tenant)
    try:
        assert_read_only_cloud("az", args)
    except ReadOnlyViolation as v:
        GUARD.block("az", ["az", *args], str(v))
        return {"state": "unknown", "left": None, "detail": str(v)}
    exe = shutil.which("az")
    if not exe:
        return {"state": "none", "left": None, "detail": "Azure CLI (az) was not found on PATH"}
    GUARD.read()
    try:
        proc = subprocess.run([exe, *args, "--only-show-errors"], capture_output=True, text=True, encoding="utf-8", errors="replace",
                              timeout=timeout, env=_az_env())
    except subprocess.TimeoutExpired:
        return {"state": "unknown", "left": None, "detail": f"timed out after {timeout}s"}
    except Exception as exc:
        return {"state": "unknown", "left": None, "detail": scrub_secrets(exc)[:160]}
    if proc.returncode != 0:
        err = scrub_secrets(proc.stderr or proc.stdout)
        if is_expired_error(err):
            return {"state": "expired", "left": None, "detail": _first_line(err, 160)}
        return {"state": "unknown", "left": None, "detail": _first_line(err, 160)}
    left = parse_expires_on(proc.stdout, now)          # the output is only parsed, never kept or shown
    if left is None:
        return {"state": "unknown", "left": None, "detail": "az printed no expiry time"}
    if left <= 0:
        return {"state": "expired", "left": None, "detail": "the access token has expired"}
    return {"state": "expiring" if left < EXPIRING_SOON_SECONDS else "active", "left": left, "detail": ""}


def build_accounts(rows):
    """The accounts known to the CLI from the subscription rows (`az account list`): one per user / service principal / managed identity:
    [{"key", "user", "kind", "tenants": [(id, name)], "n", "sub_ids", "check_sub", "check_tenant", "label"}], sorted by name."""
    by = {}
    for r in rows:
        u = r.get("user") or "?"
        a = by.setdefault(u, {"key": u, "user": u, "kind": r.get("kind") or "user", "tenants": {}, "sub_ids": [], "usable": []})
        a["tenants"].setdefault(r.get("tenant") or "?", r.get("tenant_name") or r.get("tenant") or "?")
        a["sub_ids"].append(r["id"])
        if r.get("usable", True):
            a["usable"].append(r["id"])
    out = []
    for u in sorted(by, key=str.lower):
        a = by[u]
        kind = {"user": "user", "serviceprincipal": "service principal"}.get(str(a["kind"]).lower(), str(a["kind"]))
        if u.lower() in ("systemassignedidentity", "userassignedidentity") or "identity" in u.lower() and kind != "user":
            kind = "managed identity"
        ts = sorted(a["tenants"].items(), key=lambda kv: str(kv[1]).lower())
        tl = ts[0][1] + (f" (+{len(ts) - 1} more)" if len(ts) > 1 else "")
        n = len(a["sub_ids"])
        out.append({"key": u, "user": u, "kind": kind, "tenants": ts, "n": n, "sub_ids": a["sub_ids"],
                    "check_sub": (a["usable"] or a["sub_ids"] or [None])[0], "check_tenant": ts[0][0] if len(ts) == 1 and ts[0][0] != "?" else None,
                    "label": f"{u}" + ("" if kind == "user" else f"  [{kind}]") + f"  |  {tl}  |  {n} subscription{'s' if n != 1 else ''}"})
    return out


def check_accounts(accounts, on_result=None, cancel=None, workers=ACCOUNT_CHECK_WORKERS):
    """Check several accounts' credentials, at most `workers` at the same time. Updates CRED['status']; on_result(account, status) per account."""
    def one(a):
        if cancel is not None and cancel.is_set():
            return a, None
        st = check_account_token(a.get("check_sub"), a.get("check_tenant"))
        set_account_status(a["key"], st["state"], st["left"], st["detail"])
        if on_result:
            on_result(a, st)
        return a, st
    if not accounts:
        return []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(accounts)))) as ex:
        return list(ex.map(one, accounts))


def register_account_owners(rows):
    """Remember which account each subscription belongs to (for 'whose credentials expired?' when a call fails)."""
    with CRED["lock"]:
        CRED["owner"] = {r["id"].lower(): (r.get("user") or "?") for r in rows if r.get("id")}


def _note_expired(args, target, err):
    """Called when an az call failed with an expired-credentials error: marks the owning account Expired and tells the window (CRED['hook'])."""
    sub = ((target or {}).get("subscription") or "").lower()
    if not sub:
        a = list(args) if isinstance(args, (list, tuple)) else []
        for k, tok in enumerate(a):
            if tok in ("--subscription", "--subscriptions") and k + 1 < len(a):
                sub = a[k + 1].lower()
    user = CRED["owner"].get(sub) or CRED.get("current")
    if not user and len(set(CRED["owner"].values())) == 1:
        user = next(iter(CRED["owner"].values()))
    user = user or "?"
    with CRED["lock"]:
        CRED["events"] += 1
    set_account_status(user, "expired", None, _first_line(scrub_secrets(err), 160))
    hook = CRED.get("hook")
    if hook:
        try:
            hook(user, scrub_secrets(err))
        except Exception:
            pass
    return user


def account_of_cluster(number):
    """The account (user name) that owns the subscription of cluster `number` (from the CLI listing), or None."""
    tgt = CLI_TARGETS.get(str(number)) or {}
    return CRED["owner"].get((tgt.get("subscription") or tgt.get("account") or "").lower())


def list_accounts_cli(emit=print, sign_in=True):
    """`--list-accounts`: print every account the CLI knows with the state of its credentials; an expired one is reported clearly and (with
    sign_in) the device-code sign-in is started. Returns 0 when nothing is expired / the sign-in worked, else 1."""
    if not shutil.which("az"):
        emit(f"Azure CLI (az) was not found on PATH - install it first: {AZ_INSTALL_URL}   (Windows: {AZ_INSTALL_WINGET}), then run: az login --use-device-code")
        return 1
    rows, err = load_accounts()
    if not rows:
        emit("No Azure accounts found" + (f" ({_first_line(err, 120)})" if err else "") + " - not signed in. " +
             (("Starting the sign-in ..." if not _use_manual_signin() else "") if sign_in else "Run: az login --use-device-code"))
        if sign_in:
            return 0 if cli_sign_in_any(emit, "No Azure account is signed in.")["status"] == "ok" else 1
        return 1
    register_account_owners(rows)

    def show():
        accts = build_accounts(rows)
        check_accounts(accts)
        emit("Accounts known to the Azure CLI:")
        for a in accts:
            emit(f"  {a['label']}")
            emit(f"      status: {status_text(CRED['status'].get(a['key']))}")
        return accts
    accts = show()
    bad = [a for a in accts if (CRED["status"].get(a["key"]) or {}).get("state") == "expired"]
    if not bad:
        return 0
    for a in bad:
        emit(f"Credentials for {a['user']} expired - sign in again.")
    if not sign_in:
        return 1
    if not _use_manual_signin():
        emit("Starting the device-code sign-in ...")
    res = cli_sign_in_any(emit, "Credentials expired. Sign in again.")
    if res["status"] != "ok":
        emit("Sign-in " + {"cancelled": "was cancelled", "expired": "timed out"}.get(res["status"], "failed") + ": " + (res.get("error") or "no details"))
        return 1
    rows, _e = load_accounts()
    register_account_owners(rows)
    show()
    return 0


GRAPH_QUERY = ("Resources | where type =~ 'microsoft.containerservice/managedclusters' "
               "| project id, name, resourceGroup, location, subscriptionId, aad = isnotnull(properties.aadProfile)")
LIST_WORKERS = 8             # parallel `az aks list` calls
GRAPH_MIN_SUBS = 5           # from this many subscriptions on, one Resource Graph query replaces one call per subscription
BIG_SCOPE = 20               # more subscriptions than this: the window asks before searching them
COLLECT_HINT = "Select one or more subscriptions above, then press 'Collect clusters from the selected subscriptions'."
SCAN_SCOPE = {"value": None}  # command line: None = the current / default subscription only, "all", or a list of ids / names (--subscription a,b,c | all)
GRAPH_CHUNK = 150            # subscriptions per Resource Graph query (keeps the command line under the 8191 characters cmd.exe allows)


def _has_extension(name):
    """True when this az extension is installed (extensions are never installed from here)."""
    data, err = az_cli(["extension", "list"], None, 30, subscription=False)
    return isinstance(data, list) and any(isinstance(x, dict) and (x.get("name") or "").lower() == name for x in data)


def graph_available():
    """True when the `resource-graph` az extension is installed (it is never installed from here)."""
    return _has_extension("resource-graph")


def _aks_graph(sub_ids, cancel=None):
    """AKS clusters of these subscriptions with ONE Azure Resource Graph query (paged). Returns (rows_or_None, error_or_None)."""
    rows, skip = [], 0
    while True:
        if cancel is not None and cancel.is_set():
            return rows, None
        data, err = az_cli(["graph", "query", "-q", GRAPH_QUERY, "--subscriptions", *sub_ids, "--first", "1000", "--skip", str(skip)],
                           None, 120, subscription=False)
        if err or not isinstance(data, dict):
            return None, err or "unexpected output from az graph query"
        page = data.get("data") or []
        rows += [r for r in page if isinstance(r, dict)]
        if len(page) < 1000:
            return rows, None
        skip += len(page)


def _aks_row(c, sid, sub_name):
    m = re.search(r"/subscriptions/([^/]+)/", c.get("id") or "", re.I)
    sub = sid or c.get("subscriptionId") or (m.group(1) if m else None)
    rg = c.get("resourceGroup")
    return {"name": c.get("name"), "resource_group": rg, "location": c.get("location"), "subscription": sub, "sub_name": sub_name,
            "aad": bool(c.get("aadProfile") or c.get("aad")), "where": f"{c.get('location') or '?'}/{rg}", "account": sub,
            "account_name": sub_name, "key": f"{sub}/{rg}/{c.get('name')}".lower()}


LAST_SCAN = {"subs": 0, "failed": []}     # what the last scan_clusters() searched: number of subscriptions, names of the ones that failed


def scan_summary(found):
    """One clear line for the last scan: 'Found 57 clusters in 12 subscriptions (15 searched), 3 subscriptions failed: a, b, c'."""
    n = len(found)
    with_clusters = len({(c.get("subscription") or "") for c in found})
    failed = LAST_SCAN.get("failed") or []
    msg = f"Found {count_word(n, 'cluster')} in {count_word(with_clusters, 'subscription')}"
    if LAST_SCAN.get("subs", 0) > with_clusters:
        msg += f" ({LAST_SCAN['subs']} searched)"
    if failed:
        msg += f", {count_word(len(failed), 'subscription')} failed: " + ", ".join(failed[:10]) + (" ..." if len(failed) > 10 else "")
    return msg


def count_word(n, noun):
    return f"{n} {noun}" + ("" if n == 1 else "s")


def _numkey(k):
    return int(k) if str(k).isdigit() else 10**9


def scan_clusters(accounts, emit=print, progress=None, cancel=None, on_batch=None):
    """AKS clusters in the given subscriptions ([{id, name}], no limit): returns (clusters, failed_count), de-duplicated and in
    subscription order. From GRAPH_MIN_SUBS subscriptions on, ONE Azure Resource Graph query per GRAPH_CHUNK subscriptions is used
    when the resource-graph extension is installed; everything else is listed with `az aks list` in LIST_WORKERS parallel calls.
    progress(done, total) counts subscriptions; on_batch(new_clusters) gets clusters as they arrive. Stops early when `cancel` is set."""
    note = progress or (lambda *a: None)
    total = len(accounts)
    names = {(a["id"] or "").lower(): a.get("name") for a in accounts}
    order = {(a["id"] or "").lower(): i for i, a in enumerate(accounts)}
    found, seen, failed, done = [], set(), [], 0

    def add(rows):
        new = []
        for c in rows:
            if c["name"] and c["resource_group"] and c["key"] not in seen:
                seen.add(c["key"])
                found.append(c)
                new.append(c)
        if new and on_batch:
            on_batch(new)

    def stopped():
        return cancel is not None and cancel.is_set()

    todo = [a["id"] for a in accounts]
    if total >= GRAPH_MIN_SUBS and None not in todo and graph_available():
        emit(f"Listing clusters with Azure Resource Graph ({total} subscriptions) ...")
        left = []
        for i in range(0, total, GRAPH_CHUNK):
            chunk = todo[i:i + GRAPH_CHUNK]
            if stopped():
                break
            rows, err = _aks_graph(chunk, cancel)
            if rows is None:
                emit(f"  Resource Graph query failed ({_first_line(err, 110)}) - listing {len(chunk)} subscription(s) one by one.")
                left += chunk
                continue
            rows.sort(key=lambda r: (r.get("name") or "").lower())
            add([_aks_row(r, r.get("subscriptionId"), names.get((r.get("subscriptionId") or "").lower())) for r in rows])
            done += len(chunk)
            note(done, total)
        todo = [] if stopped() else left
    if todo:
        def one(sid):
            if stopped():
                return sid, None, "cancelled"
            data, err = az_cli(["aks", "list"], {"subscription": sid}, timeout=120)
            if err or not isinstance(data, list):
                return sid, None, err or "unexpected output"
            return sid, [_aks_row(c, sid, names.get((sid or "").lower())) for c in data if isinstance(c, dict)], None
        with ThreadPoolExecutor(max_workers=max(1, min(LIST_WORKERS, len(todo)))) as pool:
            futures = [pool.submit(one, sid) for sid in todo]
            for fut in as_completed(futures):
                if fut.cancelled():
                    continue
                sid, rows, err = fut.result()
                if err == "cancelled":
                    continue
                done += 1
                if rows is None:
                    failed.append(sid)
                    if len(failed) <= 5:
                        emit(f"  subscription {names.get((sid or '').lower()) or sid or '(default)'}: could not list AKS clusters: {_first_line(err, 110)}")
                else:
                    add(rows)
                note(done, total)
                if stopped():
                    for f in futures:
                        f.cancel()
        if len(failed) > 5:
            emit(f"  ... {len(failed)} of {total} subscription(s) could not be read (no access, or no Reader role).")
    found.sort(key=lambda c: order.get((c["subscription"] or "").lower(), len(order)))      # stable: keeps the listed order inside a subscription
    LAST_SCAN.update(subs=total, failed=[names.get((s or "").lower()) or s or "(default)" for s in failed])
    return found, len(failed)


def merge_menu(found, menu):
    """Mark which clusters of the az listing are also in the akslogin menu (key 'exe_number': log in with akslogin as before), and
    append the menu entries az did not find (key 'exe_only') so nothing the menu offers is lost. A menu entry whose name belongs to
    SEVERAL clusters (same name in two subscriptions) is ambiguous: none of them is mapped to the exe - they use az aks get-credentials,
    which knows the exact subscription and resource group."""
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
            rows.append({"name": base or text, "resource_group": None, "location": None, "subscription": None, "sub_name": None, "aad": False,
                         "where": "akslogin menu", "account": None, "account_name": "", "key": f"menu/{num}", "exe_number": str(num), "exe_only": True,
                         "menu_label": text})
    return rows


def register_clusters(found, multi=False):
    """Number the clusters 1..N in the given order, fill CLI_TARGETS and return {'1': 'name (location/resource-group[/subscription])'}."""
    CLI_TARGETS.clear()
    clusters = {}
    for i, c in enumerate(found, start=1):
        CLI_TARGETS[str(i)] = c
        extra = f"/{c['sub_name']}" if multi and c.get("sub_name") else ""
        if c.get("exe_only"):
            clusters[str(i)] = f"{c['name']} (akslogin menu #{c['exe_number']})"
        else:
            clusters[str(i)] = f"{c['name']} ({c['location'] or '?'}/{c['resource_group']}{extra})"
    return clusters


def describe_cluster(number):
    """'name | resource group | subscription | location' of a numbered cluster of the az listing (for --list)."""
    t = CLI_TARGETS.get(str(number)) or {}
    sub = t.get("sub_name") or t.get("subscription") or "-"
    if t.get("sub_name") and t.get("subscription"):
        sub = f"{t['sub_name']} ({t['subscription']})"
    return (f"name: {t.get('name') or '-'} | resource group: {t.get('resource_group') or '-'} | subscription: {sub} | location: {t.get('location') or '-'}"
            + (f" | akslogin menu #{t['exe_number']}" if t.get("exe_number") else ""))


def parse_subscription_scope(text):
    """--subscription value -> None (not given), "all", or a list of ids / names (comma separated)."""
    t = (text or "").strip()
    if not t:
        return None
    if t.lower() == "all":
        return "all"
    parts = [x.strip() for x in t.split(",") if x.strip()]
    return parts or None


def resolve_scan_scope(emit=print):
    """The subscriptions the command line searches for clusters: --subscription a,b,c (ids or names) / --subscription all; with no scope only the
    current (default) subscription. Says clearly what is searched. Returns [{id, name}]."""
    scope = SCAN_SCOPE["value"]
    if scope is None and AZ_OPTS.get("subscription"):
        scope = [AZ_OPTS["subscription"]]
    subs = list_az_subscriptions()
    enabled = {sid: i for sid, i in subs.items() if i.get("state") in (None, "Enabled")}
    if scope == "all":
        accounts = [{"id": sid, "name": i.get("name")} for sid, i in enabled.items()]
        emit(f"Scope: ALL {count_word(len(accounts), 'subscription')} (--subscription all). This can take several minutes.")
        return accounts or [{"id": None, "name": None}]
    if scope:
        accounts, unknown = [], []
        for tok in scope:
            hit = next((sid for sid, i in subs.items() if sid.lower() == tok.lower()), None) or next((sid for sid, i in subs.items() if (i.get("name") or "").lower() == tok.lower()), None)
            if hit:
                if hit not in [a["id"] for a in accounts]:
                    accounts.append({"id": hit, "name": subs[hit].get("name")})
            elif re.fullmatch(r"[0-9a-fA-F-]{36}", tok):
                accounts.append({"id": tok, "name": None})
            else:
                unknown.append(tok)
        for tok in unknown:
            emit(f"  WARNING: subscription '{tok}' was not found among the subscriptions az can see - skipped.")
        emit("Scope: " + count_word(len(accounts), "subscription") + " (--subscription): " + ", ".join(a["name"] or a["id"] for a in accounts[:10]) + (" ..." if len(accounts) > 10 else ""))
        return accounts
    default = next(((sid, i) for sid, i in enabled.items() if i.get("default")), None)
    if default:
        emit(f"Scope: the current (default) subscription only: {default[1].get('name') or default[0]}. Use --subscription a,b,c or --subscription all to search more.")
        return [{"id": default[0], "name": default[1].get("name")}]
    emit("Scope: the current (default) subscription of az only. Use --subscription a,b,c or --subscription all to search more.")
    return [{"id": None, "name": None}]


def list_clusters_cli(emit=print, accounts=None, progress=None, cancel=None, on_batch=None, all_subs=False, menu=None):
    """{'1': 'name (location/resource-group)', ...} from the Azure CLI (read-only), numbered in the listed order: the chosen
    subscription, or EVERY enabled subscription when none is chosen or all_subs is set (no limit; parallel, see scan_clusters; one
    Resource Graph query when the extension exists). Fills CLI_TARGETS. `menu` ({number: name}) is the akslogin menu: its entries are
    mapped to the clusters found (see merge_menu) and kept when az cannot see them. If az is not usable the menu is returned as it is.
    `accounts` ([{id, name}]) is what the window passes (it has already checked the sign-in); from the command line it is None."""
    CLI_TARGETS.clear()
    if accounts is None:
        if not cli_ensure_az(emit):
            if menu:
                emit("The Azure CLI is not available / not signed in - showing only the clusters from the akslogin menu.")
                return dict(menu)
            return {}
        accounts = resolve_scan_scope(emit)
    found, failed = scan_clusters(accounts, emit, progress, cancel, on_batch)
    emit(scan_summary(found))
    rows = merge_menu(found, menu) if menu is not None else found
    if not rows:
        emit("No AKS clusters found" + (f" ({failed} subscription(s) could not be read)" if failed else "") + " - check the subscription and your permissions.")
        return {}
    return register_clusters(rows, len(accounts) > 1)


def cli_login(number, label, emit):
    """Log in to cluster `number` with the Azure CLI: make sure `az` is logged in, then `az aks get-credentials`
    (and `kubelogin convert-kubeconfig -l azurecli` for Entra ID clusters when kubelogin is installed).
    Returns the kubectl context name. Raises RuntimeError when it fails."""
    if not CLI_TARGETS:
        list_clusters_cli(emit)
    tgt = CLI_TARGETS.get(str(number))
    if not tgt:
        raise RuntimeError(f"cluster {number} is not in the Azure CLI cluster list - run --list with --login-method cli to see the numbers")
    if not cli_ensure_az(emit):
        raise RuntimeError("Azure CLI is not logged in (login failed, was cancelled, or az is missing)")
    cmd = [shutil.which("az"), "aks", "get-credentials", "--resource-group", tgt["resource_group"], "--name", tgt["name"]]
    if tgt.get("subscription"):
        cmd += ["--subscription", tgt["subscription"]]
    cmd += ["--overwrite-existing"]
    emit("Running: az " + " ".join(cmd[1:]))
    ok, out = _run_captured(cmd, 120)
    if not ok:
        raise RuntimeError(f"az aks get-credentials failed: {out}")
    emit(out or "kubeconfig updated.")
    if tgt.get("aad"):
        kubelogin = shutil.which("kubelogin")
        if kubelogin:
            ok, out = _run_captured([kubelogin, "convert-kubeconfig", "-l", "azurecli"], 60)
            emit("kubelogin convert-kubeconfig -l azurecli: " + ("OK" if ok else f"WARNING failed - {out}"))
        else:
            emit("NOTE: this cluster uses Microsoft Entra ID sign-in. kubectl needs kubelogin for it - install it (az aks install-cli) "
                 "and run: kubelogin convert-kubeconfig -l azurecli")
    return tgt["name"]


# ---------------------------------------------------------------------------
# kubectl helpers
# ---------------------------------------------------------------------------

KUBE_CONTEXT = None   # set by select_context(): every kubectl call is pinned to this context


def kubectl(args, timeout=KUBECTL_TIMEOUT):
    """Run kubectl (pinned to KUBE_CONTEXT once one is selected). Returns (ok, text). Never raises.
    READ-ONLY GUARANTEE: every call is checked against the allow-list first (assert_read_only_kubectl); a refused call starts no process."""
    if KUBE_CONTEXT and not (args and args[0] == "config" and len(args) > 1 and args[1] in ("get-contexts", "use-context", "current-context")):
        args = ["--context", KUBE_CONTEXT, *args]
    try:
        kind = assert_read_only_kubectl(args)
    except ReadOnlyViolation as v:
        GUARD.block("kubectl", args if isinstance(args, (list, tuple)) else [args], str(v))
        return False, str(v)
    exe = shutil.which("kubectl")
    if not exe:
        return False, "kubectl was not found on PATH"
    GUARD.local_only() if kind == "local" else GUARD.read()
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

# ---------------------------------------------------------------------------
# Plain-language table headers: no short forms in the report (text and HTML). Every header goes through full_header().
# ---------------------------------------------------------------------------

HEADER_FULL = {
    "NS": "Namespace", "SVC": "Service", "NAT GW": "Network address translation gateway", "NAT GATEWAY": "Network address translation gateway",
    "UDR": "User-defined route", "RST": "Container restarts", "RESTARTS": "Container restarts", "NSG": "Network security group",
    "VMSS/INSTANCE": "Virtual machine scale set / instance", "VM": "Virtual machine", "AZURE VM": "Azure virtual machine",
    "VM SIZE": "Virtual machine size", "OS": "Operating system", "K8S VERSION": "Kubernetes version", "MAX PODS": "Maximum pods per node",
    "IPS NEEDED AT MAX SIZE": "IP addresses needed at maximum size", "USABLE IPS": "Usable IP addresses", "FREE IPS": "Free IP addresses",
    "POD IPS": "Pod IP addresses in use now", "PREFIX": "Address prefix", "SKU": "Pricing tier", "LB KIND": "Load balancer kind",
    "LB RULES": "Load balancing rules", "SUPPORT DL": "Support team (distribution list)", "DEST PORT": "Destination port",
    "CPU USE": "Processor (CPU) used", "CPU REQ": "Processor (CPU) requested", "CPU LIM": "Processor (CPU) limit",
    "MEM USE": "Memory used", "MEM REQ": "Memory requested", "MEM LIM": "Memory limit", "DISK USE": "Disk used",
    "CPU %CL": "Processor (CPU) used, % of cluster", "MEM %CL": "Memory used, % of cluster", "CPUREQ": "Processor (CPU) requested",
    "MEMREQ": "Memory requested", "EPHEMERAL": "Temporary storage (ephemeral)", "IMAGEFS": "Container image storage",
    "SWAP": "Swap space", "PROVIDER-ID": "Cloud provider identifier", "CPU USED/ALLOC": "Processor (CPU) used / allocatable",
    "MEMORY USED/ALLOC": "Memory used / allocatable", "DISK USED/TOTAL": "Disk used / total", "HPA MIN-MAX": "Autoscaler minimum - maximum",
    "HPA": "Horizontal pod autoscaler", "STORAGECLASS": "Storage class", "RX TOTAL": "Bytes received (total)",
    "TX TOTAL": "Bytes transmitted (total)", "RX NOW": "Bytes received per second (now)", "TX NOW": "Bytes transmitted per second (now)",
    "IN AVG": "Bytes received, average per second", "IN PEAK": "Bytes received, peak per second", "IN TOTAL": "Bytes received (total)",
    "OUT AVG": "Bytes transmitted, average per second", "OUT PEAK": "Bytes transmitted, peak per second", "OUT TOTAL": "Bytes transmitted (total)",
    "SNAT CONNECTIONS": "Outbound connections (source network address translation)",
    "SNAT PORTS (PEAK)": "Outbound ports used, peak (source network address translation)",
    "BACKEND HEALTH (MIN)": "Backend health probe availability (lowest)", "TLS": "Transport Layer Security (TLS) configured",
    "CLASS": "Ingress class", "SUBNET / POD CIDR": "Subnet or pod address range", "DEFAULT-DENY": "Default-deny policy present",
    "PORTS (PORT[:NODEPORT])": "Ports (service port : node port)", "MODE": "Mode", "ID": "Identifier",
    "NAMESPACE/POD": "Namespace / pod", "NAMESPACE/NAME": "Namespace / name", "NODES": "Nodes", "LB": "Load balancer",
    "GW": "Gateway", "CNI": "Container Network Interface", "DNS": "Domain Name System", "MTU": "Maximum Transmission Unit",
    "HIGH PODS": "Pods with high usage", "CPU": "Processor (CPU)", "MEMORY": "Memory", "DISK": "Disk",
}
_HEADER_TOKENS = {"IPS": "IP addresses", "CPU": "Processor (CPU)", "MEM": "Memory", "LB": "Load balancer", "NSG": "Network security group",
                  "SNAT": "outbound (source network address translation)", "NAT": "Network address translation", "VM": "Virtual machine", "VMSS": "Virtual machine scale set",
                  "OS": "Operating system", "DL": "distribution list", "RX": "Bytes received", "TX": "Bytes transmitted", "DNS": "Domain Name System",
                  "SKU": "Pricing tier", "UDR": "User-defined route", "GW": "Gateway", "SVC": "Service", "NS": "Namespace", "K8S": "Kubernetes",
                  "HPA": "Horizontal pod autoscaler", "PVC": "Persistent volume claim", "CIDR": "Address range", "MTU": "Maximum Transmission Unit"}


def full_header(h):
    """A table header in plain words: known short forms are spelled out, UPPER CASE becomes Sentence case."""
    h = str(h)
    if not h.strip():
        return h
    hit = HEADER_FULL.get(h.strip().upper())
    if hit:
        return hit
    out = []
    for word in h.split(" "):
        key = word.upper().rstrip(",")
        out.append(_HEADER_TOKENS[key] if key in _HEADER_TOKENS and (word.isupper() or word in ("IPs",)) else word)
    text = " ".join(out)
    if text == text.upper() and any(c.isalpha() for c in text):
        text = text.capitalize()
    return text[:1].upper() + text[1:]


def _locked(fn):
    @functools.wraps(fn)
    def wrapper(self, *a, **k):
        with self._lock:
            return fn(self, *a, **k)
    return wrapper


class Report:
    """Collects the report as text lines (streamed to `emit`) AND as structured sections /
    blocks (tables, logs, timeline) that the interactive HTML report is built from."""

    def __init__(self, emit):
        self._lock = threading.RLock()      # the report can be written from several threads
        self.lines = []
        self.emit = emit
        self.sections = [{"id": "s0", "title": "Run log", "blocks": []}]
        self.current = self.sections[0]
        self.missing_about = []      # (kind, name) of every table / block written without an explanation (a test fails when this is not empty)
        self.used_terms = set()      # terms of the glossary tables written so far (they make the complete glossary at the end)

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

    def _about(self, what, kind, name):
        """The 'What this block shows' line above a block (HTML + text). A block written without one is recorded in self.missing_about."""
        if what:
            self._text(f"  What this block shows: {what}")
            self._block("about", what)
        else:
            self.missing_about.append((kind, name))

    @_locked
    def add(self, text="", about=None):
        if about:
            self._text(f"  What this block shows: {about}")
            self._block("about", about)
        for line in str(text).splitlines() or [""]:
            self._text(line)
            self._block("lines", line)

    @_locked
    def section(self, title, sid=None):
        self._text("")
        self._text("=" * 78)
        self._text(title)
        self._text("=" * 78)
        self.current = {"id": f"s{len(self.sections)}", "title": title, "blocks": [], "sid": sid}
        self.sections.append(self.current)
        info = section_text(sid)
        if info:                                         # the explanation of the section (the HTML draws it as a box under the heading)
            self._text(f"What this section shows: {info[0]}")
            self._text(f"How to use it: {info[1]}")
            self._text("")

    @_locked
    def skipped(self, title, note=""):
        """One line for a section the user did not select: 'Skipped by choice: <section>'."""
        line = f"Skipped by choice: {title}" + (f" ({note})" if note else "")
        self._text("")
        self._text(line)
        self.current = {"id": f"s{len(self.sections)}", "title": line, "blocks": [("lines", [line])], "skipped": True,
                        "sid": next((s["id"] for s in SECTIONS if s["title"] == title), None)}
        self.sections.append(self.current)

    @_locked
    def heading(self, title, what=None, about=None, text_title=None):
        """A titled sub-block inside a section, with a one-line plain explanation under it (`about` = `what`).
        The text report prints `text_title` (upper case sub-titles) when given, else `title`."""
        what = about or what
        shown = text_title or title
        self._text("")
        self._text(shown)
        self._text("-" * min(78, len(shown)))
        if what:
            self._text(f"  What this block shows: {what}")
        else:
            self.missing_about.append(("heading", title))
        self._block("heading", title, what)

    @_locked
    def check(self, title, status, what, evidence, meaning):
        """A titled check with a status (OK / Warning / Problem / Not available), the evidence found and 'what this means / what to do next'."""
        self._text("")
        self._text(f"[{status}] {title}")
        self._text(f"  What this check looks at: {what}")
        self._text(f"  Evidence: {evidence}")
        self._text(f"  What this means / what to do next: {meaning}")
        self._block("check", title, status, what, evidence, meaning)

    @_locked
    def table(self, headers, rows, limit=MAX_ROWS, maxw=58, what=None, title=None, cls="", about=None):
        """A table. `about` (alias `what`) is the one-line 'What this table shows' above it - every table needs one."""
        what = about or what
        if not rows:
            return
        if not what:
            self.missing_about.append(("table", title or ", ".join(str(h) for h in headers[:3])))
        headers = [full_header(h) for h in headers]
        rows = [["-" if c is None else str(c) for c in r] for r in rows]
        shown = rows[:limit]
        widths = [min(maxw, max([len(h)] + [len(r[i]) for r in shown])) for i, h in enumerate(headers)]

        def cell(text, w):
            return text if len(text) <= w else text[: w - 1] + "~"

        if title:
            self._text(title)
        if what:
            self._text(f"  What this table shows: {what}")
        self._text("  ".join(h.ljust(w) for h, w in zip(headers, widths)))
        for r in shown:
            self._text("  ".join(cell(c, w).ljust(w) for c, w in zip(r, widths)).rstrip())
        if len(rows) > limit:
            self._text(f"... and {len(rows) - limit} more")
        self._block("table", list(headers), rows, what, title, cls)   # the HTML report keeps ALL rows

    @_locked
    def glossary(self, keys, title="Glossary: what these terms mean", track=True):
        """Small glossary table (Term | Full name | Plain-language meaning) of just the terms the next block uses."""
        rows = [[k, GLOSSARY[k][0], GLOSSARY[k][1]] for k in keys if k in GLOSSARY]
        if track:
            self.used_terms.update(k for k in keys if k in GLOSSARY)
        self.table(["Term", "Full name", "Plain-language meaning"], rows, limit=200, maxw=110, title=title, cls="gloss",
                   what="the abbreviations and technical terms used in the next block, spelled out and explained in plain language.")

    @_locked
    def log(self, title, entries, text_entries=None, about=None):
        """entries: [(text, kind)] with kind '' | 'warn' | 'err'. The HTML keeps all of them;
        the text report prints text_entries (a shortened version) when given."""
        self._about(about, "log", title)

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
        """Structured utilization data: rendered as the interactive dashboard in the HTML only (the text report gets the one-line explanation)."""
        self._about(about, "dashboard", "utilization")
        self._block("util", data)

    @_locked
    def series(self, title, rows, note="", about=None):
        """Time series (sparkline charts) - rendered in the HTML only; the numbers are printed as tables by the caller."""
        if rows:
            self._about(about, "series", title)
            self._block("series", title, rows, note)

    @_locked
    def timeline(self, entries, about=None):
        """entries: [(datetime, text)]"""
        self._about(about, "timeline", "timeline")
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
        self.resources = None     # kubectl objects to read (None = all); set from the selected sections
        self.want_usage = True    # read the live usage numbers?
        self.sections_note = None  # (collected, total, [titles not collected]) when the user left sections out
        self.readonly = None      # GUARD.since(...) of this run: {"reads", "local", "blocked"} (the 'Read-only guarantee' block)
        self._lock = threading.Lock()

    def find(self, severity, text):
        with self._lock:
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
    wanted = {n: a for n, a in RESOURCES.items() if ctx.resources is None or n in ctx.resources}     # only what the selected sections read
    with ThreadPoolExecutor(max_workers=6) as pool:
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
    if ctx.want_usage:
        rep.add("Collecting live CPU / memory / disk / swap usage ...")
        fetch_usage(ctx, rep)
    else:
        ctx.data.update(node_stats={}, stats_error=None, top_nodes={}, top_pods={}, top_error=None, pod_usage={})


# ---------------------------------------------------------------------------
# Azure side of AKS (read-only `az` CLI calls: show / list / metrics / log-analytics query)
# ---------------------------------------------------------------------------

AZ_OPTS = {"enabled": True, "cluster": None, "resource_group": None, "subscription": None,   # "subscription" = the one you asked for
           "subscription_used": None, "subscription_reason": None, "target": None, "cluster_info": None}
MAX_SUB_TRIES = 3
MAX_CP_LOG_LINES = 30        # control-plane error lines shown
LOW_SUBNET_IPS = 50          # warn when a cluster subnet has fewer free IPs than this
GUID = re.compile(r"[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}")
CP_LOG_CATEGORIES = ["kube-apiserver", "kube-audit", "kube-audit-admin", "kube-controller-manager", "kube-scheduler",
                     "cluster-autoscaler", "cloud-controller-manager", "guard"]


def az_cli(args, target=None, timeout=90, subscription=True):
    """Run `az ...` (read-only). Returns (parsed_json_or_text_or_None, error_or_None).
    READ-ONLY GUARANTEE: only the commands of READ_ONLY_CLOUD_COMMANDS run (a refused call starts no process); extensions are never installed
    (AZURE_EXTENSION_USE_DYNAMIC_INSTALL=no in the environment of every az process)."""
    try:
        assert_read_only_cloud("az", args)
    except ReadOnlyViolation as v:
        GUARD.block("az", ["az", *args] if isinstance(args, (list, tuple)) else [args], str(v))
        return None, str(v)
    exe = shutil.which("az")
    if not exe:
        return None, "Azure CLI (az) was not found on PATH"
    GUARD.read()
    cmd = [exe, *args, "--only-show-errors", "-o", "json"]
    sub = (target or {}).get("subscription")
    if sub and subscription:
        cmd += ["--subscription", sub]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, env=_az_env())
    except subprocess.TimeoutExpired:
        return None, f"timed out after {timeout}s"
    except Exception as exc:
        return None, str(exc)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout).strip()
        if is_expired_error(err) and list(args[:2]) not in (["account", "show"], ["account", "list"]):     # 'not signed in' is not 'expired'
            _note_expired(args, target, err)          # the sign-in expired: mark the account, the window shows the banner
        return None, err
    try:
        return (json.loads(proc.stdout) if proc.stdout.strip() else {}), None
    except json.JSONDecodeError:
        return proc.stdout.strip(), None


def _first_line(err, n=170):
    return (err.strip().splitlines() or ["unknown error"])[-1][:n]


# --- subscriptions (the Azure equivalent of picking an AWS profile) ----------------------------------

def _az_subscriptions():
    """({subscription_id: {name, state, default, user, tenant}}, error_or_None) from `az account list --all` (needs `az login`). All of them, no cap."""
    data, err = az_cli(["account", "list", "--all"], None, 120, subscription=False)
    if err or not isinstance(data, list):
        return {}, (err or "unexpected output from az account list")
    subs = {s["id"]: {"name": s.get("name"), "state": s.get("state"), "default": bool(s.get("isDefault")),
                      "user": (s.get("user") or {}).get("name"), "tenant": s.get("tenantId"), "domain": s.get("tenantDefaultDomain"),
                      "kind": (s.get("user") or {}).get("type") or "user", "tenant_name": s.get("tenantDisplayName") or s.get("tenantDefaultDomain")}
            for s in data if s.get("id")}
    want = (LOGIN_OPTS.get("tenant") or "").lower()
    if want and not LOGIN_OPTS.get("gui"):                   # command line --tenant: only that tenant's subscriptions (when there are any)
        kept = {k: v for k, v in subs.items() if want in ((v.get("tenant") or "").lower(), (v.get("domain") or "").lower())}
        subs = kept or subs
    return subs, None


def list_az_subscriptions():
    """{subscription_id: {name, state, default, user, tenant}} from `az account list` (needs `az login`)."""
    return _az_subscriptions()[0]


def describe_subscription(sid, info):
    return f"{info.get('name') or '?'}  ({sid})" + ("  [default]" if info.get("default") else "") + (
        f"  {info['state']}" if info.get("state") not in (None, "Enabled") else "")


def _node_hint():
    """Where the cluster's nodes live, from the first node's providerID:
    azure:///subscriptions/<sub>/resourceGroups/<MC_node_rg>/providers/Microsoft.Compute/virtualMachineScaleSets/..."""
    ok, out = kubectl(["get", "nodes", "-o", "jsonpath={.items[0].spec.providerID}"])
    hint = {"sub": None, "node_rg": None, "server": None, "context": None}
    if ok:
        m = re.search(r"subscriptions/([^/]+)/resourceGroups/([^/]+)/", out, re.I)
        if m:
            hint["sub"], hint["node_rg"] = m.group(1).lower(), m.group(2)
    ok, out = kubectl(["config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}"])
    hint["server"] = out if ok else None
    ok, out = kubectl(["config", "current-context"])
    hint["context"] = out if ok else None
    return hint


def find_aks_cluster(sub_id, label, hint):
    """The AKS cluster object (from `az aks list`) that matches the nodes' resource group, the API server
    host, or the cluster name. Returns (cluster_or_None, error_or_None)."""
    if AZ_OPTS["cluster"] and AZ_OPTS["resource_group"]:
        data, err = az_cli(["aks", "show", "-g", AZ_OPTS["resource_group"], "-n", AZ_OPTS["cluster"]], {"subscription": sub_id})
        return (data if isinstance(data, dict) else None), err
    data, err = az_cli(["aks", "list"], {"subscription": sub_id}, timeout=120)
    if err or not isinstance(data, list):
        return None, err or "unexpected output from az aks list"
    host = ((hint.get("server") or "").split("//")[-1].split(":")[0]).lower()
    wanted = [(label or "").lower(), (AZ_OPTS["cluster"] or "").lower(), (hint.get("context") or "").lower()]
    for c in data:
        if hint.get("node_rg") and (c.get("nodeResourceGroup") or "").lower() == hint["node_rg"].lower():
            return c, None
    for c in data:
        if host and host in ((c.get("fqdn") or "") + " " + (c.get("privateFqdn") or "")).lower():
            return c, None
    for c in data:
        if (c.get("name") or "").lower() in wanted and (c.get("name") or ""):
            return c, None
    return None, None


def select_az_subscription(label, emit, preferred=None):
    """After akslogin: read the Azure subscriptions you can use, pick the one the cluster's nodes live in,
    and find the AKS cluster in it. Stores the result in AZ_OPTS. Returns the subscription id or None."""
    for k in ("subscription_used", "subscription_reason", "target", "cluster_info"):
        AZ_OPTS[k] = None
    if not shutil.which("az"):
        emit("Azure CLI (az) not found on PATH - the Azure section will be skipped (kubectl data still works). Install it and run: az login")
        return None
    subs = list_az_subscriptions()
    if not subs:
        emit("No Azure subscriptions found - run `az login` (the Azure section will be skipped; kubectl data still works).")
        return None
    emit(f"Azure subscriptions ({len(subs)}): " + ", ".join(f"{i.get('name')}" for i in subs.values())[:300])
    if preferred and preferred not in subs:
        emit(f"WARNING: subscription '{preferred}' is not one of yours - choosing automatically instead.")
        preferred = None
    hint = _node_hint()
    ranked = {}

    def add(score, sid, why):
        if sid in subs and score > ranked.get(sid, (0, ""))[0]:
            ranked[sid] = (score, why)
    if preferred:
        add(1000, preferred, "the subscription you selected")
    if hint["sub"]:
        for sid in subs:
            if sid.lower() == hint["sub"]:
                add(900, sid, "the nodes' VMs live in it (from their providerID)")
    for sid, info in subs.items():
        if info.get("default"):
            add(100, sid, "your default subscription")
        add(1, sid, "an available subscription")
    order = sorted(((s, sid, why) for sid, (s, why) in ranked.items()), key=lambda x: -x[0])
    tried = []
    for score, sid, why in order[:MAX_SUB_TRIES]:
        cluster, err = find_aks_cluster(sid, label, hint)
        if cluster:
            AZ_OPTS["subscription_used"], AZ_OPTS["subscription_reason"] = sid, why
            AZ_OPTS["cluster_info"] = cluster
            AZ_OPTS["target"] = {"subscription": sid, "resource_group": cluster.get("resourceGroup"), "cluster": cluster.get("name"),
                                 "node_rg": cluster.get("nodeResourceGroup"), "id": cluster.get("id"), "location": cluster.get("location")}
            emit(f"Azure subscription '{subs[sid].get('name')}' selected ({why}); AKS cluster {cluster.get('name')} "
                 f"in resource group {cluster.get('resourceGroup')}")
            return sid
        tried.append(subs[sid].get("name"))
        emit(f"  subscription '{subs[sid].get('name')}' ({why}): cluster not found" + (f" - {_first_line(err, 100)}" if err else ""))
    if hint["sub"]:
        for sid in subs:
            if sid.lower() == hint["sub"]:
                AZ_OPTS["subscription_used"], AZ_OPTS["subscription_reason"] = sid, "the nodes' subscription (cluster object not readable)"
                AZ_OPTS["target"] = {"subscription": sid, "resource_group": None, "cluster": label, "node_rg": hint["node_rg"], "id": None, "location": None}
    emit("WARNING: could not find the AKS cluster through `az aks list` (tried: " + ", ".join(tried) + "). "
         "Check you have Reader on it, or pass --az-cluster NAME --resource-group RG.")
    return AZ_OPTS["subscription_used"]


def resolve_az_target(label):
    """The target chosen by select_az_subscription (cluster, resource group, node resource group)."""
    t = AZ_OPTS.get("target")
    if t and t.get("cluster") and t.get("resource_group"):
        return dict(t)
    return None


# --- the Azure section -----------------------------------------------------------------------------------

def _res_name(resource_id):
    return (resource_id or "").rstrip("/").rsplit("/", 1)[-1]


def section_azure(rep, ctx, label):
    rep.section("2. AZURE KUBERNETES SERVICE CLUSTER AND INFRASTRUCTURE (cluster, node pools, network, identity, logging)", "azure")
    if not AZ_OPTS["enabled"]:
        rep.add("Skipped (Azure details turned off).")
        return
    target = resolve_az_target(label)
    if not target:
        rep.add("Could not identify the AKS cluster in Azure (az not installed / not logged in / no Reader access). "
                "Run `az login`, or pass --az-cluster NAME --resource-group RG [--subscription ID].")
        return
    c = AZ_OPTS.get("cluster_info") or {}
    ctx.data["az_target"], ctx.data["az_cluster"] = target, c
    trows = [["Cluster", target["cluster"]], ["Resource group", target["resource_group"]], ["Subscription", target["subscription"]]]
    if target.get("node_rg"):
        trows.append(["Node resource group (where Azure keeps the node machines)", target["node_rg"]])
    if AZ_OPTS.get("subscription_reason"):
        trows.append(["Why this subscription was chosen", AZ_OPTS["subscription_reason"]])
    rep.table(["Item", "Value"], trows, maxw=110,
              about="The Azure cluster this report looks at: its name, the resource group and subscription it lives in, and where Azure keeps its node machines.")
    acct, err = az_cli(["account", "show"], target, 30)
    if err:
        rep.add(f"Azure credentials: NOT WORKING ({_first_line(err)})")
        rep.add("  Try:  az login")
        ctx.find("HIGH", "Azure CLI is not logged in - Azure-side checks skipped")
        return
    rep.table(["Item", "Value"], [["Signed in as", f"{(acct.get('user') or {}).get('name')} ({(acct.get('user') or {}).get('type')})"],
                                  ["Tenant (the organisation's Azure directory)", acct.get("tenantId")], ["Subscription used", acct.get("name")]], maxw=110,
              about="The Azure identity the az tool is signed in with and the directory and subscription it reads from; every Azure value below is read with this identity.")
    if not c.get("agentPoolProfiles"):
        data, err = az_cli(["aks", "show", "-g", target["resource_group"], "-n", target["cluster"]], target)
        if err:
            rep.add(f"az aks show FAILED: {_first_line(err)}")
            ctx.find("MED", f"Could not read the AKS cluster in Azure ({_first_line(err, 90)})")
            return
        c = data
        ctx.data["az_cluster"] = c
    for step in (_az_cluster, _az_nodepools, _az_network, _az_identity, _az_addons, _az_vm_health, _az_cp_logs):
        if ctx.cancel is not None and ctx.cancel.is_set():
            return
        try:
            step(rep, ctx, target, c)
        except Exception as exc:
            rep.add(f"[!] {step.__name__.strip('_')} failed: {exc}")


def _az_cluster(rep, ctx, target, c):
    rep.heading("Cluster", "the cluster object in Azure: its state, versions, how its API server can be reached, the network plugin and address ranges, who may sign in and how upgrades are handled.",
                text_title="CLUSTER")
    rows = []
    prov, power = c.get("provisioningState", "?"), (c.get("powerState") or {}).get("code", "?")
    rows.append(["Provisioning state", prov])
    rows.append(["Power state", power])
    if prov != "Succeeded":
        ctx.find("CRIT" if prov in ("Failed", "Canceled") else "HIGH", f"AKS cluster provisioning state is {prov} (expected Succeeded)")
    if power != "Running":
        ctx.find("HIGH", f"AKS cluster power state is {power}")
    rows.append(["Kubernetes version", c.get('currentKubernetesVersion') or c.get('kubernetesVersion')])
    rows.append(["Pricing tier", (c.get('sku') or {}).get('tier', '-')])
    rows.append(["Location", c.get('location')])
    rows.append(["API server address", c.get('privateFqdn') or c.get('fqdn')])
    api = c.get("apiServerAccessProfile") or {}
    ranges = api.get("authorizedIpRanges") or []
    rows.append(["Private cluster (API server only reachable from the virtual network)", bool(api.get('enablePrivateCluster'))])
    rows.append(["Authorized IP address ranges", ','.join(ranges) or 'none (open to the internet)'])
    rows.append(["Virtual network integration of the API server", bool(api.get('enableVnetIntegration'))])
    if not api.get("enablePrivateCluster") and not ranges:
        ctx.find("INFO", "API server is public with no authorized IP ranges (reachable from the whole internet, protected only by authentication)")
    nw = c.get("networkProfile") or {}
    rows.append(["Network plugin", nw.get('networkPlugin')])
    rows.append(["Network plugin mode", nw.get('networkPluginMode') or '-'])
    rows.append(["Network policy engine", nw.get('networkPolicy') or 'none'])
    rows.append(["Network data plane", nw.get('networkDataplane') or '-'])
    rows.append(["Outbound traffic type", nw.get('outboundType')])
    rows.append(["Load balancer pricing tier", nw.get('loadBalancerSku')])
    rows.append(["Service address range", ','.join(nw.get('serviceCidrs') or [nw.get('serviceCidr') or '-'])])
    rows.append(["Cluster Domain Name System (DNS) service address", nw.get('dnsServiceIp')])
    rows.append(["Pod address range", ','.join(nw.get('podCidrs') or [nw.get('podCidr') or '-'])])
    ident = c.get("identity") or {}
    aad = c.get("aadProfile") or {}
    rows.append(["Cluster identity type", ident.get('type', '-')])
    rows.append(["Microsoft Entra ID integration", bool(aad.get('managed'))])
    rows.append(["Azure role-based access control", bool(aad.get('enableAzureRbac'))])
    rows.append(["Local accounts disabled", bool(c.get('disableLocalAccounts'))])
    up = c.get("autoUpgradeProfile") or {}
    rows.append(["Automatic upgrade channel", up.get('upgradeChannel') or 'none'])
    rows.append(["Node operating system upgrade channel", up.get('nodeOsUpgradeChannel') or '-'])
    rows.append(["OpenID Connect issuer enabled", bool((c.get('oidcIssuerProfile') or {}).get('enabled'))])
    rows.append(["Workload identity enabled", bool(((c.get('securityProfile') or {}).get('workloadIdentity') or {}).get('enabled'))])
    upg, err = az_cli(["aks", "get-upgrades", "-g", target["resource_group"], "-n", target["cluster"]], target, 60)
    if not err and isinstance(upg, dict):
        newer = [u.get("kubernetesVersion") for u in (upg.get("controlPlaneProfile") or {}).get("upgrades", []) or []]
        if newer:
            rows.append(["Kubernetes upgrades available", ', '.join(str(v) for v in newer)])
            ctx.find("INFO", f"AKS upgrade available: {', '.join(str(v) for v in newer[:3])} (running {c.get('currentKubernetesVersion')})")
    rep.glossary(["API server", "CIDR", "CNI", "DNS", "Entra ID", "Azure RBAC", "OIDC issuer", "Workload identity"])
    rep.table(["Setting", "Value"], [[k, "-" if v is None else str(v)] for k, v in rows], maxw=110,
              about="One row per setting of the cluster as Azure stores it (state, versions, API server access, network plugin and address ranges, identity and sign-in, upgrade channels).")


def _az_nodepools(rep, ctx, target, c):
    rep.heading("Node pools", "the groups of nodes (Azure virtual machine scale sets) of the cluster and whether each has all the nodes it is supposed to have.", text_title="NODE POOLS")
    ready_by_pool = Counter()
    for n in items(ctx.data.get("nodes")):
        pool = n["metadata"].get("labels", {}).get("kubernetes.azure.com/agentpool") or n["metadata"].get("labels", {}).get("agentpool")
        if pool and any(cd["type"] == "Ready" and cd["status"] == "True" for cd in n.get("status", {}).get("conditions", [])):
            ready_by_pool[pool] += 1
    rows = []
    for p in c.get("agentPoolProfiles") or []:
        notes = []
        if p.get("provisioningState") not in ("Succeeded", None):
            notes.append(f"provisioning {p.get('provisioningState')}")
            ctx.find("HIGH", f"Node pool {p['name']} provisioning state is {p.get('provisioningState')}")
        if (p.get("powerState") or {}).get("code") not in ("Running", None):
            notes.append(f"power {(p.get('powerState') or {}).get('code')}")
        want, have = p.get("count") or 0, ready_by_pool.get(p["name"], 0)
        if have < want:
            notes.append(f"only {have}/{want} nodes Ready")
            ctx.find("HIGH", f"Node pool {p['name']}: {want} nodes wanted but {have} Ready (check VMSS health, quota, subnet IPs, NSG)")
        if p.get("enableAutoScaling") and p.get("maxCount") and want >= p["maxCount"]:
            notes.append("AT MAX (autoscaler can't add nodes)")
            ctx.find("MED", f"Node pool {p['name']} is at its autoscaler maximum ({p['maxCount']})")
        rows.append([p["name"], p.get("mode"), p.get("vmSize"), f"{have} ready / count {want}" + (f" (min {p.get('minCount')}, max {p.get('maxCount')})" if p.get("enableAutoScaling") else " (no autoscaler)"),
                     f"{p.get('osType')} {p.get('osSku') or ''}".strip(), p.get("currentOrchestratorVersion") or p.get("orchestratorVersion"),
                     ",".join(p.get("availabilityZones") or []) or "-", p.get("maxPods"), "SPOT" if (p.get("scaleSetPriority") or "").lower() == "spot" else "regular",
                     "; ".join(notes) or "OK"])
    rep.glossary(["Node pool", "System pool", "Spot", "Availability zone", "Autoscaler"])
    rep.table(["Node pool", "Mode", "Virtual machine size", "Nodes", "Operating system", "Kubernetes version", "Availability zones", "Maximum pods per node", "Priority", "Health"], rows, maxw=70,
              about="One row per node pool: its role (system or user), the virtual machine size, how many nodes are ready out of the wanted count (with the autoscaler limits), the zones, whether it uses spot machines and any problem found.")


def _az_subnet_ids(c):
    ids = []
    for p in c.get("agentPoolProfiles") or []:
        if p.get("vnetSubnetId") and p["vnetSubnetId"] not in ids:
            ids.append(p["vnetSubnetId"])
    return ids


def _az_network(rep, ctx, target, c):
    rep.heading("Network", "how the nodes and pods are connected in Azure: where pod addresses come from, how many free addresses the subnets have, the firewall rules (network security groups) and the outbound path.",
                text_title="NETWORK")
    nw = c.get("networkProfile") or {}
    plugin, mode = nw.get("networkPlugin"), (nw.get("networkPluginMode") or "").lower()
    classic_cni = plugin == "azure" and mode != "overlay"
    rep.add("Pods get IP addresses from: " + ("the virtual network subnet (classic Azure Container Network Interface: every pod takes a subnet address, and addresses are reserved up front per node)" if classic_cni
                                              else ("an overlay pod address range (Azure CNI Overlay: the subnet only needs addresses for the nodes)" if mode == "overlay"
                                                    else "a private pod address range behind network address translation (kubenet)" if plugin == "kubenet" else f"plugin {plugin}")),
            about="where the pods of this cluster get their IP addresses, which decides how many addresses the subnet needs.")
    subnet_ids = _az_subnet_ids(c)
    subnets, rows = [], []
    if not subnet_ids:
        rep.add("Node pools use a virtual network managed by Azure Kubernetes Service in the node resource group (no custom subnet).")
    pools_by_subnet = defaultdict(list)
    for p in c.get("agentPoolProfiles") or []:
        if p.get("vnetSubnetId"):
            pools_by_subnet[p["vnetSubnetId"]].append(p)
    for sid in subnet_ids:
        sn, err = az_cli(["network", "vnet", "subnet", "show", "--ids", sid], target, 60)
        if err or not isinstance(sn, dict):
            rep.add(f"Subnet {_res_name(sid)}: details unavailable ({_first_line(err or 'no data', 90)})")
            continue
        prefixes = sn.get("addressPrefixes") or [sn.get("addressPrefix")]
        try:
            capacity = sum(max(0, ipaddress.ip_network(x, strict=False).num_addresses - 5) for x in prefixes if x)
        except ValueError:
            capacity = 0
        used = len(sn.get("ipConfigurations") or [])
        free = max(0, capacity - used)
        pools = pools_by_subnet[sid]
        nodes_max = sum((p.get("maxCount") if p.get("enableAutoScaling") and p.get("maxCount") else p.get("count") or 0) for p in pools)
        per_node = max((p.get("maxPods") or 30) for p in pools) + 1 if pools else 31
        need = nodes_max * per_node if classic_cni else nodes_max
        note = ""
        if free < 10:
            note = "VERY LOW IPs"
            ctx.find("HIGH", f"Subnet {sn.get('name')} has only ~{free} free IPs ({used}/{capacity} used)")
        elif free < LOW_SUBNET_IPS:
            note = "low IPs"
            ctx.find("MED", f"Subnet {sn.get('name')} has only ~{free} free IPs ({used}/{capacity} used)")
        if need > free + used and capacity:
            note = (note + "; " if note else "") + f"CANNOT HOLD scale-out ({need} IPs needed)"
            ctx.find("HIGH", f"Subnet {sn.get('name')} ({capacity} usable IPs) cannot hold the pools' maximum size: {nodes_max} nodes need ~{need} IPs"
                             + (" (classic Azure CNI reserves max-pods+1 IPs per node)" if classic_cni else ""))
        sn["_free"], sn["_used"], sn["_capacity"] = free, used, capacity
        subnets.append(sn)
        rows.append([sn.get("name"), ", ".join(p for p in prefixes if p), capacity, used, free, f"{nodes_max} nodes x {per_node if classic_cni else 1} = {need}",
                     (sn.get("networkSecurityGroup") or {}).get("id", "").split("/")[-1] or "-",
                     (sn.get("routeTable") or {}).get("id", "").split("/")[-1] or "-", (sn.get("natGateway") or {}).get("id", "").split("/")[-1] or "-", note])
    if rows:
        rep.glossary(["Subnet", "CIDR", "NSG", "UDR", "NAT gateway", "VNet"])
        rep.table(["Subnet", "Address prefix", "Usable IP addresses", "IP addresses used", "IP addresses free", "IP addresses needed at maximum size", "Network security group", "Route table",
                   "Network address translation gateway", "Note"], rows, maxw=40,
                  title="Subnets used by the node pools",
                  about="One row per subnet that holds node (and in classic mode pod) addresses: its address range, how many addresses are usable, used and free (an estimate: usable addresses minus the network interfaces attached) "
                        "and how many the pools would need at their maximum size.")
    ctx.data["az_subnets"] = subnets
    seen_nsg = set()
    for sn in subnets:
        nsg_id = (sn.get("networkSecurityGroup") or {}).get("id")
        if not nsg_id or nsg_id in seen_nsg:
            continue
        seen_nsg.add(nsg_id)
        nsg, err = az_cli(["network", "nsg", "show", "--ids", nsg_id], target, 60)
        if err or not isinstance(nsg, dict):
            rep.add(f"Network security group {_res_name(nsg_id)}: rules unavailable ({_first_line(err or 'no data', 90)})")
            continue
        rules = sorted((nsg.get("securityRules") or []), key=lambda r: r.get("priority", 0))
        ctx.data.setdefault("az_nsgs", []).append({"name": nsg.get("name"), "id": nsg_id, "rules": nsg.get("securityRules") or [], "where": "node subnet"})
        trs = []
        for r in rules:
            src = r.get("sourceAddressPrefix") or ",".join(r.get("sourceAddressPrefixes") or [])
            port = r.get("destinationPortRange") or ",".join(r.get("destinationPortRanges") or [])
            trs.append([r.get("priority"), r.get("direction"), r.get("access"), r.get("protocol"), src, port, r.get("name")])
            if r.get("direction") == "Inbound" and r.get("access") == "Allow" and src in ("*", "Internet", "0.0.0.0/0") and (port in ("*", "22", "3389") or "22" in port.split(",")):
                ctx.find("HIGH", f"NSG {nsg.get('name')} rule '{r.get('name')}' allows {port} from {src} inbound")
        rep.glossary(["NSG"])
        rep.table(["Priority", "Direction", "Access", "Protocol", "Source", "Destination port", "Rule name"], trs, limit=25,
                  title=f"Network security group {nsg.get('name')}",
                  about=f"The custom firewall rules of network security group {nsg.get('name')} in evaluation order (lowest priority number first); Azure's default rules also apply and are not listed.")
    # outbound path
    out_type = nw.get("outboundType") or "loadBalancer"
    lbp = nw.get("loadBalancerProfile") or {}
    orows = [["Outbound (egress) traffic type", out_type]]
    if lbp:
        orows += [["Managed outbound IP addresses", (lbp.get('managedOutboundIPs') or {}).get('count')],
                  ["Outbound ports allocated per virtual machine (source network address translation ports)", lbp.get('allocatedOutboundPorts')],
                  ["Idle timeout (minutes)", lbp.get('idleTimeoutInMinutes')]]
    for r in (lbp.get("effectiveOutboundIPs") or [])[:6]:
        ip, err = az_cli(["network", "public-ip", "show", "--ids", r["id"]], target, 30)
        if not err and isinstance(ip, dict):
            orows.append(["Outbound public IP address", f"{ip.get('ipAddress')}  ({ip.get('name')})"])
    rep.glossary(["SNAT", "NAT", "LB"])
    rep.table(["Setting", "Value"], [[k, "-" if v is None else str(v)] for k, v in orows], maxw=110,
              title="Outbound path",
              about="How traffic leaves the cluster to the internet: the outbound type, how many public addresses and ports the nodes share and which public addresses the outside world sees.")


def _az_identity(rep, ctx, target, c):
    rep.heading("Identities and role assignments", "the Azure identities the cluster uses (control plane and kubelet) and the roles they hold, which decide what the cluster may do in Azure, for example pull images or change a network.",
                text_title="IDENTITIES AND ROLE ASSIGNMENTS")
    principals = []
    ident = c.get("identity") or {}
    if ident.get("principalId"):
        principals.append(("cluster identity (control plane)", ident["principalId"]))
    for uid, info in (ident.get("userAssignedIdentities") or {}).items():
        if (info or {}).get("principalId"):
            principals.append((f"cluster identity {_res_name(uid)}", info["principalId"]))
    kubelet = (c.get("identityProfile") or {}).get("kubeletidentity") or {}
    if kubelet.get("objectId"):
        principals.append((f"kubelet identity {_res_name(kubelet.get('resourceId'))}", kubelet["objectId"]))
    sp = (c.get("servicePrincipalProfile") or {}).get("clientId")
    if sp and sp != "msi":
        rep.add(f"Uses a service principal ({sp}) - secrets can expire; managed identity is recommended")
        ctx.find("INFO", "AKS cluster uses a service principal instead of a managed identity (credentials can expire)")
    if not principals:
        rep.add("No managed identity details in the cluster object.")
        return
    rows = []
    for label_, pid in principals:
        roles, err = az_cli(["role", "assignment", "list", "--assignee", pid, "--all"], target, 90)
        if err:
            rows.append([label_, "-", "-", _first_line(err, 70)])
            continue
        for r in (roles or [])[:30]:
            rows.append([label_, r.get("roleDefinitionName"), (r.get("scope") or "").split("/providers/")[-1][:70] or "/", ""])
        if not roles:
            rows.append([label_, "(none)", "-", "no role assignments visible"])
    rep.glossary(["Managed identity", "Service principal", "Role assignment", "AcrPull"])
    rep.table(["Identity", "Role", "Scope", "Note"], rows, maxw=72,
              about="One row per role assignment of each cluster identity: which Azure role it holds and on which resource, resource group or subscription (up to 30 per identity).")
    kubelet_roles = [r for r in rows if r[0].startswith("kubelet")]
    if kubelet_roles and not any("AcrPull" in str(r[1]) for r in kubelet_roles):
        ctx.find("INFO", "kubelet identity has no AcrPull role assignment visible - fine if you pull from a registry another way, otherwise image pulls from ACR will fail")


def _az_addons(rep, ctx, target, c):
    rep.heading("Azure Kubernetes Service add-ons", "the optional Azure-managed components of the cluster (for example monitoring) and whether each is switched on.",
                text_title="AZURE KUBERNETES SERVICE ADD-ONS (az aks addon profiles)")
    rows = []
    for name, a in sorted((c.get("addonProfiles") or {}).items()):
        cfg = a.get("config") or {}
        extra = ""
        if name == "omsagent" and a.get("enabled"):
            extra = "workspace " + _res_name(cfg.get("logAnalyticsWorkspaceResourceID"))
        rows.append([name, "enabled" if a.get("enabled") else "disabled", extra])
    rep.glossary(["Add-on", "Container Insights"])
    rep.table(["Add-on", "State", "Detail"], rows,
              about="One row per add-on profile of the cluster: its name, whether it is enabled and, for monitoring, the Log Analytics workspace it writes to.")
    if not (c.get("addonProfiles") or {}).get("omsagent", {}).get("enabled") and not (c.get("azureMonitorProfile") or {}).get("metrics", {}).get("enabled"):
        ctx.find("INFO", "Azure Monitor / Container Insights is not enabled on this cluster (no Azure-side container metrics or logs)")


def _az_vm_health(rep, ctx, target, c):
    """VM scale set instances behind the node pools: provisioning + power state (is the VM running and provisioned?)."""
    node_rg = target.get("node_rg")
    if not node_rg:
        return
    rep.heading("Virtual machine scale set instances", "the Azure virtual machines behind the node pools: whether each is running, fully provisioned and healthy.",
                text_title="VIRTUAL MACHINE SCALE SET INSTANCES (node pools)")
    sets, err = az_cli(["vmss", "list", "-g", node_rg], target, 90)
    if err:
        rep.add(f"Unavailable: {_first_line(err, 110)}")
        return
    vm_info, rows = {}, []
    ctx.data["az_vmss"] = [{"id": x.get("id"), "name": x.get("name")} for x in (sets or [])]
    for s in sets or []:
        inst, err = az_cli(["vmss", "list-instances", "-g", node_rg, "-n", s["name"], "--expand", "instanceView"], target, 120)
        if err:
            inst, err = az_cli(["vmss", "list-instances", "-g", node_rg, "-n", s["name"]], target, 120)
        if err:
            rows.append([s["name"], "-", "-", "-", "-", _first_line(err, 70)])
            continue
        for i in inst or []:
            statuses = [(x.get("code") or "") for x in ((i.get("instanceView") or {}).get("statuses") or [])]
            power = next((x.split("/", 1)[1] for x in statuses if x.startswith("PowerState/")), "unknown")
            prov = i.get("provisioningState") or next((x.split("/", 1)[1] for x in statuses if x.startswith("ProvisioningState/")), "?")
            health = (((i.get("instanceView") or {}).get("vmHealth") or {}).get("status") or {}).get("code", "")
            health = health.split("/", 1)[-1] if health else "-"
            comp = ((i.get("osProfile") or {}).get("computerName") or i.get("name") or "").lower()
            vm_info[comp] = {"name": i.get("name"), "state": power, "prov": prov}
            bad = power not in ("running", "unknown") or prov not in ("Succeeded", "?") or health.lower() in ("unhealthy",)
            if bad:
                ctx.find("CRIT" if power == "running" else "HIGH", f"VM {i.get('name')} ({comp}): power {power}, provisioning {prov}, health {health}")
            rows.append([comp, i.get("name"), power, prov, health, "PROBLEM" if bad else "ok"])
    ctx.data["vm_info"] = vm_info
    ctx.data.pop("_node_idents", None)
    unhealthy = [r for r in rows if r[-1] != "ok"]
    rep.add(f"{len(rows)} instance(s) checked, {len(unhealthy)} with problems.")
    rep.glossary(["VMSS", "Power state"] if "Power state" in GLOSSARY else ["VMSS"])
    rep.table(["Node", "Virtual machine", "Power state", "Provisioning state", "Health", "Result"], unhealthy or rows[:6],
              about="The virtual machine scale set instances with a problem, or the first six instances when none has one: the node name, the machine, whether it is running, whether Azure finished provisioning it and its health.")


def _az_cp_logs(rep, ctx, target, c):
    res_id = target.get("id")
    rep.heading("Control-plane logging", "whether the cluster sends the logs of its control plane (API server, scheduler, controllers) to Azure through diagnostic settings and, when it does, the errors and denied requests of the window.",
                text_title="CONTROL-PLANE LOGGING (Azure diagnostic settings)")
    if not res_id:
        rep.add("Cluster resource identifier unknown.")
        return
    ds, err = az_cli(["monitor", "diagnostic-settings", "list", "--resource", res_id], target, 60)
    if err:
        rep.add(f"Unavailable: {_first_line(err, 110)}")
        return
    settings = ds.get("value") if isinstance(ds, dict) else ds
    enabled, workspaces = set(), []
    for s in settings or []:
        for lg in s.get("logs") or []:
            if lg.get("enabled") and lg.get("category"):
                enabled.add(lg["category"])
        if s.get("workspaceId"):
            workspaces.append(s["workspaceId"])
    rep.glossary(["Control plane", "API server", "kube-audit", "kube-scheduler", "kube-controller-manager", "Autoscaler", "Log Analytics"])
    rep.table(["Log category", "State"], [[k, "ON" if k in enabled else "off"] for k in CP_LOG_CATEGORIES],
              about="Each control-plane log category Azure can record and whether it is switched on for this cluster (ON means it is sent to a diagnostic destination).")
    if not enabled:
        ctx.find("MED", "AKS control-plane logs (diagnostic settings) are OFF - no API server / audit logs to troubleshoot with")
        return
    if not workspaces:
        rep.add("Logs go to storage or an event hub only - there is no Log Analytics workspace to query here.")
        return
    wid, err = az_cli(["monitor", "log-analytics", "workspace", "show", "--ids", workspaces[0], "--query", "customerId"], target, 60, subscription=False)
    if err or not isinstance(wid, str):
        rep.add(f"Workspace not readable: {_first_line(err or 'no customerId', 100)}")
        return
    mins = ctx.minutes
    errors_kql = _cp_errors_kql(mins)
    rows, err = az_cli(["monitor", "log-analytics", "query", "-w", wid, "--analytics-query", errors_kql, "--timespan", f"PT{mins}M"], target, 120, subscription=False)
    rep.heading("Control-plane log errors", f"the error-like entries of the control-plane logs in the last {mins} minutes (read from Log Analytics), newest last; the time is in UTC and the component is in square brackets.",
                text_title=f"CONTROL-PLANE LOG ERRORS (last {mins} min, from Log Analytics)")
    if err or not isinstance(rows, list):
        rep.add(f"Unavailable: {_first_line(err or 'no rows', 120)}")
    elif not rows:
        rep.add("No error-like entries in the window.")
    else:
        by_cat = Counter(r.get("Category") for r in rows)
        rep.add("Entries by component: " + ", ".join(f"{k} x{v}" for k, v in by_cat.most_common()))
        ctx.find("MED", f"{len(rows)} error-like control-plane log entries in window ({', '.join(str(k) for k, _ in by_cat.most_common(3))})")
        for r in rows[:MAX_CP_LOG_LINES][::-1]:
            ts = parse_ts(r.get("TimeGenerated"))
            rep.add(f"{ts:%H:%M:%S}Z [{str(r.get('Category'))[:24]}] {(r.get('Msg') or '').strip()[:200]}" if ts else f"[{r.get('Category')}] {(r.get('Msg') or '')[:200]}")
            if ts:
                ctx.happened(ts, f"CONTROL PLANE {str(r.get('Category'))[:24]}: {(r.get('Msg') or '').strip()[:100]}")
    if "kube-audit" in enabled or "kube-audit-admin" in enabled:
        audit_kql = _cp_audit_kql(mins)
        rows, err = az_cli(["monitor", "log-analytics", "query", "-w", wid, "--analytics-query", audit_kql, "--timespan", f"PT{mins}M"], target, 120, subscription=False)
        if err or not isinstance(rows, list):
            rep.add(f"Audit denials unavailable: {_first_line(err or 'no rows', 100)}")
        elif rows:
            rep.add(f"API requests DENIED (401/403) in the window: {sum(int(r.get('n') or 0) for r in rows)} (top callers)")
            rep.table(["User", "Verb", "Resource", "Response code", "Count"], [[r.get("user"), r.get("verb"), r.get("res"), r.get("code"), r.get("n")] for r in rows], title="Denied API requests",
                      about="The callers whose requests to the Kubernetes API server were refused in the window (401 = not signed in, 403 = not allowed), from the audit log: who, what they tried and how often.")
            ctx.find("MED", f"{sum(int(r.get('n') or 0) for r in rows)} API request(s) denied (401/403) in window, e.g. {rows[0].get('user')}")
        else:
            rep.add("No 401/403 denials in the audit log for the window.")


def section_overview(rep, ctx, label):
    rep.section(f"1. CLUSTER OVERVIEW - {label}", "overview")
    rows = [["Report time (UTC)", f"{ctx.now:%Y-%m-%d %H:%M:%S}"],
            ["Time window", f"last {ctx.minutes} minutes (since {ctx.since:%H:%M:%S} UTC)"]]
    ok, out = kubectl(["config", "current-context"])
    ctx.meta["context"] = out if ok else "unknown"
    rows.append(["kubectl context", out if ok else 'unknown (' + out[:80] + ')'])
    ok, out = kubectl(["version", "-o", "json"])
    if ok:
        try:
            v = json.loads(out)
            rows.append(["Kubernetes client version", v.get('clientVersion', {}).get('gitVersion', '?')])
            ctx.meta["server"] = v.get("serverVersion", {}).get("gitVersion", "?")
            rows.append(["Kubernetes server version", v.get('serverVersion', {}).get('gitVersion', '?')])
        except json.JSONDecodeError:
            pass
    else:
        ctx.find("CRIT", f"Cannot reach the API server: {out.splitlines()[0][:120] if out else '?'}")
        rows.append(["Kubernetes server version", f"NOT REACHABLE ({out.splitlines()[0][:120] if out else '?'})"])
    ok, out = kubectl(["get", "--raw", "/readyz?verbose"])
    if ok:
        failing = [l.strip() for l in out.splitlines() if l.startswith("[-]")]
        if failing:
            rows.append(["API server readiness (readyz)", "FAILING checks: " + "; ".join(failing)])
            ctx.find("CRIT", f"API server readyz failing: {len(failing)} check(s)")
        else:
            rows.append(["API server readiness (readyz)", "OK"])
    else:
        rows.append(["API server readiness (readyz)", f"not readable ({out.splitlines()[0][:100] if out else '?'})"])
    rep.glossary(["kubectl", "Kubernetes context", "API server", "readyz"])
    rep.table(["Item", "Value"], rows, maxw=130,
              about="The identity of the cluster behind this report: when the data was taken, the window it covers, the kubectl connection used, the Kubernetes versions "
                    "of the tool and of the cluster, and whether the API server says it is ready.")


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
    """The 'actual server' behind a Kubernetes node on AKS: the VM scale set + instance id (from spec.providerID),
    the VM resource name, zone, VM size, priority (spot / regular), node pool, IP and the VM's power state when
    the Azure section could read it. providerID looks like
    azure:///subscriptions/<sub>/resourceGroups/<MC_rg>/providers/Microsoft.Compute/virtualMachineScaleSets/<vmss>/virtualMachines/<n>"""
    meta, spec, st = node["metadata"], node.get("spec", {}), node.get("status", {})
    labels = meta.get("labels", {})
    pid = spec.get("providerID", "") or ""
    vmss = re.search(r"virtualMachineScaleSets/([^/]+)/virtualMachines/(\d+)$", pid, re.I)
    vm = re.search(r"virtualMachines/([^/]+)$", pid, re.I)
    if vmss:
        iid, vm_name = f"{vmss.group(1)}/{vmss.group(2)}", f"{vmss.group(1)}_{vmss.group(2)}"
    elif vm:
        iid, vm_name = vm.group(1), vm.group(1)
    else:
        iid, vm_name = (pid.rsplit("/", 1)[-1] if pid else ""), "-"
    addresses = {a.get("type"): a.get("address") for a in st.get("addresses", []) or []}
    info = (ctx.data.get("vm_info") or {}).get(meta["name"].lower(), {})
    spot = (labels.get("kubernetes.azure.com/scalesetpriority") or "").lower() == "spot"
    return {
        "instance_id": iid or "-", "provider_id": pid or "-",
        "zone": labels.get("topology.kubernetes.io/zone") or labels.get("failure-domain.beta.kubernetes.io/zone") or "-",
        "type": labels.get("node.kubernetes.io/instance-type") or labels.get("beta.kubernetes.io/instance-type") or "-",
        "capacity": "spot" if spot else "regular",
        "nodegroup": labels.get("kubernetes.azure.com/agentpool") or labels.get("agentpool") or "-",
        "ip": addresses.get("InternalIP", "-"), "ec2_name": vm_name, "ec2_state": info.get("state") or "-",
    }


def node_idents(ctx):
    cache = ctx.data.get("_node_idents")
    if cache is None:
        cache = {n["metadata"]["name"]: node_identity(ctx, n) for n in items(ctx.data.get("nodes"))}
        ctx.data["_node_idents"] = cache
    return cache


def node_tag(ctx, name):
    """'aks-nodepool1-123-vmss000003 [aks-nodepool1-123-vmss/3]' - the node name with its VM scale set instance."""
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
    rep.section("3. NODES - STATUS, PROCESSOR (CPU), MEMORY, DISK, SWAP SPACE", "nodes")
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
    rep.glossary(["Node pool", "VMSS", "providerID", "Availability zone", "Spot", "Allocatable", "Request", "Ephemeral storage", "ImageFS", "Swap", "kubelet", "Metrics server", "NotReady", "Cordoned"])
    rep.table(["Node", "Virtual machine scale set / instance", "Azure virtual machine", "Availability zone", "Virtual machine size", "Capacity type", "Node pool",
               "Internal IP address", "Status", "Cloud provider identifier"], inv_rows, maxw=64,
              title="Node inventory",
              about="One row per worker node: the node name together with the actual Azure virtual machine behind it (scale set and instance number, machine name, size, zone, pool and address), read from the node's labels and its provider identifier.")
    rep.table(["Node", "Virtual machine scale set / instance", "Status", "Pods (running / maximum)", "Processor (CPU) used / allocatable", "Memory used / allocatable", "Disk used / total",
               "Container image storage used / total", "Swap space used / total"],
              usage_rows, maxw=40,
              title="Live usage per node",
              about="One row per worker node: its status and how much processor, memory, root disk, image storage and swap space it uses right now compared with what it can offer to pods, and how many pod slots are taken.")
    rep.table(["Node", "Virtual machine scale set / instance", "Roles", "Virtual machine size", "Availability zone", "Kubelet version", "Age", "Processor (CPU) requested", "Memory requested",
               "Temporary storage requested / allocatable", "Findings"],
              info_rows, maxw=110,
              title="Scheduling view",
              about="One row per worker node: how much of its processor, memory and temporary storage is already reserved by the requests of the pods placed on it (not what they use), its age, kubelet version and the reasons it was flagged.")
    notready = [(n["metadata"]["name"], node_idents(ctx).get(n["metadata"]["name"]) or {}) for n in nodes
                if not any(c["type"] == "Ready" and c["status"] == "True" for c in n.get("status", {}).get("conditions", []))]
    if notready:
        rep.add("Node logs are not readable through kubectl. For the NotReady node(s) you can read them through Azure (nothing is changed):",
                about="how to read the logs of a node that is NotReady, using read-only commands that are not run by this report.")
        for node_name, ident in notready[:5]:
            vmss, _, idx = (ident.get("instance_id") or "").partition("/")
            rep.add(f"  {node_name}:  az vmss run-command invoke -g <node-resource-group> -n {vmss or '<vmss>'} --instance-id {idx or '<id>'} "
                    f"--command-id RunShellScript --scripts \"journalctl -u kubelet --since '30 min ago' --no-pager | tail -200\"")
        rep.add("  or:  kubectl debug node/<node> -it --image=mcr.microsoft.com/cbl-mariner/busybox:2.0   (creates a debug pod)")
        rep.add("  and: Azure portal -> the VM scale set instance -> Boot diagnostics / Serial console")
    if not ctx.data.get("node_stats"):
        rep.add("Note: disk and swap come from the kubelet and need the 'nodes/proxy' permission"
                + (f" ({ctx.data.get('stats_error')})" if ctx.data.get("stats_error") else "")
                + ". Without it CPU/memory come from metrics-server (kubectl top) when installed.")


def section_node_pods(rep, ctx):
    rep.section("5. PODS ON EACH NODE - PROCESSOR (CPU), MEMORY, DISK per pod", "nodepods")
    pods = items(ctx.data.get("pods"))
    usage = ctx.data.get("pod_usage") or {}
    by_node = defaultdict(list)
    for p in pods:
        if p.get("spec", {}).get("nodeName") and p.get("status", {}).get("phase") in ("Running", "Pending"):
            by_node[p["spec"]["nodeName"]].append(p)
    if not by_node:
        rep.add("No pods are scheduled on nodes.")
        return
    rep.glossary(["Request", "Limit", "Working set", "OOMKilled", "Throttling", "Ephemeral storage", "Support team"])
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
               + (f" | virtual machine {ident['ec2_name']}" if ident.get("ec2_name", "-") != "-" else "") + "]") if ident else ""
        rep.table(["Pod", "Support team (distribution list)", "Status", "Container restarts", "Processor (CPU) used", "Processor (CPU) requested", "Processor (CPU) limit",
                   "Memory used", "Memory requested", "Memory limit", "Disk used", "Notes"],
                  [r[1] for r in rows], limit=MAX_NODE_PODS, maxw=60,
                  title=f"Node {node}{who}  -  {len(by_node[node])} pod(s) ({running} running)",
                  about=f"The pods placed on node {node} (up to {MAX_NODE_PODS}, largest memory user first): their status and restarts, live processor, memory and disk use, and what each requested and may use at most.")


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
    rep.section("6. NAMESPACES - PODS USED vs CONFIGURED", "namespaces")
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
    rep.glossary(["Namespace", "Support team", "ResourceQuota", "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job", "HPA"])
    rep.table(["Namespace", "Support team (distribution list)", "Pods in total", "Running pods"], owners, maxw=70,
              title="Who to contact for each namespace",
              about=f"One row per namespace with the support team (distribution list) read from its '{SUPPORT_LABEL}' label, how many pods it has and how many run; 'NOT SET' means nobody is named to contact.")
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
    rep.table(["Namespace", "Support team (distribution list)", "Pods in total", "Running", "Pending", "Failed", "Completed", "Configured (desired) pods", "Status", "Pod quota used / limit", "Notes"], rows, maxw=60,
              title="Pods used versus configured per namespace",
              about="One row per namespace: how many pods exist in each phase compared with how many its Deployments, StatefulSets, DaemonSets and single pods are configured to run, and the pod limit of its resource quota if it has one.")

    res_rows = []
    for n, d in sorted(ns.items(), key=lambda kv: -kv[1]["mem_use"]):
        if not d["total"]:
            continue
        res_rows.append([n, support_of(ctx, n) or "-", d["total"], _cores(d["cpu_use"]) if d["has_use"] else "n/a", _cores(d["cpu_req"]) if d["cpu_req"] else "-",
                         _mi(d["mem_use"]) if d["has_use"] else "n/a", _mi(d["mem_req"]) if d["mem_req"] else "-",
                         _mi(d["disk_use"]) if d["has_use"] and d["disk_use"] else "n/a", d["restarts"]])
    rep.table(["Namespace", "Support team (distribution list)", "Pods in total", "Processor (CPU) used", "Processor (CPU) requested", "Memory used", "Memory requested", "Disk used", "Container restarts"], res_rows,
              title="Resources used by each namespace",
              about="One row per namespace: the total processor, memory and disk its pods use right now next to the total they requested, and how often its containers restarted.")

    wl_rows.sort(key=lambda r: (r[8] == "OK", r[0], r[2]))
    if wl_rows:
        rep.table(["Namespace", "Support team (distribution list)", "Kind", "Name", "Desired", "Ready", "Available", "Running", "Horizontal pod autoscaler minimum - maximum", "Status"],
                  [[r[0], support_of(ctx, r[0]) or "-"] + r[1:] for r in wl_rows],
                  title=f"Workloads behind those pods ({len(wl_rows)})",
                  about="One row per Deployment, StatefulSet and DaemonSet: the number of pods it should run (desired), how many are ready, available and running now, its autoscaler range and whether all wanted pods are ready.")

    quota_rows = []
    for n in sorted(quotas):
        for qname, key, used, hard in quotas[n]:
            pct = _pct(used, hard)
            if pct is not None and pct >= 75:
                ctx.find("HIGH" if pct >= 90 else "MED", f"Namespace {n}{support_suffix(ctx, [n])}: quota '{qname}' {key} is {pct:.0f}% used ({_fmt_qty(key, used)}/{_fmt_qty(key, hard)})")
                ctx.ns_issue(n, f"quota '{qname}' {key} is {pct:.0f}% used")
            quota_rows.append([n, support_of(ctx, n) or "-", qname, key, f"{_fmt_qty(key, used)}/{_fmt_qty(key, hard)} ({_fp(pct)})"])
    if quota_rows:
        rep.table(["Namespace", "Support team (distribution list)", "Quota", "Resource", "Used / limit"], quota_rows,
                  title="Resource quotas",
                  about="One row per limit of every resource quota: how much of the allowed pods, processor, memory or storage the namespace has already used (75 percent and above is reported as a finding).")


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
    rep.section("7. UNHEALTHY PODS", "pods")
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
    rep.glossary(["CrashLoopBackOff", "ImagePullBackOff", "Pending", "Evicted", "OOMKilled", "Terminating", "Ready (containers)", "Namespace", "Support team"])
    rep.table(["Namespace / pod", "Support team (distribution list)", "Status", "Ready containers", "Container restarts", "Node", "Age", "Why"],
              [[f"{a['ns']}/{a['name']}", support_of(ctx, a["ns"]) or "-", a["status"], a["ready"], a["restarts"], node_tag(ctx, a["node"]), a["age"],
                "; ".join(dict.fromkeys(a["problems"]))[:200]] for a in bad], maxw=110,
              title="Pods with problems",
              about="One row per pod that is not healthy (crash loop, pending, image pull error, evicted, not ready or recently restarted), worst first: its state, how many containers are ready, restarts, the node it runs on and the reason in words.")
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
    rep.section(f"8. EVENTS (last {ctx.minutes} min)", "events")
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
        rep.glossary(["BackOff", "FailedScheduling", "Evicted", "OOMKilled"])
        rep.table(["Reason", "Occurrences"], [[r, c] for r, c in count.most_common()],
                  title="Warning events by reason",
                  about="Each reason Kubernetes gave for a Warning event in the window and how many times it happened in total; the most frequent reason is the best place to start.")
        rows = []
        for t, e in sorted(warnings, key=lambda x: x[0], reverse=True)[:MAX_EVENTS]:
            obj = e.get("involvedObject") or e.get("regarding") or {}
            obj_name = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else obj.get("name", "?")
            rows.append([age(t, ctx.now) + " ago", e.get("reason", "?"),
                         f"{obj.get('kind', '?')} {obj.get('namespace', '')}/{obj_name}".replace(" /", " "),
                         support_of(ctx, obj.get("namespace")) or "-",
                         ((e.get("series") or {}).get("count") or e.get("count") or 1),
                         (e.get("message") or e.get("note") or "").replace("\n", " ")[:140]])
        rep.table(["When", "Reason", "Object", "Support team (distribution list)", "Count", "Message"], rows, limit=MAX_EVENTS,
                  title=f"Latest Warning events (up to {MAX_EVENTS})",
                  about="The newest Warning events of the window, one row each: how long ago, the reason, the object it is about (kind, namespace and name), the owning support team, how often it repeated and the message.")
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
        rep.table(["When", "Reason", "Object", "Support team (distribution list)", "Message"], rows, limit=25,
                  title="Notable Normal events (scaling, kills, node changes)",
                  about="Events that are not warnings but explain changes in the window, such as scaling, containers being killed or nodes changing; newest first, up to 25.")


def section_workloads(rep, ctx):
    rep.section("9. WORKLOADS", "workloads")
    rep.glossary(["Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job", "kube-system", "Namespace", "Support team"])
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
        rep.table(["Kind", "Namespace / name", "Support team (distribution list)", "Ready", "Issue"],
                  [[r[0], r[1], support_of(ctx, r[1].split("/")[0]) or "-", r[2], r[3]] for r in rows],
                  title="Workloads that are not fully ready",
                  about="One row per Deployment, StatefulSet or DaemonSet that has fewer ready pods than wanted: its kind, namespace and name, the ready count (ready/wanted) and what is wrong.")
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
        rep.table(["Deployment", "Support team (distribution list)", "ReplicaSet", "Ready", "Created"], recent,
                  title=f"Recent rollouts and scale changes (new ReplicaSets in the last {ctx.minutes} min)",
                  about="Every new ReplicaSet created in the window, which means a new version was rolled out or a Deployment changed: the Deployment, the new ReplicaSet, how many of its pods are ready and when it was created.")
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
        rep.table(["Job", "Support team (distribution list)", "Failed pods", "Reason", "When"], failed,
                  title="Jobs that failed in the window",
                  about="Jobs (run-to-completion tasks) that gave up in the window: the job, how many of its pods failed, the reason Kubernetes gives and when it happened.")
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
    rep.table(["Kind", "Name", "Ready", "State"], core,
              title="Core add-ons (kube-system namespace)",
              about="The Deployments and DaemonSets of the kube-system namespace (DNS, networking, storage drivers and other cluster components): how many pods are ready out of the wanted number and whether the add-on is healthy or DEGRADED.")


# ---------------------------------------------------------------------------
# Network & traffic: CNI / DNS / services / ingress / policies / routing, and traffic in the selected window
# ---------------------------------------------------------------------------

import ipaddress

TRAFFIC_SAMPLE_SECONDS = 10      # live traffic sample from the kubelet (0 = skip); the WINDOW traffic comes from Azure Monitor
NET_EVENT_PATTERN = re.compile(
    r"network|cni|sandbox|ip address|insufficientfreeaddresses|\bdns\b|\beni\b|\broute|loadbalancer|"
    r"connection refused|i/o timeout|no route|unreachable|failed to (assign|allocate)|securitygroup", re.I)
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


CNI_COMPONENTS = [  # (daemonset in kube-system, what it is)
    ("azure-cns", "Azure CNI: assigns pod IP addresses and programs the node networking"),
    ("azure-ip-masq-agent", "Azure CNI: translates pod addresses (SNAT) for traffic leaving the virtual network"),
    ("azure-npm", "Azure Network Policy Manager (when the network policy engine is 'azure')"),
    ("cilium", "Cilium agent (Azure CNI powered by Cilium)"),
    ("calico-node", "Calico (when the network policy engine is 'calico')"),
    ("kube-proxy", "Service load balancing on each node"),
    ("konnectivity-agent", "Tunnel from the managed control plane to the nodes"),
]

# ---------------------------------------------------------------------------
# Glossary: every abbreviation / technical term the network section uses (term -> (full name, plain-language meaning))
# ---------------------------------------------------------------------------

GLOSSARY = {
    "CNI": ("Container Network Interface", "The plugin that gives every pod its network address and connects it to the cluster network."),
    "Azure CNI": ("Azure Container Network Interface", "Microsoft's CNI plugin. Classic mode gives each pod an IP address from the virtual network subnet."),
    "CNI Overlay": ("Azure CNI Overlay", "Pods get addresses from a private range that exists only inside the cluster, so the subnet only needs addresses for the nodes."),
    "kubenet": ("Kubernetes basic networking (kubenet)", "A simple plugin: pods use a private range and the node translates their addresses to leave the cluster."),
    "Cilium": ("Cilium", "A networking and security layer built on eBPF (kernel programs); in AKS it can replace kube-proxy and enforce network policies."),
    "azure-cns": ("Azure Container Networking Service", "The node agent that hands out pod IP addresses and sets up pod networking on each node."),
    "azure-ip-masq-agent": ("Azure IP masquerade agent", "Node agent that rewrites the source address of pod traffic leaving the virtual network (source network address translation)."),
    "DaemonSet": ("Kubernetes DaemonSet", "A workload that runs exactly one pod on every node (or on every matching node)."),
    "CIDR": ("Classless Inter-Domain Routing range", "A way to write an address range, for example 10.0.0.0/16 means 65,536 addresses starting at 10.0.0.0."),
    "IP": ("Internet Protocol address", "The numeric network address of a pod, node or service."),
    "VNet": ("Azure Virtual Network", "The private network in Azure that holds the node subnets."),
    "Subnet": ("Subnet", "A slice of the virtual network's address range; node (and in classic mode pod) addresses come from it."),
    "DNS": ("Domain Name System", "The service that turns names such as my-service.default.svc into IP addresses."),
    "CoreDNS": ("CoreDNS", "The DNS server that runs inside the cluster and answers pod name lookups."),
    "Corefile": ("CoreDNS configuration file", "The text file that tells CoreDNS which plugins to use and where to forward outside names."),
    "forward": ("CoreDNS forward plugin", "Sends lookups CoreDNS cannot answer to an upstream DNS server."),
    "NodeLocal DNSCache": ("NodeLocal Domain Name System cache", "A small DNS cache on every node; it reduces DNS delays and load on CoreDNS in large clusters."),
    "ndots": ("Number of dots (resolver option 'ndots')", "A name with fewer dots than this is first tried with every search domain appended (default 5 in Kubernetes), which multiplies DNS lookups."),
    "search domains": ("Resolver search domains", "Suffixes such as default.svc.cluster.local that are appended to short names before lookup."),
    "MTU": ("Maximum Transmission Unit", "The largest network packet, in bytes, that can be sent without splitting it (1500 on most networks)."),
    "NAT": ("Network address translation", "Rewriting the source or destination address of traffic as it passes a gateway."),
    "SNAT": ("Source network address translation", "Outbound traffic from a pod leaves with a shared public address; each connection uses one of a limited number of ports."),
    "NAT gateway": ("Azure NAT Gateway", "A managed service that provides many outbound public ports for a subnet."),
    "kube-proxy": ("kube-proxy", "The node component that makes a Service address reach one of its pods."),
    "iptables": ("iptables mode", "kube-proxy mode that uses Linux packet-filter rules; simple, but slows down with many Services."),
    "IPVS": ("IP Virtual Server mode", "kube-proxy mode that uses a Linux kernel load balancer; scales better with many Services."),
    "conntrack": ("Connection tracking table", "The Linux table that remembers every active network connection; when it is full, new connections are dropped."),
    "NetworkPolicy": ("Kubernetes NetworkPolicy", "A rule that says which pods may talk to which other pods; it only works if a policy engine enforces it."),
    "default-deny": ("Default-deny policy", "A policy that selects every pod and allows nothing, so only explicitly allowed traffic gets through."),
    "NPM": ("Azure Network Policy Manager", "Microsoft's engine that enforces NetworkPolicy on Azure CNI."),
    "Calico": ("Calico", "An open-source network policy engine."),
    "ClusterIP": ("ClusterIP Service", "A Service with a virtual address that is reachable only inside the cluster."),
    "NodePort": ("NodePort Service", "A Service that is also opened on a fixed port (30000-32767 by default) on every node."),
    "LoadBalancer": ("LoadBalancer Service", "A Service that asks the cloud to create a load balancer with its own address in front of the pods."),
    "Endpoint": ("Service endpoint", "The address of one ready pod behind a Service; a Service with none cannot answer."),
    "Ingress": ("Kubernetes Ingress", "A rule that sends web traffic for a host name and path to a Service."),
    "Ingress controller": ("Ingress controller", "The pods (for example NGINX) that carry out Ingress rules and proxy the web traffic."),
    "TLS": ("Transport Layer Security", "The encryption used for HTTPS; it relies on a certificate that expires."),
    "502/503/504": ("HTTP status codes 502, 503, 504", "Errors returned by a proxy: bad gateway, service unavailable, gateway timeout - often a backend that is down or slow."),
    "NSG": ("Network security group", "Azure's firewall rules attached to a subnet or network interface."),
    "UDR": ("User-defined route", "A custom route in an Azure route table that overrides the default next hop."),
    "Next hop": ("Next hop", "The device that traffic for a destination is handed to, for example a firewall appliance."),
    "Azure Firewall": ("Azure Firewall", "A managed network firewall that is often placed between the cluster and the internet."),
    "LB": ("Azure load balancer", "Distributes incoming traffic over the nodes and provides outbound connectivity."),
    "Health probe": ("Load balancer health probe", "A regular test the load balancer sends to each backend; backends that fail are taken out of rotation."),
    "VMSS": ("Virtual machine scale set", "The Azure resource that runs the nodes of a node pool."),
    "ACNS": ("Advanced Container Networking Services", "An optional AKS add-on that adds network observability (flow data, Hubble) and security features."),
    "Hubble": ("Hubble", "Cilium's tool for viewing network flows between pods."),
    "Retina": ("Retina", "Microsoft's open-source network observability and packet-capture tool for Kubernetes."),
    "Container Insights": ("Azure Monitor Container Insights", "Azure's monitoring of cluster performance and logs."),
    "Flow logs": ("Network security group / virtual network flow logs", "Azure records which connections were allowed or denied; stored for analysis."),
    "Traffic Analytics": ("Azure Traffic Analytics", "A Log Analytics view built from flow logs that shows who talks to whom."),
    "Packet capture": ("Packet capture", "Recording raw network packets to see exactly what was sent and received."),
    "429 / throttling": ("HTTP 429 Too Many Requests / throttling", "The cloud or API server refuses requests because too many arrived in a short time."),
    "API server": ("Kubernetes API server", "The control plane component every kubectl command and every controller talks to."),
    "Admission webhook": ("Admission webhook", "A service that Kubernetes calls to approve or change objects before they are saved; if it is down, creating objects can fail."),
    "Failure policy": ("Webhook failure policy", "'Fail' = reject the request when the webhook cannot be reached; 'Ignore' = continue anyway."),
    "etcd": ("etcd", "The database where the control plane stores all cluster state."),
    "kubelet": ("kubelet", "The agent on every node that starts pods and reports their status."),
    "Pod sandbox": ("Pod sandbox", "The network and namespace environment Kubernetes creates for a pod before its containers start; creating it needs the CNI plugin."),
    "ContainerCreating": ("ContainerCreating (pod state)", "The pod has been placed on a node but its containers are not running yet."),
    "Node pressure": ("Node pressure conditions", "Flags a node sets when it is short of memory, disk space or process slots."),
    "Azure Monitor": ("Azure Monitor metrics", "Azure's time series of resource measurements, such as load balancer probe availability."),
    "Activity log": ("Azure Activity log", "Azure's record of management operations and their results, including throttled requests."),
    "Standard SKU": ("Standard pricing tier", "The load balancer tier that supports zones and exposes the metrics used here."),
    "Support team": ("Support distribution list", "The e-mail list of the team that owns a namespace (from the namespace label)."),
}
_NET_TERMS = frozenset(GLOSSARY)       # the terms of the network section (its own complete glossary lists only these)
GLOSSARY.update({
    # --- cluster, Azure, nodes
    "kubectl": ("kubectl", "The Kubernetes command-line tool this report uses (read-only) to read the cluster."),
    "az": ("Azure command-line interface (az)", "The Azure tool this report uses (read-only) to read the cluster, network and monitoring data from Azure."),
    "Kubernetes context": ("kubectl context", "A saved connection (cluster address plus user) in the kubectl configuration."),
    "readyz": ("API server readiness endpoint (readyz)", "The API server's own list of internal checks; a line starting with [-] is a check that fails."),
    "Node pool": ("AKS node pool", "A group of nodes with the same virtual machine size and settings; every pool is one Azure virtual machine scale set."),
    "System pool": ("System node pool", "A pool that runs the cluster's own add-ons (DNS, metrics); a user pool runs your applications."),
    "Spot": ("Azure Spot virtual machine", "Cheap spare capacity that Azure can take back at any time; only suitable for work that can be interrupted."),
    "Availability zone": ("Azure availability zone", "A physically separate data centre inside an Azure region; spreading nodes over zones survives the loss of one."),
    "Autoscaler": ("Cluster autoscaler", "Adds nodes when pods cannot be placed and removes unused nodes, between a minimum and a maximum count."),
    "Managed identity": ("Azure managed identity", "An identity that Azure creates and rotates for the cluster, so no password or secret has to be stored."),
    "Service principal": ("Azure service principal", "An application identity with a secret that can expire; managed identity is the recommended replacement."),
    "Role assignment": ("Azure role assignment", "A permission granted to an identity on a scope, for example the right to pull images or to change a network."),
    "AcrPull": ("Azure Container Registry pull role", "The role that lets the cluster download container images from an Azure Container Registry."),
    "Entra ID": ("Microsoft Entra ID", "Microsoft's identity service (formerly Azure Active Directory) that can be used to sign in to the cluster."),
    "Azure RBAC": ("Azure role-based access control", "Using Azure roles to decide who may do what inside the Kubernetes cluster."),
    "OIDC issuer": ("OpenID Connect issuer", "A public address that publishes the cluster's token signing keys; needed for workload identity."),
    "Workload identity": ("Microsoft Entra Workload ID", "Lets a pod sign in to Azure with a token instead of a stored secret."),
    "Add-on": ("AKS add-on", "An optional Azure-managed component of the cluster, for example monitoring or policy."),
    "Control plane": ("Kubernetes control plane", "The managed part of the cluster (API server, scheduler, controllers) that Azure runs for you."),
    "kube-audit": ("Kubernetes audit log", "A record of who called the API server and what answer they got; 401 = not signed in, 403 = not allowed."),
    "kube-scheduler": ("kube-scheduler", "The control plane component that decides which node a new pod runs on."),
    "kube-controller-manager": ("kube-controller-manager", "The control plane component that runs the loops keeping the real state equal to the wanted state."),
    "Log Analytics": ("Azure Log Analytics workspace", "Azure's store for logs, searched with the Kusto query language."),
    "providerID": ("Cloud provider identifier (spec.providerID)", "The unique Azure resource path of the virtual machine behind a node."),
    "Allocatable": ("Allocatable resources", "What a node offers to pods: its capacity minus what the system keeps for itself."),
    "Request": ("Resource request", "The processor or memory a pod asks to have reserved; the scheduler uses it to choose a node."),
    "Limit": ("Resource limit", "The most processor or memory a pod may use: above the memory limit it is stopped, above the processor limit it is slowed down."),
    "Throttling": ("Processor throttling", "The operating system slows a container that wants more processor than its limit."),
    "OOMKilled": ("Out-of-memory kill (OOMKilled)", "The container used more memory than its limit (or the node ran out of memory) and the system stopped it."),
    "Working set": ("Memory working set", "The memory a container really holds and the system cannot easily take back; the number used for limits."),
    "Swap": ("Swap space", "Disk space used as slow memory when physical memory runs out; any use is a sign of memory pressure."),
    "Ephemeral storage": ("Ephemeral (temporary) storage", "Disk space a pod uses for logs, its writable layer and temporary directories; it disappears with the pod."),
    "ImageFS": ("Container image file system", "The disk area where downloaded container images are stored."),
    "Metrics server": ("Kubernetes Metrics Server", "A cluster component that serves live processor and memory use; 'kubectl top' reads it."),
    "Cordoned": ("Cordoned node (SchedulingDisabled)", "A node marked so that no new pods are placed on it; running pods stay."),
    "NotReady": ("NotReady node", "The node does not report healthy to the control plane, so no pods are scheduled on it."),
    # --- namespaces, pods, workloads, storage
    "Namespace": ("Kubernetes namespace", "A named group of objects in the cluster that separates teams or applications."),
    "ResourceQuota": ("Kubernetes ResourceQuota", "A cap on how many pods, how much processor, memory or storage a namespace may use in total."),
    "Deployment": ("Kubernetes Deployment", "A workload that keeps a wanted number of identical pods running and rolls out new versions."),
    "StatefulSet": ("Kubernetes StatefulSet", "A workload for pods that need a stable name and their own storage, such as databases."),
    "ReplicaSet": ("Kubernetes ReplicaSet", "The object a Deployment creates for each version; it keeps the wanted number of pods of that version."),
    "Job": ("Kubernetes Job", "A workload that runs a task to completion, for example a nightly batch."),
    "HPA": ("Horizontal Pod Autoscaler", "Adds or removes pod replicas automatically depending on load, between a minimum and a maximum."),
    "PVC": ("Persistent volume claim", "A pod's request for storage; Bound means a matching volume was found."),
    "PV": ("Persistent volume", "A piece of storage in the cluster (for example an Azure disk) that a claim can bind to."),
    "StorageClass": ("Kubernetes StorageClass", "The kind of storage (for example premium disk) that a claim asks for."),
    "CrashLoopBackOff": ("CrashLoopBackOff (pod state)", "The container keeps crashing and Kubernetes waits longer and longer before restarting it."),
    "ImagePullBackOff": ("ImagePullBackOff / ErrImagePull (pod state)", "The node cannot download the container image (wrong name, no permission or the registry is unreachable)."),
    "Pending": ("Pending (pod state)", "The pod is accepted but not running yet, usually because no node has room for it or its storage is not ready."),
    "Evicted": ("Evicted (pod state)", "The node removed the pod to protect itself, usually because it ran short of memory or disk."),
    "Terminating": ("Terminating (state)", "The object is being deleted; when it stays so for long, something blocks the deletion."),
    "FailedScheduling": ("FailedScheduling (event)", "The scheduler found no node where the pod fits (not enough processor, memory or free pod slots)."),
    "BackOff": ("BackOff (event)", "A container keeps failing and Kubernetes is waiting before it tries again."),
    "Ready (containers)": ("Ready containers", "A container is ready when it passes its readiness check and may receive traffic."),
    "kube-system": ("kube-system namespace", "The namespace where Kubernetes and Azure run the cluster's own components."),
})

NET_OK, NET_WARN, NET_BAD, NET_NA = "OK", "Warning", "Problem", "Not available"


def _combine(statuses):
    """The status of several parts together: any Problem -> Problem, else any Warning -> Warning, else any Not available -> Not available
    (never OK when a part could not be checked), else OK."""
    sts = list(statuses)
    for want in (NET_BAD, NET_WARN, NET_NA):
        if want in sts:
            return want
    return NET_OK if sts else NET_NA


def _netcheck(rep, ctx, key, title, status, what, evidence, meaning, finding=None):
    """Write one check block and remember it for the final checklist. `finding` adds a Health-summary line for Problem / Warning."""
    ctx.data.setdefault("net_checks", {})[key] = {"title": title, "status": status, "evidence": evidence, "meaning": meaning}
    rep.check(title, status, what, evidence, meaning)
    if finding and status in (NET_BAD, NET_WARN):
        ctx.find("HIGH" if status == NET_BAD else "MED", finding)


def _short(text, n=140):
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    return text if len(text) <= n else text[: n - 1] + "~"


def _ds(ctx, name, ns="kube-system"):
    return next((d for d in items(ctx.data.get("daemonsets")) if d["metadata"]["namespace"] == ns and d["metadata"]["name"] == name), None)


def _ds_counts(ds):
    st = ds.get("status", {})
    return st.get("numberReady", 0), st.get("desiredNumberScheduled", 0)


def _ds_version(ds):
    cont = (ds.get("spec", {}).get("template", {}).get("spec", {}).get("containers") or [{}])[0]
    image = cont.get("image", "?").split("/")[-1]
    return image[:50]


def _pods_where(ctx, ns=None, labels=None, name_re=None):
    out = []
    for p in items(ctx.data.get("pods")):
        m = p["metadata"]
        if ns and m.get("namespace") != ns:
            continue
        if labels and any((m.get("labels") or {}).get(k) != v for k, v in labels.items()):
            continue
        if name_re and not re.search(name_re, m.get("name", "")):
            continue
        out.append(p)
    return out


def _pod_ready(p):
    return any(c.get("type") == "Ready" and c.get("status") == "True" for c in p.get("status", {}).get("conditions", []) or [])


def _pod_restarts(p):
    return sum(cs.get("restartCount", 0) for cs in p.get("status", {}).get("containerStatuses", []) or [])


def _cm(ctx, name, ns="kube-system"):
    """data of a ConfigMap (cached), or None when it does not exist / cannot be read."""
    cache = ctx.data.setdefault("_net_cm", {})
    key = (ns, name)
    if key not in cache:
        cache[key] = (_kobj(["get", "configmap", name, "-n", ns]) or {}).get("data")
    return cache[key]


def _flatten(obj, prefix="", depth=0):
    """Scalar settings of a nested JSON object as [(dotted.key, value)]."""
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)) and depth < 3:
                out += _flatten(v, f"{prefix}{k}.", depth + 1)
            elif not isinstance(v, (dict, list)):
                out.append((f"{prefix}{k}", v))
    elif isinstance(obj, list) and obj and not any(isinstance(x, (dict, list)) for x in obj):
        out.append((prefix.rstrip("."), ",".join(str(x) for x in obj)))
    return out


def _logs(ctx, selector=None, ns="kube-system", pod=None, tail=800):
    """(lines, error) of kubectl logs for a label selector or one pod over the report window; read-only."""
    args = ["logs", "-n", ns]
    if pod:
        args += [pod, "--all-containers"]
    else:
        args += ["-l", selector, "--all-containers", "--prefix", "--max-log-requests=20"]
    args += [f"--since={ctx.minutes}m", f"--tail={tail}"]
    ok, out = kubectl(args, timeout=90)
    if not ok:
        return None, (out.splitlines()[0][:100] if out else "unreadable")
    return out.splitlines(), None


def _prefetch_logs(ctx, jobs):
    """jobs: {key: kwargs for _logs}; runs them in parallel, stores ctx.data['net_logs'][key] = (lines, error)."""
    store = ctx.data.setdefault("net_logs", {})
    todo = {k: v for k, v in jobs.items() if k not in store}
    if not todo:
        return
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {k: pool.submit(_logs, ctx, **v) for k, v in todo.items()}
        for k, f in futs.items():
            try:
                store[k] = f.result()
            except Exception as exc:
                store[k] = (None, str(exc))


def _az_state(ctx):
    """('ok', cluster, target) when the Azure section produced the cluster, else ('off', None, None)."""
    target, cluster = ctx.data.get("az_target"), ctx.data.get("az_cluster")
    if AZ_OPTS["enabled"] and target and cluster:
        return True, cluster, target
    return False, None, None


AZ_OFF_TEXT = "the Azure details are off or could not be read (run `az login` and keep 'Azure details' on)"


# ---------------------------------------------------------------------------
# Azure reads for the network section (read-only; done once and cached in ctx.data["net_az"])
# ---------------------------------------------------------------------------

def _az_metric(ctx, target, resource_id, metric, aggregation, split=None, interval="PT1M"):
    """{dimension value: [(datetime, value)]} from Azure Monitor for the selected window, or (None, error)."""
    args = ["monitor", "metrics", "list", "--resource", resource_id, "--metric", metric, "--aggregation", aggregation,
            "--interval", interval, "--start-time", _iso(ctx.since), "--end-time", _iso(ctx.now)]
    if split:
        args += ["--filter", f"{split} eq '*'"]
    data, err = az_cli(args, target, 90)
    if err:
        return None, err
    key = aggregation.lower()
    series = {}
    for m in (data.get("value") or []) if isinstance(data, dict) else []:
        for ts in m.get("timeseries") or []:
            name = next((md.get("value") for md in (ts.get("metadatavalues") or [])), "") or ""
            pts = []
            for d in ts.get("data") or []:
                t, v = parse_ts(d.get("timeStamp")), d.get(key)
                if t is not None and v is not None:
                    pts.append((t, v))
            series[name] = sorted(pts, key=lambda x: x[0])
    return series, None


def _line(label, pts, fmt, per_second=None, total=True):
    """A series line for the HTML charts + its numbers. per_second: divide each value by this many seconds."""
    vals = [(v / per_second) if per_second else v for _, v in pts]
    return {"l": label, "f": fmt, "p": [round(v, 3) for v in vals], "ts": [int(t.timestamp()) for t, _ in pts],
            "avg": (sum(vals) / len(vals)) if vals else None, "max": max(vals) if vals else None,
            "sum": sum(v for _, v in pts) if total else None}


def _net_az_collect(ctx):
    """Route tables, public IPs, load balancers (+ their Azure Monitor metrics), NAT gateways, network security groups, flow logs and
    Azure Firewalls - each read once. Every failure is kept in az['errors'] and shown where the data is used."""
    cache = ctx.data.get("net_az")
    if cache is not None:
        return cache
    ok, cluster, target = _az_state(ctx)
    az = {"ok": ok, "errors": [], "route_tables": [], "route_rows": [], "pips": None, "lbs": None, "lb_rows": [], "nats": [], "nsgs": None,
          "lb_metrics": [], "nat_metrics": [], "flow_logs": None, "firewalls": None, "activity": None}
    ctx.data["net_az"] = az
    if not ok:
        return az
    seen = set()
    for sn in ctx.data.get("az_subnets") or []:
        rt_id = (sn.get("routeTable") or {}).get("id")
        if not rt_id or rt_id in seen:
            continue
        seen.add(rt_id)
        rt, err = az_cli(["network", "route-table", "show", "--ids", rt_id], target, 60)
        if err or not isinstance(rt, dict):
            az["errors"].append(f"route table {_res_name(rt_id)}: {_first_line(err or 'no data', 80)}")
            continue
        az["route_tables"].append(rt)
        for r in rt.get("routes") or []:
            az["route_rows"].append([rt.get("name"), r.get("name"), r.get("addressPrefix"), r.get("nextHopType"), r.get("nextHopIpAddress") or "-"])
    node_rg = target.get("node_rg")
    if node_rg:
        pips, err = az_cli(["network", "public-ip", "list", "-g", node_rg], target, 60)
        if not err and isinstance(pips, list):
            az["pips"] = [[p.get("name"), p.get("ipAddress") or "(not allocated)", (p.get("sku") or {}).get("name"), p.get("publicIPAllocationMethod"),
                           _res_name(((p.get("ipConfiguration") or {}).get("id") or "").split("/frontendIPConfigurations")[0]) or "-"] for p in pips]
        elif err:
            az["errors"].append(f"public IP addresses: {_first_line(err, 80)}")
        lb_list, err = az_cli(["network", "lb", "list", "-g", node_rg], target, 90)
        if not err and isinstance(lb_list, list):
            az["lbs"] = []
            for lb in lb_list:
                fronts = [(f.get("privateIPAddress") or "public") for f in lb.get("frontendIPConfigurations") or []]
                az["lb_rows"].append([lb.get("name"), (lb.get("sku") or {}).get("name"), ", ".join(fronts)[:50], len(lb.get("loadBalancingRules") or []),
                                      len(lb.get("outboundRules") or []), len(lb.get("probes") or []), len(lb.get("backendAddressPools") or [])])
                az["lbs"].append({"id": lb.get("id"), "name": lb.get("name"), "sku": (lb.get("sku") or {}).get("name")})
        elif err:
            az["errors"].append(f"load balancers: {_first_line(err, 80)}")
        ng, err = az_cli(["network", "nat", "gateway", "list", "-g", node_rg], target, 60)
        for g in ([] if err or not isinstance(ng, list) else ng):
            az["nats"].append({"id": g.get("id"), "name": g.get("name")})
        nsgs, err = az_cli(["network", "nsg", "list", "-g", node_rg], target, 60)
        if not err and isinstance(nsgs, list):
            az["nsgs"] = [{"name": n.get("name"), "id": n.get("id"), "rules": n.get("securityRules") or [], "where": "node resource group"} for n in nsgs]
        elif err:
            az["errors"].append(f"network security groups (node resource group): {_first_line(err, 80)}")
    for sn in ctx.data.get("az_subnets") or []:
        nid = (sn.get("natGateway") or {}).get("id")
        if nid and nid not in [n["id"] for n in az["nats"]]:
            az["nats"].append({"id": nid, "name": _res_name(nid)})
    for n in ctx.data.get("az_nsgs") or []:                      # the NSGs of the node subnets (read in the Azure section)
        if n.get("id") not in [x.get("id") for x in (az["nsgs"] or [])]:
            az["nsgs"] = (az["nsgs"] or []) + [dict(n, where="node subnet")]
    if ctx.cancel is not None and ctx.cancel.is_set():
        return az
    # --- Azure Monitor: load balancer + NAT gateway metrics for the window
    for lb in (az["lbs"] or [])[:6]:
        got, errs = {}, []
        for key, metric, agg in (("bytes", "ByteCount", "Total"), ("packets", "PacketCount", "Total"), ("snat", "SnatConnectionCount", "Total"),
                                 ("used", "UsedSnatPorts", "Average"), ("alloc", "AllocatedSnatPorts", "Average"), ("dip", "DipAvailability", "Average"),
                                 ("vip", "VipAvailability", "Average")):
            s, err = _az_metric(ctx, target, lb["id"], metric, agg)
            if s is not None and any(s.values()):
                got[key] = sorted((p for pts in s.values() for p in pts), key=lambda x: x[0]) if len(s) > 1 else next(iter(s.values()))
            elif err:
                errs.append(err)
        failed = None
        s, err = _az_metric(ctx, target, lb["id"], "SnatConnectionCount", "Total", split="ConnectionState")
        if s:
            failed = sum(v for name, pts in s.items() if "fail" in (name or "").lower() for _, v in pts) if any("fail" in (n or "").lower() for n in s) else None
        az["lb_metrics"].append({"lb": lb, "got": got, "errs": errs, "failed_snat": failed})
    for nat in az["nats"][:6]:
        got = {}
        for key, metric in (("bytes", "ByteCount"), ("packets", "PacketCount"), ("drop", "PacketDropCount"), ("snat", "SNATConnectionCount")):
            s, err = _az_metric(ctx, target, nat["id"], metric, "Total")
            if s:
                got[key] = next(iter(s.values()))
        if got:
            az["nat_metrics"].append({"nat": nat, "got": got})
    # --- flow logs (network security group / virtual network), Azure Firewalls, Activity log (throttling)
    if cluster.get("location"):
        fl, err = az_cli(["network", "watcher", "flow-log", "list", "--location", cluster["location"]], target, 60)
        if not err and isinstance(fl, list):
            az["flow_logs"] = fl
        elif err:
            az["errors"].append(f"flow logs: {_first_line(err, 80)}")
    if _has_extension("azure-firewall"):               # `az network firewall` is an extension command: never trigger an install prompt
        fw, err = az_cli(["network", "firewall", "list"], target, 60)
        if not err and isinstance(fw, list):
            az["firewalls"] = fw
    scope = node_rg or target.get("resource_group")
    if scope:
        act, err = az_cli(["monitor", "activity-log", "list", "--resource-group", scope, "--start-time", _iso(ctx.since), "--end-time", _iso(ctx.now),
                           "--max-events", "200"], target, 90)
        if not err and isinstance(act, list):
            az["activity"] = act
        elif err:
            az["errors"].append(f"Activity log: {_first_line(err, 80)}")
    return az


# ---------------------------------------------------------------------------
# 1. Pod-level networking
# ---------------------------------------------------------------------------

def _cni_kind(nw):
    plugin, mode, dataplane = (nw.get("networkPlugin") or "").lower(), (nw.get("networkPluginMode") or "").lower(), (nw.get("networkDataplane") or "").lower()
    if not plugin:
        return None
    if plugin == "kubenet":
        return "kubenet (basic networking)"
    if plugin == "none":
        return "none (bring your own plugin)"
    base = "Azure CNI Overlay" if mode == "overlay" else "Azure CNI (pods use virtual network subnet addresses)"
    return base + (" powered by Cilium" if dataplane == "cilium" else "")


def _net_settings(rep, ctx):
    ok, cluster, _target = _az_state(ctx)
    nw = (cluster or {}).get("networkProfile") or {}
    na = "not available (" + AZ_OFF_TEXT + ")"
    services = {(s["metadata"]["namespace"], s["metadata"]["name"]): s for s in items(ctx.data.get("services"))}
    dns_svc, k8s_svc = services.get(("kube-system", "kube-dns"), {}), services.get(("default", "kubernetes"), {})
    pod_cidrs = sorted({c for n in items(ctx.data.get("nodes")) for c in (n.get("spec", {}).get("podCIDRs") or [n.get("spec", {}).get("podCIDR")]) if c})
    mode = None
    cm = _cm(ctx, "kube-proxy-config")
    if cm is not None:
        m = re.search(r"^\s*mode:\s*\"?([\w-]*)\"?", cm.get("config", ""), re.M)
        mode = ((m.group(1) if m else "") or "iptables") + " (read from the kube-proxy-config ConfigMap)"
    if not mode and ((nw.get("kubeProxyConfig") or {}).get("mode")):
        mode = str(nw["kubeProxyConfig"]["mode"]).lower() + " (read from the cluster's Azure settings)"
    ctx.data["net_kp_mode"] = mode
    lbp = nw.get("loadBalancerProfile") or {}
    adv = nw.get("advancedNetworking") or {}
    rows = [
        ["Network plugin (CNI)", _cni_kind(nw) or na, "How pods get their IP addresses and connect to the network."],
        ["Network policy engine", (nw.get("networkPolicy") or "none") if ok else na, "The component that enforces NetworkPolicy rules (Azure NPM, Calico or Cilium)."],
        ["Network dataplane", (nw.get("networkDataplane") or "azure") if ok else na, "'cilium' = eBPF dataplane that can replace kube-proxy; 'azure' = the standard one."],
        ["Service address range (CIDR)", ",".join(nw.get("serviceCidrs") or [nw.get("serviceCidr") or "-"]) + f"  (kubernetes Service {k8s_svc.get('spec', {}).get('clusterIP') or '?'})",
         "Virtual addresses of Services; must not overlap the node, pod or on-premises ranges."],
        ["DNS Service address", f"{dns_svc.get('spec', {}).get('clusterIP', '?')}  ports "
         + (",".join(str(p.get('port')) + '/' + p.get('protocol', '') for p in dns_svc.get('spec', {}).get('ports', [])) or '?'), "The address every pod uses to ask CoreDNS for names."],
        ["Pod address ranges (CIDR)", ", ".join((nw.get("podCidrs") or [nw.get("podCidr")]) if (nw.get("podCidrs") or nw.get("podCidr")) else
                                                (pod_cidrs[:6] if pod_cidrs else ["none set on the nodes - pods take addresses from the virtual network subnet (classic Azure CNI)"])),
         "Where pod addresses come from (overlay / kubenet) or 'subnet' for classic Azure CNI."],
        ["Internet Protocol versions", ",".join(nw.get("ipFamilies") or ["IPv4"]) if ok else "IPv4 (assumed)", "Address families the cluster uses."],
        ["kube-proxy mode", mode or "unknown (the kube-proxy-config ConfigMap is not readable - kube-proxy may be replaced by Cilium)",
         "iptables (Linux packet-filter rules) or IPVS (kernel load balancer); IPVS scales better with thousands of Services."],
        ["Outbound (egress) type", (nw.get("outboundType") or "loadBalancer") if ok else na, "How traffic leaves the cluster: load balancer, NAT gateway or a user-defined route (firewall)."],
        ["Load balancer pricing tier", (nw.get("loadBalancerSku") or "-") if ok else na, "Standard is required for zones, outbound rules and the health metrics used in this report."],
        ["Outbound ports per node (SNAT)", (str(lbp.get("allocatedOutboundPorts")) if lbp.get("allocatedOutboundPorts") else "automatic (default)") if ok else na,
         "Ports each node may use for outbound connections; too few causes outbound connection failures."],
        ["Advanced Container Networking Services", ("enabled" if adv.get("enabled") else "not enabled") if ok else na,
         "Optional add-on for network observability (Hubble, flow data) and security."],
    ]
    rep.heading("Cluster network settings",
                "The network design of this cluster (plugin, address ranges, policy engine, kube-proxy mode, outbound path) and what each setting means.")
    rep.glossary(["CNI", "Azure CNI", "CNI Overlay", "kubenet", "Cilium", "CIDR", "DNS", "CoreDNS", "kube-proxy", "iptables", "IPVS", "NetworkPolicy", "SNAT", "ACNS"])
    rep.table(["Setting", "Value", "What it means"], rows, maxw=90,
              what="one row per network setting of the cluster, read from Azure and from the cluster itself; 'not available' means the data could not be read.")


def _net_cni_health(rep, ctx):
    ok, cluster, _t = _az_state(ctx)
    nw = (cluster or {}).get("networkProfile") or {}
    plugin = (nw.get("networkPlugin") or "").lower()
    dss = ctx.data.get("daemonsets")
    rows, bad, present = [], [], []
    for name, why in CNI_COMPONENTS:
        ds = _ds(ctx, name)
        if not ds:
            continue
        ready, want = _ds_counts(ds)
        present.append(name)
        state = "OK" if want and ready >= want else ("DEGRADED" if want else "No nodes selected")
        rows.append([name, f"{ready} / {want}", _ds_version(ds), state, why])
        if want and ready < want:
            bad.append(f"{name} {ready}/{want}")
            ctx.find("HIGH", f"Network component kube-system/{name} is degraded ({ready}/{want} ready)")
    # key settings (read-only ConfigMaps)
    settings = []
    cns = _cm(ctx, "azure-cns-config")
    for k, raw in (cns or {}).items():
        try:
            for fk, fv in _flatten(json.loads(raw))[:14]:
                settings.append(["azure-cns", fk, _short(fv, 80)])
        except (ValueError, TypeError):
            settings.append(["azure-cns", k, _short(raw, 80)])
    masq = _cm(ctx, "azure-ip-masq-agent-config-reconciled") or _cm(ctx, "azure-ip-masq-agent-config")
    for k, raw in (masq or {}).items():
        settings.append(["azure-ip-masq-agent", k, _short(raw, 100)])
    cil = _cm(ctx, "cilium-config")
    for k in ("routing-mode", "tunnel", "tunnel-protocol", "kube-proxy-replacement", "ipam", "enable-hubble", "mtu", "enable-ipv4", "enable-policy",
              "enable-l7-proxy", "enable-bpf-masquerade", "cluster-pool-ipv4-cidr"):
        if cil and k in cil:
            settings.append(["cilium", k, _short(cil[k], 80)])
    if nw.get("networkDataplane"):
        settings.append(["cluster", "networkDataplane", nw.get("networkDataplane")])
    ctx.data["net_cni_settings"] = settings
    if dss is None:
        status, ev, mean = NET_NA, "the DaemonSets could not be read (permission or API error).", "Grant read access to DaemonSets in kube-system, or check the plugin in the Azure portal (cluster > Networking)."
    elif not rows:
        status = NET_WARN if plugin in ("azure", "kubenet") else NET_NA
        ev = "none of the usual network DaemonSets were found in kube-system" + (f" although the cluster uses '{plugin}'" if plugin else "") + "."
        mean = "Check that the network add-ons are running (kubectl -n kube-system get pods -o wide); they may be managed outside kube-system."
    elif bad:
        status, ev = NET_BAD, "not fully ready: " + ", ".join(bad) + "; ready: " + ", ".join(f"{r[0]} {r[1]}" for r in rows if r[3] == "OK") + "."
        mean = ("Pods on the nodes without a ready network agent cannot get an IP address or connect - look at the failing DaemonSet pods "
                "(kubectl -n kube-system describe pod, logs) and at the node conditions; fix before debugging applications.")
    elif plugin == "azure" and "azure-cns" not in present and "cilium" not in present:
        status, ev = NET_WARN, "the cluster uses Azure CNI but no azure-cns DaemonSet was found; found: " + ", ".join(present) + "."
        mean = "Confirm in kube-system that the Azure CNI agent is present; pods may not be getting addresses."
    else:
        status, ev = NET_OK, "all network DaemonSets are ready on every node: " + ", ".join(f"{r[0]} {r[1]}" for r in rows) + "."
        mean = "The network plugin agents are healthy. If pods still cannot reach each other, continue with the logs, kube-proxy and policy checks below."
    kind = _cni_kind(nw)
    rep.glossary(["CNI", "azure-cns", "azure-ip-masq-agent", "DaemonSet", "NPM", "Calico", "Cilium", "kube-proxy"])
    _netcheck(rep, ctx, "cni_health", "Container Network Interface plugin health", status,
              "whether the network plugin agents (DaemonSets) are ready on every node, which versions run and which key settings are in use"
              + (f" (this cluster: {kind})" if kind else "") + ".", ev, mean,
              finding=None)
    rep.table(["Component", "Ready / wanted nodes", "Version (image)", "State", "What it does"], rows, maxw=70,
              what="every network DaemonSet found in kube-system with how many nodes are ready out of the nodes it should run on.")
    if settings:
        rep.table(["Component", "Setting", "Value"], settings, maxw=80,
                  what="key plugin settings read from the add-on ConfigMaps (read-only); they decide address allocation, masquerading and the Cilium dataplane.")
    else:
        rep.add("  Key plugin settings: not readable (the plugin ConfigMaps azure-cns-config / cilium-config were not found or not allowed).")
    if plugin == "azure" and (nw.get("networkPluginMode") or "").lower() != "overlay":
        rep.add("  Note: classic Azure CNI uses one virtual network IP address per pod and reserves the maximum pods per node up front - watch the free addresses below.")


def _net_ip_exhaustion(rep, ctx):
    ok, cluster, _t = _az_state(ctx)
    nw = (cluster or {}).get("networkProfile") or {}
    plugin, mode = (nw.get("networkPlugin") or "").lower(), (nw.get("networkPluginMode") or "").lower()
    pods = items(ctx.data.get("pods"))
    with_ip = [p for p in pods if p.get("status", {}).get("podIP") and not p.get("spec", {}).get("hostNetwork") and p.get("status", {}).get("phase") in ("Running", "Pending")]
    nets = []
    for sn in ctx.data.get("az_subnets") or []:
        for pref in (sn.get("addressPrefixes") or [sn.get("addressPrefix")]):
            try:
                nets.append((sn.get("name"), ipaddress.ip_network(pref, strict=False), sn.get("_free"), sn.get("_capacity")))
            except (TypeError, ValueError):
                pass
    pod_cidr = None
    for pref in (nw.get("podCidrs") or [nw.get("podCidr")]):
        try:
            net = ipaddress.ip_network(pref, strict=False)
            nets.append(("pod address range (overlay / kubenet)", net, None, net.num_addresses))
            pod_cidr = pod_cidr or net
        except (TypeError, ValueError):
            pass
    used, by_ns = Counter(), defaultdict(Counter)
    for p in with_ip:
        try:
            ip = ipaddress.ip_address(p["status"]["podIP"])
        except ValueError:
            continue
        for name, net, _f, _c in nets:
            if ip.version == net.version and ip in net:
                key = (name, str(net))
                used[key] += 1
                by_ns[key][p["metadata"]["namespace"]] += 1
                break
    rows = []
    for name, net, free, cap in nets:
        key = (name, str(net))
        rows.append([name, str(net), "n/a" if free is None else free, used[key], ", ".join(f"{n} ({c})" for n, c in by_ns[key].most_common(3)) or "-"])
    # pod slots per node (from the cluster itself)
    per_node = Counter(p.get("spec", {}).get("nodeName") for p in pods if p.get("status", {}).get("phase") in ("Running", "Pending") and p.get("spec", {}).get("nodeName"))
    full = []
    slots = used_slots = 0
    for n in items(ctx.data.get("nodes")):
        try:
            cap = int((n.get("status", {}).get("allocatable") or {}).get("pods", 0))
        except (TypeError, ValueError):
            cap = 0
        slots += cap
        u = per_node.get(n["metadata"]["name"], 0)
        used_slots += u
        if cap and u >= 0.9 * cap:
            full.append(f"{node_tag(ctx, n['metadata']['name'])} {u}/{cap}")
    ex_events = [e for e in items(ctx.data.get("events")) if re.search(r"InsufficientFreeAddresses|no (available|free) IP|IP.*exhaust|SubnetFull|subnet is full|failed to allocate", f"{e.get('reason', '')} {e.get('message') or e.get('note') or ''}", re.I)
                 and (_event_time(e) or ctx.since) >= ctx.since]
    parts, statuses, evidence = [], [], []
    subnet_min = None
    for name, net, free, cap in nets:
        if free is not None:
            subnet_min = free if subnet_min is None else min(subnet_min, free)
            evidence.append(f"{name} {net}: {free} of {cap} usable addresses free")
    if subnet_min is not None:
        statuses.append(NET_BAD if subnet_min < 10 else (NET_WARN if subnet_min < LOW_SUBNET_IPS else NET_OK))
    elif ok and plugin == "azure" and mode != "overlay":
        statuses.append(NET_NA)
        evidence.append("the node subnet's free addresses could not be read (the cluster uses the managed virtual network or Azure returned no subnet)")
    if pod_cidr is not None and plugin in ("azure", "kubenet"):
        nodes_n = len(items(ctx.data.get("nodes")))
        max_nodes = 2 ** (24 - pod_cidr.prefixlen) if pod_cidr.prefixlen <= 24 else 1
        evidence.append(f"pod address range {pod_cidr}: room for about {max_nodes} nodes (each node takes a /24 slice), {nodes_n} in use")
        statuses.append(NET_BAD if nodes_n >= max_nodes else (NET_WARN if nodes_n >= 0.85 * max_nodes else NET_OK))
    if slots:
        evidence.append(f"pod slots on the nodes: {used_slots} of {slots} in use ({100 * used_slots // max(1, slots)}%)" + (f"; nodes almost full: {', '.join(full[:4])}" if full else ""))
        statuses.append(NET_WARN if full else NET_OK)
    if ex_events:
        statuses.append(NET_BAD)
        evidence.append(f"{len(ex_events)} warning event(s) mention missing free addresses (e.g. {_short(ex_events[0].get('message') or ex_events[0].get('note'), 90)})")
    if not statuses:
        statuses.append(NET_NA)
        evidence.append("no subnet, pod address range or node pod capacity could be read")
    status = _combine(statuses)
    mean = {NET_BAD: "Pods cannot get addresses: free addresses in the subnet / pod range (or the nodes' pod slots) are used up. Use a larger subnet or an overlay pod range, "
                     "add node pools in another subnet, or reduce the maximum pods per node; scale-out will keep failing until then.",
            NET_WARN: "Address space is getting tight. Plan more room (bigger subnet, Azure CNI Overlay, or fewer pods per node) before the next scale-out.",
            NET_NA: "Part of the address data could not be read; enable the Azure details and Reader access on the virtual network to see the free addresses.",
            NET_OK: "There is enough room for pods and nodes right now."}[status]
    _netcheck(rep, ctx, "ip_exhaustion", "IP address exhaustion (node and pod subnets)", status,
              "how many addresses are still free in the node subnet and the pod address range, and how full the nodes' pod slots are.",
              "; ".join(evidence) + ".", mean, finding="IP address space for pods / nodes is nearly exhausted or exhausted - see the 'IP address exhaustion' check in the network section")
    if rows:
        rep.table(["Subnet / pod CIDR", "Range", "Free IPs", "Pod IPs", "Top namespaces"], rows, maxw=70,
                  what="each subnet or pod address range with the addresses still free and how many running pods currently hold an address inside it.")
    ctx.data["net_pod_ip_rows"] = rows


def _net_cni_logs(rep, ctx):
    jobs = {}
    for name, sel in (("azure-cns", "k8s-app=azure-cns"), ("azure-ip-masq-agent", "k8s-app=azure-ip-masq-agent"), ("cilium", "k8s-app=cilium"),
                      ("calico-node", "k8s-app=calico-node"), ("azure-npm", "k8s-app=azure-npm")):
        if _ds(ctx, name):
            jobs[name] = {"selector": sel}
    _prefetch_logs(ctx, jobs)
    pat = {"alloc": r"no (available|free) IP|failed to allocate|IP.*exhaust|insufficient.*address|subnet (is )?full|SubnetFull|out of IP",
           "throttle": r"\b429\b|TooManyRequests|throttl|RequestRateLimit|rate limit",
           "errors": r"\berror\b|level=error|\bERROR\b|\bE\d{4} "}
    rows, statuses, evidence, throttle_hits = [], [], [], 0
    for name in jobs:
        lines, err = ctx.data["net_logs"][name]
        if lines is None:
            rows.append([name, "-", "-", "-", "-", "unavailable: " + (err or "?")])
            statuses.append(NET_NA)
            evidence.append(f"{name} logs unreadable")
            continue
        c = {k: sum(1 for l in lines if re.search(p, l, re.I if k != "errors" else 0)) for k, p in pat.items()}
        sample = next((l for l in reversed(lines) if re.search(r"error|fail|timeout|429|throttl", l, re.I)), "")
        rows.append([name, len(lines), c["errors"], c["alloc"], c["throttle"], _short(sample, 110)])
        throttle_hits += c["throttle"]
        if c["alloc"]:
            ctx.find("HIGH", f"{name} logged {c['alloc']} 'no free IP / failed to allocate' errors in the window - subnet IP exhaustion")
            statuses.append(NET_BAD)
            evidence.append(f"{name}: {c['alloc']} failed IP allocation line(s)")
        elif c["throttle"]:
            statuses.append(NET_WARN)
            evidence.append(f"{name}: {c['throttle']} throttling line(s)")
        elif c["errors"] >= 20:
            statuses.append(NET_WARN)
            evidence.append(f"{name}: {c['errors']} error-like lines")
        else:
            statuses.append(NET_OK)
            evidence.append(f"{name}: {len(lines)} log lines, {c['errors']} error-like")
    ctx.data["net_throttle_cni"] = throttle_hits
    if not jobs:
        status, ev, mean = NET_NA, "no network plugin DaemonSet (azure-cns, Cilium, Calico) was found, so there are no plugin logs to read.", \
            "Check the plugin in the Azure portal; logs of a plugin that runs outside kube-system are not read here."
    else:
        status = _combine(statuses)
        ev = "; ".join(evidence) + f" (last {ctx.minutes} min)."
        mean = {NET_BAD: "The plugin could not allocate IP addresses for pods: free up or enlarge the subnet / pod range (see the address exhaustion check); new pods will stay in ContainerCreating.",
                NET_WARN: "The plugin reports throttling or many errors. Read the latest error below; if it is throttling (HTTP 429), reduce parallel scale-outs and check the Azure API throttling check.",
                NET_NA: "Some plugin logs could not be read; check kubectl logs permission in kube-system.",
                NET_OK: "No allocation failures or throttling in the plugin logs for this window."}[status]
    _netcheck(rep, ctx, "cni_logs", "Container Network Interface daemon logs", status,
              f"the last {ctx.minutes} minutes of the network plugin logs, searched for failed IP address allocation, full subnets, throttling and errors.", ev, mean)
    if rows:
        rep.table(["Component", "Log lines read", "Error-like lines", "Failed IP address allocations", "Throttling lines (429)", "Latest error"], rows, maxw=70,
                  what="how many plugin log lines were read per component and how many of them show allocation failures, throttling or errors.")


def _net_stuck_pods(rep, ctx):
    pods = ctx.data.get("pods")
    if pods is None:
        _netcheck(rep, ctx, "pods_stuck", "Pods stuck in ContainerCreating or failing to create a pod sandbox", NET_NA,
                  "pods that cannot start because the network could not be set up.", "the pod list could not be read.", "Grant read access to pods and events and run again.")
        return
    events = {}
    for e in items(ctx.data.get("events")):
        t = _event_time(e)
        if e.get("reason") in ("FailedCreatePodSandBox", "FailedCreatePodSandbox") or re.search(r"failed to (setup|set up) network|NetworkNotReady|network is not ready|failed to create pod sandbox", e.get("message") or e.get("note") or "", re.I):
            if t and t >= ctx.since:
                obj = e.get("involvedObject") or e.get("regarding") or {}
                events.setdefault((obj.get("namespace"), obj.get("name")), []).append((t, e))
    rows, old = [], 0
    seen = set()
    for p in items(pods):
        meta, st = p["metadata"], p.get("status", {})
        key = (meta.get("namespace"), meta.get("name"))
        created = parse_ts(meta.get("creationTimestamp"))
        waiting = next((((cs.get("state") or {}).get("waiting") or {}).get("reason") for cs in (st.get("containerStatuses") or []) + (st.get("initContainerStatuses") or [])
                        if ((cs.get("state") or {}).get("waiting") or {}).get("reason") == "ContainerCreating"), None)
        age_s = (ctx.now - created).total_seconds() if created else 0
        stuck = st.get("phase") == "Pending" and waiting == "ContainerCreating" and age_s >= 120
        if stuck or key in events:
            ev = sorted(events.get(key, []), key=lambda x: x[0], reverse=True)
            msg = _short((ev[0][1].get("message") or ev[0][1].get("note")) if ev else "", 110) or "-"
            rows.append([f"{key[0]}/{key[1]}", support_of(ctx, key[0]) or "-", node_tag(ctx, p.get("spec", {}).get("nodeName")) if p.get("spec", {}).get("nodeName") else "-",
                         waiting or st.get("phase", "?"), age(created, ctx.now), len(ev), msg])
            ctx.ns_issue(key[0], "pod stuck creating (network sandbox)")
            seen.add(key)
            old += 1 if age_s >= 300 or ev else 0
    n_ev = sum(len(v) for v in events.values())
    if old:
        status = NET_BAD
    elif rows:
        status = NET_WARN
    else:
        status = NET_OK
    ev_text = (f"{len(rows)} pod(s) stuck or with sandbox errors, {n_ev} sandbox/network warning event(s) in the last {ctx.minutes} min" if rows
               else f"no pod has been in ContainerCreating for more than 2 minutes and no sandbox warning events in the last {ctx.minutes} min")
    mean = {NET_BAD: "Pods cannot get their network sandbox: usually the plugin has no free IP address, the node's network agent is down, or the node is out of resources. "
                     "Read the event message below, then check the plugin health and the address exhaustion checks.",
            NET_WARN: "Some pods are slow to start; they may still be pulling images. Re-run in a few minutes; if they stay, read the event message.",
            NET_OK: "Pod sandboxes are being created normally."}[status]
    _netcheck(rep, ctx, "pods_stuck", "Pods stuck in ContainerCreating or failing to create a pod sandbox", status,
              "pods waiting in ContainerCreating and 'FailedCreatePodSandBox' / network-not-ready events, which show the network plugin failing to set a pod up.",
              ev_text + ".", mean, finding=(f"{len(rows)} pod(s) stuck creating their network sandbox (e.g. {rows[0][0]})" if rows else None))
    if rows:
        rep.glossary(["Pod sandbox", "ContainerCreating"])
        rep.table(["Namespace / pod", "SUPPORT DL", "Node", "Waiting reason", "Age", "Sandbox events", "Latest event message"], rows, maxw=70,
                  what="pods that are stuck creating their network sandbox, where they were placed and the latest event message explaining why.")


def _net_startup_order(rep, ctx):
    pods = ctx.data.get("pods")
    if pods is None:
        _netcheck(rep, ctx, "cni_order", "Container Network Interface start-up order", NET_NA,
                  "whether the network agents restarted or are not ready on some nodes.", "the pod list could not be read.", "Grant read access to pods and run again.")
        return
    comps = ("azure-cns", "azure-ip-masq-agent", "azure-npm", "cilium", "calico-node", "kube-proxy")
    rows, statuses, notes = [], [], []
    for p in items(pods):
        meta = p["metadata"]
        if meta.get("namespace") != "kube-system":
            continue
        name = next((c for c in comps if meta["name"].startswith(c)), None)
        if not name:
            continue
        ready, rst = _pod_ready(p), _pod_restarts(p)
        last = next((((cs.get("lastState") or {}).get("terminated") or {}).get("reason") for cs in p.get("status", {}).get("containerStatuses", []) or []
                     if (cs.get("lastState") or {}).get("terminated")), None)
        if not ready or rst >= 3:
            rows.append([meta["name"], node_tag(ctx, p.get("spec", {}).get("nodeName")) if p.get("spec", {}).get("nodeName") else "-", "yes" if ready else "NOT READY", rst, last or "-"])
            statuses.append(NET_BAD if not ready else NET_WARN)
            notes.append(f"{meta['name']}: {'not ready' if not ready else 'restarted'} ({rst} restarts)")
    # nodes whose network agent is not ready but that run application pods
    not_ready_nodes = {p.get("spec", {}).get("nodeName") for p in items(pods) if p["metadata"].get("namespace") == "kube-system"
                       and any(p["metadata"]["name"].startswith(c) for c in ("azure-cns", "cilium", "calico-node")) and not _pod_ready(p)}
    n_on = sum(1 for p in items(pods) if p.get("spec", {}).get("nodeName") in not_ready_nodes and p["metadata"].get("namespace") != "kube-system")
    if not statuses:
        status = NET_OK
        ev = "every network agent pod in kube-system is ready and has restarted fewer than 3 times."
    else:
        status = _combine(statuses)
        ev = "; ".join(notes[:5]) + (f"; {n_on} application pod(s) run on nodes whose network agent is not ready" if n_on else "") + "."
    mean = {NET_BAD: "A network agent (or kube-proxy) is not ready on a node: pods started there before it was ready can get no network or no Service routing. "
                     "Check the pod's last state and events (kubectl -n kube-system describe pod), then restart affected application pods once the agent is ready.",
            NET_WARN: "A network agent restarted several times - often a crash loop caused by memory limits, configuration or API throttling; read its previous logs.",
            NET_OK: "No sign of start-up ordering problems."}[status]
    _netcheck(rep, ctx, "cni_order", "Container Network Interface start-up order", status,
              "restarts and readiness of the network agents and kube-proxy; pods started before them are ready can fail to get network.", ev, mean,
              finding=("Network agent pod(s) not ready or restarting: " + "; ".join(notes[:3])) if status != NET_OK else None)
    if rows:
        rep.table(["Pod", "Node", "Ready", "Container restarts", "Last stop reason"], rows, maxw=60,
                  what="network agent pods (and kube-proxy) that are not ready or have restarted, with the reason their last container stopped.")


# ---------------------------------------------------------------------------
# 2. Node networking
# ---------------------------------------------------------------------------

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


def _net_node_conditions(rep, ctx):
    nodes = ctx.data.get("nodes")
    if nodes is None:
        _netcheck(rep, ctx, "node_cond", "Node status and pressure conditions", NET_NA, "whether nodes are Ready and free of memory, disk, process-count and network problems.",
                  "the node list could not be read.", "Grant read access to nodes and run again.")
        return
    rows, notready, pressure, netdown = [], [], [], []
    for n in items(nodes):
        conds = {c.get("type"): c.get("status") for c in n.get("status", {}).get("conditions", []) or []}
        name = n["metadata"]["name"]

        def flag(t):
            v = conds.get(t)
            return "-" if v is None else ("PRESSURE" if v == "True" else "OK")
        ready = conds.get("Ready")
        rows.append([node_tag(ctx, name), "Ready" if ready == "True" else "NotReady", flag("MemoryPressure"), flag("DiskPressure"), flag("PIDPressure"),
                     ("NO NETWORK" if conds.get("NetworkUnavailable") == "True" else ("OK" if "NetworkUnavailable" in conds else "-"))])
        if ready != "True":
            notready.append(node_tag(ctx, name))
        if any(conds.get(t) == "True" for t in ("MemoryPressure", "DiskPressure", "PIDPressure")):
            pressure.append(node_tag(ctx, name))
        if conds.get("NetworkUnavailable") == "True":
            netdown.append(node_tag(ctx, name))
    if notready or netdown:
        status = NET_BAD
    elif pressure:
        status = NET_WARN
    else:
        status = NET_OK
    ev = f"{len(rows)} node(s): " + (f"NotReady: {', '.join(notready[:5])}; " if notready else "") + (f"network unavailable: {', '.join(netdown[:5])}; " if netdown else "") \
        + (f"under pressure: {', '.join(pressure[:5])}; " if pressure else "") + ("all Ready with no pressure conditions" if status == NET_OK else "")
    mean = {NET_BAD: "A node that is NotReady (or reports its network unavailable) cannot serve pod traffic; pods there lose connectivity. Check the node's events and the virtual machine health, "
                     "then cordon / drain / replace it if it does not recover.",
            NET_WARN: "A node is short of memory, disk space or process slots; this slows or drops connections. Free resources, add nodes or move pods away.",
            NET_OK: "Node conditions give no sign of network trouble."}[status]
    _netcheck(rep, ctx, "node_cond", "Node status and pressure conditions", status,
              "whether each node is Ready and whether it reports memory, disk, process-count pressure or an unavailable network.", ev.rstrip("; ") + ".", mean,
              finding=None)
    rep.glossary(["Node pressure"])
    rep.table(["Node", "Ready", "Memory pressure", "Disk pressure", "Process-count pressure", "Network unavailable"], rows, maxw=50,
              what="the standard Kubernetes condition flags of every node; PRESSURE or NO NETWORK marks a node with a problem.")


def _net_node_nics(rep, ctx):
    stats = ctx.data.get("node_stats") or {}
    if not stats:
        _netcheck(rep, ctx, "node_nic", "Node network interface errors and dropped packets", NET_NA,
                  "error counters of each node's network interface, read from the kubelet statistics.",
                  "the kubelet statistics are not readable (they need the 'nodes/proxy' permission).",
                  "Grant 'get' on nodes/proxy to read them, or look at the virtual machine network metrics in Azure Monitor. Dropped packets are not exposed by the kubelet at all.")
        return
    rows, bad = [], []
    for node, s in sorted(stats.items()):
        t = _net_totals((s.get("node") or {}).get("network"))
        if not t:
            continue
        rows.append([node_tag(ctx, node), _fmt_bytes(t[0]), _fmt_bytes(t[1]), t[2], t[3]])
        if t[2] + t[3] > 0:
            bad.append(f"{node_tag(ctx, node)} (receive {t[2]}, transmit {t[3]})")
    if not rows:
        status, ev = NET_NA, "the kubelet returned no network counters."
        mean = "Use Azure Monitor virtual machine network metrics instead."
    elif bad:
        status, ev = NET_WARN, f"{len(bad)} node(s) report interface errors since boot: " + ", ".join(bad[:5]) + "."
        mean = "Interface errors point to a faulty or overloaded network interface, driver or accelerated-networking problem; check whether the count grows between runs and move workloads off the node if it does."
        ctx.find("MED", f"{len(bad)} node(s) have network interface errors since boot: " + ", ".join(bad[:3]))
    else:
        status, ev = NET_OK, f"{len(rows)} node(s) read: 0 receive and 0 transmit errors since boot. Dropped packets are not exposed by the kubelet."
        mean = "No interface errors. Packet drops (for example from a full connection table) need the connection-tracking check below or node access."
    _netcheck(rep, ctx, "node_nic", "Node network interface errors and dropped packets", status,
              "receive and transmit error counters of each node's network interface since the node booted (kubelet statistics, read-only).", ev, mean)
    rep.table(["Node", "Bytes received (total)", "Bytes transmitted (total)", "Receive errors", "Transmit errors"], rows, maxw=50,
              what="network interface totals per node since boot, with error counters; any non-zero error count is worth watching.")


def _find_keys(obj, pattern, depth=0):
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if re.search(pattern, str(k), re.I) and not isinstance(v, (dict, list)):
                out.append((str(k), v))
            if depth < 4:
                out += _find_keys(v, pattern, depth + 1)
    elif isinstance(obj, list) and depth < 4:
        for v in obj[:50]:
            out += _find_keys(v, pattern, depth + 1)
    return out


def _net_mtu(rep, ctx):
    ok, cluster, _t = _az_state(ctx)
    nw = (cluster or {}).get("networkProfile") or {}
    found = []
    for name in ("cilium-config", "azure-cns-config", "azure-ip-masq-agent-config-reconciled", "calico-config"):
        data = _cm(ctx, name)
        for k, v in (data or {}).items():
            if re.search(r"mtu", k, re.I):
                found.append((f"{name}: {k}", str(v).strip()))
            else:
                try:
                    for fk, fv in _find_keys(json.loads(v), r"mtu"):
                        found.append((f"{name}: {fk}", str(fv)))
                except (ValueError, TypeError):
                    if re.search(r"\bmtu\b\W+\d+", v or "", re.I):
                        found.append((f"{name}: {k}", _short(v, 40)))
    for n in items(ctx.data.get("nodes"))[:50]:
        for src in (n["metadata"].get("labels") or {}, n["metadata"].get("annotations") or {}):
            for k, v in src.items():
                if re.search(r"mtu", k, re.I):
                    found.append((f"node {n['metadata']['name']}: {k}", str(v)))
    hints = []
    cil = _cm(ctx, "cilium-config") or {}
    if str(cil.get("tunnel", cil.get("routing-mode", ""))).lower() in ("vxlan", "geneve", "tunnel"):
        hints.append("Cilium runs in tunnel mode, which adds about 50 bytes of overhead per packet (usable pod MTU about 1450).")
    if (nw.get("networkPluginMode") or "").lower() == "overlay":
        hints.append("Azure CNI Overlay forwards pod traffic natively inside the virtual network, so the limit is normally the 1500-byte interface MTU.")
    if found:
        vals = []
        for _k, v in found:
            m = re.search(r"\d{3,5}", v)
            if m:
                vals.append(int(m.group(0)))
        low = [v for v in vals if v < 1400]
        status = NET_WARN if low else NET_OK
        ev = "; ".join(f"{k} = {v}" for k, v in found[:4]) + (". " + " ".join(hints) if hints else ".")
        mean = ("An MTU below 1400 bytes is unusually small; it usually means a tunnel or VPN overhead - make sure every hop agrees, or large packets will be dropped." if low else
                "The configured MTU looks normal. If big transfers hang while small requests work, compare it with the path MTU between the failing endpoints.")
    else:
        status = NET_NA
        ev = "cannot be read without node access (the tool never uses SSH). " + (" ".join(hints) if hints else "No MTU setting appears in the plugin ConfigMaps, node labels or annotations.")
        mean = "If large requests hang or time out but small ones work, suspect a Maximum Transmission Unit mismatch: check the node interface MTU with `ip link` through your normal access path, and the Cilium / VPN settings."
    _netcheck(rep, ctx, "mtu", "Maximum Transmission Unit hints", status,
              "the largest packet size the pod network is configured for, from the plugin settings, node labels and annotations (no node access).", ev, mean)


def _net_throttling(rep, ctx):
    az = _net_az_collect(ctx)
    sources, hits, evidence = [], 0, []
    cni = ctx.data.get("net_throttle_cni")
    if cni is not None:
        sources.append("network plugin logs")
        if cni:
            hits += cni
            evidence.append(f"{cni} throttling line(s) in the network plugin logs")
    ev_hits = [e for e in items(ctx.data.get("events")) if re.search(r"\b429\b|TooManyRequests|throttl|RequestRateLimit|rate limit exceeded", f"{e.get('reason', '')} {e.get('message') or e.get('note') or ''}", re.I)
               and (_event_time(e) or ctx.since) >= ctx.since]
    if ctx.data.get("events") is not None:
        sources.append("cluster events")
        if ev_hits:
            hits += len(ev_hits)
            evidence.append(f"{len(ev_hits)} event(s) mention throttling (e.g. {_short(ev_hits[0].get('message') or ev_hits[0].get('note'), 90)})")
    if az.get("activity") is not None:
        sources.append("Azure Activity log")
        act = [a for a in az["activity"] if re.search(r"TooManyRequests|429|throttl", json.dumps({"s": a.get("status"), "ss": a.get("subStatus"), "d": a.get("description")}, default=str), re.I)]
        if act:
            hits += len(act)
            evidence.append(f"{len(act)} Azure Activity log entries show TooManyRequests / throttling (e.g. {_short(act[0].get('operationName', {}).get('localizedValue') if isinstance(act[0].get('operationName'), dict) else act[0].get('operationName'), 70)})")
    missing = []
    if az.get("activity") is None:
        missing.append("Azure Activity log")
    if cni is None:
        missing.append("network plugin logs")
    if not sources:
        status, ev = NET_NA, "none of the sources could be read (" + ", ".join(missing) + ")."
        mean = "Read access to kube-system logs and to the Azure Activity log of the node resource group is needed to see throttling."
    elif hits:
        status = NET_BAD if hits >= 10 else NET_WARN
        ev = "; ".join(evidence) + "."
        mean = ("The cloud is refusing calls because there are too many in a short time (HTTP 429). This slows IP allocation, load balancer updates and scale-outs. "
                "Spread scale-out operations, reduce cluster autoscaler / controller frequency, and check for other tools calling the same subscription's API.")
    else:
        status = NET_OK
        ev = "no throttling found in: " + ", ".join(sources) + (f"; not readable: {', '.join(missing)}" if missing else "") + "."
        mean = "No sign of cloud API throttling in the sources read." + (" The sources that could not be read may still hold evidence." if missing else "")
        if missing:
            status = NET_NA if len(sources) < 2 else NET_OK
    _netcheck(rep, ctx, "throttle", "Cloud service throttling evidence (too many requests)", status,
              "429 / TooManyRequests / rate-limit text in the plugin logs, cluster events and the Azure Activity log of the node resource group.", ev, mean,
              finding=("Cloud API throttling (HTTP 429) evidence: " + "; ".join(evidence[:2])) if hits else None)


# ---------------------------------------------------------------------------
# 3. kube-proxy and service routing
# ---------------------------------------------------------------------------

def _net_kube_proxy(rep, ctx):
    ds = _ds(ctx, "kube-proxy")
    dss = ctx.data.get("daemonsets")
    mode = ctx.data.get("net_kp_mode")
    jobs = {"kube-proxy": {"selector": "component=kube-proxy"}} if ds else {}
    _prefetch_logs(ctx, jobs)
    rows, errs = [], None
    if ds:
        ready, want = _ds_counts(ds)
        rows.append(["DaemonSet kube-proxy", f"{ready} of {want} nodes ready", _ds_version(ds)])
        lines, err = ctx.data["net_logs"].get("kube-proxy", (None, "not read"))
        if lines is None and err and "no resources" not in err:
            lines2, err2 = _logs(ctx, "k8s-app=kube-proxy")
            lines, err = lines2, err2
            ctx.data["net_logs"]["kube-proxy"] = (lines, err)
        if lines is not None:
            errs = [l for l in lines if re.search(r"\bE\d{4} |error|fail|unable|cannot", l, re.I)]
            rows.append(["Log lines read / error-like", f"{len(lines)} / {len(errs)}", f"last {ctx.minutes} min"])
            if errs:
                rows.append(["Latest error", _short(errs[-1], 120), ""])
    rows.append(["Mode", mode or "unknown", "iptables or IPVS"])
    cil = _ds(ctx, "cilium")
    if dss is None:
        status, ev = NET_NA, "the DaemonSets could not be read."
        mean = "Grant read access to DaemonSets in kube-system."
    elif not ds:
        if cil:
            status, ev = NET_OK, "no kube-proxy DaemonSet; the Cilium agent is present and normally replaces it (eBPF Service routing)."
            mean = "Service routing is handled by Cilium; use the Cilium status for Service problems."
        else:
            status, ev = NET_WARN, "no kube-proxy DaemonSet was found and no Cilium agent either."
            mean = "Without kube-proxy (or Cilium) Service addresses cannot be reached; check kube-system for the component that provides Service routing."
    else:
        ready, want = _ds_counts(ds)
        parts = []
        if want and ready < want:
            parts.append(NET_BAD)
        if errs:
            parts.append(NET_WARN)
        if errs is None:
            parts.append(NET_NA)
        status = _combine(parts) if parts else NET_OK
        ev = f"{ready} of {want} kube-proxy pods ready; mode {mode or 'unknown'}; " + (f"{len(errs)} error-like log line(s) in {ctx.minutes} min" if errs is not None else "logs not readable") + "."
        mean = {NET_BAD: "kube-proxy is not ready on some nodes, so Services do not work for pods there. Inspect the pod (describe, logs) and the node, then restart it.",
                NET_WARN: "kube-proxy logs show errors (often iptables lock contention or API access trouble); read the latest error below.",
                NET_NA: "kube-proxy runs, but its logs could not be read.",
                NET_OK: "kube-proxy is healthy on every node; Service routing is working at the node level."}[status]
        if status == NET_BAD:
            ctx.find("HIGH", f"kube-proxy is not ready on every node ({ready}/{want})")
    rep.heading("kube-proxy and Service routing",
                "how Service addresses are turned into pod addresses on every node: kube-proxy health and mode, Services without ready pods, and the Service types in use.")
    rep.glossary(["kube-proxy", "iptables", "IPVS", "DaemonSet", "Cilium"])
    _netcheck(rep, ctx, "kube_proxy", "kube-proxy health and mode", status,
              "whether kube-proxy runs on every node, whether it uses iptables or IPVS, and whether its recent logs contain errors.", ev, mean)
    rep.table(["Item", "Value", "Detail"], rows, maxw=90, what="kube-proxy readiness, mode and recent log errors in one place.")


def _net_services(rep, ctx):
    services = ctx.data.get("services")
    endpoints_data = ctx.data.get("endpoints")
    rep.glossary(["Endpoint", "ClusterIP", "NodePort", "LoadBalancer"])
    if services is None:
        _netcheck(rep, ctx, "svc_noep", "Services with no ready endpoints", NET_NA, "Services whose selector matches no ready pod.", "the Service list could not be read.", "Grant read access to Services and Endpoints.")
        return
    endpoints = {(e["metadata"]["namespace"], e["metadata"]["name"]): e for e in items(endpoints_data)}
    noep, pend = [], []
    cluster_ip, node_port, lbs = [], [], []
    for s in items(services):
        meta, spec = s["metadata"], s.get("spec", {})
        key = (meta["namespace"], meta["name"])
        stype = spec.get("type", "ClusterIP")
        ports = ", ".join(f"{p.get('port')}" + (f":{p['nodePort']}" if p.get("nodePort") else "") + "/" + p.get("protocol", "TCP") for p in spec.get("ports", [])[:4])
        ready_n = None
        if key in endpoints:
            ready_n = sum(len(sub.get("addresses") or []) for sub in endpoints[key].get("subsets") or [])
        if spec.get("selector") and endpoints_data is not None and key[1] != "kubernetes":
            if ready_n == 0 or (key not in endpoints):
                noep.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], stype, ", ".join(f"{k}={v}" for k, v in list(spec["selector"].items())[:3])])
        if stype in ("ClusterIP",):
            cluster_ip.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], spec.get("clusterIP", "-"), ports,
                               "-" if ready_n is None else ready_n])
        if stype == "NodePort":
            node_port.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], ports, spec.get("externalTrafficPolicy") or "Cluster", spec])
        if stype == "LoadBalancer":
            lb = (s.get("status", {}).get("loadBalancer") or {}).get("ingress") or []
            ann = meta.get("annotations") or {}
            internal = ann.get("service.beta.kubernetes.io/azure-load-balancer-internal", "").lower() == "true"
            addr = (lb[0].get("hostname") or lb[0].get("ip")) if lb else None
            lbs.append({"ns": meta["namespace"], "name": meta["name"], "internal": internal, "addr": addr, "ports": ports, "spec": spec})
            if not addr:
                pend.append(f"{meta['namespace']}/{meta['name']}")
    by_type = Counter(s.get("spec", {}).get("type", "ClusterIP") for s in items(services))
    ctx.data["net_services"] = {"noep": noep, "nodeport": node_port, "lbs": lbs, "pending": pend}
    # ---- Services with no ready endpoints
    if endpoints_data is None:
        st, ev = NET_NA, "the Endpoints could not be read."
        mean = "Grant read access to Endpoints (or EndpointSlices)."
    elif noep:
        st = NET_BAD if any(r[0] == "kube-system" and r[2] in ("kube-dns", "metrics-server") for r in noep) else NET_WARN
        ev = f"{len(noep)} Service(s) have a selector but no ready pod behind them (e.g. {noep[0][0]}/{noep[0][2]})."
        mean = ("Requests to these Services fail (connection refused / timeout). Usually the pods are not Ready (failing readiness probe, crash loop), the Service selector does not match the pod labels, "
                "or the workload is scaled to zero. Compare `kubectl get pods --show-labels` with the Service selector.")
    else:
        st, ev = NET_OK, f"all {sum(1 for s in items(services) if s.get('spec', {}).get('selector'))} Services with a selector have at least one ready endpoint."
        mean = "Every Service has pods to send traffic to."
    _netcheck(rep, ctx, "svc_noep", "Services with no ready endpoints", st,
              "Services whose label selector currently matches no ready pod, so nothing can answer requests to the Service address.", ev, mean,
              finding=None)
    if noep:
        rep.table(["Namespace", "SUPPORT DL", "Service", "Type", "Selector"], noep, maxw=60,
                  what="Services that have no ready endpoint, with the label selector they use to find pods.")
        for r in noep:
            ctx.ns_issue(r[0], f"service {r[2]}: no ready endpoints")
    # ---- ClusterIP services
    if cluster_ip:
        _netcheck(rep, ctx, "svc_clusterip", "ClusterIP Services", NET_OK,
                  "Services that are reachable only inside the cluster, and how many ready endpoints each has.",
                  f"{by_type.get('ClusterIP', 0)} ClusterIP Service(s) in total; {len([r for r in cluster_ip if r[5] == 0])} listed below without endpoints.",
                  "Internal Services are normal. Use the endpoint count column to find ones without pods; 'Not available' endpoint counts mean the Endpoints object was not readable.")
        rep.table(["Namespace", "SUPPORT DL", "Service", "Cluster IP address", "Ports (service port : node port)", "Ready endpoints"], cluster_ip[:MAX_ROWS], limit=MAX_ROWS, maxw=60,
                  what=f"the first {min(len(cluster_ip), MAX_ROWS)} of {len(cluster_ip)} internal Services with their virtual address, ports and number of ready endpoints.")
    # ---- NodePort services (+ network security group)
    az = _net_az_collect(ctx)
    ports_used = []
    for r in node_port:
        for p in r[5].get("ports", []):
            if p.get("nodePort"):
                ports_used.append((r[0], r[2], p["nodePort"], p.get("protocol", "TCP")))
    nsg_info = _nodeport_vs_nsg(az, [x[2] for x in ports_used] or [])
    if not node_port:
        st, ev = NET_OK, "no NodePort Services exist" + (f" (LoadBalancer Services also use node ports: {sum(1 for l in lbs for p in l['spec'].get('ports', []) if p.get('nodePort'))})" if lbs else "") + "."
        mean = "Nothing is exposed directly on the nodes."
    else:
        ev = f"{len(node_port)} NodePort Service(s) use {len(ports_used)} node port(s) (default range 30000-32767; the range actually configured on the API server cannot be read here). " + nsg_info["text"]
        st = nsg_info["status"]
        mean = {NET_WARN: "A network security group rule denies traffic to a node port in use, so clients outside the virtual network cannot connect; allow the port or use a load balancer.",
                NET_NA: "The network security groups could not be read, so it is unknown whether the node ports are reachable. Check the node subnet's / node resource group's NSG for the port range.",
                NET_OK: "No custom network security group rule blocks the node ports in use. (Azure's default rules allow traffic from inside the virtual network and from the load balancer.)"}.get(st, "")
    _netcheck(rep, ctx, "svc_nodeport", "NodePort Services and the node port range", st,
              "Services opened on a fixed port on every node, and whether a network security group rule could block those ports.", ev, mean)
    if node_port:
        rep.table(["Namespace", "SUPPORT DL", "Service", "Ports (service port : node port)", "External traffic policy"], [r[:5] for r in node_port], maxw=60,
                  what="NodePort Services with the port they open on every node; 'Local' traffic policy only reaches nodes that run a matching pod.")
    # ---- LoadBalancer services and probe status
    az_metrics = {m["lb"]["name"]: m for m in az.get("lb_metrics", [])}
    dips = {}
    for name, m in az_metrics.items():
        vals = [v for _, v in m["got"].get("dip", [])]
        dips[name] = min(vals) if vals else None
    lbrows, probe_vals = [], []
    for l in lbs:
        lbname = next((n for n in dips if (n.endswith("-internal")) == l["internal"]), None)
        dip = dips.get(lbname)
        lbrows.append([l["ns"], support_of(ctx, l["ns"]) or "-", l["name"], "internal" if l["internal"] else "INTERNET-FACING", l["addr"] or "NO ADDRESS YET", l["ports"],
                       (f"{dip:.0f}%" if dip is not None else ("not available" if az.get("ok") else "not available (Azure details off)"))])
        if dip is not None:
            probe_vals.append(dip)
    if not lbs:
        st, ev, mean = NET_OK, "no LoadBalancer Services exist.", "No cloud load balancer is in use for Services."
    else:
        parts = []
        if pend:
            parts.append(NET_WARN)
        if probe_vals:
            parts.append(NET_BAD if min(probe_vals) < 50 else (NET_WARN if min(probe_vals) < 100 else NET_OK))
        else:
            parts.append(NET_NA)
        st = _combine(parts)
        ev = (f"{len(lbs)} LoadBalancer Service(s)" + (f", {len(pend)} without an external address yet ({', '.join(pend[:3])})" if pend else "")
              + (f"; lowest backend health probe availability {min(probe_vals):.0f}%" if probe_vals else "; backend health probe availability not available (Azure Monitor data missing)") + ".")
        mean = {NET_BAD: "Most backends fail the load balancer health probe: traffic is only sent to the few that pass. Check that the pods answer on the node port and that the externalTrafficPolicy and probe path are right.",
                NET_WARN: "Either an address is still pending (check events of the Service and the cloud controller) or some backends failed the health probe in the window.",
                NET_NA: "Probe availability could not be read from Azure Monitor (needs a Standard load balancer and Monitoring Reader), so health is unknown.",
                NET_OK: "Every LoadBalancer Service has an address and the load balancer's health probes stayed at 100%."}[st]
    _netcheck(rep, ctx, "lb_services", "LoadBalancer Services and health-probe status", st,
              "Services that created an Azure load balancer address, whether the address is assigned, and the load balancer's backend health probe availability from Azure Monitor.", ev, mean)
    if lbrows:
        rep.glossary(["LoadBalancer", "Health probe", "LB"])
        rep.table(["Namespace", "SUPPORT DL", "Service", "Exposure", "External address", "Ports (service port : node port)", "Backend health probe availability (lowest)"], lbrows, maxw=60,
                  what="every LoadBalancer Service, whether it is internet-facing, its address and the lowest health probe availability in the window.")
    public = [f"{l['ns']}/{l['name']}" for l in lbs if not l["internal"]]
    if public:
        ctx.find("INFO", f"{len(public)} LoadBalancer Service(s) are internet-facing (no internal annotation): " + ", ".join(public[:6]) + (" ..." if len(public) > 6 else "")
                 + support_suffix(ctx, {x.split("/")[0] for x in public}))
    for ns_name in pend:
        ctx.find("MED", f"LoadBalancer Service {ns_name} has no external address yet")


def _port_covers(rule, port):
    ranges = []
    if rule.get("destinationPortRange"):
        ranges.append(str(rule["destinationPortRange"]))
    ranges += [str(x) for x in (rule.get("destinationPortRanges") or [])]
    for r in ranges:
        if r == "*":
            return True
        m = re.fullmatch(r"(\d+)-(\d+)", r)
        if m and int(m.group(1)) <= port <= int(m.group(2)):
            return True
        if r.isdigit() and int(r) == port:
            return True
    return False


def _nodeport_vs_nsg(az, ports):
    """Do the custom NSG rules (inbound Deny) cover the node ports in use? {'status', 'text'}"""
    if not ports:
        return {"status": NET_OK, "text": ""}
    if az.get("nsgs") is None:
        return {"status": NET_NA, "text": "Network security groups could not be read."}
    blocked = []
    for nsg in az["nsgs"]:
        for r in nsg["rules"]:
            if r.get("direction") == "Inbound" and r.get("access") == "Deny" and (r.get("priority") or 65000) < 65000:
                hit = [p for p in ports if _port_covers(r, p)]
                if hit:
                    blocked.append(f"rule '{r.get('name')}' (priority {r.get('priority')}) in {nsg['name']} denies port(s) {','.join(str(h) for h in hit[:3])}")
    if blocked:
        return {"status": NET_WARN, "text": "; ".join(blocked[:3]) + "."}
    return {"status": NET_OK, "text": f"Read {len(az['nsgs'])} network security group(s): no custom Deny rule covers these ports."}


# ---------------------------------------------------------------------------
# 4. Domain Name System
# ---------------------------------------------------------------------------

def _net_dns(rep, ctx):
    rep.heading("Domain Name System: name lookups inside the cluster",
                "CoreDNS pods, its configuration, error logs, NodeLocal DNS cache and how pods are set up to resolve names.")
    rep.glossary(["DNS", "CoreDNS", "Corefile", "forward", "NodeLocal DNSCache", "ndots", "search domains"])
    pods = _pods_where(ctx, "kube-system", {"k8s-app": "kube-dns"}) if ctx.data.get("pods") is not None else None
    deploy = next((d for d in items(ctx.data.get("deployments")) if d["metadata"]["namespace"] == "kube-system" and d["metadata"]["name"] == "coredns"), None)
    # --- pods
    if pods is None:
        _netcheck(rep, ctx, "dns_pods", "CoreDNS pods ready and restarts", NET_NA, "whether the CoreDNS pods are ready and stable.", "the pod list could not be read.", "Grant read access to pods.")
    else:
        rows = [[p["metadata"]["name"], node_tag(ctx, p.get("spec", {}).get("nodeName")) if p.get("spec", {}).get("nodeName") else "-", "yes" if _pod_ready(p) else "NOT READY",
                 _pod_restarts(p), age(parse_ts(p["metadata"].get("creationTimestamp")), ctx.now)] for p in pods]
        ready = sum(1 for p in pods if _pod_ready(p))
        want = (deploy or {}).get("spec", {}).get("replicas") or len(pods)
        rst = sum(_pod_restarts(p) for p in pods)
        if not pods:
            st, ev, mean = NET_BAD, "no CoreDNS pod (label k8s-app=kube-dns) was found.", "Without CoreDNS no name lookup works inside the cluster: check the coredns Deployment in kube-system."
        elif ready == 0:
            st, ev, mean = NET_BAD, f"0 of {len(pods)} CoreDNS pods are ready.", "No pod can resolve names: describe the CoreDNS pods and check events, resource limits and the node they run on."
        elif ready < want or rst >= 5:
            st = NET_WARN
            ev = f"{ready} of {want} CoreDNS pods ready; {rst} container restarts in total."
            mean = "DNS capacity is reduced or unstable; restarts usually mean out-of-memory or crashes - check the pod's last state and consider more replicas or memory."
        else:
            st, ev, mean = NET_OK, f"{ready} of {want} CoreDNS pods ready; {rst} container restarts in total.", "CoreDNS is running normally."
        _netcheck(rep, ctx, "dns_pods", "CoreDNS pods ready and restarts", st, "whether the CoreDNS pods are Ready and how often they restarted.", ev, mean,
                  finding="CoreDNS is unhealthy: " + ev if st == NET_BAD else None)
        if rows:
            rep.table(["Pod", "Node", "Ready", "Container restarts", "Age"], rows, maxw=50, what="every CoreDNS pod with its node, readiness and restart count.")
    # --- Corefile
    cm = _cm(ctx, "coredns")
    corefile = (cm or {}).get("Corefile", "")
    custom = _cm(ctx, "coredns-custom")
    if cm is None:
        _netcheck(rep, ctx, "dns_conf", "CoreDNS configuration (Corefile) summary", NET_NA, "the CoreDNS configuration: forwarders, cache, loop and ready plugins and custom settings.",
                  "the coredns ConfigMap could not be read.", "Grant read access to ConfigMaps in kube-system.")
    else:
        plugins = set(re.findall(r"^\s*([a-z0-9_]+)\b", re.sub(r"\{[^{}]*\}", "", corefile), re.M))
        forwards = re.findall(r"forward\s+\.\s+(.+)", corefile)
        cache = re.search(r"^\s*cache\s*(\d+)?", corefile, re.M)
        rows = [["Upstream forwarders", ", ".join(f.strip() for f in forwards) or "none", "Where lookups for outside names are sent (/etc/resolv.conf = the node's resolver, normally the Azure DNS)."],
                ["Cache", ("yes" + (f", {cache.group(1)} seconds" if cache and cache.group(1) else "")) if cache else "no", "Answers are kept for a while so repeated lookups are fast."],
                ["Loop detection plugin", "present" if "loop" in plugins else "MISSING", "Stops CoreDNS when it forwards to itself in a circle."],
                ["Ready plugin", "present" if "ready" in plugins else "MISSING", "Lets Kubernetes know when CoreDNS can serve."],
                ["Health plugin", "present" if "health" in plugins else "not set", "Lets Kubernetes restart CoreDNS if it hangs."],
                ["Error logging", "present" if "errors" in plugins else "not set", "Writes lookup failures to the pod log."],
                ["Custom settings (coredns-custom)", (", ".join(sorted(custom))[:80] if custom else ("none" if custom is not None else "not found")), "Extra rules added by the cluster operators (stub domains, overrides)."],
                ["Plugins in the Corefile", ", ".join(sorted(plugins))[:100] or "-", "Everything that is switched on."]]
        missing = [n for n, p in (("loop", "loop"), ("ready", "ready")) if p not in plugins]
        if not corefile:
            st, ev, mean = NET_NA, "the coredns ConfigMap has no Corefile key.", "Check how DNS is configured (custom DNS add-on?)."
        elif missing:
            st = NET_WARN
            ev = f"the Corefile lacks the {', '.join(missing)} plugin(s); forwarders: {', '.join(f.strip() for f in forwards) or 'none'}."
            mean = "Add the missing plugins (put custom changes in the coredns-custom ConfigMap, not in coredns, which AKS overwrites)."
        else:
            st = NET_OK
            ev = f"forwarders {', '.join(f.strip() for f in forwards) or 'none'}; cache {'on' if cache else 'off'}; loop and ready plugins present; custom ConfigMap {'present' if custom else 'absent'}."
            mean = "The CoreDNS configuration looks standard."
        _netcheck(rep, ctx, "dns_conf", "CoreDNS configuration (Corefile) summary", st,
                  "the CoreDNS configuration: forwarders, cache, loop and ready plugins and custom settings.", ev, mean)
        rep.table(["Setting", "Value", "What it means"], rows, maxw=90, what="the important parts of the CoreDNS configuration file and what each one does.")
    # --- logs
    lines, err = ctx.data.get("net_logs", {}).get("coredns", (None, None))
    if "coredns" not in ctx.data.get("net_logs", {}):
        _prefetch_logs(ctx, {"coredns": {"selector": "k8s-app=kube-dns"}})
        lines, err = ctx.data["net_logs"]["coredns"]
    if lines is None:
        _netcheck(rep, ctx, "dns_logs", "Domain Name System error logs", NET_NA, f"error lines in the CoreDNS logs of the last {ctx.minutes} minutes.", f"the CoreDNS logs could not be read ({err}).",
                  "Grant 'get' on pods/log in kube-system.")
    else:
        pat = {"SERVFAIL": r"SERVFAIL", "REFUSED": r"REFUSED", "timeouts": r"i/o timeout|timed out", "NXDOMAIN": r"NXDOMAIN", "errors": r"\[ERROR\]|plugin/errors"}
        counts = {k: sum(1 for l in lines if re.search(p, l)) for k, p in pat.items()}
        sample = next((l for l in reversed(lines) if re.search(r"error|fail|timeout|SERVFAIL|REFUSED", l, re.I)), "")
        bad = counts["timeouts"] + counts["SERVFAIL"]
        if bad >= 10:
            st = NET_BAD
            ctx.find("MED", f"CoreDNS: {counts['timeouts']} timeouts and {counts['SERVFAIL']} SERVFAIL in the window - DNS problems likely")
        elif bad or counts["errors"]:
            st = NET_WARN
        else:
            st = NET_OK
        ev = f"{len(lines)} log lines read; " + (", ".join(f"{k} {v}" for k, v in counts.items() if v) or "no errors") + "."
        mean = {NET_BAD: "Many lookups time out or fail: the upstream DNS server may be slow or unreachable, or CoreDNS is overloaded. Check the forwarders from a pod, add replicas / NodeLocal DNS cache.",
                NET_WARN: "Some lookup errors. Often normal (names that do not exist, NXDOMAIN) but repeated timeouts mean upstream trouble; read the latest error below.",
                NET_OK: "No DNS errors in the window."}[st]
        _netcheck(rep, ctx, "dns_logs", "Domain Name System error logs", st, f"error lines in the CoreDNS logs of the last {ctx.minutes} minutes (SERVFAIL, REFUSED, timeouts, NXDOMAIN).", ev, mean)
        rep.table(["Component", "Log lines read", "Error-like lines", "Breakdown", "Latest error"],
                  [["CoreDNS", len(lines), counts["errors"], ", ".join(f"{k} {v}" for k, v in counts.items() if v and k != "errors") or "-", _short(sample, 110)]], maxw=70,
                  what="how many CoreDNS log lines were read and how many report errors, with the most recent error.")
    # --- NodeLocal DNSCache
    nl = _ds(ctx, "node-local-dns")
    nodes_n = len(items(ctx.data.get("nodes")))
    dns_bad = ctx.data.get("net_checks", {}).get("dns_logs", {}).get("status") in (NET_BAD, NET_WARN)
    if ctx.data.get("daemonsets") is None:
        st, ev, mean = NET_NA, "the DaemonSets could not be read.", "Grant read access to DaemonSets in kube-system."
    elif nl:
        r, w = _ds_counts(nl)
        st = NET_OK if r >= w else NET_WARN
        ev = f"NodeLocal DNS cache is installed ({r} of {w} nodes ready)."
        mean = "Lookups are cached on every node." if st == NET_OK else "The cache is not ready on every node; pods on those nodes fall back to CoreDNS."
    elif nodes_n >= 50 or dns_bad:
        st = NET_WARN
        ev = f"NodeLocal DNS cache is not installed; the cluster has {nodes_n} node(s)" + (" and DNS errors were seen." if dns_bad else ".")
        mean = ("Recommended for clusters of this size or with DNS timeouts: it removes most conntrack and UDP load from CoreDNS. "
                "See the AKS documentation 'Configure NodeLocal DNS cache' (it is a DaemonSet you add; nothing is changed by this report).")
    else:
        st = NET_OK
        ev = f"NodeLocal DNS cache is not installed; fine for a cluster of {nodes_n} node(s) without DNS errors."
        mean = "Consider it if the cluster grows beyond about 50 nodes or DNS timeouts appear."
    _netcheck(rep, ctx, "dns_nodelocal", "NodeLocal Domain Name System cache", st, "whether a per-node DNS cache is installed, which reduces DNS delays in large clusters.", ev, mean)
    # --- ndots / search from pod specs (read from the pod objects, nothing is executed in the pods)
    pods_all = items(ctx.data.get("pods"))
    if ctx.data.get("pods") is None:
        _netcheck(rep, ctx, "dns_ndots", "Pod ndots and search-domain settings", NET_NA, "how pods are configured to resolve short names.", "the pod list could not be read.", "Grant read access to pods.")
        return
    cnt, policies = Counter(), Counter()
    sample_n = 0
    for p in pods_all[:3000]:
        spec = p.get("spec", {})
        pol = spec.get("dnsPolicy") or "ClusterFirst"
        policies[pol] += 1
        opts = {o.get("name"): o.get("value") for o in (spec.get("dnsConfig") or {}).get("options", []) or []}
        if pol == "Default" or spec.get("hostNetwork") and pol != "ClusterFirstWithHostNet":
            cnt["node resolver (no cluster search domains)"] += 1
        elif "ndots" in opts:
            cnt[f"ndots:{opts['ndots']} (set in the pod spec)"] += 1
        elif pol == "None":
            cnt["custom (dnsPolicy None)"] += 1
        else:
            cnt["ndots:5 (Kubernetes default, applied by the kubelet)"] += 1
        sample_n += 1
    rows = [[k, v, f"{100 * v // max(1, sample_n)}%"] for k, v in cnt.most_common()]
    d5 = cnt.get("ndots:5 (Kubernetes default, applied by the kubelet)", 0)
    st = NET_OK
    ev = f"{sample_n} pod(s) examined from their specifications (nothing is run inside pods): " + ", ".join(f"{v} with {k}" for k, v in cnt.most_common(3)) + "."
    mean = ("Info: with ndots:5 every lookup of an external name such as api.example.com first tries up to 5 search-domain variants, "
            "which multiplies DNS traffic. Setting dnsConfig ndots: 2 on chatty workloads, or using fully qualified names ending in a dot, reduces it.") if d5 else \
        "Pods do not use the default ndots:5, or use the node resolver."
    if d5:
        ctx.find("INFO", f"{d5} of {sample_n} pods use the default DNS setting ndots:5 (each external name lookup tries several search domains first); consider ndots:2 for chatty workloads")
    _netcheck(rep, ctx, "dns_ndots", "Pod ndots and search-domain settings", st,
              "how pods are configured to resolve names (dnsPolicy and the number-of-dots rule), read from the pod specifications only.", ev, mean)
    rep.table(["Effective DNS setting", "Pods", "Share"], rows, maxw=70,
              what="how many pods use each effective name-resolution setting; the default ndots:5 is flagged as information because it multiplies lookups.")


# ---------------------------------------------------------------------------
# 5. Network policies and cloud firewalls
# ---------------------------------------------------------------------------

def _net_policies(rep, ctx):
    ok, cluster, _t = _az_state(ctx)
    nw = (cluster or {}).get("networkProfile") or {}
    rep.heading("Network policies and cloud firewalls",
                "which pod-to-pod rules exist per namespace and who enforces them, and which cloud firewall rules and routes sit between the nodes and the outside.")
    rep.glossary(["NetworkPolicy", "default-deny", "NPM", "Calico", "Cilium", "NSG", "UDR", "Next hop", "Azure Firewall"])
    pols_data = ctx.data.get("networkpolicies")
    pods = items(ctx.data.get("pods"))
    engine = (nw.get("networkPolicy") or "").lower() if ok else None
    seen_engine = [n for n, label in (("azure-npm", "azure"), ("calico-node", "calico"), ("cilium", "cilium")) if _ds(ctx, n)]
    if pols_data is None:
        _netcheck(rep, ctx, "policies", "Network policies per namespace and the policy engine", NET_NA,
                  "NetworkPolicy objects per namespace, whether a default-deny policy exists and which engine enforces them.", "the NetworkPolicy objects could not be read.",
                  "Grant read access to networkpolicies.networking.k8s.io.")
    else:
        pols = items(pols_data)
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
        prow = [[n, support_of(ctx, n) or "-", pod_ns[n], per_ns[n]["n"],
                 ("ingress " if per_ns[n]["deny_in"] else "") + ("egress" if per_ns[n]["deny_out"] else "") or ("none" if not per_ns[n]["n"] else "no")]
                for n in sorted(set(pod_ns) | set(per_ns))]
        open_ns = [n for n in pod_ns if not per_ns[n]["n"] and n not in ("kube-system",)]
        if open_ns:
            ctx.find("INFO", f"{len(open_ns)} namespace(s) with pods have no NetworkPolicy (all pod-to-pod traffic allowed unless a mesh/CNI restricts it): "
                             + ", ".join(sorted(open_ns)[:8]) + (" ..." if len(open_ns) > 8 else ""))
        enforced = bool(engine) or bool(seen_engine)
        if pols and not enforced and ok:
            st = NET_BAD
            ev = f"{len(pols)} NetworkPolicy object(s) exist but no policy engine was found (cluster setting: {engine or 'none'}; no azure-npm, calico-node or cilium DaemonSet)."
            mean = "Policies are only stored, nothing enforces them: pods are NOT isolated. Enable a network policy engine (Azure, Calico or Cilium) on the cluster."
        elif pols and not enforced:
            st = NET_NA
            ev = f"{len(pols)} NetworkPolicy object(s) exist; whether an engine enforces them could not be determined (" + AZ_OFF_TEXT + ")."
            mean = "Check the cluster's network policy setting in the Azure portal; without an engine the policies do nothing."
        elif not pols:
            st = NET_WARN if pod_ns else NET_OK
            ev = f"no NetworkPolicy exists; {len(open_ns)} namespace(s) with pods are fully open." + (f" Engine: {engine or ', '.join(seen_engine) or 'none'}." if enforced else " No engine either.")
            mean = "All pod-to-pod traffic is allowed. If you need isolation add a default-deny policy per namespace and then allow only the required flows."
        elif open_ns:
            st = NET_WARN
            ev = (f"{len(pols)} policy object(s) in {len(per_ns)} namespace(s), enforced by {engine or ', '.join(seen_engine)}; {len(open_ns)} namespace(s) with pods have none "
                  f"({', '.join(sorted(open_ns)[:4])}); default-deny exists in {sum(1 for d in per_ns.values() if d['deny_in'] or d['deny_out'])}.")
            mean = "Namespaces without a policy accept traffic from every pod. Add a default-deny policy where isolation matters; if a pod unexpectedly cannot connect, look for a policy that selects it."
        else:
            st = NET_OK
            ev = f"{len(pols)} policy object(s) in {len(per_ns)} namespace(s), enforced by {engine or ', '.join(seen_engine)}; every namespace with pods has at least one."
            mean = "Policies exist everywhere; if traffic is blocked unexpectedly, check which policy selects the destination pod."
        _netcheck(rep, ctx, "policies", "Network policies per namespace and the policy engine", st,
                  "NetworkPolicy objects per namespace, whether a default-deny policy exists and which engine (Azure NPM, Calico, Cilium) enforces them.", ev, mean,
                  finding="NetworkPolicy objects exist but no policy engine enforces them" if st == NET_BAD else None)
        rep.table(["Namespace", "SUPPORT DL", "Pods", "Policies", "Default-deny present"], prow, maxw=60,
                  what="for every namespace with pods or policies: how many pods run, how many policies exist and whether a default-deny policy is present.")
    # ---- cloud firewalls
    az = _net_az_collect(ctx)
    if not az.get("ok"):
        _netcheck(rep, ctx, "firewalls", "Cloud firewalls: network security groups, routes and Azure Firewall", NET_NA,
                  "network security group rules on the node subnets / node resource group, route table next hops and any Azure Firewall.",
                  AZ_OFF_TEXT + ".", "Run `az login`, keep 'Azure details' on and make sure you have Reader on the cluster, virtual network and node resource group.")
        return
    statuses, evidence = [], []
    nsg_rows = []
    for nsg in az["nsgs"] or []:
        rules = nsg["rules"]
        open_in = [r for r in rules if r.get("direction") == "Inbound" and r.get("access") == "Allow"
                   and (r.get("sourceAddressPrefix") in ("*", "Internet", "0.0.0.0/0"))]
        risky = [r for r in open_in if _port_covers(r, 22) or _port_covers(r, 3389) or r.get("destinationPortRange") == "*"]
        deny = [r for r in rules if r.get("access") == "Deny"]
        nsg_rows.append([nsg["name"], nsg.get("where", "-"), len(rules), len(open_in), len(deny), ", ".join(str(r.get("name")) for r in risky[:3]) or "-"])
        if risky:
            statuses.append(NET_WARN)
            evidence.append(f"{nsg['name']}: inbound rule(s) from the internet to remote-access / all ports ({', '.join(str(r.get('name')) for r in risky[:2])})")
    if az["nsgs"] is None:
        statuses.append(NET_NA)
        evidence.append("network security groups could not be read")
    else:
        evidence.append(f"{len(az['nsgs'])} network security group(s) read")
        statuses.append(NET_OK) if not any(s == NET_WARN for s in statuses) else None
    default_none = [r for r in az["route_rows"] if r[2] == "0.0.0.0/0" and r[3] == "None"]
    to_appliance = [r for r in az["route_rows"] if r[2] == "0.0.0.0/0" and r[3] == "VirtualAppliance"]
    fw_names = []
    for fw in az.get("firewalls") or []:
        for ipc in fw.get("ipConfigurations") or []:
            fw_names.append((ipc.get("privateIPAddress"), fw.get("name")))
    if default_none:
        statuses.append(NET_BAD)
        evidence.append(f"default route 0.0.0.0/0 in {default_none[0][0]} goes to 'None' (traffic is dropped)")
    elif to_appliance:
        hop = to_appliance[0][4]
        fw = next((n for ip, n in fw_names if ip == hop), None)
        evidence.append(f"all outbound traffic goes through {('Azure Firewall ' + fw) if fw else 'a virtual appliance'} at {hop}; its rules must allow the AKS required endpoints")
        statuses.append(NET_OK)
    elif az["route_rows"]:
        evidence.append(f"{len(az['route_rows'])} user-defined route(s), none overrides the default route")
        statuses.append(NET_OK)
    else:
        evidence.append("no user-defined routes on the node subnets" + (" (route tables could not be read: " + "; ".join(e for e in az['errors'] if 'route' in e)[:80] + ")" if any('route' in e for e in az['errors']) else ""))
        statuses.append(NET_NA if any('route' in e for e in az['errors']) else NET_OK)
    status = _combine(statuses)
    mean = {NET_BAD: "Outbound traffic is black-holed by a route: nodes cannot reach registries or the control plane. Fix the next hop of the 0.0.0.0/0 route.",
            NET_WARN: "A rule lets the internet reach remote-access ports or all ports on the nodes; restrict the source to your admin ranges or remove it.",
            NET_NA: "Some firewall data could not be read; check Reader access to the network security groups and route tables.",
            NET_OK: "No firewall rule or route that blocks the cluster traffic was found in the data read. If traffic is still blocked, look at the Azure Firewall rules and at flow logs."}[status]
    _netcheck(rep, ctx, "firewalls", "Cloud firewalls: network security groups, routes and Azure Firewall", status,
              "network security group rules on the node subnets / node resource group, route table next hops and any Azure Firewall.", "; ".join(evidence) + ".", mean)
    if nsg_rows:
        rep.table(["Network security group", "Found in", "Custom rules", "Inbound allow rules from the internet", "Deny rules", "Risky rules (remote access or all ports)"], nsg_rows, maxw=50,
                  what="each network security group read for the cluster with how many custom rules it has and whether any open remote-access or all ports to the internet.")
    if az["route_rows"]:
        rep.table(["Route table", "Route", "Address prefix", "Next hop type", "Next hop IP address"], az["route_rows"], maxw=50,
                  what="user-defined routes on the node subnets; a 0.0.0.0/0 route decides where all outbound traffic goes (for example to a firewall).")
        for r in default_none:
            ctx.find("HIGH", f"Route table {r[0]}: default route 0.0.0.0/0 goes to 'None' - nodes can't reach the internet / registries")
        for r in to_appliance:
            ctx.find("INFO", f"Route table {r[0]}: all outbound traffic goes through a virtual appliance/firewall ({r[4]}) - allow the AKS required endpoints there")
    if az.get("pips"):
        rep.table(["Name", "Address", "Pricing tier", "Allocation", "Attached to"], az["pips"], maxw=50,
                  what="public IP addresses in the node resource group, with the resource each one is attached to.")
    for line in az["errors"]:
        rep.add(f"  [!] not read: {line}")


# ---------------------------------------------------------------------------
# 6. Load balancers and ingress
# ---------------------------------------------------------------------------

INGRESS_NAMESPACES = {"app-routing-system", "ingress-nginx", "azure-alb-system", "ingress-basic", "ingress", "nginx-ingress", "traefik", "kong", "projectcontour",
                      "haproxy-ingress", "istio-ingress", "contour", "ingress-controller"}
INGRESS_NAME = re.compile(r"ingress|nginx|traefik|alb-controller|agic|contour|haproxy|emissary|envoy-gateway|kong|istio-ingressgateway", re.I)


def _cert_not_after(pem_text):
    """notAfter (aware datetime) of the first certificate in a PEM text, parsed from the DER bytes by hand (no extra packages), or None."""
    import base64
    m = re.search(r"-----BEGIN CERTIFICATE-----(.+?)-----END CERTIFICATE-----", pem_text, re.S)
    if not m:
        return None
    try:
        der = base64.b64decode(re.sub(r"\s+", "", m.group(1)))

        def tlv(i):
            tag, ln = der[i], der[i + 1]
            i += 2
            if ln & 0x80:
                n = ln & 0x7F
                ln = int.from_bytes(der[i:i + n], "big")
                i += n
            return tag, i, ln
        _t, i, _l = tlv(0)                  # Certificate
        _t, i, _l = tlv(i)                  # TBSCertificate (value starts at i)
        tag, v, ln = tlv(i)
        if tag == 0xA0:                     # [0] version
            i = v + ln
        for _ in range(3):                  # serial, signature algorithm, issuer
            _t, v, ln = tlv(i)
            i = v + ln
        _t, v, ln = tlv(i)                  # Validity
        _t, v1, l1 = tlv(v)                 # notBefore
        t2, v2, l2 = tlv(v1 + l1)           # notAfter
        text = der[v2:v2 + l2].decode("ascii")
        fmt = "%y%m%d%H%M%SZ" if t2 == 0x17 else "%Y%m%d%H%M%SZ"
        return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _ingress_controllers(ctx):
    """The ingress controller pods (their logs are read)."""
    ctrl = []
    for p in items(ctx.data.get("pods")):
        ns = p["metadata"].get("namespace")
        lab = p["metadata"].get("labels") or {}
        if (ns in INGRESS_NAMESPACES and INGRESS_NAME.search(p["metadata"]["name"])) or lab.get("app.kubernetes.io/name") in ("ingress-nginx", "traefik", "alb-controller", "contour") \
                or (ns == "kube-system" and re.search(r"ingress-appgw|agic", p["metadata"]["name"])):
            ctrl.append(p)
    return ctrl


def _tls_secrets(ctx):
    """([(namespace, ingress, secret, hosts)], the same without duplicates, at most 40): the TLS certificates the Ingress objects use."""
    tls = []
    for i in items(ctx.data.get("ingresses")):
        meta, spec = i["metadata"], i.get("spec", {})
        for t in spec.get("tls") or []:
            if t.get("secretName"):
                tls.append((meta["namespace"], meta["name"], t["secretName"], ", ".join(t.get("hosts") or [])))
        kv = (meta.get("annotations") or {}).get("kubernetes.azure.com/tls-cert-keyvault-uri")
        if kv:
            tls.append((meta["namespace"], meta["name"], "(Azure Key Vault)", kv))
    return tls, list(dict.fromkeys(tls))[:40]


def _cert_args(ns, secret):
    return ["get", "secret", secret, "-n", ns, "-o", r"jsonpath={.data.tls\.crt}"]


def _net_ingress(rep, ctx):
    rep.heading("Load balancers and ingress",
                "web traffic entering the cluster: the ingress controller pods, the Services behind each Ingress, the Azure load balancer health probes and the TLS certificates in use.")
    rep.glossary(["Ingress", "Ingress controller", "502/503/504", "Health probe", "LB", "TLS", "Endpoint"])
    pods_data = ctx.data.get("pods")
    ings = items(ctx.data.get("ingresses"))
    # ---- controllers
    ctrl = _ingress_controllers(ctx)
    jobs = {("ing", p["metadata"]["namespace"], p["metadata"]["name"]): {"ns": p["metadata"]["namespace"], "pod": p["metadata"]["name"], "tail": 500} for p in ctrl[:8]}
    _prefetch_logs(ctx, {k: v for k, v in jobs.items()})
    crow, statuses, ev_parts, total5 = [], [], [], 0
    for p in ctrl[:8]:
        key = ("ing", p["metadata"]["namespace"], p["metadata"]["name"])
        lines, err = ctx.data["net_logs"][key]
        ready = _pod_ready(p)
        c = {"502": 0, "503": 0, "504": 0, "err": 0}
        if lines is not None:
            for l in lines:
                m = re.search(r"\"\s(50[234])\s|status[=:\s\"]+(50[234])\b|\s(50[234])\s\d", l)
                if m:
                    c[next(g for g in m.groups() if g)] += 1
                elif re.search(r"\[error\]|level=error|\bE\d{4} ", l):
                    c["err"] += 1
        n5 = c["502"] + c["503"] + c["504"]
        total5 += n5
        crow.append([p["metadata"]["namespace"], p["metadata"]["name"], "yes" if ready else "NOT READY", _pod_restarts(p),
                     c["502"] if lines is not None else "n/a", c["503"] if lines is not None else "n/a", c["504"] if lines is not None else "n/a",
                     c["err"] if lines is not None else "n/a"])
        statuses.append(NET_BAD if not ready else (NET_WARN if n5 else (NET_NA if lines is None else NET_OK)))
        if not ready:
            ev_parts.append(f"{p['metadata']['name']} not ready")
    if pods_data is None:
        st, ev, mean = NET_NA, "the pod list could not be read.", "Grant read access to pods."
    elif not ctrl:
        if ings:
            st = NET_WARN
            ev = f"{len(ings)} Ingress object(s) exist but no ingress controller pod was found in the usual namespaces (app-routing-system, ingress-nginx, azure-alb-system ...)."
            mean = "If the controller is a managed one (Application Gateway for Containers, Application Gateway) its health is in the Azure portal; otherwise the controller is missing and the Ingress rules do nothing."
        else:
            st, ev, mean = NET_OK, "no Ingress objects and no ingress controller pods exist.", "Ingress is not used in this cluster."
    else:
        st = _combine(statuses)
        ev = f"{len(ctrl)} controller pod(s); {total5} gateway error(s) 502/503/504 in the last {ctx.minutes} min" + ("; " + ", ".join(ev_parts) if ev_parts else "") + "."
        mean = {NET_BAD: "An ingress controller pod is not ready; web traffic may be partly or fully down. Describe the pod and check its logs.",
                NET_WARN: "The controller returned 502/503/504 errors: its backends were unreachable, not ready or slow. Check the backend pods and Services listed below, and the backend timeouts.",
                NET_NA: "Controller logs could not be read; check pod log access.",
                NET_OK: "Controller pods are ready and logged no gateway errors in the window."}[st]
    _netcheck(rep, ctx, "ingress_ctrl", "Ingress controllers: pod health and recent gateway errors", st,
              "the ingress controller pods (NGINX / App Routing, Application Gateway for Containers, others), their readiness and the 502 / 503 / 504 errors in their recent logs.", ev, mean,
              finding=(f"Ingress controller logged {total5} gateway errors (502/503/504) in the window") if total5 else None)
    if crow:
        rep.table(["Namespace", "Pod", "Ready", "Container restarts", "Errors 502 (bad gateway)", "Errors 503 (service unavailable)", "Errors 504 (gateway timeout)", "Other error lines"], crow, maxw=50,
                  what="each ingress controller pod with readiness and the number of 502 / 503 / 504 responses and other errors found in its recent logs.")
    # ---- ingress backends
    endpoints = {(e["metadata"]["namespace"], e["metadata"]["name"]): e for e in items(ctx.data.get("endpoints"))}
    svcs = {(s["metadata"]["namespace"], s["metadata"]["name"]) for s in items(ctx.data.get("services"))}
    brows, bad, noaddr = [], [], []
    for i in ings:
        meta, spec = i["metadata"], i.get("spec", {})
        lb = (i.get("status", {}).get("loadBalancer") or {}).get("ingress") or []
        address = (lb[0].get("hostname") or lb[0].get("ip")) if lb else ""
        if not address:
            noaddr.append(f"{meta['namespace']}/{meta['name']}")
            ctx.find("MED", f"Ingress {meta['namespace']}/{meta['name']} has no load balancer address" + support_suffix(ctx, [meta["namespace"]]))
            ctx.ns_issue(meta["namespace"], f"ingress {meta['name']} has no address")
        paths = []
        for r in spec.get("rules", []) or []:
            for pth in (r.get("http") or {}).get("paths", []) or []:
                paths.append((r.get("host") or "*", pth.get("path") or "/", ((pth.get("backend") or {}).get("service") or {}).get("name")))
        db = ((spec.get("defaultBackend") or {}).get("service") or {}).get("name")
        if db:
            paths.append(("(default)", "/", db))
        klass = spec.get("ingressClassName") or (meta.get("annotations") or {}).get("kubernetes.io/ingress.class") or "-"
        for host, path, svc in paths:
            key = (meta["namespace"], svc)
            found = key in svcs
            ready_n = None
            if key in endpoints:
                ready_n = sum(len(sub.get("addresses") or []) for sub in endpoints[key].get("subsets") or [])
            problem = (svc is not None and not found) or (found and ready_n == 0)
            brows.append([meta["namespace"], support_of(ctx, meta["namespace"]) or "-", meta["name"], klass, host, path, svc or "-", "yes" if found else "MISSING",
                          "-" if ready_n is None else ready_n, address[:40] or "NO ADDRESS"])
            if problem:
                bad.append(f"{meta['namespace']}/{meta['name']} -> {svc}")
    if ctx.data.get("ingresses") is None:
        st, ev, mean = NET_NA, "the Ingress objects could not be read.", "Grant read access to ingresses.networking.k8s.io."
    elif not ings:
        st, ev, mean = NET_OK, "no Ingress objects exist.", "Ingress is not used."
    else:
        parts = [NET_BAD] if bad else ([NET_WARN] if noaddr else [NET_OK])
        st = _combine(parts)
        ev = f"{len(ings)} Ingress object(s), {len(brows)} backend path(s); " + (f"backends missing or without ready pods: {', '.join(bad[:3])}" if bad else "every backend Service exists and has ready pods") \
            + (f"; no address yet: {', '.join(noaddr[:3])}" if noaddr else "") + "."
        mean = {NET_BAD: "Requests to these paths end in 503 / 502 from the controller: the backend Service does not exist (typo in the name) or none of its pods are ready.",
                NET_WARN: "An Ingress has no address yet: the controller has not reconciled it (check controller logs and the Ingress events).",
                NET_OK: "Every Ingress points to a Service with ready pods."}[st]
    _netcheck(rep, ctx, "ingress_backends", "Ingress backends: Services behind each Ingress", st,
              "whether the Service named by every Ingress rule exists and has ready pods, and whether the Ingress got an address.", ev, mean,
              finding=(f"Ingress backend Service missing or without ready pods: {', '.join(bad[:3])}") if bad else None)
    if brows:
        rep.table(["Namespace", "SUPPORT DL", "Ingress", "Ingress class", "Host", "Path", "Backend Service", "Service found", "Ready endpoints", "Address"], brows, maxw=50,
                  what="every host and path of every Ingress with the backend Service it sends traffic to and whether that Service has ready pods.")
    # ---- Azure load balancer health probes
    az = _net_az_collect(ctx)
    lrows, dvals = [], []
    for m in az.get("lb_metrics", []):
        got = m["got"]
        dip = [v for _, v in got.get("dip", [])]
        vip = [v for _, v in got.get("vip", [])]
        lrows.append([m["lb"]["name"], m["lb"].get("sku"), f"{min(dip):.0f}%" if dip else "no data", f"{min(vip):.0f}%" if vip else "no data"])
        if dip:
            dvals.append(min(dip))
            if min(dip) < 100:
                ctx.find("MED" if min(dip) >= 50 else "HIGH", f"Load balancer {m['lb']['name']}: backend health probe availability dropped to {min(dip):.0f}% in the window")
    if not az.get("ok"):
        st, ev, mean = NET_NA, AZ_OFF_TEXT + ".", "Enable the Azure details to read the load balancer metrics."
    elif not dvals:
        st = NET_NA
        ev = "no health probe metric was returned" + (" (" + "; ".join(az["errors"])[:100] + ")" if az["errors"] else " (no Standard load balancer found in the node resource group, or Monitoring Reader is missing)") + "."
        mean = "Standard load balancers only expose probe availability; grant Monitoring Reader on the node resource group, or look at the load balancer in the Azure portal (Insights)."
    else:
        low = min(dvals)
        st = NET_BAD if low < 50 else (NET_WARN if low < 100 else NET_OK)
        ev = f"{len(dvals)} load balancer(s) with probe data; lowest backend health probe availability in the last {ctx.minutes} min: {low:.0f}%."
        mean = {NET_BAD: "Over half the probes failed: backends (nodes) are not answering the probe port or the pods behind the Service are unhealthy; check externalTrafficPolicy, the probe path and the node network security group.",
                NET_WARN: "Some probes failed in the window - a node or pod was briefly not answering. Compare the time with node or pod events.",
                NET_OK: "All backends answered every probe."}[st]
    _netcheck(rep, ctx, "lb_probes", "Azure load balancer backend health probes", st,
              "the share of load balancer health probes that succeeded in the window (Azure Monitor metric 'Health probe status'), per load balancer.", ev, mean)
    if lrows:
        rep.table(["Load balancer", "Pricing tier", "Backend health probe availability (lowest)", "Virtual IP address availability (lowest)"], lrows, maxw=50,
                  what="for each load balancer in the node resource group, the lowest share of successful health probes and of address availability in the window.")
    # ---- TLS certificates
    tls, uniq = _tls_secrets(ctx)
    trows, parts = [], []

    def read_cert(ns, secret):
        ok, out = kubectl(_cert_args(ns, secret), timeout=30)
        if not ok:
            return None, (out.splitlines()[0][:80] if out else "unreadable")
        try:
            import base64
            return _cert_not_after(base64.b64decode(out).decode("utf-8", "replace")), None
        except Exception as exc:
            return None, str(exc)[:60]
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {t: (pool.submit(read_cert, t[0], t[2]) if t[2] != "(Azure Key Vault)" else None) for t in uniq}
    for t in uniq:
        if futs[t] is None:
            trows.append([t[0], t[1], t[2], t[3], "not readable here", "-"])
            parts.append(NET_NA)
            continue
        exp, err = futs[t].result()
        if exp is None:
            trows.append([t[0], t[1], t[2], t[3], "unreadable" + (f" ({err})" if err else ""), "-"])
            parts.append(NET_NA)
            continue
        days = (exp - ctx.now).days
        trows.append([t[0], t[1], t[2], t[3], f"{exp:%Y-%m-%d}", days if days >= 0 else f"EXPIRED {-days} days ago"])
        parts.append(NET_BAD if days < 0 else (NET_WARN if days < 30 else NET_OK))
    if not tls:
        st, ev, mean = NET_OK, "no Ingress references a TLS certificate secret.", "No certificates to expire at the ingress."
    else:
        st = _combine(parts)
        ev = f"{len(uniq)} certificate reference(s): " + (f"{parts.count(NET_BAD)} expired, " if NET_BAD in parts else "") + (f"{parts.count(NET_WARN)} expire within 30 days, " if NET_WARN in parts else "") \
            + (f"{parts.count(NET_NA)} could not be read, " if NET_NA in parts else "") + f"{parts.count(NET_OK)} valid for more than 30 days."
        mean = {NET_BAD: "An expired certificate makes browsers and clients refuse the connection. Renew the certificate (cert-manager issuer, Key Vault) and update the secret now.",
                NET_WARN: "A certificate expires within 30 days: renew it before then (check that automatic renewal works).",
                NET_NA: "Some certificates could not be read (secrets access denied or managed in Key Vault); check their expiry in the owning system.",
                NET_OK: "All referenced certificates are valid for more than 30 days."}[st]
    _netcheck(rep, ctx, "tls", "Transport Layer Security certificates used by Ingress", st,
              "the expiry date of every certificate secret an Ingress uses (only the public certificate part is read, never the private key).", ev, mean,
              finding=(f"TLS certificate expired or expiring within 30 days: {ev}") if st in (NET_BAD, NET_WARN) else None)
    if trows:
        rep.table(["Namespace", "Ingress", "Secret", "Hosts", "Expires on", "Days left"], trows, maxw=50,
                  what="each certificate used by an Ingress with the date it expires and the days left.")


# ---------------------------------------------------------------------------
# 7. Connection tracking and outbound port exhaustion
# ---------------------------------------------------------------------------

def _exporter_pods(ctx):
    return [p for p in items(ctx.data.get("pods")) if re.search(r"node-exporter|ama-metrics-node", p["metadata"]["name"]) and p.get("status", {}).get("phase") == "Running"]


def _exporter_args(p):
    port = 9100
    for c in p.get("spec", {}).get("containers", []) or []:
        for cp in c.get("ports", []) or []:
            if cp.get("name") in ("metrics", "http-metrics", "node-exporter") or cp.get("containerPort") == 9100:
                port = cp.get("containerPort", 9100)
    ns, name = p["metadata"]["namespace"], p["metadata"]["name"]
    return ["get", "--raw", f"/api/v1/namespaces/{ns}/pods/{name}:{port}/proxy/metrics"]


def _net_conntrack(rep, ctx):
    rep.heading("Connection tracking and outbound port exhaustion",
                "whether the nodes' connection tables or the outbound ports (source network address translation) are running out, which makes new connections fail.")
    rep.glossary(["conntrack", "SNAT", "NAT gateway", "LB"])
    exporters = _exporter_pods(ctx)

    def read(p):
        ok, out = kubectl(_exporter_args(p), timeout=45)
        if not ok:
            return None, (out.splitlines()[0][:80] if out else "unreadable")
        ent = re.search(r"^node_nf_conntrack_entries\s+([0-9.e+]+)", out, re.M)
        lim = re.search(r"^node_nf_conntrack_entries_limit\s+([0-9.e+]+)", out, re.M)
        return ((float(ent.group(1)), float(lim.group(1))) if ent and lim else None), None if (ent and lim) else "metric node_nf_conntrack_entries not exposed"
    rows, usage, errs = [], [], []
    if exporters:
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(read, exporters[:20]))
        for p, (vals, err) in zip(exporters[:20], results):
            node = p.get("spec", {}).get("nodeName", "?")
            if vals:
                pct = 100 * vals[0] / vals[1] if vals[1] else 0
                usage.append(pct)
                rows.append([node_tag(ctx, node), f"{vals[0]:.0f}", f"{vals[1]:.0f}", f"{pct:.0f}% used"])
            else:
                errs.append(err)
    if not exporters:
        st = NET_NA
        ev = "needs node-exporter or node access: no node-exporter pod (or ama-metrics-node pod) is running in the cluster."
        mean = ("Connection-table usage is not visible from the Kubernetes API alone. Install node-exporter (or Azure Managed Prometheus) and re-run, or check `conntrack -C` and `nf_conntrack_max` "
                "through your normal node access path. Symptoms: random connection timeouts and 'nf_conntrack: table full, dropping packet' in the node's kernel log.")
    elif not usage:
        st = NET_NA
        ev = f"{len(exporters)} node-exporter pod(s) found but the connection-tracking metrics could not be read through the API proxy ({', '.join(sorted(set(e for e in errs if e)))[:100]})."
        mean = "Allow 'get' on pods/proxy, or read the metric from your Prometheus (node_nf_conntrack_entries / node_nf_conntrack_entries_limit)."
    else:
        top = max(usage)
        st = NET_BAD if top >= 90 else (NET_WARN if top >= 75 else NET_OK)
        ev = f"{len(usage)} node(s) read; highest connection-table usage {top:.0f}%."
        mean = {NET_BAD: "A node's connection table is almost full; new connections are dropped. Raise nf_conntrack_max (node configuration), reduce short-lived connections, or spread the load over more nodes.",
                NET_WARN: "Connection table usage is high; watch it, especially during traffic peaks.",
                NET_OK: "Plenty of room in the connection tables."}[st]
    _netcheck(rep, ctx, "conntrack", "Connection tracking table usage", st,
              "how full each node's connection tracking table is, read from node-exporter metrics through the Kubernetes API proxy (read-only).", ev, mean,
              finding=(f"Connection tracking table nearly full on a node: {ev}") if st == NET_BAD else None)
    if rows:
        rep.table(["Node", "Tracked connections", "Maximum connections", "Usage"], rows, maxw=40,
                  what="the number of connections each node is tracking against its limit.")
    # ---- outbound ports
    az = _net_az_collect(ctx)
    srows, parts, evid = [], [], []
    for m in az.get("lb_metrics", []):
        got = m["got"]
        used = max((v for _, v in got.get("used", [])), default=None)
        alloc = max((v for _, v in got.get("alloc", [])), default=None)
        pct = (100 * used / alloc) if used is not None and alloc else None
        failed = m.get("failed_snat")
        srows.append([m["lb"]["name"], f"{alloc:.0f}" if alloc is not None else "n/a", f"{used:.0f}" if used is not None else "n/a",
                      f"{pct:.0f}%" if pct is not None else "n/a", f"{failed:.0f}" if failed is not None else "n/a"])
        if pct is not None:
            parts.append(NET_BAD if pct >= 95 else (NET_WARN if pct >= 80 else NET_OK))
            evid.append(f"{m['lb']['name']}: {pct:.0f}% of the allocated outbound ports used at peak")
            if pct >= 80:
                ctx.find("HIGH" if pct >= 95 else "MED", f"Load balancer {m['lb']['name']}: SNAT ports {pct:.0f}% used ({used:.0f}/{alloc:.0f}) - risk of outbound connection failures")
        if failed:
            parts.append(NET_BAD)
            evid.append(f"{m['lb']['name']}: {failed:.0f} failed outbound connection(s)")
    for nm in az.get("nat_metrics", []):
        d = sum(v for _, v in nm["got"].get("drop", []))
        if d:
            parts.append(NET_WARN)
            evid.append(f"NAT gateway {nm['nat']['name']}: {d:.0f} dropped packets")
    if not az.get("ok"):
        st, ev, mean = NET_NA, AZ_OFF_TEXT + ".", "Enable the Azure details; the outbound port metrics come from Azure Monitor."
    elif not parts:
        st = NET_NA
        ev = "no outbound port metric was returned" + (" (cluster uses a NAT gateway / user-defined route instead of the load balancer, or Monitoring Reader is missing)" if True else "") + "."
        mean = "Outbound ports are only measured on a Standard load balancer. For a NAT gateway check its metrics 'SNAT connection count' and 'Dropped packets' in the portal."
    else:
        st = _combine(parts)
        ev = "; ".join(evid) + "."
        mean = {NET_BAD: "Outbound ports are exhausted: new outbound connections fail or time out intermittently. Add outbound IP addresses or ports per node, use a NAT gateway, reuse connections "
                         "(connection pooling) and avoid opening many short connections to the same destination.",
                NET_WARN: "Outbound port use is high; add outbound IP addresses before it reaches the limit.",
                NET_OK: "Outbound port use is far below the allocation."}[st]
    _netcheck(rep, ctx, "snat", "Outbound port exhaustion (source network address translation)", st,
              "the peak share of allocated outbound ports in use on the load balancer, failed outbound connections and NAT gateway drops (Azure Monitor).", ev, mean)
    if srows:
        rep.table(["Load balancer", "Outbound ports allocated", "Outbound ports used (peak)", "Share used", "Failed outbound connections"], srows, maxw=40,
                  what="for each load balancer the outbound ports it allocated, the most that were in use at once and any failed outbound connections.")


# ---------------------------------------------------------------------------
# 8. Observability and packet capture
# ---------------------------------------------------------------------------

def _net_observability(rep, ctx):
    rep.heading("Network observability and packet capture",
                "which network monitoring features are switched on for this cluster, and which read-only tools you can use for a deeper look at packets.")
    rep.glossary(["ACNS", "Container Insights", "Flow logs", "Traffic Analytics", "Packet capture", "Hubble", "Retina", "NSG", "VNet"])
    ok, cluster, _t = _az_state(ctx)
    az = _net_az_collect(ctx)
    nw = (cluster or {}).get("networkProfile") or {}
    adv = nw.get("advancedNetworking") or {}
    addon = ((cluster or {}).get("addonProfiles") or {}).get("omsagent") or {}
    amp = (cluster or {}).get("azureMonitorProfile") or {}
    insights = bool(addon.get("enabled")) or bool((amp.get("containerInsights") or {}).get("enabled"))
    prom = bool((amp.get("metrics") or {}).get("enabled"))
    rows = []
    if ok:
        rows += [["Advanced Container Networking Services - observability", "enabled" if (adv.get("observability") or {}).get("enabled") else "not enabled",
                  "Hubble flow data and per-pod network metrics (AKS add-on)."],
                 ["Advanced Container Networking Services - security", "enabled" if (adv.get("security") or {}).get("enabled") else "not enabled", "Fully-qualified-domain-name based policies and flow logging (Cilium only)."],
                 ["Container Insights", "enabled" if insights else "not enabled", "Azure Monitor performance and log collection for the cluster."],
                 ["Managed Prometheus metrics", "enabled" if prom else "not enabled", "Prometheus metrics (including node-exporter) stored in Azure Monitor."]]
    fl = az.get("flow_logs")
    node_nsg_ids = {(n.get("id") or "").lower() for n in (az.get("nsgs") or [])}
    vnet_ids = set()
    for sn in ctx.data.get("az_subnets") or []:
        m = re.match(r"(.*/virtualNetworks/[^/]+)/subnets/", sn.get("id") or "", re.I)
        if m:
            vnet_ids.add(m.group(1).lower())
    covered = False
    if fl is not None:
        for f in fl:
            tgt = (f.get("targetResourceId") or "").lower()
            ta = ((f.get("flowAnalyticsConfiguration") or {}).get("networkWatcherFlowAnalyticsConfiguration") or {}).get("enabled")
            rows.append([f"Flow log {f.get('name')}", "enabled" if f.get("enabled") else "disabled",
                         f"target {_res_name(tgt) or tgt}; Traffic Analytics {'on' if ta else 'off'}; retention {(f.get('retentionPolicy') or {}).get('days', '?')} days"])
            if f.get("enabled") and (tgt in node_nsg_ids or tgt in vnet_ids):
                covered = True
        if not fl:
            rows.append(["Flow logs (network security group / virtual network)", "none", "No flow log exists in this region's Network Watcher."])
    elif ok:
        rows.append(["Flow logs (network security group / virtual network)", "not available", "could not be read (Network Watcher missing or no access)"])
    parts, ev = [], []
    if ok:
        parts.append(NET_OK if ((adv.get("observability") or {}).get("enabled") or insights or prom) else NET_WARN)
        ev.append("cluster monitoring: " + ", ".join(n for n, on in (("Advanced Container Networking observability", (adv.get("observability") or {}).get("enabled")), ("Container Insights", insights),
                                                                       ("Managed Prometheus", prom)) if on) or "cluster monitoring: none enabled")
        if fl is None:
            parts.append(NET_NA)
            ev.append("flow logs not readable")
        else:
            parts.append(NET_OK if covered else NET_WARN)
            ev.append("flow logs " + ("cover the node subnets" if covered else "do not cover the node subnets / virtual network"))
    st = _combine(parts) if parts else NET_NA
    ev_text = ("; ".join(ev) if ev else AZ_OFF_TEXT) + "."
    mean = {NET_OK: "Network flows and metrics are being recorded, so past incidents can be investigated.",
            NET_WARN: "Part of the network observability is off. Turn on flow logs for the node subnet's network security group (with Traffic Analytics) and Advanced Container Networking Services observability so that "
                      "connection failures can be traced after the fact.",
            NET_NA: "Some features could not be read; check Reader / Network Contributor access to Network Watcher and the cluster."}[st]
    _netcheck(rep, ctx, "observability", "Provider network observability features", st,
              "which cloud network monitoring features are enabled: Advanced Container Networking Services observability, Container Insights, Managed Prometheus, flow logs and Traffic Analytics.", ev_text, mean)
    if rows:
        rep.table(["Feature", "State", "Detail"], rows, maxw=90, what="each network observability feature of this cluster, whether it is switched on and what it records.")
    # ---- packet capture availability + guidance (text only, nothing is run)
    tools = [p["metadata"]["name"] for p in items(ctx.data.get("pods")) if re.search(r"hubble-relay|hubble-ui|retina", p["metadata"]["name"])]
    tool_ds = [n for n in ("retina-agent", "cilium") if _ds(ctx, n)]
    if ctx.data.get("pods") is None:
        st2, ev2 = NET_NA, "the pod list could not be read."
        m2 = "Grant read access to pods."
    elif tools or "retina-agent" in tool_ds or (adv.get("observability") or {}).get("enabled"):
        st2 = NET_OK
        ev2 = "in-cluster network capture / flow tooling detected: " + ", ".join((tools + tool_ds)[:4] or ["Advanced Container Networking observability"]) + "."
        m2 = "You can look at flows with Hubble or take a bounded packet capture with Retina (see the guidance below)."
    else:
        st2 = NET_WARN
        ev2 = "no Retina, Hubble or Advanced Container Networking observability detected; packet capture would have to be done on a node or with Network Watcher."
        m2 = "Before an incident, enable Advanced Container Networking Services observability (or install Retina) so a capture can be taken without logging in to nodes."
    _netcheck(rep, ctx, "capture", "Packet capture availability", st2,
              "whether the cluster has in-cluster tooling that can record or show packets without node access.", ev2, m2)
    guide = [["Hubble (hubble observe)", "Shows live flows between pods with verdicts (forwarded / dropped) and the policy that dropped them.", "Yes - read-only",
              "hubble observe --namespace <ns> --verdict DROPPED   (needs Advanced Container Networking Services)"],
             ["Retina capture", "Records packets on selected nodes / pods for a bounded time and saves them to a file or storage.", "Starts a short-lived capture job - ask for change approval",
              "kubectl retina capture create --name cap1 --namespace <ns> --pod-selectors app=<name> --duration 60s"],
             ["Azure Network Watcher packet capture", "Captures packets on a node's virtual machine (scale set instance) from Azure, no login needed.", "Creates a capture resource - ask for approval",
              "az network watcher packet-capture create --vm <vmss-instance> --name cap1 ..."],
             ["Flow logs and Traffic Analytics", "Show allowed / denied connections per network security group rule after the fact.", "Yes - read-only",
              "az network watcher flow-log list --location <region>"],
             ["Connection and event inspection", "Events, Service endpoints, pod and node conditions show most routing and policy problems without any capture.", "Yes - read-only",
              "kubectl get events -A --field-selector type=Warning ; kubectl get endpoints -A"]]
    rep.table(["Tool", "What it gives you", "Safe to run (read-only)?", "Example command (not run by this report)"], guide, maxw=80,
              what="read-only and capture tools you can use for deeper troubleshooting; this report ran none of them, no pod commands, no node logins, no packet capture.",
              title="Guidance: packet capture and deeper investigation")


# ---------------------------------------------------------------------------
# 9. Control plane / API server
# ---------------------------------------------------------------------------

def _net_control_plane(rep, ctx):
    rep.heading("Control plane (the cluster management server)",
                "whether the Kubernetes API server is rejecting requests, whether admission webhooks fail, and whether the control plane database is healthy.")
    rep.glossary(["API server", "429 / throttling", "Admission webhook", "Failure policy", "etcd"])
    ok, out = kubectl(["get", "--raw", "/metrics"], timeout=60)
    if not ok or "apiserver_request_total" not in out:
        reason = (out.splitlines()[0][:100] if (not ok and out) else "the endpoint returned no API server counters")
        _netcheck(rep, ctx, "apiserver", "Control plane request throttling", NET_NA,
                  "requests the API server rejected (HTTP 429 and priority-and-fairness rejections), read from its /metrics endpoint.",
                  f"the metrics endpoint is not readable here ({reason}).",
                  "AKS often does not allow reading /metrics; use the Azure Monitor control plane metrics (API server requests by code) or the control plane logs in Log Analytics instead.")
    else:
        total = rejected = r429 = 0.0
        for line in out.splitlines():
            if line.startswith("apiserver_flowcontrol_rejected_requests_total"):
                rejected += float(line.rsplit(" ", 1)[1])
            elif line.startswith("apiserver_request_total{"):
                try:
                    v = float(line.rsplit(" ", 1)[1])
                except ValueError:
                    continue
                total += v
                if 'code="429"' in line:
                    r429 += v
        share = 100 * r429 / total if total else 0
        st = NET_BAD if share >= 1 else (NET_WARN if (rejected or r429) else NET_OK)
        ev = f"since the API server started: {total:.0f} requests, {r429:.0f} answered 429 ({share:.2f}%), {rejected:.0f} rejected by priority and fairness."
        mean = {NET_BAD: "The API server is shedding load: clients get 'Too Many Requests'. Find the noisy client (controllers, CI, monitoring agents polling too often) and slow it down; scale up the control plane tier if it is legitimate load.",
                NET_WARN: "A few requests were rejected since start; usually harmless, but watch whether the count grows between runs.",
                NET_OK: "No requests were rejected."}[st]
        _netcheck(rep, ctx, "apiserver", "Control plane request throttling", st,
                  "requests the API server rejected (HTTP 429 and priority-and-fairness rejections), read from its /metrics endpoint.", ev, mean,
                  finding=("API server is throttling clients: " + ev) if st == NET_BAD else None)
    # ---- webhooks
    wh_rows, bad, kinds_ok = [], [], []
    for kind, res in (("Validating", "validatingwebhookconfigurations"), ("Mutating", "mutatingwebhookconfigurations")):
        data, err = kjson(["get", res])
        kinds_ok.append(data is not None)
        for cfg in items(data):
            for wh in cfg.get("webhooks") or []:
                svc = (wh.get("clientConfig") or {}).get("service") or {}
                target = f"{svc.get('namespace')}/{svc.get('name')}" if svc else ((wh.get("clientConfig") or {}).get("url") or "-")
                ready_n = None
                if svc:
                    ep = next((e for e in items(ctx.data.get("endpoints")) if e["metadata"]["namespace"] == svc.get("namespace") and e["metadata"]["name"] == svc.get("name")), None)
                    if ctx.data.get("endpoints") is not None:
                        ready_n = sum(len(s.get("addresses") or []) for s in (ep or {}).get("subsets") or []) if ep else 0
                fp = wh.get("failurePolicy") or "Fail"
                wh_rows.append([kind, cfg["metadata"]["name"], wh.get("name"), fp, wh.get("timeoutSeconds") or 10, target, "-" if ready_n is None else ready_n])
                if fp == "Fail" and ready_n == 0:
                    bad.append(f"{cfg['metadata']['name']} -> {target}")
    wh_events = [e for e in items(ctx.data.get("events")) if re.search(r"failed calling webhook|admission webhook .*(denied|timeout|timed out|unreachable)|webhook.*(timeout|connection refused)",
                                                                       e.get("message") or e.get("note") or "", re.I) and (_event_time(e) or ctx.since) >= ctx.since]
    if not any(kinds_ok):
        st, ev = NET_NA, "the webhook configurations could not be read."
        mean = "Grant read access to validatingwebhookconfigurations and mutatingwebhookconfigurations."
    elif bad:
        st = NET_BAD
        ev = f"{len(wh_rows)} webhook(s); with failure policy Fail but no ready backend pod: {', '.join(bad[:3])}."
        mean = "Creating or updating objects the webhook covers will be rejected (often with 'failed calling webhook' or a timeout, which can block deployments and node scaling). Restore the webhook's pods, or set its failure policy to Ignore if it is not critical."
    elif wh_events:
        st = NET_WARN
        ev = f"{len(wh_rows)} webhook(s); {len(wh_events)} warning event(s) mention webhook failures (e.g. {_short(wh_events[0].get('message') or wh_events[0].get('note'), 100)})."
        mean = "A webhook timed out or refused a connection recently. Check the webhook's pods, its timeout and network policies that could block the API server from reaching it."
    else:
        st = NET_OK
        ev = f"{len(wh_rows)} webhook(s) read; each Fail-policy webhook has ready backend pods and no failure events in the window."
        mean = "No webhook problems found."
    _netcheck(rep, ctx, "webhooks", "Admission webhook failures and timeouts", st,
              "admission webhooks with their failure policy and timeout, whether their backend pods are ready, and recent webhook failure events.", ev, mean,
              finding=(f"Admission webhook with failure policy Fail has no ready backend: {', '.join(bad[:2])}") if bad else None)
    if wh_rows:
        rep.table(["Kind", "Configuration", "Webhook", "Failure policy", "Timeout (seconds)", "Target Service", "Ready backend pods"], wh_rows, maxw=50,
                  what="every admission webhook with what happens when it cannot be reached (failure policy) and whether its backend has ready pods.")
    # ---- etcd (only if exposed)
    ok, out = kubectl(["get", "--raw", "/readyz/etcd"], timeout=30)
    if not ok:
        _netcheck(rep, ctx, "etcd", "etcd health (only if exposed)", NET_NA, "the API server's readiness check of its etcd database.",
                  f"not exposed to this user ({_short(out, 90) or 'no answer'}). On AKS the control plane is managed by Azure.",
                  "Nothing to do for managed AKS; use the control plane logs (kube-apiserver) in Log Analytics if you suspect slow requests.")
    else:
        healthy = out.strip().lower() == "ok"
        _netcheck(rep, ctx, "etcd", "etcd health (only if exposed)", NET_OK if healthy else NET_BAD, "the API server's readiness check of its etcd database.",
                  f"/readyz/etcd answered: {_short(out, 80)}.", "etcd is healthy." if healthy else "etcd reports a problem; on AKS open an Azure support request.",
                  finding=None if healthy else "etcd readiness check failed")


# ---------------------------------------------------------------------------
# 10. Traffic measured in the selected window
# ---------------------------------------------------------------------------

def _net_events(rep, ctx):
    events = []
    for e in items(ctx.data.get("events")):
        t = _event_time(e)
        text = f"{e.get('reason', '')} {e.get('message') or e.get('note') or ''}"
        if e.get("type") == "Warning" and t and t >= ctx.since and NET_EVENT_PATTERN.search(text):
            events.append((t, e))
    if events:
        rows = []
        for t, e in sorted(events, key=lambda x: x[0], reverse=True)[:30]:
            obj = e.get("involvedObject") or e.get("regarding") or {}
            name = node_tag(ctx, obj.get("name")) if obj.get("kind") == "Node" else f"{(obj.get('namespace') + '/') if obj.get('namespace') else ''}{obj.get('name', '?')}"
            rows.append([age(t, ctx.now) + " ago", e.get("reason", "?"), f"{obj.get('kind', '?')} {name}", support_of(ctx, obj.get("namespace")) or "-",
                         (e.get("series") or {}).get("count") or e.get("count") or 1, (e.get("message") or e.get("note") or "").replace("\n", " ")[:130]])
            ctx.ns_issue(obj.get("namespace"), f"network warning: {e.get('reason', '?')}")
        rep.table(["When", "Reason", "Object", "SUPPORT DL", "Count", "Message"], rows, maxw=70, title=f"Network-related warning events in the last {ctx.minutes} min ({len(events)})",
                  what="Kubernetes warning events that mention network, DNS, routing or load balancer problems in the selected window, newest first.")
        ctx.find("MED", f"{len(events)} network-related Warning event(s) in the window (e.g. {rows[0][1]})")
    else:
        rep.add(f"Network-related warning events in the last {ctx.minutes} min: none")


def _net_pod_traffic(rep, ctx):
    stats = ctx.data.get("node_stats") or {}
    if not stats:
        rep.add("  Pod and node network counters: not available - the kubelet statistics need the 'nodes/proxy' permission. (Traffic over the window comes from Azure Monitor below.)")
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
        pre = _PF.traffic_sample() if _PF is not None else None      # the parallel collection takes the second sample in the background
        if pre is not None:
            t_first, t_second, fresh = pre
            dt = max(1.0, t_second - t_first)
        else:
            rep.emit(f"  sampling live traffic for {TRAFFIC_SAMPLE_SECONDS}s ...")
            time.sleep(TRAFFIC_SAMPLE_SECONDS)

            def one(node):
                ok, out = _kubectl_uncached(["get", "--raw", f"/api/v1/nodes/{node}/proxy/stats/summary"], timeout=60)
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
    rep.table(["Node", "RX TOTAL", "TX TOTAL", "RX NOW", "TX NOW", "Errors"], rows, maxw=60, title="Network counters per node (from each node's kubelet)",
              what="bytes each node received and transmitted since it booted (total) and the live rate over the sampling time (now), plus interface errors.")
    prow = []
    for (ns, name), (t, node) in pod_a.items():
        r = rates_pod.get((ns, name))
        prow.append(((r[0] + r[1]) if r else 0, t[0] + t[1], [f"{ns}/{name}", support_of(ctx, ns) or "-", node_tag(ctx, node), _fmt_bytes(t[0]), _fmt_bytes(t[1]),
                                                           _fmt_rate(r[0]) if r else "-", _fmt_rate(r[1]) if r else "-", t[2] + t[3]]))
    prow.sort(key=lambda x: (-x[0], -x[1]))
    rep.table(["Pod", "SUPPORT DL", "Node", "RX TOTAL", "TX TOTAL", "RX NOW", "TX NOW", "Errors"], [x[2] for x in prow[:15]], maxw=60,
              title="Top pods by network traffic",
              what="the 15 pods that moved the most data; total is since the pod started (not the selected window), now is the live rate over the sampling time.")
    ns_tot = defaultdict(lambda: [0, 0])
    for (ns, _name), (t, _node) in pod_a.items():
        ns_tot[ns][0] += t[0]
        ns_tot[ns][1] += t[1]
    rep.table(["Namespace", "SUPPORT DL", "RX TOTAL", "TX TOTAL"],
              [[n, support_of(ctx, n) or "-", _fmt_bytes(v[0]), _fmt_bytes(v[1])] for n, v in sorted(ns_tot.items(), key=lambda kv: -(kv[1][0] + kv[1][1]))[:15]],
              title="Network traffic by namespace",
              what="bytes received and transmitted by all pods of each namespace since those pods started.")


def _net_azure_traffic(rep, ctx):
    ok, _cluster, target = _az_state(ctx)
    mins = ctx.minutes
    if not ok:
        rep.add("  Azure traffic and load balancer metrics: not available - " + AZ_OFF_TEXT + ".")
        return
    az = _net_az_collect(ctx)
    idents = node_idents(ctx)
    by_vm = {i["ec2_name"].lower(): n for n, i in idents.items() if i["ec2_name"] != "-"}
    series_rows, rows, totals = [], [], {"in": defaultdict(float), "out": defaultdict(float)}
    first_err = None
    for vmss in ctx.data.get("az_vmss") or []:
        pin, err1 = _az_metric(ctx, target, vmss["id"], "Network In Total", "Total", split="VMName")
        pout, err2 = _az_metric(ctx, target, vmss["id"], "Network Out Total", "Total", split="VMName")
        first_err = first_err or err1 or err2
        for vm_name in sorted(set(pin or {}) | set(pout or {})):
            node = by_vm.get(vm_name.lower())
            if not node:
                continue
            lin, lout = _line("Bytes received", (pin or {}).get(vm_name, []), "Bps", 60), _line("Bytes transmitted", (pout or {}).get(vm_name, []), "Bps", 60)
            for t, v in (pin or {}).get(vm_name, []):
                totals["in"][t] += v / 60
            for t, v in (pout or {}).get(vm_name, []):
                totals["out"][t] += v / 60
            series_rows.append({"n": node, "s": f"{idents[node]['instance_id']} | {idents[node]['zone']} | {idents[node]['type']}", "lines": [lin, lout]})
            rows.append([node_tag(ctx, node), _fmt_rate(lin["avg"]), _fmt_rate(lin["max"]), _fmt_bytes(lin["sum"]),
                         _fmt_rate(lout["avg"]), _fmt_rate(lout["max"]), _fmt_bytes(lout["sum"])])
    if not rows:
        rep.add("  Node traffic: not available - no Azure Monitor data returned" + (f" ({_first_line(first_err, 100)})" if first_err else "")
                + ". Needs Reader (Monitoring Reader) on the node resource group.")
    else:
        keys = sorted(set(totals["in"]) | set(totals["out"]))
        allin = _line("Bytes received", [(k, totals["in"].get(k, 0.0)) for k in keys], "Bps", None, total=False)
        allout = _line("Bytes transmitted", [(k, totals["out"].get(k, 0.0)) for k in keys], "Bps", None, total=False)
        series_rows.insert(0, {"n": "All nodes together (sum)", "s": f"{len(rows)} nodes", "lines": [allin, allout]})
        rep.add(f"  All nodes together: received avg {_fmt_rate(allin['avg'])} peak {_fmt_rate(allin['max'])}; transmitted avg {_fmt_rate(allout['avg'])} peak {_fmt_rate(allout['max'])}")
        rep.table(["Node", "IN avg", "IN peak", "IN total", "OUT avg", "OUT peak", "OUT total"], rows, maxw=60, title="Node traffic over the window",
                  what="bytes each node received (in) and transmitted (out) in the selected window: average and peak per second, and the total bytes.")
        busiest = max(series_rows[1:], key=lambda r: (r["lines"][1]["max"] or 0) + (r["lines"][0]["max"] or 0))
        ctx.find("INFO", f"Busiest node on the network in the window: {busiest['n']} (peak in {_fmt_rate(busiest['lines'][0]['max'])}, out {_fmt_rate(busiest['lines'][1]['max'])})")
    rep.series(f"Node network traffic, last {mins} min (bytes per second)", series_rows,
               "Virtual machine scale set metrics 'Network In Total' / 'Network Out Total' per node (1-minute points).",
               about="small line charts of the bytes per second each node (and all nodes together) received and transmitted, one point per minute over the window, so you can see when traffic peaked.")
    lb_series, lb_rows = [], []
    for m in az.get("lb_metrics", []):
        lb, got, errs = m["lb"], m["got"], m["errs"]
        if not got:
            lb_rows.append([lb["name"], lb.get("sku"), "no Azure Monitor data" + (f" ({_first_line(errs[0], 60)})" if errs else ""), "", "", "", ""])
            continue
        tot = lambda k: sum(v for _, v in got.get(k, []))
        dip_min = min((v for _, v in got.get("dip", [])), default=None)
        used_max, alloc_max = max((v for _, v in got.get("used", [])), default=None), max((v for _, v in got.get("alloc", [])), default=None)
        snat_pct = (100 * used_max / alloc_max) if used_max is not None and alloc_max else None
        lb_rows.append([lb["name"], lb.get("sku"), _fmt_bytes(tot("bytes")), f"{tot('packets'):.0f}", f"{tot('snat'):.0f}",
                        f"{snat_pct:.0f}% ({used_max:.0f}/{alloc_max:.0f})" if snat_pct is not None else "-", f"{dip_min:.0f}%" if dip_min is not None else "-"])
        lines = []
        if got.get("bytes"):
            lines.append(_line("Bytes per minute", got["bytes"], "B"))
        if got.get("packets"):
            lines.append(_line("Packets per minute", got["packets"], "count"))
        if got.get("snat"):
            lines.append(_line("Outbound connections", got["snat"], "count"))
        if got.get("used"):
            lines.append(_line("Outbound ports used", got["used"], "count", None, total=False))
        if got.get("dip"):
            lines.append(_line("Backend health percent", got["dip"], "count", None, total=False))
        lb_series.append({"n": lb["name"], "s": f"load balancer | {lb.get('sku')}", "lines": lines})
    if lb_rows:
        rep.table(["Load balancer", "SKU", "Bytes", "Packets", "SNAT CONNECTIONS", "SNAT PORTS (peak)", "BACKEND HEALTH (MIN)"], lb_rows, maxw=44, title="Load balancer traffic in the window",
                  what="what passed through each Standard load balancer (bytes, packets, outbound connections), the peak share of outbound ports in use and the lowest backend probe availability.")
        rep.series(f"Load balancer traffic, last {mins} min", lb_series, "Per-minute values from Azure Monitor (Standard load balancers only).",
                   about="small line charts per load balancer of its traffic, outbound connection use and backend health, one point per minute over the window, from Azure Monitor.")
    nat_series, nat_rows = [], []
    for nm in az.get("nat_metrics", []):
        got, nat = nm["got"], nm["nat"]
        tot = lambda k: sum(v for _, v in got.get(k, []))
        nat_rows.append([nat["name"], _fmt_bytes(tot("bytes")), f"{tot('packets'):.0f}", f"{tot('drop'):.0f}", f"{tot('snat'):.0f}"])
        if tot("drop") > 0:
            ctx.find("MED", f"NAT gateway {nat['name']}: {tot('drop'):.0f} dropped packets in the window")
        nat_series.append({"n": nat["name"], "s": "network address translation gateway",
                           "lines": [_line("Bytes per minute", got.get("bytes", []), "B"), _line("Dropped packets", got.get("drop", []), "count")]})
    if nat_rows:
        rep.table(["NAT gateway", "Bytes", "Packets", "Dropped packets", "SNAT connections"], nat_rows, title="Network address translation gateway traffic in the window",
                  what="what passed through each NAT gateway in the window, with dropped packets and outbound connections.")
        rep.series(f"Network address translation gateway traffic, last {mins} min", nat_series, "",
                   about="small line charts per network address translation gateway of its traffic, dropped packets and outbound connections, one point per minute over the window, from Azure Monitor.")
    if az.get("lbs") is not None and az.get("lb_rows"):
        rep.table(["Load balancer", "SKU", "Frontends", "LB rules", "Outbound rules", "Probes", "Backend pools"], az["lb_rows"], title=f"Load balancers in the node resource group ({len(az['lb_rows'])})",
                  what="the Azure load balancers AKS created for the cluster, with the number of rules, probes and backend pools each has.")


# ---------------------------------------------------------------------------
# 11. Traffic issue checklist  +  12. Glossary
# ---------------------------------------------------------------------------

CHECKLIST = [
    ("Pod reachability evidence", ("pods_stuck", "svc_noep")),
    ("Container Network Interface logs", ("cni_logs", "cni_health", "cni_order")),
    ("Node health", ("node_cond", "node_nic")),
    ("kube-proxy (Service routing on each node)", ("kube_proxy",)),
    ("Domain Name System", ("dns_pods", "dns_conf", "dns_logs")),
    ("Network policies", ("policies",)),
    ("Cloud firewalls (security groups, routes)", ("firewalls",)),
    ("Load balancer health checks", ("lb_probes", "snat")),
    ("Packet capture availability", ("capture",)),
    ("Provider observability tools", ("observability",)),
]


def _net_checklist(rep, ctx):
    checks = ctx.data.get("net_checks", {})
    rows = []
    for name, keys in CHECKLIST:
        have = [checks[k] for k in keys if k in checks]
        if not have:
            rows.append([name, NET_NA, "this check did not run (the data it needs could not be read)", "Re-run with kubectl and Azure access."])
            continue
        status = _combine(c["status"] for c in have)
        worst = next((c for c in have if c["status"] == status), have[0])
        evidence = " | ".join(f"{c['title']}: {_short(c['evidence'], 110)}" for c in (have if status == NET_OK else [c for c in have if c["status"] == status] or have))[:420]
        nxt = worst["meaning"] if status != NET_OK else "No action needed; continue with the next check."
        rows.append([name, status, evidence, _short(nxt, 260)])
    cnt = Counter(r[1] for r in rows)
    rep.heading("Traffic issue checklist",
                "the standard list for traffic troubleshooting: for each of the 10 checks the result found in this report, the evidence and what to do next. 'Not available' always says the data could not be read - never read it as OK.")
    rep.table(["Check", "Result", "Evidence found", "What to do next"], rows, limit=20, maxw=110,
              what="the ten standard traffic checks with the result (OK, Warning, Problem or Not available), the evidence behind it and the next step.")
    rep.add(f"  Result: {cnt.get(NET_OK, 0)} OK, {cnt.get(NET_WARN, 0)} Warning, {cnt.get(NET_BAD, 0)} Problem, {cnt.get(NET_NA, 0)} Not available.")


def _net_glossary(rep, ctx):
    rep.heading("Glossary: all terms used in the network section",
                "every abbreviation and technical term used above, spelled out and explained in plain language (a short glossary also sits before each block that uses terms).")
    rep.glossary(sorted(_NET_TERMS, key=lambda k: k.lower()), title="Complete glossary", track=False)


def section_network_details(rep, ctx, label):
    rep.section(f"10. NETWORK AND TRAFFIC - PODS, NODES, SERVICES, NAME LOOKUPS, FIREWALLS, LOAD BALANCERS (last {ctx.minutes} min)", "network")
    ctx.data.setdefault("net_checks", {})
    rep.add("Read-only: this section only reads Kubernetes objects, pod logs, Azure resources and Azure Monitor. It runs no command inside pods, logs in to no node and captures no packets.")
    groups = [
        ("Pod-level networking", "how pods get IP addresses and connect: the plugin, its agents, address exhaustion, plugin logs, stuck pods and start-up order.",
         (_net_settings, _net_cni_health, _net_ip_exhaustion, _net_cni_logs, _net_stuck_pods, _net_startup_order)),
        ("Node networking", "whether the nodes themselves are healthy for networking: Ready state and pressure, interface errors, packet size settings and cloud throttling.",
         (_net_node_conditions, _net_node_nics, _net_mtu, _net_throttling)),
        (None, None, (_net_kube_proxy, _net_services)),
        (None, None, (_net_dns,)),
        (None, None, (_net_policies,)),
        (None, None, (_net_ingress,)),
        (None, None, (_net_conntrack,)),
        (None, None, (_net_observability,)),
        (None, None, (_net_control_plane,)),
    ]
    for title, what, fns in groups:
        if title:
            rep.heading(title, what)
        for fn in fns:
            if ctx.cancel is not None and ctx.cancel.is_set():
                return
            try:
                fn(rep, ctx)
            except Exception as exc:
                rep.add(f"[!] {fn.__name__} failed: {exc}")
                ctx.data.setdefault("net_errors", []).append(fn.__name__)
    rep.heading("Traffic measured in the selected window",
                f"events, kubelet counters and Azure Monitor metrics for the last {ctx.minutes} minutes: who moved how much data, load balancer and NAT gateway traffic.")
    for fn in (_net_events, _net_pod_traffic, _net_azure_traffic):
        if ctx.cancel is not None and ctx.cancel.is_set():
            return
        try:
            fn(rep, ctx)
        except Exception as exc:
            rep.add(f"[!] {fn.__name__} failed: {exc}")
    for fn in (_net_checklist, _net_glossary):
        try:
            fn(rep, ctx)
        except Exception as exc:
            rep.add(f"[!] {fn.__name__} failed: {exc}")


def section_scaling_storage_network(rep, ctx):
    rep.section("11. AUTOSCALING, STORAGE, NETWORKING", "scaling")
    rep.glossary(["HPA", "PVC", "PV", "StorageClass", "Endpoint", "LoadBalancer", "Terminating", "Namespace", "Support team"])
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
        rep.table(["Horizontal pod autoscaler", "Support team (distribution list)", "Replicas (current / desired / maximum)", "Issue"], rows,
                  title="Horizontal pod autoscalers with issues",
                  about="Autoscalers that cannot add more replicas because they reached their maximum, or that are unable to scale: the autoscaler, its owning team, the current, desired and maximum replica counts and the issue.")
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
        rep.table(["Persistent volume claim / volume", "Support team (distribution list)", "Phase", "Storage class", "Age"], pvcs + pvs,
                  title="Storage problems (persistent volume claims not Bound, persistent volumes Failed)",
                  about="Storage requests that no volume satisfies (claims that are not Bound) and volumes in the Failed phase: the name, owning team, phase, the storage class asked for and the age.")
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
        rep.table(["Service", "Support team (distribution list)", "Type", "Issue"], svc_rows,
                  title="Services with problems",
                  about="Services that cannot work yet: a LoadBalancer Service without an external address, or a Service whose selector matches no ready pod (no endpoints); with the owning team and the issue.")
        ctx.find("MED", f"{len(svc_rows)} Service(s) with no endpoints / pending LoadBalancer" + support_suffix(ctx, {r[0].split("/")[0] for r in svc_rows}))
        for r in svc_rows:
            ctx.ns_issue(r[0].split("/")[0], f"service {r[0].split('/', 1)[1]}: {r[3]}")
    else:
        rep.add("Services: all selector services have ready endpoints; no pending LoadBalancers.")

    term = [n["metadata"]["name"] for n in items(ctx.data.get("namespaces")) if n.get("status", {}).get("phase") == "Terminating"]
    if term:
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
    rep.section("4. RESOURCE UTILIZATION - PROCESSOR (CPU) AND MEMORY BY NAMESPACE", "utilization")
    data = build_utilization(ctx)
    if not data["namespaces"] and not data["nodes"]:
        rep.add("No node or pod data.")
        return
    c = data["cluster"]
    cpu_p, mem_p = _pct(c["cu"], c["ca"]), _pct(c["mu"], c["ma"])
    rep.glossary(["Allocatable", "Request", "Limit", "Namespace", "kubelet", "Metrics server"])
    rep.table(["Resource", "Used now (share of allocatable)", "Allocatable (what the nodes offer to pods)", "Requested by pods (share of allocatable)"],
              [["Processor (CPU)", f"{_cores(c['cu']) if c['cu'] is not None else 'n/a'} ({_fp(cpu_p)})", f"{c['ca']:.1f} cores", f"{c['cr']:.1f} cores ({_fp(_pct(c['cr'], c['ca']))})"],
               ["Memory", f"{fmt_gib(c['mu']) if c['mu'] is not None else 'n/a'} ({_fp(mem_p)})", fmt_gib(c['ma']), f"{fmt_gib(c['mr'])} ({_fp(_pct(c['mr'], c['ma']))})"]],
              maxw=70, title="Whole cluster",
              about="The cluster as one: how much processor and memory all pods use right now and have requested, compared with what all nodes together can offer to pods.")
    if not data["hasUsage"]:
        rep.add("Live usage is not available (needs the kubelet stats permission or metrics-server), so this section shows what pods REQUEST and are LIMITED to.")

    rep.util(data, about="an interactive dashboard of the same numbers: gauges for the whole cluster, who uses the cluster by namespace, every node with its load, an explorer of namespaces and their pods, and the top consumers.")       # the interactive dashboard comes first in the HTML; the table below is the detailed list

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
    rep.table(["Namespace", "Support team (distribution list)", "Pods", "Processor (CPU) used", "Processor (CPU) requested", "Processor (CPU) limit", "Processor (CPU) used, % of cluster",
               "Memory used", "Memory requested", "Memory limit", "Memory used, % of cluster", "Pods with high usage"], rows,
              title="Namespaces ranked by memory",
              about="One row per namespace, the biggest memory users first: processor and memory used, requested and limited, each as a share of the whole cluster, and how many of its pods are at 90 percent or more of their limit.")

    for metric, key, fmt in (("CPU", "cu", lambda v: f"{v:.2f} cores"), ("memory", "mu", _mi)):
        top = max((x for x in data["namespaces"] if x[key]), key=lambda x: x[key], default=None)
        total = c[key]
        if top and total:
            ctx.find("INFO", f"Top {metric} consumer: namespace {top['name']} ({fmt(top[key])}, "
                             f"{100 * top[key] / total:.0f}% of the cluster's {metric} use)")


def section_top(rep, ctx):
    rep.section("12. TOP RESOURCE CONSUMERS (live)", "top")
    usage = ctx.data.get("pod_usage") or {}
    if not usage:
        rep.add("No live pod usage available (needs the kubelet stats permission or metrics-server).")
        return
    pods = {(p["metadata"]["namespace"], p["metadata"]["name"]): p for p in items(ctx.data.get("pods"))}
    rep.glossary(["Limit", "Throttling", "OOMKilled", "Namespace", "Support team"])
    for label, key in (("processor (CPU)", "cpu"), ("memory", "mem")):
        ranked = sorted(((k, u) for k, u in usage.items() if u.get(key) is not None), key=lambda kv: -kv[1][key])[:10]
        rows = []
        for (ns, name), u in ranked:
            res = _pod_resources(pods[(ns, name)]) if (ns, name) in pods else {"cpu_lim": 0, "mem_lim": 0}
            lim = res["cpu_lim"] if key == "cpu" else res["mem_lim"]
            rows.append([ns, support_of(ctx, ns) or "-", name, _cores(u["cpu"]) if u.get("cpu") is not None else "n/a", _mi(u["mem"]) if u.get("mem") is not None else "n/a",
                         _mi(u["disk"]) if u.get("disk") else "-", _fp(_pct(u[key], lim)) if lim else "no limit"])
        rep.table(["Namespace", "Support team (distribution list)", "Pod", "Processor (CPU) used", "Memory used", "Disk used", ("Processor (CPU)" if key == "cpu" else "Memory") + " used, % of its limit"], rows,
                  title=f"Top 10 pods by {label}",
                  about=f"The ten pods using the most {label} right now, with their owning team, what they use in processor, memory and disk and how close the {label} use is to the pod's limit ('no limit' means the pod may take everything).")


CORE_ADDON_PREFIXES = ("coredns", "coredns-autoscaler", "azure-cns", "azure-ip-masq-agent", "azure-npm", "kube-proxy", "csi-azuredisk",
                       "csi-azurefile", "csi-blob", "metrics-server", "cloud-node-manager", "konnectivity-agent", "tunnelfront",
                       "ama-logs", "ama-metrics", "omsagent", "azure-policy", "cilium", "calico", "cluster-autoscaler", "keda",
                       "secrets-store", "aks-secrets-store", "ingress-nginx", "nginx", "external-dns", "cert-manager")
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
       3. core add-ons                (coredns, azure-cns, kube-proxy, CSI drivers, autoscalers ...)
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


def _log_jobs(targets, pods):
    jobs = []   # (namespace, pod, container, previous?, reason, restart_count)
    for (ns, name), reason in targets:
        names, by_name = _containers_to_read(pods[(ns, name)])
        for cname in names:
            restarts = by_name.get(cname, {}).get("restartCount", 0)
            jobs.append((ns, name, cname, False, reason, restarts))
            if restarts:
                jobs.append((ns, name, cname, True, reason, restarts))
    return jobs


def section_logs(rep, ctx, options=None):
    from concurrent.futures import as_completed
    options = options or {}
    scope = "unhealthy pods, pods with warning events, core add-ons" + (", ALL pods" if options.get("all_logs") else "")
    rep.section(f"13. LOGS (last {ctx.minutes} min) - {scope}", "logs")
    targets, pods = pick_log_targets(ctx, options)
    if not targets:
        rep.add("No pods qualified for log collection (nothing unhealthy, no Warning events, no core add-ons found).")
        rep.add("Turn on 'Logs of ALL pods' (or use --logs-all) to read every running pod.")
        return

    jobs = _log_jobs(targets, pods)
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
        blocks.append((title, entries, text_entries,
                       f"the {'previous (before its last restart)' if prev else 'current'} log of container {cname} in pod {ns}/{name}, collected because: {reason}; "
                       f"error-like lines are red and warnings amber, the full text is kept in the HTML (up to {LOG_TAIL_LINES} lines)."))
        if errs and reason.startswith("unhealthy"):
            ctx.find("MED", f"Error-like logs in {ns}/{name} [{cname}] {kind.split(' ')[0]}: {len(errs)} line(s), "
                            f"latest: {TIMESTAMP_PREFIX.sub('', errs[-1])[:90]}")

    rep.table(["Pod", "Support team (distribution list)", "Container", "Log", "Why collected", "Lines", "Error-like lines", "Warning lines", "Last line"], rows, maxw=90,
              title=f"Overview of {len(rows)} log stream(s)",
              about="One row per log that was read: the pod and container, whether it is the running (current) or the previous container, why it was chosen, how many lines it has, how many look like errors or warnings and the last line.")
    errs_total = sum(r[6] for r in rows)
    rep.add(f"Total: {sum(r[5] for r in rows)} line(s), {errs_total} error-like.")
    for title, entries, text_entries, about in blocks:
        rep.log(title, entries, text_entries, about=about)
    if ctx.cancel is not None and ctx.cancel.is_set():
        rep.add("(log collection was stopped early)")


def section_glossary(rep, ctx):
    """The complete glossary of the report: every term of the small glossaries above (all sections), once, sorted - plus what the
    severity and status words mean."""
    rep.section("15. GLOSSARY - every term and abbreviation used in this report (A to Z)", "glossary")
    rep.table(["Severity", "Meaning"], [list(x) for x in LEGEND_SEVERITY], maxw=130, cls="gloss", title="What the severity levels mean",
              about="the four severity levels used for findings in the health summary, from most to least serious, and what each means.")
    rep.table(["Status", "Meaning"], [list(x) for x in LEGEND_STATUS], maxw=130, cls="gloss", title="What the status words mean",
              about="the four result words used by the checks of the network section and what each means; 'Not available' is never the same as OK.")
    terms = sorted(rep.used_terms, key=lambda k: k.lower())
    rep.table(["Term", "Full name", "Plain-language meaning"], [[k, GLOSSARY[k][0], GLOSSARY[k][1]] for k in terms], limit=1000, maxw=130, cls="gloss", title="Complete glossary",
              about="every term and abbreviation that appears in the small glossaries of this report, listed once and sorted alphabetically, with its full name and a plain-language meaning.")


def section_timeline(rep, ctx):
    rep.section(f"14. TIMELINE - what happened in the last {ctx.minutes} min (oldest first)", "timeline")
    if not ctx.timeline:
        rep.add("Nothing notable recorded in this window.")
        return
    entries = sorted(set(ctx.timeline), key=lambda x: x[0])
    skipped = max(0, len(entries) - MAX_TIMELINE)
    if skipped:
        rep.add(f"({skipped} older entries not shown)")
    rep.timeline(entries[-MAX_TIMELINE:], about=f"the notable events of the last {ctx.minutes} minutes in time order (UTC, oldest first), each with a coloured kind such as POD, NODE, EVENT or ROLLOUT.")


def build_summary(ctx, label):
    order = {"CRIT": 0, "HIGH": 1, "MED": 2, "INFO": 3}
    lines = ["=" * 78, f"HEALTH SUMMARY - {label}  (last {ctx.minutes} min, {ctx.now:%Y-%m-%d %H:%M:%S} UTC)", "=" * 78,
             f"What this section shows: {EXTRA_SECTIONS['summary'][0]}", f"How to use it: {EXTRA_SECTIONS['summary'][1]}",
             "What the severity levels mean: " + " ".join(f"{n} = {m}" for n, m in LEGEND_SEVERITY) + " (in the list below: CRIT = Critical, HIGH = High, MED = Medium, INFO = Information)", ""]
    if ctx.sections_note:
        done_n, all_n, left_out = ctx.sections_note
        lines.append(f"Sections collected: {done_n} of {all_n}. Skipped by choice: {', '.join(left_out)}.")
    if not ctx.findings:
        lines.append("No problems detected in the collected data.")
    else:
        counts = Counter(s for s, _ in ctx.findings)
        lines.append("Findings: " + ", ".join(f"{counts[s]} {s}" for s in ("CRIT", "HIGH", "MED", "INFO") if counts[s]))
        for sev, text in sorted(ctx.findings, key=lambda x: order[x[0]]):
            lines.append(f"  [{sev}] {text}")
    contacts = contact_rows(ctx)
    if contacts:
        lines += ["", "TEAMS TO CONTACT (namespaces with problems, by support team distribution list):"]
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
h3.bh{margin:20px 0 4px;font-size:15px;padding-top:10px;border-top:1px solid var(--line)}h4.tt{margin:14px 0 2px;font-size:13px}
p.what{margin:2px 0 6px;color:var(--muted);font-size:12.5px}
.check{border:1px solid var(--line);border-left:5px solid var(--muted);border-radius:8px;padding:8px 12px;margin:14px 0 8px;background:var(--card)}
.check p{margin:4px 0}.check p.what{margin:2px 0 4px}.chead{display:flex;justify-content:space-between;gap:10px;align-items:center}.chead h4{margin:0;font-size:14px}
.stbadge{font-size:11px;font-weight:700;border-radius:5px;padding:1px 8px;white-space:nowrap}
.st-ok{border-left-color:var(--good)}.stbadge.st-ok{background:var(--goodbg);color:var(--good)}
.st-warn{border-left-color:#e8a317}.stbadge.st-warn{background:var(--medbg);color:var(--med)}
.st-bad{border-left-color:var(--crit)}.stbadge.st-bad{background:var(--critbg);color:var(--crit)}
.st-na{border-left-color:var(--muted)}.stbadge.st-na{background:var(--code);color:var(--muted)}
.tablewrap.gloss{font-size:12px;opacity:.95}
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
const BAD=/^(NotReady|CrashLoopBackOff|Error|Failed|Evicted|ImagePullBackOff|ErrImagePull|OOMKilled|DEGRADED|CREATE_FAILED|FAILED|PROBLEM|impaired|CRIT|Unknown|MISSING|FAILING|Problem|Expired)/i;
const WARN=/^(Pending|Terminating|Ready,SchedulingDisabled|SchedulingDisabled|UPDATING|CREATING|low IPs|VERY LOW|HIGH|AT MAX|insufficient|NEW node|Warning|PRESSURE)/i;
const GOOD=/^(Ready|Running|ACTIVE|OK|ok|Bound|Succeeded|Completed|available)$/;
$$('table.data').forEach(tb=>{
 const heads=$$('th',tb);const rows=$$('tbody tr',tb);
 rows.forEach(tr=>$$('td',tr).forEach((td,i)=>{
  const t=td.textContent.trim(),h=(heads[i]?heads[i].textContent:'');
  if(tb.id!=='findings'){
   if(BAD.test(t)||/MISSING|NOT READY|DEGRADED|FAILING|PROBLEM|impaired/.test(t))td.classList.add('bad');
   else if(WARN.test(t)||/HIGH REQUESTS|AT MAX|low IPs|SWAP in use|MEM \d+% of limit/.test(t))td.classList.add('warn');
   else if(GOOD.test(t))td.classList.add('good');
   const p=t.match(/\((\d+)%\)\s*$/)||(/CPU|MEM|DISK|IMAGEFS|SWAP|req|Processor|Memory|Disk|Swap|storage|percent|%/i.test(h)&&t.match(/^(\d+)%$/));
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
    return ("""
:root{--accent:%(p)s;--brand:%(p)s;--brand2:%(a)s;--branddark:%(d)s;--brandpale:%(pale)s}
[data-theme=dark]{--accent:%(a)s;--brandpale:#12263a}
header{padding:0!important}
.band{position:relative;overflow:hidden;display:flex;align-items:center;gap:14px;padding:10px 18px;background:linear-gradient(100deg,%(d)s 0%%,%(p)s 62%%,%(a)s 135%%);color:#fff}
.band .logo{flex:none;filter:drop-shadow(0 1px 2px rgba(0,0,0,.25))}.band h1{color:#fff;margin:0;font-size:19px;letter-spacing:.2px}
.band .meta{color:rgba(255,255,255,.88)}.band .txt{position:relative;z-index:1;min-width:0}
.band .deco{position:absolute;right:-10px;top:-14px;height:90px;opacity:.16;pointer-events:none}
.band .deco2{right:210px;top:20px;height:60px;opacity:.12}
.band .tag{position:relative;z-index:1;margin-left:auto;background:rgba(255,255,255,.18);border:1px solid rgba(255,255,255,.35);border-radius:14px;padding:3px 12px;font-size:12px;white-space:nowrap}
.tools{padding:8px 18px 10px;border-bottom:3px solid var(--brand2)}.tools .toolbar{margin-top:0}
nav{top:132px!important;max-height:calc(100vh - 148px)!important}
button:hover{border-color:var(--brand);color:var(--brand)}
#expand,#collapse,#theme,#print,#dl{background:var(--brandpale)}
.chip.on,.sevcard.on{background:var(--brand);border-color:var(--brand)}
details.sec>summary{border-left:4px solid var(--brand)}details.sec>summary .ico{margin-right:8px}
details.sec.skipped>summary{border-left-color:var(--muted);color:var(--muted);font-weight:500;font-size:13.5px}
nav a.skip{color:var(--muted);font-size:12px}nav a .ico{margin-right:6px}
.timing{background:var(--brandpale);border:1px solid var(--line);border-radius:10px;padding:8px 14px;margin:8px 0}
footer b{color:var(--brand)}
details.sec>summary>span:first-of-type{margin-right:auto}
@media(max-width:760px){.band{padding:8px 12px;gap:10px}.band h1{font-size:15px}.band .logo{width:44px;height:28px}.band .tag,.band .deco{display:none}nav{top:auto!important}header{position:static!important}}
""" % {"p": BRAND_PRIMARY, "a": BRAND_ACCENT, "d": BRAND_DARK, "pale": BRAND_PALE})


def _section_icon(sec):
    """The emoji for a report section: found by the number in its title ('3. NODES ...') or by the registry id of a skipped one."""
    m = re.match(r"(\d+)\.", sec.get("title", ""))
    reg = SECTIONS[int(m.group(1)) - 1] if m and 0 < int(m.group(1)) <= len(SECTIONS) else SECTION_BY_ID.get(sec.get("sid") or "")
    return ICONS.get(reg["icon"], ("", ""))[0] if reg else ""


_SEV_ORDER = {"CRIT": 0, "HIGH": 1, "MED": 2, "INFO": 3}
_SEV_NAME = {"CRIT": "Critical", "HIGH": "High", "MED": "Medium", "INFO": "Information"}      # what the HTML shows (the data keeps the short keys)


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
if(!D.hasUsage)root.appendChild(el('div','note','<b>Live processor (CPU) and memory use is not available</b> (it needs the kubelet stats permission or the metrics server). Everything below shows what pods <b>request</b> and are <b>limited</b> to instead of what they use right now.'));

// ---- cluster tiles
function tile(label,use,total,fmt,req){
  const p=pct(use,total),rp=pct(req,total),t=el('div','tile');
  t.innerHTML='<div class="tl">'+label+'</div><div class="tv">'+(use==null?'n/a':fmt(use))+' <small>of '+fmt(total)+' allocatable ('+fPct(p)+')</small></div>'
   +'<div class="gauge"><i class="g '+sev(p)+'" style="width:'+Math.min(100,p||0)+'%"></i>'+(rp!=null?'<i class="m" style="left:'+Math.min(100,rp)+'%" title="requested"></i>':'')+'</div>'
   +'<div class="ts">requested '+fmt(req)+' ('+fPct(rp)+') &nbsp;|&nbsp; the tick marks the requested amount</div>';
  return t;}
root.appendChild(el('p','what','<b>What this block shows:</b> gauges for the whole cluster: how much processor, memory and pod slots are used (the filled bar) and requested (the tick mark) compared with what all nodes offer. Green is below 75 percent, amber from 75 and red from 90 percent.'));
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
  const wrap=el('div');wrap.appendChild(el('h3',null,title+(D.hasUsage?' (used)':' (requested)')));wrap.appendChild(el('p','what','<b>What this block shows:</b> one bar split by namespace: each colour is a namespace and its width is its share of what all pods use (or request when live use is unknown); hover a colour for the numbers.'));
  const bar=el('div','stack'),leg=el('div','legend');
  top.forEach(n=>{const s=el('span');s.style.width=(100*n[useK]/sum)+'%';s.style.background=nsColor(n.name);s.title=n.name+': '+fmt(n[useK])+' ('+Math.round(100*n[useK]/sum)+'% of what pods use; '+fPct(pct(n[useK],total))+' of allocatable)';bar.appendChild(s);
    leg.appendChild(el('span',null,'<b style="background:'+nsColor(n.name)+'"></b>'+esc(n.name)+' '+fmt(n[useK])+' ('+Math.round(100*n[useK]/sum)+'%)'));});
  if(rest){const s=el('span');s.style.width=(100*rest/sum)+'%';s.style.background='#98a2b3';s.title='other namespaces: '+fmt(rest);bar.appendChild(s);leg.appendChild(el('span',null,'<b style="background:#98a2b3"></b>others '+fmt(rest)));}
  wrap.appendChild(bar);wrap.appendChild(leg);root.appendChild(wrap);}
stacked('Processor (CPU) share by namespace','cu','cr',fCpu,C.ca);
stacked('Memory share by namespace','mu','mr',fMem,C.ma);

// ---- nodes
root.appendChild(el('h3',null,'Nodes (sorted by the most loaded)'));root.appendChild(el('p','what','<b>What this block shows:</b> one card per node with gauges for processor, memory, disk and pod slots, the machine behind it and its status; cards with a red border are 90 percent full or more and amber ones 75 percent or more.'));
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
    +'<div class="nid"><b>'+esc(n.id||'-')+'</b> &middot; '+esc(n.zone||'-')+' &middot; '+esc(n.type||'-')+(n.ec2&&n.ec2!=='-'?' &middot; virtual machine: '+esc(n.ec2):'')+'</div>';
  c.appendChild(nrow('Processor (CPU)',n.cu,n.ca,fCpu,n.cr));c.appendChild(nrow('Memory',n.mu,n.ma,fMem,n.mr));
  if(n.du!=null)c.appendChild(nrow('Disk',n.du,n.dc,fMem));
  c.appendChild(nrow('Pods',n.pods,n.mp,x=>String(Math.round(x))));
  if(n.su)c.appendChild(el('div','nv','<span class="pill c">swap in use '+fMem(n.su)+'</span>'));
  grid.appendChild(c);});
root.appendChild(grid);

// ---- namespace explorer
root.appendChild(el('h3',null,'By namespace (click a namespace to see its pods)'));root.appendChild(el('p','what','<b>What this block shows:</b> one row per namespace with a bar of what its pods use or request and the support team to contact; the tick marks show the requested and limit totals, and clicking a row lists its pods with their own bars.'));
const S={metric:'mem',mode:D.hasUsage?'use':'req',sort:'use',high:false,q:''};
const KEY={cpu:{use:'cu',req:'cr',lim:'cl',nolim:'ncl',fmt:fCpu,tot:C.ca,name:'Processor (CPU)'},mem:{use:'mu',req:'mr',lim:'ml',nolim:'nml',fmt:fMem,tot:C.ma,name:'Memory'},disk:{use:'du',req:null,lim:null,fmt:fMem,tot:null,name:'Disk'}};
const ctl=el('div','ctl');
ctl.innerHTML='<span class="seg" id="u-metric"><button data-v="cpu">Processor (CPU)</button><button data-v="mem" class="on">Memory</button><button data-v="disk">Disk</button></span>'
 +'<span class="seg" id="u-mode"><button data-v="use"'+(D.hasUsage?' class="on"':' disabled title="no live usage"')+'>Used</button><button data-v="req"'+(D.hasUsage?'':' class="on"')+'>Requested</button></span>'
 +'<label>Sort <select id="u-sort"><option value="use">highest value</option><option value="limpct">closest to limit (worst pod)</option><option value="share">share of cluster</option><option value="rs">most restarts</option><option value="name">name</option></select></label>'
 +'<label><input type="checkbox" id="u-high"> only namespaces with a pod at '+WARN+'%+ of its limit</label><input type="search" id="u-q" placeholder="Filter namespace or support team...">';
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
root.appendChild(el('h3',null,'Top consumers (pods)'));root.appendChild(el('p','what','<b>What this block shows:</b> the pods that use the most (or request the most, when live use is unknown) of each resource, with their share of the limit they were given; amber and red bars mean the pod is close to its limit.'));
const all=[];const DLMAP={};D.namespaces.forEach(n=>{DLMAP[n.name]=n.dl;n.pl.forEach(p=>all.push(Object.assign({ns:n.name},p)));});
const tops=el('div','toplists');
function toplist(title,valFn,fmt,sevFn,subFn,about){
  const items=all.map(p=>({p:p,v:valFn(p)})).filter(x=>x.v!=null&&x.v>0).sort((a,b)=>b.v-a.v).slice(0,12),box=el('div','toplist');
  box.appendChild(el('h4',null,title));box.appendChild(el('p','what','<b>What this block shows:</b> '+about));if(!items.length){box.appendChild(el('div','small','no data'));tops.appendChild(box);return;}
  const mx=items[0].v;items.forEach(x=>{const r=el('div','tr'),s=sevFn?sevFn(x.p):'';
    r.innerHTML='<span class="nm" title="'+esc(x.p.ns+'/'+x.p.n+(DLMAP[x.p.ns]?'  -  support: '+DLMAP[x.p.ns]:''))+'"><b style="display:inline-block;width:8px;height:8px;border-radius:2px;background:'+nsColor(x.p.ns)+';margin-right:5px"></b>'+esc(x.p.ns)+'/'+esc(x.p.n)+'</span>'
     +'<div class="ub '+s+'"><i class="f" style="width:'+(100*x.v/mx)+'%"></i></div><span>'+fmt(x.v)+(subFn?'<br><small class="small">'+subFn(x.p)+'</small>':'')+'</span>';box.appendChild(r);});
  tops.appendChild(box);}
if(D.hasUsage){
  toplist('Top processor (CPU) use',p=>p.cu,fCpu,p=>sev(pct(p.cu,p.cl)),p=>p.cl?fPct(pct(p.cu,p.cl))+' of limit':'no limit','the twelve pods using the most processor right now, with their share of the processor limit.');
  toplist('Top memory use',p=>p.mu,fMem,p=>sev(pct(p.mu,p.ml)),p=>p.ml?fPct(pct(p.mu,p.ml))+' of limit':'no limit','the twelve pods using the most memory right now, with their share of the memory limit.');
  toplist('Closest to the memory limit (risk of an out-of-memory kill)',p=>pct(p.mu,p.ml),fPct,p=>sev(pct(p.mu,p.ml)),p=>fMem(p.mu)+' / '+fMem(p.ml),'the pods whose memory use is the largest share of their memory limit; at 100 percent the system stops the container (out-of-memory kill).');
  toplist('Closest to the processor (CPU) limit (risk of being slowed down)',p=>pct(p.cu,p.cl),fPct,p=>sev(pct(p.cu,p.cl)),p=>fCpu(p.cu)+' / '+fCpu(p.cl),'the pods whose processor use is the largest share of their processor limit; at the limit they are slowed down (throttled).');
  toplist('Using more memory than requested',p=>(p.mu!=null&&p.mr)?pct(p.mu,p.mr):null,fPct,null,p=>fMem(p.mu)+' vs requested '+fMem(p.mr),'the pods using the most memory compared with what they asked to reserve; they can be evicted first when the node runs short.');
}else{
  toplist('Largest memory requests',p=>p.mr,fMem,null,null,'the pods that reserved the most memory (live use is not available, so requests are shown instead).');toplist('Largest processor (CPU) requests',p=>p.cr,fCpu,null,null,'the pods that reserved the most processor (live use is not available, so requests are shown instead).');
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


_ABOUT_CSS = r"""
.about{background:var(--brandpale);border:1px solid var(--line);border-left:4px solid var(--brand);border-radius:8px;padding:8px 14px;margin:10px 0 6px}
.about p{margin:4px 0}.about p.how{color:var(--muted)}.about b:first-child{color:var(--brand)}
p.what.missing{color:var(--crit)}th[title]{text-decoration:underline dotted var(--muted);text-underline-offset:3px}
.legendrow{display:flex;flex-wrap:wrap;gap:12px;margin:6px 0}.legendrow>div{flex:1 1 360px;min-width:0}
.toc-note{margin:2px 0 8px}
"""


def _th(h):
    """A table header cell; the tooltip says what the column means when the name alone is not obvious."""
    tip = COLUMN_HELP.get(str(h).strip().lower())
    return ('<th title="%s">%s</th>' % (_html.escape(tip, quote=True), _html.escape(h))) if tip else "<th>%s</th>" % _html.escape(h)


def _what_html(about, kind="table"):
    """The 'What this table shows' / 'What this block shows' line above a table or block; a missing description is shown loudly (and caught by the tests)."""
    if about:
        return '<p class="what"><b>What this %s shows:</b> %s</p>' % (kind, _html.escape(about))
    return '<p class="what missing"><b>What this %s shows:</b> (no description - this is a defect of the report)</p>' % kind


def _box_html(sid):
    """The 'What this section shows' box under a section heading (from the SECTIONS registry); '' for an unknown id."""
    info = section_text(sid)
    if not info:
        return ""
    return ('<div class="about"><p><b>What this section shows</b></p><p>%s</p><p class="how"><b>How to use it:</b> %s</p></div>'
            % (_html.escape(info[0]), _html.escape(info[1])))


def _html_table(headers, rows, about=None, cls="", title=None, filters=True):
    esc = _html.escape
    head = "".join(_th(h) for h in headers)
    body = "".join("<tr>" + "".join("<td>%s</td>" % esc(str(c)) for c in r) + "</tr>" for r in rows)
    tools = ('<input class="tfilter" type="search" placeholder="Filter rows..."><span class="tcount"></span><button class="csv" type="button">CSV</button>' if filters
             else '<span class="tcount"></span><input class="tfilter" style="display:none"><button class="csv" style="display:none">CSV</button>')
    return ((('<h4 class="tt">%s</h4>' % esc(title)) if title else "") + _what_html(about)
            + '<div class="tablewrap %s"><div class="tbtools">%s</div>'
              '<div class="tscroll"><table class="data"><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div></div>' % (esc(cls), tools, head, body))


def _legend_html():
    """The two small legend tables: what the severity levels and the status words mean."""
    return ('<div class="legendrow"><div>%s</div><div>%s</div></div>'
            % (_html_table(["Severity", "Meaning"], [list(x) for x in LEGEND_SEVERITY], cls="gloss", title="What the severity levels mean", filters=False,
                           about="the four severity levels used for the findings below, from most to least serious, and what each one means."),
               _html_table(["Status", "Meaning"], [list(x) for x in LEGEND_STATUS], cls="gloss", title="What the status words mean", filters=False,
                           about="the four result words used by the checks of the network section and what each one means; 'Not available' is never the same as OK.")))


_ACRONYMS = ("Azure", "Kubernetes", "CPU")


def _short_title(title):
    """'3. NODES - STATUS, CPU, ...' -> 'Nodes'; 'CLUSTER OVERVIEW - prod-eks' -> 'Cluster overview'."""
    t = re.sub(r"^\d+\.\s*", "", title).split(" (")[0].split(" - ")[0].strip()
    if t.isupper():
        t = t.capitalize()
        for word in _ACRONYMS:
            t = re.sub(r"\b%s\b" % word, word, t, flags=re.I)
    return t


_DECO_CLOUDS = ('<svg class="deco" viewBox="0 0 100 64" aria-hidden="true"><g fill="#fff"><ellipse cx="25" cy="46" rx="21" ry="16"/><ellipse cx="45" cy="30" rx="23" ry="23"/>'
                '<ellipse cx="70" cy="41" rx="24" ry="21"/><rect x="24" y="38" width="48" height="24"/></g></svg>'
                '<svg class="deco deco2" viewBox="0 0 100 64" aria-hidden="true"><g fill="#fff"><ellipse cx="25" cy="46" rx="21" ry="16"/><ellipse cx="45" cy="30" rx="23" ry="23"/>'
                '<ellipse cx="70" cy="41" rx="24" ry="21"/><rect x="24" y="38" width="48" height="24"/></g></svg>')


def render_html(label, ctx, rep, steps_log, raw_text, timing_lines=None):
    """The whole report as ONE self-contained interactive HTML page (collapsible sections,
    sortable/filterable tables, severity filters, global search, timeline filters, dark mode)."""
    esc = _html.escape
    findings = sorted(ctx.findings_full, key=lambda f: _SEV_ORDER.get(f[0], 9))
    counts = Counter(f[0] for f in findings)
    sec_titles = {s["id"]: s["title"] for s in rep.sections}
    sec_titles.setdefault("readonly", "Read-only guarantee")
    per_section = defaultdict(Counter)
    for sev, _, sid in findings:
        per_section[sid][sev] += 1

    # --- navigation
    nav = [f'<a href="#summary" class="jump"><span><span class="ico">{ICONS["list"][0]}</span>Health summary</span></a>']
    for s in rep.sections:
        if s["id"] == "s0" or not s["blocks"]:
            continue
        if s.get("skipped"):
            nav.append('<a href="#%s" class="jump skip"><span><span class="ico">%s</span>%s</span></a>' % (s["id"], ICONS["skip"][0], esc(s["title"])))
            continue
        worst = next((sv for sv in ("CRIT", "HIGH", "MED", "INFO") if per_section[s["id"]][sv]), None)
        badge = f'<span class="badge b-{worst.lower()}">{sum(per_section[s["id"]].values())}</span>' if worst else ""
        nav.append('<a href="#%s" class="jump"><span><span class="ico">%s</span>%s</span>%s</a>' % (s["id"], _section_icon(s), esc(_short_title(s["title"])), badge))

    # --- summary
    cards = []
    for sev, cls in (("CRIT", "c-crit"), ("HIGH", "c-high"), ("MED", "c-med"), ("INFO", "c-info")):
        cards.append(f'<div class="sevcard {cls}" data-sev="{sev}" title="click to show/hide"><b>{counts[sev]}</b>{ICONS[sev.lower()][0]} {_SEV_NAME[sev]}</div>')
    rows = []
    for sev, text, sid in findings:
        where = '<a class="jump" href="#%s">%s</a>' % (sid, esc(_short_title(sec_titles.get(sid, ""))))
        rows.append('<tr data-sev="%s"><td><span class="sevtag badge b-%s">%s</span></td><td>%s</td><td>%s</td></tr>'
                    % (sev, sev.lower(), _SEV_NAME[sev], esc(text), where))
    if findings:
        summary_html = (f'<div class="cards">{"".join(cards)}</div>'
                        + _what_html("every problem found in this run, one row per finding, most serious first; click a severity card above to hide or show a level, and a link in the Where column to jump to the section that explains it.")
                        + f'<div class="tablewrap"><div class="tbtools"><span class="tcount"></span></div>'
                        f'<div class="tscroll"><table class="data" id="findings"><thead><tr>{_th("Severity")}{_th("Finding")}{_th("Where")}</tr></thead>'
                        f'<tbody>{"".join(rows)}</tbody></table></div></div>')
        # the findings table has no per-table filter/CSV; give it the same hooks, hidden
        summary_html = summary_html.replace('<span class="tcount"></span>', '<span class="tcount"></span><input class="tfilter" style="display:none"><button class="csv" style="display:none">CSV</button>')
    else:
        summary_html = f'<div class="cards"><div class="sevcard c-ok"><b>{ICONS["ok"][0]} OK</b>No problems detected in the collected data</div></div>'

    if ctx.sections_note:
        summary_html = ('<p class="small">Sections collected: %d of %d. Skipped by choice: %s.</p>' % (ctx.sections_note[0], ctx.sections_note[1], esc(", ".join(ctx.sections_note[2])))) + summary_html
    contacts = contact_rows(ctx)
    if contacts:
        summary_html += ('<h3 style="margin:18px 0 6px;font-size:14px">Teams to contact - namespaces with problems, grouped by support team distribution list ('
                         + esc(SUPPORT_LABEL) + ')</h3>'
                         + _html_table(["Support team (distribution list)", "Namespaces", "Number of issues", "Issues in short"], contacts,
                                       about="one row per support team: the namespaces of that team that have problems, how many problems there are and a short list of them, so you know who to call."))
    summary_html += _legend_html()

    # --- sections
    def render_block(block, prev=None):
        kind = block[0]
        if kind == "about":
            return _what_html(block[1], "block")
        if kind in ("log", "series", "util", "timeline") and prev != "about":
            return _what_html(None, "block") + render_block(block, "about")
        if kind == "lines":
            lines = block[1]
            if not any(l.strip() for l in lines):
                return ""
            spans = "".join(f'<span class="ln {_line_class(l)}">{esc(l)}</span>\n' for l in lines)
            return f'<pre class="lines">{spans}</pre>'
        if kind == "heading":
            _, title, what = block
            return f'<h3 class="bh">{esc(title)}</h3>' + _what_html(what, "block")
        if kind == "check":
            _, title, status, what, evidence, meaning = block
            cls = {"OK": "ok", "Warning": "warn", "Problem": "bad", "Not available": "na"}.get(status, "na")
            return (f'<div class="check st-{cls}"><div class="chead"><h4>{esc(title)}</h4><span class="stbadge st-{cls}">{esc(status)}</span></div>'
                    f'<p class="what"><b>What this check looks at:</b> {esc(what)}</p>'
                    f'<p><b>Evidence:</b> {esc(evidence)}</p><p><b>What this means / what to do next:</b> {esc(meaning)}</p></div>')
        if kind == "table":
            headers, trs = block[1], block[2]
            what, title, cls = (list(block[3:]) + [None, None, ""])[:3]
            head = "".join(_th(h) for h in headers)
            body = "".join("<tr>" + "".join(f"<td>{esc(c)}</td>" for c in r) + "</tr>" for r in trs)
            return ((f'<h4 class="tt">{esc(title)}</h4>' if title else "")
                    + _what_html(what)
                    + f'<div class="tablewrap {esc(cls or "")}"><div class="tbtools"><input class="tfilter" type="search" placeholder="Filter rows...">'
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
        if s.get("skipped"):
            sections_html.append(f'<details class="sec skipped" id="{s["id"]}"><summary><span><span class="ico">{ICONS["skip"][0]}</span>{esc(s["title"])}</span></summary></details>')
            continue
        worst = next((sv for sv in ("CRIT", "HIGH", "MED", "INFO") if per_section[s["id"]][sv]), None)
        badge = f'<span class="badge b-{worst.lower()}">{sum(per_section[s["id"]].values())} finding(s)</span>' if worst else ""
        body = "".join(render_block(b, s["blocks"][i - 1][0] if i else None) for i, b in enumerate(s["blocks"]))
        sections_html.append(f'<details class="sec" id="{s["id"]}" open><summary><span><span class="ico">{_section_icon(s)}</span>{esc(s["title"])}</span>{badge}</summary>'
                             f'<div class="secbody">{_box_html(s.get("sid"))}{body}</div></details>')

    run_rows = "".join(f"<tr><td>{esc(t)}</td><td>{esc(st)}</td><td>{esc(sec)}</td></tr>" for t, st, sec in steps_log)
    steps_html = (f'<details class="sec" id="steps"><summary><span>Collection steps</span></summary><div class="secbody">{_box_html("steps")}'
                  + _what_html("one row per step of this run (the shared cluster data load and one step per section): whether it finished, failed or was skipped and how long it took.")
                  + f'<div class="tablewrap"><div class="tbtools"><span class="tcount"></span><input class="tfilter" style="display:none"><button class="csv" style="display:none">CSV</button></div>'
                  f'<div class="tscroll"><table class="data"><thead><tr>{_th("Step")}{_th("Result")}{_th("Time")}</tr></thead><tbody>{run_rows}</tbody></table></div></div></div></details>')

    timing_html = ""
    if timing_lines:
        timing_html = ('<details class="sec" id="timing"><summary><span><span class="ico">%s</span>Run timing</span></summary><div class="secbody">%s%s<div class="timing"><pre class="lines">%s</pre></div></div></details>'
                       % (ICONS["speed"][0], _box_html("timing"), _what_html("the total time of the run, how the parallel collection was used and the time of every step.", "block"), esc("\n".join(timing_lines))))
    total_txt = (timing_lines[2] if timing_lines and len(timing_lines) > 2 else "")
    ro = ctx.readonly or {"reads": 0, "local": 0, "blocked": []}
    ro_text = guard_lines(ro)
    ro_html = ('<details class="sec" id="readonly"%s><summary><span><span class="ico">%s</span>Read-only guarantee</span>'
               '<span class="badge b-%s">%d read calls, %d blocked</span></summary><div class="secbody">%s%s<div class="timing"><pre class="lines">%s</pre></div></div></details>'
               % (" open" if ro["blocked"] else "", ICONS["ok"][0], "crit" if ro["blocked"] else "info", ro["reads"], len(ro["blocked"]), _box_html("readonly"),
                  _what_html("the number of read-only commands this run used, the number refused by the safety guard and the rules the guard enforces; a refused command would be listed here in red.", "block"),
                  esc("\n".join(ro_text))))
    nav.append('<a href="#readonly" class="jump"><span><span class="ico">%s</span>Read-only guarantee</span></a>' % ICONS["ok"][0])
    meta = [f"Context: {ctx.meta.get('context', '?')}", f"Server: {ctx.meta.get('server', '?')}",
            f"Window: last {ctx.minutes} min", f"Generated: {ctx.now:%Y-%m-%d %H:%M:%S} UTC"]
    raw = raw_text.replace("</script", "<\\/script")
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AKS debug - {esc(label)}</title><style>{_HTML_CSS}{_UTIL_CSS}{_SERIES_CSS}{_brand_css()}{_ABOUT_CSS}</style></head><body>
<header><div class="band">{logo_svg(64)}<div class="txt"><h1>{esc(PRODUCT_NAME)} report - {esc(label)}</h1><div class="meta">{esc("  |  ".join(meta))}</div></div>
<span class="tag">{ICONS["cloud"][0]} {esc(CLOUD_NAME)}</span>{_DECO_CLOUDS}</div>
<div class="tools"><div class="toolbar"><input id="q" type="search" placeholder="Search everything ( press / )"><span id="hits" class="small"></span>
<button id="expand" type="button">Expand all</button><button id="collapse" type="button">Collapse all</button>
<button id="theme" type="button">Dark / light</button><button id="print" type="button">Print</button><button id="dl" type="button">Download .txt</button></div></div></header>
<div class="layout"><nav>{"".join(nav)}</nav><main>
<details class="sec" id="summary" open><summary><span><span class="ico">{ICONS["list"][0]}</span>Health summary</span><span class="badge b-info">{len(findings)} finding(s)</span></summary><div class="secbody">{_box_html("summary")}{summary_html}</div></details>
{"".join(sections_html)}{steps_html}{ro_html}{timing_html}</main></div>
<footer><b>{ICONS["cloud"][0]} {esc(CLOUD_NAME)}</b> - generated by aks_debug.py - read-only data. {esc(READ_ONLY_STATEMENT)} Commands used: {ro["reads"]} read calls, {len(ro["blocked"])} blocked. {esc(total_txt)} Pod logs may contain sensitive information.</footer>
<script type="text/plain" id="rawtext">{raw}</script><script>{_HTML_JS}</script><script>{_UTIL_JS}</script><script>{_SERIES_JS}</script></body></html>"""
    return page


# ---------------------------------------------------------------------------
# Parallel collection (after the login)
#
# Design: the report sections still run one after another and write the report in the same fixed order (so the report is
# deterministic and identical to a sequential run). What runs in parallel is the slow READING: when a run starts, a plan of
# all the read-only kubectl / az calls the selected sections will make is handed to a thread pool. Every call goes through a
# thread-safe single-flight cache keyed by the exact command, so a section that asks for something that is already fetched (or in
# flight) gets it immediately, and nothing is ever fetched twice. At most KUBECTL_CONCURRENCY kubectl and AZ_CONCURRENCY az calls
# run at the same moment. A call nobody planned simply runs when the section asks for it (as in a sequential run).
# --workers 1 turns all of this off (the exact old behaviour).
# ---------------------------------------------------------------------------

_PF = None            # the running Prefetcher (None = sequential)


class _Entry:
    __slots__ = ("event", "res", "exc")

    def __init__(self):
        self.event = threading.Event()
        self.res = None
        self.exc = None


class _Latch:
    """Calls fn() once, after hit() was called n times."""

    def __init__(self, n, fn):
        self.n, self.fn, self.lock = n, fn, threading.Lock()
        if n <= 0:
            fn()

    def hit(self, *_a):
        with self.lock:
            self.n -= 1
            fire = self.n == 0
        if fire:
            self.fn()


class Prefetcher:
    def __init__(self, workers, cancel=None, on_progress=None):
        self.workers = max(2, int(workers))
        self.cancel, self.on_progress = cancel, on_progress
        self.pool = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="collect")
        self.lock = threading.Lock()
        self.entries = {}
        self.k_sem, self.az_sem = threading.BoundedSemaphore(KUBECTL_CONCURRENCY), threading.BoundedSemaphore(AZ_CONCURRENCY)
        self.total = self.done = 0
        self.errors = []
        self.calls = self.hits = 0
        self.call_seconds = 0.0
        self.running, self.peak = {"k": 0, "az": 0}, {"k": 0, "az": 0}
        self.misses = []            # calls a section made before the plan had got to them
        self.task_keys = set()      # every call the plan asked for
        self.closed = False
        self.data = {}              # kubectl objects read by the plan (what later planning steps look at)
        self.tl = threading.local()
        self.started = time.time()
        self.stats_ts = None
        self.traffic, self.traffic_started, self.traffic_evt = None, False, threading.Event()
        self.orig_kubectl = self.orig_az = self.w_kubectl = self.w_az = None

    # ---- state
    def cancelled(self):
        return self.closed or (self.cancel is not None and self.cancel.is_set())

    # ---- the single-flight cache
    def call(self, kind, key, fn):
        with self.lock:
            ent = self.entries.get(key)
            owner = ent is None
            if owner:
                ent = self.entries[key] = _Entry()
        if getattr(self.tl, "in_task", False):
            self.task_keys.add(key)
        if owner:
            if not getattr(self.tl, "in_task", False):
                self.misses.append(key)
            if self.cancelled():
                res, exc = ((False, "cancelled") if kind == "k" else (None, "cancelled")), None
            else:
                with (self.k_sem if kind == "k" else self.az_sem):
                    with self.lock:
                        self.running[kind] += 1
                        self.peak[kind] = max(self.peak[kind], self.running[kind])
                    t = time.time()
                    try:
                        res, exc = fn(), None
                    except Exception as e:      # kept, and raised again for everybody who asked for this call
                        res, exc = None, e
                    with self.lock:
                        self.running[kind] -= 1
                        self.calls += 1
                        self.call_seconds += time.time() - t
            ent.res, ent.exc = (copy.deepcopy(res) if kind == "az" else res), exc
            ent.event.set()
            if exc is not None:
                raise exc
            return res
        while not ent.event.wait(0.2):
            if self.cancelled():
                return (False, "cancelled") if kind == "k" else (None, "cancelled")
        with self.lock:
            self.hits += 1
        if ent.exc is not None:
            raise ent.exc
        return copy.deepcopy(ent.res) if kind == "az" else ent.res

    def fresh_kubectl(self, args, timeout=KUBECTL_TIMEOUT):
        """A kubectl call that must NOT come from the cache (live sampling), still limited by the kubectl cap."""
        with self.k_sem:
            return self.orig_kubectl(args, timeout)

    # ---- tasks
    def go(self, fn, *args, **kwargs):
        if self.cancelled():
            return
        with self.lock:
            self.total += 1
        try:
            self.pool.submit(self._task, fn, args, kwargs)
        except RuntimeError:        # pool already closed
            with self.lock:
                self.total -= 1

    def _task(self, fn, args, kwargs):
        self.tl.in_task = True
        try:
            if not self.cancelled():
                fn(*args, **kwargs)
        except Exception as exc:    # one failing task never stops the others; the section that needs the data reports the failure itself
            self.errors.append(f"{getattr(fn, '__name__', 'task')}: {exc}")
        finally:
            self.tl.in_task = False
            with self.lock:
                self.done += 1
                done, total = self.done, self.total
            if self.on_progress:
                try:
                    self.on_progress(done, total)
                except Exception:
                    pass

    def k(self, args, timeout=KUBECTL_TIMEOUT, then=None):
        def run():
            r = kubectl(args, timeout)
            if then:
                then(*r)
        self.go(run)

    def kj(self, name, args, then=None):
        def run():
            data, err = kjson(args)
            self.data[name] = data
            if then:
                then(name, data, err)
        self.go(run)

    def az(self, args, target, timeout=90, subscription=True, then=None):
        def run():
            r = az_cli(args, target, timeout, subscription)
            if then:
                then(*r)
        self.go(run)

    # ---- live traffic sample, started in the background so nothing waits for it
    def start_sampler(self, names):
        self.traffic_started = True

        def work():
            wait = self.stats_ts + TRAFFIC_SAMPLE_SECONDS - time.time()
            if wait > 0:
                (self.cancel.wait(wait) if self.cancel is not None else time.sleep(wait))
            if self.cancelled():
                self.traffic_evt.set()
                return
            fresh = {}

            def fin():
                self.traffic = (self.stats_ts, time.time(), {n: fresh[n] for n in names if n in fresh})      # node order, like a sequential run
                self.traffic_evt.set()
            latch = _Latch(len(names), fin)

            def one(node):
                try:
                    ok, out = self.fresh_kubectl(["get", "--raw", f"/api/v1/nodes/{node}/proxy/stats/summary"], 60)
                    if ok:
                        fresh[node] = json.loads(out)
                except Exception:
                    pass
                finally:
                    latch.hit()
            for n in names:
                self.go(one, n)
        threading.Thread(target=work, daemon=True).start()

    def traffic_sample(self):
        """(time of the first sample, time of the second, {node: stats}) once the background sample is done; None when none was started."""
        if not self.traffic_started:
            return None
        while not self.traffic_evt.wait(0.2):
            if self.cancelled():
                return None
        return self.traffic

    # ---- install / remove the cache in front of kubectl() and az_cli()
    def install(self):
        global kubectl, az_cli, _PF
        self.orig_kubectl, self.orig_az = kubectl, az_cli
        pf = self

        def kubectl_parallel(args, timeout=KUBECTL_TIMEOUT):
            return pf.call("k", ("k", tuple(args)), lambda: pf.orig_kubectl(args, timeout))

        def az_cli_parallel(args, target=None, timeout=90, subscription=True):
            key = ("az", tuple(args), ((target or {}).get("subscription") if subscription else None))
            return pf.call("az", key, lambda: pf.orig_az(args, target, timeout, subscription))
        kubectl_parallel.__wrapped__, az_cli_parallel.__wrapped__ = self.orig_kubectl, self.orig_az
        self.w_kubectl, self.w_az = kubectl_parallel, az_cli_parallel
        kubectl, az_cli, _PF = kubectl_parallel, az_cli_parallel, self

    def close(self):
        """Cancel what is still queued (calls already running end by their own timeout) and put the plain functions back."""
        global kubectl, az_cli, _PF
        self.closed = True
        self.pool.shutdown(wait=False, cancel_futures=True)
        if kubectl is self.w_kubectl:
            kubectl = self.orig_kubectl
        if az_cli is self.w_az:
            az_cli = self.orig_az
        if _PF is self:
            _PF = None

    def summary(self):
        with self.lock:
            return {"workers": self.workers, "tasks": self.total, "tasks_done": self.done, "calls": self.calls, "cache_hits": self.hits,
                    "call_seconds": self.call_seconds, "peak_kubectl": self.peak["k"], "peak_az": self.peak["az"], "errors": list(self.errors),
                    "unplanned": [k for k in self.misses if k not in self.task_keys]}


def _kubectl_uncached(args, timeout=KUBECTL_TIMEOUT):
    """kubectl without the parallel cache (for live samples that must be taken NOW)."""
    return getattr(kubectl, "__wrapped__", kubectl)(args, timeout)


def _cp_errors_kql(mins):
    return ("union isfuzzy=true AKSControlPlane, AzureDiagnostics "
            f"| where TimeGenerated > ago({mins}m) "
            "| where Category in ('kube-apiserver','kube-controller-manager','kube-scheduler','cluster-autoscaler','cloud-controller-manager') "
            "| extend Msg = coalesce(tostring(column_ifexists('Message','')), tostring(column_ifexists('log_s',''))) "
            "| where Msg matches regex '(?i)error|fail|forbidden|unauthorized|denied|timeout' "
            "| project TimeGenerated, Category, Msg | top 200 by TimeGenerated desc")


def _cp_audit_kql(mins):
    return ("union isfuzzy=true AKSAudit, AKSAuditAdmin, AzureDiagnostics "
            f"| where TimeGenerated > ago({mins}m) "
            "| extend raw = coalesce(tostring(column_ifexists('Log','')), tostring(column_ifexists('log_s',''))) "
            "| where isnotempty(raw) | extend j = parse_json(raw) "
            "| where toint(j.responseStatus.code) in (401, 403) "
            "| summarize n=count() by user=tostring(j.user.username), verb=tostring(j.verb), res=tostring(j.objectRef.resource), code=toint(j.responseStatus.code) "
            "| top 10 by n")


def _az_principal_ids(c):
    """The identities whose role assignments the Azure section lists (same order as _az_identity)."""
    out = []
    ident = c.get("identity") or {}
    if ident.get("principalId"):
        out.append(ident["principalId"])
    for _uid, info in (ident.get("userAssignedIdentities") or {}).items():
        if (info or {}).get("principalId"):
            out.append(info["principalId"])
    kubelet = (c.get("identityProfile") or {}).get("kubeletidentity") or {}
    if kubelet.get("objectId"):
        out.append(kubelet["objectId"])
    return out


def _plan_azure(pf, ctx, view, label, live):
    """Queue every Azure read the Azure section (and the network section) will make."""
    target = resolve_az_target(label)
    if not target:
        return
    net = "network" in live
    c0 = AZ_OPTS.get("cluster_info") or {}
    rg, cl, node_rg, mins = target["resource_group"], target["cluster"], target.get("node_rg"), ctx.minutes
    pf.az(["account", "show"], target, 30)

    def with_cluster(c):
        pf.az(["aks", "get-upgrades", "-g", rg, "-n", cl], target, 60)
        seen, lock = set(), threading.Lock()

        def once(key):
            with lock:
                if key in seen:
                    return False
                seen.add(key)
                return True

        def subnet_done(sn, err):
            if not isinstance(sn, dict):
                return
            nsg_id = (sn.get("networkSecurityGroup") or {}).get("id")
            if nsg_id and once(("nsg", nsg_id)):
                pf.az(["network", "nsg", "show", "--ids", nsg_id], target, 60)
            rt_id = (sn.get("routeTable") or {}).get("id")
            if net and rt_id and once(("rt", rt_id)):
                pf.az(["network", "route-table", "show", "--ids", rt_id], target, 60)
        for sid in _az_subnet_ids(c):
            pf.az(["network", "vnet", "subnet", "show", "--ids", sid], target, 60, then=subnet_done)
        lbp = (c.get("networkProfile") or {}).get("loadBalancerProfile") or {}
        for r in (lbp.get("effectiveOutboundIPs") or [])[:6]:
            pf.az(["network", "public-ip", "show", "--ids", r["id"]], target, 30)
        for pid in _az_principal_ids(c):
            pf.az(["role", "assignment", "list", "--assignee", pid, "--all"], target, 90)

        def instances(s):
            def fin(inst, err):
                if err:
                    pf.az(["vmss", "list-instances", "-g", node_rg, "-n", s["name"]], target, 120)
            pf.az(["vmss", "list-instances", "-g", node_rg, "-n", s["name"], "--expand", "instanceView"], target, 120, then=fin)

        def vmss_done(sets, err):
            for s in (sets or []) if not err else []:
                instances(s)
                if net:
                    for metric in ("Network In Total", "Network Out Total"):
                        pf.go(_az_metric, view, target, s["id"], metric, "Total", split="VMName")
        if node_rg:
            pf.az(["vmss", "list", "-g", node_rg], target, 90, then=vmss_done)

        def ds_done(ds, err):
            if err:
                return
            settings = ds.get("value") if isinstance(ds, dict) else ds
            enabled, workspaces = set(), []
            for s in settings or []:
                for lg in s.get("logs") or []:
                    if lg.get("enabled") and lg.get("category"):
                        enabled.add(lg["category"])
                if s.get("workspaceId"):
                    workspaces.append(s["workspaceId"])
            if not enabled or not workspaces:
                return

            def ws_done(wid, err2):
                if err2 or not isinstance(wid, str):
                    return
                pf.az(["monitor", "log-analytics", "query", "-w", wid, "--analytics-query", _cp_errors_kql(mins), "--timespan", f"PT{mins}M"], target, 120, subscription=False)
                if "kube-audit" in enabled or "kube-audit-admin" in enabled:
                    pf.az(["monitor", "log-analytics", "query", "-w", wid, "--analytics-query", _cp_audit_kql(mins), "--timespan", f"PT{mins}M"], target, 120, subscription=False)
            pf.az(["monitor", "log-analytics", "workspace", "show", "--ids", workspaces[0], "--query", "customerId"], target, 60, subscription=False, then=ws_done)
        if target.get("id"):
            pf.az(["monitor", "diagnostic-settings", "list", "--resource", target["id"]], target, 60, then=ds_done)
        if not net:
            return
        # ---- what the network section reads (see _net_az_collect)
        if node_rg:
            pf.az(["network", "public-ip", "list", "-g", node_rg], target, 60)

            def lb_done(lbs, err):
                if err or not isinstance(lbs, list):
                    return
                for lb in lbs[:6]:
                    for metric, agg in (("ByteCount", "Total"), ("PacketCount", "Total"), ("SnatConnectionCount", "Total"), ("UsedSnatPorts", "Average"),
                                        ("AllocatedSnatPorts", "Average"), ("DipAvailability", "Average"), ("VipAvailability", "Average")):
                        pf.go(_az_metric, view, target, lb["id"], metric, agg)
                    pf.go(_az_metric, view, target, lb["id"], "SnatConnectionCount", "Total", split="ConnectionState")
            pf.az(["network", "lb", "list", "-g", node_rg], target, 90, then=lb_done)

            def nat_done(gws, err):
                ids = [g.get("id") for g in (gws if isinstance(gws, list) and not err else [])]
                for sid in _az_subnet_ids(c):
                    sn, _e = az_cli(["network", "vnet", "subnet", "show", "--ids", sid], target, 60)
                    nid = (sn.get("natGateway") or {}).get("id") if isinstance(sn, dict) else None
                    if nid and nid not in ids:
                        ids.append(nid)
                for nid in ids[:6]:
                    for metric in ("ByteCount", "PacketCount", "PacketDropCount", "SNATConnectionCount"):
                        pf.go(_az_metric, view, target, nid, metric, "Total")
            pf.az(["network", "nat", "gateway", "list", "-g", node_rg], target, 60, then=nat_done)
            pf.az(["network", "nsg", "list", "-g", node_rg], target, 60)
        if c.get("location"):
            pf.az(["network", "watcher", "flow-log", "list", "--location", c["location"]], target, 60)

        def fw():
            if _has_extension("azure-firewall"):
                az_cli(["network", "firewall", "list"], target, 60)
        pf.go(fw)
        scope = node_rg or rg
        if scope:
            pf.az(["monitor", "activity-log", "list", "--resource-group", scope, "--start-time", _iso(ctx.since), "--end-time", _iso(ctx.now), "--max-events", "200"], target, 90)

    if c0.get("agentPoolProfiles"):
        with_cluster(c0)
    else:
        pf.az(["aks", "show", "-g", rg, "-n", cl], target, 90, then=lambda d, e: with_cluster(d) if isinstance(d, dict) else None)


def plan_collection(pf, ctx, label, plan):
    """Hand the reads of the selected sections to the pool (kubectl first: the sections need it first)."""
    live = set(plan["collect"]) | set(plan["hidden"])
    view = types.SimpleNamespace(data=pf.data, minutes=ctx.minutes, now=ctx.now, since=ctx.since, cancel=pf.cancel)
    net = "network" in live
    res = [n for n in RESOURCES if n in plan["resources"]]

    def stage_nodes():
        names = [n["metadata"]["name"] for n in items(pf.data.get("nodes"))]
        okn = []

        def fin():
            pf.stats_ts = time.time()
            if net and TRAFFIC_SAMPLE_SECONDS > 0 and okn:
                pf.start_sampler(list(okn))
        latch = _Latch(len(names), fin)

        def done(name):
            def cb(ok, out):
                if ok:
                    okn.append(name)
                latch.hit()
            return cb
        for name in names:
            pf.k(["get", "--raw", f"/api/v1/nodes/{name}/proxy/stats/summary"], 60, then=done(name))

    def stage_all():
        if net:
            for name, sel in (("azure-cns", "k8s-app=azure-cns"), ("azure-ip-masq-agent", "k8s-app=azure-ip-masq-agent"), ("cilium", "k8s-app=cilium"),
                              ("calico-node", "k8s-app=calico-node"), ("azure-npm", "k8s-app=azure-npm")):
                if _ds(view, name):
                    pf.go(_logs, view, selector=sel)
            if _ds(view, "kube-proxy"):
                pf.go(_logs, view, selector="component=kube-proxy")
            pf.go(_logs, view, selector="k8s-app=kube-dns")
            for p in _ingress_controllers(view)[:8]:
                pf.go(_logs, view, ns=p["metadata"]["namespace"], pod=p["metadata"]["name"], tail=500)
            for ns_, _ing, secret, _hosts in _tls_secrets(view)[1]:
                if secret != "(Azure Key Vault)":
                    pf.k(_cert_args(ns_, secret), 30)
            for p in _exporter_pods(view)[:20]:
                pf.k(_exporter_args(p), 45)

    latch_all = _Latch(len(res), stage_all)

    def on_resource(name, data, err):
        if name == "nodes" and plan["usage"]:
            stage_nodes()
        latch_all.hit()
    for name in res:
        pf.kj(name, RESOURCES[name], then=on_resource)
    if "overview" in live:
        pf.k(["config", "current-context"])
        pf.k(["version", "-o", "json"])
        pf.k(["get", "--raw", "/readyz?verbose"])
    if plan["usage"]:
        pf.k(["top", "nodes", "--no-headers"])
        pf.k(["top", "pods", "-A", "--no-headers"])
    if net:
        pf.k(["get", "--raw", "/metrics"], 60)
        pf.k(["get", "--raw", "/readyz/etcd"], 30)
        for r_ in ("validatingwebhookconfigurations", "mutatingwebhookconfigurations"):
            pf.go(kjson, ["get", r_])
        for cm in ("kube-proxy-config", "azure-cns-config", "azure-ip-masq-agent-config-reconciled", "azure-ip-masq-agent-config", "cilium-config",
                   "coredns", "coredns-custom", "calico-config"):
            pf.go(kjson, ["get", "configmap", cm, "-n", "kube-system"])
    if "azure" in live and AZ_OPTS["enabled"]:
        _plan_azure(pf, ctx, view, label, live)


def plan_log_collection(pf, ctx, options):
    """Called once the pods are analysed: queue the log reads of the pods the Pod logs section will pick."""
    targets, pods = pick_log_targets(ctx, options)
    for ns, name, cname, prev, _reason, _restarts in _log_jobs(targets, pods):
        pf.go(_container_logs, ns, name, cname, ctx.minutes, previous=prev)


def format_timing(steps_log, total_secs, pf_summary):
    """The timing footer: total time, time per step, and the parallel figures."""
    lines = ["RUN TIMING", "-" * 10, f"Total time: {total_secs:.1f}s"]
    if pf_summary:
        est = pf_summary["call_seconds"]
        lines.append(f"Parallel collection: {pf_summary['workers']} workers, {pf_summary['tasks_done']} of {pf_summary['tasks']} collection tasks done, "
                     f"{pf_summary['calls']} calls made ({pf_summary['cache_hits']} answered from the cache), at most {pf_summary['peak_kubectl']} kubectl and "
                     f"{pf_summary['peak_az']} az calls at once")
        lines.append(f"Waiting time of all calls added together (about a one-after-another run): {est:.1f}s")
    else:
        lines.append("Collection ran one step after another (--workers 1).")
    lines.append("Time per step:")
    for title, status, secs in steps_log:
        lines.append(f"  {title}: {secs} ({status})")
    return lines


# ---------------------------------------------------------------------------
# Orchestration (step by step, with progress + cancel)
# ---------------------------------------------------------------------------

def run_steps(options=None):
    """The ordered collection steps: (key, title, optional_option_name_or_None). Built from SECTIONS (plus the always-run 'data' step)."""
    steps = []
    for s in SECTIONS:
        steps.append((s["id"], s["step"], {"azure": "azure", "logs": "logs"}.get(s["id"])))
        if s["id"] == "overview":
            steps.append(("data", "Collect cluster data", None))
    return steps


def _run_silent(ctx, rep, fn):
    """Run a section only for the DATA it leaves in ctx (another selected section needs it). Nothing it writes reaches the report, the
    findings, the timeline or the contact list; the live findings callback stays quiet."""
    snap = (len(ctx.findings), len(ctx.findings_full), len(ctx.timeline), {k: list(v) for k, v in ctx.ns_issues.items()}, ctx.on_finding)
    quiet = Report(lambda line: None)
    ctx.report, ctx.on_finding = quiet, None
    try:
        fn(quiet)
    finally:
        del ctx.findings[snap[0]:], ctx.findings_full[snap[1]:], ctx.timeline[snap[2]:]
        ctx.ns_issues.clear()
        ctx.ns_issues.update({k: v for k, v in snap[3].items()})
        ctx.report, ctx.on_finding = rep, snap[4]


def run_debug(label, minutes, emit, progress=None, cancel=None, options=None, on_finding=None):
    """Collect everything for the CURRENT kubectl context and write the .txt and the
    interactive .html report. `progress(key, status, seconds)` is called for every step
    (status: running / done / failed / skipped); `cancel` is a threading.Event - when it is
    set the remaining steps are skipped and a partial report is still written.
    options: azure, logs, all_logs, log_namespaces, sections (ids to collect, None = all), workers (parallel collection tasks, 1 = one by one),
    task_progress (callback(done, total) for the 'x of y collection tasks' counter).
    Returns the path of the HTML report."""
    options = {"azure": AZ_OPTS["enabled"], "logs": True, "all_logs": False, "log_namespaces": "", "sections": None, "workers": None,
               "task_progress": None, **(options or {})}
    AZ_OPTS["enabled"] = bool(options["azure"])
    plan = plan_sections(options)
    ctx = Ctx(minutes)
    ctx.resources, ctx.want_usage = plan["resources"], plan["usage"]
    rep = Report(emit)
    ctx.report, ctx.on_finding, ctx.cancel = rep, on_finding, cancel
    note = progress or (lambda *a, **k: None)
    steps_log = []
    t_run = time.time()
    guard_mark = GUARD.mark()          # the read-only guarantee block counts only this run
    live = set(plan["collect"]) | set(plan["hidden"])
    skipped_titles = [SECTION_BY_ID[s]["title"] for s in SECTIONS_ORDER if s not in plan["collect"]]
    ctx.sections_note = ((len(plan["collect"]), len(SECTIONS), skipped_titles) if skipped_titles else None)

    runners = {
        "overview": lambda r: section_overview(r, ctx, label),
        "data": lambda r: load_data(ctx, r),
        "azure": lambda r: section_azure(r, ctx, label),
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
    workers = int(options.get("workers") or PARALLEL_WORKERS)
    pf = None
    if workers > 1:
        pf = Prefetcher(workers, cancel, options.get("task_progress"))
        pf.install()
        try:
            plan_collection(pf, ctx, label, plan)
        except Exception as exc:       # the plan is only an accelerator: whatever was queued still helps, the rest is read when needed
            emit(f"[!] could not plan the parallel collection completely ({exc}) - the rest is collected step by step")
    stopped = False
    try:
        for key, title, opt in run_steps(options):
            sec = SECTION_BY_ID.get(key)
            full_title = sec["title"] if sec else title
            if stopped or (cancel is not None and cancel.is_set()):
                if not stopped:
                    rep.add("")
                    rep.add("Stopped by user - the remaining steps were skipped. The report below is partial.")
                stopped = True
                note(key, "skipped", None)
                steps_log.append((title, "skipped (stopped)", "-"))
                continue
            if key != "data" and key not in live:
                reason = plan["skipped"].get(key, "not selected")
                rep.skipped(full_title, "" if reason == "not selected" else reason)
                note(key, "skipped", None)
                steps_log.append((title, "skipped (turned off)" if reason != "not selected" else "skipped by choice", "-"))
                continue
            note(key, "running", None)
            t0 = time.time()
            if key in plan["hidden"] and key not in plan["collect"]:     # not selected, but another selected section needs its data
                who = ", ".join(SECTION_BY_ID[s]["title"] for s in plan["hidden"][key])
                rep.skipped(full_title, f"not shown; its data is collected quietly because {who} needs it")
                try:
                    _run_silent(ctx, rep, runners[key])
                    status = "done"
                except Exception as exc:
                    status = "failed"
                    rep.add(f"[!] step '{title}' failed: {exc}")
                secs = time.time() - t0
                note(key, status, secs)
                steps_log.append((title, f"collected quietly for {who}" if status == "done" else status, f"{secs:.1f}s"))
            else:
                try:
                    runners[key](rep)
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
            if key == "pods" and pf is not None and "logs" in live and not pf.cancelled():
                try:
                    plan_log_collection(pf, ctx, options)         # the pods are analysed: start reading their logs in the background
                except Exception:
                    pass
    finally:
        if pf is not None:
            pf.close()

    try:
        section_glossary(rep, ctx)        # the complete glossary closes the report (always, so every term used above is explained)
    except Exception as exc:
        rep.add(f"[!] glossary failed: {exc}")
    ro_info = GUARD.since(guard_mark)
    if ro_info["blocked"]:                # something tried to change / run / install: refused, and said loudly
        rep.current = {"id": "readonly", "title": "Read-only guarantee", "blocks": []}      # the finding links to the read-only block
        ctx.find("CRIT", f"READ-ONLY GUARD refused {len(ro_info['blocked'])} command(s) that would change something (nothing was run): "
                         + "; ".join(f"{t} {c[:80]}" for _w, t, c, _r in ro_info["blocked"][:3]))
        ro_info = GUARD.since(guard_mark)
    rep.current = rep.sections[0]         # the summary lines below belong to no section (they are the health summary, drawn separately)
    ctx.readonly = ro_info
    summary = build_summary(ctx, label)
    rep.add("")
    for line in summary:
        rep.add(line)
    raw_text = "\n".join(summary + [""] + rep.lines[: len(rep.lines) - len(summary) - 1])
    pf_summary = pf.summary() if pf is not None else None
    timing_lines = format_timing(steps_log, time.time() - t_run, pf_summary)
    ro_lines = guard_lines(ro_info)
    raw_text = "READ-ONLY: " + ro_lines[0] + "\n\n" + raw_text      # no counts above RUN TIMING: they differ between sequential and parallel runs
    raw_text += "\n\n" + "\n".join(timing_lines[:2] + [f"What this section shows: {EXTRA_SECTIONS['timing'][0]}", f"How to use it: {EXTRA_SECTIONS['timing'][1]}"] + timing_lines[2:])
    raw_text += ("\n\n" + "READ-ONLY GUARANTEE\n" + "-" * 19 + f"\nWhat this section shows: {EXTRA_SECTIONS['readonly'][0]}\nHow to use it: {EXTRA_SECTIONS['readonly'][1]}\n"
                 + "\n".join(ro_lines))

    note("report", "running", None)
    os.makedirs(REPORT_DIR, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", label)[:60]
    base0 = os.path.abspath(os.path.join(REPORT_DIR, f"aks_debug_{safe}_{datetime.now():%Y%m%d_%H%M%S}"))
    base, n = base0, 1
    while os.path.exists(base + ".html") or os.path.exists(base + ".txt"):   # never overwrite an earlier report
        n += 1
        base = f"{base0}_{n}"
    with open(base + ".txt", "w", encoding="utf-8") as f:
        f.write(raw_text)
    with open(base + ".html", "w", encoding="utf-8") as f:
        f.write(render_html(label, ctx, rep, steps_log, raw_text, timing_lines=timing_lines))
    note("report", "done", None)
    emit("")
    for line in timing_lines[:4]:
        emit(line)
    emit(f"Report saved to: {base}.txt")
    emit(f"Interactive HTML report: {base}.html")
    result = ReportPath(base + ".html")
    result.txt = base + ".txt"
    result.counts = Counter(f[0] for f in ctx.findings_full)
    result.findings = list(ctx.findings_full)
    result.section_titles = {sec["id"]: sec["title"] for sec in rep.sections}
    result.section_titles.setdefault("readonly", "Read-only guarantee")
    result.missing_about = list(rep.missing_about)       # tables / blocks written without an explanation (must stay empty; a test checks it)
    result.partial = stopped
    result.meta = dict(ctx.meta)
    result.sections = [s for s in SECTIONS_ORDER if s in plan["collect"]]
    result.timing = {"total": time.time() - t_run, "steps": list(steps_log), "parallel": pf_summary}
    return result


def find_context(label):
    """Pick the kubectl context that belongs to the selected cluster. Returns
    (context_name_or_None, [candidates], current_context). Matching looks at the context
    name and at the cluster it points to (AKS contexts are usually the cluster ARN, or
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
    # The az listing (CLI method, or the custom login with 'all clusters') knows every cluster's subscription / resource group / location
    tgt = CLI_TARGETS.get(str(cluster_number)) if (cli or CLI_TARGETS) else None
    ctx_label = label
    if not skip_login:
        note("login", "running", None)
        t0 = time.time()
        use_az = cli or (tgt is not None and not tgt.get("exe_number"))     # a cluster that is not in the akslogin menu cannot use the exe
        if use_az:
            if cli:
                emit(f"Logging in to cluster {cluster_number} ({label}) with the Azure CLI ...")
            else:
                emit(f"Cluster {cluster_number} ({label}) is not in the akslogin menu - logging in with the Azure CLI (az aks get-credentials) instead ...")
            try:
                ctx = cli_login(cluster_number, label, emit)
            except RuntimeError:
                note("login", "failed", time.time() - t0)
                raise
            context = context or ctx
            tgt = CLI_TARGETS.get(str(cluster_number))
        else:
            exe_no = tgt["exe_number"] if tgt else cluster_number
            emit(f"Logging in to cluster {cluster_number} ({label}) with akslogin" + (f" (menu number {exe_no})" if tgt else "") + " ...")
            if not akslogin(exe_no):
                note("login", "failed", time.time() - t0)
                raise RuntimeError(f"akslogin failed for cluster {cluster_number}")
            if tgt and not tgt.get("exe_only"):
                ctx_label = tgt["name"]
        emit("Login OK.")
        note("login", "done", time.time() - t0)
    else:
        note("login", "skipped", None)
        if tgt:
            ctx_label = tgt["name"]
    saved = {k: AZ_OPTS[k] for k in ("cluster", "resource_group", "subscription")}
    if tgt and tgt.get("resource_group"):   # the listing already knows the cluster: hand it to the existing Azure subscription step so nothing is guessed
        AZ_OPTS.update(cluster=tgt["name"], resource_group=tgt["resource_group"], subscription=tgt.get("subscription") or AZ_OPTS["subscription"])
    try:
        note("context", "running", None)
        t0 = time.time()
        select_context(ctx_label, emit, forced=context)
        note("context", "done", time.time() - t0)
        if (options or {}).get("azure", AZ_OPTS["enabled"]) and AZ_OPTS["enabled"]:
            note("profile", "running", None)
            t0 = time.time()
            try:
                select_az_subscription(label, emit, preferred=AZ_OPTS["subscription"])
            except Exception as exc:  # never block the kubectl data because of Azure subscription trouble
                emit(f"WARNING: could not choose an Azure subscription: {exc}")
            note("profile", "done", time.time() - t0)
        else:
            note("profile", "skipped", None)
        return run_debug(label, minutes, emit, progress, cancel, options, on_finding)
    finally:
        AZ_OPTS.update(saved)


class ReportPath(str):
    """The path of a cluster's HTML report. It also carries what the run found, which the
    multi-cluster summary page is built from."""
    txt = None
    counts = None
    findings = None
    section_titles = None
    partial = False
    meta = None
    sections = None
    timing = None


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
    """Run the whole debug for several clusters, ONE AFTER ANOTHER (akslogin, the kubectl context
    and the Azure subscription are shared state, so they must not overlap). Every cluster gets its own
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
                 "counts": Counter(), "findings": [], "titles": {}, "secs": None, "error": None, "sections": None}
        entries.append(entry)
        if cancel is not None and cancel.is_set():
            entry["status"] = "not run (stopped)"
            notify(i, n, label, entry["status"], entry)
            continue
        emit("")
        emit("#" * 78)
        emit(f"# CLUSTER {i} of {n}: {label}  (#{number})")
        emit("#" * 78)
        owner = account_of_cluster(number) if LOGIN_OPTS["method"] == "cli" or CLI_TARGETS else None
        if owner and (CRED["status"].get(owner) or {}).get("state") == "expired":
            entry.update(status="credentials expired", error=f"Credentials for {owner} expired - sign in again (step 2), then run this cluster again.")
            emit(f"SKIPPED cluster {label}: credentials for {owner} expired - sign in again. Continuing with the next cluster.")
            notify(i, n, label, entry["status"], entry)
            continue
        notify(i, n, label, "running", entry)
        t0 = time.time()
        events0 = CRED["events"]
        finding_cb = on_finding
        if on_finding and n > 1:
            finding_cb = lambda sev, text, _l=label: on_finding(sev, f"[{_l}] {text}")
        try:
            result = login_and_debug(number, label, minutes, emit, skip_login, context if n == 1 else None,
                                     progress, cancel, options, finding_cb)
            entry.update(status="stopped (partial)" if getattr(result, "partial", False) else "ok",
                         html=str(result), txt=getattr(result, "txt", None), counts=getattr(result, "counts", Counter()),
                         findings=getattr(result, "findings", []), titles=getattr(result, "section_titles", {}),
                         sections=getattr(result, "sections", None))
        except Exception as exc:
            entry.update(status="failed", error=str(exc))
            emit(f"ERROR on cluster {label}: {exc}")
            if is_expired_error(str(exc)) or CRED["events"] > events0:
                who_ = owner or CRED.get("current") or "the signed-in account"
                set_account_status(who_, "expired", None, str(exc))
                entry.update(status="credentials expired", error=f"Credentials for {who_} expired - sign in again (step 2), then run this cluster again.")
                emit(f"Credentials for {who_} expired - sign in again. Continuing with the next cluster.")
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
    base0 = os.path.abspath(os.path.join(REPORT_DIR, f"aks_debug_summary_{datetime.now():%Y%m%d_%H%M%S}"))
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
        secs_done = en.get("sections")
        sections_txt = "-" if secs_done is None else ("all %d" % len(SECTIONS) if len(secs_done) == len(SECTIONS) else "%d of %d" % (len(secs_done), len(SECTIONS)))
        sections_tip = "" if secs_done is None else esc(", ".join(SECTION_BY_ID[s]["title"] for s in secs_done))
        rows.append('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td title="%s">%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'
                    % (esc(en["label"]), esc(shown), c["CRIT"], c["HIGH"], c["MED"], c["INFO"], sections_tip, sections_txt,
                       ("%.0fs" % en["secs"]) if en["secs"] else "-", esc(top), report))
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
    clusters_table = (_what_html("one row per selected cluster: whether its run succeeded, how many Critical, High, Medium and Information findings it has, how many report sections were collected, how long it took, its most serious finding and a link to its full report.")
                      + '<div class="tablewrap"><div class="tbtools"><input class="tfilter" type="search" placeholder="Filter clusters...">'
                      '<span class="tcount"></span><button class="csv" type="button">CSV</button></div><div class="tscroll">'
                      '<table class="data"><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div></div>'
                      % ("".join(_th(h) for h in ("Cluster", "Status", "Critical", "High", "Medium", "Information", "Sections collected", "Time taken", "Top finding", "Report")), "".join(rows)))
    findings_table = (_what_html("every finding of every selected cluster in one list, most serious first; click a severity card to hide or show a level and use a link in the Where column to open the matching section of the cluster report.")
                      + '<div class="tablewrap"><div class="tbtools"><span class="tcount"></span><input class="tfilter" style="display:none">'
                      '<button class="csv" style="display:none">CSV</button></div><div class="tscroll"><table class="data" id="findings"><thead>'
                      '<tr>%s</tr></thead><tbody>%s</tbody></table></div></div>'
                      % ("".join(_th(h) for h in ("Severity", "Cluster", "Finding", "Where")),
                         "".join(finding_rows) or '<tr data-sev="INFO"><td></td><td></td><td>No findings</td><td></td></tr>'))
    raw_text = "\n".join([f"AKS DEBUG - {len(entries)} clusters, last {minutes} min, {stamp}", ""] + raw).replace("</script", "<\\/script")
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>AKS debug - {len(entries)} clusters</title><style>{_HTML_CSS}{_brand_css()}{_ABOUT_CSS}</style></head><body>
<header><div class="band">{logo_svg(64)}<div class="txt"><h1>{esc(PRODUCT_NAME)} - summary of {len(entries)} clusters</h1><div class="meta">Window: last {minutes} min  |  Generated: {stamp}</div></div>
<span class="tag">{ICONS["cloud"][0]} {esc(CLOUD_NAME)}</span>{_DECO_CLOUDS}</div>
<div class="tools"><div class="toolbar"><input id="q" type="search" placeholder="Search everything ( press / )"><span id="hits" class="small"></span>
<button id="expand" type="button">Expand all</button><button id="collapse" type="button">Collapse all</button>
<button id="theme" type="button">Dark / light</button><button id="print" type="button">Print</button><button id="dl" type="button">Download .txt</button></div></div></header>
<div class="layout"><nav><a href="#clusters" class="jump">Clusters</a><a href="#allfindings" class="jump">All findings</a></nav><main>
<details class="sec" id="clusters" open><summary><span><span class="ico">{ICONS["helm"][0]}</span>Clusters</span><span class="badge b-info">{len(entries)}</span></summary><div class="secbody">{_box_html("clusters")}{clusters_table}</div></details>
<details class="sec" id="allfindings" open><summary><span><span class="ico">{ICONS["warn"][0]}</span>All findings (every cluster)</span><span class="badge b-info">{sum(total.values())}</span></summary><div class="secbody">{_box_html("allfindings")}<div class="cards">{cards}</div>{findings_table}{_legend_html()}</div></details>
</main></div><footer>Generated by aks_debug.py - read-only data. {esc(READ_ONLY_STATEMENT)} The per-cluster reports are the files linked above (keep them in the same folder).</footer>
<script type="text/plain" id="rawtext">{raw_text}</script><script>{_HTML_JS}</script></body></html>"""


# ---------------------------------------------------------------------------
# PRIVILEGED ROLES (PIM) - the ONE opt-in exception to "this tool only reads".
#
# The 4th tab of the window (and --pim-list / --pim-activate-all) shows the roles and groups the signed-in user holds through Privileged
# Identity Management (active now / eligible) and can submit SELF-ACTIVATION requests for the eligible ones - ONLY after the user
# confirms in a dialog. It never runs during a report, never assigns anything to anyone and never creates, changes or deletes assignments,
# policies, groups or users. Enforced HERE by assert_pim_get() / assert_pim_write() (allow-lists); everything else is refused with
# "blocked: read-only mode". All calls go through `az rest` (az owns the sign-in; the token never reaches this tool):
#   1. Azure resource roles ......... ARM PIM API (management.azure.com, api-version 2020-10-01, SelfActivate)
#   2. Microsoft Entra roles and
#      Privileged access groups ..... the PIM service API behind the portal's own PIM blade (api.azrbac.mspim.azure.com, az rest --resource
#                                     01fc33a7-78ba-4d2f-a4b7-768e336e890e) - no Microsoft Graph consent needed
#   3. fallback ..................... if (2) is blocked: the exact error is shown and the portal PIM page can be opened
# ---------------------------------------------------------------------------

PIM_NOTE = "Reports only read. The Privileged roles tab can activate your own eligible roles when you confirm."
PIM_EXCEPTION_STATEMENT = ("Opt-in exception: the Privileged roles (PIM) tab can submit SELF-ACTIVATION requests for your own, already eligible roles and groups, "
                           "only after you confirm in a dialog. It never assigns access to anyone and never creates, changes or deletes assignments, policies, groups or users.")
PIM_ARM_HOST = "management.azure.com"
PIM_MSPIM_HOST = "api.azrbac.mspim.azure.com"
PIM_MSPIM_RESOURCE = "01fc33a7-78ba-4d2f-a4b7-768e336e890e"
PIM_MSPIM_PREFIX = "/api/v2/privilegedAccess/"
PIM_ARM_API = "2020-10-01"
PIM_WORKERS = 4                    # activation requests that run at the same time
PIM_THROTTLE_WAITS = (2, 5, 10)    # seconds to wait before each retry after HTTP 429
PIM_MAX_PAGES = 20
PIM_FAMILIES = ("arm", "entra", "group")
PIM_TYPE = {"arm": "Azure resource role", "entra": "Microsoft Entra role", "group": "Privileged access group"}
PIM_FAMILY_NAME = {"arm": "Azure resource roles", "entra": "Microsoft Entra roles", "group": "Privileged access groups"}
PIM_PORTAL_URLS = {"entra": "https://portal.azure.com/#view/Microsoft_Azure_PIMCommon/ActivationMenuBlade/~/aadmigratedroles",
                   "group": "https://portal.azure.com/#view/Microsoft_Azure_PIMCommon/ActivationMenuBlade/~/aadgroup"}
PIM_SCRIPT_HINT = "activate_pim_roles_ui.py (your existing browser-automation script, next to this tool; not run or imported by it)"
PIM_UNKNOWN_MAX_MINUTES = 60       # when the policy cannot be read and no hours were typed: ask for 1 hour only
PIM_COLUMN_HELP = {
    "Role or group name": "The name of the role (Azure resource role or Microsoft Entra role) or of the privileged access group.",
    "Type": "Azure resource role = a role on a subscription / resource group / resource; Microsoft Entra role = a directory role of your tenant; Privileged access group = membership or ownership of a group.",
    "Scope": "Where the role applies: the subscription / resource group / management group, the whole directory, or the group membership kind (member / owner).",
    "Status": "Active = usable right now. Eligible = you may activate it. Pending approval = you asked and an approver must decide. Expired soon = active but less than 30 minutes left.",
    "Expires at (time left)": "When the activation ends (your local time) and how long is left. Eligible roles show until when you stay eligible.",
    "Maximum duration allowed": "The longest activation the role policy allows. The tool never asks for more than this.",
    "Requires": "What the policy asks at activation: justification, ticket, approval, multi-factor authentication, authentication context. Empty = could not be read or nothing.",
}
_GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")
_PIM_SCOPE_RE = re.compile(r"^(?:/(?:subscriptions|providers)(?:/[A-Za-z0-9._()\-]+)+)?$")
_PIM_DUR_RE = re.compile(r"^PT(?:\d{1,4}H)?(?:\d{1,5}M)?$")
_PIM_QUERY_CHARS = re.compile(r"^[A-Za-z0-9 _/()',.:=\-$&%+~@]*$")
_PIM_THROTTLE_RE = re.compile(r"429|TooManyRequests|Too Many Requests|throttl", re.I)
_PIM_ARM_ROOT_RE = re.compile(r"^/providers/Microsoft\.Authorization/(roleEligibilityScheduleInstances|roleAssignmentScheduleInstances|roleAssignmentScheduleRequests)$")
_PIM_ARM_SCOPED_RE = re.compile(r"^(?P<scope>(?:/[A-Za-z0-9._()\-]+)*)/providers/Microsoft\.Authorization/"
                                r"(?P<kind>roleManagementPolicyAssignments|roleManagementPolicies/[0-9a-fA-F\-]{36}|roleDefinitions/[0-9a-fA-F\-]{36})$")
_PIM_MSPIM_GET_RE = re.compile(r"^" + re.escape(PIM_MSPIM_PREFIX) + r"(aadroles|aadGroups)/(roleAssignments|roleSettingsV2|resources)$")
_PIM_MSPIM_POST_RE = re.compile(r"^" + re.escape(PIM_MSPIM_PREFIX) + r"(aadroles|aadGroups)/roleAssignmentRequests$")
PIM_STATE = {"me": None, "variant": {}, "pending": set(), "lock": threading.Lock()}     # remembered while the program runs (never written to a file)


def _pim_block(what, tool="pim"):
    msg = _blocked_message(what)
    GUARD.block(tool, what, msg)
    raise ReadOnlyViolation(msg)


def _pim_url(url):
    from urllib.parse import urlsplit, parse_qsl
    if not isinstance(url, str) or not url or any(ord(c) < 32 for c in url):
        _pim_block("(malformed PIM address)")
    u = urlsplit(url)
    if u.scheme != "https" or u.username or u.fragment or u.port not in (None, 443):
        _pim_block("PIM address " + url[:80])
    if not _PIM_QUERY_CHARS.match(u.query):
        _pim_block("PIM query " + u.query[:80])
    return u, dict(parse_qsl(u.query, keep_blank_values=True))


def assert_pim_get(url, resource=None):
    """Allow-list for the PIM list calls (GET only): ARM role (eligibility / assignment) schedule instances and requests of the signed-in user, role
    policies and role definitions; the PIM service's roleAssignments / roleSettingsV2 / resources. Raises ReadOnlyViolation otherwise. Returns "read"."""
    u, q = _pim_url(url)
    host, path = u.hostname.lower(), u.path
    if host == PIM_ARM_HOST:
        if resource not in (None, ""):
            _pim_block("GET " + path + " (unexpected --resource)")
        if _PIM_ARM_ROOT_RE.match(path):
            if q.get("api-version") != PIM_ARM_API or q.get("$filter") != "asTarget()" or set(q) - {"api-version", "$filter", "$skiptoken"}:
                _pim_block("GET " + path + " with these parameters")
            return "read"
        m = _PIM_ARM_SCOPED_RE.match(path)
        if m and _PIM_SCOPE_RE.match(m.group("scope")) and ".." not in path:
            kind = m.group("kind")
            if kind == "roleManagementPolicyAssignments":
                f = q.get("$filter", "")
                if q.get("api-version") == PIM_ARM_API and set(q) <= {"api-version", "$filter", "$skiptoken"} and re.match(r"^roleDefinitionId eq '/[A-Za-z0-9._()/\-]+'$", f):
                    return "read"
            elif kind.startswith("roleManagementPolicies/"):
                if q.get("api-version") == PIM_ARM_API and set(q) == {"api-version"}:
                    return "read"
            elif kind.startswith("roleDefinitions/"):
                if q.get("api-version") in ("2022-04-01", "2018-01-01-preview") and set(q) == {"api-version"}:
                    return "read"
        _pim_block("GET " + path)
    if host == PIM_MSPIM_HOST:
        if resource != PIM_MSPIM_RESOURCE:
            _pim_block("GET " + path + " (wrong --resource)")
        if _PIM_MSPIM_GET_RE.match(path) and set(q) <= {"$expand", "$filter", "$count", "$orderby", "$top", "$skiptoken", "$select"}:
            return "read"
        _pim_block("GET " + path)
    _pim_block("GET " + host + path)


def _pim_iso_minutes(text):
    """'PT8H' / 'PT30M' / 'P1D' / 'PT1H30M' -> minutes (None when it is not a duration)."""
    m = re.match(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$", str(text or ""))
    if not m or not any(m.groups()):
        return None
    d, h, mi, s = (int(x) if x else 0 for x in m.groups())
    return d * 1440 + h * 60 + mi + (1 if s else 0)


def pim_duration_text(minutes):
    minutes = max(1, int(minutes))
    h, m = divmod(minutes, 60)
    return "PT" + (f"{h}H" if h else "") + (f"{m}M" if m or not h else "")


def _same_id(a, b):
    return isinstance(a, str) and isinstance(b, str) and a.strip().lower() == b.strip().lower() and bool(a.strip())


def _pim_check_ticket(t):
    return isinstance(t, dict) and set(t) == {"ticketNumber", "ticketSystem"} and all(isinstance(v, str) and len(v) <= 200 for v in t.values())


def assert_pim_write(method, url, body, me_oid, max_minutes=None):
    """Allow-list for the ONLY writes of the tool - self-activation of your OWN eligible role / group (three exact shapes):
       ARM    PUT  {scope}/providers/Microsoft.Authorization/roleAssignmentScheduleRequests/{guid}?api-version=2020-10-01   {properties:{requestType:SelfActivate,...}}
       PIM    POST .../aadroles/roleAssignmentRequests   and   .../aadGroups/roleAssignmentRequests   {type:UserAdd, assignmentState:Active, subjectId = you, ...}
    Everything else (other request types, other principals, deletes, policy / setting updates, groups, users ...) raises ReadOnlyViolation. Returns "pim-activate"."""
    from datetime import datetime as _dt
    if not isinstance(me_oid, str) or not _GUID_RE.match(me_oid):
        _pim_block("PIM activation (the signed-in user's object id is unknown)")
    u, q = _pim_url(url)
    host, path, meth = u.hostname.lower(), u.path, str(method).upper()
    if not isinstance(body, dict):
        _pim_block(f"{meth} {path} (no request body)")
    if host == PIM_ARM_HOST:
        m = re.match(r"^(?P<scope>(?:/[A-Za-z0-9._()\-]+)*)/providers/Microsoft\.Authorization/roleAssignmentScheduleRequests/(?P<id>[0-9a-fA-F\-]{36})$", path)
        if meth != "PUT" or not m or not _GUID_RE.match(m.group("id")) or not _PIM_SCOPE_RE.match(m.group("scope")) or ".." in path \
                or q != {"api-version": PIM_ARM_API}:
            _pim_block(f"{meth} {path}")
        if set(body) != {"properties"} or not isinstance(body["properties"], dict):
            _pim_block(f"PUT {path} (body shape)")
        p = body["properties"]
        need = {"principalId", "roleDefinitionId", "requestType", "justification", "scheduleInfo"}
        if not need <= set(p) or set(p) - need - {"linkedRoleEligibilityScheduleId", "ticketInfo"}:
            _pim_block(f"PUT {path} (properties other than the self-activation fields)")
        if p["requestType"] != "SelfActivate":
            _pim_block(f"requestType {p['requestType']}")
        if not _same_id(p["principalId"], me_oid):
            _pim_block("activation for another principal")
        if not isinstance(p["justification"], str) or not p["justification"].strip():
            _pim_block("activation without a justification")
        if not isinstance(p["roleDefinitionId"], str) or not re.search(r"/providers/Microsoft\.Authorization/roleDefinitions/[0-9a-fA-F\-]{36}$", p["roleDefinitionId"]):
            _pim_block("roleDefinitionId " + str(p["roleDefinitionId"])[:60])
        if "linkedRoleEligibilityScheduleId" in p and not isinstance(p["linkedRoleEligibilityScheduleId"], str):
            _pim_block("linkedRoleEligibilityScheduleId")
        if "ticketInfo" in p and not _pim_check_ticket(p["ticketInfo"]):
            _pim_block("ticketInfo")
        s = p["scheduleInfo"]
        if not isinstance(s, dict) or set(s) != {"startDateTime", "expiration"} or not isinstance(s["expiration"], dict) \
                or set(s["expiration"]) != {"type", "duration"} or s["expiration"]["type"] != "AfterDuration" \
                or not isinstance(s["expiration"]["duration"], str) or not _PIM_DUR_RE.match(s["expiration"]["duration"]):
            _pim_block("scheduleInfo")
        mins = _pim_iso_minutes(s["expiration"]["duration"])
        if not mins or (max_minutes and mins > max_minutes):
            _pim_block(f"duration {s['expiration']['duration']} (over the policy maximum)")
        return "pim-activate"
    if host == PIM_MSPIM_HOST:
        if meth != "POST" or not _PIM_MSPIM_POST_RE.match(path) or q:
            _pim_block(f"{meth} {path}")
        need = {"roleDefinitionId", "resourceId", "subjectId", "assignmentState", "type", "reason", "schedule", "linkedEligibleRoleAssignmentId"}
        if not need <= set(body) or set(body) - need - {"ticketNumber", "ticketSystem"}:
            _pim_block(f"POST {path} (fields other than the self-activation fields)")
        if body["type"] != "UserAdd":
            _pim_block(f"request type {body['type']}")
        if body["assignmentState"] != "Active":
            _pim_block(f"assignmentState {body['assignmentState']}")
        if not _same_id(body["subjectId"], me_oid):
            _pim_block("activation for another subject")
        if not isinstance(body["reason"], str) or not body["reason"].strip():
            _pim_block("activation without a justification")
        for k in ("roleDefinitionId", "resourceId", "linkedEligibleRoleAssignmentId"):
            if not isinstance(body[k], str) or not body[k].strip() or len(body[k]) > 200:
                _pim_block(f"{k} missing")
        if not _GUID_RE.match(body["resourceId"]):
            _pim_block("resourceId")
        for k in ("ticketNumber", "ticketSystem"):
            if k in body and not isinstance(body[k], str):
                _pim_block(k)
        s = body["schedule"]
        if not isinstance(s, dict) or set(s) != {"type", "startDateTime", "endDateTime"} or s["type"] != "Once":
            _pim_block("schedule")
        try:
            t0 = _dt.fromisoformat(str(s["startDateTime"]).replace("Z", "+00:00"))
            t1 = _dt.fromisoformat(str(s["endDateTime"]).replace("Z", "+00:00"))
        except ValueError:
            _pim_block("schedule dates")
        mins = (t1 - t0).total_seconds() / 60
        if mins <= 0 or mins > 30 * 1440 or (max_minutes and mins > max_minutes + 1):
            _pim_block("schedule length (over the policy maximum)")
        return "pim-activate"
    _pim_block(f"{meth} {host}{path}")


# --- transport: `az rest` (az signs the request; the token never reaches this program) ----------------------------------------

def _cmd_safe_json(body):
    """JSON text for the command line: every character cmd.exe could treat as an operator is written as a \\u escape (still valid JSON)."""
    text = json.dumps(body, separators=(",", ":"), ensure_ascii=True)
    return "".join("\\u%04x" % ord(c) if c in "&|<>^%()!" else c for c in text)


def _win_quote(arg):
    """Quote an argument for a cmd.exe command line (always quoted, so & | ( ) in a URL stay literal)."""
    bs = chr(92)
    q = re.sub("(" + re.escape(bs) + "*)\"", lambda m: m.group(1) * 2 + bs + "\"", arg)
    q = re.sub("(" + re.escape(bs) + "+)$", lambda m: m.group(1) * 2, q)
    return '"' + q + '"'


def _pim_exec(cmd, timeout=90):
    run = cmd
    if os.name == "nt" and str(cmd[0]).lower().endswith((".cmd", ".bat")):
        run = " ".join(_win_quote(str(a)) for a in cmd)
    return subprocess.run(run, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout, env=_az_env())


_pim_sleep = time.sleep


def pim_error_text(err, n=700):
    """The real error, trimmed: HTTP status name + the response body (tokens scrubbed)."""
    text = scrub_secrets(str(err or "").strip())
    text = re.sub(r"^ERROR:\s*", "", text)
    return text if len(text) <= n else text[:n] + " ..."


def pim_error_status(err):
    text = str(err or "")
    names = (("Bad Request", 400), ("Unauthorized", 401), ("Forbidden", 403), ("Not Found", 404), ("Conflict", 409), ("Too Many Requests", 429),
             ("Internal Server Error", 500), ("Bad Gateway", 502), ("Service Unavailable", 503))
    for name, code in names:
        if name.lower() in text.lower():
            return code
    m = re.search(r"\b(?:HTTP|status)[ :]*(\d{3})\b", text)
    if m:
        return int(m.group(1))
    if "aadsts" in text.lower():
        return 401
    return None


def _pim_call(method, url, body=None, resource=None, timeout=90):
    """Run ONE az rest call (already allow-list checked by the caller). Returns (json, None) or (None, error text). HTTP 429 is retried with back-off."""
    exe = shutil.which("az")
    if not exe:
        return None, "Azure CLI (az) was not found on PATH"
    cmd = [exe, "rest", "--method", method.lower(), "--url", url]
    if resource:
        cmd += ["--resource", resource]
    if body is not None:
        cmd += ["--body", _cmd_safe_json(body)]
    cmd += ["--only-show-errors", "-o", "json"]
    err = None
    for attempt in range(len(PIM_THROTTLE_WAITS) + 1):
        try:
            proc = _pim_exec(cmd, timeout)
        except subprocess.TimeoutExpired:
            return None, f"timed out after {timeout}s"
        except Exception as exc:
            return None, str(exc)
        if proc.returncode == 0:
            try:
                return (json.loads(proc.stdout) if proc.stdout.strip() else {}), None
            except json.JSONDecodeError:
                return proc.stdout.strip(), None
        err = (proc.stderr or proc.stdout or "").strip() or f"az rest failed (exit {proc.returncode})"
        if _PIM_THROTTLE_RE.search(err) and attempt < len(PIM_THROTTLE_WAITS):
            _pim_sleep(PIM_THROTTLE_WAITS[attempt])
            continue
        break
    if is_expired_error(err):
        _note_expired(["rest"], None, err)
    return None, err


def pim_read(url, resource=None):
    """GET through the PIM allow-list. (json, None) or (None, error)."""
    try:
        assert_pim_get(url, resource)
    except ReadOnlyViolation as v:
        return None, str(v)
    return _pim_call("GET", url, None, resource)


def pim_write(method, url, body, resource=None, me_oid=None, max_minutes=None):
    """THE ONLY WRITE of the tool: a self-activation request, checked by assert_pim_write() first. Report runs never call this function."""
    try:
        assert_pim_write(method, url, body, me_oid, max_minutes)
    except ReadOnlyViolation as v:
        return None, str(v)
    if urlsplit_host(url) == PIM_MSPIM_HOST and resource != PIM_MSPIM_RESOURCE:
        return None, _blocked_message("POST (wrong --resource)")
    return _pim_call(method, url, body, resource)


def urlsplit_host(url):
    from urllib.parse import urlsplit
    return (urlsplit(url).hostname or "").lower()


# --- who am I ------------------------------------------------------------------------------------------------------------

def pim_identity(force=False):
    """{"ok", "upn", "tenant", "oid", "kind": user|guest|servicePrincipal|managedIdentity, "error", "notice"} from `az account show` and `az ad signed-in-user show`."""
    with PIM_STATE["lock"]:
        if PIM_STATE["me"] and not force and PIM_STATE["me"].get("ok"):
            return dict(PIM_STATE["me"])
    me = {"ok": False, "upn": None, "tenant": None, "oid": None, "kind": "user", "error": None, "notice": None}
    show, err = az_cli(["account", "show"], None, 60, subscription=False)
    if err or not isinstance(show, dict):
        me["error"] = "Not signed in to the Azure CLI (" + _first_line(err or "no answer from az account show") + "). Sign in on tab 1 first."
        return me
    user = show.get("user") or {}
    me.update(upn=user.get("name"), tenant=show.get("tenantId"))
    utype = (user.get("type") or "user").lower()
    if utype != "user":
        me["kind"] = "managedIdentity" if "managed" in utype or utype == "systemassigned" or utype == "userassigned" else "servicePrincipal"
        me["error"] = "PIM is not available for a service principal / managed identity (" + str(me["upn"]) + "): it is for people. Sign in with your own account."
        return me
    if "#ext#" in str(me["upn"]).lower():
        me["kind"] = "guest"
        me["notice"] = "This looks like a guest account (" + str(me["upn"]) + "). PIM is usually not available for guests of another tenant; if nothing is listed, sign in with your organisation account."
    oid, err2 = az_cli(["ad", "signed-in-user", "show", "--query", "id"], None, 60, subscription=False)
    if isinstance(oid, str) and _GUID_RE.match(oid.strip().strip('"')):
        me["oid"] = oid.strip().strip('"')
    me["ok"] = True
    me["oid_error"] = None if me["oid"] else _first_line(err2 or "az ad signed-in-user show returned no id")
    with PIM_STATE["lock"]:
        PIM_STATE["me"] = dict(me)
    return me


# --- parsing -------------------------------------------------------------------------------------------------------------

def _pim_dt(value):
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    s = re.sub(r"(\.\d{6})\d+", r"\1", s)
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def pim_left_text(end, now=None):
    if end is None:
        return "no end date"
    now = now or datetime.now(timezone.utc)
    secs = int((end - now).total_seconds())
    if secs <= 0:
        return "expired"
    d, rem = divmod(secs, 86400)
    h, m = divmod(rem // 60, 60)
    return (f"{d} d " if d else "") + (f"{h} h " if h else "") + (f"{m} min" if (m or not (d or h)) else "")


def pim_expiry_text(end, active, now=None):
    if end is None:
        return "Permanent" if active else "No end date"
    local = end.astimezone()
    stamp = local.strftime("%Y-%m-%d %H:%M")
    return f"{stamp}  ({pim_left_text(end, now)} left)" if active else f"until {stamp}"


def pim_max_text(minutes):
    if not minutes:
        return "unknown (1 h is requested)"
    h, m = divmod(int(minutes), 60)
    return f"{h} h" + (f" {m} min" if m else "") if h else f"{m} min"


def _json_setting(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return {}
    return v if isinstance(v, dict) else {}


def pim_arm_policy(rules):
    """Maximum activation minutes and what an ARM role policy asks, from its rules (roleManagementPolicyAssignment effectiveRules / policy rules)."""
    by = {r.get("id"): r for r in (rules or []) if isinstance(r, dict)}
    mx = _pim_iso_minutes((by.get("Expiration_EndUser_Assignment") or {}).get("maximumDuration"))
    need = []
    en = (by.get("Enablement_EndUser_Assignment") or {}).get("enabledRules") or []
    if "Justification" in en:
        need.append("justification")
    if "Ticketing" in en:
        need.append("ticket")
    if (((by.get("Approval_EndUser_Assignment") or {}).get("setting")) or {}).get("isApprovalRequired"):
        need.append("approval")
    if "MultiFactorAuthentication" in en:
        need.append("multi-factor authentication")
    if (by.get("AuthenticationContext_EndUser_Assignment") or {}).get("isEnabled"):
        need.append("authentication context")
    return {"max_minutes": mx, "requires": need}


def pim_mspim_policy(settings):
    """Same for the PIM service's roleSettingsV2 (userMemberSettings: ExpirationRule, JustificationRule, TicketingRule, MfaRule, ApprovalRule, AcrsRule)."""
    mx, need = None, []
    for r in settings or []:
        rid, s = r.get("ruleIdentifier"), _json_setting(r.get("setting"))
        if rid == "ExpirationRule":
            v = s.get("maximumGrantPeriodInMinutes")
            if v is None and s.get("maximumGrantPeriod"):
                v = _pim_iso_minutes(s.get("maximumGrantPeriod"))
            mx = int(v) if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()) else mx
        elif rid == "JustificationRule" and s.get("required"):
            need.append("justification")
        elif rid == "TicketingRule" and s.get("ticketingRequired"):
            need.append("ticket")
        elif rid == "ApprovalRule" and (s.get("enabled") or s.get("isApprovalRequired") or s.get("approvers") or s.get("Approvers")):
            need.append("approval")
        elif rid == "MfaRule" and s.get("mfaRequired"):
            need.append("multi-factor authentication")
        elif rid == "AcrsRule" and (s.get("acrsRequired") or s.get("enabled")):
            need.append("authentication context")
    return {"max_minutes": mx, "requires": need}


def _arm_scope_text(scope_id, ep_scope):
    names = {"subscription": "Subscription", "resourcegroup": "Resource group", "managementgroup": "Management group"}
    sid = scope_id or "/"
    last = sid.rstrip("/").split("/")[-1] or "/"
    kind = (ep_scope or {}).get("type") or ""
    if not kind:
        kind = "subscription" if re.fullmatch(r"/subscriptions/[^/]+", sid) else "resourcegroup" if re.fullmatch(r"/subscriptions/[^/]+/resourceGroups/[^/]+", sid) \
            else "managementgroup" if "/managementGroups/" in sid else ""
    name = (ep_scope or {}).get("displayName") or last
    label = names.get(kind.lower(), "Resource" if kind else "")
    return f"{label}: {name}" if label else name


def _row_base(family, key, name, scope_text, scope_id, status, end, active):
    return {"key": key, "family": family, "type": PIM_TYPE[family], "name": name, "scope": scope_text, "scope_id": scope_id, "status": status, "end": end,
            "expires": pim_expiry_text(end, active), "max_minutes": None, "max": "", "requires": [], "requires_text": "", "role_def_id": None, "resource_id": None,
            "link_id": None, "access_id": None, "assignment": None}


def pim_arm_rows(items, active, now=None):
    now = now or datetime.now(timezone.utc)
    rows = []
    for it in items or []:
        p = it.get("properties") or {}
        ep = p.get("expandedProperties") or {}
        sid = p.get("scope") or (ep.get("scope") or {}).get("id") or "/"
        rd = p.get("roleDefinitionId") or (ep.get("roleDefinition") or {}).get("id") or ""
        guid = rd.rstrip("/").split("/")[-1].lower()
        name = (ep.get("roleDefinition") or {}).get("displayName") or ""
        end = _pim_dt(p.get("endDateTime"))
        status = "Active" if active else "Eligible"
        if active and end is not None and (end - now).total_seconds() < EXPIRING_SOON_SECONDS and p.get("assignmentType") != "Assigned":
            status = "Expired soon"
        r = _row_base("arm", f"arm|{sid.lower()}|{guid}", name or guid, _arm_scope_text(sid, ep.get("scope")), sid, status, end, active)
        r.update(role_def_id=rd, principal_id=p.get("principalId"), assignment=p.get("assignmentType") or ("Eligible" if not active else None),
                 link_id=p.get("linkedRoleEligibilityScheduleId") or p.get("roleEligibilityScheduleId"),
                 needs_name=not name)
        if active and p.get("assignmentType") == "Assigned" and end is None:
            r["expires"] = "Permanent (not activated through PIM)"
        rows.append(r)
    return rows


def pim_mspim_rows(kind, items, active, now=None):
    """Rows for the PIM service's roleAssignments (aadroles = Entra roles, aadGroups = privileged access groups)."""
    now = now or datetime.now(timezone.utc)
    fam = "entra" if kind == "entra" else "group"
    rows = []
    for it in items or []:
        rdef = it.get("roleDefinition") or {}
        res = rdef.get("resource") or it.get("resource") or {}
        rd_id = it.get("roleDefinitionId") or rdef.get("id") or ""
        res_id = it.get("resourceId") or rdef.get("resourceId") or res.get("id") or ""
        end = _pim_dt(it.get("endDateTime"))
        status = "Active" if active else "Eligible"
        if active and end is not None and (end - now).total_seconds() < EXPIRING_SOON_SECONDS:
            status = "Expired soon"
        if fam == "entra":
            scoped = it.get("scopedResource") or {}
            sc_id = it.get("scopedResourceId") or ""
            scope_text = f"{scoped.get('displayName') or sc_id}  (administrative scope)" if sc_id and sc_id.lower() != res_id.lower() else "Directory (whole tenant)"
            name = rdef.get("displayName") or rd_id
            key = f"entra|{rd_id.lower()}|{sc_id.lower()}"
        else:
            access = (rdef.get("displayName") or rdef.get("externalId") or "member").strip()
            name = res.get("displayName") or res_id
            scope_text = ("Owner of the group" if access.lower().startswith("owner") else "Member of the group")
            key = f"group|{res_id.lower()}|{access.lower()}"
        r = _row_base(fam, key, name, scope_text, res_id, status, end, active)
        r.update(role_def_id=rd_id, resource_id=res_id, link_id=it.get("linkedEligibleRoleAssignmentId") if active else it.get("id"),
                 assignment=it.get("memberType") or ("Eligible" if not active else None), eligible_id=it.get("id"),
                 access_id="owner" if fam == "group" and access.lower().startswith("owner") else ("member" if fam == "group" else None))
        rows.append(r)
    return rows


def pim_merge(active_rows, eligible_rows, pending_keys=()):
    """(active, eligible): eligible roles that are already active are dropped from the eligible list (nothing to request); eligible roles with a
    request waiting for approval are marked 'Pending approval'."""
    act_keys = {r["key"] for r in active_rows}
    out = []
    for r in eligible_rows:
        if r["key"] in act_keys:
            continue
        if r["key"] in pending_keys:
            r = dict(r, status="Pending approval")
        out.append(r)
    return active_rows, out


def _pim_pages(url, resource=None):
    items, err = [], None
    for _ in range(PIM_MAX_PAGES):
        data, err = pim_read(url, resource)
        if err:
            return items, err
        if isinstance(data, dict):
            items += [x for x in (data.get("value") or []) if isinstance(x, dict)]
            url = data.get("nextLink") or data.get("odata.nextLink") or data.get("@odata.nextLink")
        else:
            url = None
        if not url:
            break
    return items, None


def _pim_arm_list_url(name):
    return f"https://{PIM_ARM_HOST}/providers/Microsoft.Authorization/{name}?api-version={PIM_ARM_API}&$filter=asTarget()"


def pim_arm_policy_for(row):
    """Read the policy of one eligible Azure role (one GET): returns {"max_minutes", "requires"} or None."""
    sid = row["scope_id"] if row["scope_id"] != "/" else ""
    url = (f"https://{PIM_ARM_HOST}{sid}/providers/Microsoft.Authorization/roleManagementPolicyAssignments?api-version={PIM_ARM_API}"
           f"&$filter=roleDefinitionId eq '{row['role_def_id']}'")
    data, err = pim_read(url)
    if err or not isinstance(data, dict):
        return None
    vals = [v for v in (data.get("value") or []) if isinstance(v, dict)]
    best = next((v for v in vals if str((v.get("properties") or {}).get("scope", "")).lower() == (row["scope_id"] or "").lower()), vals[0] if vals else None)
    if not best:
        return None
    props = best.get("properties") or {}
    rules = props.get("effectiveRules")
    if not rules and props.get("policyId"):
        pol, err = pim_read(f"https://{PIM_ARM_HOST}{props['policyId']}?api-version={PIM_ARM_API}")
        rules = ((pol or {}).get("properties") or {}).get("effectiveRules") or ((pol or {}).get("properties") or {}).get("rules") if isinstance(pol, dict) else None
    return pim_arm_policy(rules) if rules else None


def pim_mspim_policy_for(row):
    res = "aadroles" if row["family"] == "entra" else "aadGroups"
    url = (f"https://{PIM_MSPIM_HOST}{PIM_MSPIM_PREFIX}{res}/roleSettingsV2?$filter=(resource/id eq '{row['resource_id']}') and "
           f"(roleDefinition/id eq '{row['role_def_id']}')")
    data, err = pim_read(url, PIM_MSPIM_RESOURCE)
    if err or not isinstance(data, dict):
        return None
    vals = [v for v in (data.get("value") or []) if isinstance(v, dict)]
    if not vals:
        return None
    return pim_mspim_policy(vals[0].get("userMemberSettings") or vals[0].get("userMemberSetting") or [])


def pim_enrich(rows, workers=PIM_WORKERS):
    """Fill the maximum duration and 'Requires' of the eligible rows (one policy read per row, 4 at a time). A failed read leaves them empty."""
    def one(r):
        try:
            pol = pim_arm_policy_for(r) if r["family"] == "arm" else pim_mspim_policy_for(r)
        except Exception:
            pol = None
        if pol:
            r["max_minutes"], r["requires"] = pol["max_minutes"], pol["requires"]
        r["max"] = pim_max_text(r["max_minutes"])
        r["requires_text"] = ", ".join(r["requires"]) if r["requires"] else ("" if pol else "not readable")
    if rows:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            list(ex.map(one, rows))
    return rows


def _pim_fix_arm_names(rows):
    for r in rows:
        if r.pop("needs_name", False):
            sid = r["scope_id"] if r["scope_id"] != "/" else ""
            data, err = pim_read(f"https://{PIM_ARM_HOST}{sid}/providers/Microsoft.Authorization/roleDefinitions/{r['name']}?api-version=2022-04-01")
            nm = (((data or {}).get("properties") or {}).get("roleName")) if isinstance(data, dict) else None
            if nm:
                r["name"] = nm


def pim_load_arm(me):
    out = {"state": "ok", "error": None, "active": [], "eligible": [], "pending": set(), "note": ""}
    elig, err = _pim_pages(_pim_arm_list_url("roleEligibilityScheduleInstances"))
    if err:
        out.update(state="error", error=pim_error_text(err))
    act, err2 = _pim_pages(_pim_arm_list_url("roleAssignmentScheduleInstances"))
    if err2:
        out.update(state="error", error=pim_error_text(err2) if not err else out["error"])
    reqs, _e = _pim_pages(_pim_arm_list_url("roleAssignmentScheduleRequests"))
    for it in reqs:
        p = it.get("properties") or {}
        if str(p.get("status", "")).lower().startswith("pendingapproval") and str(p.get("requestType", "SelfActivate")).lower() == "selfactivate":
            guid = str(p.get("roleDefinitionId", "")).rstrip("/").split("/")[-1].lower()
            out["pending"].add(f"arm|{str(p.get('scope', '')).lower()}|{guid}")
    out["active"] = pim_arm_rows(act, True)
    out["eligible"] = pim_arm_rows(elig, False)
    for it in (elig + act):                       # the object id for the other families, when az ad signed-in-user could not give it
        pid = (it.get("properties") or {}).get("principalId")
        if pid and _GUID_RE.match(str(pid)):
            out["principal_id"] = pid
            break
    _pim_fix_arm_names(out["active"] + out["eligible"])
    return out


def _mspim_variants(oid, state):
    f = f"(subject/id eq '{oid}') and (assignmentState eq '{state}')"
    return ["$expand=linkedEligibleRoleAssignment,subject,scopedResource,roleDefinition($expand=resource)&$filter=" + f,
            "$expand=roleDefinition($expand=resource)&$filter=" + f,
            "$filter=" + f]


def pim_load_mspim(kind, me):
    res = "aadroles" if kind == "entra" else "aadGroups"
    out = {"state": "ok", "error": None, "active": [], "eligible": [], "pending": set(), "note": ""}
    base = f"https://{PIM_MSPIM_HOST}{PIM_MSPIM_PREFIX}{res}/roleAssignments"
    lists = {}
    for st in ("Eligible", "Active"):
        vs = _mspim_variants(me["oid"], st)
        first = PIM_STATE["variant"].get(kind)
        order = ([first] if first is not None else []) + [i for i in range(len(vs)) if i != first]
        err = None
        for i in order:
            items, err = _pim_pages(base + "?" + vs[i], PIM_MSPIM_RESOURCE)
            if not err:
                PIM_STATE["variant"][kind] = i                  # the working method is remembered
                lists[st] = items
                break
            if pim_error_status(err) in (401, 403):             # a different query will not help: the service itself refuses
                break
        if err:
            out.update(state="error", error=pim_error_text(err))
            return out
    out["active"] = pim_mspim_rows(kind, lists["Active"], True)
    out["eligible"] = pim_mspim_rows(kind, [x for x in lists["Eligible"]], False)
    return out


def pim_collect(me, emit=None):
    """Read-only: the three families side by side. -> {"active": [...], "eligible": [...], "families": {fam: {state, text, error}}, "me": me}."""
    say = emit or (lambda _l: None)
    me = dict(me)
    res = {}
    if not me.get("oid"):                      # no object id from az ad signed-in-user: the ARM answer carries it
        res["arm"] = pim_load_arm(me)
        me["oid"] = res["arm"].get("principal_id")
        if me["oid"]:
            with PIM_STATE["lock"]:
                if PIM_STATE["me"]:
                    PIM_STATE["me"]["oid"] = me["oid"]
    tasks = {}
    if "arm" not in res:
        tasks["arm"] = lambda: pim_load_arm(me)
    for fam in ("entra", "group"):
        tasks[fam] = (lambda f=fam: pim_load_mspim(f, me)) if me.get("oid") else (
            lambda f=fam: {"state": "error", "error": "Your object id could not be read (az ad signed-in-user show failed: " + str(me.get("oid_error")) + ")",
                           "active": [], "eligible": [], "pending": set()})
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(fn): fam for fam, fn in tasks.items()}
        for f in as_completed(futs):
            try:
                res[futs[f]] = f.result()
            except Exception as exc:
                res[futs[f]] = {"state": "error", "error": str(exc), "active": [], "eligible": [], "pending": set()}
    pending = set(PIM_STATE["pending"])
    for r in res.values():
        pending |= r.get("pending", set())
    active, eligible, fams = [], [], {}
    for fam in PIM_FAMILIES:
        r = res[fam]
        a, e = pim_merge(r["active"], r["eligible"], pending)
        active += a
        eligible += e
        if r["state"] == "ok":
            pend = len([x for x in e if x["status"] == "Pending approval"])
            fams[fam] = {"state": "ok", "text": f"OK ({len(a)} active, {len(e) - pend} eligible" + (f", {pend} pending approval" if pend else "") + ")", "error": None}
        else:
            fams[fam] = {"state": "error", "text": "Failed - see the message", "error": r["error"]}
        say(f"PIM {PIM_FAMILY_NAME[fam]}: " + (fams[fam]["text"] if r["state"] == "ok" else "FAILED: " + str(r["error"])))
    pim_enrich([r for r in eligible if r["status"] == "Eligible"])
    order = {"arm": 0, "entra": 1, "group": 2}
    active.sort(key=lambda r: (order[r["family"]], r["name"].lower()))
    eligible.sort(key=lambda r: (order[r["family"]], r["name"].lower()))
    return {"active": active, "eligible": eligible, "families": fams, "me": me}


def pim_no_eligible_text(result):
    """Plain explanation for 'nothing eligible' (shown only when no family listed any eligible role)."""
    ok = [f for f, v in result["families"].items() if v["state"] == "ok"]
    if result["eligible"] or not ok:
        return ""
    return ("No eligible roles were found for " + str(result["me"].get("upn")) + ". Likely causes: you are not eligible for anything through PIM, you are signed in to a different tenant "
            "than the one that holds your eligibility, PIM is not enabled for your roles, or the account lacks permission to read them. Check with `az account show`.")


# --- activation ----------------------------------------------------------------------------------------------------------

def pim_effective_minutes(max_minutes, hours):
    """Minutes to request: the policy maximum, or the hours typed - never more than the maximum. Unknown maximum and no hours typed: 1 hour."""
    typed = None if hours in (None, "") else max(1, int(round(float(hours) * 60)))
    if max_minutes:
        return min(typed, max_minutes) if typed else max_minutes
    return typed or PIM_UNKNOWN_MAX_MINUTES


def pim_build_request(row, me, justification, hours=None, ticket_number="", ticket_system="", now=None, request_id=None):
    """-> (method, url, body, resource, minutes) for ONE eligible row. Pure: nothing is sent."""
    now = now or datetime.now(timezone.utc)
    mins = pim_effective_minutes(row.get("max_minutes"), hours)
    ticket_number, ticket_system = (ticket_number or "").strip(), (ticket_system or "").strip()
    if row["family"] == "arm":
        sid = row["scope_id"] if row["scope_id"] != "/" else ""
        props = {"principalId": me["oid"], "roleDefinitionId": row["role_def_id"], "requestType": "SelfActivate", "justification": justification,
                 "scheduleInfo": {"startDateTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "expiration": {"type": "AfterDuration", "duration": pim_duration_text(mins)}}}
        if row.get("link_id"):
            props["linkedRoleEligibilityScheduleId"] = row["link_id"]
        if ticket_number or ticket_system:
            props["ticketInfo"] = {"ticketNumber": ticket_number, "ticketSystem": ticket_system}
        url = f"https://{PIM_ARM_HOST}{sid}/providers/Microsoft.Authorization/roleAssignmentScheduleRequests/{request_id or uuid.uuid4()}?api-version={PIM_ARM_API}"
        return "PUT", url, {"properties": props}, None, mins
    res = "aadroles" if row["family"] == "entra" else "aadGroups"
    body = {"roleDefinitionId": row["role_def_id"], "resourceId": row["resource_id"], "subjectId": me["oid"], "assignmentState": "Active", "type": "UserAdd",
            "reason": justification, "ticketNumber": ticket_number, "ticketSystem": ticket_system,
            "schedule": {"type": "Once", "startDateTime": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                         "endDateTime": (now + timedelta(minutes=mins)).strftime("%Y-%m-%dT%H:%M:%S.000Z")},
            "linkedEligibleRoleAssignmentId": row.get("link_id") or row.get("eligible_id") or ""}
    return "POST", f"https://{PIM_MSPIM_HOST}{PIM_MSPIM_PREFIX}{res}/roleAssignmentRequests", body, PIM_MSPIM_RESOURCE, mins


def pim_classify_error(err):
    """(outcome, reason) for a failed request: Already active / Pending approval / Denied + the service's reason."""
    text = pim_error_text(err, 400)
    low = text.lower()
    if "roleassignmentexists" in low or "already exists" in low or "already active" in low or "activeduration" in low:
        return "Already active", "RoleAssignmentExists - the role is already active"
    if _PIM_THROTTLE_RE.search(text):
        return "Denied", "Throttled by the service (HTTP 429) after 3 retries - wait a minute and press Refresh, then activate again. " + text
    if any(x in low for x in ("multifactorauthenticationrule", "mfarule", "multi-factor", "multifactor", "aadsts50076", "aadsts50079", "aadsts50158", "authenticationcontext",
                              "acrsrule", "claims challenge", "interaction_required", "aadsts53003")):
        return "Denied", "Authentication context / multi-factor authentication required - complete it in the Azure portal, then try again. " + text
    if "justificationrule" in low or "justification" in low and "required" in low:
        return "Denied", "The role policy requires a justification. " + text
    if "ticketingrule" in low or "ticket" in low and "required" in low:
        return "Denied", "The role policy requires a ticket number and system. " + text
    if "expirationrule" in low or "maximum" in low and "duration" in low or "exceeds" in low:
        return "Denied", "Policy limit: the duration is longer than the role policy allows. " + text
    if "pendingapproval" in low:
        return "Pending approval", "An approver must approve the request."
    if any(x in low for x in ("forbidden", "unauthorized", "authorizationfailed", "aadsts", "401", "403")):
        return "Denied", "Access denied - " + text
    return "Denied", text


def pim_classify_response(data):
    """(outcome, reason) for an accepted request, from the status the service returns."""
    status = None
    if isinstance(data, dict):
        p = data.get("properties") if isinstance(data.get("properties"), dict) else data
        st = p.get("status")
        status = (st.get("status") or st.get("subStatus")) if isinstance(st, dict) else st
        detail = st.get("statusDetails") if isinstance(st, dict) else None
    else:
        detail = None
    low = str(status or "").lower()
    if low.startswith("pendingapproval") or low in ("pendingadmindecision", "pendingevaluation") and "approval" in json.dumps(data).lower():
        return "Pending approval", "Waiting for an approver (status " + str(status) + ")"
    if low in ("denied", "failed", "canceled", "cancelled", "revoked", "admindenied", "timedout", "invalid", "failedasresourceisinlocked"):
        return "Denied", f"The service answered status {status}" + (f": {json.dumps(detail)[:200]}" if detail else "")
    return "Activated", "" if not low else f"status {status}"


def pim_activate_one(row, me, justification, hours=None, ticket_number="", ticket_system="", now=None):
    """Send ONE self-activation request. Returns the result dict."""
    res = {"key": row["key"], "type": row["type"], "name": row["name"], "scope": row["scope"], "outcome": None, "reason": "", "minutes": None}
    try:
        method, url, body, resource, mins = pim_build_request(row, me, justification, hours, ticket_number, ticket_system, now)
    except Exception as exc:
        res.update(outcome="Denied", reason=f"could not build the request: {exc}")
        return res
    res["minutes"] = mins
    data, err = pim_write(method, url, body, resource, me.get("oid"), row.get("max_minutes"))
    if err and err.startswith("blocked: read-only mode"):
        res.update(outcome="Denied", reason=err)
        return res
    GUARD.pim_record({"time": datetime.now(timezone.utc).strftime("%H:%M:%S"), "type": row["type"], "name": row["name"], "scope": row["scope"], "minutes": mins})
    if err:
        res["outcome"], res["reason"] = pim_classify_error(err)
    else:
        res["outcome"], res["reason"] = pim_classify_response(data)
    if res["outcome"] == "Pending approval":
        with PIM_STATE["lock"]:
            PIM_STATE["pending"].add(row["key"])
    return res


def pim_activate_many(rows, me, justification, hours=None, ticket_number="", ticket_system="", on_start=None, on_result=None, cancel=None, emit=None, workers=PIM_WORKERS):
    """Self-activate the eligible rows: 4 at a time. Rows that are already active or waiting for approval are skipped (no request is sent).
    Raises ValueError without a justification. Returns the list of result dicts in the order of `rows`."""
    if not (justification or "").strip():
        raise ValueError("a justification is required")
    say = emit or (lambda _l: None)
    results = [None] * len(rows)
    todo = []
    for i, r in enumerate(rows):
        if r["status"] in ("Active", "Expired soon"):
            results[i] = {"key": r["key"], "type": r["type"], "name": r["name"], "scope": r["scope"], "outcome": "Already active", "reason": "already active - skipped, no request sent", "minutes": None}
            say(f"PIM skipped '{r['name']}' ({r['type']}): already active")
            if on_result:
                on_result(results[i])
        elif r["status"] == "Pending approval":
            results[i] = {"key": r["key"], "type": r["type"], "name": r["name"], "scope": r["scope"], "outcome": "Pending approval", "reason": "a request is already waiting for approval - skipped", "minutes": None}
            if on_result:
                on_result(results[i])
        else:
            todo.append(i)

    def work(i):
        r = rows[i]
        if cancel is not None and cancel.is_set():
            return i, {"key": r["key"], "type": r["type"], "name": r["name"], "scope": r["scope"], "outcome": "Denied", "reason": "stopped before this request was sent", "minutes": None}
        if on_start:
            on_start(r)
        say(f"PIM requesting activation of '{r['name']}' ({r['type']}, {r['scope']}) ...")
        res = pim_activate_one(r, me, justification, hours, ticket_number, ticket_system)
        say(f"PIM {r['name']} ({r['type']}): {res['outcome']}" + (f" - {res['reason']}" if res["reason"] else ""))
        return i, res
    with ThreadPoolExecutor(max_workers=max(1, min(workers, PIM_WORKERS))) as ex:
        for f in as_completed([ex.submit(work, i) for i in todo]):
            i, res = f.result()
            results[i] = res
            if on_result:
                on_result(res)
    return results


def pim_summary(results):
    c = Counter(r["outcome"] for r in results)
    return ", ".join(f"{c[k]} {k.lower()}" for k in ("Activated", "Already active", "Pending approval", "Denied") if c.get(k)) or "nothing requested"


def pim_confirm_text(n):
    return f"This submits {n} self-activation request{'' if n == 1 else 's'} for roles you are already eligible for. It does not grant new access."


# --- command line ---------------------------------------------------------------------------------------------------------

def pim_table_text(rows, title):
    lines = [title]
    if not rows:
        return lines + ["  (none)"]
    for r in rows:
        lines.append(f"  [{r['type']}] {r['name']}  |  {r['scope']}  |  {r['status']}  |  {r['expires']}  |  max {r['max'] or '-'}"
                     + (f"  |  requires: {r['requires_text']}" if r.get("requires_text") else ""))
    return lines


def pim_cli(args):
    """--pim-list / --pim-activate-all. Returns the exit code."""
    out = lambda l: print(l, flush=True)
    me = pim_identity(force=True)
    if not me["ok"]:
        print(me["error"], file=sys.stderr)
        return 1
    if me.get("notice"):
        out("NOTE: " + me["notice"])
    out(f"Privileged roles for {me['upn']} (tenant {me['tenant']})")
    res = pim_collect(me, lambda l: out("  " + l))
    for fam in PIM_FAMILIES:
        f = res["families"][fam]
        out(f"  {PIM_FAMILY_NAME[fam]}: {f['text']}" + (f"  {f['error']}" if f["error"] else ""))
    for line in pim_table_text(res["active"], f"ACTIVE now ({len(res['active'])})") + pim_table_text(res["eligible"], f"ELIGIBLE, not active ({len(res['eligible'])})"):
        out(line)
    why = pim_no_eligible_text(res)
    if why:
        out(why)
    for fam in ("entra", "group"):
        if res["families"][fam]["state"] == "error":
            out(f"{PIM_FAMILY_NAME[fam]} could not be read automatically. Open {PIM_PORTAL_URLS[fam]} in your browser, or use {PIM_SCRIPT_HINT}.")
    if not args.pim_activate_all:
        return 0
    todo = [r for r in res["eligible"] if r["status"] == "Eligible"]
    if not todo:
        out("Nothing to activate.")
        return 0
    out("")
    out(pim_confirm_text(len(todo)))
    for r in todo:
        out(f"  would request: {r['name']} ({r['type']}, {r['scope']}) for {pim_max_text(pim_effective_minutes(r['max_minutes'], args.hours))}")
    if not args.yes:
        out("Nothing was submitted. Add --yes to submit these requests.")
        return 0
    results = pim_activate_many(todo, me, args.justification, args.hours, args.ticket_number or "", args.ticket_system or "", emit=lambda l: out("  " + l))
    out("")
    out("RESULT: " + pim_summary(results))
    for r in results:
        out(f"  {r['outcome']:<16} {r['name']} ({r['type']})" + (f" - {r['reason']}" if r["reason"] else ""))
    return 1 if any(r["outcome"] == "Denied" for r in results) else 0


# ---------------------------------------------------------------------------
# GUI: live, interactive collection
# ---------------------------------------------------------------------------

_GUI = {}   # widgets of the running window (used by the tests)
PREFS_FILE = os.path.join(_HERE, "aks_debug_gui.json")     # the section choice and the worker count are remembered here (and for the session)
_SESSION = {"sections": None, "workers": None, "signin": None, "subs": None}      # remembered while the program runs: sections, workers, sign-in method, chosen subscriptions


def _load_prefs():
    try:
        with open(PREFS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        ids = [s for s in (data.get("sections") or []) if s in SECTION_BY_ID]
        return {"sections": ids or None, "workers": int(data["workers"]) if str(data.get("workers", "")).isdigit() else None}
    except Exception:
        return {"sections": None, "workers": None}


def _save_prefs(sections, workers):
    _SESSION.update(sections=list(sections), workers=workers)
    try:
        with open(PREFS_FILE, "w", encoding="utf-8") as f:
            json.dump({"sections": list(sections), "workers": workers}, f)
    except Exception:
        pass


def _blend(c1, c2, t):
    """Colour between two '#rrggbb' colours (t = 0 -> c1, 1 -> c2)."""
    a, b = [tuple(int(c[i:i + 2], 16) for i in (1, 3, 5)) for c in (c1, c2)]
    return "#%02x%02x%02x" % tuple(round(x + (y - x) * t) for x, y in zip(a, b))


class _Tip:
    """A small tooltip for a Tk widget."""

    def __init__(self, widget, text):
        import tkinter as tk
        self.widget, self.text, self.tw, self.tk = widget, text, None, tk
        widget.bind("<Enter>", self.show, add="+")
        widget.bind("<Leave>", self.hide, add="+")

    def show(self, _e=None):
        if self.tw or not self.text:
            return
        tk = self.tk
        self.tw = tk.Toplevel(self.widget)
        self.tw.wm_overrideredirect(True)
        self.tw.wm_geometry("+%d+%d" % (self.widget.winfo_rootx() + 18, self.widget.winfo_rooty() + self.widget.winfo_height() + 2))
        tk.Label(self.tw, text=self.text, bg="#FFFBE6", fg="#1B2A3A", relief="solid", borderwidth=1, padx=6, pady=3, wraplength=420, justify="left").pack()

    def hide(self, _e=None):
        if self.tw:
            self.tw.destroy()
            self.tw = None


def run_gui(default_minutes, skip_login=False, context=None):
    import pathlib
    import tkinter as tk
    import webbrowser
    from tkinter import ttk, scrolledtext

    root = tk.Tk()
    root.title(PRODUCT_NAME)
    root.geometry("1280x860")
    root.minsize(1100, 700)
    if os.environ.get("AKS_DEBUG_ASCII_ICONS"):
        _ICON_PLAIN[0] = True
    else:
        try:                    # emoji need a Tk that can show characters above U+FFFF; otherwise every icon falls back to plain text
            tk.Label(root, text="\U0001F50D").destroy()
            _ICON_PLAIN[0] = False
        except Exception:
            _ICON_PLAIN[0] = True
    msgs = queue.Queue()
    state = {"busy": False, "cancel": None, "html": None, "t0": None, "finished": 0, "total": 1,
             "counts": Counter(), "clusters": {}, "reports": {}, "n": 1,
             "auth": {"state": "unchecked"}, "checking": False, "signing": False, "auth_for": None, "recheck": None,
             "signin": None, "tick": None, "acct_all": [], "accts": [], "acct_map": {}, "sel_acct": None, "checking_accts": False, "chips": {},
             "accounts": [], "acct_by_id": {}, "acct_chosen": set(), "acct_loading": False, "accounts_loaded": False,
             "acct_locked": False, "cl_locked": False, "pre": False,
             "crows": [], "by_key": {}, "cchosen": set(), "listing": False, "list_cancel": None, "listed": set(),
             "list_done": 0, "list_total": 0, "rebuild": False, "said": [],
             "menu": {}, "src_user": False, "src_fallback": False}
    ICON = {"pending": icon("pending"), "running": icon("run"), "done": icon("ok"), "failed": icon("fail"), "skipped": icon("skip")}
    SEV_ICON = {"CRIT": "crit", "HIGH": "high", "MED": "med", "INFO": "info"}
    COLORS = {"ok": "#067647", "err": "#c00000", "warn": "#9a7d0a", "info": "#1f4e79", "dim": "#777777"}
    NEED_SIGNIN = True      # the list of subscriptions itself needs the sign-in (AWS profiles are read from ~/.aws)
    PER_ACCOUNT = False      # the sign-in belongs to the chosen subscription (AWS profile)
    ACCOUNT0 = AZ_OPTS["subscription"]   # the subscription given on the command line, if any
    SEARCH_ICON = "Find:" if _ICON_PLAIN[0] else icon("search")

    def numeric(k):
        return int(k) if str(k).isdigit() else 10**9

    def count_of(n, noun):
        return f"{n} {noun}" + ("" if n == 1 else "s")

    # ---- look: Azure theme (ttk.Style), banner with the logo, footer status bar, a scrollable page
    BG, LINE, STRIPE = "#F3F7FB", "#C9D9EA", "#F1F6FC"
    root.configure(bg=BG)
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass
    import tkinter.font as tkfont
    for fname in ("TkDefaultFont", "TkTextFont", "TkMenuFont", "TkHeadingFont"):
        try:
            tkfont.nametofont(fname).configure(family="Segoe UI", size=10)
        except tk.TclError:
            pass
    style.configure(".", background=BG, foreground=BRAND_TEXT, font=("Segoe UI", 10), bordercolor=LINE, focuscolor=BRAND_ACCENT)
    style.configure("TFrame", background=BG)
    style.configure("TLabel", background=BG, foreground=BRAND_TEXT)
    style.configure("TLabelframe", background=BG, bordercolor=BRAND_PRIMARY, relief="solid", borderwidth=1)
    style.configure("TLabelframe.Label", background=BG, foreground=BRAND_PRIMARY, font=("Segoe UI", 10, "bold"))
    for w_ in ("TCheckbutton", "TRadiobutton"):
        style.configure(w_, background=BG, foreground=BRAND_TEXT)
        style.map(w_, background=[("active", BRAND_PALE)], foreground=[("disabled", "#8A97A6")])
    style.configure("TButton", padding=(10, 4), background="#E1EEFA", foreground=BRAND_TEXT, bordercolor="#9DC4E8", relief="flat")
    style.map("TButton", background=[("disabled", "#EEF2F6"), ("pressed", BRAND_DARK), ("active", "#CBE3F8")], foreground=[("disabled", "#9AA7B4"), ("pressed", "white")])
    style.configure("Accent.TButton", background=BRAND_PRIMARY, foreground="white", font=("Segoe UI", 10, "bold"), padding=(14, 6), bordercolor=BRAND_DARK)
    style.map("Accent.TButton", background=[("disabled", "#A9CBEA"), ("pressed", BRAND_DARK), ("active", BRAND_DARK)], foreground=[("disabled", "white"), ("!disabled", "white")])
    style.configure("Alert.TButton", background="#C00000", foreground="white", font=("Segoe UI", 10, "bold"), padding=(14, 6), bordercolor="#8B0000")
    style.map("Alert.TButton", background=[("disabled", "#E6A5A5"), ("pressed", "#8B0000"), ("active", "#8B0000")], foreground=[("disabled", "white"), ("!disabled", "white")])
    style.configure("Stop.TButton", background="#FDE7E7", foreground="#B42318", bordercolor="#F3A6A0", padding=(12, 6))
    style.map("Stop.TButton", background=[("disabled", "#EEF2F6"), ("active", "#F9C9C9")], foreground=[("disabled", "#9AA7B4")])
    style.configure("Treeview", background="white", fieldbackground="white", rowheight=24, bordercolor=LINE)
    style.configure("Treeview.Heading", background=BRAND_PALE, foreground=BRAND_DARK, font=("Segoe UI", 10, "bold"), relief="flat")
    style.map("Treeview", background=[("selected", BRAND_PRIMARY)], foreground=[("selected", "white")])
    style.map("Treeview.Heading", background=[("active", "#D6E9F9")])
    for w_ in ("TEntry", "TCombobox", "TSpinbox"):
        style.configure(w_, fieldbackground="white", bordercolor=LINE)
    style.configure("Horizontal.TProgressbar", troughcolor="#DCE9F6", background=BRAND_PRIMARY, bordercolor=LINE, lightcolor=BRAND_PRIMARY, darkcolor=BRAND_PRIMARY)
    style.configure("TPanedwindow", background=BG)

    style.configure("Footer.TLabel", background=BG)
    style.configure("Note.TLabel", foreground="#6B7685")
    style.configure("Desc.TLabel", foreground="#6B7685", font=("Segoe UI", 9))
    style.configure("TNotebook", background=BG, bordercolor=LINE)
    style.configure("TNotebook.Tab", padding=(14, 6), font=("Segoe UI", 10, "bold"), background=BRAND_PALE, foreground=BRAND_DARK)
    style.map("TNotebook.Tab", background=[("selected", "white")], foreground=[("selected", BRAND_PRIMARY)])

    def card(parent, text, stripe=None, padding=6, **pack):
        """A card: a labelled frame with a coloured stripe on its left edge (same helper as in the EKS / GKE windows). Returns the labelled
        frame; `pack` places the whole card."""
        outer = tk.Frame(parent, bg=BG)
        tk.Frame(outer, width=4, bg=stripe or BRAND_ACCENT).pack(side="left", fill="y")
        lf = ttk.LabelFrame(outer, text=text, padding=padding)
        lf.pack(side="left", fill="both", expand=True)
        outer.pack(**pack)
        lf.outer = outer
        return lf

    banner = tk.Canvas(root, height=78, highlightthickness=0, bd=0, bg=BRAND_PRIMARY)
    banner.pack(side="top", fill="x")

    def banner_sub():
        try:
            mins = int(minutes_var.get())
        except Exception:
            mins = default_minutes
        who = state["auth"].get("who") if state["auth"].get("state") == "ok" else None
        ident = (f"Signed in as {who}" if who else "Not signed in") if is_cli() else "Custom login (akslogin)"
        n = len(state["accounts"])
        subs = (count_of(n, "subscription")) if state["accounts"] else "subscriptions not loaded yet"
        return f"Last {mins} min   |   {ident}   |   {subs}"

    def pale_cloud(cx, cy, s, colour):
        for x0, y0, x1, y1 in ((-40, 4, -4, 30), (-26, -16, 12, 24), (-6, -4, 40, 30)):
            banner.create_oval(cx + x0 * s, cy + y0 * s, cx + x1 * s, cy + y1 * s, fill=colour, outline=colour, tags="deco")
        banner.create_rectangle(cx - 24 * s, cy + 6 * s, cx + 20 * s, cy + 30 * s, fill=colour, outline=colour, tags="deco")

    def draw_banner(_e=None):
        w = max(banner.winfo_width(), 400)
        banner.delete("all")
        steps_n = 64

        def grad(f):
            return _blend(BRAND_DARK, BRAND_PRIMARY, f / 0.6) if f < 0.6 else _blend(BRAND_PRIMARY, BRAND_ACCENT, (f - 0.6) / 0.4 * 0.6)
        for i in range(steps_n):
            col = grad(i / steps_n)
            banner.create_rectangle(w * i / steps_n, 0, w * (i + 1) / steps_n + 1, 78, fill=col, outline=col, tags="gradient")
        for cx, cy, s in ((w - 110, 26, 0.95), (w - 330, 46, 0.6), (w * 0.55, 14, 0.5)):       # soft pale clouds, behind the text
            pale_cloud(cx, cy, s, _blend(grad(cx / w), "#FFFFFF", 0.13))
        banner.create_rectangle(0, 75, w, 78, fill=BRAND_ACCENT, outline=BRAND_ACCENT, tags="accent")
        draw_logo(banner, 20, 8, 0.95, tag="logo")
        banner.create_text(132, 28, anchor="w", text=PRODUCT_NAME, fill="white", font=("Segoe UI", 17, "bold"), tags="banner_title")
        banner.create_text(132, 54, anchor="w", text=banner_sub(), fill="#E6F7FF", font=("Segoe UI", 10), tags="banner_sub")
        banner.create_text(w - 18, 56, anchor="e", text=f"{icon('cloud')} {CLOUD_NAME}", fill="white", font=("Segoe UI", 11, "bold"), tags="banner_cloud")

    def update_banner():
        if banner.find_withtag("banner_sub"):
            banner.itemconfigure("banner_sub", text=banner_sub())
    banner.bind("<Configure>", draw_banner)

    footer = tk.Frame(root, bg=BRAND_DARK)
    footer.pack(side="bottom", fill="x")
    status = tk.StringVar(value="Ready.")
    tasks_var = tk.StringVar(value="")
    elapsed = tk.StringVar(value="")
    tk.Label(footer, textvariable=status, bg=BRAND_DARK, fg="white", anchor="w", padx=10, pady=4, font=("Segoe UI", 10)).pack(side="left", fill="x", expand=True)
    tk.Label(footer, textvariable=elapsed, bg=BRAND_DARK, fg="#BFE6FF", padx=8).pack(side="right")
    tk.Label(footer, text=PIM_NOTE, bg=BRAND_DARK, fg="#BFE6FF", padx=8, font=("Segoe UI", 9)).pack(side="right")
    tk.Label(footer, textvariable=tasks_var, bg=BRAND_DARK, fg=BRAND_ACCENT, padx=8, font=("Segoe UI", 10, "bold")).pack(side="right")

    # ---- action bar (always visible), then the three tabs (same layout and names as the EKS / GKE windows)
    top = ttk.Frame(root, padding=(10, 8, 10, 4))
    top.pack(side="top", fill="x")
    ttk.Label(top, text="Last (minutes):").pack(side="left")
    minutes_var = tk.StringVar(value=str(default_minutes))
    ttk.Spinbox(top, from_=1, to=1440, width=6, textvariable=minutes_var).pack(side="left", padx=(4, 0))
    az_var = tk.BooleanVar(value=AZ_OPTS["enabled"])
    logs_var = tk.BooleanVar(value=True)
    open_var = tk.BooleanVar(value=True)
    alllogs_var = tk.BooleanVar(value=False)
    ns_var = tk.StringVar(value="")
    sec_count = tk.StringVar(value="")
    run_btn = ttk.Button(top, text=f"{icon('run')} Login & Debug selected cluster(s)", style="Accent.TButton")
    run_btn.pack(side="left", padx=(16, 4))
    stop_btn = ttk.Button(top, text=f"{icon('stop')} Stop", state="disabled", style="Stop.TButton")
    stop_btn.pack(side="left")
    sec_chip = tk.Label(top, textvariable=sec_count, bg=BRAND_PALE, fg=BRAND_DARK, padx=10, pady=2, font=("Segoe UI", 9, "bold"), relief="flat")
    sec_chip.pack(side="right", padx=(0, 8))
    nb = ttk.Notebook(root)
    nb.pack(side="top", fill="both", expand=True, padx=8, pady=(2, 0))
    tab_clusters = ttk.Frame(nb, padding=(2, 6, 2, 2))
    tab_collect = ttk.Frame(nb, padding=(2, 6, 2, 2))
    tab_run = ttk.Frame(nb, padding=(2, 6, 2, 2))
    tab_pim = ttk.Frame(nb, padding=(2, 6, 2, 2))
    nb.add(tab_clusters, text=f"{icon('key')} 1  Sign in and choose clusters")
    nb.add(tab_collect, text=f"{icon('list')} 2  What to collect")
    nb.add(tab_run, text=f"{icon('run')} 3  Run and results")
    nb.add(tab_pim, text=f"{icon('shield')} 4  Privileged roles (PIM)")

    host = tk.Frame(tab_clusters, bg=BG)
    host.pack(side="top", fill="both", expand=True)
    page_scroll = ttk.Scrollbar(host, orient="vertical")
    page_scroll.pack(side="right", fill="y")
    page_canvas = tk.Canvas(host, bg=BG, highlightthickness=0, yscrollcommand=page_scroll.set)
    page_canvas.pack(side="left", fill="both", expand=True)
    page_scroll.configure(command=page_canvas.yview)
    page = ttk.Frame(page_canvas)
    page_win = page_canvas.create_window((0, 0), window=page, anchor="nw")

    def fit_page(_e=None):
        page_canvas.itemconfigure(page_win, width=max(page_canvas.winfo_width(), 1), height=max(page_canvas.winfo_height(), page.winfo_reqheight()))
        page_canvas.configure(scrollregion=page_canvas.bbox("all"))
    page_canvas.bind("<Configure>", fit_page)
    page.bind("<Configure>", fit_page)

    def on_wheel(ev):
        if str(ev.widget.winfo_class()) in ("Treeview", "Text", "TCombobox", "Listbox", "TSpinbox"):
            return
        target = state.get("pim_canvas") if str(nb.select()) == str(tab_pim) and state.get("pim_canvas") is not None else page_canvas
        target.yview_scroll(-1 if ev.delta > 0 else 1, "units")
    root.bind_all("<MouseWheel>", on_wheel)

    def stripe(tree):
        """Alternate row shading (keeps the other tags of a row)."""
        tree.tag_configure("odd", background=STRIPE)
        for i, iid in enumerate(tree.get_children()):
            cur = tree.item(iid, "tags")
            cur = [x for x in (list(cur) if isinstance(cur, (tuple, list)) else ([cur] if cur else [])) if x != "odd"]
            tree.item(iid, tags=tuple(cur + (["odd"] if i % 2 else [])))

    # ---- guide: steps 1 - 4 (login method, sign in, choose subscription, choose clusters) with a message line
    guide = ttk.Frame(page, padding=(8, 6, 8, 0))
    guide.pack(fill="x")
    row12 = ttk.Frame(guide)
    row12.pack(fill="x")
    s1 = card(row12, f"{icon('gear')} Step 1 - Login method", side="left", fill="y")
    method_combo = ttk.Combobox(s1, width=30, state="readonly", values=[LOGIN_LABELS["exe"], LOGIN_LABELS["cli"]])
    method_combo.set(LOGIN_LABELS[LOGIN_OPTS["method"]])
    method_combo.pack(anchor="w")
    method_info = tk.StringVar(value="")
    ttk.Label(s1, textvariable=method_info, wraplength=340, justify="left").pack(anchor="w", pady=(4, 0))
    ro_note = ttk.Label(s1, text="Read-only guarantee: reports only read - they never install, create, change or delete anything on the cluster or in the cloud account. "
                             "Opt-in exception: the Privileged roles (PIM) tab can activate your own eligible roles, only when you confirm.",
                        wraplength=340, justify="left", foreground=COLORS["ok"], font=("Segoe UI", 9, "bold"))
    ro_note.pack(anchor="w", pady=(6, 0))
    src_var = tk.StringVar(value="menu")     # which clusters the list (step 4) shows: the akslogin menu (instant, default) or az on demand
    ttk.Label(s1, text="Cluster list:").pack(anchor="w", pady=(6, 0))
    src_all_rb = ttk.Radiobutton(s1, text="Collect clusters with az from selected subscriptions (on demand)", value="all", variable=src_var)
    src_all_rb.pack(anchor="w")
    src_menu_rb = ttk.Radiobutton(s1, text="Clusters from the akslogin menu (instant)", value="menu", variable=src_var)
    src_menu_rb.pack(anchor="w")
    s2 = card(row12, f"{icon('key')} Step 2 - Sign in", stripe=BRAND_PRIMARY, side="left", fill="both", expand=True, padx=(8, 0))
    s2a = ttk.Frame(s2)
    s2a.pack(fill="x")
    ttk.Label(s2a, text="Status:").pack(side="left")
    auth_icon = tk.Label(s2a, text=icon("pending"), fg="white", bg=COLORS["dim"], padx=6, pady=2, font=("Segoe UI", 9, "bold"))
    auth_icon.pack(side="left", padx=(6, 0))
    auth_badge = tk.Label(s2a, text="Not checked", fg="white", bg=COLORS["dim"], padx=10, pady=2, font=("Segoe UI", 9, "bold"))
    auth_badge.pack(side="left", padx=(0, 6))
    signin_btn = ttk.Button(s2a, text=f"{icon('key')} Sign in", style="Accent.TButton")
    signin_btn.pack(side="left", padx=(8, 4))
    check_btn = ttk.Button(s2a, text=f"{icon('search')} Check status")
    check_btn.pack(side="left")
    device_var = tk.BooleanVar(value=LOGIN_OPTS["device_code"])
    device_chk = ttk.Checkbutton(s2a, text="Use device code (default)", variable=device_var)
    device_chk.pack(side="left", padx=(12, 0))
    who_var = tk.StringVar(value="Signed in as: -")
    ttk.Label(s2a, textvariable=who_var, font=("Segoe UI", 9, "bold")).pack(side="left", padx=(14, 0))
    s2m = ttk.Frame(s2)
    s2m.pack(fill="x", pady=(4, 0))
    ttk.Label(s2m, text="Sign-in method:").pack(side="left")
    SIGNIN_KEYS = {v: k for k, v in SIGNIN_METHOD_LABELS.items()}
    LOGIN_OPTS["signin"] = _SESSION.get("signin") or LOGIN_OPTS.get("signin") or "manual"          # remembered for the session; default: manual
    signin_var = tk.StringVar(value=SIGNIN_METHOD_LABELS[LOGIN_OPTS["signin"]])
    signin_combo = ttk.Combobox(s2m, textvariable=signin_var, width=44, state="readonly", values=list(SIGNIN_METHOD_LABELS.values()))
    signin_combo.pack(side="left", padx=6)
    # accounts known to the Azure CLI: dropdown (type to search) + status chip, re-check buttons, one chip per account
    s2c = ttk.Frame(s2)
    s2c.pack(fill="x", pady=(6, 0))
    ttk.Label(s2c, text="Account:").pack(side="left")
    acct_combo = ttk.Combobox(s2c, width=62, state="normal")
    acct_combo.pack(side="left", padx=(6, 6))
    acct_chip = tk.Label(s2c, text="Not checked", fg="white", bg=COLORS["dim"], padx=8, pady=2, font=("Segoe UI", 9, "bold"))
    acct_chip.pack(side="left")
    recheck_btn = ttk.Button(s2c, text="Re-check")
    recheck_btn.pack(side="left", padx=(8, 0))
    check_all_btn = ttk.Button(s2c, text="Check all accounts")
    check_all_btn.pack(side="left", padx=(4, 0))
    chips_row = ttk.Frame(s2)
    chips_row.pack(fill="x", pady=(4, 0))
    s2d = ttk.Frame(s2)
    s2d.pack(fill="x", pady=(6, 0))
    ttk.Label(s2d, text="Tenant (optional):").pack(side="left")
    tenant_var = tk.StringVar(value=LOGIN_OPTS.get("tenant") or "")
    tenant_entry = ttk.Entry(s2d, textvariable=tenant_var, width=40)
    tenant_entry.pack(side="left", padx=(6, 8))
    switch_btn = ttk.Button(s2d, text=f"{icon('key')} Sign in with a different account")
    switch_btn.pack(side="left")
    auth_msg = tk.StringVar(value="")
    auth_msg_lbl = ttk.Label(s2, textvariable=auth_msg, wraplength=820, justify="left")
    auth_msg_lbl.pack(fill="x", pady=(4, 0))
    # ---- manual sign-in (the DEFAULT): the exact commands, each with a Copy button; the window waits and notices the sign-in by itself
    man_panel = ttk.LabelFrame(s2, text="Sign in - run a command yourself", padding=8)
    man = state["man"] = {"shown": False, "active": False, "t0": None, "after": None, "probing": False, "expect": None, "different": False}
    man_form = tk.StringVar(value="device")
    man_instr_var = tk.StringVar(value=MANUAL_INSTRUCTIONS)
    ttk.Label(man_panel, textvariable=man_instr_var, wraplength=820, justify="left", font=("Segoe UI", 10, "bold")).pack(fill="x")
    man_cli_var = tk.StringVar(value="")
    man_cli_lbl = tk.Label(man_panel, textvariable=man_cli_var, anchor="w", justify="left", font=("Segoe UI", 10, "bold"), fg=COLORS["ok"], bg=BG)
    man_cli_lbl.pack(fill="x", pady=(4, 0))
    man_inst = tk.Label(man_panel, text="Install the Azure CLI first: " + AZ_INSTALL_HINT, anchor="w", justify="left", wraplength=820, fg=COLORS["err"], bg=BG,
                        font=("Segoe UI", 9))
    man_msg_var = tk.StringVar(value="")
    man_msg_lbl = tk.Label(man_panel, textvariable=man_msg_var, anchor="w", justify="left", wraplength=820, font=("Segoe UI", 10, "bold"), fg=COLORS["warn"], bg=BG)
    man_msg_lbl.pack(fill="x", pady=(2, 0))
    man_cmd_vars, man_copy_btns, man_entries, man_rows = {}, {}, {}, {}
    for _it in manual_commands("tenant"):
        _row = ttk.Frame(man_panel)
        man_rows[_it["key"]] = _row
        if _it["key"] != "device-tenant":                       # the 1b variant line is shown only when the Tenant box has a value
            _row.pack(fill="x", pady=(4, 0))
        _top = ttk.Frame(_row)
        _top.pack(fill="x")
        if _it["key"] in _TERMINAL_FORMS:
            ttk.Radiobutton(_top, text=f"{_it['n']}.", variable=man_form, value=_it["key"], width=4).pack(side="left")
        else:
            ttk.Label(_top, text=f"{_it['n']}.", width=5).pack(side="left", padx=(18, 0))
        ttk.Label(_top, text=_it["note"], style="Desc.TLabel", wraplength=760, justify="left").pack(side="left", fill="x", expand=True)
        _line = ttk.Frame(_row)
        _line.pack(fill="x", padx=(40, 0))
        man_cmd_vars[_it["key"]] = tk.StringVar(value=_it["cmd"])
        man_entries[_it["key"]] = ttk.Entry(_line, textvariable=man_cmd_vars[_it["key"]], state="readonly",
                                            font=("Consolas", 12, "bold") if _it["key"] == "device" else ("Consolas", 10))
        man_entries[_it["key"]].pack(side="left", fill="x", expand=True)
        man_copy_btns[_it["key"]] = ttk.Button(_line, text="Copy", style="Accent.TButton" if _it["key"] == "device" else "TButton")
        man_copy_btns[_it["key"]].pack(side="left", padx=(6, 0))
    ttk.Label(man_panel, text=MANUAL_STEPS, wraplength=820, justify="left", font=("Segoe UI", 10)).pack(fill="x", pady=(8, 0))
    ttk.Label(man_panel, text="The round button in front of 1 - 3 chooses which command 'Open a terminal for me' runs. Commands 4a / 4b are only shown here as text. "
                              "This tool never installs anything and never runs these commands itself.", style="Desc.TLabel", wraplength=820, justify="left").pack(fill="x", pady=(4, 0))
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
    man_result_lbl = tk.Label(man_panel, textvariable=man_result_var, anchor="w", justify="left", wraplength=820, font=("Segoe UI", 10, "bold"), fg=COLORS["info"], bg=BG)
    man_result_lbl.pack(fill="x", pady=(4, 0))
    # ---- Sign-in details panel (device-code URL + code, countdown, status, cancel): shown while / after a sign-in attempt
    sp = ttk.LabelFrame(s2, text="Sign-in details", padding=6)
    sp_url_row = ttk.Frame(sp)
    sp_url_row.pack(fill="x")
    ttk.Label(sp_url_row, text="Open this URL:").pack(side="left")
    sp_url = tk.Label(sp_url_row, text="", fg="#0b5cad", cursor="hand2", font=("Segoe UI", 10, "underline"), anchor="w")
    sp_url.pack(side="left", padx=(6, 10))
    sp_open_btn = ttk.Button(sp_url_row, text="Open in browser")
    sp_open_btn.pack(side="left")
    sp_copy_url_btn = ttk.Button(sp_url_row, text="Copy URL")
    sp_copy_url_btn.pack(side="left", padx=(4, 0))
    sp_code_row = ttk.Frame(sp)
    sp_code_row.pack(fill="x", pady=(4, 0))
    ttk.Label(sp_code_row, text="Enter this code:").pack(side="left")
    sp_code = tk.Label(sp_code_row, text="", font=("Consolas", 22, "bold"), fg=BRAND_DARK, padx=10)
    sp_code.pack(side="left")
    sp_copy_code_btn = ttk.Button(sp_code_row, text="Copy code")
    sp_copy_code_btn.pack(side="left")
    sp_cancel_btn = ttk.Button(sp_code_row, text="Cancel sign-in", style="Stop.TButton")
    sp_cancel_btn.pack(side="right")
    sp_status_row = ttk.Frame(sp)
    sp_status_row.pack(fill="x", pady=(4, 0))
    sp_chip = tk.Label(sp_status_row, text="", fg="white", bg=COLORS["dim"], padx=8, pady=2, font=("Segoe UI", 9, "bold"))
    sp_chip.pack(side="left")
    sp_countdown = tk.StringVar(value="")
    ttk.Label(sp_status_row, textvariable=sp_countdown, font=("Segoe UI", 10, "bold")).pack(side="left", padx=(8, 0))
    sp_account = tk.StringVar(value="")
    ttk.Label(sp, textvariable=sp_account, foreground=COLORS["dim"]).pack(anchor="w", pady=(2, 0))
    sp_detail = tk.StringVar(value="")
    ttk.Label(sp, textvariable=sp_detail, wraplength=800, justify="left").pack(anchor="w", pady=(2, 0))
    sp_raw = tk.StringVar(value="")
    ttk.Label(sp, textvariable=sp_raw, wraplength=800, justify="left", foreground=COLORS["dim"], font=("Consolas", 8)).pack(anchor="w")
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
        auth_icon.configure(text={"ok": icon("ok"), "err": icon("fail"), "warn": icon("warn")}.get(kind, icon("pending")), bg=COLORS.get(kind, COLORS["dim"]))

    def search_box(parent, var, width=30):
        box = ttk.Frame(parent)
        ttk.Label(box, text=SEARCH_ICON).pack(side="left")
        entry = ttk.Entry(box, textvariable=var, width=width)
        entry.pack(side="left", fill="x", expand=True, padx=4)
        clear = ttk.Button(box, text="✕", width=3, command=lambda: var.set(""))
        clear.pack(side="left")
        return box, entry, clear

    row34 = ttk.Frame(guide)
    row34.pack(fill="x", pady=(6, 0))
    # ---- step 3: the subscriptions (searchable list)
    s3 = card(row34, f"{icon('cloud')} Step 3 - Choose subscription(s)  (Ctrl/Shift-click for several)", stripe=BRAND_ACCENT, side="left", fill="both", expand=True)
    acct_filter = tk.StringVar(value="")
    box3, acct_search, acct_search_x = search_box(s3, acct_filter)
    box3.pack(fill="x")
    acct_count = tk.StringVar(value="")
    ttk.Label(s3, textvariable=acct_count).pack(anchor="w")
    scope_var = tk.StringVar(value="sel")                  # kept for the tests / sync_login_opts: the scope is always 'the selected subscriptions'
    scope_row = ttk.Frame(s3)                               # (not shown: nothing is searched unless the user selects subscriptions and presses the collect button)
    scope_all_rb = ttk.Radiobutton(scope_row, text="", value="all", variable=scope_var)
    scope_sel_rb = ttk.Radiobutton(scope_row, text="", value="sel", variable=scope_var)
    acct_sel_count = tk.StringVar(value="")
    ttk.Label(s3, textvariable=acct_sel_count, font=("Segoe UI", 10, "bold"), foreground=BRAND_PRIMARY).pack(anchor="w")
    a_wrap = ttk.Frame(s3)
    a_wrap.pack(fill="both", expand=True)
    acct_tree = ttk.Treeview(a_wrap, columns=("code", "info"), show="tree headings", selectmode="extended", height=7)
    acct_tree.heading("#0", text="Subscription")
    acct_tree.heading("code", text="Subscription id")
    acct_tree.heading("info", text="State")
    acct_tree.column("#0", width=230)
    acct_tree.column("code", width=230)
    acct_tree.column("info", width=110)
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
    acct_reload_btn = ttk.Button(a_btns, text=f"{icon('reload')} Reload subscriptions")
    acct_reload_btn.pack(side="left")
    acct_status = tk.StringVar(value="")
    ttk.Label(s3, textvariable=acct_status, wraplength=640, justify="left").pack(anchor="w", pady=(4, 0))

    # ---- step 4: the clusters (searchable multi-select list)
    s4 = card(row34, f"{icon('helm')} Step 4 - Choose clusters  (click one, Ctrl/Shift-click for several; they run one after another)", stripe=BRAND_PRIMARY, side="left", fill="both", expand=True, padx=(8, 0))
    collect_row = ttk.Frame(s4)
    collect_row.pack(fill="x", pady=(0, 4))
    collect_btn = ttk.Button(collect_row, text="Collect clusters from selected subscriptions", style="Accent.TButton")
    collect_btn.pack(side="left")
    collect_stop_btn = ttk.Button(collect_row, text="Stop", style="Stop.TButton", state="disabled")
    collect_stop_btn.pack(side="left", padx=(6, 0))
    filter_var = tk.StringVar(value="")
    box4, cl_search, cl_search_x = search_box(s4, filter_var)
    box4.pack(fill="x")
    cl_count = tk.StringVar(value="")
    ttk.Label(s4, textvariable=cl_count).pack(anchor="w")
    c_wrap = ttk.Frame(s4)
    c_wrap.pack(fill="both", expand=True)
    cluster_tree = ttk.Treeview(c_wrap, columns=("where", "acct"), show="tree headings", selectmode="extended", height=7)
    cluster_tree.heading("#0", text="Cluster")
    cluster_tree.heading("where", text="Location / resource group")
    cluster_tree.heading("acct", text="Subscription")
    cluster_tree.column("#0", width=260)
    cluster_tree.column("where", width=190)
    cluster_tree.column("acct", width=190)
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
    refresh_btn = ttk.Button(c_btns, text=f"{icon('reload')} Refresh selected")
    refresh_btn.pack(side="left")
    manual_var = tk.StringVar(value="")
    ttk.Label(c_btns, text="  or type numbers:").pack(side="left")
    manual_entry = ttk.Entry(c_btns, textvariable=manual_var, width=16)
    manual_entry.pack(side="left", padx=4)
    ttk.Label(c_btns, text="e.g. 1,3,5  2-4  all").pack(side="left")
    sel_text = tk.StringVar(value="Selected: none")
    ttk.Label(s4, textvariable=sel_text, wraplength=640, justify="left").pack(anchor="w", pady=(4, 0))
    cl_status = tk.StringVar(value="")
    ttk.Label(s4, textvariable=cl_status, wraplength=640, justify="left").pack(anchor="w")
    list_bar = ttk.Progressbar(s4, mode="determinate", length=300)
    list_bar.pack(anchor="w", pady=(2, 0))
    acct_widgets = [acct_search, acct_search_x, scope_all_rb, scope_sel_rb, acct_all_btn, acct_clear_btn]
    cl_widgets = [cl_search, cl_search_x, select_all_btn, clear_btn, manual_entry]

    # ---- tab 2: what to collect - one box per report section (the SECTIONS registry drives them), quick presets, a counter
    saved_prefs = _load_prefs()
    sec_vars, sec_checks = {}, {}
    start_sel = _SESSION["sections"] or saved_prefs["sections"] or (INITIAL_SECTIONS if INITIAL_SECTIONS is not None else SECTIONS_ORDER)
    sec_note = tk.StringVar(value="")
    sections_card = card(tab_collect, f"{icon('list')} What to collect  (untick what you do not need: it is not collected at all)", fill="both", expand=True)
    sec_head = ttk.Frame(sections_card)
    sec_head.pack(fill="x")
    ttk.Label(sec_head, textvariable=sec_count, font=("Segoe UI", 11, "bold"), foreground=BRAND_PRIMARY).pack(side="left")
    sec_btns = sec_head
    ttk.Label(sections_card, text="Tick what the report should contain. Unticked sections are never collected: no command is run for them. "
                                  "Sections that need another section's data read it silently (see the note below).", style="Note.TLabel",
              wraplength=900, justify="left").pack(fill="x", pady=(6, 4))
    sec_grid = ttk.Frame(sections_card)
    sec_grid.pack(fill="both", expand=True)

    def selected_sections():
        return [s["id"] for s in SECTIONS if sec_vars[s["id"]].get()]

    def selectable_ids():
        return [s["id"] for s in SECTIONS if not s.get("locked")]

    def update_sections_info(*_a):
        picked = selected_sections()
        n = len([s for s in picked if s in selectable_ids()])
        sec_count.set(f"{n} of {len(selectable_ids())} sections selected")
        plan_ = plan_sections({"sections": picked, "azure": az_var.get(), "logs": True})
        notes = [f"{SECTION_BY_ID[d]['title']} (needed by {', '.join(SECTION_BY_ID[x]['title'] for x in who)})" for d, who in plan_["hidden"].items()]
        sec_note.set(("Collected quietly, not shown in the report: " + "; ".join(notes) + ".") if notes else "")
        _SESSION["sections"] = list(picked)

    SEC_ROWS = (len(SECTIONS) + 1) // 2
    sec_grid.columnconfigure(0, weight=1, uniform="sec")
    sec_grid.columnconfigure(1, weight=1, uniform="sec")
    for i, s in enumerate(SECTIONS):
        v = logs_var if s["id"] == "logs" else tk.BooleanVar()
        v.set(s["id"] in start_sel or bool(s.get("locked")))
        sec_vars[s["id"]] = v
        cell = ttk.Frame(sec_grid, padding=(4, 2, 12, 5))
        cell.grid(row=i % SEC_ROWS, column=i // SEC_ROWS, sticky="nsew")
        cb = ttk.Checkbutton(cell, text=f"{icon(s['icon'])}  {s['title']}", variable=v, command=update_sections_info)
        if s.get("locked"):
            cb.state(["disabled"])
        cb.pack(anchor="w")
        desc = ttk.Label(cell, text=s["desc"], style="Desc.TLabel", justify="left", wraplength=420)
        desc.pack(anchor="w", padx=(24, 0))
        cell.bind("<Configure>", lambda e_, d=desc: d.configure(wraplength=max(200, e_.width - 48)))
        sec_checks[s["id"]] = cb

    def set_sections(ids):
        for sid, var in sec_vars.items():
            var.set(sid in ids or bool(SECTION_BY_ID[sid].get("locked")))
        update_sections_info()
    preset_btns = {}
    for key, label_, ids in (("no_net", "Everything except networking", SECTION_PRESETS["no_networking"]),
                             ("only_net", f"{icon('net')} Only networking", SECTION_PRESETS["only_networking"]),
                             ("none", "Clear all", []), ("all", "Select all", SECTION_PRESETS["all"])):
        b_ = ttk.Button(sec_head, text=label_, command=lambda ids=ids: set_sections(ids))
        b_.pack(side="right", padx=(4, 0))
        preset_btns[key] = b_
    sec_note_lbl = ttk.Label(sections_card, textvariable=sec_note, style="Note.TLabel", wraplength=900, justify="left")
    sec_note_lbl.pack(fill="x", pady=(4, 0))
    logopt = ttk.Frame(sections_card)
    logopt.pack(fill="x", pady=(8, 0))
    az_chk = ttk.Checkbutton(logopt, text=f"{icon('cloud')} Azure details (az)", variable=az_var)
    az_chk.pack(side="left")
    _Tip(az_chk, "Off = no Azure CLI call is made at all. (To leave only the Azure section out of the report, untick it above.)")
    ttk.Checkbutton(logopt, text=f"{icon('log')} Pod logs", variable=logs_var).pack(side="left", padx=(10, 0))
    ttk.Checkbutton(logopt, text="Logs of ALL pods", variable=alllogs_var).pack(side="left", padx=(10, 0))
    ttk.Label(logopt, text="only namespaces (comma separated, blank = all):").pack(side="left", padx=(10, 2))
    ttk.Entry(logopt, textvariable=ns_var, width=26).pack(side="left")
    workers_var = tk.StringVar(value=str(_SESSION["workers"] or saved_prefs["workers"] or PARALLEL_WORKERS))
    workers_row = ttk.Frame(sections_card)
    workers_row.pack(fill="x", pady=(6, 0))
    ttk.Label(workers_row, text=f"{icon('speed')} Parallel workers:").pack(side="left")
    workers_spin = ttk.Spinbox(workers_row, from_=1, to=16, width=4, textvariable=workers_var)
    workers_spin.pack(side="left", padx=(6, 0))
    _Tip(workers_spin, "How many collection tasks run at the same time after the login (1 = one after another).")
    az_var.trace_add("write", update_sections_info)
    update_sections_info()
    for w_ in (workers_row, logopt, sec_note_lbl):          # the option rows keep their place at the bottom; the grid takes what is left (1100x700 stays usable)
        w_.pack_forget()
        w_.pack(side="bottom", fill="x", pady=(6, 0))
    sec_grid.pack_forget()
    sec_grid.pack(side="top", fill="both", expand=True)

    # ---- tab 3: run bar (progress, options, report buttons), steps + clusters-in-run + live findings (left), live log (right)
    run_bar = ttk.Frame(tab_run)
    run_bar.pack(fill="x", pady=(0, 4))
    progress_bar = ttk.Progressbar(run_bar, mode="determinate", length=340)
    progress_bar.pack(side="left")
    ttk.Label(run_bar, textvariable=tasks_var, font=("Segoe UI", 10, "bold"), foreground=BRAND_PRIMARY).pack(side="left", padx=10)
    ttk.Checkbutton(run_bar, text="Open report when done", variable=open_var).pack(side="left", padx=(10, 0))
    folder_btn = ttk.Button(run_bar, text=f"{icon('folder')} Open reports folder")
    folder_btn.pack(side="right")
    open_btn = ttk.Button(run_bar, text=f"{icon('report')} Open HTML report", state="disabled", style="Accent.TButton")
    open_btn.pack(side="right", padx=6)

    # ---- body: steps + clusters-in-run + live findings (left), live log (right)
    body = ttk.PanedWindow(tab_run, orient="horizontal")
    body.pack(fill="both", expand=True)
    left = ttk.Frame(body, width=450)
    body.add(left, weight=0)
    steps_box = card(left, f"{icon('list')} Collection steps (current cluster)", padding=4, fill="x")
    steps = ttk.Treeview(steps_box, columns=("status", "time"), height=8, show="tree headings", selectmode="none")
    steps.heading("#0", text="Step")
    steps.heading("status", text="Status")
    steps.heading("time", text="Time")
    steps.column("#0", width=270)
    steps.tag_configure("odd", background=STRIPE)
    steps.column("status", width=80, anchor="center")
    steps.column("time", width=60, anchor="e")
    steps.pack(fill="x")
    for tag, color in (("running", "#1f4e79"), ("done", "#067647"), ("failed", "#c00000"), ("skipped", "#888888")):
        steps.tag_configure(tag, foreground=color)
    steps.tag_configure("running", font=("Segoe UI", 9, "bold"))

    run_box = card(left, f"{icon('helm')} Clusters in this run (double-click a finished one to open its report)", stripe=BRAND_PRIMARY, padding=4, fill="x", pady=(6, 0))
    run_tree = ttk.Treeview(run_box, columns=("status", "crit", "high"), height=4, show="tree headings", selectmode="browse")
    run_tree.heading("#0", text="Cluster")
    run_tree.heading("status", text="Status")
    run_tree.heading("crit", text="CRIT")
    run_tree.heading("high", text="HIGH")
    run_tree.column("#0", width=210)
    run_tree.tag_configure("odd", background=STRIPE)
    run_tree.column("status", width=110, anchor="center")
    run_tree.column("crit", width=50, anchor="center")
    run_tree.column("high", width=50, anchor="center")
    run_tree.pack(fill="x")
    for tag, color in (("running", "#1f4e79"), ("ok", "#067647"), ("failed", "#c00000"), ("partial", "#9a7d0a"), ("notrun", "#888888")):
        run_tree.tag_configure(tag, foreground=color)

    find_box = card(left, f"{icon('warn')} Findings (live - updates while collecting)", stripe="#C00000", padding=4, fill="both", expand=True, pady=(6, 0))
    counters = ttk.Frame(find_box)
    counters.pack(fill="x")
    counter_vars = {}
    for sev, color in (("CRIT", "#c00000"), ("HIGH", "#d35400"), ("MED", "#9a7d0a"), ("INFO", "#1f6feb")):
        counter_vars[sev] = tk.StringVar(value=f"{icon(SEV_ICON[sev])} {sev} 0")
        tk.Label(counters, textvariable=counter_vars[sev], fg=color, font=("Segoe UI", 10, "bold")).pack(side="left", padx=(0, 14))
    findings = ttk.Treeview(find_box, columns=("sev", "text"), show="headings", height=6)
    findings.heading("sev", text="Sev")
    findings.heading("text", text="Finding")
    findings.column("sev", width=78, anchor="center")
    findings.tag_configure("odd", background=STRIPE)
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

    # ---- tab 4: Privileged roles (PIM). The ONE opt-in exception to read-only: self-activation of your own eligible roles, only after the confirmation dialog.
    PIM_ALL = "All types"
    pim = {"active": [], "elig": [], "me": None, "busy": False, "cancel": None, "loaded": False, "q": queue.Queue(), "fams": {}, "dialog": None,
           "results": {}, "total": 0, "done": 0, "summary": "", "errors": {}, "visited": False}
    pim_host = tk.Frame(tab_pim, bg=BG)
    pim_host.pack(side="top", fill="both", expand=True)
    pim_scroll = ttk.Scrollbar(pim_host, orient="vertical")
    pim_scroll.pack(side="right", fill="y")
    pim_canvas = tk.Canvas(pim_host, bg=BG, highlightthickness=0, yscrollcommand=pim_scroll.set)
    pim_canvas.pack(side="left", fill="both", expand=True)
    pim_scroll.configure(command=pim_canvas.yview)
    pim_page = ttk.Frame(pim_canvas)
    pim_win = pim_canvas.create_window((0, 0), window=pim_page, anchor="nw")
    state["pim_canvas"] = pim_canvas

    def pim_fit(_e=None):
        pim_canvas.itemconfigure(pim_win, width=max(pim_canvas.winfo_width(), 1), height=max(pim_canvas.winfo_height(), pim_page.winfo_reqheight()))
        pim_canvas.configure(scrollregion=pim_canvas.bbox("all"))
    pim_canvas.bind("<Configure>", pim_fit)
    pim_page.bind("<Configure>", pim_fit)
    pim_top = card(pim_page, f"{icon('shield')} Privileged roles (PIM) - what you hold now and what you can activate", stripe=BRAND_PRIMARY, fill="x")
    pim_note = ttk.Label(pim_top, text=PIM_NOTE, foreground=COLORS["ok"], font=("Segoe UI", 9, "bold"), wraplength=1100, justify="left")
    pim_note.pack(anchor="w")
    pim_chip_row = ttk.Frame(pim_top)
    pim_chip_row.pack(fill="x", pady=(3, 0))
    pim_chips = {}
    for _i, _fam in enumerate(PIM_FAMILIES):
        pim_chips[_fam] = tk.Label(pim_chip_row, text=f"{PIM_FAMILY_NAME[_fam]}: not loaded", bg=COLORS["dim"], fg="white", padx=6, pady=2, font=("Segoe UI", 9, "bold"), anchor="w")
        pim_chips[_fam].grid(row=0, column=_i, sticky="we", padx=(0, 6))
        pim_chip_row.columnconfigure(_i, weight=1, uniform="pimchip")
    pim_btn_row = ttk.Frame(pim_top)
    pim_btn_row.pack(fill="x", pady=(4, 0))
    pim_refresh_btn = ttk.Button(pim_btn_row, text=f"{icon('reload')} Refresh")
    pim_refresh_btn.pack(side="left")
    pim_sel_btn = ttk.Button(pim_btn_row, text="Activate selected", state="disabled")
    pim_sel_btn.pack(side="left", padx=(8, 0))
    pim_all_btn = ttk.Button(pim_btn_row, text="Activate ALL eligible roles at once", style="Accent.TButton", state="disabled")
    pim_all_btn.pack(side="left", padx=(8, 0))
    pim_stop_btn = ttk.Button(pim_btn_row, text=f"{icon('stop')} Stop", style="Stop.TButton", state="disabled")
    pim_stop_btn.pack(side="left", padx=(8, 0))
    pim_help = tk.Label(pim_btn_row, text="ⓘ Column help" if not _ICON_PLAIN[0] else "(?) Column help", fg=BRAND_PRIMARY, bg=BG, cursor="question_arrow", font=("Segoe UI", 9, "bold"))
    pim_help.pack(side="right")
    pim_count_var = tk.StringVar(value="")
    ttk.Label(pim_btn_row, textvariable=pim_count_var, font=("Segoe UI", 10, "bold"), foreground=BRAND_PRIMARY).pack(side="right", padx=(0, 16))
    _Tip(pim_help, "\n".join(f"{k}: {v}" for k, v in PIM_COLUMN_HELP.items()))
    pim_filter_row = ttk.Frame(pim_top)
    pim_filter_row.pack(fill="x", pady=(4, 0))
    pim_search_var = tk.StringVar(value="")
    pim_box, pim_search, pim_search_x = search_box(pim_filter_row, pim_search_var, 30)
    pim_box.pack(side="left")
    ttk.Label(pim_filter_row, text="Type:").pack(side="left", padx=(10, 2))
    pim_type_var = tk.StringVar(value=PIM_ALL)
    pim_type_combo = ttk.Combobox(pim_filter_row, textvariable=pim_type_var, width=24, state="readonly", values=[PIM_ALL] + [PIM_TYPE[f] for f in PIM_FAMILIES])
    pim_type_combo.pack(side="left")
    pim_selall_btn = ttk.Button(pim_filter_row, text="Select all eligible")
    pim_selall_btn.pack(side="left", padx=(10, 0))
    pim_clear_btn = ttk.Button(pim_filter_row, text="Clear")
    pim_clear_btn.pack(side="left", padx=(4, 0))
    pim_msg_var = tk.StringVar(value="")
    pim_msg = tk.Label(pim_top, textvariable=pim_msg_var, anchor="w", justify="left", font=("Segoe UI", 9, "bold"), padx=6, pady=1, bg="#eef3f8", fg=COLORS["info"])
    pim_msg.pack(fill="x", pady=(4, 0))
    pim_msg.bind("<Configure>", lambda e: pim_msg.configure(wraplength=max(300, e.width - 20)))
    pim_fb_slot = tk.Frame(pim_page, bg=BG)
    pim_fb_slot.pack(fill="x")
    pim_fb = ttk.LabelFrame(pim_fb_slot, text="This tenant blocked the automatic method", padding=6)
    pim_fb_var = tk.StringVar(value="")
    tk.Label(pim_fb, textvariable=pim_fb_var, anchor="w", justify="left", bg=BG, fg=COLORS["err"], font=("Consolas", 9), wraplength=1100).pack(fill="x")
    pim_fb_help = tk.StringVar(value="")
    tk.Label(pim_fb, textvariable=pim_fb_help, anchor="w", justify="left", bg=BG, fg=BRAND_TEXT, font=("Segoe UI", 9), wraplength=1100).pack(fill="x", pady=(4, 0))
    pim_fb_btns = ttk.Frame(pim_fb)
    pim_fb_btns.pack(fill="x", pady=(4, 0))
    pim_open_entra_btn = ttk.Button(pim_fb_btns, text="Open the PIM page in your browser (Microsoft Entra roles)")
    pim_open_entra_btn.pack(side="left")
    pim_open_group_btn = ttk.Button(pim_fb_btns, text="Open the PIM page in your browser (groups)")
    pim_open_group_btn.pack(side="left", padx=(6, 0))
    pim_copy_err_btn = ttk.Button(pim_fb_btns, text="Copy the error text")
    pim_copy_err_btn.pack(side="left", padx=(6, 0))

    pim_paned = pim_page

    def pane_card(title, stripe_colour, weight):
        outer = tk.Frame(pim_page, bg=BG)
        tk.Frame(outer, width=4, bg=stripe_colour).pack(side="left", fill="y")
        lf = ttk.LabelFrame(outer, text=title, padding=4)
        lf.pack(side="left", fill="both", expand=True)
        outer.pack(fill="x", pady=(4, 0))
        return lf

    PIM_COLS = (("type", "Type", 160), ("scope", "Scope", 190), ("status", "Status", 115), ("expires", "Expires at (time left)", 185),
                ("max", "Maximum duration allowed", 160), ("requires", "Requires", 190))

    def pim_tree(parent, selectmode, height):
        wrap = ttk.Frame(parent)
        wrap.pack(fill="x")
        tv = ttk.Treeview(wrap, columns=[c[0] for c in PIM_COLS], show="tree headings", selectmode=selectmode, height=height)
        tv.heading("#0", text="Role or group name")
        tv.column("#0", width=210)
        for cid, ctext, cw in PIM_COLS:
            tv.heading(cid, text=ctext)
            tv.column(cid, width=cw, anchor="w")
        sb = ttk.Scrollbar(wrap, orient="vertical", command=tv.yview)
        tv.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        tv.pack(side="left", fill="x", expand=True)
        for tag, colour in (("Active", "#067647"), ("Expired soon", "#9a7d0a"), ("Eligible", "#1f4e79"), ("Pending approval", "#9a7d0a")):
            tv.tag_configure(tag, foreground=colour)
        return tv

    pim_active_box = pane_card(f"{icon('ok')} Active roles - what you can use right now", "#067647", 1)
    pim_active_tree = pim_tree(pim_active_box, "browse", 4)
    ttk.Label(pim_active_box, style="Desc.TLabel", wraplength=1100, justify="left",
              text="What this table shows: the roles and groups you hold at this moment (activated through PIM, or assigned permanently), with when each one ends. Read-only.").pack(anchor="w")
    pim_elig_box = pane_card(f"{icon('key')} Eligible roles - allowed but not active yet (select the ones to activate)", BRAND_PRIMARY, 2)
    pim_elig_tree = pim_tree(pim_elig_box, "extended", 7)
    ttk.Label(pim_elig_box, style="Desc.TLabel", wraplength=1100, justify="left",
              text="What this table shows: the roles and groups you may activate (eligible) but that are not active. Roles already active are not listed here. "
                   "Nothing is requested until you confirm in the dialog.").pack(anchor="w")
    pim_res_box = pane_card(f"{icon('list')} Activation results", "#9a7d0a", 1)
    pim_res_tree = ttk.Treeview(pim_res_box, columns=("type", "result", "reason"), show="tree headings", selectmode="browse", height=5)
    pim_res_tree.heading("#0", text="Role or group name")
    pim_res_tree.heading("type", text="Type")
    pim_res_tree.heading("result", text="Result")
    pim_res_tree.heading("reason", text="Details (the service's reason)")
    pim_res_tree.column("#0", width=230)
    pim_res_tree.column("type", width=150)
    pim_res_tree.column("result", width=130)
    pim_res_tree.column("reason", width=700)
    for _tag, _colour in (("Activated", "#067647"), ("Already active", "#1f4e79"), ("Pending approval", "#9a7d0a"), ("Denied", "#c00000"), ("Queued", "#888888"), ("Requesting", "#1f4e79")):
        pim_res_tree.tag_configure(_tag, foreground=_colour)
    pim_res_tree.pack(fill="x")
    pim_res_row = ttk.Frame(pim_res_box)
    pim_res_row.pack(fill="x", pady=(2, 0))
    pim_progress = ttk.Progressbar(pim_res_row, mode="determinate", length=220)
    pim_progress.pack(side="left")
    pim_summary_var = tk.StringVar(value="No activation requested yet.")
    ttk.Label(pim_res_row, textvariable=pim_summary_var, font=("Segoe UI", 10, "bold")).pack(side="left", padx=(10, 0))

    def pim_say(text, kind="info"):
        pim_msg_var.set(text)
        pim_msg.configure(fg=COLORS.get(kind, COLORS["info"]))

    def pim_gate():
        """None when the tab can work, else the message (it needs the Cloud CLI (az) sign-in)."""
        if not is_cli():
            return ("The Privileged roles tab works with the Cloud CLI (az) sign-in. On tab 1 choose 'Cloud CLI (az)' as the login method (Step 1) and sign in "
                    "(az login --use-device-code), then come back here and press Refresh. The custom akslogin sign-in cannot be used for PIM.")
        if state["auth"].get("state") != "ok":
            return "Sign in with the Azure CLI first (tab 1, Step 2: az login --use-device-code), then press Refresh."
        return None

    def pim_visible(rows):
        ts, ty = terms(pim_search_var), pim_type_var.get()
        return [r for r in rows if (ty == PIM_ALL or r["type"] == ty)
                and hit(" ".join((r["name"], r["scope"], r["type"], r["status"], r.get("requires_text") or "")).lower(), ts)]

    def pim_fill(tv, rows, eligible):
        keep = set(tv.selection())
        tv.delete(*tv.get_children())
        for r in rows:
            tv.insert("", "end", iid=r["key"], text=r["name"], tags=(r["status"],),
                      values=(r["type"], r["scope"], r["status"], r["expires"], (r.get("max") or "-") if eligible else "-", (r.get("requires_text") or "-") if eligible else "-"))
        keep = [k for k in keep if tv.exists(k)]
        if keep:
            tv.selection_set(keep)
        stripe(tv)

    def pim_buttons():
        busy = pim["busy"]
        todo = [r for r in pim["elig"] if r["status"] == "Eligible"]
        sel = [k for k in pim_elig_tree.selection()]
        pim_refresh_btn.state(["disabled"] if busy else ["!disabled"])
        pim_all_btn.state(["!disabled"] if (not busy and todo and pim["me"] and pim["me"].get("oid")) else ["disabled"])
        pim_sel_btn.state(["!disabled"] if (not busy and sel and pim["me"] and pim["me"].get("oid")) else ["disabled"])
        pim_stop_btn.state(["!disabled"] if (busy and pim["cancel"] is not None) else ["disabled"])

    def pim_render():
        va, ve = pim_visible(pim["active"]), pim_visible(pim["elig"])
        pim_fill(pim_active_tree, va, False)
        pim_fill(pim_elig_tree, ve, True)
        na, ne = len(pim["active"]), len([r for r in pim["elig"] if r["status"] == "Eligible"])
        txt = f"{na} active, {ne} eligible" + (f"   (showing {len(va)} and {len(ve)})" if (len(va), len(ve)) != (na, len(pim["elig"])) else "")
        pim_count_var.set(txt if pim["loaded"] else "")
        pim_buttons()

    def pim_set_chip(fam, text, kind):
        pim_chips[fam].configure(text=f"{PIM_FAMILY_NAME[fam]}: {text}", bg=COLORS.get(kind, COLORS["dim"]))

    def pim_apply(res):
        pim.update(me=res["me"], active=res["active"], elig=res["elig"] if "elig" in res else res["eligible"], loaded=True, busy=False, fams=res["families"])
        errors = {}
        for fam in PIM_FAMILIES:
            f = res["families"][fam]
            if f["state"] == "ok":
                pim_set_chip(fam, f["text"], "ok")
            else:
                st = pim_error_status(f["error"])
                pim_set_chip(fam, "failed" + (f" (HTTP {st})" if st else "") + " - see below", "err")
                errors[fam] = f["error"]
        pim["errors"] = errors
        fb = [f"{PIM_FAMILY_NAME[f]}: {errors[f]}" for f in ("entra", "group") if f in errors]
        if fb:
            pim_fb_var.set("\n".join(fb))
            pim_fb_help.set("The automatic method uses the PIM service behind the portal's own PIM page (through 'az rest'), no Microsoft Graph consent. It was refused or failed (message above - "
                            "paste it back if you need help). Options: (a) open the PIM page in your browser and activate there; (b) use " + PIM_SCRIPT_HINT + ".")
            pim_fb.pack(fill="x")
        else:
            pim_fb.pack_forget()
        if "arm" in errors:
            pim_say(f"{PIM_FAMILY_NAME['arm']} could not be read: {errors['arm']}", "err")
        else:
            why = pim_no_eligible_text(res)
            notice = res["me"].get("notice")
            pim_say(why or notice or (f"Signed in as {res['me'].get('upn')}. Select eligible roles and press 'Activate selected', or 'Activate ALL eligible roles at once' "
                                       f"- you confirm in a dialog first."), "warn" if (why or notice) else "info")
        pim_render()

    def pim_refresh(_e=None):
        if pim["busy"]:
            return
        gate = pim_gate()
        if gate:
            pim_say(gate, "warn")
            pim_buttons()
            return
        pim["busy"] = True
        pim["visited"] = True
        for fam in PIM_FAMILIES:
            pim_set_chip(fam, "reading ...", "dim")
        pim_say("Reading your privileged roles (read-only) ...", "info")
        pim_buttons()

        def work():
            try:
                me = pim_identity(force=True)
                if not me["ok"]:
                    pim["q"].put(("list_err", me["error"]))
                    return
                pim["q"].put(("list", pim_collect(me, lambda l: pim["q"].put(("log", l)))))
            except Exception as exc:
                pim["q"].put(("list_err", f"{type(exc).__name__}: {exc}"))
        threading.Thread(target=work, daemon=True).start()

    # -- the confirmation dialog: nothing is requested until Activate is pressed there
    def pim_open_confirm(rows):
        todo = [r for r in rows if r["status"] == "Eligible"]
        skipped = len(rows) - len(todo)
        if not todo:
            pim_say("Nothing to activate: no eligible role is selected (roles that are already active or waiting for approval are skipped).", "warn")
            return None
        if pim["dialog"] is not None:
            try:
                pim["dialog"]["win"].destroy()
            except Exception:
                pass
        win = tk.Toplevel(root)
        win.title("Activate privileged roles")
        win.configure(bg=BG)
        win.geometry("1000x680")
        win.transient(root)
        d = {"win": win, "todo": todo, "rows": rows}
        ttk.Label(win, text=f"{icon('shield')} Activate {len(todo)} role{'' if len(todo) == 1 else 's / groups'} (self-activation)", font=("Segoe UI", 13, "bold"),
                  foreground=BRAND_PRIMARY).pack(anchor="w", padx=12, pady=(10, 2))
        ttk.Label(win, text="These will be requested for you:", font=("Segoe UI", 10, "bold")).pack(anchor="w", padx=12)
        lw = ttk.Frame(win)
        lw.pack(fill="both", expand=True, padx=12, pady=(2, 6))
        tv = ttk.Treeview(lw, columns=("type", "scope", "max", "requires"), show="tree headings", height=8, selectmode="none")
        tv.heading("#0", text="Role or group name")
        tv.heading("type", text="Type")
        tv.heading("scope", text="Scope")
        tv.heading("max", text="Maximum duration allowed")
        tv.heading("requires", text="Requires")
        tv.column("#0", width=290)
        tv.column("type", width=170)
        tv.column("scope", width=200)
        tv.column("max", width=170)
        tv.column("requires", width=150)
        sb = ttk.Scrollbar(lw, orient="vertical", command=tv.yview)
        tv.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        tv.pack(side="left", fill="both", expand=True)
        for r in todo:
            tv.insert("", "end", iid=r["key"], text=r["name"], values=(r["type"], r["scope"], r.get("max") or "-", r.get("requires_text") or "-"))
        stripe(tv)
        d["tree"] = tv
        if skipped:
            ttk.Label(win, text=f"{skipped} selected item{'' if skipped == 1 else 's'} skipped (already active or waiting for approval).", style="Desc.TLabel").pack(anchor="w", padx=12)
        form = ttk.Frame(win)
        form.pack(fill="x", padx=12, pady=(4, 0))
        d["just_var"] = tk.StringVar(value=_SESSION.get("pim_just") or "")
        d["hours_var"] = tk.StringVar(value="")
        d["tno_var"] = tk.StringVar(value="")
        d["tsys_var"] = tk.StringVar(value="")
        ttk.Label(form, text="Justification (required):").grid(row=0, column=0, sticky="w")
        d["just_entry"] = ttk.Entry(form, textvariable=d["just_var"], width=80)
        d["just_entry"].grid(row=0, column=1, sticky="we", padx=(6, 0), pady=2)
        ttk.Label(form, text="Duration in hours (optional):").grid(row=1, column=0, sticky="w")
        hrow = ttk.Frame(form)
        hrow.grid(row=1, column=1, sticky="w", padx=(6, 0), pady=2)
        d["hours_entry"] = ttk.Entry(hrow, textvariable=d["hours_var"], width=8)
        d["hours_entry"].pack(side="left")
        ttk.Label(hrow, text="empty = each role's policy maximum; a number = that long, never more than a role allows", style="Desc.TLabel").pack(side="left", padx=8)
        ttk.Label(form, text="Ticket number (optional):").grid(row=2, column=0, sticky="w")
        trow = ttk.Frame(form)
        trow.grid(row=2, column=1, sticky="w", padx=(6, 0), pady=2)
        ttk.Entry(trow, textvariable=d["tno_var"], width=22).pack(side="left")
        ttk.Label(trow, text="Ticket system:").pack(side="left", padx=(10, 4))
        ttk.Entry(trow, textvariable=d["tsys_var"], width=22).pack(side="left")
        form.columnconfigure(1, weight=1)
        d["notice_var"] = tk.StringVar(value=pim_confirm_text(len(todo)))
        tk.Label(win, textvariable=d["notice_var"], anchor="w", justify="left", bg="#FFF8E1", fg=BRAND_TEXT, font=("Segoe UI", 10, "bold"), padx=8, pady=6,
                 wraplength=940).pack(fill="x", padx=12, pady=(8, 0))
        d["err_var"] = tk.StringVar(value="")
        tk.Label(win, textvariable=d["err_var"], anchor="w", bg=BG, fg=COLORS["err"], font=("Segoe UI", 9, "bold")).pack(fill="x", padx=12)
        bar = ttk.Frame(win)
        bar.pack(fill="x", padx=12, pady=(4, 10))
        d["cancel_btn"] = ttk.Button(bar, text="Cancel")
        d["cancel_btn"].pack(side="right")
        d["go_btn"] = ttk.Button(bar, text=f"Activate {len(todo)}", style="Accent.TButton")
        d["go_btn"].pack(side="right", padx=(0, 8))

        def valid(*_a):
            ok = bool(d["just_var"].get().strip())
            d["go_btn"].state(["!disabled"] if ok else ["disabled"])
            d["err_var"].set("" if ok else "A justification is required.")
            return ok
        d["just_var"].trace_add("write", valid)
        valid()

        def close():
            pim["dialog"] = None
            _GUI["pim_dialog"] = None
            try:
                win.destroy()
            except Exception:
                pass

        def go():
            just = d["just_var"].get().strip()
            if not just:
                d["err_var"].set("A justification is required.")
                return
            hv = d["hours_var"].get().strip()
            hours = None
            if hv:
                try:
                    hours = float(hv.replace(",", "."))
                    if hours <= 0 or hours > 24 * 30:
                        raise ValueError
                except ValueError:
                    d["err_var"].set("Duration must be a number of hours (for example 4 or 0.5), or empty.")
                    return
            _SESSION["pim_just"] = just            # remembered for this session only (memory, never written to a file)
            tno, tsys = d["tno_var"].get().strip(), d["tsys_var"].get().strip()
            close()
            pim_run(todo, just, hours, tno, tsys)
        d["go_btn"].configure(command=go)
        d["cancel_btn"].configure(command=close)
        win.protocol("WM_DELETE_WINDOW", close)
        d["close"], d["go"] = close, go
        pim["dialog"] = d
        _GUI["pim_dialog"] = d
        try:
            win.grab_set()
        except Exception:
            pass
        d["just_entry"].focus_set()
        return d

    def pim_run(rows, just, hours, tno, tsys):
        if pim["busy"] or not pim["me"]:
            return
        pim["busy"] = True
        pim["cancel"] = threading.Event()
        pim["results"] = {}
        pim["total"], pim["done"] = len(rows), 0
        pim_res_tree.delete(*pim_res_tree.get_children())
        for r in rows:
            pim_res_tree.insert("", "end", iid="q|" + r["key"], text=r["name"], values=(r["type"], "Queued", ""), tags=("Queued",))
        pim_progress.configure(maximum=max(1, len(rows)), value=0)
        pim_summary_var.set(f"Requesting {len(rows)} ...")
        pim_say(f"Submitting {len(rows)} self-activation request{'' if len(rows) == 1 else 's'} (4 at a time) ...", "info")
        pim_buttons()
        me, ev, q = dict(pim["me"]), pim["cancel"], pim["q"]

        def work():
            try:
                results = pim_activate_many(rows, me, just, hours, tno, tsys, on_start=lambda r: q.put(("start", r["key"])), on_result=lambda res: q.put(("result", res)),
                                            cancel=ev, emit=lambda l: q.put(("log", l)))
                q.put(("adone", results))
            except Exception as exc:
                q.put(("adone_err", f"{type(exc).__name__}: {exc}"))
        threading.Thread(target=work, daemon=True).start()

    def pim_activate_selected(_e=None):
        keys = list(pim_elig_tree.selection())
        rows = [r for r in pim["elig"] if r["key"] in keys]
        if not rows:
            pim_say("Select one or more eligible roles first (or use 'Activate ALL eligible roles at once').", "warn")
            return None
        return pim_open_confirm(rows)

    def pim_activate_all(_e=None):
        return pim_open_confirm(list(pim["elig"]))

    def pim_stop(_e=None):
        if pim["cancel"] is not None:
            pim["cancel"].set()
            pim_say("Stopping: requests not sent yet are cancelled. A request that was already sent cannot be recalled - Refresh shows what happened.", "warn")

    def pim_pump():
        try:
            while True:
                try:
                    kind, *rest = pim["q"].get_nowait()
                except queue.Empty:
                    break
                if kind == "log":
                    write(rest[0])
                elif kind == "list":
                    pim_apply(rest[0])
                elif kind == "list_err":
                    pim["busy"] = False
                    pim["loaded"] = False
                    for fam in PIM_FAMILIES:
                        pim_set_chip(fam, "not loaded", "dim")
                    pim_say(rest[0], "err")
                    pim_buttons()
                elif kind == "start":
                    iid = "q|" + rest[0]
                    if pim_res_tree.exists(iid):
                        pim_res_tree.item(iid, values=(pim_res_tree.set(iid, "type"), "Requesting ...", ""), tags=("Requesting",))
                elif kind == "result":
                    res = rest[0]
                    iid = "q|" + res["key"]
                    pim["done"] += 1
                    pim["results"][res["key"]] = res
                    vals = (res["type"], res["outcome"], res["reason"])
                    if pim_res_tree.exists(iid):
                        pim_res_tree.item(iid, values=vals, tags=(res["outcome"],))
                    else:
                        pim_res_tree.insert("", "end", iid=iid, text=res["name"], values=vals, tags=(res["outcome"],))
                    pim_progress.configure(value=pim["done"])
                    pim_summary_var.set(f"{pim['done']} of {pim['total']} done")
                elif kind in ("adone", "adone_err"):
                    pim["busy"] = False
                    pim["cancel"] = None
                    if kind == "adone":
                        pim["summary"] = pim_summary(rest[0])
                        pim_summary_var.set("Summary: " + pim["summary"])
                        pim_say("Finished: " + pim["summary"] + ". Refreshing the active list ...", "ok" if not any(x["outcome"] == "Denied" for x in rest[0]) else "warn")
                        write("PIM summary: " + pim["summary"])
                    else:
                        pim_summary_var.set("Failed: " + rest[0])
                        pim_say("Activation failed: " + rest[0], "err")
                    pim_buttons()
                    root.after(300, pim_refresh)                    # auto-refresh the active list
        except tk.TclError:
            return
        root.after(150, pim_pump)

    def pim_open_portal(kind):
        try:
            webbrowser.open(PIM_PORTAL_URLS[kind])
        except Exception:
            pass
        pim_say("Opened the PIM page in your browser. Activate your roles there, then come back and press Refresh.", "info")

    def pim_select_all(_e=None):
        ks = [k for k in pim_elig_tree.get_children() if "Eligible" in pim_elig_tree.item(k, "tags")]
        if ks:
            pim_elig_tree.selection_set(ks)
        pim_buttons()

    def pim_clear_sel(_e=None):
        pim_elig_tree.selection_remove(pim_elig_tree.selection())
        pim_buttons()

    def pim_on_tab(_e=None):
        try:
            if nb.nametowidget(nb.select()) is not tab_pim:
                return
        except Exception:
            return
        if not pim["visited"] and not pim["busy"]:
            pim["visited"] = True
            gate = pim_gate()
            if gate:
                pim_say(gate, "warn")
            else:
                pim_refresh()

    pim_refresh_btn.configure(command=pim_refresh)
    pim_sel_btn.configure(command=pim_activate_selected)
    pim_all_btn.configure(command=pim_activate_all)
    pim_stop_btn.configure(command=pim_stop)
    pim_selall_btn.configure(command=pim_select_all)
    pim_clear_btn.configure(command=pim_clear_sel)
    pim_open_entra_btn.configure(command=lambda: pim_open_portal("entra"))
    pim_open_group_btn.configure(command=lambda: pim_open_portal("group"))
    pim_copy_err_btn.configure(command=lambda: copy_text(pim_fb_var.get()))
    pim_elig_tree.bind("<<TreeviewSelect>>", lambda _e: pim_buttons())
    pim_search_var.trace_add("write", lambda *_a: pim_render())
    pim_type_combo.bind("<<ComboboxSelected>>", lambda _e: pim_render())
    nb.bind("<<NotebookTabChanged>>", pim_on_tab, add="+")
    pim_say(pim_gate() or "Press Refresh to read your privileged roles (read-only).", "info")
    root.after(150, pim_pump)
    _GUI.update(tab_pim=tab_pim, pim=pim, pim_refresh=pim_refresh, pim_refresh_btn=pim_refresh_btn, pim_sel_btn=pim_sel_btn, pim_all_btn=pim_all_btn, pim_stop_btn=pim_stop_btn,
                pim_active_tree=pim_active_tree, pim_elig_tree=pim_elig_tree, pim_res_tree=pim_res_tree, pim_chips=pim_chips, pim_msg_var=pim_msg_var,
                pim_count_var=pim_count_var, pim_search_var=pim_search_var, pim_type_var=pim_type_var, pim_type_combo=pim_type_combo, pim_selall_btn=pim_selall_btn,
                pim_clear_btn=pim_clear_btn, pim_fb=pim_fb, pim_fb_var=pim_fb_var, pim_open_entra_btn=pim_open_entra_btn, pim_open_group_btn=pim_open_group_btn,
                pim_summary_var=pim_summary_var, pim_progress=pim_progress, pim_activate_selected=pim_activate_selected, pim_activate_all=pim_activate_all,
                pim_note=pim_note, pim_gate=pim_gate, pim_open_confirm=pim_open_confirm, pim_dialog=None, pim_pump=pim_pump, pim_stop=pim_stop, pim_msg=pim_msg,
                pim_paned=pim_paned, pim_copy_err_btn=pim_copy_err_btn, pim_fb_help=pim_fb_help)

    def prepare(r):
        r["hay"] = " ".join(str(x) for x in (r.get("number") or "", r.get("label") or "", r["name"], r.get("where") or "",
                                              r.get("account_name") or "", r.get("account") or "")).lower()
        return r

    def index_rows():
        state["by_key"] = {r["key"]: r for r in state["crows"]}

    def effective_accounts():
        """The subscriptions the cluster listing covers: ONLY the selected ones (nothing is searched unless the user asks)."""
        return [a for a in state["accounts"] if a["id"] in state["acct_chosen"] and a.get("usable", True)]      # a disabled subscription has no reachable clusters

    def signin_account():
        """The account the sign-in belongs to (the AWS profile; the other clouds sign in once)."""
        chosen = [a["id"] for a in state["accounts"] if a["id"] in state["acct_chosen"]]
        return chosen[0] if scope_var.get() == "sel" and chosen else None

    def update_sel_counter():
        acct_sel_count.set(f"{len(state['acct_chosen'])} of {len(state['accounts'])} selected" if state["accounts"] and not state["acct_locked"] else "")

    def update_scope_labels():
        usable = len([a for a in state["accounts"] if a.get("usable", True)])
        if is_cli():
            scope_all_rb.configure(text=f"All subscriptions ({usable})")
            scope_sel_rb.configure(text=f"Only the selected subscriptions ({len(state['acct_chosen'])})")
        else:
            scope_all_rb.configure(text="(auto) - picked after login")
            scope_sel_rb.configure(text=f"Use the selected subscription ({len(state['acct_chosen'])})")

    # ---- step 3 list
    def rebuild_account_list():
        acct_tree.delete(*acct_tree.get_children())
        if state["acct_locked"]:
            acct_tree.insert("", "end", iid="__hint__", text="Sign in first (step 2)", tags=("hint",))
            acct_count.set("")
            return
        ts = terms(acct_filter)
        shown = [a for a in state["accounts"] if hit(a["hay"], ts)]
        for i_, a in enumerate(shown):
            acct_tree.insert("", "end", iid=a["id"], text=a["name"], values=(a["code"], a["info"]), tags=(("odd",) if i_ % 2 else ()))
        acct_tree.selection_set([a["id"] for a in shown if a["id"] in state["acct_chosen"]])
        acct_count.set(f"Showing {len(shown)} of {len(state['accounts'])}")
        update_sel_counter()

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
        scope_var.set("sel")
        rebuild_account_list()
        on_scope_change()

    def on_scope_change():
        _SESSION["subs"] = set(state["acct_chosen"])             # remembered for the session
        update_sel_counter()
        update_scope_labels()
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
        return state["crows"]                    # every cluster collected so far (the cache keeps earlier subscriptions)

    def rebuild_cluster_list():
        cluster_tree.delete(*cluster_tree.get_children())
        if state["cl_locked"]:
            cluster_tree.insert("", "end", iid="__hint__", text="Sign in first (step 2)", tags=("hint",))
            cl_count.set("")
            update_selected_label()
            return
        rows = scoped_rows()
        if not rows and not state["listing"] and not state["listed"] and (is_cli() or src_var.get() == "all"):
            cluster_tree.insert("", "end", iid="__hint__", text=COLLECT_HINT, tags=("hint",))
            cl_count.set("")
            update_selected_label()
            return
        ts = terms(filter_var)
        shown = [r for r in rows if hit(r["hay"], ts)]
        for i_, r in enumerate(shown):
            cluster_tree.insert("", "end", iid=r["key"], text=(f"{r['number']} - {r['name']}" if r.get("number") else r["name"]),
                                values=(r.get("where") or "", r.get("account_name") or ""), tags=(("odd",) if i_ % 2 else ()))
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

    def cluster_hint():
        """The line under the cluster list: where the list stands."""
        if state["cl_locked"]:
            cl_status.set("Sign in first (step 2).")
        elif state["listing"]:
            el_ = int(time.time() - (state.get("list_t0") or time.time()))
            cl_status.set(f"Listing clusters: {state['list_done']}/{state['list_total']} subscriptions ... ({count_of(len(state['crows']), 'cluster')} so far) - elapsed {el_ // 60}:{el_ % 60:02d}")
        elif state.get("list_stopped"):
            pass                                              # keep the 'Listing stopped' line
        elif (is_cli() or src_var.get() == "all") and not state["listed"] and not state["crows"]:
            n_ = len(effective_accounts())
            cl_status.set(COLLECT_HINT if not n_ else f"{count_of(n_, 'subscription')} selected - press 'Collect clusters from selected subscriptions'.")
        elif (is_cli() or src_var.get() == "all") and not state.get("list_stopped"):
            todo = [a for a in effective_accounts() if a["id"] not in state["listed"]]
            if todo:
                cl_status.set(f"{len(todo)} of the selected subscriptions are not collected yet - press 'Collect clusters from selected subscriptions'.")

    # ---- sign-in state: badge, message, what is unlocked
    def update_controls():
        cli = is_cli()
        busy, listing = state["busy"], state["listing"]
        auth_ok = state["auth"]["state"] == "ok"
        working = state["signing"] or state["checking"]
        idle = not busy and not listing and not working
        set_state(signin_btn, cli and idle)
        set_state(check_btn, cli and idle)
        set_state(device_chk, cli and not working)
        set_state(switch_btn, cli and idle)
        set_state(tenant_entry, cli and not working)
        set_state(sp_cancel_btn, bool(state["signin"]) and state["signin"].get("state") == "running" and state["signing"])
        acct_combo.configure(state="normal" if (cli and auth_ok and idle) else "disabled")
        set_state(recheck_btn, cli and auth_ok and idle and not state["checking_accts"])
        set_state(check_all_btn, cli and auth_ok and idle and not state["checking_accts"])
        set_state(src_all_rb, not cli and not busy and not listing)
        set_state(src_menu_rb, not cli and not busy and not listing)
        state["acct_locked"] = cli and NEED_SIGNIN and not auth_ok
        state["cl_locked"] = cli and not auth_ok
        for w in acct_widgets:
            set_state(w, not state["acct_locked"])
        for w in cl_widgets:
            set_state(w, not state["cl_locked"])
        set_state(acct_all_btn, not state["acct_locked"])
        set_state(acct_reload_btn, not state["acct_locked"] and not busy and not state["acct_loading"])
        set_state(refresh_btn, not state["cl_locked"] and not busy and not listing and (cli or src_var.get() == "all"))
        set_state(collect_btn, not state["cl_locked"] and not busy and not listing and (cli or src_var.get() == "all"))
        set_state(collect_stop_btn, listing)
        set_state(select_all_btn, not state["cl_locked"])
        set_state(clear_btn, not state["cl_locked"])
        acct_tree.configure(selectmode="none" if state["acct_locked"] else "extended")
        cluster_tree.configure(selectmode="none" if state["cl_locked"] else "extended")
        method_combo.configure(state="disabled" if (busy or listing or working) else "readonly")
        signin_combo.configure(state="disabled" if (busy or listing or working) else "readonly")
        run_btn.state(["disabled"] if (busy or listing) else ["!disabled"])
        stop_btn.state(["!disabled"] if (busy or listing) else ["disabled"])
        for sid_, cb_ in sec_checks.items():            # the 'What to collect' panel is frozen while a run is active
            cb_.state(["disabled"] if (busy or SECTION_BY_ID[sid_].get("locked") or (sid_ == "azure" and not az_var.get())) else ["!disabled"])
        for b_ in preset_btns.values():
            b_.state(["disabled"] if busy else ["!disabled"])
        workers_spin.state(["disabled"] if busy else ["!disabled"])
        update_banner()
        s3.configure(text=f"{icon('cloud')} Step 3 - Choose subscription(s)  (Ctrl/Shift-click for several)" + ("   [locked - sign in first]" if state["acct_locked"] else
                                                       (f"   [{len(state['accounts'])} loaded]" if state["accounts"] else "")))
        s4.configure(text=f"{icon('helm')} Step 4 - Choose clusters  (click one, Ctrl/Shift-click for several; they run one after another)"
                          + ("   [locked - sign in first]" if state["cl_locked"] else ""))
        if state.get("locks") != (state["acct_locked"], state["cl_locked"]):
            state["locks"] = (state["acct_locked"], state["cl_locked"])
            rebuild_account_list()
            rebuild_cluster_list()
        cluster_hint()

    def apply_method_ui():
        if is_cli():
            method_info.set("Uses the Azure CLI (az): sign in in step 2, then the subscriptions and clusters are read from Azure.")
        else:
            method_info.set("Uses akslogin.exe: it signs in when you press Run. The cluster list below can show every cluster you can access "
                            "(read with az; clusters that are not in the akslogin menu are logged in with az aks get-credentials) or only the akslogin menu.")
        update_scope_labels()
        if not is_cli():
            man_hide()
            state["auth"] = {"state": "unchecked"}
            set_badge("Handled by akslogin", "dim")
            auth_msg.set("Uses akslogin.exe - it signs in when you press Run. The sign-in buttons are only used with the Cloud CLI method.")
            say("Custom login: choose the subscription if you want to force one (otherwise it is picked after login), select the clusters in step 4 and press "
                "'Login & Debug'.", "info")
        else:
            set_badge("Not checked", "dim")
            auth_msg.set("")

    def apply_auth(res, source):
        if source == "manual_auto":            # the background check of the manual panel: only a success matters
            man["probing"] = False
            if res["state"] != "ok" or not man["active"]:
                return
            if man.get("expect") and res.get("who") != man["expect"]:        # opened for an expired account: wait until THAT account is valid again
                return
            source = "manual_verify"
        state["checking"] = state["signing"] = False
        if not is_cli():                      # the method was switched while this was running
            update_controls()
            return
        was_ok = state["auth"]["state"] == "ok"
        prev_who = state["auth"].get("who") if was_ok else None
        if not (source == "manual_verify" and res["state"] != "ok" and res.get("expired") and was_ok):      # expired credentials: the account stays selected
            state["auth"] = res
        st = res["state"]
        who_var.set("Signed in as: " + (f"{res.get('who') or '?'}" + (f"  (tenant {res['tenant']})" if res.get("tenant") else "") if st == "ok" else "-"))
        if st == "ok":
            CRED["current"] = res.get("who") or CRED.get("current")
            who = res.get("who") or "?"
            n_subs = res.get("n_subs")
            if source == "manual_verify":
                tok = res.get("tok") or {}
                set_account_status(who, tok.get("state") if tok.get("state") in ("active", "expiring") else "active", tok.get("left"), "")
                if (not was_ok) or man.get("different") or man.get("expect") or (prev_who and prev_who != who):
                    state.update(accounts_loaded=False, listed=set(), crows=[], by_key={}, cchosen=set(), clusters={})        # a new / renewed sign-in: read everything again
                if man.get("different"):
                    state["sel_acct"] = who
                man.update(different=False, expect=None)
            set_badge(f"Signed in as {who}" + (f" - {count_of(n_subs, 'subscription')}" if source == "manual_verify" and n_subs is not None else ""), "ok")
            auth_msg.set("You are signed in. Next: step 3 and step 4.")
            lead = {"signin_ok": "Login OK - ", "manual_verify": "Login OK - ", "already": "Already signed in - "}.get(source, "")
            if man["shown"] and (man["active"] or source == "manual_verify"):
                man_signed_in(res)
            say(f"{lead}signed in as {who}. Next: choose the subscription in step 3 (or keep 'All subscriptions') and the clusters in step 4.", "ok")
        elif source == "manual_verify":
            hint = signin_failure_help(res, man_tenant(), man_account_tenant())
            set_badge("az not installed" if st == "no_cli" else ("Credentials expired" if res.get("expired") else "Not signed in"), "err")
            auth_msg.set(hint)
            say(hint.replace("\n", "  "), "err")
            man_failed(hint)
        elif st == "no_cli":
            set_badge("az not installed", "err")
            auth_msg.set(res.get("detail") or "")
            say(f"{res.get('detail')} {res.get('hint')}", "err")
        else:
            set_badge("Not signed in", "err")
            auth_msg.set(f"Reason: {res.get('detail') or 'unknown'}\nNext: {res.get('hint')}")
            if source == "signin_fail":
                say("Sign-in failed or was cancelled (" + (res.get("detail") or "no details") + "). Press 'Sign in' to try again, or run 'az login' in a "
                    "terminal and then press 'Check status'.", "err")
            else:
                say("Not signed in. " + (res.get("hint") or ""), "err")
        if st != "ok" and source == "check" and LOGIN_OPTS.get("signin") == "manual":
            man_open()                                  # the default method: show the commands right away (also when az is not installed)
        if st == "ok" and state["accounts_loaded"] and not state["accounts"]:
            say(acct_status.get(), "warn")
        update_controls()
        refresh_expiry_banner()
        if st == "ok" and (not was_ok or source in ("signin_ok", "already", "manual_verify")):
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
        auth_msg.set("Checking whether az is signed in ...")
        say("Checking the sign-in ...", "info")
        update_controls()

        def work():
            try:
                res = verify_signin(acct) if source == "manual_verify" else login_status(acct)
            except Exception as exc:
                res = {"state": "not_signed_in", "who": None, "detail": str(exc), "hint": "Press 'Sign in'."}
            msgs.put(("auth", res, source))
        threading.Thread(target=work, daemon=True).start()

    # ---- the account dropdown, chips and the credentials state
    ACCT_KIND = {"active": "ok", "expiring": "warn", "expired": "err", "none": "dim", "unknown": "dim"}

    def selected_account_key():
        return state["sel_acct"] or (state["auth"].get("who") if state["auth"].get("state") == "ok" else None)

    def account_status_of(key):
        return CRED["status"].get(key)

    def acct_expired():
        """The account in use (selected one, else the signed-in one) whose credentials are expired, or None."""
        key = selected_account_key()
        return key if key and (account_status_of(key) or {}).get("state") == "expired" else None

    def chip_text(st):
        return status_text(st) if st else "Not checked"

    def render_accounts():
        """Dropdown values (type to search), the chip of the selected account and one chip per account."""
        accts = state["accts"]
        state["acct_map"] = {}
        values = ["All accounts"]
        for a_ in accts:
            st = account_status_of(a_["key"])
            txt = f"[{ACCOUNT_STATES.get((st or {}).get('state'), 'Not checked')}]  {a_['label']}"
            state["acct_map"][txt] = a_["key"]
            values.append(txt)
        state["acct_values"] = values
        typed = acct_combo.get()
        cur = next((t for t, k in state["acct_map"].items() if k == state["sel_acct"]), "All accounts")
        flt = [v for v in values if typed.lower() in v.lower()] if (typed and typed != cur and typed not in values) else values
        acct_combo.configure(values=flt or values)
        if not (typed and typed != cur and typed not in values):
            acct_combo.set(cur)
        key = selected_account_key()
        st = account_status_of(key) if key else None
        acct_chip.configure(text=chip_text(st) if key else "-", bg=COLORS[ACCT_KIND.get((st or {}).get("state"), "dim")])
        for w in chips_row.winfo_children():
            w.destroy()
        state["chips"] = {}
        for a_ in accts[:8]:
            st = account_status_of(a_["key"])
            kind = ACCT_KIND.get((st or {}).get("state"), "dim")
            lbl = tk.Label(chips_row, text=f"{a_['user']}: {ACCOUNT_STATES.get((st or {}).get('state'), 'Not checked')}", fg="white", bg=COLORS[kind],
                           padx=6, pady=1, font=("Segoe UI", 8, "bold"), cursor="hand2")
            lbl.pack(side="left", padx=(0, 4))
            lbl.bind("<Button-1>", lambda _e, k=a_["key"]: choose_account(k))
            state["chips"][a_["key"]] = lbl
        if len(accts) > 8:
            tk.Label(chips_row, text=f"+{len(accts) - 8} more (use the dropdown)", fg=COLORS["dim"]).pack(side="left")

    def refresh_expiry_banner():
        """Expired credentials of the account in use: red badge + message ('Credentials for X expired. Press Sign in to renew.') and a highlighted button."""
        if not is_cli() or state["signing"]:
            return
        key = acct_expired()
        if key:
            set_badge("Credentials expired", "err")
            msg = f"Credentials for {key} expired. Press Sign in to renew."
            auth_msg.set(msg)
            say(msg, "err")
            signin_btn.configure(text=f"{icon('key')} Sign in again", style="Alert.TButton")
            if LOGIN_OPTS.get("signin") == "manual":
                man_open(reason=None, force=True, quiet=True)       # the commands right under the message: renew without leaving this window
        else:
            signin_btn.configure(text=f"{icon('key')} Sign in", style="Accent.TButton")
        render_accounts()

    def check_accounts_async(accts):
        accts = [a_ for a_ in accts if a_.get("check_sub") or a_.get("check_tenant") or len(accts) == 1]
        if not accts:
            return
        state["checking_accts"] = True
        update_controls()

        def work():
            try:
                check_accounts(accts, lambda a_, st: msgs.put(("acctstat", a_["key"])))
            finally:
                msgs.put(("acctdone",))
        threading.Thread(target=work, daemon=True).start()

    def apply_account_scope():
        """Step 3 shows only the chosen account's subscriptions (its tenant); the clusters are listed again."""
        sel = state["sel_acct"]
        keys = {a_["key"] for a_ in state["accts"]}
        if sel not in keys:
            sel = state["sel_acct"] = None
        state["accounts"] = [a_ for a_ in state["acct_all"] if sel is None or a_.get("user") == sel]
        for a_ in state["accounts"]:
            a_["hay"] = f"{a_['name']} {a_['code']} {a_['info']}".lower()
        state["acct_by_id"] = {a_["id"]: a_ for a_ in state["accounts"]}
        state["acct_chosen"] &= set(state["acct_by_id"])
        if scope_var.get() == "sel" and not state["acct_chosen"]:
            scope_var.set("sel")
        state.update(crows=[], by_key={}, cchosen=set(), clusters={}, listed=set())
        update_scope_labels()
        rebuild_account_list()
        rebuild_cluster_list()
        refresh_expiry_banner()

    def choose_account(key):
        if not is_cli() or state["busy"] or state["listing"] or state["signing"] or state["acct_loading"]:
            status.set("Wait for the current run / listing / sign-in to finish before changing the account.")
            render_accounts()
            return
        state["sel_acct"] = key
        CRED["current"] = key or CRED.get("current")
        apply_account_scope()
        if key:
            a_ = next((x for x in state["accts"] if x["key"] == key), None)
            tn = (a_["tenants"][0][1] if a_ and a_["tenants"] else "")
            acct_status.set(f"Account {key}: {count_of(len(state['accounts']), 'subscription')}" + (f" in tenant {tn}" if tn else "") + ".")
            if a_:
                check_accounts_async([a_])
        else:
            acct_status.set(f"All accounts: {count_of(len(state['accounts']), 'subscription')}.")
        update_controls()
        if not acct_expired():
            maybe_auto_list()

    def on_acct_combo(_event=None):
        txt = acct_combo.get()
        if txt == "All accounts":
            choose_account(None)
        elif txt in state["acct_map"]:
            choose_account(state["acct_map"][txt])

    def on_acct_type(event=None):
        if event is not None and event.keysym in ("Return", "Up", "Down", "Escape", "Tab"):
            return
        typed = acct_combo.get().strip().lower()
        vals = state.get("acct_values") or ["All accounts"]
        acct_combo.configure(values=[v for v in vals if typed in v.lower()] or vals)

    def recheck_selected():
        key = selected_account_key()
        a_ = next((x for x in state["accts"] if x["key"] == key), None)
        if a_ is None:
            check_status()
        else:
            check_accounts_async([a_])

    def check_all_accounts():
        check_accounts_async(list(state["accts"]))

    # ---- sign-in with the details panel
    def fmt_left(sec):
        sec = max(0, int(sec))
        return f"{sec // 60:02d}:{sec % 60:02d}"

    def panel_reset(sess):
        sp.pack(fill="x", pady=(6, 0))
        dev = sess["device"] and not sess.get("console")
        sp_url.configure(text="waiting for az to print the sign-in URL ..." if dev else (
            "A console window opened for the sign-in - complete it there." if sess.get("console") else "A browser window opens for the sign-in - complete it there."))
        sp_code.configure(text="........" if dev else ("(see the console window)" if sess.get("console") else "(no code needed)"))
        sp_chip.configure(text="Waiting", bg=COLORS["info"])
        sp_detail.set("")
        sp_raw.set("")
        sp_account.set("Account / tenant: " + (f"tenant {sess['tenant']}" if sess["tenant"] else "chosen when you sign in")
                       + (f"   (replacing {sess['previous']})" if sess.get("previous") else ""))
        sp_countdown.set("Starting sign-in ...")
        set_state(sp_open_btn, False)
        set_state(sp_copy_url_btn, False)
        set_state(sp_copy_code_btn, False)

    def tick():
        state["tick"] = None
        sess = state["signin"]
        if not sess or sess.get("state") != "running":
            return
        if (sess["device"] and not sess.get("console") and not sess.get("url") and not sess.get("nourl") and time.time() - sess["t0"] > NO_URL_SECONDS):
            sess["nourl"] = sess["autoswitch"] = True               # az printed no URL: stop it and switch to the commands the user runs in their own terminal
            sess["cancel"].set()
            man_open(reason=NO_URL_REASON, force=True)
        if sess.get("expires"):
            left = sess["expires"] - time.time()
            sp_countdown.set(f"waiting for you to sign in... {fmt_left(left)}" if left > 0 else "The code has expired - cancel and start the sign-in again.")
            if left <= 0:
                sp_chip.configure(text="Expired", bg=COLORS["warn"])
        else:
            sp_countdown.set("waiting for you to sign in..." if not sess["device"] else "Starting sign-in ...")
        state["tick"] = root.after(500, tick)

    def on_signin_event(sess, ev):
        if sess is not state["signin"]:
            return
        if ev["kind"] == "line":
            sess["lines"] = (sess["lines"] + [ev["line"]])[-5:]
            if not sess.get("code"):
                sp_raw.set("\n".join(sess["lines"]))              # parsing failed (or not yet): the raw output stays readable
        elif ev["kind"] == "code":
            sess.update(url=ev["url"], code=ev["code"], expires=ev["expires_at"])
            sp_url.configure(text=ev["url"])
            sp_code.configure(text=ev["code"])
            sp_raw.set("")
            for b_ in (sp_open_btn, sp_copy_url_btn, sp_copy_code_btn):
                set_state(b_, True)
            sp_chip.configure(text="Waiting for sign-in", bg=COLORS["info"])
            set_badge("Waiting for sign-in", "info")
            say(f"Open {ev['url']} and enter the code {ev['code']} (details in step 2).", "info")
            tick()

    def open_signin_url(_e=None):
        u = (state["signin"] or {}).get("url")
        if u:
            webbrowser.open(u)

    def copy_text(txt):
        if txt:
            root.clipboard_clear()
            root.clipboard_append(txt)

    def cancel_signin():
        sess = state["signin"]
        if sess and sess.get("state") == "running":
            sess["cancel"].set()
            sp_chip.configure(text="Cancelling", bg=COLORS["warn"])
            sp_countdown.set("Cancelling the sign-in ...")

    def start_signin(different=False):
        if not is_cli() or state["checking"] or state["signing"] or state["busy"] or state["listing"]:
            return
        sync_login_opts()
        tenant = tenant_var.get().strip() or None
        if tenant and not TENANT_RE.match(tenant):
            say("The tenant must be a tenant id (GUID) or a domain name such as contoso.onmicrosoft.com - or leave the box empty.", "warn")
            return
        LOGIN_OPTS["tenant"] = tenant
        acct = signin_account()
        force = different or bool(acct_expired())
        if LOGIN_OPTS["signin"] == "manual":                # the default: show the commands, the user runs one in their own terminal, then presses Verify
            man_open(force=force, different=different)
            return
        sess = {"cancel": threading.Event(), "state": "running", "device": bool(device_var.get()), "console": LOGIN_OPTS["signin"] == "console",
                "tenant": tenant, "url": None, "code": None,
                "expires": None, "lines": [], "different": different, "previous": (state["auth"].get("who") if different else None), "t0": time.time()}
        state.update(signing=True, auth_for=acct)
        set_badge("Signing in...", "info")
        auth_msg.set("Running the sign-in ...")
        update_controls()

        def work():
            try:
                if not force:
                    pre = login_status(acct)
                    if pre["state"] != "not_signed_in":
                        msgs.put(("auth", pre, "already" if pre["state"] == "ok" else "check"))
                        return
                msgs.put(("signin_begin", sess))
                res = cli_sign_in_detailed(lambda l: msgs.put(("line", l)), lambda ev: msgs.put(("signin_ev", sess, ev)), sess["cancel"], tenant, sess["device"], sess["console"])
                msgs.put(("signin_done", sess, res, login_status(acct)))
            except Exception as exc:
                msgs.put(("signin_done", sess, {"status": "error", "error": str(exc), "rc": None, "lines": []},
                          {"state": "not_signed_in", "who": None, "detail": str(exc), "hint": "Press 'Sign in' to try again."}))
        threading.Thread(target=work, daemon=True).start()

    def sign_in(_event=None):
        start_signin(False)

    def sign_in_different(_event=None):
        start_signin(True)

    def on_signin_begin(sess):
        state["signin"] = sess
        panel_reset(sess)
        update_controls()
        say("Signing in: " + ("a console window opened - complete the sign-in there; this window continues when it closes ..." if sess.get("console") else
                              ("the device-code URL and code appear in 'Sign-in details' below ..." if sess["device"] else "complete the sign-in in the browser window ...")), "info")
        tick()

    def on_signin_done(sess, res, st):
        if sess is not state["signin"]:
            state["signing"] = False
            update_controls()
            return
        status_ = res.get("status")
        sess["state"] = status_
        if state["tick"]:
            root.after_cancel(state["tick"])
            state["tick"] = None
        sp_countdown.set("")
        if sess.get("autoswitch"):                          # no URL: the manual commands were opened by tick(); nothing more to report
            sp_chip.configure(text="No URL", bg=COLORS["warn"])
            sp_detail.set("az printed no sign-in URL - use the commands in the panel above.")
            state["signing"] = False
            update_controls()
            return
        chip = {"ok": ("Signed in", "ok"), "cancelled": ("Cancelled", "warn"), "expired": ("Code expired", "warn")}.get(status_, ("Failed", "err"))
        sp_chip.configure(text=chip[0], bg=COLORS[chip[1]])
        if status_ == "ok" and st.get("state") == "ok":
            who = st.get("who") or "?"
            sess["finalize"] = who
            sp_detail.set(f"Signed in as {who} - checking subscriptions ...")
            state.update(accounts_loaded=False, listed=set(), crows=[], by_key={}, cchosen=set(), clusters={})
            if sess["different"]:
                state["sel_acct"] = who
            set_account_status(who, "active")
            CRED["current"] = who
            state["signing"] = False
            apply_auth(st, "signin_ok")
            return
        why = res.get("error") or (st.get("detail") if st.get("state") != "ok" else "") or "no details"
        sp_detail.set({"cancelled": "Sign-in cancelled."}.get(status_, why))
        state["signing"] = False
        if st.get("state") == "ok":                         # a failed switch leaves the previous account signed in
            apply_auth(st, "check")
            say({"cancelled": "Sign-in cancelled - you are still signed in as " + str(st.get("who")) + "."}.get(status_, f"Sign-in failed: {why}"),
                "warn" if status_ == "cancelled" else "err")
        else:
            apply_auth(dict(st, detail=why), "signin_fail")
            if status_ == "cancelled":
                say("Sign-in cancelled. Press 'Sign in' to start again.", "warn")
        if status_ not in ("ok", "cancelled"):                # failed / expired / could not start / no URL: switch to the commands the user runs themselves
            man_open(reason=NO_URL_REASON if not sess.get("url") else FAILED_REASON, force=True, quiet=True)
        update_controls()

    # ---- manual sign-in: show the commands, wait for the user, verify read-only (az account show / expiry check / az account list)
    def man_tenant():
        t = tenant_var.get().strip()
        return t if t and TENANT_RE.match(t) else None

    def man_account_tenant():
        """The real tenant id of the selected account (else of the signed-in one), when known."""
        a_ = next((x for x in state["accts"] if x["key"] == state["sel_acct"]), None) if state["sel_acct"] else None
        if a_ and len(a_["tenants"]) == 1 and a_["tenants"][0][0] not in (None, "?"):
            return a_["tenants"][0][0]
        return state["auth"].get("tenant_id") if state["auth"].get("state") == "ok" else None

    def man_refresh():
        """Put the tenant into the commands (1b variant line when the Tenant box has a value; command 3 uses the real tenant id when known)."""
        items = manual_commands(man_tenant(), man_account_tenant())
        for item in items:
            man_cmd_vars[item["key"]].set(item["cmd"])
        if any(i["key"] == "device-tenant" for i in items):
            if not man_rows["device-tenant"].winfo_ismapped():
                man_rows["device-tenant"].pack(fill="x", pady=(4, 0), after=man_rows["device"])
        else:
            man_rows["device-tenant"].pack_forget()
            if man_form.get() == "device-tenant":
                man_form.set("device")

    def man_cli_check():
        """The 'Azure CLI installed?' line: PATH lookup now, `az --version` (first line) in the background; install hint as TEXT when az is missing."""
        text, found = az_cli_line()
        man_cli_var.set(text)
        man_cli_lbl.configure(fg=COLORS["ok"] if found else COLORS["err"])
        if found:
            man_inst.pack_forget()

            def work():
                try:
                    ver = az_version()
                except Exception:
                    ver = None
                msgs.put(("man", "cliinfo", ver))
            threading.Thread(target=work, daemon=True).start()
        else:
            man_inst.pack(fill="x", pady=(2, 0), after=man_cli_lbl)

    def man_show():
        if not man["shown"]:
            man["shown"] = True
            man_panel.pack(fill="x", pady=(6, 0), after=auth_msg_lbl)

    def man_chip_set(text, kind):
        man_chip.configure(text=text, bg=COLORS.get(kind, COLORS["dim"]))

    def man_open(reason=None, force=False, different=False, quiet=False):
        """Show the manual sign-in panel and start waiting (checks every MANUAL_POLL_SECONDS whether the sign-in happened). Idempotent while waiting."""
        if not is_cli():
            return
        if state["auth"]["state"] == "ok" and not force and not reason and not man["active"]:
            say(f"Already signed in as {state['auth'].get('who') or '?'}. Use 'Sign in with a different account' to sign in with another account.", "ok")
            return
        man_show()
        man_refresh()
        man_cli_check()
        man["different"] = man["different"] or different
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
        man.update(active=True, t0=time.time(), probing=False, expect=acct_expired())
        man_stop_btn.state(["!disabled"])
        man_chip_set("Waiting for you", "info")
        man_status_var.set("Waiting for you to sign in...")
        man["after"] = root.after(int(MANUAL_POLL_SECONDS * 1000), man_tick)
        if not quiet:
            say("Run one of the numbered commands in step 2 in your own terminal (open the URL it prints, enter the code), then press 'I have signed in - Verify'. "
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
                    res = verify_signin(None)
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
        if man["shown"]:
            man["shown"] = False
            man_panel.pack_forget()

    def man_verify():
        if state["checking"] or state["signing"] or state["busy"] or state["listing"]:
            man_result_var.set("Please wait for the current action to finish, then press Verify again.")
            return
        man_result_var.set("Verifying (read-only): az account show, the expiry check and az account list ...")
        man_result_lbl.configure(fg=COLORS["info"])
        man_chip_set("Verifying", "info")
        check_status(source="manual_verify")

    def man_signed_in(res):
        man_stop("")
        n_ = res.get("n_subs")
        man_chip_set("Signed in", "ok")
        man_status_var.set("Signed in.")
        man_msg_var.set("")
        man_result_var.set(f"Signed in as {res.get('who') or '?'}" + (f" - {count_of(n_, 'subscription')}" if n_ is not None else "")
                           + (f"   (tenant {res['tenant']})" if res.get("tenant") else ""))
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
        """'Open a terminal for me': a visible PowerShell window with the chosen az login form (user-initiated, local-only; nothing else is ever started this way)."""
        form = man_form.get()
        ok, info = open_terminal_signin(form, man_tenant() if form == "device-tenant" else (man_tenant() or man_account_tenant()))
        if ok:
            if not man["active"]:
                man_open(force=True, quiet=True)
            man_result_var.set(f"A PowerShell window opened and runs: {info}   Complete the sign-in there, then press 'I have signed in - Verify'.")
            man_result_lbl.configure(fg=COLORS["info"])
        else:
            man_result_var.set("Could not open a terminal: " + info)
            man_result_lbl.configure(fg=COLORS["err"])

    def on_signin_method(_event=None):
        LOGIN_OPTS["signin"] = _SESSION["signin"] = SIGNIN_KEYS.get(signin_var.get(), "manual")
        if LOGIN_OPTS["signin"] == "manual":
            say("Sign-in method: you run the command yourself. The commands are shown in step 2.", "info")
            if is_cli() and state["auth"]["state"] != "ok":
                man_open()
        else:
            man_hide()
            say(f"Sign-in method: {SIGNIN_METHOD_LABELS[LOGIN_OPTS['signin']]}. Press 'Sign in' to start it.", "info")

    # ---- step 3 loading (subscriptions are read once and cached for the session; 'Reload subscriptions' refreshes)
    def load_accounts_async(force=False):
        if state["acct_loading"] or (state["accounts_loaded"] and not force):
            return
        state["acct_loading"] = True
        acct_status.set("Loading subscriptions ...")
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
        state["acct_all"] = accts
        state["accts"] = build_accounts(accts)
        register_account_owners(accts)
        if state["sel_acct"] not in {a_["key"] for a_ in state["accts"]}:
            state["sel_acct"] = None
        if state["sel_acct"]:
            accts = [a_ for a_ in accts if a_.get("user") == state["sel_acct"]]
        if is_cli() and state["accts"]:
            check_accounts_async(list(state["accts"]))
        sess_ = state["signin"]
        if sess_ and sess_.get("finalize"):
            who_ = sess_.pop("finalize")
            n_ = len([a_ for a_ in state["acct_all"] if a_.get("user") == who_]) or len(state["acct_all"])
            sp_detail.set(f"Signed in as {who_} - {count_of(n_, 'subscription')}")
        state["accounts"] = accts
        state["acct_by_id"] = {a["id"]: a for a in accts}
        state["acct_chosen"] &= set(state["acct_by_id"])
        if not state["acct_chosen"] and _SESSION.get("subs"):
            state["acct_chosen"] = set(_SESSION["subs"]) & set(state["acct_by_id"])          # the last selection of this session
        if ACCOUNT0 and not state["pre"]:
            state["pre"] = True
            if ACCOUNT0 in state["acct_by_id"]:
                state["acct_chosen"] = {ACCOUNT0}
                scope_var.set("sel")
        if scope_var.get() == "sel" and not state["acct_chosen"]:
            scope_var.set("sel")
        update_scope_labels()
        update_controls()
        rebuild_account_list()
        rebuild_cluster_list()
        render_accounts()
        state["acct_err"] = err
        if not accts:
            acct_status.set("No subscriptions found. Your Azure sign-in can see no subscription (or the list could not be read). Fix: press 'Sign in' and use an account that has a subscription, or ask for the Reader role on one, then press 'Reload subscriptions'." + (f" (details: {err})" if err else ""))
            if state["auth"]["state"] in ("ok", "unchecked") or not is_cli():
                say(acct_status.get(), "warn" if is_cli() else "info")
            return
        usable = len([a for a in accts if a.get("usable", True)])
        acct_status.set(f"{count_of(len(accts), 'subscription')} loaded" + (f" ({usable} usable)" if usable != len(accts) else "") + ".")
        if is_cli() and state["auth"]["state"] == "ok":
            say(f"{count_of(len(accts), 'subscription')} loaded. Step 3: select one or more subscriptions (or 'Select all (shown)'), then press 'Collect clusters from selected subscriptions' in step 4.", "ok")

    # ---- step 4 loading (clusters are listed per subscription in parallel; the list grows while it runs)
    def maybe_auto_list():
        """Clusters are NEVER listed automatically any more: only the 'Collect clusters from selected subscriptions' button does it."""
        cluster_hint()

    def load_clusters(_event=None, refresh=False):
        """The 'Collect clusters from selected subscriptions' button (and 'Refresh selected'): lists the clusters of the SELECTED subscriptions only,
        skipping the ones already collected (per-subscription cache) unless refresh=True. The custom login's default list is the akslogin menu (instant)."""
        if state["busy"]:
            status.set("Wait for the current run to finish (or press Stop) before collecting clusters.")
            return
        if state["listing"]:
            return
        sync_login_opts()
        if not is_cli() and src_var.get() != "all":                      # the akslogin menu: instant, no cloud call
            CLI_TARGETS.clear()
            state["listing"] = True
            cl_status.set("Loading the cluster list from akslogin ...")
            update_controls()
            threading.Thread(target=lambda: msgs.put(("clusters", list_clusters())), daemon=True).start()
            return
        if is_cli() and state["auth"]["state"] != "ok":
            say("Sign in first (step 2): run one of the commands shown there, then press 'I have signed in - Verify'.", "warn")
            return
        if not shutil.which("az"):
            say("The Azure CLI (az) is not installed - install it first (" + AZ_INSTALL_URL + ").", "warn")
            return
        accts = effective_accounts()
        if not accts:
            say("Select one or more subscriptions above, then press 'Collect clusters from the selected subscriptions'.", "warn")
            cluster_hint()
            return
        todo = accts if refresh else [a for a in accts if a["id"] not in state["listed"]]
        if not todo:
            say(f"The {count_of(len(accts), 'selected subscription')} {'is' if len(accts) == 1 else 'are'} already collected (cached). Press 'Refresh selected' to read again.", "info")
            return
        if len(todo) > BIG_SCOPE and not confirm_big_scope(len(todo)):
            say("Cluster collection cancelled.", "info")
            return
        ids = {a["id"] for a in todo}
        state["crows"] = [r for r in state["crows"] if r.get("account") not in ids]       # these are listed again
        state["listed"] -= ids
        index_rows()
        cancel = threading.Event()
        state.update(listing=True, list_cancel=cancel, list_done=0, list_total=len(todo), list_t0=time.time(), src_fallback=False, list_stopped=False)
        list_bar.configure(maximum=max(1, len(todo)), value=0)
        simple = [{"id": a["id"], "name": a["name"]} for a in todo]
        with_menu = not is_cli()
        say(f"Listing clusters in {count_of(len(todo), 'subscription')} ... the list below fills in as results arrive (Stop cancels).", "info")
        update_controls()

        def work():
            try:
                if with_menu:
                    menu = list_clusters()
                    msgs.put(("menu", dict(menu)))
                    res = login_status()
                    if res["state"] != "ok":
                        msgs.put(("srcfallback", res.get("detail") or "az is not signed in", dict(menu)))
                        return
                msgs.put(("lstart", len(simple)))
                found, failed = scan_clusters(simple, lambda l: msgs.put(("line", l)), lambda d, t: msgs.put(("lprog", d, t)), cancel,
                                              lambda batch: msgs.put(("cbatch", list(batch))))
                msgs.put(("cdone", found, failed, cancel.is_set(), [a["id"] for a in simple]))
            except Exception as exc:
                msgs.put(("cerr", str(exc), [a["id"] for a in simple]))
        threading.Thread(target=work, daemon=True).start()

    def confirm_big_scope(n):
        from tkinter import messagebox
        return bool(messagebox.askyesno(PRODUCT_NAME, f"This will search {n} subscriptions and can take several minutes. Continue?", parent=root))

    def refresh_selected():
        load_clusters(refresh=True)

    def show_menu_clusters(menu, note=None):
        """The list is the akslogin menu only (numbers = the menu numbers)."""
        CLI_TARGETS.clear()
        state["listing"] = False
        state["clusters"] = dict(menu)
        set_rows([prepare({"key": k, "number": k, "label": v, "name": v, "where": "", "account": None, "account_name": ""})
                  for k, v in sorted(menu.items(), key=lambda kv: numeric(kv[0]))])
        update_controls()
        if menu:
            cl_status.set(f"{len(menu)} cluster(s) from akslogin." if not note else f"{len(menu)} cluster(s) from the akslogin menu only.")
            say(note or f"{len(menu)} cluster(s) found. Select one or more in step 4, then press 'Login & Debug'.", "warn" if note else "ok")
            status.set(f"{len(menu)} cluster(s) found. Select one or more, then press Login & Debug.")
        else:
            cl_status.set("No clusters.")
            say((note + " " if note else "") + "Could not read the cluster list from akslogin - type the cluster number(s) in the box in step 4, or check akslogin.exe / clusters.json.", "warn")
            status.set("Could not read the cluster list from akslogin - type the cluster number(s) in the box.")

    def on_source_change(_event=None):
        if state["busy"] or state["listing"]:
            src_var.set("menu" if src_var.get() == "all" else "all")
            status.set("Wait for the current run / listing to finish (or press Stop) before changing the cluster list.")
            return
        state["src_user"] = True
        state.update(crows=[], by_key={}, cchosen=set(), clusters={}, listed=set(), menu={}, src_fallback=False)
        manual_var.set("")
        rebuild_cluster_list()
        if src_var.get() == "menu":
            load_clusters()                                  # the akslogin menu is instant; az needs the button
        else:
            cluster_hint()
            update_controls()

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
        if not is_cli():                       # custom login: the clusters that are in the akslogin menu keep using it (the others use az)
            for r in rows:
                r.pop("exe_number", None)
            rows = merge_menu(rows, state["menu"])
            state["src_fallback"] = False
        if not cancelled:
            state["listed"] |= scanned
        aidx = {a["id"]: i for i, a in enumerate(state["accounts"])}
        rows.sort(key=lambda r: aidx.get(r.get("account"), len(aidx)))             # stable: the listed order stays inside a subscription
        clusters = register_clusters(rows, len(state["listed"] | scanned) > 1)
        for i, r in enumerate(rows, start=1):
            r["number"], r["label"] = str(i), clusters[str(i)]
            prepare(r)
        state["clusters"] = clusters
        set_rows(rows)
        list_bar.configure(value=state["list_total"])
        n = len(rows)
        skipped = (" Failed: " + ", ".join((LAST_SCAN.get("failed") or [])[:6]) + " - see the log.") if failed else ""
        summary = scan_summary([r for r in rows if not r.get("exe_only")])
        state["list_stopped"] = bool(cancelled)
        if cancelled:
            cl_status.set(f"Listing stopped - {count_of(n, 'cluster')} so far.")
            say(f"Listing stopped - {count_of(n, 'cluster')} found so far. Press 'Reload clusters' to list again.", "warn")
        elif not n:
            cl_status.set("No clusters found.")
            say("No AKS clusters found in the selected subscription(s). Check that they contain AKS clusters and that you have Reader access, or click 'All subscriptions'." + skipped, "warn")
        else:
            cl_status.set(summary + ".")
            say(f"{summary}. Step 4: search / select the clusters, then press 'Login & Debug'.", "ok" if not failed else "warn")
        update_controls()

    # ---- run options, steps list
    def plan(options):
        rows = [("login", "Login (Cloud CLI: az)" if is_cli() else "Login (akslogin)"), ("context", "Select kubectl context"), ("profile", "Select Azure subscription")]
        rows += [(k, t) for k, t, _ in run_steps(options)] + [("report", "Write HTML report")]
        return rows

    def reset_steps():
        steps.delete(*steps.get_children())
        rows = plan({})
        for key, title in rows:
            steps.insert("", "end", iid=key, text=title, values=(ICON["pending"], ""))
        stripe(steps)
        state["total"], state["finished"] = len(rows), 0
        tasks_var.set("")
        progress_bar.configure(maximum=len(rows), value=0)

    def reset_run(selected):
        findings.delete(*findings.get_children())
        state["counts"] = Counter()
        for sev in counter_vars:
            counter_vars[sev].set(f"{icon(SEV_ICON[sev])} {sev} 0")
        run_tree.delete(*run_tree.get_children())
        state["reports"] = {}
        for number, label in selected:
            run_tree.insert("", "end", iid=number, text=f"{number} - {label}", values=("waiting", "", ""), tags=("notrun",))
        stripe(run_tree)

    def sync_login_opts():
        """Read the login method and the chosen subscription (and region) into the options a run (or a cluster listing) uses."""
        LOGIN_OPTS["method"] = "cli" if method_combo.get() == LOGIN_LABELS["cli"] else "exe"
        LOGIN_OPTS["device_code"] = device_var.get()
        LOGIN_OPTS["signin"] = _SESSION["signin"] = SIGNIN_KEYS.get(signin_var.get(), "manual")        # remembered for the session
        LOGIN_OPTS["tenant"] = tenant_var.get().strip() or None
        LOGIN_OPTS["all_clusters"] = False        # the window has its own 'Cluster list' choice (list_clusters() then means the akslogin menu)
        picked = state["acct_chosen"] if scope_var.get() == "sel" else set()
        if state["pre"] or not ACCOUNT0:      # keep the command-line value until the list has been read
            AZ_OPTS["subscription"] = next(iter(picked)) if len(picked) == 1 else None

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
        picked = selected_sections()
        if not [s for s in picked if s in selectable_ids()]:
            status.set("Nothing is selected under 'What to collect'.")
            nb.select(tab_collect)
            say("Nothing is selected under 'What to collect'. Tick at least one section (or press 'Select all'), then press 'Login & Debug'.", "warn")
            return
        try:
            workers = max(1, min(16, int(workers_var.get())))
        except ValueError:
            workers = PARALLEL_WORKERS
        _save_prefs(picked, workers)
        options = {"azure": az_var.get(), "logs": logs_var.get(), "all_logs": alllogs_var.get(), "log_namespaces": ns_var.get(),
                   "sections": picked, "workers": workers, "task_progress": lambda d, tot: msgs.put(("tasks", d, tot))}
        state.update(busy=True, cancel=threading.Event(), html=None, t0=time.time(), n=len(selected))
        update_controls()
        open_btn.state(["disabled"])
        text.delete("1.0", "end")
        reset_steps()
        reset_run(selected)
        status.set(f"Starting {len(selected)} cluster(s) ...")
        nb.select(tab_run)                     # tab 3 (live steps, findings, log) opens when a run starts

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
                msg = msgs.get_nowait()
                kind = msg[0]
                if kind == "line":
                    write(msg[1])
                elif kind == "say":
                    say(msg[1], msg[2])
                elif kind == "auth":
                    apply_auth(msg[1], msg[2])
                elif kind == "signin_begin":
                    on_signin_begin(msg[1])
                elif kind == "signin_ev":
                    on_signin_event(msg[1], msg[2])
                elif kind == "man":
                    if msg[1] == "cliinfo" and msg[2]:
                        man_cli_var.set("Azure CLI installed: " + msg[2])
                elif kind == "signin_done":
                    on_signin_done(msg[1], msg[2], msg[3])
                elif kind == "acctstat":
                    refresh_expiry_banner()
                elif kind == "acctdone":
                    state["checking_accts"] = False
                    update_controls()
                    refresh_expiry_banner()
                elif kind == "expired":                  # an az call failed because the credentials expired (mid-use)
                    refresh_expiry_banner()
                    if not state["signing"]:
                        who_ = acct_expired() or msg[1]
                        say(f"Credentials for {who_} expired. Press Sign in to renew.", "err")
                        if is_cli():
                            set_badge("Credentials expired", "err")
                            signin_btn.configure(text=f"{icon('key')} Sign in again", style="Alert.TButton")
                            auth_msg.set(f"Credentials for {who_} expired. Press Sign in to renew.")
                            update_controls()
                elif kind == "accounts":
                    on_accounts(msg[1], msg[2])
                elif kind == "menu":
                    state["menu"] = dict(msg[1])
                elif kind == "lstart":
                    state["list_total"] = msg[1]
                    list_bar.configure(maximum=max(1, msg[1]), value=0)
                    cl_status.set(f"Listing clusters: 0/{msg[1]} subscriptions ...")
                elif kind == "srcfallback":
                    state["src_fallback"] = True
                    src_var.set("menu")
                    show_menu_clusters(msg[2], f"Azure CLI (az) is not usable ({msg[1]}), so only the clusters from the akslogin menu are shown. "
                                               "Sign in with 'az login', choose the az option in step 1 and press 'Collect clusters from selected subscriptions'.")
                elif kind == "lprog":
                    state["list_done"], state["list_total"] = msg[1], msg[2]
                    list_bar.configure(maximum=max(1, msg[2]), value=msg[1])
                    cluster_hint()
                    status.set(f"Listing clusters: {msg[1]}/{msg[2]} subscriptions ...")
                elif kind == "cbatch":
                    for c in msg[1]:
                        if c["key"] not in state["by_key"]:
                            state["crows"].append(prepare(c))
                            state["by_key"][c["key"]] = c
                    rebuild_soon()
                elif kind == "cdone":
                    on_clusters_done(msg[1], msg[2], msg[3], msg[4])
                    status.set("Cluster listing done." if not msg[3] else "Cluster listing stopped.")
                    if acct_expired():
                        refresh_expiry_banner()                  # the listing failed because the credentials expired: say that, not a generic failure
                elif kind == "cerr":
                    state["listing"] = False
                    cl_status.set("Listing failed.")
                    say(f"Could not list the clusters: {msg[1]}", "err")
                    update_controls()
                    refresh_expiry_banner()
                elif kind == "step":
                    _, key, st, secs = msg
                    if steps.exists(key):
                        steps.item(key, values=(ICON.get(st, st), f"{secs:.1f}s" if secs else ""),
                                   tags=(st,) + (("odd",) if steps.index(key) % 2 else ()))
                    if st in ("done", "skipped", "failed"):
                        state["finished"] += 1
                        progress_bar.configure(value=state["finished"])
                    elif st == "running" and steps.exists(key):
                        steps.see(key)
                        status.set(f"{steps.item(key, 'text')} ...")
                elif kind == "cluster":
                    _, i, n, label, st, entry = msg
                    number = entry["number"]
                    shown = {"ok": "OK", "failed": "FAILED", "running": "running ...", "credentials expired": "CREDENTIALS EXPIRED"}.get(st, st)
                    c = entry["counts"] if entry.get("counts") else Counter()
                    if run_tree.exists(number):
                        run_tree.item(number, values=(shown, c["CRIT"] if st != "running" else "", c["HIGH"] if st != "running" else ""),
                                      tags=(RUN_TAG.get(st, "notrun"),))
                    if st == "running":
                        reset_steps()                       # fresh checklist for this cluster
                        status.set(f"Cluster {i} of {n}: {label} ...")
                    elif entry.get("html"):
                        state["reports"][number] = entry["html"]
                elif kind == "tasks":
                    tasks_var.set(f"{msg[1]} of {msg[2]} collection tasks done")
                elif kind == "finding":
                    _, sev, t = msg
                    state["counts"][sev] += 1
                    counter_vars[sev].set(f"{icon(SEV_ICON[sev])} {sev} {state['counts'][sev]}")
                    findings.insert("", "end", values=(f"{icon(SEV_ICON[sev])} {sev}", t), tags=(sev,) + (("odd",) if len(findings.get_children()) % 2 else ()))
                    findings.yview_moveto(1.0)
                elif kind == "clusters":          # the custom login's own cluster list (akslogin menu only)
                    show_menu_clusters(msg[1])
                elif kind == "done":
                    results = msg[1]
                    ok = [r for r in results["items"] if r["html"]]
                    state["html"] = results["index"] or (ok[0]["html"] if ok else None)
                    if not is_cli():
                        load_accounts_async(force=True)          # akslogin may have created / refreshed the subscriptions
                    progress_bar.configure(value=state["total"])
                    if state["html"]:
                        open_btn.state(["!disabled"])
                    bad = [r["label"] for r in results["items"] if r["status"] in ("failed", "credentials expired")]
                    finish(f"Done: {len(ok)} of {len(results['items'])} cluster(s) reported"
                           + (f" ({len(bad)} failed: {', '.join(bad)})" if bad else "")
                           + ". Click 'Open HTML report'." if state["html"] else "Finished, but no report could be written - see the log.")
                    if state["html"] and open_var.get():
                        open_report()
                    if not is_cli() and state["src_fallback"]:      # akslogin may have signed az in: try the full list again
                        load_clusters()
                elif kind == "error":
                    write(f"ERROR: {msg[1]}")
                    finish(f"ERROR: {msg[1]}")
        except queue.Empty:
            pass
        if state["listing"]:
            cluster_hint()                                  # progress line with the elapsed time
        if state["busy"] and state["t0"]:
            s = int(time.time() - state["t0"])
            elapsed.set(f"elapsed {s // 60}:{s % 60:02d}")
        root.after(150, poll)

    minutes_var.trace_add("write", lambda *_: update_banner())
    run_btn.configure(command=start)
    stop_btn.configure(command=stop)
    refresh_btn.configure(command=refresh_selected)
    collect_btn.configure(command=load_clusters)
    collect_stop_btn.configure(command=stop)
    open_btn.configure(command=open_report)
    folder_btn.configure(command=open_folder)
    select_all_btn.configure(command=select_all)
    clear_btn.configure(command=clear_selection)
    signin_btn.configure(command=sign_in)
    check_btn.configure(command=check_status)
    acct_all_btn.configure(command=acct_select_all)
    acct_clear_btn.configure(command=acct_clear)
    acct_reload_btn.configure(command=lambda: load_accounts_async(force=True))
    src_all_rb.configure(command=on_source_change)
    src_menu_rb.configure(command=on_source_change)
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
    tenant_var.trace_add("write", lambda *_: (sync_login_opts(), man_refresh() if man["shown"] else None))
    signin_combo.bind("<<ComboboxSelected>>", on_signin_method)
    for _k, _b in man_copy_btns.items():
        _b.configure(command=lambda k=_k: man_copy(k))
    man_term_btn.configure(command=man_terminal)
    man_verify_btn.configure(command=man_verify)
    man_stop_btn.configure(command=lambda: man_stop())
    acct_combo.bind("<<ComboboxSelected>>", on_acct_combo)
    acct_combo.bind("<KeyRelease>", on_acct_type)
    acct_combo.bind("<Return>", on_acct_combo)
    recheck_btn.configure(command=recheck_selected)
    check_all_btn.configure(command=check_all_accounts)
    switch_btn.configure(command=sign_in_different)
    sp_cancel_btn.configure(command=cancel_signin)
    sp_open_btn.configure(command=open_signin_url)
    sp_url.bind("<Button-1>", open_signin_url)
    sp_copy_url_btn.configure(command=lambda: copy_text((state["signin"] or {}).get("url")))
    sp_copy_code_btn.configure(command=lambda: copy_text((state["signin"] or {}).get("code")))
    CRED["hook"] = lambda user, text: msgs.put(("expired", user, text))
    reset_steps()
    apply_method_ui()
    update_controls()
    _GUI.update(root=root, cluster_tree=cluster_tree, run_btn=run_btn, stop_btn=stop_btn, open_btn=open_btn, steps=steps,
                findings=findings, text=text, status=status, state=state, counter_vars=counter_vars,
                open_var=open_var, az_var=az_var, logs_var=logs_var, progress=progress_bar,
                alllogs_var=alllogs_var, ns_var=ns_var, load_profiles=lambda: load_accounts_async(force=True),
                select_all_btn=select_all_btn, clear_btn=clear_btn, filter_var=filter_var, manual_var=manual_var,
                run_tree=run_tree, sel_text=sel_text, chosen_clusters=chosen_clusters,
                method_combo=method_combo, device_var=device_var, on_method_change=on_method_change,
                acct_tree=acct_tree, acct_filter=acct_filter, acct_count=acct_count, cl_count=cl_count, scope_var=scope_var,
                acct_all_btn=acct_all_btn, acct_clear_btn=acct_clear_btn, acct_reload_btn=acct_reload_btn, refresh_btn=refresh_btn,
                signin_btn=signin_btn, check_btn=check_btn, switch_btn=switch_btn, tenant_var=tenant_var, tenant_entry=tenant_entry,
                acct_combo=acct_combo, acct_chip=acct_chip, recheck_btn=recheck_btn, check_all_btn=check_all_btn, chips_row=chips_row,
                who_var=who_var, sp=sp, sp_url=sp_url, sp_code=sp_code, sp_open_btn=sp_open_btn, sp_copy_url_btn=sp_copy_url_btn,
                sp_copy_code_btn=sp_copy_code_btn, sp_cancel_btn=sp_cancel_btn, sp_chip=sp_chip, sp_countdown=sp_countdown,
                sp_account=sp_account, sp_detail=sp_detail, sp_raw=sp_raw, device_chk=device_chk, choose_account=choose_account,
                acct_expired=acct_expired, device_text=lambda: device_chk.cget("text"), auth_badge=auth_badge, auth_msg=auth_msg, guide_msg=guide_msg,
                acct_status=acct_status, cl_status=cl_status, method_info=method_info, search_icon=SEARCH_ICON,
                s3=s3, s4=s4, list_bar=list_bar, acct_search=acct_search, cl_search=cl_search, scope_all_rb=scope_all_rb,
                scope_sel_rb=scope_sel_rb, src_var=src_var, src_all_rb=src_all_rb, src_menu_rb=src_menu_rb, on_source_change=on_source_change,
                check_status=check_status, sign_in=sign_in, load_clusters=load_clusters, refresh_selected=refresh_selected,
                collect_btn=collect_btn, collect_stop_btn=collect_stop_btn, acct_sel_count=acct_sel_count, confirm_big_scope=confirm_big_scope,
                banner=banner, draw_banner=draw_banner, banner_sub=banner_sub, page=page, page_canvas=page_canvas, footer=footer, style=style,
                sec_vars=sec_vars, sec_checks=sec_checks, preset_btns=preset_btns, sec_count=sec_count, sec_note=sec_note, sections_card=sections_card, notebook=nb, s1=s1, s2=s2, tab_clusters=tab_clusters, tab_collect=tab_collect, tab_run=tab_run, ro_note=ro_note,
                top=top, run_bar=run_bar, sec_chip=sec_chip, body=body, steps_box=steps_box, run_box=run_box, find_box=find_box, logopt=logopt,
                selected_sections=selected_sections, set_sections=set_sections, tasks_var=tasks_var, workers_var=workers_var, workers_spin=workers_spin,
                ICON=ICON, auth_icon=auth_icon, az_chk=az_chk, minutes_var=minutes_var, stripe=stripe,
                signin_var=signin_var, signin_combo=signin_combo, man=man, man_panel=man_panel, man_instr_var=man_instr_var, man_cli_var=man_cli_var,
                man_cli_lbl=man_cli_lbl, man_inst=man_inst, man_msg_var=man_msg_var, man_cmd_vars=man_cmd_vars, man_copy_btns=man_copy_btns,
                man_entries=man_entries, man_rows=man_rows, man_chip=man_chip, man_status_var=man_status_var, man_stop_btn=man_stop_btn,
                man_verify_btn=man_verify_btn, man_term_btn=man_term_btn, man_result_var=man_result_var, man_form=man_form, man_open=man_open,
                man_tick=man_tick, man_verify=man_verify, man_stop=man_stop, man_terminal=man_terminal, man_copy=man_copy, auth_msg_lbl=auth_msg_lbl,
                s2m=s2m, on_signin_method=on_signin_method, man_cli_check=man_cli_check, man_result_lbl=man_result_lbl, man_msg_lbl=man_msg_lbl)
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

def section_choice(sections=None, skip_sections=None, only_networking=False, no_networking=False):
    """The command-line section options -> list of ids to collect (None = all). Raises ValueError for a bad combination or unknown id."""
    if sum(bool(x) for x in (sections, only_networking, no_networking)) > 1:
        raise ValueError("use only one of --sections, --only-networking and --no-networking")
    chosen = None
    if only_networking:
        chosen = list(SECTION_PRESETS["only_networking"])
    elif no_networking:
        chosen = list(SECTION_PRESETS["no_networking"])
    elif sections:
        chosen = parse_section_list(sections)
    if skip_sections:
        skip = set(parse_section_list(skip_sections))
        chosen = [s for s in (chosen if chosen is not None else SECTIONS_ORDER) if s not in skip]
    if chosen is not None and not [s for s in chosen if not SECTION_BY_ID[s].get("locked")]:
        raise ValueError("no section left to collect (the cluster overview alone is not a report) - see --list-sections")
    return chosen


def _cli_progress(key, status, secs):
    if status != "running":
        print(f"  [{key}] {status}" + (f" ({secs:.1f}s)" if secs else ""), flush=True)


def main():
    global LOOKBACK_MINUTES, AKSLOGIN_EXE, LOG_TAIL_LINES, SUPPORT_LABEL, TRAFFIC_SAMPLE_SECONDS, PARALLEL_WORKERS, INITIAL_SECTIONS
    parser = argparse.ArgumentParser(description="AKS debugger: log in, then show what happened in the last N minutes")
    parser.add_argument("--minutes", type=int, default=None, help=f"time window in minutes (default {LOOKBACK_MINUTES})")
    parser.add_argument("--cluster", help="cluster number(s) or name to log in to and debug (no GUI): 3 | 1,3,5 | 2-4 | all | my-cluster. "
                                          "Several clusters run one after another and get a combined summary page")
    parser.add_argument("--name", help="label for the report when ONE cluster is given with --cluster (default: from the cluster list)")
    parser.add_argument("--list", action="store_true", help="print the clusters akslogin offers (with --all-clusters / --login-method cli: every accessible cluster with resource group, subscription and location) and exit")
    parser.add_argument("--skip-login", action="store_true", help="don't run akslogin; use the current kubectl context")
    parser.add_argument("--akslogin", help="path to akslogin.exe")
    parser.add_argument("--no-gui", action="store_true", help="never open the GUI")
    parser.add_argument("--all-clusters", action="store_true",
                        help="list / choose among the clusters az can reach in the chosen scope (--subscription a,b,c | all; default: the current subscription only) - works with --list and --cluster N|name|all. "
                             "With the akslogin method the clusters that are in its menu still log in with akslogin; the others use az aks get-credentials")
    parser.add_argument("--login-method", choices=["exe", "cli"], default="exe",
                        help="how to log in: exe = the custom akslogin.exe (default); cli = the standard Azure CLI (az) - then --list / --cluster use the cluster list read from az")
    parser.add_argument("--device-code", action=argparse.BooleanOptionalAction, default=True,
                        help="sign in with a device code (az login --use-device-code): DEFAULT. The URL and code are printed in a box (and shown in the window). "
                             "--no-device-code = the browser flow (az login)")
    parser.add_argument("--signin-method", choices=["manual", "captured", "console"], default=default_signin_method(),
                        help="how a sign-in is done: manual (default) = the exact commands (az login --use-device-code ...) are printed, you run one in your own terminal and "
                             "press Enter, then the tool verifies read-only; captured = the tool runs az login and shows the URL / code; console = az login in its own console window. "
                             "--device-code / --no-device-code apply to captured and console")
    parser.add_argument("--tenant", help="Azure tenant id / domain for the sign-in (az login --tenant) and to restrict the subscription list to that tenant")
    parser.add_argument("--list-accounts", action="store_true",
                        help="list the accounts the Azure CLI knows (user / service principal / managed identity, tenant, subscriptions) with the state of their credentials "
                             "(Active, Expiring soon, Expired, Not signed in); an expired one is reported and the device-code sign-in is started")
    parser.add_argument("--sign-in-only", action="store_true",
                        help="only sign in to Azure (see --signin-method; the manual commands are printed by default) and show the signed-in account, then exit")
    parser.add_argument("--az-cluster", help="AKS cluster name for the Azure checks (default: found from the nodes' resource group)")
    parser.add_argument("--resource-group", help="resource group of the AKS cluster (use with --az-cluster)")
    parser.add_argument("--subscription", help="Azure subscription(s) to search for clusters: one id / name, a comma list (a,b,c) or 'all'. Default: only the current (default) "
                                               "subscription - clusters are never searched in every subscription unless you ask (all)")
    parser.add_argument("--context", help="kubectl context to use (default: matched from the selected cluster)")
    parser.add_argument("--list-subscriptions", action="store_true", help="list the Azure subscriptions `az` can see and exit")
    parser.add_argument("--open", action="store_true", help="open the HTML report in your browser when done")
    parser.add_argument("--no-logs", action="store_true", help="skip pulling pod logs")
    parser.add_argument("--logs-all", action="store_true", help="also read logs of ALL running pods (capped), not just unhealthy / warning / core add-on pods")
    parser.add_argument("--log-namespaces", default="", help="with --logs-all: only these namespaces (comma separated)")
    parser.add_argument("--log-lines", type=int, default=None, help=f"max log lines per container (default {LOG_TAIL_LINES})")
    parser.add_argument("--traffic-sample", type=int, default=None,
                        help=f"seconds to sample live pod/node traffic from the kubelet (default {TRAFFIC_SAMPLE_SECONDS}, 0 = skip)")
    parser.add_argument("--no-azure", action="store_true", help="skip the Azure CLI sections (kubectl data only)")
    parser.add_argument("--support-label", default=None, help=f"namespace label that names the team to contact (default {SUPPORT_LABEL})")
    parser.add_argument("--workers", type=int, default=None, metavar="N",
                        help=f"collection tasks that run in parallel after the login (default {PARALLEL_WORKERS}; 1 = one after another). kubectl calls are capped at {KUBECTL_CONCURRENCY} and az calls at {AZ_CONCURRENCY} at a time")
    parser.add_argument("--sections", metavar="a,b,c", help="collect ONLY these report sections (ids from --list-sections; the cluster overview is always included)")
    parser.add_argument("--skip-sections", metavar="x,y", help="collect everything except these sections")
    parser.add_argument("--only-networking", action="store_true", help="collect only the network and traffic section (plus the cluster overview)")
    parser.add_argument("--no-networking", action="store_true", help="collect everything except the network and traffic section")
    parser.add_argument("--list-sections", action="store_true", help="print the section ids and titles and exit")
    parser.add_argument("--pim-list", action="store_true",
                        help="print your Privileged Identity Management roles: ACTIVE now and ELIGIBLE (Azure resource roles, Microsoft Entra roles, privileged access groups); read-only; needs `az login`")
    parser.add_argument("--pim-activate-all", action="store_true",
                        help="list them, then SELF-ACTIVATE every eligible role / group that is not active (needs --justification and --yes; without --yes it only prints what it would do). "
                             "The only thing in this tool that is not a read - see the README 'Privileged roles (PIM) tab'")
    parser.add_argument("--justification", metavar="TEXT", help="with --pim-activate-all: the justification sent with every activation request (required)")
    parser.add_argument("--hours", type=float, default=None, metavar="N", help="with --pim-activate-all: hours to request (default and upper limit: each role's policy maximum)")
    parser.add_argument("--ticket-number", help="with --pim-activate-all: optional ticket number")
    parser.add_argument("--ticket-system", help="with --pim-activate-all: optional ticket system")
    parser.add_argument("--yes", action="store_true", help="with --pim-activate-all: really submit the requests (without it nothing is submitted)")
    args = parser.parse_args()

    if args.list_sections:
        print("Report sections (use the ids with --sections / --skip-sections):")
        for s in SECTIONS:
            print(f"  {s['id']:<12} {s['title']}" + ("   [always collected]" if s.get("locked") else "") + (f"   (needs the data of: {', '.join(s['needs'])})" if s["needs"] else ""))
        print("Presets: --only-networking, --no-networking.  Older switches: --no-azure (azure), --no-logs (logs).")
        return
    try:
        sections = section_choice(args.sections, args.skip_sections, args.only_networking, args.no_networking)
    except ValueError as exc:
        parser.error(str(exc))
    INITIAL_SECTIONS = sections
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
    SCAN_SCOPE["value"] = parse_subscription_scope(args.subscription)
    multi_scope = SCAN_SCOPE["value"] == "all" or (isinstance(SCAN_SCOPE["value"], list) and len(SCAN_SCOPE["value"]) > 1)
    AZ_OPTS.update(cluster=args.az_cluster, resource_group=args.resource_group, subscription=None if multi_scope else args.subscription, enabled=not args.no_azure)
    if args.akslogin:
        AKSLOGIN_EXE = args.akslogin
    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass
    if args.tenant and not TENANT_RE.match(args.tenant):
        parser.error("--tenant must be a tenant id (GUID) or a domain name")
    LOGIN_OPTS.update(method=args.login_method, device_code=args.device_code, all_clusters=args.all_clusters, tenant=args.tenant, signin=args.signin_method)

    if args.pim_list or args.pim_activate_all:
        if args.pim_activate_all and not (args.justification or "").strip():
            parser.error("--pim-activate-all needs --justification TEXT")
        if args.hours is not None and not (0 < args.hours <= 24 * 30):
            parser.error("--hours must be a number of hours greater than 0")
        sys.exit(pim_cli(args))

    if args.list_accounts:
        sys.exit(list_accounts_cli(lambda l: print(l, flush=True)))

    if args.sign_in_only:
        say_ = lambda l: print(l, flush=True)
        res = cli_sign_in_any(say_)
        if res["status"] != "ok":
            print("Sign-in " + {"cancelled": "was cancelled", "expired": "timed out"}.get(res["status"], "failed") + ": " + (res.get("error") or "no details"), file=sys.stderr)
            sys.exit(1)
        st = login_status()
        n = len(list_az_subscriptions()) if st["state"] == "ok" else 0
        print(f"Signed in as {st.get('who') or '?'}" + (f" (tenant {st['tenant']})" if st.get("tenant") else "") + f" - {n} subscription{'s' if n != 1 else ''}")
        return

    if args.list_subscriptions:
        found = list_az_subscriptions()
        print("Azure subscriptions (az account list):")
        for sid, info in sorted(found.items(), key=lambda kv: kv[1].get("name") or ""):
            print("  " + describe_subscription(sid, info))
        if not found:
            print("  (none - run `az login`)")
        return

    if args.list:
        clusters = list_selected_clusters()
        if not clusters:
            print("No clusters found (see the messages above)." if LOGIN_OPTS["method"] == "cli" else
                  "No clusters found (could not parse the akslogin menu). Create clusters.json: {\"1\": \"name\", ...}")
        for k, v in sorted(clusters.items(), key=lambda kv: _numkey(kv[0])):
            print(f"{k} - {v}" + (f"   [{describe_cluster(k)}]" if k in CLI_TARGETS else ""))
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
            progress=_cli_progress, options={"azure": not args.no_azure, "logs": not args.no_logs,
                                             "all_logs": args.logs_all, "log_namespaces": args.log_namespaces,
                                             "sections": sections, "workers": PARALLEL_WORKERS})
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

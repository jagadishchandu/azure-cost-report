# AKS Debugger

`aks_debug.py` is the **AKS version** of the EKS debugger (`..\eks-debug`, which is unchanged). It logs in with your **`akslogin`**, then collects the debugging picture of **what happened and what is happening in the last N minutes** and saves it as an **interactive HTML report** and a text report. Everything is **read-only**.

All the features of the EKS version are here: live step-by-step collection with Stop, multi-cluster runs with a summary page, the resource utilization dashboard by namespace, namespaces (pods used vs configured, quotas), pod logs (unhealthy / warning-event / core add-on / all pods), node name + the actual VM, the support-DL label (`elvh-app-support-dl`) and **Teams to contact**, the network section, and the **traffic over the selected window**. The Azure parts replace the AWS parts.

## Quick start

```powershell
cd C:\Users\jchandraprasad\Downloads\app\aks-debug
python aks_debug.py                         # window: select one OR SEVERAL clusters
python aks_debug.py --minutes 60
python aks_debug.py --cluster 3             # no window: log in to cluster #3 and collect
python aks_debug.py --cluster 1,3,5         # several clusters, one after another (also 2-4, or all)
python aks_debug.py --list                  # clusters akslogin offers
python aks_debug.py --list --all-clusters   # EVERY cluster you can access (all subscriptions): name, resource group, subscription, location
python aks_debug.py --all-clusters --cluster prod-aks   # pick among all of them (also N, 1,3,5, 2-4, all)
python aks_debug.py --cluster 3 --skip-login
python aks_debug.py --cluster 3 --subscription <subscription-id>
python aks_debug.py --list-subscriptions
python aks_debug.py --cluster 3 --workers 4     # parallel collection (default 8; 1 = one after another)
python aks_debug.py --cluster 3 --az-cluster my-aks --resource-group my-rg   # point at the cluster by hand
```

Put `akslogin.exe` in this folder (or pass `--akslogin C:\path\akslogin.exe`). `kubectl` and the **Azure CLI (`az`)** must be on PATH, and `az login` must have been done for the Azure sections. No extra Python packages are needed.

The login is your `akslogin(cluster_number)` function in the same shape as `ekslogin`: it runs `akslogin.exe`, sends the cluster number on stdin and waits 2 seconds. **If your `akslogin` behaves differently** (different menu, extra arguments), send me the function and I'll match it. The cluster list comes from the `CLUSTERS` dict at the top of the script, a `clusters.json` next to it (`{"1": "name", ...}`), or the menu `akslogin.exe` prints (parsed as `1. name`, `1) name`, `[1] name`, `1 - name`). If it can't be parsed, type the cluster numbers into the box.

## Login methods (custom login or the Azure CLI)

There are two ways to log in. The default is unchanged.

| | `--login-method exe` (default) | `--login-method cli` |
|---|---|---|
| Login | your `akslogin.exe` (the cluster number is sent on stdin) | the standard **Azure CLI** (`az`) |
| Cluster list | `CLUSTERS` / `clusters.json` / the `akslogin.exe` menu; **every accessible cluster with `--all-clusters`** (window: the *Cluster list* choice) | read from `az` (below) |
| Window | login method **Custom login (akslogin)** | login method **Cloud CLI (az)** |

### All the clusters you can access (both methods)

After you sign in (and on **Reload clusters**) the list can show **every cluster the signed-in user can access**, read with the Azure CLI across **all subscriptions you can see** - no cap, de-duplicated:

- one Azure Resource Graph query per 150 subscriptions (`az graph query`) when the `resource-graph` extension is installed (never installed for you), otherwise `az aks list` per subscription, 8 in parallel; a subscription that fails (no Reader role, timeout) is **logged and skipped**;
- a clear message with counts, for example `Found 57 clusters in 12 subscriptions (15 searched), 3 subscriptions failed: sub-a, sub-b, sub-c`.

| | custom login (akslogin) | Azure CLI |
|---|---|---|
| Window | step 1 has **Cluster list**: **All clusters I can access (via az)** (default when `az` is installed and signed in) or **Only the clusters from akslogin menu**. If `az` is not installed / not signed in the list falls back to the akslogin menu and says why; after the run it tries again (akslogin may have signed `az` in) | always every cluster of the subscriptions chosen in step 3 (default: all) |
| Command line | default = akslogin menu only; **`--all-clusters`** = every accessible cluster (the sign-in check may run `az login`); works with `--list` and `--cluster N|name|all`; `--list` prints name, resource group, subscription and location | `--all-clusters` ignores `--subscription` for the listing |
| Running a cluster | a cluster that **is in the akslogin menu** (matched by name; a name that exists in two subscriptions is ambiguous and is not mapped) logs in with **akslogin and its menu number**, as before. A cluster **not in the menu** falls back to **`az aks get-credentials`** (exact resource group and subscription). Menu entries `az` cannot see are kept in the list and run with akslogin | `az aks get-credentials` |


```
python aks_debug.py --login-method cli --list                       # list the clusters az can see (numbered 1..N)
python aks_debug.py --login-method cli --subscription <id> --cluster 1
python aks_debug.py --login-method cli --subscription <id> --cluster all
python aks_debug.py --login-method cli --cluster prod-aks     # no --subscription: every enabled subscription is searched
python aks_debug.py --login-method cli --subscription <id> --cluster 1 --device-code
```

**What the `cli` method does**

1. **Signed in?** `az account show` must work. If it does not, the script runs `az login` (add `--device-code` for `az login --use-device-code`). The sign-in is interactive (browser or device code): from the command line it runs in your console; from the window (which has no console) it opens its own console window on Windows, waits for it to close, then checks again. Nothing about the sign-in is captured, and no tokens are ever printed.
2. **Cluster list.** `az aks list --subscription S` for the subscription you chose (`--subscription` / step 3 of the window), or for every enabled subscription when none is chosen (no limit; the window handles 300+ subscriptions - see below). The clusters are numbered 1..N in the order listed, so `--cluster 1,3`, `2-4`, `all` and a plain **cluster name** (`--cluster my-cluster`) all work. `--list` prints the same list.
3. **Connect.** For each selected cluster, in turn: `az aks get-credentials --resource-group RG --name NAME --subscription S --overwrite-existing`, then `kubectl config use-context`. For a cluster that uses Microsoft Entra ID sign-in, and when `kubelogin` is on PATH, it also runs `kubelogin convert-kubeconfig -l azurecli`; without kubelogin it prints a hint (`az aks install-cli`).
4. **Everything after that is the same as before**: the kubectl context is pinned, the cloud details step runs (the subscription, resource group and cluster name go straight into the Azure target (`--subscription`, `--resource-group`, `--az-cluster` for that run), so the cluster is not searched for again), and the report is written.

With several clusters each one gets its own sign-in check and credentials step. The Stop button and the live step messages work as usual; a cluster whose login fails or is cancelled is marked FAILED with the reason and the next one still runs. `--skip-login` skips the login (no `az` login or credentials step; the kubectl context is matched by the cluster name as before). Reading the cluster list and writing the kubeconfig entry are the only things the CLI method does besides the usual read-only checks. If `az` is not installed, the message says where to get it; if no cluster is found, it says so and what to check.

**In the window** the top is a guided, numbered layout with a status line and a message area (it always says what happened and what to do next):

1. **Login method** - *Custom login (akslogin)* or *Cloud CLI (az)*. With the custom login, step 2 says "Uses akslogin.exe - it signs in when you press Run"; below the method box, **Cluster list** lets you choose **All clusters I can access (via az)** or **Only the clusters from akslogin menu** (see above).
2. **Sign in** - a badge shows **Not checked / Checking... / Signed in as <identity> (green) / Not signed in (red)** with the exact reason and the next action. **Sign in** runs the interactive login (`az login`; the device-code / no-browser tick box is honoured) in a console window - complete it there, this window continues when it closes. **Check status** re-checks without changing anything. A missing CLI is reported with its install link; a cancelled or failed sign-in says so and what to do.
3. **Choose subscription** - a searchable list. Step 3 lists **all** your Azure subscriptions (name, id, state). Pick **All subscriptions (N)** (the default, an explicit labelled choice) or select one or several rows (Ctrl/Shift-click): selecting narrows the cluster list below at once; choosing "All" clears the selection. The list is read once per session; **Reload subscriptions** refreshes it.
4. **Choose clusters** - a searchable multi-select list, filled **while it is listed** with progress text such as "Listing clusters: 120/300 subscriptions ..." (**Stop** cancels and keeps what arrived). There is **no cap**: 300+ subscriptions are fine. Listing runs in a background thread (the window never freezes) using Azure Resource Graph (`az graph query`, one query per 150 subscriptions, when the `resource-graph` extension is installed - it is never installed for you) or, without it, `az aks list` per subscription; individual calls run 8 in parallel and the results are de-duplicated. **Reload clusters** lists again.

Steps unlock in order: until you are signed in (Cloud CLI method) step 4 shows "Sign in first".

**Search** (both lists): type in the box next to the magnifier (case-insensitive; several words must all match; name, id, location / group, subscription ... are searched); **x** clears it; "Showing X of Y" tells how many rows match; your selection is kept while you filter; **Select all (shown)** and **Clear** act on the rows in view. In the cluster list "or type numbers" still works (`1,3,5`, `2-4`, `all`).

A cluster of another subscription than the one picked in step 3 always uses **its own** subscription for the run.

**Prerequisites for the `cli` method:** `kubectl`, the Azure CLI (`az`), and `kubelogin` for Entra ID (AAD) clusters.

## What is different from EKS

| EKS version | AKS version |
|---|---|
| `ekslogin` | `akslogin` |
| AWS CLI + `~/.aws` profiles, picked after login | **Azure CLI + subscription**, picked after login: the subscription the nodes' VMs live in (read from the node `providerID`), checked by finding your cluster with `az aks list`. Step 3 of the window lets you choose another one. |
| EKS cluster, nodegroups, add-ons, Fargate | **AKS cluster state, node pools, add-ons** (`az aks show`) |
| VPC, subnets (free IPs), security groups, route tables, NAT, VPC endpoints | **VNet subnets with IP capacity**, **NSG rules**, route tables, outbound type and IPs, NAT gateway, public IPs, load balancers |
| IAM roles, `aws-auth` | **Managed identities and their role assignments**, Entra ID / Azure RBAC settings |
| EC2 instance ids / Name tag, EC2 status checks | **VM scale set + instance** (`aks-nodepool1-123-vmss/3`), VM name, zone, size, spot/regular, node pool, **VM power / provisioning state** |
| CloudWatch control-plane logs | **Diagnostic settings** and **Log Analytics** (errors and 401/403 denials in the window) |
| CloudWatch traffic (EC2, ALB/NLB, NAT) | **Azure Monitor** metrics: VM scale set network in/out per VM, load balancer bytes / packets / **SNAT ports** / **backend health**, NAT gateway |
| VPC CNI (`aws-node`) settings | **Azure CNI** details: plugin (classic / overlay / kubenet), policy, `azure-cns`, `azure-ip-masq-agent`, pod IPs per subnet or per pod CIDR |
| `--aws-cluster / --region / --profile / --no-aws / --list-profiles` | `--az-cluster / --resource-group / --subscription / --no-azure / --list-subscriptions` |

The Kubernetes sections (nodes, pods, events, workloads, namespaces, utilization, logs, DNS, services, ingress, network policies, kubelet pod counters, the Teams-to-contact list) are the same as in the EKS version.

## The Azure section (section 2)

- **Cluster:** provisioning and power state, Kubernetes version and available upgrades, tier, API server (private or public, authorized IP ranges), network profile, identity, Entra ID / Azure RBAC, upgrade channels.
- **Node pools:** mode, VM size, ready nodes vs count, autoscaler limits (a pool at its maximum is flagged), OS, zones, max pods, spot/regular, health.
- **Network:** how pods get IPs (classic Azure CNI uses a **subnet IP per pod and reserves max-pods IPs per node**), each node subnet's usable / used / free IPs and the **IPs needed if the pools scale to their maximum** (a subnet that can't hold it is a HIGH finding), NSG rules (SSH/RDP open to the internet is a HIGH finding), outbound type and the outbound public IPs.
- **Identities & role assignments** of the cluster and kubelet identities (a note if the kubelet identity has no AcrPull).
- **Add-ons** and whether Azure Monitor / Container Insights is on.
- **VM scale set instances:** power state, provisioning state and health of each node VM (a deallocated or failed VM is a finding).
- **Control-plane logging:** which diagnostic categories are on, and, when they go to Log Analytics, the **error-like entries in the window** and the **top 401/403 denials** from the audit log.

## Network & traffic (section 10)

Read-only: only Kubernetes objects, pod logs, Azure resources and Azure Monitor are read. Nothing runs inside pods, no node login (no SSH), no packet capture.

**Plain wording.** No short forms in headings or table column headers anywhere in the report (HTML and text): "Bytes received", "Network security group", "Virtual machine scale set", "Load balancer", "Outbound ports (source network address translation)", "Support team (distribution list)" ... Terms that cannot be avoided (Container Network Interface, Domain Name System, Maximum Transmission Unit, network address translation, kube-proxy, iptables / IPVS, conntrack, NetworkPolicy, ClusterIP / NodePort ...) are explained in a small **Glossary: what these terms mean** table (Term | Full name | Plain-language meaning) **immediately before** the block that uses them, plus one **complete glossary at the end of the section** (and the report-wide one at the very end of the report, see "Every section explains itself"). Every table and block heading has a line **"What this table shows"** / **"What this check looks at"**.

**Every check is its own titled block** with a status - **OK / Warning / Problem / Not available (with the reason)** - the evidence found, and **"What this means / what to do next"**. A check never says OK when its data could not be read.

| Group | Checks |
|---|---|
| **Pod-level networking** | cluster network settings (plugin, policy engine, dataplane, address ranges, kube-proxy mode); **Container Network Interface plugin health** (Azure CNI / CNI Overlay / kubenet, `azure-cns` and `azure-ip-masq-agent` ready / wanted, versions, key settings from the plugin ConfigMaps, Cilium details); **IP address exhaustion** in the node / pod subnets (free addresses per subnet or pod range, node pod slots); **plugin daemon logs** (failed IP allocation, subnet full, throttling); **pods stuck in ContainerCreating / FailedCreatePodSandBox**; **start-up order** (network agent restarts / not ready, kube-proxy) |
| **Node networking** | **node Ready and pressure conditions** (memory, disk, process count, network unavailable); **interface errors** from the kubelet statistics (dropped packets are not exposed there); **Maximum Transmission Unit hints** from plugin settings / labels (otherwise "cannot be read without node access"); **cloud throttling evidence** (HTTP 429 in plugin logs, events and the Azure Activity log) |
| **kube-proxy and Service routing** | kube-proxy health, mode (iptables / IPVS from its ConfigMap or the cluster settings) and log errors; **Services with no ready endpoints**; ClusterIP Services; **NodePort Services** (and whether a network security group Deny rule covers the node port); **LoadBalancer Services** with their backend health probe availability |
| **Domain Name System** | CoreDNS pods ready / restarts; Corefile summary (forwarders, cache, loop / ready plugins, `coredns-custom`); DNS error logs; NodeLocal DNS cache (recommended for 50+ nodes or with DNS errors); `ndots` / search settings read from the pod specs (the default `ndots:5` is an INFO) |
| **Network policies and cloud firewalls** | NetworkPolicies per namespace, default-deny present, the policy engine (Azure NPM / Calico / Cilium); network security groups, user-defined routes and next hops, Azure Firewall |
| **Load balancers and ingress** | ingress controller pods and recent **502 / 503 / 504** counts in their logs; ingress backends (Service exists, ready pods, address); Azure load balancer **backend health probe** availability; **TLS certificate** expiry of the secrets the Ingresses use (only `tls.crt` is read, decoded locally; never the private key) |
| **Connection tracking and outbound ports** | conntrack usage from node-exporter / `ama-metrics-node` metrics via the Kubernetes API proxy (otherwise "needs node-exporter or node access"); outbound port (SNAT) use and failed outbound connections from Azure Monitor, NAT gateway drops |
| **Network observability and packet capture** | Advanced Container Networking Services, Container Insights, Managed Prometheus, network security group / virtual network **flow logs** and Traffic Analytics; whether Retina / Hubble is available; guidance table of read-only tools and capture tools (text only - none is run) |
| **Control plane** | API server throttling (`apiserver_request_total` 429 / flow-control rejections from `/metrics`, if allowed); **admission webhooks** (failure policy, timeout, backend pods ready, recent failure events); etcd readiness if exposed |

Then **Traffic measured in the selected window** (network warning events, kubelet pod / node counters, Azure Monitor node traffic, load balancer and NAT gateway traffic with charts - unchanged data, new column names), the **Traffic issue checklist** (10 rows: pod reachability evidence, Container Network Interface logs, node health, kube-proxy, Domain Name System, network policies, cloud firewalls, load balancer health checks, packet capture availability, provider observability tools; columns **Check | Result | Evidence found | What to do next**), and the **complete glossary**. In the HTML the tables keep sorting, filtering and CSV export; results are coloured.

Limits: kube-proxy mode / Maximum Transmission Unit / conntrack can only be read when the ConfigMaps or exporters exist; AKS often blocks the API server `/metrics` endpoint (then the check says Not available); Azure Monitor probe and port metrics need a Standard load balancer and Monitoring Reader; pod-to-pod traffic needs flow logs or Advanced Container Networking Services.

## Faster collection: parallel after the login (`--workers N`)

After the login the slow, read-only reads run **at the same time**: `kubectl get` of every object type, `kubectl top`, the kubelet statistics of every node, the pod logs, and the `az` calls (cluster, node pools, subnets, security groups, route tables, identities, VM scale sets, Azure Monitor metrics, Log Analytics queries). `--workers N` (default **8**; the window has a *Parallel workers* box) sets how many run at once; `--workers 1` collects one step after another exactly as before.

- **The report does not change.** The sections are still written one after another in the same fixed order; only the waiting is shared. A section that needs something already fetched (or in flight) takes it from a thread-safe cache keyed by the exact command, so nothing is read twice.
- **Kind to the servers:** at most **6 kubectl** and **4 az** calls at the same moment, whatever `--workers` says.
- **Live sampling** (`--traffic-sample`) takes its second kubelet sample in the background at the right moment, so it no longer holds anything up.
- **Stop** cancels everything still queued; calls already running end by their own timeout. A failing call never stops the others; the section that needed it says what failed, as before.
- The window shows `x of y collection tasks done`; the end of every run prints a **timing summary** (total time, time per step, number of calls, the waiting time of all calls added together) and the same appears at the end of the `.txt` ("RUN TIMING") and in the HTML ("Run timing").
- Measured on the mocked harness with 0.15 s added to every kubectl / az call: **8.8 s with `--workers 1`, 2.3 s with `--workers 8` (3.8 times faster)**, identical report text.

## Choosing what to collect (sections)

Every report section is one entry of the `SECTIONS` registry at the top of the script (id, full title, icon, description, what it needs). The collection steps, the scheduler, the report, its table of contents and the window's check boxes are all driven by it.

| id | section | needs the data of |
|---|---|---|
| `overview` | Cluster overview (always collected: the report identity) | |
| `azure` | Azure cluster and infrastructure | |
| `nodes` | Nodes: processor, memory, disk and swap | |
| `utilization` | Resource utilization by namespace | |
| `nodepods` | Pods on each node | |
| `namespaces` | Namespaces: pods used versus configured | |
| `pods` | Unhealthy pods | |
| `events` | Warning events | |
| `workloads` | Workloads | |
| `network` | Network and traffic (with the checklist) | `azure` |
| `scaling` | Autoscaling and storage | |
| `top` | Top consumers | |
| `logs` | Pod logs | `pods` |
| `timeline` | Timeline | |

```powershell
python aks_debug.py --cluster 3 --only-networking          # overview + network and traffic only
python aks_debug.py --cluster 3 --no-networking            # everything except network and traffic
python aks_debug.py --cluster 3 --sections nodes,events    # only these (ids or aliases such as networking, ns)
python aks_debug.py --cluster 3 --skip-sections logs,top
python aks_debug.py --list-sections                        # ids and titles
```

`--no-azure` and `--no-logs` still work and simply untick the `azure` / `logs` section (`--no-azure` also keeps every `az` call away, as before).

- **Unticked sections are not collected at all:** their kubectl / az calls are never scheduled (only the Kubernetes objects the chosen sections read are fetched, and the live usage numbers only when a chosen section shows them).
- **Data a ticked section needs from an unticked one** (Network and traffic reads the Azure data, Pod logs reads the analysed pods) is collected quietly: nothing of it reaches the report, the findings, the timeline or the counters. The window tells you in a note under the check boxes.
- **In the report** a skipped section is one line, `Skipped by choice: <section>` (a collapsed entry in the HTML contents list); the health summary says `Sections collected: 3 of 14` and counts only what was collected. The network checklist exists only when Network and traffic was collected. The multi-cluster summary page has a *Sections collected* column.
- **In the window**, the *What to collect* panel has one check box per section (hover for the description), **Select all**, **Clear all**, **Only networking**, **Everything except networking**, and a counter such as *7 of 13 sections selected*. Starting with nothing selected is refused with a message; the panel is frozen while a run is active. The choice is remembered for the session and in `aks_debug_gui.json` next to the script.

## Every section explains itself (plain wording in the whole report)

The report is meant to be read by people who did not write it, so **everything in it is explained** - in the HTML and in the text report:

- **"What this section shows"** box directly under every section heading (Health summary, cluster overview, Azure, nodes, utilization dashboard, pods on each node, namespaces, unhealthy pods, events, workloads, network and traffic, autoscaling / storage, top consumers, pod logs, timeline, collection steps, read-only guarantee, run timing, glossary, and both sections of the multi-cluster summary page): 2-3 plain sentences with *what data it contains, where it comes from (kubectl / az / Azure Monitor), which time window it covers and how to read the colours*, plus a **"How to use it"** hint. The text comes from the `SECTIONS` registry (`shows` and `howto` fields), so it stays the same in the HTML, the text report and the GUI list, and a section you did not select shows no box (it is one `Skipped by choice` line).
- **"What this table shows"** directly above every table and **"What this block shows"** above every sub-heading, log, chart, dashboard and the timeline - specific to that table (for example *"One row per worker node: its status, how much processor and memory it uses, and which virtual machine in the scale set it runs on"*). In code: `rep.table(..., about="...")`, `rep.heading(title, about="...")`, `rep.log(..., about=...)`, `rep.series(..., about=...)`, `rep.util(data, about=...)`, `rep.timeline(..., about=...)`. A table or block written without an explanation is recorded in `rep.missing_about`, shown as a red "no description" line in the HTML, and **fails the explanation test** of the mock harness.
- **Column tooltips**: hover a column header (dotted underline) to read what the column means where the name alone is not obvious (`COLUMN_HELP`).
- **Legends**: what *Critical / High / Medium / Information* mean and what *OK / Warning / Problem / Not available* mean - once in the Health summary and once in the glossary.
- **No short forms in headings or column headers** (processor (CPU), memory, persistent volume claim, horizontal pod autoscaler, availability zone, network security group, virtual machine scale set, load balancer, support team (distribution list), IP address, container restarts ...). A term that cannot be avoided inside a cell or a name (kube-proxy, CoreDNS, Entra ID, OOMKilled, CrashLoopBackOff ...) is explained in a small **Glossary: what these terms mean** table (Term | Full name | Plain-language meaning) immediately **before** the table that uses it, and **one complete glossary at the very end of the report** (section 15: every term used anywhere, de-duplicated, sorted A to Z, with the legends) which is linked from the table of contents.

The explanation test (mock harness, not shipped in this folder) generates the full report, the `--only-networking` report and the multi-cluster page from the mock and asserts that every section has its box, tables == "What this table shows" lines, every block is explained, no abbreviation of a deny-list (CPU, MEM, PVC, HPA, NSG, VMSS, LB, DL, RX, TX, IP, ID, OOM, CNI, DNS, MTU, NAT, SNAT, VNet, UDR ...) is in a heading or column header unless its full form is next to it or a glossary row precedes the table, the glossary is complete and linked, and the text is identical with `--workers 1` and `--workers 8`.

## The window and the report: Azure look

The window has an Azure banner (a gradient in `#0078D4` / `#50E6FF`, a stylised cloud with an "A" mark drawn with Canvas shapes, soft pale clouds) with the title *Azure Kubernetes Service (AKS) Debugger* and a subtitle with the time window, the signed-in identity and the subscription count; icons on the step headers, buttons, step status (`✔ ✖ ▶ ○ ➖`), the severity counters and findings (`🔴 🟠 🟡 🔵`); striped lists, Azure-blue primary buttons / selected rows / progress bar; a footer status bar. It works at 1100 x 700 (the page scrolls) and resizes cleanly. Where the font cannot draw the symbols the icons fall back to plain text (force it with `AKS_DEBUG_ASCII_ICONS=1`). The HTML report has the same header band with the logo drawn in inline SVG, the Azure accent colour and an emoji in every section heading; all its interactive features are unchanged.

The branding is data: `CLOUD_NAME`, `PRODUCT_NAME`, `BRAND_PRIMARY`, `BRAND_ACCENT`, `BRAND_DARK`, `BRAND_PALE`, `LOGO_SHAPES()` (the drawing, used by both the window and the HTML) at the top of the script. The sibling tools only change those.

## Read-only guarantee

**This tool only reads. It does not install, create, change or delete anything on the cluster or in the cloud account.** It installs no tools, runs nothing inside pods or nodes, and downloads and executes nothing. This is enforced in the code, not just promised: every `kubectl` call (also `kjson`, the parallel cache and the live traffic sampler, which all go through the same function) is checked against an **allow-list** before a process is started, and so is every `az` call. A command that is not on the list is **refused without starting any process**, the attempt is recorded, and the report shows it in its **Read-only guarantee** block with a CRITICAL finding. A normal report says: *Commands used: N read calls, 0 blocked.* The same sentence is in the window (step 1 note and footer), the HTML header/footer and the `.txt` report.

**Allowed `kubectl` (read):** `get` (including `get --raw`, which is always an HTTP GET; refused with `-f`, `--data`, or a path ending in `exec`/`attach`/`portforward`), `logs`, `top`, `version`, `api-resources`, `api-versions`, `cluster-info`, `explain`, `auth can-i`, and `config get-contexts | current-context | view` (`view` without `--raw`, so credentials stay redacted). Flags that run, write or change identity are refused on every verb (`-f/--filename`, `--overwrite`, `--force`, `--token`, `--as`, `--server`, `-i`, `-t`, `--follow` ...). Everything else (`apply`, `create`, `run`, `exec`, `debug`, `delete`, `patch`, `replace`, `edit`, `label`, `annotate`, `scale`, `rollout`, `cordon`, `drain`, `taint`, `cp`, `attach`, `port-forward`, `set`, `expose`, `autoscale`, `certificate`, `helm` ...) is refused.

**Allowed `az` (read), table `READ_ONLY_CLOUD_COMMANDS` in the script:** `account list|show`, `extension list`, `graph query`, `aks list|show|get-upgrades`, `network vnet subnet show`, `network nsg show|list`, `network public-ip show|list`, `network route-table show`, `network lb list`, `network nat gateway list`, `network watcher flow-log list`, `network firewall list`, `role assignment list`, `vmss list|list-instances`, `monitor diagnostic-settings list`, `monitor metrics list`, `monitor activity-log list`, `monitor log-analytics workspace show`, `monitor log-analytics query`. Everything else (`create`, `delete`, `update`, `set`, `start`, `stop`, `restart`, `run-command`, `scale`, `upgrade`, `rest`, `extension add|update|remove`, `config set` ...) is refused, as are `--yes`, `--no-wait`, `--set`, `--add`, `--remove`, `--allow-preview`.

**Nothing is installed on this machine either.** Every `az` process is started with `AZURE_EXTENSION_USE_DYNAMIC_INSTALL=no` (an environment variable; `az config set` is never run), so `az graph` or `az network firewall` never downloads an extension; if the extension is missing the tool falls back to the per-subscription calls as before. No code path runs `az extension add`, `pip install`, `gcloud components install`, `aws configure` or `az aks install-cli`; the hints that mention an install command are printed as text only.

**The only things that are not reads (all local-only, run with fixed argument shapes, anything else is refused):**

| Command | What it changes |
|---|---|
| `az login [--use-device-code]` | your interactive sign-in: the local Azure CLI token cache (started by the Sign in button / on request) |
| `az aks get-credentials --resource-group --name [--subscription] --overwrite-existing` | **local-only**: the local kubeconfig file, never the cluster |
| `kubelogin convert-kubeconfig -l azurecli` | **local-only**: the local kubeconfig file, never the cluster |
| `kubectl config use-context <name>` | **local-only**: the current context in the local kubeconfig file |
| `akslogin.exe` | your organisation's own sign-in tool, only when you choose the custom login; sign-in only |

## Permissions (read-only)

Without one of these, that part says why it is missing and the rest still runs.

- **Azure RBAC:** `Reader` on the AKS cluster, its **node resource group** (`MC_...`) and the **VNet** resource group; `Monitoring Reader` for metrics and diagnostic settings; `Log Analytics Reader` on the workspace for the control-plane log queries; permission to list role assignments for the identities section.
- **Kubernetes:** the same as the EKS version (`nodes/proxy` for kubelet stats, list on pods / nodes / events / namespaces, etc.).

## Not verified against a real Azure subscription

I built and tested this against mocked `kubectl` and `az` output, not a real AKS cluster. The `az` JSON field names, the Azure Monitor metric names (`Network In Total`, `ByteCount`, `UsedSnatPorts`, `DipAvailability`, ...) and the Log Analytics table names (`AKSControlPlane`, `AKSAudit`, `AzureDiagnostics`) follow the documented Azure CLI and API, but **run it once on a real cluster and check each section**. A call that fails prints its error in the report instead of stopping the run.

## Reports

`reports\aks_debug_<cluster>_<time>.html` (interactive) and `.txt`; with several clusters also `reports\aks_debug_summary_<time>.html`. Pod logs and the report can contain sensitive data; share them carefully.

Settings at the top of the script: `LOOKBACK_MINUTES`, `MAX_LOG_PODS`, `LOG_TAIL_LINES`, `UTIL_WARN` / `UTIL_CRIT`, `SUPPORT_LABEL`, `TRAFFIC_SAMPLE_SECONDS`, `LOW_SUBNET_IPS`.

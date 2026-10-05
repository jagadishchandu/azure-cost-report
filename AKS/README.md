# AKS Debugger

`aks_debug.py` is the **AKS version** of the EKS debugger (`..\eks-debug`, which is unchanged). It logs in with your **`akslogin`**, then collects the debugging picture of **what happened and what is happening in the last N minutes** and saves it as an **interactive HTML report** and a text report. Everything is **read-only**, with one explicit, opt-in exception: the **Privileged roles (PIM)** tab (see *Read-only guarantee* and *Privileged roles (PIM) tab* below).

All the features of the EKS version are here: live step-by-step collection with Stop, multi-cluster runs with a summary page, the resource utilization dashboard by namespace, namespaces (pods used vs configured, quotas), pod logs (unhealthy / warning-event / core add-on / all pods), node name + the actual VM, the support-DL label (`elvh-app-support-dl`) and **Teams to contact**, the network section, and the **traffic over the selected window**. The Azure parts replace the AWS parts.

## Quick start

```powershell
cd C:\Users\jchandraprasad\Downloads\app\aks-debug
python aks_debug.py                         # window: select one OR SEVERAL clusters
python aks_debug.py --minutes 60
python aks_debug.py --cluster 3             # no window: log in to cluster #3 and collect
python aks_debug.py --cluster 1,3,5         # several clusters, one after another (also 2-4, or all)
python aks_debug.py --list                  # clusters akslogin offers
python aks_debug.py --login-method cli --list      # clusters of the CURRENT subscription only (add --subscription a,b,c or --subscription all)
python aks_debug.py --login-method cli --subscription all --list    # every subscription (can take minutes): name, resource group, subscription, location
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
| Cluster list | `CLUSTERS` / `clusters.json` / the `akslogin.exe` menu (instant); clusters of **selected subscriptions** with `az` only on demand (window: the *Cluster list* choice; command line: `--subscription`) | collected on demand from `az` (below) |
| Window | login method **Custom login (akslogin)** | login method **Cloud CLI (az)** |

### All the clusters you can access (both methods)

Clusters are **never searched automatically**. After you sign in the window lists your subscriptions (one cheap `az account list`); you **select** one or several subscriptions and press **Collect clusters from selected subscriptions**. Only those subscriptions are searched, with `az` (read-only, no cap, de-duplicated):

- one Azure Resource Graph query per 150 subscriptions (`az graph query`) when the `resource-graph` extension is installed (never installed for you), otherwise `az aks list` per subscription, 8 in parallel; a subscription that fails (no Reader role, timeout) is **logged and skipped**;
- a clear message with counts, for example `Found 57 clusters in 12 subscriptions (15 searched), 3 subscriptions failed: sub-a, sub-b, sub-c`.
- a **per-subscription cache** for the session: pressing the button again fetches only the subscriptions not collected yet; **Refresh selected** reads the selected ones again.
- more than 20 subscriptions to search: the window asks first (*This will search 300 subscriptions and can take several minutes. Continue?*).

| | custom login (akslogin) | Azure CLI |
|---|---|---|
| Window | step 1 has **Cluster list**: **Clusters from the akslogin menu (instant)** (default; shown at once, no cloud call) or **Collect clusters with az from selected subscriptions (on demand)** (select subscriptions in step 3, press the button in step 4). If `az` is not installed / not signed in the list falls back to the akslogin menu and says why | the button **Collect clusters from selected subscriptions** is the only way (nothing is listed after the sign-in) |
| Command line | default = akslogin menu only; **`--all-clusters`** = also read the clusters `az` can reach, **in the scope you give**: `--subscription a,b,c` (ids or names), `--subscription all`, or - with no scope - only the current (default) subscription (a line says which scope is searched); works with `--list` and `--cluster N|name|all`; `--list` prints name, resource group, subscription and location | same: `--subscription a,b,c` / `all` / default subscription only |
| Running a cluster | a cluster that **is in the akslogin menu** (matched by name; a name that exists in two subscriptions is ambiguous and is not mapped) logs in with **akslogin and its menu number**, as before. A cluster **not in the menu** falls back to **`az aks get-credentials`** (exact resource group and subscription). Menu entries `az` cannot see are kept in the list and run with akslogin | `az aks get-credentials` |


```
python aks_debug.py --login-method cli --list                       # list the clusters az can see (numbered 1..N)
python aks_debug.py --login-method cli --subscription <id> --cluster 1
python aks_debug.py --login-method cli --subscription <id> --cluster all
python aks_debug.py --login-method cli --cluster prod-aks     # no --subscription: only the current (default) subscription is searched
python aks_debug.py --login-method cli --subscription <id> --cluster 1 --no-device-code    # browser sign-in instead of the default device code
python aks_debug.py --sign-in-only                  # prints the sign-in commands (manual, default), waits for Enter, verifies; --signin-method captured = the old device-code box
python aks_debug.py --sign-in-only --tenant contoso.onmicrosoft.com
python aks_debug.py --list-accounts                 # every account the CLI knows + whether its credentials are still valid
```

**What the `cli` method does**

1. **Signed in?** `az account show` must work and the credentials must not be expired. If not, the sign-in method decides (see step 2 of the window below): by default (**`--signin-method manual`**) the numbered commands are printed (`az login --use-device-code`, `az login`, `az login --tenant <tenant-id> --use-device-code`, `az account list --output table` / `az account show`, plus an install hint when `az` is missing), then *Press Enter after you have signed in, or Ctrl+C to stop*; the tool verifies read-only and continues. `--signin-method captured` runs `az login --use-device-code` with its output captured (**device code is the default**; `--no-device-code` uses the browser flow; `--tenant <id or domain>` signs in to that tenant) and prints the URL / code box; `console` runs it in its own console window. `--sign-in-only` does just this and exits. `--list-accounts` lists every account the CLI knows with the state of its credentials; an expired one is reported (*Credentials for X expired - sign in again.*) and the sign-in method starts. No tokens are ever printed.
2. **Cluster list.** `az aks list --subscription S` for the scope you gave: `--subscription a,b,c` (ids or names), `--subscription all`, or - with no scope - only the current (default) subscription; a line says what is searched. The clusters are numbered 1..N in the order listed, so `--cluster 1,3`, `2-4`, `all` and a plain **cluster name** (`--cluster my-cluster`) all work over the collected list. `--list` prints the same list.
3. **Connect.** For each selected cluster, in turn: `az aks get-credentials --resource-group RG --name NAME --subscription S --overwrite-existing`, then `kubectl config use-context`. For a cluster that uses Microsoft Entra ID sign-in, and when `kubelogin` is on PATH, it also runs `kubelogin convert-kubeconfig -l azurecli`; without kubelogin it prints a hint (`az aks install-cli`).
4. **Everything after that is the same as before**: the kubectl context is pinned, the cloud details step runs (the subscription, resource group and cluster name go straight into the Azure target (`--subscription`, `--resource-group`, `--az-cluster` for that run), so the cluster is not searched for again), and the report is written.

With several clusters each one gets its own sign-in check and credentials step. The Stop button and the live step messages work as usual; a cluster whose login fails or is cancelled is marked FAILED with the reason and the next one still runs. `--skip-login` skips the login (no `az` login or credentials step; the kubectl context is matched by the cluster name as before). Reading the cluster list and writing the kubeconfig entry are the only things the CLI method does besides the usual read-only checks. If `az` is not installed, the message says where to get it; if no cluster is found, it says so and what to check.

**The window has four tabs** (tabs 1-3 are the same layout as the EKS and GKE tools; tab 4 is new), under the Azure banner and above the status bar; the action bar with **Login & Debug selected cluster(s)** and **Stop** is always visible: **1 Sign in and choose clusters** (the four steps below), **2 What to collect** (the section check boxes with Select all / Clear all / Only networking / Everything except networking, the counter, the quiet-collection note, pod-log options, the Azure details switch and the parallel workers box), **3 Run and results** (progress, live collection steps, clusters in this run - double-click a finished one to open its report -, live findings and counters, the live log, *Open report when done*, *Open HTML report*, *Open reports folder*). Tab 3 opens by itself when a run starts; if nothing is ticked on tab 2, the window shows tab 2. **4 Privileged roles (PIM)** shows your active and eligible PIM roles and can activate them when you confirm (see *Privileged roles (PIM) tab*). It stays usable at 1100x700 (tabs 1 and 4 scroll).

**Tab 1**: a guided, numbered layout with a status line and a message area (it always says what happened and what to do next):

1. **Login method** - *Custom login (akslogin)* or *Cloud CLI (az)*. With the custom login, step 2 says "Uses akslogin.exe - it signs in when you press Run"; below the method box, **Cluster list** lets you choose **Clusters from the akslogin menu (instant)** (default) or **Collect clusters with az from selected subscriptions (on demand)** (see above).
2. **Sign in** - a badge shows **Not checked / Checking... / Signed in as <identity> - N subscriptions (green) / Not signed in (red) / Credentials expired (red)** with the exact reason and the next action. **Sign-in method** (remembered for the session; command line `--signin-method manual|captured|console`): **I run the command myself (recommended)** = the default, **Show URL and code here (captured)** (the device-code flow: `az login --use-device-code` with its output captured and a **Sign-in details** panel with the URL, the code, a countdown and **Cancel sign-in**) and **Open a console window for me**. **Use device code (default)** applies to the last two. **Check status** re-checks without changing anything.
   **Manual panel (the default):** it says *Sign in from your own Command Prompt or PowerShell. If the Azure CLI is not installed yet, install it first, then run this command:* and shows read-only monospace boxes, each with a **Copy** button: **1** `az login --use-device-code` (the most prominent; a second line `az login --use-device-code --tenant <id>` appears when the Tenant box has a value), **2** `az login`, **3** `az login --tenant <tenant-id> --use-device-code` (the real tenant id of the selected account when known), **4a** `az account list --output table`, **4b** `az account show`; then the plain steps (open the URL az prints, https://microsoft.com/devicelogin, enter the code, approve / MFA, come back and press **I have signed in - Verify**). An **Azure CLI installed?** line shows the first line of `az --version`, or - in red - *Azure CLI (az) not found on this computer - install it first* with the install guidance as text only (https://learn.microsoft.com/cli/azure/install-azure-cli, Windows: `winget install -e --id Microsoft.AzureCLI`); the tool never installs anything. **Open a terminal for me** opens a visible PowerShell window that runs the chosen `az login` form (the round button in front of 1 - 3). **I have signed in - Verify** runs the read-only checks (`az account show`, the expiry check, `az account list`) and shows *Signed in as <user> - N subscriptions* or the exact error with what to try next (AADSTS errors, wrong tenant, no subscriptions); while the panel is open the window also checks every 5 seconds by itself (*Waiting for you to sign in...*, **Stop waiting**, at most 15 minutes). If a captured / console attempt fails or shows no URL, the panel opens by itself (*Automatic sign-in did not show a URL. Run one of these commands in your own terminal, then press Verify.*). When credentials expire, the red banner *Credentials for X expired. Press Sign in to renew.* shows the same commands right under it.
   **Accounts:** the **Account** dropdown lists *All accounts* and every account the Azure CLI knows (users, service principals and managed identities, labelled as such) with tenant name and subscription count, plus one coloured chip per account: **Active** (green, time left), **Expiring soon** (amber, under 30 minutes), **Expired** (red, *Credentials expired - sign in again*), **Not signed in** / **Unknown** (grey, with the reason). Type in the box to search a long list. Selecting an account scopes step 3 to that account's subscriptions (its tenant) and clears the collected clusters (collect again for the new scope). **Sign in with a different account** shows the sign-in commands (or runs a fresh `az login --use-device-code` with the captured / console method); afterwards the new account is selected and its subscriptions are listed. **Re-check** and **Check all accounts** refresh the statuses (other accounts are also checked in the background, at most 4 at a time). When the account in use is expired the badge and message bar say *Credentials for <user> expired. Press Sign in to renew.* and the button becomes **Sign in again**. Expiry is also detected during use: if collecting clusters or a run fails because the credentials expired, the account is marked Expired and the banner is shown (collecting continues with the other subscriptions); in a multi-cluster run the clusters of that account are marked *credentials expired* and the others still run. The last account, tenant and device-code choice are remembered for the session.
3. **Choose subscription(s)** - a searchable multi-select list (Ctrl/Shift-click) of **all** your Azure subscriptions (name, id, state; one `az account list`, 300 are fine) with **Select all (shown)** (respects the search box), **Clear** and a counter such as *3 of 300 selected*. Nothing is searched for clusters until you press the collect button in step 4. The list is read once per session; **Reload subscriptions** refreshes it. The last selection is remembered for the session.
4. **Choose clusters** - an empty list with the hint *Select one or more subscriptions above, then press 'Collect clusters from the selected subscriptions'.* and the prominent button **Collect clusters from selected subscriptions**. Only the selected subscriptions are listed (a background thread, the window never freezes): Azure Resource Graph (`az graph query`, one query per 150 of the selected subscriptions, when the `resource-graph` extension is installed - never installed for you) or `az aks list --subscription S` per selected subscription, 8 in parallel. A progress bar and *Listing clusters: x/y subscriptions ... elapsed m:ss* are shown, results appear as they arrive and are de-duplicated, **Stop** cancels and keeps what arrived. A per-subscription cache means a second press only fetches subscriptions not collected yet; **Refresh selected** reads the selected ones again. **Select all (shown)** + collect = every subscription (the old behaviour, only when you ask; more than 20 subscriptions ask for a confirmation first).

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

**Reports only read.** (In the window: *Reports only read. The Privileged roles tab can activate your own eligible roles when you confirm.*) The one exception, described in its own subsection at the end of this section, is the opt-in **Privileged roles (PIM)** tab.

**This tool only reads. It does not install, create, change or delete anything on the cluster or in the cloud account.** It installs no tools, runs nothing inside pods or nodes, and downloads and executes nothing. This is enforced in the code, not just promised: every `kubectl` call (also `kjson`, the parallel cache and the live traffic sampler, which all go through the same function) is checked against an **allow-list** before a process is started, and so is every `az` call. A command that is not on the list is **refused without starting any process**, the attempt is recorded, and the report shows it in its **Read-only guarantee** block with a CRITICAL finding. A normal report says: *Commands used: N read calls, 0 blocked.* The same sentence is in the window (step 1 note and footer), the HTML header/footer and the `.txt` report.

**Allowed `kubectl` (read):** `get` (including `get --raw`, which is always an HTTP GET; refused with `-f`, `--data`, or a path ending in `exec`/`attach`/`portforward`), `logs`, `top`, `version`, `api-resources`, `api-versions`, `cluster-info`, `explain`, `auth can-i`, and `config get-contexts | current-context | view` (`view` without `--raw`, so credentials stay redacted). Flags that run, write or change identity are refused on every verb (`-f/--filename`, `--overwrite`, `--force`, `--token`, `--as`, `--server`, `-i`, `-t`, `--follow` ...). Everything else (`apply`, `create`, `run`, `exec`, `debug`, `delete`, `patch`, `replace`, `edit`, `label`, `annotate`, `scale`, `rollout`, `cordon`, `drain`, `taint`, `cp`, `attach`, `port-forward`, `set`, `expose`, `autoscale`, `certificate`, `helm` ...) is refused.

**Allowed `az` (read), table `READ_ONLY_CLOUD_COMMANDS` in the script:** `account list|show`, `account get-access-token --query expiresOn -o tsv [--tenant T] [--subscription S]` (the credential check: **only the expiry time is queried, the token itself never reaches this tool**; every other form - no `--query`, `--query accessToken`, `-o json`, `--resource`, `--scope` ... - is refused without starting a process), `extension list`, `graph query`, `aks list|show|get-upgrades`, `network vnet subnet show`, `network nsg show|list`, `network public-ip show|list`, `network route-table show`, `network lb list`, `network nat gateway list`, `network watcher flow-log list`, `network firewall list`, `role assignment list`, `vmss list|list-instances`, `monitor diagnostic-settings list`, `monitor metrics list`, `monitor activity-log list`, `monitor log-analytics workspace show`, `monitor log-analytics query`. Everything else (`create`, `delete`, `update`, `set`, `start`, `stop`, `restart`, `run-command`, `scale`, `upgrade`, `rest`, `extension add|update|remove`, `config set` ...) is refused, as are `--yes`, `--no-wait`, `--set`, `--add`, `--remove`, `--allow-preview`.

**Nothing is installed on this machine either.** Every `az` process is started with `AZURE_EXTENSION_USE_DYNAMIC_INSTALL=no` (an environment variable; `az config set` is never run), so `az graph` or `az network firewall` never downloads an extension; if the extension is missing the tool falls back to the per-subscription calls as before. No code path runs `az extension add`, `pip install`, `gcloud components install`, `aws configure` or `az aks install-cli`; the hints that mention an install command are printed as text only.

**The only things that are not reads (all local-only, run with fixed argument shapes, anything else is refused):**

| Command | What it changes |
|---|---|
| `az login [--use-device-code] [--tenant T] [-o none] [--only-show-errors]` | your interactive sign-in: the local Azure CLI token cache (started by the *captured* / *console* sign-in, `--sign-in-only` and on request). Exactly these options and nothing else: no service principal, password, identity or `--allow-no-subscriptions`. `az logout`, `az account set` and `az config` are never run |
| `powershell -NoExit -Command "az login [--use-device-code] [--tenant T]"` | **user-initiated, local-only**: the **Open a terminal for me** button of the manual sign-in panel opens a visible PowerShell window with exactly one of these `az login` forms and nothing else (the tool does not wait for it and cannot see what you type there) |
| `az --version` | information only (the first line is shown in the *Azure CLI installed?* line); changes nothing |
| `az aks get-credentials --resource-group --name [--subscription] --overwrite-existing` | **local-only**: the local kubeconfig file, never the cluster |
| `kubelogin convert-kubeconfig -l azurecli` | **local-only**: the local kubeconfig file, never the cluster |
| `kubectl config use-context <name>` | **local-only**: the current context in the local kubeconfig file |
| `akslogin.exe` | your organisation's own sign-in tool, only when you choose the custom login; sign-in only |

### The one opt-in exception: Privileged roles (PIM) self-activation

Everything above is about reports and the data collection, and it never changes. The **4th tab (Privileged roles (PIM))** and the command line `--pim-activate-all --yes` are the **only** place where the tool sends a write, and only because **you** press a button and confirm in a dialog (or pass `--yes`). What it can send is limited by an allow-list in the code (`assert_pim_get` / `assert_pim_write`; everything else is refused without sending anything and answered with *blocked: read-only mode*):

- **reads:** the list calls described in the next section (your own eligible / active schedule instances and requests, role policies, role definitions, the PIM service's `roleAssignments` / `roleSettingsV2` / `resources`) and `az ad signed-in-user show` (your object id);
- **writes: only your own SELF-ACTIVATION requests**, exactly three shapes - an Azure resource role `PUT {scope}/providers/Microsoft.Authorization/roleAssignmentScheduleRequests/{guid}?api-version=2020-10-01` with `requestType` **SelfActivate**, and the PIM service `POST .../aadroles/roleAssignmentRequests` and `POST .../aadGroups/roleAssignmentRequests` with `type` **UserAdd**, `assignmentState` **Active**. The guard checks the body field by field: the principal / subject must be **your own object id**, a non-empty justification, a linked eligibility, a duration within the role policy maximum, and no other field. Assigning a role to anyone, `AdminAssign` / `AdminAdd` / `AdminRemove` / `UserRemove` / `AdminUpdate`, `assignmentState` Eligible, deleting an assignment, changing a policy or role setting, creating a group or a user, another principal, an empty justification - all refused;
- **never during a report:** the report runs, the section collection and tabs 1-3 never call the PIM write function (a test makes it raise during full report runs);
- **recorded:** the report's *Read-only guarantee* block lists the PIM activations requested in the session (count, time, role, scope, duration), or says `0`.

A self-activation does not give you new access: it switches on a role you are already eligible for, for a limited time, exactly as the portal's PIM page would. Your organisation's policy still decides (justification, ticket, approval, multi-factor authentication).

## Privileged roles (PIM) tab

For companies that give access through **Privileged Identity Management**: after you sign in with the Azure CLI you can see everything you hold and activate all your eligible roles at once from tab 4, without opening the Azure portal.

**What it shows** (after **Refresh**; the first visit to the tab reads it once, read-only): an **Active roles** table (what you can use right now) and an **Eligible roles** table (allowed, not active), for three families - **Azure resource roles** (subscriptions, resource groups, ...), **Microsoft Entra roles** (directory roles) and **Privileged access groups**. Columns (full words, with a *Column help* tooltip and a *What this table shows* line under each table): Role or group name, Type, Scope, Status (*Active*, *Eligible*, *Pending approval*, *Expired soon* = under 30 minutes left), Expires at (time left), Maximum duration allowed (from the role policy), Requires (justification, ticket, approval, multi-factor authentication, authentication context). A search box, a type filter, **Select all eligible** / **Clear** and a counter (*5 active, 12 eligible*). Under the buttons a status chip per family tells what worked (*Azure resource roles: OK (2 active, 3 eligible)*, or *failed (HTTP 403) - see below*). If nothing is eligible the tab says so plainly with the likely causes (not eligible, a different tenant, PIM not enabled, no permission). A service principal / managed identity gets *PIM is not available* (PIM is for people); a guest account gets a notice that PIM is usually not available for guests.

**How to activate:** **Activate selected** (select rows in the Eligible table) or **Activate ALL eligible roles at once**. Both open a **confirmation dialog first** - nothing is ever activated silently, on tab open, by Refresh or during a report. The dialog lists every role / group that will be requested, asks for the **justification** (required; empty by default; the last text is remembered for the session only, in memory), the **duration in hours** (empty = each role's policy maximum; a number = that long but **never more than a role allows**), an optional **ticket number / system**, and says *This submits N self-activation requests for roles you are already eligible for. It does not grant new access.* with **Cancel** and **Activate**. Roles already active, or waiting for approval, are skipped. The requests run **4 at a time**; the results table shows each role as *Activated*, *Already active* (RoleAssignmentExists), *Pending approval* or *Denied* with the service's reason (justification, ticket, policy limit, *Authentication context / multi-factor authentication required - complete it in the Azure portal*, access denied); HTTP 429 is retried with back-off (2, 5, 10 s). A summary line follows and the active list refreshes by itself. **Stop** cancels the requests not sent yet (one already sent cannot be recalled). Every request and result is a line in the live log (tab 3); no token and no justification is ever written to a file (the justification only lives in memory for the session).

**How it talks to Azure (no Microsoft Graph, no tokens in the tool):** everything goes through `az rest`, so the Azure CLI signs the request and the token never reaches this program (it is never printed, logged or stored).

1. **Azure resource roles:** the ARM PIM API (`https://management.azure.com`, api-version `2020-10-01`): `roleEligibilityScheduleInstances` / `roleAssignmentScheduleInstances` / `roleAssignmentScheduleRequests` with `$filter=asTarget()`; the maximum duration and requirements come from the role's policy assignment (`Expiration_EndUser_Assignment`, `Enablement_EndUser_Assignment`, `Approval_EndUser_Assignment`); activation is the `SelfActivate` PUT with `justification`, `scheduleInfo` (`AfterDuration`, the capped duration), `linkedRoleEligibilityScheduleId` and `ticketInfo` when given.
2. **Microsoft Entra roles and Privileged access groups:** the PIM service API behind the portal's own PIM blade, `https://api.azrbac.mspim.azure.com/api/v2/privilegedAccess/` (`aadroles` / `aadGroups`), with `az rest --resource 01fc33a7-78ba-4d2f-a4b7-768e336e890e`. The Azure CLI can obtain a token for it without any Graph consent, which is why it is used where the Microsoft Graph route (`Activate-PimRoles.ps1`) is blocked for lack of admin consent. Lists: `roleAssignments?$expand=...&$filter=(subject/id eq '<you>') and (assignmentState eq 'Eligible'|'Active')` (if that query is refused, simpler variants are tried and the working one is remembered); limits: `roleSettingsV2`; activation: `POST roleAssignmentRequests` with `type` UserAdd, `assignmentState` Active, `subjectId` you, `reason`, `schedule` (`Once`, start, end = start + duration), `linkedEligibleRoleAssignmentId`. Your object id comes from `az ad signed-in-user show --query id` (or, if that fails, from the Azure answer); the tenant from `az account show`.
3. **If method 2 is blocked** (401 / 403 / AADSTS / anything), the tab shows the panel *This tenant blocked the automatic method* with the HTTP status and the response body (tokens scrubbed), a button **Open the PIM page in your browser** (Microsoft Entra roles / groups) and a hint to use your existing `activate_pim_roles_ui.py` (text only: this tool never imports or runs it). Azure resource roles keep working independently of this.

**Needs:** the **Cloud CLI (az)** sign-in (step 1 of tab 1 - with the akslogin-only method the tab explains how to sign in with `az login --use-device-code`) and that you are signed in to the tenant that holds your PIM eligibility.

**Command line:** `python aks_debug.py --pim-list` prints the active and eligible roles (read-only). `python aks_debug.py --pim-activate-all --justification "TEXT" [--hours N] [--ticket-number N --ticket-system S] --yes` prints the list, then activates every eligible role that is not active; **without `--yes` it only prints what it would request and exits**.

**Limits / not verified:** the PIM service endpoints and the exact answers could not be tested against a real tenant (only against mocked answers), so run it once on your tenant; if a family fails, the tab shows the status and body - send that text. Approvals and multi-factor / Conditional Access steps may need extra work in the portal (*Pending approval*, *authentication context required*). An Entra role limited to an administrative unit may need the portal. Activating through the tool does not bypass any policy.

## Permissions (read-only)

Without one of these, that part says why it is missing and the rest still runs.

- **Azure RBAC:** `Reader` on the AKS cluster, its **node resource group** (`MC_...`) and the **VNet** resource group; `Monitoring Reader` for metrics and diagnostic settings; `Log Analytics Reader` on the workspace for the control-plane log queries; permission to list role assignments for the identities section.
- **Kubernetes:** the same as the EKS version (`nodes/proxy` for kubelet stats, list on pods / nodes / events / namespaces, etc.).
- **Privileged roles tab (opt-in):** only your own PIM eligibility; no extra role. Activation is allowed by your organisation's PIM policy, not by this tool.

## Not verified against a real Azure subscription

I built and tested this against mocked `kubectl` and `az` output, not a real AKS cluster. The `az` JSON field names, the Azure Monitor metric names (`Network In Total`, `ByteCount`, `UsedSnatPorts`, `DipAvailability`, ...) and the Log Analytics table names (`AKSControlPlane`, `AKSAudit`, `AzureDiagnostics`) follow the documented Azure CLI and API, but **run it once on a real cluster and check each section**. A call that fails prints its error in the report instead of stopping the run.

## Reports

`reports\aks_debug_<cluster>_<time>.html` (interactive) and `.txt`; with several clusters also `reports\aks_debug_summary_<time>.html`. Pod logs and the report can contain sensitive data; share them carefully.

Settings at the top of the script: `LOOKBACK_MINUTES`, `MAX_LOG_PODS`, `LOG_TAIL_LINES`, `UTIL_WARN` / `UTIL_CRIT`, `SUPPORT_LABEL`, `TRAFFIC_SAMPLE_SECONDS`, `LOW_SUBNET_IPS`.

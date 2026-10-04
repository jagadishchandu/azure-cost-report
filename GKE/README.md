# GKE Debugger

`gke_debug.py` is the **GKE version** of the EKS debugger (`..\eks-debug`) and the AKS debugger (`..\aks-debug`), both unchanged. It logs in with your **`gkelogin`**, then collects the debugging picture of **what happened and what is happening in the last N minutes** and saves it as an **interactive HTML report** and a text report. Everything is **read-only**.

All the features of the EKS / AKS versions are here: live step-by-step collection with Stop, multi-cluster runs with a summary page, the resource utilization dashboard by namespace, namespaces (pods used vs configured, quotas), pod logs (unhealthy / warning-event / core add-on / all pods), node name + the actual VM, the support-DL label (`elvh-app-support-dl`) and **Teams to contact**, the network section, and the **traffic over the selected window**. The Google Cloud parts replace the AWS / Azure parts.

## Quick start

```powershell
cd C:\Users\jchandraprasad\Downloads\app\gke-debug
python gke_debug.py                         # window: select one OR SEVERAL clusters
python gke_debug.py --minutes 60
python gke_debug.py --cluster 3             # no window: log in to cluster #3 and collect
python gke_debug.py --cluster 1,3,5         # several clusters, one after another (also 2-4, or all)
python gke_debug.py --list                  # clusters gkelogin offers
python gke_debug.py --list --project my-proj,other-proj   # clusters of THOSE projects only (add --all-clusters to merge the gkelogin menu); --project all = every project (slow, only when you ask)
python gke_debug.py --all-clusters --project my-proj --cluster my-gke      # also: --cluster 3 | 1,3,5 | 2-4 | all (numbers of the collected list)
python gke_debug.py --cluster 3 --skip-login
python gke_debug.py --cluster 3 --project my-gcp-project
python gke_debug.py --list-projects
python gke_debug.py --cluster 3 --gke-cluster prod-gke --location us-central1 --project my-gcp-project   # point at the cluster by hand
python gke_debug.py --cluster 3 --no-gcp    # kubectl only
python gke_debug.py --cluster 3 --only-networking          # only the Network and traffic section
python gke_debug.py --cluster 3 --no-networking            # everything except the Network and traffic section
python gke_debug.py --cluster 3 --sections nodes,pods,networking      # exactly these sections (ids: --list-sections)
python gke_debug.py --cluster 3 --skip-sections logs,timeline
python gke_debug.py --list-sections         # the report sections, what each one shows and what it needs
python gke_debug.py --cluster 3 --workers 4 # 4 collection tasks at a time (default 8; 1 = one after another, the old order)
```

Put `gkelogin.exe` in this folder (or pass `--gkelogin C:\path\gkelogin.exe`). `kubectl` and the **Google Cloud CLI (`gcloud`)** must be on PATH, and you must be signed in to gcloud for the GCP sections (the window / `--login-method cli` can do the sign-in for you, see below). No extra Python packages are needed.

The login is your `gkelogin(cluster_number)` function in the same shape as `ekslogin` / `akslogin`: it runs `gkelogin.exe`, sends the cluster number on stdin and waits 2 seconds. **This is an assumption: I have not seen your `gkelogin`.** If it behaves differently (different menu, extra arguments, a different exe name), paste the function and I will match it. The cluster list comes from the `CLUSTERS` dict at the top of the script, a `clusters.json` next to it (`{"1": "name", ...}`), or the menu `gkelogin.exe` prints (parsed as `1. name`, `1) name`, `[1] name`, `1 - name`). If it can't be parsed, type the cluster numbers into the box.

## Read-only guarantee

**This tool only reads. It does not install, create, change or delete anything on the cluster or in the cloud account, it runs nothing inside the cluster (no exec, no debug pod), and it installs nothing on your machine.** This is enforced in code, not just by habit: every kubectl and gcloud command is checked against an allow-list *before* a process is started, and the two Google API calls against an allow-list of URLs. Anything else is refused with `blocked: read-only mode - '<verb>' is not allowed`, **no process or HTTP request is started**, the attempt is recorded, shown in a "Read-only guarantee" block of the report (text and HTML; a blocked attempt also raises a CRITICAL finding), and the report states the real counts ("N read calls, 0 blocked"). The window shows "read-only" in the banner and a note in step 1.

Allowed kubectl (`kubectl_violation`): `get` (including `get --raw /path`, a GET; never with `-f`, `--filename`, `-k`, `--data`), `logs`, `top`, `version`, `api-resources`, `api-versions`, `cluster-info` (not `dump`), `explain`, `auth can-i`, `config get-contexts | current-context | view` (never `--raw`). Everything else - apply, create, run, exec, debug, delete, patch, replace, edit, label, annotate, scale, rollout, cordon, drain, taint, cp, attach, port-forward, set, expose, autoscale, helm, any other `config` / `auth` subcommand, any other binary - is refused.

Allowed gcloud (`READ_ONLY_CLOUD_COMMANDS`, one table at the top of the guard section): `auth list`, `auth print-access-token`, `config get-value`, `projects list | describe | get-iam-policy`, `organizations list`, `asset search-all-resources`, `iam service-accounts describe`, `container clusters list | describe`, `container get-server-config`, `container operations list`, `compute instances list`, `compute instance-groups managed list | list-errors`, `compute networks describe`, `compute networks subnets describe`, `compute firewall-rules list`, `compute routers list | get-nat-mapping-info`, `compute routes list`, `compute addresses list`, `compute forwarding-rules list`, `compute backend-services list | get-health`, `compute packet-mirrorings list`, `network-management connectivity-tests list`, `logging read`. Anything with create / delete / update / add / remove / set (except `config get-value`) / start / stop / reset / resize / patch / deploy / run / ssh / scp / `components install | update` / enable / disable / import / export / set-iam-policy is refused. The REST helper only allows Cloud Monitoring `timeSeries.list` (GET) and Cloud Logging `entries:list` (a read, sent as POST). Every gcloud subprocess runs with `CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK=1` and `CLOUDSDK_CORE_DISABLE_PROMPTS=1`, so nothing auto-installs or prompts.

**Local-only exceptions** (they write only on this machine, never to the cluster or the cloud account): your own interactive sign-in, exactly `gcloud auth login [--no-launch-browser] [--account EMAIL]` (you start it - with the captured / console sign-in methods or the **Open a terminal for me** button, which opens a visible PowerShell window `powershell -NoExit -Command "gcloud auth login --no-launch-browser"` and nothing else; the guard accepts nothing else for `auth login`: no `--no-browser`, no `--cred-file`, no `auth revoke`, no `config set`), `gcloud container clusters get-credentials` (writes the local kubeconfig), `kubectl config use-context` (switches the context in the local kubeconfig) and the custom `gkelogin.exe` you chose. **Choosing / pinning an account writes nothing**: the tool never runs `gcloud config set account`, `gcloud auth revoke` or `gcloud auth activate-service-account`; it only adds `--account EMAIL` (a read-only global flag) to the gcloud calls it makes. Messages that mention installing something (for example `gcloud components install gke-gcloud-auth-plugin`) are printed text only; the tool never runs them, never runs `pip install` and never downloads or executes a tool.

## Signing in (Cloud CLI method): three methods

Step 2 has a **Sign-in method** selector (command line: `--signin-method manual|captured|console`, default `manual`; the choice is remembered for the session).

1. **I run the command myself (recommended, default).** The window shows, in read-only monospace boxes with **Copy** buttons: "Sign in from your own Command Prompt or PowerShell. If the Google Cloud CLI is not installed yet, install it first, then run this command:" and
   1. `gcloud auth login --no-launch-browser` (first and prominent: prints a URL - open it, sign in, copy the verification code and paste it back into the terminal; with an account in the *hint* box an extra `... --account <email>` line appears),
   2. `gcloud auth login` (normal browser flow),
   3. `gcloud auth list` and `gcloud projects list` (see the accounts / verify),
   plus, as **text only** (never run by this tool): `gcloud auth application-default login` and, when the plugin is missing, `gcloud components install gke-gcloud-auth-plugin`.
   A line shows "Google Cloud CLI installed: <first line of `gcloud --version`>" or, in red, "Google Cloud CLI (gcloud) not found on this computer - install it first" with https://cloud.google.com/sdk/docs/install and `winget install -e --id Google.CloudSDK` as text (the tool never installs anything). **Open a terminal for me** opens a visible PowerShell window running the chosen command (`powershell -NoExit -Command "gcloud auth login --no-launch-browser"`; a user-initiated local-only exception limited to exactly `gcloud auth login [--no-launch-browser] [--account X]`). **I have signed in - Verify** runs the read-only checks (`gcloud auth list`, the token check, `gcloud projects list`) and shows "Signed in as <account> - N projects" or the exact error and the next command to try (for example "Reauthentication required"). The window also checks every 5 seconds (for at most 15 minutes) and flips to signed in by itself ("Waiting for you to sign in..."); **Stop waiting** ends the polling. An expired-credentials banner shows these commands directly under the message.
2. **Show URL here and paste the code (captured).** `gcloud auth login --no-launch-browser` runs with its output captured: raw chunks are read (not line by line) from stdout and stderr, so a prompt or a link without a newline is still seen; `CLOUDSDK_CORE_DISABLE_PROMPTS` is not set for it. A live, collapsible **Raw output from gcloud** box with the exact command line is always visible. If no URL appears within about 6 seconds the panel shows "No URL received from gcloud yet" and switches to the manual commands with **Retry**, **Run in a console window instead** and **Copy command** (a late URL switches back). When the process ends without success the exit code, the last output lines and likely causes are shown.
3. **Open a console window for me.** The same command in its own console window; the window waits and re-checks.

Command line: with `manual` the numbered command block is printed (with the install hint when gcloud is missing), the tool waits ("Press Enter after you have signed in, or Ctrl+C to stop"), verifies and continues. `--list-accounts`, `--account` and `--no-device-code` keep working.

## Faster: the collection runs in parallel (after the login)

After the login the collection is a set of **tasks that run at the same time** instead of one after another:

- Every report section is a task. A section starts as soon as the data it reads exists (the cluster data first; the Google Cloud section before the sections that label nodes with their VM; Pod logs after Unhealthy pods; the Timeline last), so Overview, Cluster data, Google Cloud, Network and traffic, Events, Workloads ... overlap.
- **Inside** the heavy sections independent reads run together too: the 17 kubectl object reads and the metrics-server reads, the kubelet stats of every node, the pod logs, the Google Cloud reads (instance groups, subnet / firewall rules / Cloud NAT, IAM policy and service accounts, forwarding rules, backend-service health, NAT mapping), every Cloud Monitoring query (node bytes, load balancers, Cloud NAT) and the four Cloud Logging queries, and the ~35 blocks of the network section. The **live traffic sample** (`--traffic-sample`) sleeps in its own block while everything else keeps working.
- **The report does not depend on timing.** Every task writes into its own sub-report and findings list; the results are merged in the fixed section order (and, inside a section, in block order). `--workers 1` and `--workers 8` give the same text report (timestamps aside; the mock tests compare them line by line). `--workers 1` keeps the old one-after-another order.
- **Limits that protect the cluster and the cloud APIs:** never more than `KUBECTL_CONCURRENCY = 6` kubectl calls and `GCLOUD_CONCURRENCY = 4` gcloud / Google API calls at the same moment (constants at the top of the script).
- A task that fails is reported (`[!] ... failed`) and the others carry on. **Stop** cancels everything that has not started: no new call is made, the calls already running finish (at most 6 + 4), and a partial report is written.
- Progress: the window shows "x of y collection tasks done"; the end of the run prints a **timing summary** (total, per step, how much faster than one after another) and the HTML footer shows it too.

Measured on the mock cluster with 0.15 - 0.2 s of artificial latency per call (about 90 calls): **12.0 s one after another -> 3.3 s with 8 workers (3.6x)**; with a 2 s live traffic sample 4.4 s -> 2.6 s. On a real cluster the gain depends on the latency of the calls; the caps above limit it.

## What to collect: a tick box for every section

One registry (`SECTIONS` at the top of the script) lists every report section with its number, full title, icon, one-line description and what it needs. The collection, the report, the table of contents, the health summary and the window are all driven by it.

| # | id | section |
|---|---|---|
| 1 | `overview` | Cluster overview |
| 2 | `gcp` | Google Kubernetes Engine cluster and infrastructure |
| 3 | `nodes` | Nodes |
| 4 | `utilization` | Resource utilization |
| 5 | `nodepods` | Pods on each node |
| 6 | `namespaces` | Namespaces |
| 7 | `pods` | Unhealthy pods |
| 8 | `events` | Events |
| 9 | `workloads` | Workloads |
| 10 | `networking` | Network and traffic |
| 11 | `scaling` | Autoscaling, storage and services |
| 12 | `top` | Top resource consumers |
| 13 | `logs` | Pod logs |
| 14 | `timeline` | Timeline |

- **Window:** tab 2 "What to collect" has one tick box per section (icon, full title, one-line description), the buttons **Select all**, **Clear all**, **Only networking**, **Everything except networking** and the counter "7 of 14 sections selected". The selection is kept while the window is open, the panel is locked during a run, and a run with nothing ticked is refused. The older "GCP details" and "Pod logs" ticks are the same boxes (cluster infrastructure, Pod logs).
- **Command line:** `--sections a,b,c` (only these), `--skip-sections x,y`, `--only-networking`, `--no-networking`, `--list-sections`. `--no-logs` and `--no-gcp` are aliases of unticking Pod logs / the cluster infrastructure section (`--no-gcp` also means *no gcloud call at all*, not even silently). Names also work as words (`network`, `traffic`, `gke`, `autoscaling`, ...).
- **Unticked sections are never collected** - no call is scheduled for them. In the report each one is a single line `Skipped by choice: <number>. <section>` (also in the HTML table of contents; the multi-cluster summary shows "N of 14" sections collected per cluster). The health summary, the counters and the findings count **only the collected sections**; with Network and traffic unticked there is no checklist.
- **Needed data of an unticked section is read silently:** Network and traffic needs the cloud network and VM data of the Google Cloud section (subnet, firewall rules, Cloud NAT, VMs); Pod logs needs the Unhealthy pods data. Only those parts are fetched, they are not shown, their findings are not counted, and a small note says so (window, summary, HTML). So "only networking" still gives a complete network report. (Nodes, Utilization, Pods on each node, Unhealthy pods and Events only label nodes with their VM when the Google Cloud section is ticked; they do not trigger a gcloud call.)
- The cluster data (the kubectl object reads) is always collected: every section works from it. The live usage reads (kubelet stats, metrics-server) are skipped when no ticked section uses them.

## The look: Google Cloud branding

The window and the HTML report are drawn from the constants at the top of the script (`CLOUD_NAME`, `BRAND_PRIMARY` `#4285F4`, `BRAND_ACCENT`, `BRAND_COLORS`, the `draw_logo` / `logo_svg` functions), so another cloud can be skinned by changing them.

- **Window:** a header banner with a Google-Cloud-style logo (a stylised cloud in the four Google colours with the Kubernetes helm inside, drawn from Tk canvas shapes: no image files, no copied artwork), the title "Google Kubernetes Engine (GKE) Debugger", a subtitle with the window minutes, the signed-in account and the number of projects, and soft decorative clouds. Three tabs (sign in and clusters / what to collect / live run), card panels with a coloured stripe, a modern ttk theme, striped lists, status chips, symbols (cloud, helm, lock, magnifier, play / stop, check / cross, warning, VM, globe, document, refresh, severity dots) with a plain fallback when Tk cannot draw them, and a status bar with progress, elapsed time and "x of y collection tasks done". It is usable at 1100 x 700 and resizes cleanly.
- **HTML report:** the same header band with an inline SVG logo, the accent colour, section icons in the headings and the table of contents, and the timing in the footer. Nothing is loaded from the internet; all interactive features (search, sorting, filters, dark mode, charts) are unchanged.

## Login methods (custom login or the Google Cloud CLI)

There are two ways to log in. The default is unchanged.

| | `--login-method exe` (default) | `--login-method cli` |
|---|---|---|
| Login | your `gkelogin.exe` (the cluster number is sent on stdin) | the standard **Google Cloud CLI** (`gcloud`) |
| Cluster list | `CLUSTERS` / `clusters.json` / the `gkelogin.exe` menu; with `--all-clusters` (default in the window): **every cluster `gcloud` can see** plus the menu entries | read from `gcloud` (below) |
| Window | login method **Custom login (gkelogin)** | login method **Cloud CLI (gcloud)** |

```
python gke_debug.py --login-method cli --list                       # list the clusters gcloud can see (numbered 1..N)
python gke_debug.py --login-method cli --project my-proj --cluster 1
python gke_debug.py --login-method cli --project my-proj --cluster all
python gke_debug.py --login-method cli --project my-proj --cluster my-gke
python gke_debug.py --login-method cli --project my-proj --cluster 1                    # device code / no-browser sign-in is the DEFAULT
python gke_debug.py --login-method cli --project my-proj --cluster 1 --no-device-code   # normal browser sign-in (gcloud auth login)
python gke_debug.py --login-method cli --account me@corp.com --cluster 1                # use this signed-in account for every gcloud call
python gke_debug.py --list-accounts                                                      # accounts gcloud knows, active one marked, status (Active / credentials expired)
```

**What the `cli` method does**

1. **Signed in?** `gcloud auth list` must show an account and `gcloud auth print-access-token --account X` must work for it (the token is thrown away at once, never stored or printed). If there is none, or its **credentials expired** (the message says "Credentials for X expired"), the script signs in according to `--signin-method` (see **Signing in** below): **manual** (default) prints the numbered commands and waits for you ("Press Enter after you have signed in, or Ctrl+C to stop"), then verifies read-only (`gcloud auth list`, token check, `gcloud projects list`) and continues; **captured** runs `gcloud auth login --no-launch-browser [--account X]` with its output captured, prints the framed box with the link and asks for the verification code (hidden input, never printed); **console** runs it in your console / its own console window. `--no-device-code` (without an explicit `--signin-method`) = console with the normal `gcloud auth login`. `--no-browser` is not used.
2. **Cluster list - only for the projects you name.** `gcloud container clusters list --project P --format=json` for **exactly** the projects in scope: `--project a,b,c` (comma list), `--project all` (every project `gcloud` can see - slow, only when you ask), a single `--project P`, or - when no scope is given - only the currently configured `gcloud` project. A line says what is searched ("Cluster search scope: ..."). `--all-clusters` alone no longer scans everything. Projects without access or without the Kubernetes Engine API are skipped. The clusters are numbered 1..N in the order collected, so `--cluster 1,3`, `2-4`, `all` and a plain **cluster name** all work (numbering is over the collected list). `--list` prints the same list.
3. **Connect.** For each selected cluster, in turn: `gcloud container clusters get-credentials NAME --location LOC --project P`, then `kubectl config use-context gke_P_LOC_NAME`. kubectl needs the `gke-gcloud-auth-plugin`; if it is not on PATH the script prints `gcloud components install gke-gcloud-auth-plugin`.
4. **Everything after that is the same as before**: the kubectl context is pinned, the cloud details step runs (the project, location and cluster name go straight into the GCP target (`--project`, `--location`, `--gke-cluster` for that run), so the cluster is not searched for again), and the report is written.

With several clusters each one gets its own sign-in check and credentials step. The Stop button and the live step messages work as usual; a cluster whose login fails or is cancelled is marked FAILED with the reason and the next one still runs. `--skip-login` skips the login (no `gcloud` login or credentials step; the kubectl context is matched by the cluster name as before). Reading the cluster list and writing the kubeconfig entry are the only things the CLI method does besides the usual read-only checks. If `gcloud` is not installed, the message says where to get it; if no cluster is found, it says so and what to check.

**In the window** the top is a guided, numbered layout with a status line and a message area (it always says what happened and what to do next):

1. **Login method** - *Custom login (gkelogin)* or *Cloud CLI (gcloud)*. With the custom login, step 2 says "Uses gkelogin.exe - it signs in when you press Run" and the cluster list comes from gkelogin as before.
2. **Sign in** - a badge shows **Not checked / Checking... / Signed in as <identity> (green) / Not signed in (red) / Credentials expired (red)** with the exact reason and the next action. **Sign-in method** (remembered for the session; command line `--signin-method manual|captured|console`): **I run the command myself (recommended)** = the DEFAULT; **Show URL here and paste the code (captured)**; **Open a console window for me**. See **Signing in** below. The device-code panel, account dropdown, expired-credentials banner and **Check status** work as before. A missing CLI is reported with its install link.
   **Accounts:** the **Account** dropdown lists every account gcloud knows (`gcloud auth list`, user and service accounts, the active one marked; type to filter a long list) with a coloured status per account: **Active** (green), **Credentials expired - sign in again** (red), **Not signed in** / **Unknown** (grey, with the reason). Status is read-only (`gcloud auth print-access-token --account X`, token discarded); the other accounts are checked lazily in the background (4 at a time) and **Check all accounts / Re-check** refreshes them. Choosing an account (or **Use this account**) pins it: the tool adds `--account EMAIL` to every gcloud call (`auth list` excepted, it lists all) and takes the REST token from `gcloud auth print-access-token --account EMAIL`; nothing is written to gcloud (no `config set account`, no `auth revoke`), the choice is remembered for the session, and the projects and clusters are read again. **Sign in with a different account** starts a fresh sign-in (optional account hint box -> `--account HINT`); the newly signed-in account is then the one in use. Limit: for kubectl the pin is passed as `CLOUDSDK_CORE_ACCOUNT` to the `gke-gcloud-auth-plugin`; if you have Application Default Credentials set up the plugin may prefer them (gcloud's own calls always honour `--account`).
   **Expired credentials:** if the account in use is expired - found by the check or during use (project / cluster listing, Monitoring / Logging calls, a run) - the badge and message bar turn red ("Credentials for X expired. Press Sign in to renew."), **Sign in** is highlighted and starts the device-code flow for exactly that account; a multi-cluster run marks the clusters **credentials expired** (not a generic failure) and carries on with the others where it can. gcloud user tokens last about an hour and refresh by themselves until your organisation's session length ends, so there is no reliable "time left": the account shows Active until it is expired.
3. **Choose project** - a searchable list. Step 3 lists **all** the projects `gcloud` can see (name, id, state). Pick **All projects (N)** (the default, an explicit labelled choice) or select one or several rows (Ctrl/Shift-click): selecting narrows the cluster list below at once; choosing "All" clears the selection. The list is read once per session; **Reload projects** refreshes it.
4. **Choose clusters (on demand)** - the list starts **empty** with the hint "Select one or more projects above, then press 'Collect clusters from the selected projects'." **Nothing is searched after sign-in or when the window opens** (searching every project is slow). Step 3 lists the projects (one cheap `gcloud projects list`) as a searchable multi-select list (Ctrl/Shift-click) with **Select all (shown)**, **Clear** and a counter ("3 of 300 selected"); the last selection is remembered for the session; the **Account** dropdown keeps scoping. The prominent button **Collect clusters from selected projects** runs `gcloud container clusters list --project P` **only for the selected projects** (8 in parallel; with 25+ projects ONE Cloud Asset Inventory search, restricted to the selected projects, is tried first), fills the list as results arrive with "Listing clusters: x/y projects ... elapsed m:ss", a progress bar and **Stop**. A per-project cache means pressing again fetches only projects not collected yet; **Refresh selected** re-reads the selected ones. More than 20 projects ask first: "This will search 300 projects and can take several minutes. Continue?". **Select all** + **Collect** (or the explicit **All projects** option) = every project, but only when you ask. A project that fails is logged and skipped; an expired account is marked expired and the run carries on.

Steps unlock in order: until you are signed in (Cloud CLI method) step 4 shows "Sign in first".

**Search** (both lists): type in the box next to the magnifier (case-insensitive; several words must all match; name, id, location / group, project ... are searched); **x** clears it; "Showing X of Y" tells how many rows match; your selection is kept while you filter; **Select all (shown)** and **Clear** act on the rows in view. In the cluster list "or type numbers" still works (`1,3,5`, `2-4`, `all`).

A cluster of another project than the one picked in step 3 always uses **its own** project for the run.

**Prerequisites for the `cli` method:** `kubectl`, the Google Cloud CLI (`gcloud`), and `gke-gcloud-auth-plugin`.

## All clusters I can access (both login methods)

Both methods can show **every cluster the signed-in user can access**, not only what the `gkelogin` menu offers.

- **How it is listed (read-only), on demand.** Only when you press **Collect clusters from selected projects** (window) or give `--project a,b,c|all` (command line): `gcloud container clusters list --project P` for each project in scope, 8 in parallel (25+ projects: ONE Cloud Asset Inventory search first, restricted to the scope, used when it works). Results are de-duplicated; a project that fails (no access, Kubernetes Engine API off) is **logged and skipped**. A clear line is shown: `Found 57 clusters in 12 projects (300 searched), 3 projects failed: a, b, c`.
- **Cloud CLI method:** the on-demand button is the only way to fill the list (`--project` / step 3 decide which projects).
- **Custom login (gkelogin) method:** in the window, step 1 has **Cluster list: (o) Clusters from the gkelogin menu (instant)** (the default; shown at once, no cloud call) **/ ( ) Collect clusters with gcloud from selected projects (on demand)** (choose projects in step 3, then press the button; the menu entries are merged in). On the command line `--all-clusters` plus `--project a,b|all` does the same (without `--all-clusters` the exe method shows only the menu). The gkelogin menu is read as before and merged into the list: a menu entry whose name `gcloud` also lists is marked with its menu number and **keeps logging in with `gkelogin`**; a menu entry `gcloud` cannot see stays in the list (shown as `name (gkelogin menu #N)`); a cluster that is **not in the menu** is logged in with `gcloud container clusters get-credentials NAME --location LOC --project P`. A menu name that belongs to several clusters (same name in two projects) is ambiguous and uses `gcloud`, which knows the exact project.
- **If `gcloud` is not usable** (not installed, not signed in, sees no project) the list falls back to the gkelogin menu with a clear message, press **Collect clusters from selected projects** again after `gcloud auth login`.
- `--list` prints `N - name (location/project)   [name: ... | project: ... | location: ... | gkelogin menu #M]`, then the count line. `--cluster` accepts a number, `1,3,5`, `2-4`, `all` or a cluster name of that list.

## What is different from EKS and AKS

| EKS version | AKS version | GKE version |
|---|---|---|
| `ekslogin` | `akslogin` | `gkelogin` |
| AWS CLI + `~/.aws` profiles | Azure CLI + subscription | **Google Cloud CLI + project**, picked after login: `--project` / step 3 of the window, else the project in the nodes' `providerID` (`gce://<project>/<zone>/<instance>`), else the project in the kubectl context name (`gke_<project>_<location>_<cluster>`), else your `gcloud config` default; checked by finding your cluster with `gcloud container clusters list` |
| EKS cluster, nodegroups, add-ons, Fargate | AKS cluster, node pools, add-ons | **GKE cluster, node pools, release channel, upgrades, add-ons** (`gcloud container clusters describe`, `get-server-config`), Autopilot or Standard |
| VPC, subnets, security groups, route tables, NAT | VNet subnets, NSG, route tables, NAT gateway | **VPC subnet + pod / service secondary ranges, firewall rules, routes, Cloud Router / Cloud NAT**, static IPs |
| IAM roles, `aws-auth` | Managed identities, role assignments | **Node service account and its project roles** (`projects get-iam-policy`), GKE service agent |
| EC2 instance id / Name tag, status checks | VMSS instance, power / provisioning state | **Compute Engine instance** (name, numeric id, zone, machine type, spot / preemptible), **VM status** and **managed instance groups** |
| CloudWatch control-plane logs | Diagnostic settings + Log Analytics | **Cloud Logging**: control-plane errors, cluster autoscaler events, Kubernetes audit PERMISSION_DENIED / UNAUTHENTICATED, failed GKE admin calls; **GKE operations** (upgrades, repairs, resizes) |
| CloudWatch traffic (EC2, ALB/NLB, NAT) | Azure Monitor (VM, LB, NAT gateway) | **Cloud Monitoring**: node network in/out, load balancer requests / 5xx / latency (HTTP(S)) or bytes (network LB), Cloud NAT drops / allocation failures / port use |
| VPC CNI (`aws-node`) | Azure CNI | **netd / Dataplane V2 (`anetd`, Cilium)**, calico, ip-masq-agent, kube-dns (or Cloud DNS), konnectivity-agent, gke-metadata-server; pod IPs per node block of the pod secondary range |
| Service annotation `aws-load-balancer-internal` | `azure-load-balancer-internal` | `networking.gke.io/load-balancer-type: Internal`; a LoadBalancer Service without it is `INTERNET-FACING`. Ingress classes `gce` (external) / `gce-internal`, plus the `ingress.kubernetes.io/backends` health that GKE Ingress writes |
| `--aws-cluster / --region / --profile / --no-aws / --list-profiles` | `--az-cluster / --resource-group / --subscription / --no-azure / --list-subscriptions` | `--gke-cluster / --location / --project / --no-gcp / --list-projects` |

The Kubernetes sections (nodes, pods, events, workloads, namespaces, utilization, logs, DNS, services, ingress, network policies, kubelet pod counters, the Teams-to-contact list) are the same as in the other two versions.

## The GCP section (section 2)

- **Cluster:** status (RUNNING / RECONCILING / ERROR ...), mode (Autopilot / Standard), regional or zonal, control-plane and node versions, release channel and **available upgrades**, endpoint, **private nodes / private endpoint**, **master authorized networks** (none = INFO), dataplane (legacy or Dataplane V2) and network policy, VPC-native, Workload Identity, shielded nodes, legacy ABAC (HIGH), logging and monitoring configuration.
- **Node pools:** machine type, ready nodes vs what the managed instance groups want, autoscaler min / max (a pool at its maximum is flagged), zones, max pods per node, spot / preemptible / standard, version, status (a pool in ERROR or RUNNING_WITH_ERROR is HIGH), auto-repair.
- **Network:** the subnet's node range and the **pod and service secondary ranges**. GKE reserves a block of the pod range per node (/24 for 110 pods per node, /26 for 32, ...), so the report shows how many nodes the range can hold, how many the pools can grow to, and flags a pod range that cannot hold the scale-out (HIGH). Service range use, Private Google Access, **firewall rules** (SSH / RDP or all ports open to `0.0.0.0/0` is HIGH), and the **Cloud NAT** gateways (private nodes with no Cloud NAT is HIGH).
- **Node service account & IAM:** which service account each pool runs as, its project-level roles, the **default Compute Engine service account with Editor / Owner (HIGH)**, a disabled node service account (HIGH), a custom account without logging / metrics roles (INFO), and the GKE service agent role.
- **Add-ons**, **node VMs** (status RUNNING / TERMINATED / STOPPING ..., VMs of the cluster that never became nodes, nodes with no VM) and **managed instance groups** (not stable, and their recent errors such as QUOTA_EXCEEDED or stockouts).
- **GKE operations** on the cluster in the window (upgrades, repairs, resizes, failed operations).
- **Cloud Logging:** error-level control-plane / cluster log entries, **cluster autoscaler** events (scale up / down, "cannot scale up" and its reason, quota errors), Kubernetes audit **PERMISSION_DENIED / UNAUTHENTICATED** callers, and failed GKE admin API calls.

## Network & traffic (section 10)

Section 10 is written to be read by someone who does not know the abbreviations:

- **No short forms in headings or column headers.** Column headers use full words ("BYTES RECEIVED (SINCE BOOT)", "SUPPORT DISTRIBUTION LIST", "VIRTUAL PRIVATE CLOUD NETWORK", "NETWORK ENDPOINT GROUP", "LOAD BALANCER"); the same headers are used in the text report and the HTML (sorting, filtering, CSV and the charts work as before). Full words are also used in the other sections' column headers and section titles.
- **"What this table shows"** - one plain sentence under every table, and **"What this block shows"** under every block heading.
- **Glossary right before the block that uses a term** ("Glossary: what these terms mean": Term | Full name | Plain-language meaning, only the terms of that block, e.g. Container Network Interface (CNI), Domain Name System (DNS), Maximum Transmission Unit (MTU), network address translation (NAT), kube-proxy, iptables / IPVS, conntrack, NetworkPolicy, ClusterIP / NodePort, BackendConfig ...) and **one complete glossary at the end of the section**.
- **Every check is a titled block with a status** - **OK / Warning / Problem / Not available (with the reason)** - and a "What this means / what to do next" sentence. *Not available* is never shown as OK.

The blocks, in order:

| Group | Blocks |
|---|---|
| Overview | how to read the section; cluster network settings and the network components of kube-system |
| Pod-level networking | **Container Network Interface (CNI) plugin health** (Dataplane V2 `anetd` / Cilium, `netd`, `calico-node`, `ip-masq-agent`: readiness, versions, `cbr0` / bridge note); **IP address exhaustion** (node subnet, pod secondary range, service range: total / used / free / percent, plus pods per range); **CNI daemon logs** (failed allocations, "no IP addresses available in range set"); **pods stuck in `ContainerCreating` / `FailedCreatePodSandBox`**; network-related warning events; **CNI start-up order** (agent restarts / not ready, kube-proxy not ready, nodes tainted `node.cilium.io/agent-not-ready` or `NetworkUnavailable`) |
| Node networking | node `NotReady` and memory / disk / process / network-unavailable conditions; **node interface counters** (errors from the kubelet; drops are not reported by the kubelet) plus live traffic and top pods / namespaces; **Maximum Transmission Unit hints** (`gcloud compute networks describe` mtu, VPN-route mismatch note; node / pod interface MTU "cannot be read without node access"); **Cloud API throttling / quota evidence** (`RATE_LIMIT_EXCEEDED` / quota in Cloud Logging and in Kubernetes events) |
| kube-proxy and service routing | **kube-proxy health and mode** (iptables / IPVS from its ConfigMap, or "replaced by Dataplane V2"); kube-proxy log errors; **Services with no ready endpoints** (list, with the likely cause); ClusterIP services; **NodePort services and the node port firewall check** (is 30000-32767 allowed by a rule); LoadBalancer services |
| DNS | kube-dns / CoreDNS pods ready + restarts, Cloud DNS for GKE scope, kube-dns ConfigMap (`stubDomains`, `upstreamNameservers`) / CoreDNS Corefile summary; **DNS error logs**; **NodeLocal DNSCache** (present? recommended from 50 nodes); **`ndots` / search settings** from a sample of up to 300 pod specs (ndots:5 is an INFO finding; no pod exec) |
| Network policies and firewalls | NetworkPolicy objects per namespace (default-deny present?); **policy engine and enforcement** (Dataplane V2 / Calico, enabled on the cluster? policies without an engine are a Problem); **virtual private cloud firewall rules** (open to the internet, Google health check ranges, node port range, gke-* rules); **Cloud NAT, VPC peering and private cluster routing** (control-plane peering, authorized networks, Private Google Access, NAT gateways, peering state) |
| Load balancers and ingress | Ingress objects; **ingress controllers** (GKE managed `gce` / `gce-internal`, nginx pods + 502 / 503 / 504 counts from their logs, Gateway API objects); **BackendConfig and network endpoint group (NEG) annotations** (missing BackendConfig is a Problem); **load balancer backend health** (`backend-services get-health`); **TLS certificates** (TLS Secrets: expiry date from the public certificate only; Google-managed certificates: status and expiry) |
| Conntrack and port exhaustion | **connection tracking usage** from node-exporter (`node_nf_conntrack_entries` / `_limit`, read through the API server pod proxy; without node-exporter: "needs node-exporter or node access"); **Cloud NAT** address / port mapping and **port exhaustion / dropped packets** |
| Observability and packet capture | which network observability features are on (Dataplane V2 observability / Hubble, VPC Flow Logs on the subnet, Cloud Monitoring / Managed Prometheus, Connectivity Tests, Packet Mirroring); **how to capture packets** (guidance text only: toolbox + tcpdump, Hubble, Packet Mirroring, debug container; nothing is run) |
| Control plane / API server | **request throttling** (`apiserver_flowcontrol_rejected_requests_total`, HTTP 429 from `kubectl get --raw /metrics`); **admission webhooks** (failurePolicy, timeout, ready endpoints, recent events); **etcd health** (only if exposed) |
| Google Cloud routing and traffic | routes, static IPs, load balancer forwarding rules, then the **traffic over the selected window** (Cloud Monitoring, 1-minute points: node bytes received / sent, load balancer requests / server errors / latency, Cloud NAT drops and per-node port use, with charts in the HTML) |
| Checklist and glossary | **Traffic issue checklist** (10 rows: pod reachability evidence, CNI logs, node health, kube-proxy, DNS, network policies, cloud firewalls, load balancer health checks, packet capture availability, provider observability tools; columns Check / Result / Evidence found / What to do next - the result is the worst status of the blocks behind it); the complete glossary |

Strictly read-only: no pod exec, no node login, no packet capture, no change to the cluster or to Google Cloud. The new reads are `kubectl get` / `logs` (kube-system agents, kube-proxy, DNS, ingress controllers), `kubectl get --raw /metrics`, `/readyz/etcd` and the node-exporter pod proxy (`/api/v1/namespaces/N/pods/POD:PORT/proxy/metrics`), `kubectl get secret NAME -o jsonpath={.data.tls\.crt}` (the public certificate only, never the key), webhook / BackendConfig / ManagedCertificate / Gateway objects, and `gcloud compute networks describe`, `network-management connectivity-tests list`, `compute packet-mirrorings list` plus Cloud Logging reads.

## Permissions (read-only)

Without one of these, that part says why it is missing and the rest still runs.

- **GCP IAM** on the project (and the host project when the cluster uses a Shared VPC): `roles/container.viewer` (clusters, operations), `roles/compute.viewer` (instances, instance groups, subnets, firewall rules, routes, routers / NAT, forwarding rules, backend services and their health), `roles/logging.viewer` (the control-plane / autoscaler / admin-activity logs; Kubernetes **data access** audit entries need `roles/logging.privateLogViewer`), `roles/monitoring.viewer` (the traffic series), and something that allows `resourcemanager.projects.getIamPolicy` for the IAM section, for example `roles/iam.securityReviewer` (also gives `iam.serviceAccounts.get`). `resourcemanager.projects.list` is only needed for the project list / `--list-projects`; without it the project is taken from the nodes / context.
- **Kubernetes:** the same as the EKS version (`nodes/proxy` for kubelet stats, list on pods / nodes / events / namespaces, etc.). The extra network checks use `get` on secrets (certificate only), webhook configurations, BackendConfigs, ManagedCertificates and Gateways, `pods/log` in kube-system and the ingress namespace, `pods/proxy` for node-exporter, and the non-resource URLs `/metrics` and `/readyz/etcd`; without a permission that block says **Not available** with the reason.
- **Extra GCP reads for the network checks:** `roles/compute.viewer` (network MTU / peerings, packet mirrorings), `roles/networkmanagement.viewer` (Connectivity Tests list), `roles/logging.viewer` (rate-limit / quota log entries). `roles/cloudasset.viewer` (Cloud Asset Inventory) and `resourcemanager.projects.list` are used for the all-clusters list.

The script only runs `gcloud ... list / describe / get-*` commands, `gcloud config get-value`, `gcloud auth print-access-token`, and two read-only Google API queries (Cloud Monitoring `timeSeries.list` and Cloud Logging `entries.list`) with that token. The token is kept in memory only and is never printed or written to a report. The API helper refuses any other URL.

## Not verified against a real GCP project

I built and tested this against mocked `kubectl`, `gcloud` and Google API output (including a 300+ project listing for the all-clusters list and mock clusters for every network block), **not a real GKE cluster**. The `gcloud` JSON field names, the Cloud Monitoring metric names / label names (`compute.googleapis.com/instance/network/received_bytes_count`, `loadbalancing.googleapis.com/https/request_count`, `router.googleapis.com/nat/dropped_sent_packets_count`, `nat_allocation_failed`, `allocated_ports`, `port_usage`, ...) and the Cloud Logging filters follow the documented Google Cloud CLI and APIs, but **run it once on a real cluster and check each section**; the Cloud NAT per-VM port figures in particular are approximate. A call that fails prints its error in the report instead of stopping the run. Other assumptions: `gkelogin` behaves like `ekslogin` (see above); `gcloud container operations list` is read without a location filter; `gcloud monitoring time-series list` is not used (it is not generally available), the Monitoring API is called directly with the `gcloud` access token.

On Windows the Cloud Logging API is queried directly (a filter with quotes does not reliably survive `gcloud.cmd`); `gcloud logging read` is used as the fallback, and first elsewhere.

## Every section explains itself (HTML and text report)

The report is written for people who did not build the tool, so nothing is left unexplained and no short form is used as a heading:

- **"What this section shows"** box directly under every section heading (Health summary, sections 1 to 14, Collection steps and timing, Glossary and, on the multi-cluster summary page, Clusters and All findings): what data it holds, where it comes from (kubectl, gcloud, Cloud Monitoring, Cloud Logging), the time window it covers, how to read the colours and statuses, and a "How to use it" hint. The wording lives in the `SECTIONS` registry (`shows` and `how` fields), so the scheduler, the text report and the HTML use the same text. A section that was skipped by choice stays one line with no box.
- **"What this table shows"** (one specific sentence) above every table and **"What this block shows"** above every block (sub-heading, dashboard, chart group, timeline). `rep.table(..., about=...)`, `rep.sub(title, about, terms)`, `rep.util / series / timeline(..., about=...)` take it; a call without `about` is recorded in `MISSING_ABOUT` and the test suite fails, so nothing can be added without an explanation.
- **"What each column means"**: hover a column header (dotted underline) for a tooltip where the name alone is not obvious (`COLUMN_HELP`).
- **No short forms in headings or table headers**: `CPU`, `MEM`, `HPA`, `PVC`, `DL`, `RST`, `ID`, `IP`, `NAT`, `VPC`, `LB` ... are written out ("Processor (CPU) used", "Memory", "Horizontal pod autoscaler", "Support distribution list", "Restarts", "Instance identifier", "IP address", "Network address translation"). When a term has to stay inside a cell or a name (kube-proxy, kube-dns, IAM, Cloud NAT ...), a small **"Glossary: what these terms mean"** table (Term | Full name | Plain-language meaning, only the terms of that block) is printed right before the table or block that uses it.
- **Legends** of the severity levels (Critical / High / Medium / Information) and of the check results (OK / Warning / Problem / Not available) in the Health summary and in the glossary.
- **Complete glossary** at the very end of the HTML (linked from the table of contents) and of the text report: every term of every section, de-duplicated and sorted.
- The text report carries the same: a "What this section shows" line after every section title, a "What this table shows" line above every table, and the complete glossary at the end.

Test: the mock-cluster test generates the full report, `--only-networking`, a run with skipped sections and the multi-cluster page, and asserts that every collected section has its box, every table is immediately preceded by its "What this table shows" line (tables == explanation lines, no whitelist), no abbreviation of a deny-list stands in a heading or table header unless its full form is next to it, the complete glossary exists and is linked, and the text is identical for `--workers 1` and `--workers 8`.

## Reports

`reports\gke_debug_<cluster>_<time>.html` (interactive) and `.txt`; with several clusters also `reports\gke_debug_summary_<time>.html`. Pod logs and the report can contain sensitive data; share them carefully.

Settings at the top of the script: `LOOKBACK_MINUTES`, `MAX_LOG_PODS`, `LOG_TAIL_LINES`, `UTIL_WARN` / `UTIL_CRIT`, `SUPPORT_LABEL`, `TRAFFIC_SAMPLE_SECONDS`, `LOW_SUBNET_IPS`, `PARALLEL_WORKERS`, `KUBECTL_CONCURRENCY`, `GCLOUD_CONCURRENCY`, and the branding (`CLOUD_NAME`, `BRAND_PRIMARY`, `BRAND_ACCENT`, `BRAND_COLORS`).

Limits of the parallel collection: **Stop** cannot interrupt a gcloud / kubectl call that is already running (at most 6 + 4 of them finish; a log read can take up to its 60 s timeout); a section that did not start before Stop is listed as "skipped (stopped)". Progress-only lines of a section (for example "sampling live traffic") reach the live log as they happen, so they can appear before the section's own lines, which are shown (in report order) when the section is merged; the saved report is always in the fixed order. If the Tk build cannot draw emoji (older Python 3.9 builds) the plain fallback symbols are used. The branding and the section choice were checked with the mock harnesses and screenshots, not against a real cluster.

Limits of the network section: the node and pod interface MTU, the real `/etc/resolv.conf` of pods, conntrack without node-exporter, packet drops and `cbr0` need node or pod access and are reported as *Not available* / guidance; the kube-proxy mode is only known when its ConfigMap exists; TLS Secret expiry uses Python's own certificate decoder and reads only the first certificate of the chain; the API server counters are cumulative since it started (not limited to the report window); `CPU` is spelled "processor (CPU)" in headers, and a few product names that are themselves the name (kube-proxy, Cilium, NodeLocal DNSCache, BackendConfig) are kept and explained in the glossary.

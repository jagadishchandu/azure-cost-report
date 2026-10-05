# EKS Debugger

`eks_debug.py` uses the **AWS credentials that already exist in `~/.aws`** (no sign-in in this tool), connects to the EKS clusters you choose, and collects the basic debugging picture of **what happened and what is happening** in the last N minutes. Everything it runs is **read-only** and this is enforced by an allow-list (see "Read-only guarantee" below).

## Quick start

```powershell
cd C:\Users\jchandraprasad\Downloads\app\eks-debug
python eks_debug.py                      # opens the window: choose AWS profile(s), regions, clusters
python eks_debug.py --minutes 60         # same, default window 60 min
python eks_debug.py --list-accounts      # the profiles of ~/.aws with their credentials status
python eks_debug.py --list --profile dev,prod --region us-east-1,eu-west-1   # clusters of THESE profiles x regions only (nothing is scanned unless you name the scope)
python eks_debug.py --list --profile all --region all                        # every profile x every region - only because you asked for it
python eks_debug.py --profile dev --region us-east-1 --cluster 1             # no GUI: debug cluster #1 of that list
python eks_debug.py --profile dev,prod --region all --cluster 1,3,5          # several clusters, one after another (also 2-4, all, a cluster name)
python eks_debug.py --cluster 3 --skip-kubeconfig   # don't write the kubeconfig entry: use the current kubectl context as it is (--skip-login is the same)
python eks_debug.py --cluster 3 --context my-ctx    # force a specific kubectl context
python eks_debug.py --connect ekslogin --list       # the clusters your ekslogin.exe menu offers; --cluster N then runs ekslogin first
```

`kubectl` and the AWS CLI (`aws`) must be on PATH. No extra Python packages are needed (tkinter ships with Python). `ekslogin.exe` is only needed if you choose **Log in with ekslogin first** (put it in this folder, or pass `--ekslogin C:\path\ekslogin.exe`).

## Credentials: from `~/.aws`, no sign-in in this tool

This tool **never signs you in**. It uses what the `aws` CLI itself would use: `~/.aws/config` and `~/.aws/credentials` (honouring `AWS_CONFIG_FILE` / `AWS_SHARED_CREDENTIALS_FILE`), `AWS_PROFILE`, and the standard `AWS_ACCESS_KEY_ID` ... variables. Whatever your company's own process puts there - the SSO cache, a credentials file, `credential_process`, assumed-role profiles - just works through the aws CLI.

**Step 1 of the window lists every profile** found in `~/.aws/config` and `~/.aws/credentials` (plus the pseudo-entry **Environment / default credentials** when `AWS_ACCESS_KEY_ID` / `AWS_PROFILE` ... are set, or when there is no profile at all) with its profile name, account id, role / SSO info, region and a **credentials status chip**:

| chip | meaning |
|---|---|
| **Active** (green, `Active - 7h 19m left`) | `aws sts get-caller-identity --profile P` works. The time left is shown when it can be read locally: the expiry of the SSO token in `~/.aws/sso/cache`, or the expiration written next to temporary credentials (`aws_session_expiration` / `expiration` in the credentials file, `AWS_CREDENTIAL_EXPIRATION` for the environment) |
| **Expiring soon** (amber) | the same, with less than 30 minutes left |
| **Credentials expired - renew them outside this tool** (red) | the credentials / SSO session are expired |
| **Not configured: reason** / **Unknown: reason** (grey) | no credentials were found for the profile, or the check could not be completed (for example no network); the reason is the first line of what `aws` said |

The status is checked in the background with the read-only `aws sts get-caller-identity --profile P`, **4 at a time**, selected profiles first (up to 40 profiles are all checked on start; above that the selected ones, plus **Re-check all**). Buttons: **Check credentials** (the selected profiles, or all when none is selected) and **Re-check all**. The local files are read only for the non-secret fields (`startUrl`, `expiresAt`, account, region, role names): **access keys, secret keys, session tokens and SSO tokens are never read into the window, the log or a report.** The profile list is searchable and multi-select (Ctrl/Shift-click, **Select all (shown)**, **Clear**, "N selected"); **Reload profiles** reads the files again.

**When a selected profile has expired or missing credentials** the window shows a red message (and a **Copy example command** button - text only, nothing is run):

> Credentials for profile X are expired or missing. This tool does not sign in. Renew them the way your company does it (for example your company's login tool, or `aws sso login --profile X` in your own terminal), then press Re-check.

If an `ekslogin.exe` is available the message adds *Or choose 'Log in with ekslogin first' under 'How to connect to a cluster'.* After you renew the credentials, press **Check credentials** / **Re-check all**. On the command line the same sentence is printed for every non-working profile (`--list`, `--cluster`, `--list-accounts`); if no profile in scope works, the exit code is 1. Nothing prompts, nothing signs in.

## How to connect to a cluster

A small selector in step 1 (`--connect`):

- **Use my existing AWS credentials from ~/.aws (no sign-in)** - the **default**. For each selected cluster: check the profile's credentials (`aws sts get-caller-identity`), run `aws eks update-kubeconfig --name N --region R --profile P --alias A` (it writes only your **local** kubeconfig), then `kubectl config use-context`, then the normal report. A cluster whose profile turns out to be expired is marked **credentials expired** (with the renew message) and the next clusters still run.
- **Log in with ekslogin first** (`--connect ekslogin`) - runs your own `ekslogin.exe` with the cluster's **menu number** (exactly as before: the number is sent on stdin), then **re-reads `~/.aws`** and re-checks the credentials (ekslogin may have created or refreshed profiles), then continues. A cluster found with aws that is not in the ekslogin menu (or whose name exists in several accounts / regions, which is never matched to a menu entry) is connected with `aws eks update-kubeconfig` and its own profile instead.

`--skip-kubeconfig` (alias `--skip-login`) skips this step: no ekslogin, no kubeconfig entry; the kubectl context is matched by the cluster name / `--context` as before.

## Read-only guarantee

**This tool only reads.** It does not install, create, change or delete anything on the cluster or in the cloud account, it never runs a command inside a pod or on a node, and it never installs anything on your machine (no `pip`, package manager, download or `aws configure`). **The only things written locally** are the reports and the kubeconfig entry / current context (`aws eks update-kubeconfig`, `kubectl config use-context`).

This is **enforced in code, not just promised**: every command goes through an allow-list *before* a process is started. A command that is not on the list is refused (`blocked: read-only mode - '<verb>' is not allowed`), **no process is started**, the attempt is recorded, and the report shows it in its **Read-only guarantee** block with a CRIT finding. Every report ends with: *"This tool only reads ... N read call(s), 0 blocked."* (real counts from the guard); the window shows the same note in step 1, the banner and the status bar. The guard sits in `kubectl()`, `kjson()`, `aws_cli()`, the parallel cache layer and the local-command helper.

Allowed, and nothing else:

| tool | allowed |
|---|---|
| `kubectl` | `get` (including `get --raw <path>` as a plain GET; never with `-f` / `--data` / `-X`), `logs`, `top`, `version`, `api-resources`, `api-versions`, `cluster-info`, `explain`, `auth can-i`, `config get-contexts` / `current-context` / `view` |
| `aws` | exactly: `sts get-caller-identity`; `eks describe-addon / describe-cluster / describe-fargate-profile / describe-nodegroup / list-access-entries / list-addons / list-clusters / list-fargate-profiles / list-nodegroups`; `ec2 describe-flow-logs / describe-instance-status / describe-instance-types / describe-instances / describe-nat-gateways / describe-network-acls / describe-network-interfaces / describe-regions / describe-route-tables / describe-security-groups / describe-subnets / describe-traffic-mirror-sessions / describe-vpc-endpoints / describe-vpcs`; `elbv2 describe-load-balancers / describe-target-groups / describe-target-health`; `elb describe-instance-health / describe-load-balancers`; `iam list-attached-role-policies`; `cloudwatch get-metric-statistics`; `logs filter-log-events` (the table is `READ_ONLY_CLOUD_COMMANDS` at the top of the script) |

**Local-only exceptions** (they never touch the cluster or the cloud account's resources; they are marked `LOCAL-ONLY` in the code): `aws eks update-kubeconfig` (writes only your **local** kubeconfig), `kubectl config use-context` (switches the local current context), and - only when you choose **Log in with ekslogin first** - your own `ekslogin.exe`. There is **no sign-in code** in this tool: no device code, no browser flow, no terminal helper; `aws configure` and every other `aws sso` command are refused by the guard, and the renew message above is the only place a command is mentioned (as text).

Everything else - `apply`, `create`, `run`, `exec`, `debug`, `delete`, `patch`, `replace`, `edit`, `label`, `annotate`, `scale`, `rollout`, `cordon`, `drain`, `taint`, `cp`, `attach`, `port-forward`, `set`, `expose`, any other binary, and every aws command with a create / delete / update / put / modify / start / stop / run / terminate / tag ... verb - is refused. There are no wildcards on mutating verbs.

## Faster collection: parallel after the kubeconfig step (`--workers N`)

After the login the slow, independent read-only calls run **at the same time**: the cluster data, the AWS reads (per node group, per add-on, per load balancer, every CloudWatch query ...), the per-node kubelet statistics, the pod logs and the live traffic sample (`--traffic-sample`) all run side by side. Defaults: `PARALLEL_WORKERS = 8`; at most `KUBECTL_CONCURRENCY = 6` kubectl and `AWS_CONCURRENCY = 4` aws calls at the same moment; AWS throttling errors are retried with a growing wait (`AWS_RETRIES`). `--workers 1` runs everything one after another, exactly as before.

How it stays correct: every call goes through a thread-safe cache keyed by the command, "run-ahead" tasks start the calls of the sections ahead of the report, and the ordinary section code then writes the report **in the same fixed section order** - so the `.txt` report is identical for `--workers 1` and `--workers 8` (only the timing lines differ). One failing call or task never aborts the others. **Stop** cancels the pending calls (only the few calls already running finish). The window shows "x of y collection tasks done"; the end of the run (and the report footer) has a **timing summary** (total + per step). In the test mock with 0.15-0.30 s per call the full run took about 19 s sequentially and 4 s with 8 workers.

## Choosing what to collect: sections

The report has 14 sections (`python eks_debug.py --list-sections`): overview (always), AWS control plane and infrastructure, nodes, resource utilization, pods on each node, namespaces, unhealthy pods, warning events, workloads, network and traffic, autoscaling and storage, top consumers, pod logs, timeline.

- **Window:** the **What to collect** tab has one check box per section (icon, full title, one-line description), the buttons **Select all**, **Clear all**, **Only networking**, **Everything except networking**, and a counter such as "11 of 14 sections selected". The selection is remembered for the session, the panel is locked while a run is active, and a run with nothing ticked is refused.
- **Command line:** `--sections a,b,c`, `--skip-sections x,y`, `--only-networking`, `--no-networking`, `--list-sections`. `--no-aws` and `--no-logs` are aliases that untick the `aws` / `logs` sections.
- **Unticked sections are never collected** (no command runs for them; the kubectl objects only they need are not read either). If a ticked section needs the data of an unticked one (network needs `aws`, logs needs `pods`) the data is read silently, the unticked section is not shown, and a small note says so (`--no-aws` is respected: then it is not read).
- **Report:** a skipped section is one line, `Skipped by choice: <section>`, in the text and in the table of contents; the health summary and counters cover only the collected sections; the network checklist is absent when networking is skipped; the multi-cluster summary shows "N of 14 sections collected" per cluster.

## Look of the window and the report

The window is branded for Amazon Web Services: a banner drawn from tkinter primitives (stylised cloud with the orange smile arrow, the Kubernetes helm, soft decorative clouds), the title *Amazon Elastic Kubernetes Service (EKS) Debugger*, a subtitle with the window minutes, where the credentials come from (`Uses your existing AWS credentials from ~/.aws - no sign-in in this tool`) and the profile count, AWS navy / orange styling, cards with a coloured stripe, striped lists, status chips and a footer status bar, in three tabs so that it is usable at 1100 x 700. Symbols fall back to plain text when the font cannot draw them. The HTML report has the same header band (inline SVG logo), accent colours and section icons. All branding is data-driven from constants at the top of the script (`CLOUD_NAME`, `BRAND_PRIMARY`, `BRAND_ACCENT`, `LOGO_SHAPES`).

## Collecting clusters on demand (selected profiles x regions)

Nothing is listed automatically (listing every profile x every region would be slow). **You choose the scope, then you press a button.**

**Step 2 - Choose regions and collect clusters.** Optionally type the **Region(s)** (several, comma separated), or tick **All regions** (`all`; reads the enabled regions with `aws ec2 describe-regions`); blank = each selected profile's own region. The estimate `N profile/region pair(s) selected (P profiles x R regions)` is always visible. Then press **Collect clusters from selected profiles**. Until then step 3 shows `Select one or more profiles in step 1, then press 'Collect clusters from selected profiles'.` and `aws eks list-clusters` is not called at all.

- Only profiles whose status is **Active / Expiring soon / Unknown** are collected; **Expired** and **Not configured** profiles are **skipped with a note** (and the red renew message stays visible). If every selected profile is skipped, nothing is collected.
- One `aws eks list-clusters` per **selected** profile + region, run **in parallel (16 at a time)**, results added **incrementally**, **de-duplicated** (the same account + region + cluster name seen through two profiles appears once). Progress reads `Listing clusters: 12/40 (profile, region)` with the elapsed time; **Stop** cancels and keeps what arrived.
- **Per-scope cache:** every profile/region pair that was collected is remembered, so pressing the button again fetches **only the pairs not collected yet**; **Refresh selected** collects the selected pairs again.
- Above 20 pairs the window asks first (`This will search 300 profile/region pairs and can take several minutes. Continue?`).
- A lookup that fails (no access, region not enabled, credentials expired ...) is **logged and skipped**; a profile found expired while collecting is marked Expired and the collection continues. At the end one line sums it up, for example: `Found 57 clusters in 12 accounts, 3 accounts failed: acc03, acc07, acc11; 1 region lookup failed and was skipped (15 accounts / profiles, 255 region lookups).`

With **Log in with ekslogin first** there is also the source **Clusters from the ekslogin menu (instant)** (the `CLUSTERS` dict, `clusters.json`, or the menu `ekslogin.exe` prints - see "Where the cluster list comes from"): shown at once, no cloud call. A collected cluster whose name matches an ekslogin menu entry uses the ekslogin number; any other uses `aws eks update-kubeconfig` with its own profile and region.

**Step 3 - Choose clusters.** A searchable multi-select list with the columns **Cluster**, **Account / Profile**, **Region** and **Connect with** (`aws`, `ekslogin #3`, `aws (not in ekslogin menu)`). Search: type in the box next to the magnifier (case-insensitive; several words must all match); **x** clears it; "Showing X of Y"; your selection is kept while you filter; **Select all (shown)** and **Clear**; "or type numbers" still works (`1,3,5`, `2-4`, `all`). The action bar button **Login & Debug selected cluster(s)** runs the clusters one after another (see "Choosing clusters"). The last selected profiles and regions are remembered for the session.

**Command line.** `--list` and `--cluster` search **only the scope you name**: `--profile a,b,c` or `--profile all`, and `--region r1,r2` or `--region all`. With no scope only the default profile (what the aws CLI would use) and its own region are searched. The scope is printed first (`Searching for EKS clusters in: 2 profiles (dev, prod) x us-east-1, eu-west-1.`). Profiles whose credentials are expired or missing are skipped with the renew message; if none works the exit code is 1. `--cluster` numbers refer to the collected list:

```
python eks_debug.py --list --profile dev                                  # one profile, its own region
python eks_debug.py --list --profile dev,prod --region us-east-1,eu-west-1   # 2 x 2 pairs only
python eks_debug.py --profile dev --region us-east-1 --cluster 12
python eks_debug.py --profile all --region all --cluster all --minutes 15
python eks_debug.py --connect ekslogin --all-clusters --profile dev,prod --cluster prod-eks   # menu cluster -> ekslogin; any other -> aws eks update-kubeconfig
```

`--list` prints `N - name (region/profile)   [account 123456789012, profile NAME]` (with `--connect ekslogin`: `, connect: ekslogin #3`). A name that exists in several accounts / regions is **never** matched to an `ekslogin` menu entry (that could log in to the wrong cluster).

**In the window** the top is a numbered, three-step layout with a message line at the bottom (it always says what happened and what to do next). A cluster of another profile than the one selected in step 1 always uses **its own** profile for the run.

**Prerequisites:** `kubectl`, the AWS CLI (`aws`), and working credentials in `~/.aws` (for SSO profiles: a profile with `sso_session` / `sso_start_url` in `~/.aws/config`, signed in by your company's own process).

## Live collection (GUI) and the interactive HTML report

**While it collects (GUI)**
- A **step checklist** (kubeconfig entry / ekslogin, context, each section) ticks through in real time with the time each step took, and a **progress bar** and elapsed timer run along the bottom.
- **Findings appear live** with CRIT / HIGH / MED / INFO counters as they are discovered, before the run is finished.
- The log fills in as each section completes. Problems are coloured.
- **Stop** ends the run after the current step. The remaining steps are skipped and a **partial report is still saved**.
- Tick what to collect in the **What to collect** tab (one box per section; the older **AWS details** / **Pod logs** boxes are the `aws` / `logs` sections), and **Open report when done** to open the HTML automatically. **Open HTML report** and **Open reports folder** buttons are at the bottom.

**The saved reports** (`reports\eks_debug_<cluster>_<time>.html` and `.txt`)
- The `.html` is **one self-contained file** (no internet, no libraries), so you can email or archive it. It has:
  - a **health summary**: clickable Critical / High / Medium / Information cards that filter the findings list, each finding linking to its section;
  - a **side menu** with a badge per section, and **collapsible sections** (Expand all / Collapse all);
  - **sortable tables** (click a header; numbers and units like `1950Mi` or `90%` sort correctly), a **filter box per table**, **CSV export**, usage **bars** for CPU/memory/disk percentages, and status words coloured red/amber/green;
  - **all rows** of every table (the text report truncates long tables);
  - collapsible **pod logs** with an "errors only" switch;
  - a **timeline** you can filter by type (NODE, POD, EVENT, ROLLOUT, JOB, CONTROL PLANE);
  - a **global search** across everything (press `/`), **dark/light** theme, Print, and **Download .txt**.
- Reports are never overwritten: a second run in the same second gets a `_2` suffix.
- Command line: `--open` opens the HTML when done, `--no-logs` / `--no-aws` skip those steps (see "Pod logs" for `--logs-all`).
- Pod logs can contain sensitive data, and so can the HTML (it embeds everything). Share it carefully.

### Every section and table explains itself (HTML and text)

- **Every section** of the report - Health summary, the 14 collection sections, Collection steps, Timing summary, Read-only guarantee, the closing Glossary, and both parts of the multi-cluster summary page - starts with a **"What this section shows"** box (what data it contains, where it comes from - kubectl, AWS, CloudWatch -, the time window it covers) and a **"How to use it"** hint (colours, what to click). The wording lives in the `SECTION_TEXT` entries of the `SECTIONS` registry (fields `shows`, `how`, `terms`), so the HTML and the text report always agree. A section that was skipped is still one line and has no box.
- **Every table** has a one-line **"What this table shows"** directly above it, and every block heading (and every chart, log list, timeline and the dashboard parts) a **"What this block shows"** line. In the code the helpers take `about=` (`rep.table(..., about=...)`, `rep.subhead(title, about=...)`, `rep.series(..., about=...)`, `rep.timeline(..., about=...)`, `rep.util(..., about=...)`); a table or block written without it is recorded in `Report.unexplained` (also `run_debug(...).unexplained`) and never hidden - the test suite fails on it.
- **Column headers are written in full** (for example "Processor (CPU) used", "Memory requested", "Persistent volume claim", "Horizontal pod autoscaler", "Support team contact", "Availability zone", "Instance identifier"); hover a header for **"What each column means"**.
- **Words and colours:** the Health summary and the closing glossary carry a legend for the severities (**Critical**, **High**, **Medium**, **Information**; shown as `CRIT` / `HIGH` / `MED` / `INFO` in the text report) and for the check statuses (**OK**, **Warning**, **Problem**, **Not available**).
- **Glossary:** an unavoidable term (EC2, IAM, kube-proxy, CoreDNS, HPA, PVC, OOMKilled ...) is explained in a small **"Glossary: what these terms mean"** table (Term | Full name | Plain-language meaning) before the first block of that section that uses it, and **one complete glossary at the very end** of the report lists all terms of the whole report (de-duplicated, sorted); the table of contents links to it. The `.txt` report has the same section intro lines, table description lines and closing glossary.

## Network & traffic (section 10)

Section 10 answers the standard traffic-troubleshooting questions. **Every check is its own block** with a **status** - `OK`, `Warning`, `Problem` or `Not available` (with the reason; a check is never OK when its data could not be read) - and a **"What this means / what to do next"** sentence. Under every heading there is a one-line explanation (**"What this block shows"**, and **"What this table shows"** above each table). Everything is read-only: no `kubectl exec`, no SSH to nodes, no packet capture.

**Plain words, no short forms.** Headings and table column headers are written in full (for example "Bytes received", "Security group", "Elastic network interface", "Availability zone", "Support team contact"; the same expansion is applied to every table in the report, in HTML and text). Where an abbreviation or technical term cannot be avoided (CNI, DNS, MTU, NAT, SNAT, VPC, ENI, kube-proxy, iptables, IPVS, conntrack, NetworkPolicy, ClusterIP, NodePort, ALB, NLB ...), a small **"Glossary: what these terms mean"** table (Term | Full name | Plain-language meaning) is printed **immediately before** the block that uses them (only those terms), and one **complete glossary closes the section**. Sorting, filtering, CSV export and the charts work as before.

| Block | What it checks (status from the data collected) |
|---|---|
| **Cluster network settings** | service range and IP family, DNS service address, pod ranges, kube-proxy mode, CoreDNS readiness / upstreams, network plugin |
| **Container Network Interface plugin health (Amazon VPC CNI)** | `aws-node` DaemonSet ready / desired, version, the settings `WARM_ENI_TARGET`, `WARM_IP_TARGET`, `MINIMUM_IP_TARGET`, prefix delegation, custom networking, `ENABLE_POD_ENI` with an **assessment per setting** (flags e.g. `MINIMUM_IP_TARGET` without `WARM_IP_TARGET`, custom networking without an ENIConfig selector) |
| **IP address exhaustion and per-instance network limits** | free IPs per subnet (Problem under 10, Warning under 50) and pods per subnet; per node the pod limit from the instance type's network interfaces (`ec2 describe-instance-types`: interfaces x (IPs - 1) + 2, x16 with prefix delegation) against the kubelet's `maxPods` and the pods running (node FULL = Problem) |
| **Container Network Interface daemon logs (L-IPAMD)** | `aws-node` logs in the window: failed to assign an IP address, network interface errors, **`RequestLimitExceeded` throttling**, other errors |
| **Pods stuck creating** | pods Pending on a node for over 2 minutes (ContainerCreating) and `FailedCreatePodSandBox` / network plugin events |
| **Network start-up order** | per node `aws-node` and `kube-proxy` ready / restarts, kube-proxy ready before aws-node, `NetworkUnavailable` |
| **Network-related warning events** | Warning events that mention networking |
| **Node conditions** | NotReady, `MemoryPressure`, `DiskPressure`, `PIDPressure`, `NetworkUnavailable` |
| **Node and pod network counters** | kubelet counters (needs `nodes/proxy`): bytes received / transmitted, interface **errors** (the kubelet does not report dropped packets), live rate sample, top pods and namespaces |
| **Maximum Transmission Unit hints** | `AWS_VPC_ENI_MTU` / `POD_MTU` from the CNI settings (flags pod MTU above the interface MTU); otherwise "cannot be read without node access" - the tool never connects to a node |
| **Cloud API throttling evidence** | `RequestLimitExceeded` / throttling in the CNI logs and events |
| **kube-proxy health, mode and logs** | DaemonSet ready / desired, mode (iptables / IPVS from `kube-proxy-config`), error and rule-sync lines in its logs, iptables mode with 1000+ Services |
| **Services** | counts by type, **Services with no ready endpoints** (listed), ClusterIP / NodePort / LoadBalancer / ExternalName with address and exposure (internet-facing LoadBalancers are an INFO finding), **node ports in use against the security group rules** (the 30000-32767 range) |
| **Domain Name System (DNS)** | CoreDNS pods ready / restarts, Corefile summary (forwarders, `cache`, `loop`, `ready` ...), DNS error logs (SERVFAIL, REFUSED, timeouts, NXDOMAIN), **NodeLocal DNSCache** present? (recommended and an INFO finding for large clusters: 50+ nodes or 1000+ pods), how pods are set to resolve names (`ndots:5` default vs explicit, read from the pod specs; an INFO finding when pods use the default `ndots:5` and the NAT gateways carried 1 GiB+) |
| **Network policies and the policy engine** | policies per namespace, default-deny present, engine detected (VPC CNI network policy agent, Calico, Cilium ...); policies without an engine = Warning (not enforced) |
| **Cloud firewalls** | **security group rules** and **network ACLs of the cluster subnets** (`ec2 describe-network-acls`): rules in order, probes for HTTPS, DNS and return traffic on high ports in both directions (a blocked probe = Problem, explicit deny rules = Warning) |
| **Load balancers and ingress** | ingress controller pods (ready, restarts), **502 / 503 / 504 counts** and errors in their logs, **ingress backends** (missing Service / no ready endpoint), **TLS certificate expiry** read from the referenced Secrets (only the public certificate; expired = Problem, under 30 days = Warning), **cloud target health** (ALB / NLB target groups, classic instance health) |
| **VPC addressing, routing, NAT, endpoints / Elastic network interfaces / Traffic in the window** | as before (see below) |
| **Connection tracking and port exhaustion** | `node_nf_conntrack_entries` / `_limit` read through the Kubernetes API proxy **only if node-exporter pods exist** (70%+ Warning, 90%+ Problem), else "needs node-exporter or node access"; **NAT gateway port allocation errors** (`ErrorPortAllocation`) from CloudWatch; the VPC CNI SNAT setting |
| **Network observability** | Container Insights, **Container Network Observability / CloudWatch Network Flow Monitor** (add-on or DaemonSet), **VPC Flow Logs** on the VPC |
| **Packet capture options (guidance only)** | a table of read-only commands and tools (flow logs, Traffic Mirroring, tcpdump through Session Manager, `kubectl debug node` ...) with whether each changes anything. The tool runs none of them; it only reports whether a Traffic Mirroring session exists |
| **API server request throttling** | `apiserver_request_total{code="429"}` share and `apiserver_flowcontrol_rejected_requests_total` from `kubectl get --raw /metrics` (totals since the API server started) |
| **Admission webhooks** | every validating / mutating webhook with failure policy, timeout and target; a webhook set to `Fail` whose service has no ready pod = Problem; "failed calling webhook" events |
| **etcd health** | the `/readyz/etcd` probe (etcd is managed by AWS and otherwise not visible) |
| **Traffic issue checklist** | **10 rows** - pod reachability evidence, CNI logs and health, node health, kube-proxy, DNS, network policies, cloud firewalls, load balancer health checks, packet capture availability, provider observability tools - with columns **Check / Result (OK, Warning, Problem, Not available) / Evidence found / What to do next**, filled from the blocks above |

**AWS network and traffic (needs the AWS section)**
- VPC address ranges (including secondary), **per-subnet routing** (public via internet gateway / private via NAT / **isolated with no default route**, a HIGH finding), NAT gateways and their state, **VPC endpoints** (and which common ones are missing for isolated subnets).
- **Node network interfaces and IPs** against the pods using VPC IPs.
- **Traffic in the selected window** (from CloudWatch, the same last-N-minutes as everything else): nodes (EC2 NetworkIn / NetworkOut, average / peak / total, one point per 5 minutes with basic monitoring), load balancers (requests, 2xx / 4xx / 5xx, **5xx error rate** - 1%+ MED, 5%+ HIGH - target response time, bytes) and NAT gateways (bytes, dropped packets, **port allocation errors** = HIGH). In the HTML each is a **chart card**; hover a point for its time and value.
- Live pod / node counters use `--traffic-sample SECONDS` (default 10, 0 = skip). The totals are *since the pod or node started*, not the selected window.

Permissions used (read-only): `ec2:DescribeVpcs`, `DescribeRouteTables`, `DescribeNatGateways`, `DescribeVpcEndpoints`, `DescribeNetworkInterfaces`, `DescribeNetworkAcls`, `DescribeSecurityGroups`, `DescribeInstanceTypes`, `DescribeFlowLogs`, `DescribeTrafficMirrorSessions`; `elasticloadbalancing:DescribeLoadBalancers`, `DescribeTargetGroups`, `DescribeTargetHealth`, `DescribeInstanceHealth`; `cloudwatch:GetMetricStatistics`; `eks:ListAddons`. Kubernetes reads: `get/list` on pods, nodes, events, services, endpoints, ingresses, networkpolicies, daemonsets, configmaps (`kube-proxy-config`, `coredns`), validating / mutating webhook configurations, `get` on the Secrets that ingresses name (only `tls.crt` is used), `pods/log`, `nodes/proxy` (kubelet counters) and `get` on `/metrics`, `/readyz/etcd` and the node-exporter pod proxy. Without one of them, that block says **Not available** with the reason and the rest still runs.

Not included: **pod-to-pod / per-connection traffic** (that needs VPC Flow Logs analysis or a service mesh), **per-pod traffic over the window** (Container Insights / Network Flow Monitor), and anything that needs a command on a node or in a pod (real interface MTU, `conntrack -S`, packet capture).

## Node servers: the EC2 instance behind every node

Wherever a node name appears, the report also shows the **actual server** behind it. The **Node inventory** table (section 3) is the same data as

```powershell
kubectl get nodes -o custom-columns=NAME:.metadata.name,INSTANCE-ID:.spec.providerID,ZONE:.metadata.labels."topology\.kubernetes\.io/zone",TYPE:.metadata.labels."node\.kubernetes\.io/instance-type"
```

with these columns: **NODE, INSTANCE-ID** (the `i-...` id, parsed from `spec.providerID`), **EC2 NAME** (the instance's `Name` tag), **ZONE, TYPE, CAPACITY** (on-demand / spot), **NODEGROUP, INTERNAL IP, STATUS** and the raw **PROVIDER-ID**.

- The instance id also appears in the live-usage and scheduling tables, in node events and in the timeline (`NODE name [i-0abc]: ...`), in the heading of each node's pod list (`NODE name [i-0abc | us-east-1a | m5.xlarge | EC2 name]`), in the NODE column of unhealthy pods (`name [i-0abc]`), in node findings (`Node name [i-0abc] is NotReady`), in the EC2 status table, and on each node card of the utilization dashboard.
- **EC2 NAME needs AWS access** (`ec2:DescribeInstances`, read when the AWS section runs). Without it the name shows `-`; the instance id, zone and type still come from `kubectl`.
- For a NotReady node the report prints the ready-to-run `aws ssm start-session --target i-...` command.

## Who to contact: the namespace support team contact (DL)

Each namespace's **support team** comes from the label **`elvh-app-support-dl`**, the same data as

```powershell
kubectl get namespaces -l elvh-app-support-dl -o custom-columns=NAME:.metadata.name,SUPPORT_DL:.metadata.labels.elvh-app-support-dl
```

- A **SUPPORT DL** column sits next to the namespace in **every table that lists namespaced objects**: namespaces, resources, workloads, quotas, unhealthy pods, pods on each node, events, rollouts, failed jobs, HPAs, volumes, services, top consumers and the logs overview. The utilization dashboard shows `support: <DL>` under each namespace (you can filter by it).
- The namespaces section starts with a **"Who to contact for each namespace"** table (NOT SET when a namespace has no label). Namespaces with pods but no label raise an INFO finding, because nobody can be contacted for them.
- Findings about a namespace end with `(support: <DL>)`.
- **Teams to contact** (at the top of the HTML summary and the end of the `.txt` summary): the namespaces that have problems, **grouped by support DL**, with how many issues each has and what they are (pods not running or unhealthy, configured pods missing, failed jobs, unbound volumes, quota almost full, pods at 90%+ of a limit). A team can be sent exactly its own list. Namespaces without the label are listed together as "(no support DL label)".
- A different label? `--support-label my-label`, or change `SUPPORT_LABEL` at the top of the script.

## Resource utilization dashboard (processor and memory, by namespace)

Section 4 of the HTML report shows **high CPU and memory use clearly, organised by namespace**. The `.txt` report gets a ranked table of the same numbers.

- **Cluster gauges**: CPU and memory used against what the nodes can allocate, with a tick for what pods have *requested*, and pods running against the nodes' total pod capacity.
- **Who uses the cluster**: a stacked bar of each namespace's share of CPU and of memory (the 10 biggest plus "others"), with a legend. Hover a segment for exact numbers.
- **Nodes**: one card per node, sorted by the most loaded, with gauges for CPU, memory, disk and pod count. A node at 75%+ gets an amber outline and 90%+ a red one. Swap in use is flagged.
- **By namespace** (the main view):
  - Switch **CPU / Memory / Disk** and **Used / Requested**, sort by highest value, **closest to limit**, share of cluster, restarts or name, filter by name, or show only namespaces that have a pod at 75%+ of its limit.
  - Each namespace is a bar of what it uses, with a dark tick for its request and a red tick for its limit. Red and amber badges count its pods at 90%+ and 75%+ of their limit.
  - **Click a namespace to open its pods**: each pod has its own bar (used, with request and limit ticks), `used / request / limit`, **% of its limit** (red at 90%+, amber at 75%+), restarts, node and status.
  - If only some pods in a namespace have a limit, the namespace limit is not summed (it says "set on 2 of 3 pods") so it can't mislead.
- **Top consumers**: top pods by CPU and by memory, **closest to memory limit (OOM risk)**, **closest to CPU limit (throttling)**, and pods using more memory than they requested. Pods without a limit are shown in blue.
- **No live metrics?** If neither the kubelet stats nor metrics-server are available, the dashboard says so and switches to what pods **request and are limited to**, so it is still useful.

Thresholds are `UTIL_WARN` (75) and `UTIL_CRIT` (90) at the top of the script.

## Namespaces: pods used versus configured

Section 5 answers "how many pods is each namespace using, and how many is it supposed to have?".

- **PODS / RUNNING / PENDING / FAILED / COMPLETED**: what is in the namespace now (pods used).
- **CONFIGURED**: the pods the namespace is set up to run: the **desired replicas** of its Deployments, StatefulSets and DaemonSets, plus any standalone pods (each counts as one). Jobs are not counted here because they come and go; their pods show under COMPLETED / PODS.
- **STATUS**: `OK`, or `N missing` when fewer configured pods are running than configured. A missing pod raises a finding in the health summary (HIGH if none of the namespace's pods are running).
- **POD QUOTA**: pods used / limit from the namespace's ResourceQuota, when it has one. The separate **Resource quotas** table lists every quota (pods, CPU, memory, storage...) with a usage bar; 75% or more is flagged, 90% or more is HIGH.
- **Resources per namespace**: live CPU / memory / disk use (needs the kubelet stats or metrics-server, otherwise `n/a`), requested CPU / memory, and restarts.
- **Workloads**: every Deployment / StatefulSet / DaemonSet with desired, ready, available and running counts and its HPA min-max (and the current replica count).
- The summary line also shows the **cluster's pod capacity**: pods running or pending against the sum of what the nodes allow (each node has a maximum pod count).

In the HTML all of these are sortable and filterable (for example, sort by CONFIGURED or filter on `missing`).

## Pod logs

Logs are read **in parallel** with `kubectl logs --since=<window> --tail=200 --timestamps` (capped at 256 KB per container) for:

1. **Unhealthy pods**: crash loops, not ready, restarts, evicted, image pull errors (up to 20 pods).
2. **Pods named in Warning events** during the window.
3. **Core add-ons**: coredns, aws-node, kube-proxy, EBS/EFS CSI, metrics-server, autoscalers, load-balancer controller, external-dns, cert-manager, fluent-bit and similar (up to 12 pods).
4. **Every running pod** when you tick **Logs of ALL pods** (or use `--logs-all`). Capped at 60 pods, and you can limit it to namespaces: GUI box "only namespaces", or `--log-namespaces prod,payments`.

For a container that has restarted, the **previous** container's log is read as well (that is usually where the crash reason is). Up to 3 containers per pod, starting with the ones that are not ready or have restarted. Pending pods have no logs and are skipped; a container that can't be read shows the reason (for example "waiting to start") instead of disappearing.

**In the HTML**
- An **overview table** first: pod, container, current/previous, why it was collected, line count, **errors**, **warnings** and the last line. Sort it (for example by ERRORS) to find the noisy pod.
- Then one **collapsible log per container** with **all** the lines that were read. Logs with errors are open by default.
- Error-like lines are red and warnings amber. Each log has **errors only**, **no wrap** and **Copy**. The global search (`/`) searches log lines too, and **Expand all** opens every log.
- The `.txt` report keeps only the error lines and the last few lines per container, so it stays readable.

Error-like lines in an unhealthy pod also appear as a **finding** in the health summary, with the latest error as an example.

```powershell
python eks_debug.py --cluster 3 --logs-all                       # logs of every running pod (capped)
python eks_debug.py --cluster 3 --logs-all --log-namespaces prod  # only the prod namespace
python eks_debug.py --cluster 3 --log-lines 500                  # up to 500 lines per container
python eks_debug.py --cluster 3 --no-logs                        # skip logs
```

Stop ends log reading too: reads already running finish, the ones not yet started are skipped.

## The time window

`LOOKBACK_MINUTES = 30` at the top of `eks_debug.py` is the global default. Override it with `--minutes N`, or with the "Last (minutes)" box in the window. The window applies to events, restarts, node changes, rollouts, failed jobs, the timeline and the pod logs (`--since`).

## Which kubectl context is used

After the kubeconfig entry is written (or ekslogin succeeded), the script **switches kubectl to the selected cluster** (`kubectl config use-context <ctx>`) and then pins every later `kubectl` call to that context with `--context`, so a different "current" context can never send the checks to the wrong cluster. The line `kubectl context switched: <old> -> <new>` shows what happened, and the report prints the context at the top.

- The context is matched from the selected cluster's name, against both the context name and the cluster it points to (EKS contexts are usually the cluster ARN, or an alias equal to the cluster name): exact match first, then `.../<name>`, then "contains".
- **Several contexts match** (e.g. `prod-eks` matches `prod-eks-use1` and `prod-eks-use2`): it does **not** guess. It warns and keeps the current context. Choose with `--context NAME`.
- **No match**, or you typed only a cluster number: it warns and uses whatever context is current. Check the context shown at the top of the report, or pass `--context NAME`.
- `--context NAME` always wins over matching.

## Choosing clusters (one or several)

**In the window**, the **Clusters** box is a list you can select several clusters in:
- click one, **Ctrl-click / Shift-click** for more, **Select all (shown)** for everything in view, **Clear** to start over;
- the **search box** (magnifier) narrows the list live - "Showing X of Y"; your selection is kept while you filter;
- or **type numbers** into "or type numbers": `1,3,5`, `2-4` or `all`. Typed numbers are added to the clicked ones, and a number that isn't in the list still works;
- "Selected N: ..." under the box always shows exactly what will run.

**On the command line:** `--cluster 3`, `--cluster 1,3,5`, `--cluster 2-4` or `--cluster all`.

**Several clusters run one after another**, not at the same time: the kubeconfig entry, the kubectl context and the AWS profile are shared, so they can't overlap. For each cluster the script writes the kubeconfig entry (`aws eks update-kubeconfig`, or ekslogin), switches the kubectl context, picks the AWS profile, collects everything, and saves that cluster's own `.html` and `.txt`. Then:
- **A summary page** (`reports\eks_debug_summary_<time>.html`) is written, with one row per cluster (status, CRIT/HIGH/MED/INFO counts, time, its top finding, a link to its report) and **all findings from all clusters in one sortable, filterable table** (each links straight to the section in that cluster's report). Keep the reports together in the `reports` folder so the links work.
- **A cluster that fails** (for example ekslogin fails) is recorded as FAILED and the next cluster still runs; **a cluster whose profile's credentials expired** is marked `credentials expired` (with the renew message) and the next cluster still runs.
- In the window, **Clusters in this run** shows each cluster's status and CRIT/HIGH counts as it finishes (double-click a finished one to open its report), the findings list is prefixed with the cluster name, and the step checklist restarts for each cluster.
- **Stop** finishes the current step of the current cluster (its partial report is saved) and marks the clusters not yet started as "not run".
- Selecting **one** cluster works as before: no summary page.
- `--context NAME` and `--name` only apply to a single cluster. With several clusters the context is matched per cluster automatically.

### Where the cluster list comes from

With **Log in with ekslogin first** the source **Clusters from the ekslogin menu (instant)** (see "Collecting clusters on demand") reads, in this order:

1. the `CLUSTERS = {"1": "name", ...}` dict at the top of the script,
2. a `clusters.json` next to the script (`{"1": "name", "2": "name"}`),
3. the menu `ekslogin.exe` prints when it is started with no selection. This guesses the menu format (`1. name`, `1) name`, `[1] name`, `1 - name`). If it can't parse it, **type the cluster numbers into the box**, or create `clusters.json`.

ekslogin is run with your `ekslogin(cluster_number)` function as given, once per cluster (it sends the number on stdin, waits 2 seconds); afterwards `~/.aws` is read again.

## What you get

| # | Section | Contents |
|---|---|---|
| 1 | Cluster overview | context, kubectl/server version, API server `/readyz` health |
| 2 | **AWS EKS control plane & infrastructure** | cluster **status** (ACTIVE?), **API server endpoint** and public/private access, **certificate authority** data (and whether your kubeconfig matches), **VPC / subnets (free IPs) / security group rules**, **IAM role policies** (cluster + node roles), aws-auth / access entries, nodegroups, EKS add-ons, Fargate (incl. log router), EC2 status checks of the nodes, **control-plane logging** settings with recent CloudWatch errors and 401/403 denials |
| 3 | **Nodes** | status (Ready/NotReady/cordoned), pressure conditions, **pods running/max, CPU used vs allocatable, memory used vs allocatable, disk used/total, image filesystem, swap**, requests vs allocatable, version/zone/type |
| 4 | **Resource utilization** | **CPU and memory dashboard**: cluster gauges, share of the cluster used by each namespace, per-node cards, a namespace explorer (used vs requested vs limit, click a namespace to see its pods, high usage amber/red) and top consumers |
| 5 | **Pods on each node** | per node: every pod with **CPU / memory / disk use, requests and limits**, restarts, and flags such as "MEM 95% of limit" |
| 6 | **Namespaces: pods used vs configured** | per namespace: pods **running / total**, pods **configured** (desired replicas), what is missing, **ResourceQuota** (pod count, CPU, memory) usage, CPU / memory / disk per namespace, and the workloads behind them with HPA min-max; cluster-wide use of the nodes' pod capacity |
| 7 | Unhealthy pods | CrashLoopBackOff, OOMKilled, ImagePullBackOff, Pending (with the scheduler's reason), Evicted, not-ready, recent restarts |
| 8 | Events | Warning events in the window (grouped by reason, plus latest), notable Normal events (kills, scaling, node changes) |
| 9 | Workloads | Deployments / StatefulSets / DaemonSets not ready, new ReplicaSets (rollouts), failed Jobs, kube-system add-ons |
| 10 | **Network & traffic** | status blocks (OK / Warning / Problem / Not available) for the network plugin, IP exhaustion, plugin logs, stuck pods, node conditions, MTU, kube-proxy, Services, DNS, network policies, cloud firewalls (security groups + network ACLs), ingress / load balancers / certificates, conntrack and port exhaustion, observability, packet capture guidance, API server throttling, webhooks, etcd; VPC routing / NAT / endpoints; **traffic over the selected window**; a 10-row **Traffic issue checklist**; glossaries |
| 11 | Autoscaling, storage, network | HPAs at max, PVCs not Bound, LoadBalancers pending, Services with no endpoints, Terminating namespaces |
| 12 | Top consumers | top 10 pods by CPU and by memory |
| 13 | **Logs** | container logs from the last N minutes, shown in full in the HTML (see "Pod logs" below) |
| 14 | Timeline | everything that happened in the window, oldest first |
| - | Health summary | all findings ranked CRIT / HIGH / MED / INFO |

The report is shown on screen and saved as `reports\eks_debug_<cluster>_<time>.html` (interactive) and `.txt` (summary first). **Pod logs can contain sensitive data**, so treat the file accordingly.

## Kubernetes data versus AWS data

- **Kubernetes data (sections 3-11) needs only `kubectl` and the context that was just selected.** Nodes, pods, events, logs, usage, workloads: all of that comes from the cluster's own API.
- **The AWS section (2) is different: it comes from the AWS APIs, which `kubectl` cannot see.** Cluster status, subnets, security groups, IAM roles, nodegroups, add-ons, EC2 checks and the CloudWatch control-plane logs need the **AWS CLI (`aws`) plus working AWS credentials**.
  - Your kubeconfig almost certainly already uses the AWS CLI (EKS tokens come from `aws eks get-token`), so `aws` and credentials usually exist. The script reads the cluster name, region and **AWS profile** from that kubeconfig so it uses the same login. The section prints the target it resolved (`Target: cluster=… region=… profile=…`) and which identity the credentials belong to, so you can confirm it.
  - **What I can't tell from here is whether the role behind your credentials has AWS read permissions.** Roles built only for Kubernetes access often lack them. In that case a call fails with `AccessDenied`, the section prints which call failed, and the rest of the report still runs. Nothing else is affected.
- Quick check (replace the name and region):
  ```powershell
  aws sts get-caller-identity
  aws eks describe-cluster --name <cluster> --region <region> --query cluster.status
  ```
- Read-only AWS permissions the section uses: `sts:GetCallerIdentity`, `eks:DescribeCluster`, `eks:ListNodegroups`, `eks:DescribeNodegroup`, `eks:ListAddons`, `eks:DescribeAddon`, `eks:ListFargateProfiles`, `eks:DescribeFargateProfile`, `eks:ListAccessEntries`, `ec2:DescribeSubnets`, `ec2:DescribeSecurityGroups`, `ec2:DescribeInstanceStatus`, `ec2:DescribeInstances` (the EC2 Name tag), `iam:ListAttachedRolePolicies`, `logs:FilterLogEvents`.
- The profile is chosen from `~/.aws` (see "AWS profiles from ~/.aws" below). Options: `--aws-cluster NAME --region R --profile P` to set the target by hand, `--no-aws` (or untick **AWS details** in the window) to skip the section.
- **Node logs** (kubelet, containerd) can't be read through `kubectl` or the AWS APIs. For a NotReady node, the report prints the exact `aws ssm start-session --target i-…` and `journalctl` commands to run.

## AWS profiles from `~/.aws`

The script reads (again after `ekslogin`, which may have created or refreshed profiles) the profiles in `~/.aws/config` and `~/.aws/credentials` (or the files named by `AWS_CONFIG_FILE` / `AWS_SHARED_CREDENTIALS_FILE`), picks the right one for the selected cluster, **checks that it really works**, and uses it for every AWS-side check. This is the step **"Select AWS profile (~/.aws)"** in the window.

How the profile is chosen (best first):
1. the profile you selected in step 1 of the window, or `--profile NAME`;
2. the profile your **kubeconfig** already uses for kubectl (from its `aws eks get-token --profile ...` arguments);
3. a profile whose **name matches the cluster** (equal to it, or containing it);
4. a profile for the **same AWS account** as the cluster (account id taken from the cluster ARN; profiles supply `sso_account_id` or the account in `role_arn`);
5. the `AWS_PROFILE` environment variable, then `default`, then any other profile.

Each candidate is tried with `aws sts get-caller-identity` (up to 4 of them), and the **first one with working credentials wins**. The log shows which profiles were tried, why each was chosen or rejected, and which identity it belongs to. The AWS section of the report repeats the chosen profile and the reason. If a profile's credentials are **expired or missing**, the log shows the renew message (renew them outside this tool). If none work, the AWS section shows the exact error, and everything from `kubectl` still runs.

- **Window:** step 1 lists the profiles with their account, region, role / SSO info and credentials status; it reloads after each ekslogin run.
- `python eks_debug.py --list-profiles` prints what was found.
- Only the non-secret fields (region, account, role, SSO settings) are read. **Access keys and tokens are never loaded or shown.**
- The profile's region is used when the cluster's region isn't known from the kubeconfig.

## Where processor, memory, disk and swap numbers come from

- **Best source: the kubelet on each node** (`kubectl get --raw /api/v1/nodes/<node>/proxy/stats/summary`). It gives node and per-pod CPU, memory, root and image filesystem, and swap. It needs the **`nodes/proxy`** permission.
- **Fallback: metrics-server** (`kubectl top`). CPU and memory only, so **disk and swap show `n/a`**.
- If neither is available, the live usage columns show `n/a`; requests, limits, status and everything else still work. The report says which source it used.
- **Swap:** EKS nodes normally have swap off, so it shows `none/not reported`. If a node reports swap in use, that is flagged as a memory-pressure sign.
- Percentages are against what the node **allocates to pods** (allocatable), not the raw instance size.

## Settings you can change (top of the script)

`LOOKBACK_MINUTES`, `MAX_LOG_PODS` (20), `MAX_CORE_LOG_PODS` (12), `MAX_ALL_LOG_PODS` (60), `LOG_TAIL_LINES` (200), `LOG_MAX_BYTES` (256 KB per container), `LOG_WORKERS` (6 parallel reads), `MAX_EVENTS` (60), `MAX_NODE_PODS` (25 pods listed per node), `UTIL_WARN` (75) / `UTIL_CRIT` (90) (amber / red thresholds in the utilization dashboard), `MAX_ROWS` (40), `REPORT_DIR`.

## Notes

- Kubernetes keeps events for about an hour by default, so a very long window can't show older events.
- `kubectl` uses the context that was just selected. The report prints the context name at the top so you can confirm it is the right cluster.
- Without access to some resources (RBAC), that section prints `could not read ...` and the rest still runs.

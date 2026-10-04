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
python gke_debug.py --cluster 3 --skip-login
python gke_debug.py --cluster 3 --project my-gcp-project
python gke_debug.py --list-projects
python gke_debug.py --cluster 3 --gke-cluster prod-gke --location us-central1 --project my-gcp-project   # point at the cluster by hand
python gke_debug.py --cluster 3 --no-gcp    # kubectl only
```

Put `gkelogin.exe` in this folder (or pass `--gkelogin C:\path\gkelogin.exe`). `kubectl` and the **Google Cloud CLI (`gcloud`)** must be on PATH, and `gcloud auth login` must have been done for the GCP sections. No extra Python packages are needed.

The login is your `gkelogin(cluster_number)` function in the same shape as `ekslogin` / `akslogin`: it runs `gkelogin.exe`, sends the cluster number on stdin and waits 2 seconds. **This is an assumption: I have not seen your `gkelogin`.** If it behaves differently (different menu, extra arguments, a different exe name), paste the function and I will match it. The cluster list comes from the `CLUSTERS` dict at the top of the script, a `clusters.json` next to it (`{"1": "name", ...}`), or the menu `gkelogin.exe` prints (parsed as `1. name`, `1) name`, `[1] name`, `1 - name`). If it can't be parsed, type the cluster numbers into the box.

## Login methods (custom login or the Google Cloud CLI)

There are two ways to log in. The default is unchanged.

| | `--login-method exe` (default) | `--login-method cli` |
|---|---|---|
| Login | your `gkelogin.exe` (the cluster number is sent on stdin) | the standard **Google Cloud CLI** (`gcloud`) |
| Cluster list | `CLUSTERS` / `clusters.json` / the `gkelogin.exe` menu | read from `gcloud` (below) |
| Window | login method **Custom login (gkelogin)** | login method **Cloud CLI (gcloud)** |

```
python gke_debug.py --login-method cli --list                       # list the clusters gcloud can see (numbered 1..N)
python gke_debug.py --login-method cli --project my-proj --cluster 1
python gke_debug.py --login-method cli --project my-proj --cluster all
python gke_debug.py --login-method cli --project my-proj --cluster my-gke
python gke_debug.py --login-method cli --project my-proj --cluster 1 --device-code      # same as --no-launch-browser
```

**What the `cli` method does**

1. **Signed in?** `gcloud auth list --filter=status:ACTIVE` must show an account. If it does not, the script runs `gcloud auth login` (add `--device-code` / `--no-launch-browser` for `gcloud auth login --no-launch-browser`). The sign-in is interactive (browser or device code): from the command line it runs in your console; from the window (which has no console) it opens its own console window on Windows, waits for it to close, then checks again. Nothing about the sign-in is captured, and no tokens are ever printed.
2. **Cluster list.** `gcloud container clusters list --project P --format=json` for the project you chose (`--project` / the dropdown), or for every project `gcloud` can see when none is chosen (the first 60; projects without access or without the Kubernetes Engine API are skipped). The clusters are numbered 1..N in the order listed, so `--cluster 1,3`, `2-4`, `all` and a plain **cluster name** (`--cluster my-cluster`) all work. `--list` prints the same list.
3. **Connect.** For each selected cluster, in turn: `gcloud container clusters get-credentials NAME --location LOC --project P`, then `kubectl config use-context gke_P_LOC_NAME`. kubectl needs the `gke-gcloud-auth-plugin`; if it is not on PATH the script prints `gcloud components install gke-gcloud-auth-plugin`.
4. **Everything after that is the same as before**: the kubectl context is pinned, the cloud details step runs (the project, location and cluster name go straight into the GCP target (`--project`, `--location`, `--gke-cluster` for that run), so the cluster is not searched for again), and the report is written.

With several clusters each one gets its own sign-in check and credentials step. The Stop button and the live step messages work as usual; a cluster whose login fails or is cancelled is marked FAILED with the reason and the next one still runs. `--skip-login` skips the login (no `gcloud` login or credentials step; the kubectl context is matched by the cluster name as before). Reading the cluster list and writing the kubeconfig entry are the only things the CLI method does besides the usual read-only checks. If `gcloud` is not installed, the message says where to get it; if no cluster is found, it says so and what to check.

**In the window**, the **Login method** box switches the cluster list: pick *Cloud CLI (gcloud)* and the list is reloaded from `gcloud` (it may open the sign-in window first); pick *Custom login (gkelogin)* to go back. The profile / subscription / project box and, for AWS, the **Region(s)** box feed the listing. Press **Reload clusters** after signing in elsewhere. The tick box next to it is the `--device-code` option.

**Prerequisites for the `cli` method:** `kubectl`, the Google Cloud CLI (`gcloud`), and `gke-gcloud-auth-plugin`.

## What is different from EKS and AKS

| EKS version | AKS version | GKE version |
|---|---|---|
| `ekslogin` | `akslogin` | `gkelogin` |
| AWS CLI + `~/.aws` profiles | Azure CLI + subscription | **Google Cloud CLI + project**, picked after login: `--project` / the dropdown, else the project in the nodes' `providerID` (`gce://<project>/<zone>/<instance>`), else the project in the kubectl context name (`gke_<project>_<location>_<cluster>`), else your `gcloud config` default; checked by finding your cluster with `gcloud container clusters list` |
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

Same layout as the other versions with GCP sources: network components (`netd`, `anetd`, `calico-node`, `ip-masq-agent`, `kube-dns`, `konnectivity-agent`, ...), services (a LoadBalancer without the `networking.gke.io/load-balancer-type: Internal` annotation is flagged `INTERNET-FACING`), ingresses (class `gce` = external, `gce-internal` = internal, and unhealthy backends from the Ingress annotation), network policies, **pod IPs per range**, network warning events, kube-dns / netd / anetd log errors ("no IP addresses available in range set" = pod range exhaustion), then:

- **Routes** (a missing default route is HIGH; a default route to an appliance / VPN is INFO), **static IPs** (reserved but unused external IPs are INFO).
- **Load balancers of this cluster** (forwarding rules matched to your Services / Ingresses), the **backend services and their health** (`backend-services get-health`; all backends unhealthy is HIGH, with the health-check firewall ranges to check).
- **Cloud NAT**: which NAT IPs and how many ports each node got (`routers get-nat-mapping-info`).
- **Traffic over the selected window** (Cloud Monitoring, **1-minute points**): per-node network in/out and all nodes together; load balancer requests by response class / **5xx share (1%+ flagged)** / p95 latency (or bytes for network LBs); Cloud NAT **dropped packets** (OUT_OF_RESOURCES = port exhaustion is HIGH) and **NAT allocation failed** (HIGH), and per-node NAT port use (80%+ flagged), with charts in the HTML. Kubelet pod and node counters (totals since start plus a live sample, `--traffic-sample SECONDS`) are as in the other versions. Not included: pod-to-pod traffic (needs VPC Flow Logs / Dataplane V2 observability) and per-pod traffic over the window.

## Permissions (read-only)

Without one of these, that part says why it is missing and the rest still runs.

- **GCP IAM** on the project (and the host project when the cluster uses a Shared VPC): `roles/container.viewer` (clusters, operations), `roles/compute.viewer` (instances, instance groups, subnets, firewall rules, routes, routers / NAT, forwarding rules, backend services and their health), `roles/logging.viewer` (the control-plane / autoscaler / admin-activity logs; Kubernetes **data access** audit entries need `roles/logging.privateLogViewer`), `roles/monitoring.viewer` (the traffic series), and something that allows `resourcemanager.projects.getIamPolicy` for the IAM section, for example `roles/iam.securityReviewer` (also gives `iam.serviceAccounts.get`). `resourcemanager.projects.list` is only needed for the project dropdown / `--list-projects`; without it the project is taken from the nodes / context.
- **Kubernetes:** the same as the EKS version (`nodes/proxy` for kubelet stats, list on pods / nodes / events / namespaces, etc.).

The script only runs `gcloud ... list / describe / get-*` commands, `gcloud config get-value`, `gcloud auth print-access-token`, and two read-only Google API queries (Cloud Monitoring `timeSeries.list` and Cloud Logging `entries.list`) with that token. The token is kept in memory only and is never printed or written to a report. The API helper refuses any other URL.

## Not verified against a real GCP project

I built and tested this against mocked `kubectl`, `gcloud` and Google API output, **not a real GKE cluster**. The `gcloud` JSON field names, the Cloud Monitoring metric names / label names (`compute.googleapis.com/instance/network/received_bytes_count`, `loadbalancing.googleapis.com/https/request_count`, `router.googleapis.com/nat/dropped_sent_packets_count`, `nat_allocation_failed`, `allocated_ports`, `port_usage`, ...) and the Cloud Logging filters follow the documented Google Cloud CLI and APIs, but **run it once on a real cluster and check each section**; the Cloud NAT per-VM port figures in particular are approximate. A call that fails prints its error in the report instead of stopping the run. Other assumptions: `gkelogin` behaves like `ekslogin` (see above); `gcloud container operations list` is read without a location filter; `gcloud monitoring time-series list` is not used (it is not generally available), the Monitoring API is called directly with the `gcloud` access token.

On Windows the Cloud Logging API is queried directly (a filter with quotes does not reliably survive `gcloud.cmd`); `gcloud logging read` is used as the fallback, and first elsewhere.

## Reports

`reports\gke_debug_<cluster>_<time>.html` (interactive) and `.txt`; with several clusters also `reports\gke_debug_summary_<time>.html`. Pod logs and the report can contain sensitive data; share them carefully.

Settings at the top of the script: `LOOKBACK_MINUTES`, `MAX_LOG_PODS`, `LOG_TAIL_LINES`, `UTIL_WARN` / `UTIL_CRIT`, `SUPPORT_LABEL`, `TRAFFIC_SAMPLE_SECONDS`, `LOW_SUBNET_IPS`.

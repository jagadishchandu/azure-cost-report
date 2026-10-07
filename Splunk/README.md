# Splunk Kubernetes Log Fetcher

A small browser app to pull Kubernetes logs out of Splunk:

1. **Connect** with an API token (or username and password)
2. **Cluster**: only the clusters your Splunk role can access
3. **Namespaces**: every namespace in that cluster (all selected by default)
4. **Time and pods**: quick ranges (15 min, 1 h, 6 h, 24 h) or a custom start/end date and time, then all pods or a chosen few
5. **Fetch** and explore the logs (table, log view, activity charts), filter them, and download as CSV, JSON lines or text

## Run it

```
cd splunk-log-tool
pip install -r requirements.txt
streamlit run app.py
```

Your browser opens at http://localhost:8501.

## Configure (optional but recommended)

Copy `splunk_config.example.json` to `splunk_config.json` and set at least:

| Key | What it is |
|---|---|
| `host`, `port` | Splunk server and its **management** port (default 8089, not 8000) |
| `cluster_field`, `namespace_field`, `pod_field` | The field names that hold the cluster, namespace and pod in your events |
| `default_indexes` | Indexes pre-selected in the app (empty = every index you can access) |
| `ca_bundle` | Path to your company CA file if you get certificate errors |

Field names depend on how logs get into Splunk. Open one Kubernetes event in Splunk and check:

| Log shipper | cluster_field | namespace_field | pod_field |
|---|---|---|---|
| Splunk Connect for Kubernetes | `cluster_name` | `namespace` | `pod` |
| OpenTelemetry collector | `k8s.cluster.name` | `k8s.namespace.name` | `k8s.pod.name` |

Never put a token or password in the config file. Type it in the app, or set the `SPLUNK_TOKEN` environment variable.

## Authentication

- **API token** (recommended): sent as `Authorization: Bearer <token>`. Create it in Splunk under Settings > Tokens.
- **Username and password**: posted once to `/services/auth/login` for a session key. This only works for Splunk-local accounts. If your company uses SSO (Okta / SAML) for Splunk, use a token.

Credentials live only in the browser session's memory.

## Efficiency

What the tool does so it is light on Splunk, your network and your browser:

| Where | What it does |
|---|---|
| Splunk | Cluster, namespace and pod lists use `tstats` (reads index metadata, not the events) and are cached for 5 minutes |
| Splunk | Once a cluster is chosen, only the **indexes that cluster lives in** are searched, not `index=*` |
| Splunk | The log search keeps only the 5 needed fields and ends with `head N`, so Splunk **stops as soon as it has enough events** |
| Splunk | If `tstats` finds nothing for a field, the tool remembers that and stops retrying it |
| Network | Result pages are downloaded **in parallel** (`parallel_pages`, default 4); connections are reused; finished search jobs are deleted |
| Network | Status polling starts fast (0.15 s) and backs off, so short lookups return quickly |
| Browser | Results are turned into a table, statistics and charts **once**. Typing in the filter box, switching tabs or moving sliders never rebuilds them |
| Browser | Download files (CSV, JSON lines, text) are only built when you click the button |
| Browser | The table shows the first 2,000 rows only; the downloads contain everything |

Measured against a local fake Splunk (so network and Splunk search time are not included): fetching **100,000 events** took about 1 second including page download, sorting and building the results; typing a filter re-ran the page in about 0.05 s; building a 12 MB CSV took about 0.13 s.

**Newest vs earliest.** If your time range holds more events than *Maximum events*, you choose which ones to keep under *Options*:
- **Newest (fast, default):** Splunk already returns newest-first, so it stops early. The app sorts what it gets oldest-first.
- **Earliest (slower):** Splunk must sort every matching event first. Use it only when you expect to hit the cap and need the start of the range.
If the range has no more events than the maximum, both options return exactly the same logs.

**Tips for big pulls:** pick specific namespaces or pods, use a shorter time range, and keep the maximum as low as you need.

## Good to know

- **Access control is Splunk's.** The cluster, namespace and pod lists come from searches run as you, so you only see what your role can search.
- **Listing is cheap.** It uses `tstats` (fast, needs indexed fields) and automatically falls back to a normal search if that finds nothing.
- **Fetch order.** Logs are shown oldest first, up to *Maximum events* (default 50,000). If the cap is reached the app tells you which end it kept; narrow the range or pods.
- **Look-back.** Clusters and namespaces are listed from the last 24 hours by default (changeable under *Search scope*). Pods are listed from your chosen time range.
- **Time zone.** Quick ranges are relative to now. For custom times pick local time or UTC.

## Tests

`python test_client.py` runs offline tests of the Splunk client against a small fake Splunk server (sign-in, discovery, paging, caps, cleanup, SPL escaping). They need no real Splunk.

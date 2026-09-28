import csv
import json
import subprocess
import sys
import time

OUTPUT_CSV = "cluster_node_instance_ids.csv"
CLUSTER_RANGE = range(1, 46)  # 1 to 45 inclusive


def run(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def ekslogin(cluster_number):
    # ekslogin.exe prints a menu and waits on stdin for the cluster number(s).
    proc = subprocess.run(
        [".\\ekslogin.exe"],
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


def get_current_context():
    code, out, err = run("kubectl config current-context")
    return out.strip() if code == 0 else "unknown"


def get_nodes():
    code, out, err = run("kubectl get nodes -o json")
    if code != 0:
        print(f"kubectl get nodes failed: {err.strip()}")
        return []

    data = json.loads(out)
    rows = []
    for item in data.get("items", []):
        name = item["metadata"]["name"]
        provider_id = item["spec"].get("providerID", "")
        instance_id = provider_id.split("/")[-1] if provider_id else ""
        instance_type = item["metadata"].get("labels", {}).get(
            "node.kubernetes.io/instance-type", ""
        )
        zone = item["metadata"].get("labels", {}).get(
            "topology.kubernetes.io/zone", ""
        )
        status = "Unknown"
        for cond in item.get("status", {}).get("conditions", []):
            if cond.get("type") == "Ready":
                status = "Ready" if cond.get("status") == "True" else "NotReady"

        rows.append(
            {
                "node_name": name,
                "instance_id": instance_id,
                "instance_type": instance_type,
                "zone": zone,
                "status": status,
            }
        )
    return rows


def main():
    all_rows = []

    for cluster_number in CLUSTER_RANGE:
        print(f"Logging into cluster {cluster_number}...")
        if not ekslogin(cluster_number):
            continue

        context = get_current_context()
        nodes = get_nodes()
        print(f"  context={context} nodes={len(nodes)}")

        for node in nodes:
            node["cluster_number"] = cluster_number
            node["context"] = context
            all_rows.append(node)

    if not all_rows:
        print("No data collected.")
        sys.exit(1)

    fieldnames = [
        "cluster_number",
        "context",
        "node_name",
        "instance_id",
        "instance_type",
        "zone",
        "status",
    ]

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nDone. Wrote {len(all_rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

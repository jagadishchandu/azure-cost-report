#!/bin/bash
#
# Scans all namespaces in the current OpenShift/Kubernetes cluster context
# for deployments related to "harness". Writes a summary table (namespace,
# name, image) plus full deployment YAML and full pod details for every
# matching namespace into one output file.
#
# Usage: ./find-harness-deployments.sh [search-term] [output-file]
#   search-term  : optional, defaults to "harness" (case-insensitive)
#   output-file  : optional, defaults to harness-deployments-<timestamp>.txt

set -uo pipefail

SEARCH_TERM="${1:-harness}"
OUTPUT_FILE="${2:-harness-deployments-$(date +%Y%m%d-%H%M%S).txt}"

if ! command -v oc &> /dev/null; then
    echo "Error: 'oc' CLI not found in PATH. Make sure you're logged into the cluster." >&2
    exit 1
fi

echo "Collecting deployment data from all namespaces (filtering for: $SEARCH_TERM)..."

SUMMARY=$(oc get deployments --all-namespaces \
    -o jsonpath='{range .items[*]}{.metadata.namespace}{"\t"}{.metadata.name}{"\t"}{.spec.template.spec.containers[0].image}{"\n"}{end}' \
    | grep -i "$SEARCH_TERM")

{
    echo "=================================================================="
    echo " SUMMARY: matching deployments"
    echo "=================================================================="
    printf "NAMESPACE\tNAME\tIMAGE\n"
    if [ -n "$SUMMARY" ]; then
        echo "$SUMMARY"
    else
        echo "No deployments matching '$SEARCH_TERM' were found."
    fi
} > "$OUTPUT_FILE"

if [ -z "$SUMMARY" ]; then
    echo "Done. No matches. Results saved to: $OUTPUT_FILE"
    cat "$OUTPUT_FILE"
    exit 0
fi

# Collect the unique namespace/deployment pairs so we can pull full detail for each.
while IFS=$'\t' read -r ns name image; do
    [ -z "$ns" ] && continue

    {
        echo ""
        echo "=================================================================="
        echo " NAMESPACE: $ns  |  DEPLOYMENT: $name  |  IMAGE: $image"
        echo "=================================================================="

        echo ""
        echo "--- Namespace details ---"
        oc get namespace "$ns" -o yaml

        echo ""
        echo "--- Deployment full detail (describe) ---"
        oc describe deployment "$name" -n "$ns"

        echo ""
        echo "--- Deployment full YAML ---"
        oc get deployment "$name" -n "$ns" -o yaml

        echo ""
        echo "--- Pods in namespace (wide) ---"
        oc get pods -n "$ns" -o wide

        echo ""
        echo "--- Pod full details (describe, for pods owned by this deployment) ---"
        for pod in $(oc get pods -n "$ns" -l "app=$name" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
            echo ""
            echo "### Pod: $pod ###"
            oc describe pod "$pod" -n "$ns"
        done
    } >> "$OUTPUT_FILE"
done <<< "$SUMMARY"

echo "Done. Full results saved to: $OUTPUT_FILE"

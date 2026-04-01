#!/usr/bin/env bash
# =============================================================================
# AI Lakehouse Gateway Cleanup Script
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="etl-compute"

echo "🗑️  Tearing down Gateway components in namespace '$NAMESPACE'..."

# Delete Deployment and Service
kubectl delete -f "${SCRIPT_DIR}/gateway-deploy.yaml" --ignore-not-found

# Delete local gateway secret
kubectl delete secret etl-gateway-secret -n "$NAMESPACE" --ignore-not-found

echo "✅ Gateway components removed."
echo ""
echo "💡 Note: 'etl-secrets' and 'etl-config' were left intact," 
echo "   as they are shared with the Ray cluster."

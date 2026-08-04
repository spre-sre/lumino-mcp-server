#!/usr/bin/env bash
# certificate-expiry-test.sh
#
# Test fixture for the certificate-expiry runbook (SPRE-5976).
#
# Creates TLS secrets with deliberately short-lived certificates to trigger
# warning and critical states in check_cluster_certificate_health, enabling
# end-to-end validation of the runbook diagnostic steps.
#
# Prerequisites:
#   - oc or kubectl CLI configured against the target cluster
#   - openssl available on the local machine
#
# Usage:
#   bash certificate-expiry-test.sh [setup|teardown]
#
# Runbook diagnostic steps to validate after setup:
#   Step 1: check_cluster_certificate_health (warning_threshold_days=30, critical_threshold_days=7)
#           → expect critical-expiry-cert (3 days) and warning-expiry-cert (15 days) in results
#   Step 2: check_cluster_certificate_health with namespaces=["spre-cert-test"]
#           → scoped scan confirms both certs flagged
#   Step 3: investigate_tls_certificate_issues (time_range="24h")
#           → TLS error patterns correlated with events in the namespace

set -euo pipefail

NAMESPACE="spre-cert-test"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

OC="${OC:-oc}"   # override with OC=kubectl if not on OpenShift

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
warning() { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Dependency checks ─────────────────────────────────────────────────────────
check_deps() {
    command -v openssl &>/dev/null || error "openssl is required but not installed."
    command -v "$OC"  &>/dev/null || error "'$OC' CLI is required but not installed."
    "$OC" whoami &>/dev/null      || error "Not logged in to a cluster. Run 'oc login' first."
}

# ── Generate a self-signed certificate expiring in N days ─────────────────────
generate_cert() {
    local cn="$1" days="$2" cert_file="$3" key_file="$4"
    openssl req -x509 -newkey rsa:2048 \
        -keyout "$key_file" \
        -out    "$cert_file" \
        -days   "$days" \
        -nodes \
        -subj   "/CN=${cn}/O=SPRE-Test/OU=Runbook-Validation" \
        -addext "subjectAltName=DNS:${cn}" \
        2>/dev/null
}

# ── Setup ─────────────────────────────────────────────────────────────────────
setup() {
    check_deps

    info "Creating namespace: $NAMESPACE"
    "$OC" create namespace "$NAMESPACE" --dry-run=client -o yaml | "$OC" apply -f -

    info "Generating certificates in $WORKDIR"

    # Critical: expires in 3 days — within the 7-day critical threshold
    generate_cert "critical-cert.spre-cert-test.svc" 3 \
        "$WORKDIR/critical.crt" "$WORKDIR/critical.key"
    info "Generated: critical-expiry-cert (3 days remaining)"

    # Warning: expires in 15 days — within the 30-day warning threshold
    generate_cert "warning-cert.spre-cert-test.svc" 15 \
        "$WORKDIR/warning.crt" "$WORKDIR/warning.key"
    info "Generated: warning-expiry-cert (15 days remaining)"

    # Healthy: expires in 180 days — outside both thresholds (control)
    generate_cert "healthy-cert.spre-cert-test.svc" 180 \
        "$WORKDIR/healthy.crt" "$WORKDIR/healthy.key"
    info "Generated: healthy-cert (180 days remaining)"

    info "Creating TLS Secrets in namespace $NAMESPACE"

    "$OC" create secret tls critical-expiry-cert \
        --cert="$WORKDIR/critical.crt" \
        --key="$WORKDIR/critical.key" \
        --namespace="$NAMESPACE" \
        --dry-run=client -o yaml | "$OC" apply -f -

    "$OC" create secret tls warning-expiry-cert \
        --cert="$WORKDIR/warning.crt" \
        --key="$WORKDIR/warning.key" \
        --namespace="$NAMESPACE" \
        --dry-run=client -o yaml | "$OC" apply -f -

    "$OC" create secret tls healthy-cert \
        --cert="$WORKDIR/healthy.crt" \
        --key="$WORKDIR/healthy.key" \
        --namespace="$NAMESPACE" \
        --dry-run=client -o yaml | "$OC" apply -f -

    echo ""
    info "Setup complete. Secrets created in namespace '$NAMESPACE':"
    "$OC" get secrets -n "$NAMESPACE" --field-selector type=kubernetes.io/tls \
        -o custom-columns="NAME:.metadata.name,TYPE:.type,CREATED:.metadata.creationTimestamp"

    echo ""
    info "Verify certificate expiry dates locally:"
    echo "  critical-expiry-cert: $(openssl x509 -in "$WORKDIR/critical.crt" -noout -enddate)"
    echo "  warning-expiry-cert:  $(openssl x509 -in "$WORKDIR/warning.crt"  -noout -enddate)"
    echo "  healthy-cert:         $(openssl x509 -in "$WORKDIR/healthy.crt"  -noout -enddate)"

    echo ""
    info "Now run the runbook diagnostic steps via the lumino MCP server:"
    echo "  Step 1: check_cluster_certificate_health (warning_threshold_days=30, critical_threshold_days=7)"
    echo "          → critical-expiry-cert must appear as CRITICAL"
    echo "          → warning-expiry-cert must appear as WARNING"
    echo "          → healthy-cert must appear as HEALTHY"
    echo "  Step 2: check_cluster_certificate_health (namespaces=[\"$NAMESPACE\"])"
    echo "          → scoped scan confirms same results"
    echo "  Step 3: investigate_tls_certificate_issues (time_range=\"24h\")"
    echo "          → TLS error patterns correlated with cluster events"
}

# ── Teardown ──────────────────────────────────────────────────────────────────
teardown() {
    check_deps
    warning "Deleting namespace '$NAMESPACE' and all resources within it..."
    "$OC" delete namespace "$NAMESPACE" --ignore-not-found
    info "Teardown complete."
}

# ── Entrypoint ────────────────────────────────────────────────────────────────
case "${1:-setup}" in
    setup)    setup    ;;
    teardown) teardown ;;
    *) echo "Usage: $0 [setup|teardown]"; exit 1 ;;
esac

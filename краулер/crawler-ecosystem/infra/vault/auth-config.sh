#!/bin/bash
# =============================================================================
# File: infra/vault/auth-config.sh
# Configures Kubernetes authentication for Vault.
# Run once after Vault is initialized.
# =============================================================================

set -euo pipefail

export VAULT_ADDR="http://vault.crawler-ecosystem.com:8200"
export VAULT_TOKEN="${VAULT_ROOT_TOKEN}"

echo "Enabling Kubernetes auth method..."
vault auth enable kubernetes

echo "Configuring Kubernetes auth..."
vault write auth/kubernetes/config \
    token_reviewer_jwt="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" \
    kubernetes_host="https://${KUBERNETES_PORT_443_TCP_ADDR}:443" \
    kubernetes_ca_cert=@/var/run/secrets/kubernetes.io/serviceaccount/ca.crt

echo "Creating admin role..."
vault write auth/kubernetes/role/crawler-admin \
    bound_service_account_names=crawler-admin \
    bound_service_account_namespaces=crawler \
    policies=admin-policy \
    ttl=24h

echo "Creating operator role..."
vault write auth/kubernetes/role/crawler-operator \
    bound_service_account_names=crawler-operator \
    bound_service_account_namespaces=crawler \
    policies=operator-policy \
    ttl=12h

echo "Creating viewer role..."
vault write auth/kubernetes/role/crawler-viewer \
    bound_service_account_names=crawler-viewer \
    bound_service_account_namespaces=crawler \
    policies=viewer-policy \
    ttl=8h

echo "Vault Kubernetes auth configured."
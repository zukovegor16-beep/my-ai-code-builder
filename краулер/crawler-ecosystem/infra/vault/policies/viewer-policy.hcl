# =============================================================================
# File: infra/vault/policies/viewer-policy.hcl
# =============================================================================

path "secret/data/crawler/public/*" {
  capabilities = ["read", "list"]
}

path "secret/metadata/crawler/public/*" {
  capabilities = ["list", "read"]
}
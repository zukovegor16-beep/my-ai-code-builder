# =============================================================================
# File: infra/vault/policies/operator-policy.hcl
# =============================================================================

path "secret/data/crawler/*" {
  capabilities = ["create", "read", "update", "list"]
}

path "secret/metadata/crawler/*" {
  capabilities = ["list", "read"]
}

path "database/creds/crawler-*" {
  capabilities = ["read"]
}

path "transit/encrypt/crawler-*" {
  capabilities = ["create", "update"]
}

path "transit/decrypt/crawler-*" {
  capabilities = ["create", "update"]
}
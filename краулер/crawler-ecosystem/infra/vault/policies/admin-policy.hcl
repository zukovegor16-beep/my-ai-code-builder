# =============================================================================
# Part 12 — HashiCorp Vault Policies and Kubernetes Auth Configuration
# =============================================================================
# File: infra/vault/policies/admin-policy.hcl
# =============================================================================

path "secret/data/crawler/*" {
  capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}

path "secret/metadata/crawler/*" {
  capabilities = ["list", "read"]
}

path "database/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "transit/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "auth/token/roles/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "sys/audit/*" {
  capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}

path "sys/policies/acl/*" {
  capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}

path "secret/data/crawler/encryption-key" {
  capabilities = ["read", "update"]
  required_parameters = ["key"]
}
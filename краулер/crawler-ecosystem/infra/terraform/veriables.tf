# =============================================================================
# Part 1 — All variables with descriptions, types, defaults, and validations
# =============================================================================

# ---------------------------------- Tokens ----------------------------------
variable "hcloud_token" {
  description = "Hetzner Cloud API token"
  type        = string
  sensitive   = true
}

variable "cloudflare_api_token" {
  description = "Cloudflare API token"
  type        = string
  sensitive   = true
}

variable "cloudflare_account_id" {
  description = "Cloudflare account ID for Workers/R2 (optional)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "vault_address" {
  description = "HashiCorp Vault address"
  type        = string
  default     = "http://vault.crawler-ecosystem.com:8200"
}

variable "vault_token" {
  description = "Vault token"
  type        = string
  sensitive   = true
  default     = ""
}

variable "state_access_key" {
  description = "S3 access key for Terraform state"
  type        = string
  sensitive   = true
}

variable "state_secret_key" {
  description = "S3 secret key for Terraform state"
  type        = string
  sensitive   = true
}

# --------------------------------- General ----------------------------------
variable "environment" {
  description = "Deployment environment (dev, staging, production)"
  type        = string
  default     = "production"

  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "Environment must be dev, staging, or production."
  }
}

variable "cluster_name" {
  description = "K3s cluster name"
  type        = string
  default     = "crawler-k3s"
}

variable "zone_name" {
  description = "Cloudflare zone domain name"
  type        = string
  default     = "crawler-ecosystem.com"
}

variable "region" {
  description = "Data center region (nbg1, fsn1, hel1, etc.)"
  type        = string
  default     = "nbg1"
}

# ---------------------------------- Nodes -----------------------------------
variable "master_count" {
  description = "Number of master nodes (always 3 for HA)"
  type        = number
  default     = 3

  validation {
    condition     = var.master_count >= 3
    error_message = "Minimum 3 master nodes for high availability."
  }
}

variable "worker_count" {
  description = "Number of worker nodes"
  type        = number
  default     = 2
}

variable "master_server_type" {
  description = "Server type for master nodes"
  type        = string
  default     = "cx22"
}

variable "worker_server_type" {
  description = "Server type for worker nodes"
  type        = string
  default     = "cx32"
}

variable "gpu_server_type" {
  description = "Server type for GPU workers (if required)"
  type        = string
  default     = "gpu1"
}

# ---------------------------------- Disk ------------------------------------
variable "data_volume_size" {
  description = "Additional data disk size (GB)"
  type        = number
  default     = 40
}

# ---------------------------------- SSH -------------------------------------
variable "ssh_public_key_path" {
  description = "Path to public SSH key"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

# --------------------------------- Network ----------------------------------
variable "network_ip_range" {
  description = "Private network CIDR block"
  type        = string
  default     = "10.0.0.0/8"
}

variable "subnet_ip_range" {
  description = "Subnet CIDR block"
  type        = string
  default     = "10.0.1.0/24"
}

# ----------------------------------- DNS ------------------------------------
variable "api_subdomain" {
  description = "Subdomain for API"
  type        = string
  default     = "api"
}

variable "monitoring_subdomain" {
  description = "Subdomain for monitoring"
  type        = string
  default     = "monitoring"
}

# ---------------------------------- Labels ----------------------------------
variable "labels" {
  description = "Common labels for all resources"
  type        = map(string)
  default = {
    project    = "crawler-ecosystem"
    managed_by = "terraform"
  }
}
# =============================================================================
# Part 1 — Terraform Core: Providers, Backend, Variables, Locals, Random
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.42"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    vault = {
      source  = "hashicorp/vault"
      version = "~> 3.15"
    }
  }

  backend "s3" {
    bucket                      = "crawler-terraform-state"
    key                         = "infra/terraform.tfstate"
    region                      = "eu-central"
    endpoint                    = "https://s3.example.com"
    access_key                  = var.state_access_key
    secret_key                  = var.state_secret_key
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    force_path_style            = true
  }
}

# -----------------------------------------------------------------------------
# Providers
# -----------------------------------------------------------------------------
provider "hcloud" {
  token = var.hcloud_token
}

provider "cloudflare" {
  api_token = var.cloudflare_api_token
}

provider "vault" {
  address = var.vault_address
  token   = var.vault_token
}

# -----------------------------------------------------------------------------
# Local values used across the configuration
# -----------------------------------------------------------------------------
locals {
  common_labels = merge(var.labels, {
    environment = var.environment
  })

  master_ips = hcloud_server.k3s_master.*.ipv4_address
  worker_ips = hcloud_server.k3s_worker.*.ipv4_address
}

# -----------------------------------------------------------------------------
# Random token for K3s cluster join
# -----------------------------------------------------------------------------
resource "random_password" "k3s_token" {
  length  = 48
  special = false
}
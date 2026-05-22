# =============================================================================
# Part 5 — DNS Records, TLS Settings, and Cloudflare Configuration
# =============================================================================
# File: infra/terraform/dns.tf

# ----------------------------------------------------------------
# Cloudflare zone data source
# ----------------------------------------------------------------
data "cloudflare_zone" "main" {
  name = var.zone_name
}

# ----------------------------------------------------------------
# A record for API gateway (pointing to first master)
# ----------------------------------------------------------------
resource "cloudflare_record" "api" {
  zone_id = data.cloudflare_zone.main.id
  name    = var.api_subdomain
  value   = local.master_ips[0]
  type    = "A"
  ttl     = 120
  proxied = true   # Enables Cloudflare CDN and DDoS protection

  comment = "API endpoint for Mega Crawler"
}

# ----------------------------------------------------------------
# Wildcard A record for monitoring services
# ----------------------------------------------------------------
resource "cloudflare_record" "monitoring" {
  zone_id = data.cloudflare_zone.main.id
  name    = "*.${var.monitoring_subdomain}"
  value   = local.master_ips[0]
  type    = "A"
  ttl     = 120
  proxied = true

  comment = "Wildcard for monitoring subdomains (grafana, prometheus, loki, etc.)"
}

# ----------------------------------------------------------------
# CNAME record for Kubernetes dashboard (optional)
# ----------------------------------------------------------------
resource "cloudflare_record" "kubernetes_dashboard" {
  count   = var.environment == "production" ? 1 : 0
  zone_id = data.cloudflare_zone.main.id
  name    = "k8s"
  type    = "CNAME"
  value   = "${var.api_subdomain}.${var.zone_name}"
  ttl     = 300
  proxied = true

  comment = "Kubernetes dashboard alias"
}

# ----------------------------------------------------------------
# TXT record for domain verification (optional)
# ----------------------------------------------------------------
resource "cloudflare_record" "domain_verification" {
  count   = var.environment == "production" ? 1 : 0
  zone_id = data.cloudflare_zone.main.id
  name    = var.zone_name
  type    = "TXT"
  value   = "\"crawler-verification=${random_password.k3s_token.result}\""
  ttl     = 3600

  comment = "Domain verification record"
}

# ----------------------------------------------------------------
# Page Rule for automatic HTTPS redirect
# ----------------------------------------------------------------
resource "cloudflare_page_rule" "always_use_https" {
  zone_id  = data.cloudflare_zone.main.id
  target   = "*${var.zone_name}/*"
  priority = 1

  actions {
    always_use_https = true
  }
}

# ----------------------------------------------------------------
# Outputs specific to DNS
# ----------------------------------------------------------------
output "api_dns_name" {
  value = cloudflare_record.api.hostname
}

output "monitoring_dns_name" {
  value = cloudflare_record.monitoring.hostname
}
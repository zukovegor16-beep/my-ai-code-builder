# =============================================================================
# Outputs
# =============================================================================
# File: infra/terraform/outputs.tf

output "master_ips" {
  value       = local.master_ips
  description = "Public IP addresses of all master nodes"
}

output "worker_ips" {
  value       = local.worker_ips
  description = "Public IP addresses of all worker nodes"
}

output "gpu_worker_ips" {
  value       = hcloud_server.k3s_gpu_worker.*.ipv4_address
  description = "Public IP addresses of GPU worker nodes (if any)"
}

output "private_network_id" {
  value       = hcloud_network.crawler_net.id
  description = "ID of the private network"
}

output "k3s_token" {
  value       = random_password.k3s_token.result
  sensitive   = true
  description = "K3s cluster join token"
}

output "api_url" {
  value       = "https://${var.api_subdomain}.${var.zone_name}"
  description = "URL of the API gateway"
}

output "grafana_url" {
  value       = "https://grafana.${var.monitoring_subdomain}.${var.zone_name}"
  description = "URL of the Grafana dashboard"
}

output "prometheus_url" {
  value       = "https://prometheus.${var.monitoring_subdomain}.${var.zone_name}"
  description = "URL of the Prometheus server"
}

output "loki_url" {
  value       = "https://loki.${var.monitoring_subdomain}.${var.zone_name}"
  description = "URL of Loki log aggregation"
}

output "ssh_private_key_path" {
  value       = local_sensitive_file.private_key.filename
  description = "Local path to the generated SSH private key"
}
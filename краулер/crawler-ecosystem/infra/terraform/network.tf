# =============================================================================
# Part 2 — Terraform Network, Firewall, SSH Keys
# =============================================================================
# File: infra/terraform/network.tf

# ----- Private network -----
resource "hcloud_network" "crawler_net" {
  name     = "crawler-network"
  ip_range = var.network_ip_range
}

# Primary subnet
resource "hcloud_network_subnet" "crawler_subnet" {
  network_id   = hcloud_network.crawler_net.id
  type         = "cloud"
  network_zone = "eu-central"
  ip_range     = var.subnet_ip_range
}

# Backup subnets in different zones (increase availability)
resource "hcloud_network_subnet" "crawler_subnet_backup" {
  count        = 2
  network_id   = hcloud_network.crawler_net.id
  type         = "cloud"
  network_zone = count.index == 0 ? "eu-west" : "eu-east"
  ip_range     = "10.0.${count.index + 2}.0/24"
}

# ----- Firewall -----
resource "hcloud_firewall" "crawler_firewall" {
  name = "crawler-firewall"

  # SSH access for management
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = [
      "0.0.0.0/0"   # in production restrict to your jump host IP
    ]
    description = "SSH"
  }

  # Kubernetes API server
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "6443"
    source_ips = [
      "0.0.0.0/0"   # required for worker registration, can be narrowed
    ]
    description = "K3s API server"
  }

  # HTTP/HTTPS for API and frontend
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "80"
    source_ips = ["0.0.0.0/0"]
    description = "HTTP"
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "443"
    source_ips = ["0.0.0.0/0"]
    description = "HTTPS"
  }

  # Custom API port (Node.js)
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "3000"
    source_ips = ["0.0.0.0/0"]
    description = "Crawler API"
  }

  # K3s internal communication (Cilium/Flannel VXLAN)
  rule {
    direction  = "in"
    protocol   = "udp"
    port       = "8472"
    source_ips = [var.network_ip_range]
    description = "Flannel VXLAN"
  }

  # K3s node port range (for services)
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "30000-32767"
    source_ips = [var.network_ip_range]
    description = "NodePort services"
  }

  # ICMP for diagnostics
  rule {
    direction  = "in"
    protocol   = "icmp"
    source_ips = ["0.0.0.0/0"]
    description = "Ping"
  }

  # Outgoing everything (default)
  rule {
    direction  = "out"
    protocol   = "tcp"
    port       = "any"
    destination_ips = ["0.0.0.0/0"]
    description = "Allow all outbound TCP"
  }

  rule {
    direction  = "out"
    protocol   = "udp"
    port       = "any"
    destination_ips = ["0.0.0.0/0"]
    description = "Allow all outbound UDP"
  }
}

# ----- SSH Key -----
resource "tls_private_key" "crawler_ssh" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "hcloud_ssh_key" "crawler_ssh" {
  name       = "crawler-key"
  public_key = tls_private_key.crawler_ssh.public_key_openssh
}

# ----- Output local file with private key (optional) -----
resource "local_sensitive_file" "private_key" {
  content  = tls_private_key.crawler_ssh.private_key_pem
  filename = "${path.module}/crawler_ssh_key.pem"

  provisioner "local-exec" {
    command = "chmod 600 ${path.module}/crawler_ssh_key.pem"
  }
}
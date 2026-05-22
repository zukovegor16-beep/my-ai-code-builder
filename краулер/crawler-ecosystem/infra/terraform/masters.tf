# =============================================================================
# Part 3 — Master Nodes with Volumes, User-Data, and HA Configuration
# =============================================================================
# File: infra/terraform/masters.tf

# ------------------------------------------------
# Master nodes (3 for HA)
# ------------------------------------------------
resource "hcloud_server" "k3s_master" {
  count        = var.master_count
  name         = "k3s-master-${count.index + 1}"
  image        = "ubuntu-22.04"
  server_type  = var.master_server_type
  datacenter   = var.region

  ssh_keys = [hcloud_ssh_key.crawler_ssh.id]

  network {
    network_id = hcloud_network.crawler_net.id
    ip         = "10.0.1.${10 + count.index}"
  }

  public_net {
    ipv4_enabled = true
    ipv6_enabled = false
  }

  volume {
    name   = "data-master-${count.index}"
    size   = var.data_volume_size
    format = "ext4"
    automount = false   # we will mount it via user-data
  }

  user_data = templatefile("${path.module}/templates/user_data_master.tpl", {
    role          = "master"
    cluster_token = random_password.k3s_token.result
    node_index    = count.index
    master_ip     = "10.0.1.${10 + count.index}"
  })

  labels = merge(local.common_labels, {
    role    = "master"
    cluster = var.cluster_name
  })

  firewall_ids = [hcloud_firewall.crawler_firewall.id]

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [
    hcloud_network_subnet.crawler_subnet,
    hcloud_network_subnet.crawler_subnet_backup,
    hcloud_firewall.crawler_firewall,
  ]
}

# ------------------------------------------------
# Template: user_data_master.tpl (embedded heredoc for Terraform)
# ------------------------------------------------
# Note: This file resides in infra/terraform/templates/user_data_master.tpl
# Here we include it as a resource for completeness, but Terraform
# expects it as a separate file. We show the content in a null_resource
# that creates the template file if it does not exist.

resource "local_file" "user_data_master_template" {
  filename = "${path.module}/templates/user_data_master.tpl"
  content  = <<-EOT
#!/bin/bash
set -euo pipefail

# ==========================================
# K3s Master Node Bootstrap Script
# ==========================================
# This script is executed by cloud-init on each master node.

ROLE="${role}"
CLUSTER_TOKEN="${cluster_token}"
NODE_INDEX=${node_index}
MASTER_IP="${master_ip}"
DATA_VOLUME="/dev/disk/by-id/scsi-0HC_Volume_data-master-''${NODE_INDEX}"

# Wait for data volume to appear
for i in {1..30}; do
    if [ -e $DATA_VOLUME ]; then
        break
    fi
    sleep 1
done

# Format and mount data volume if not already formatted
if [ -e $DATA_VOLUME ]; then
    blkid $DATA_VOLUME || mkfs.ext4 -F $DATA_VOLUME
    mkdir -p /mnt/data
    mount $DATA_VOLUME /mnt/data
    echo "$DATA_VOLUME /mnt/data ext4 defaults,noatime 0 2" >> /etc/fstab
fi

# ------------------------------------------------------
# Install prerequisites
# ------------------------------------------------------
apt-get update -y
apt-get install -y curl wget gnupg ca-certificates lsb-release jq nfs-common

# Disable swap (required by Kubernetes)
swapoff -a
sed -i '/ swap / s/^/#/' /etc/fstab

# Kernel settings for Kubernetes networking
cat <<EOF > /etc/sysctl.d/99-kubernetes-cri.conf
net.bridge.bridge-nf-call-iptables  = 1
net.ipv4.ip_forward                 = 1
net.bridge.bridge-nf-call-ip6tables = 1
EOF
sysctl --system

# ------------------------------------------------------
# Install K3s server (master)
# ------------------------------------------------------
# For the first master, use cluster-init; for subsequent masters,
# join the cluster pointed to the first master's IP.
FIRST_MASTER_IP="10.0.1.10"   # fixed IP of k3s-master-1

if [ $NODE_INDEX -eq 0 ]; then
    # First master
    curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server \\
        --cluster-init \\
        --token $CLUSTER_TOKEN \\
        --node-taint CriticalAddonsOnly=true:NoExecute \\
        --disable servicelb \\
        --disable traefik \\
        --write-kubeconfig-mode 644 \\
        --flannel-backend=wireguard-native \\
        --data-dir=/mnt/data/k3s" \\
        sh -
else
    # Other masters
    curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server \\
        --server https://$FIRST_MASTER_IP:6443 \\
        --token $CLUSTER_TOKEN \\
        --node-taint CriticalAddonsOnly=true:NoExecute \\
        --disable servicelb \\
        --disable traefik \\
        --write-kubeconfig-mode 644 \\
        --flannel-backend=wireguard-native \\
        --data-dir=/mnt/data/k3s" \\
        sh -
fi

# Wait for node to be Ready
sleep 10
until k3s kubectl get node $(hostname) -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' | grep -q True; do
    sleep 5
done

echo "Master node $(hostname) ready."
EOT
}

# ------------------------------------------------
# Output the generated template file path
# ------------------------------------------------
output "user_data_master_template_path" {
  value       = local_file.user_data_master_template.filename
  description = "Path to the generated master user-data template"
}

# ------------------------------------------------
# IAM / RBAC placeholder: Service accounts for K3s
# ------------------------------------------------
# Although these are Kubernetes resources, it is good practice to
# prepare manifests that will be applied after cluster bootstrap.
# We create a local file that can be applied manually or via CI/CD.

resource "local_file" "k3s_rbac_manifest" {
  filename = "${path.module}/manifests/k3s-rbac.yaml"
  content  = <<-EOT
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: crawler-admin
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: crawler-admin-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
- kind: ServiceAccount
  name: crawler-admin
  namespace: kube-system
EOT
}

# ------------------------------------------------
# Manifest for Longhorn (optional, CSI for volumes)
# ------------------------------------------------
resource "local_file" "longhorn_manifest" {
  filename = "${path.module}/manifests/longhorn.yaml"
  content  = <<-EOT
---
apiVersion: v1
kind: Namespace
metadata:
  name: longhorn-system
---
# Longhorn deployment requires Helm, this is just placeholder to remind
# that CSI driver must be installed for dynamic persistent volumes.
EOT
}

# ------------------------------------------------
# Kubernetes secret for pulling images (placeholder)
# ------------------------------------------------
resource "local_file" "dockerconfigjson" {
  filename = "${path.module}/manifests/dockerconfig.json"
  content  = jsonencode({
    auths = {
      "https://index.docker.io/v1/" = {
        auth = base64encode("${var.docker_username}:${var.docker_password}")
      }
    }
  })
}

variable "docker_username" {
  description = "Docker registry username"
  type        = string
  default     = ""
  sensitive   = true
}

variable "docker_password" {
  description = "Docker registry password"
  type        = string
  default     = ""
  sensitive   = true
}
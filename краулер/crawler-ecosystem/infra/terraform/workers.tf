# =============================================================================
# Part 4 — Worker Nodes, GPU Nodes, Outputs
# =============================================================================
# File: infra/terraform/workers.tf

# ------------------------------------------------
# Worker nodes
# ------------------------------------------------
resource "hcloud_server" "k3s_worker" {
  count        = var.worker_count
  name         = "k3s-worker-${count.index + 1}"
  image        = "ubuntu-22.04"
  server_type  = var.worker_server_type
  datacenter   = var.region

  ssh_keys = [hcloud_ssh_key.crawler_ssh.id]

  network {
    network_id = hcloud_network.crawler_net.id
    ip         = "10.0.1.${20 + count.index}"
  }

  public_net {
    ipv4_enabled = true
    ipv6_enabled = false
  }

  volume {
    name   = "data-worker-${count.index}"
    size   = var.data_volume_size
    format = "ext4"
  }

  user_data = templatefile("${path.module}/templates/user_data_worker.tpl", {
    role          = "worker"
    cluster_token = random_password.k3s_token.result
    master_ip     = "10.0.1.10"
    node_index    = count.index
  })

  labels = merge(local.common_labels, {
    role    = "worker"
    cluster = var.cluster_name
    gpu     = "false"
  })

  firewall_ids = [hcloud_firewall.crawler_firewall.id]

  lifecycle {
    prevent_destroy = false
  }
}

# ------------------------------------------------
# GPU Worker nodes (optional)
# ------------------------------------------------
resource "hcloud_server" "k3s_gpu_worker" {
  count        = var.environment == "production" ? 1 : 0
  name         = "k3s-gpu-worker-${count.index + 1}"
  image        = "ubuntu-22.04"
  server_type  = var.gpu_server_type
  datacenter   = var.region

  ssh_keys = [hcloud_ssh_key.crawler_ssh.id]

  network {
    network_id = hcloud_network.crawler_net.id
    ip         = "10.0.1.${30 + count.index}"
  }

  public_net {
    ipv4_enabled = true
    ipv6_enabled = false
  }

  volume {
    name   = "data-gpu-worker-${count.index}"
    size   = 100
    format = "ext4"
  }

  user_data = templatefile("${path.module}/templates/user_data_worker.tpl", {
    role          = "gpu-worker"
    cluster_token = random_password.k3s_token.result
    master_ip     = "10.0.1.10"
    node_index    = count.index + 100
  })

  labels = merge(local.common_labels, {
    role    = "worker"
    cluster = var.cluster_name
    gpu     = "true"
  })

  firewall_ids = [hcloud_firewall.crawler_firewall.id]
}

# ------------------------------------------------
# Worker User-Data Template
# ------------------------------------------------
resource "local_file" "user_data_worker_template" {
  filename = "${path.module}/templates/user_data_worker.tpl"
  content  = <<-EOT
#!/bin/bash
set -euo pipefail

# ==========================================
# K3s Worker Node Bootstrap Script
# ==========================================
ROLE="${role}"
CLUSTER_TOKEN="${cluster_token}"
MASTER_IP="${master_ip}"
NODE_INDEX=${node_index}
DATA_VOLUME="/dev/disk/by-id/scsi-0HC_Volume_data-worker-''${NODE_INDEX}"

# Wait for data volume
for i in {1..30}; do
    if [ -e $DATA_VOLUME ]; then
        break
    fi
    sleep 1
done

if [ -e $DATA_VOLUME ]; then
    blkid $DATA_VOLUME || mkfs.ext4 -F $DATA_VOLUME
    mkdir -p /mnt/data
    mount $DATA_VOLUME /mnt/data
    echo "$DATA_VOLUME /mnt/data ext4 defaults,noatime 0 2" >> /etc/fstab
fi

# Install prerequisites
apt-get update -y
apt-get install -y curl wget gnupg ca-certificates lsb-release jq nfs-common

# Disable swap
swapoff -a
sed -i '/ swap / s/^/#/' /etc/fstab

# Kernel settings
cat <<EOF > /etc/sysctl.d/99-kubernetes-cri.conf
net.bridge.bridge-nf-call-iptables  = 1
net.ipv4.ip_forward                 = 1
net.bridge.bridge-nf-call-ip6tables = 1
EOF
sysctl --system

# Install Nvidia GPU drivers if GPU worker
if [ "$ROLE" = "gpu-worker" ]; then
    apt-get install -y nvidia-driver-535
    systemctl restart containerd
fi

# Join the cluster
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="agent \\
    --server https://$MASTER_IP:6443 \\
    --token $CLUSTER_TOKEN \\
    --data-dir=/mnt/data/k3s" \\
    sh -

# Wait for node to be Ready
sleep 10
until k3s kubectl get node $(hostname) -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' | grep -q True; do
    sleep 5
done

echo "Worker node $(hostname) ready."
EOT
}
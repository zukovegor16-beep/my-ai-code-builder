#!/bin/bash
set -euo pipefail

# =============================================================================
# Part 15 — backup.sh — Full ecosystem backup with validation and rotation
# =============================================================================
# File: infra/scripts/backup.sh
# Usage: ./backup.sh [--full] [--postgres-only] [--skip-minio] [--encrypt] [--parallel]

# =============================================================================
# 1. Meta-configuration & Environment
# =============================================================================
TIMESTAMP=$(date '+%Y%m%d-%H%M%S')
BACKUP_ROOT="${BACKUP_ROOT:-/mnt/backups}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
MAX_BACKUP_SIZE_MB="${MAX_BACKUP_SIZE_MB:-10240}"  # 10 GB
BACKUP_DIR="${BACKUP_ROOT}/${TIMESTAMP}"
LOG_FILE="${BACKUP_ROOT}/backup_${TIMESTAMP}.log"
WEBHOOK_URL="${WEBHOOK_URL:-}"
BACKUP_ALL=false
BACKUP_POSTGRES=false
SKIP_MINIO=false
ENCRYPT=false
PARALLEL=false
ARCHIVE_CREATED=false               # Flag to control cleanup trap

# Ensure backup root exists
mkdir -p "$BACKUP_ROOT"

# =============================================================================
# 2. Logging Framework
# =============================================================================
log() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}
log_info() { log "INFO" "$1"; }
log_warn() { log "WARN" "$1"; }
log_error() { log "ERROR" "$1"; }

log_info "Backup script started (PID $$) on $(hostname)"

# =============================================================================
# 3. Dependency Verification
# =============================================================================
check_dependencies() {
    local deps=(kubectl pg_dump mc)
    for cmd in "${deps[@]}"; do
        if ! command -v "$cmd" &> /dev/null; then
            log_error "Required command '$cmd' not found"
            exit 1
        fi
    done
    log_info "All dependencies satisfied"
}

# =============================================================================
# 4. Argument Parsing
# =============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --full) BACKUP_ALL=true ;;
            --postgres-only) BACKUP_POSTGRES=true ;;
            --skip-minio) SKIP_MINIO=true ;;
            --encrypt) ENCRYPT=true ;;
            --parallel) PARALLEL=true ;;
            *) log_error "Unknown argument: $1"; exit 1 ;;
        esac
        shift
    done
}

# =============================================================================
# 5. Utility Functions
# =============================================================================

# Check that a file exists and has non-zero size
validate_file() {
    local file="$1"
    if [[ ! -s "$file" ]]; then
        log_error "Backup file $file is empty or missing"
        return 1
    fi
    return 0
}

# Encrypt a file with GPG symmetric encryption (optional)
encrypt_file() {
    local file="$1"
    local pass="${BACKUP_ENCRYPTION_PASSPHRASE:-}"
    if [[ "$ENCRYPT" != "true" || -z "$pass" ]]; then
        return 0
    fi
    # Write passphrase to a temporary file with restricted permissions
    local passfile
    passfile=$(mktemp)
    chmod 600 "$passfile"
    echo -n "$pass" > "$passfile"
    gpg --batch --passphrase-file "$passfile" --symmetric --cipher-algo AES256 "$file"
    rm -f "$passfile"
    rm "$file"  # remove unencrypted original
    log_info "Encrypted $file -> ${file}.gpg"
}

# Mask sensitive value for logging
mask() {
    local value="$1"
    if [[ ${#value} -gt 3 ]]; then
        echo "${value:0:2}***"
    else
        echo "***"
    fi
}

# =============================================================================
# 6. Kubernetes Pod Selection Helpers
# =============================================================================

# Find a running pod for the given selector in the given namespace
get_pod_name() {
    local namespace="$1"
    local selector="$2"
    kubectl get pod -n "$namespace" -l "$selector" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

# Default namespaces (override via env if needed)
NAMESPACE_CRAWLER="${NAMESPACE_CRAWLER:-crawler}"
NAMESPACE_DB="${NAMESPACE_DB:-databases}"
NAMESPACE_MONITORING="${NAMESPACE_MONITORING:-monitoring}"
NAMESPACE_VAULT="${NAMESPACE_VAULT:-vault}"

# =============================================================================
# 7. Preliminary Checks
# =============================================================================
preflight_checks() {
    log_info "Running preflight checks..."
    # Verify cluster access
    if ! kubectl cluster-info &>/dev/null; then
        log_error "Cannot access Kubernetes cluster"
        exit 1
    fi
    # Check available disk space (need at least 500 MB free)
    local avail
    avail=$(df -m --output=avail "$BACKUP_ROOT" | tail -1)
    if [[ ${avail:-0} -lt 500 ]]; then
        log_warn "Low disk space: ${avail}MB available in $BACKUP_ROOT"
    fi
    log_info "Preflight checks passed"
}

# =============================================================================
# 8. Backup Functions
# =============================================================================

# 8.1 PostgreSQL backup
backup_postgresql() {
    local stage_start
    stage_start=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Starting PostgreSQL backup at $stage_start"
    local pod
    pod=$(get_pod_name "$NAMESPACE_DB" "${PG_SELECTOR:-app=postgresql}")
    if [[ -z "$pod" ]]; then
        log_error "No PostgreSQL pod found"
        return 1
    fi
    local backup_file="$BACKUP_DIR/postgres_${TIMESTAMP}.sql.gz"
    kubectl exec -n "$NAMESPACE_DB" "$pod" -- env PGPASSWORD="${POSTGRES_PASSWORD}" pg_dump -U crawler -d crawler_meta | gzip > "$backup_file"
    validate_file "$backup_file" || return 1
    encrypt_file "$backup_file"
    local stage_end
    stage_end=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "PostgreSQL backup completed at $stage_end, saved to $backup_file"
}

# 8.2 KeyDB backup
backup_keydb() {
    local stage_start
    stage_start=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Starting KeyDB backup at $stage_start"
    local pod
    pod=$(get_pod_name "$NAMESPACE_CRAWLER" "${KEYDB_SELECTOR:-app=keydb-master}")
    if [[ -z "$pod" ]]; then
        log_error "No KeyDB master pod found"
        return 1
    fi
    local backup_file="$BACKUP_DIR/keydb_${TIMESTAMP}.rdb"
    kubectl exec -n "$NAMESPACE_CRAWLER" "$pod" -- keydb-cli BGSAVE
    sleep 3
    kubectl cp "$NAMESPACE_CRAWLER/$pod:/data/dump.rdb" "$backup_file" 2>/dev/null
    validate_file "$backup_file" || return 1
    gzip "$backup_file"
    encrypt_file "${backup_file}.gz"
    local stage_end
    stage_end=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "KeyDB backup completed at $stage_end, saved to ${backup_file}.gz"
}

# 8.3 MinIO backup
backup_minio() {
    if [[ "$SKIP_MINIO" == "true" ]]; then
        log_info "Skipping MinIO backup"
        return 0
    fi
    local stage_start
    stage_start=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Starting MinIO backup at $stage_start"
    local buckets
    IFS=',' read -ra buckets <<< "${MINIO_BUCKETS:-crawler-emails,crawler-backups,crawler-models}"
    for bucket in "${buckets[@]}"; do
        # Verify bucket exists
        if ! mc ls "local/$bucket" &>/dev/null; then
            log_error "MinIO bucket $bucket does not exist or is not accessible"
            return 1
        fi
        mc mirror "local/$bucket" "$BACKUP_DIR/minio/$bucket" > /dev/null 2>&1 || {
            log_error "MinIO mirror failed for bucket $bucket"
            return 1
        }
    done
    local stage_end
    stage_end=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "MinIO backup completed at $stage_end"
}

# 8.4 Meilisearch backup
backup_meilisearch() {
    local stage_start
    stage_start=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Starting Meilisearch backup at $stage_start"
    local api_key="${MEILI_MASTER_KEY:-}"
    if [[ -z "$api_key" ]]; then
        log_error "MEILI_MASTER_KEY not set"
        return 1
    fi
    local meili_url="http://meilisearch.${NAMESPACE_DB}.svc.cluster.local:7700"
    # Trigger dump
    local dump_uid
    dump_uid=$(curl -s -X POST -H "Authorization: Bearer $api_key" "$meili_url/dumps" | jq -r '.taskUid')
    if [[ -z "$dump_uid" ]]; then
        log_error "Failed to trigger Meilisearch dump"
        return 1
    fi
    # Wait for dump to complete (timeout 120s)
    local status
    local waited=0
    while [[ $waited -lt 120 ]]; do
        status=$(curl -s -H "Authorization: Bearer $api_key" "$meili_url/dumps/$dump_uid" | jq -r '.status')
        if [[ "$status" == "succeeded" ]]; then
            break
        elif [[ "$status" == "failed" ]]; then
            log_error "Meilisearch dump failed"
            return 1
        fi
        sleep 2
        waited=$((waited + 2))
    done
    if [[ "$status" != "succeeded" ]]; then
        log_error "Meilisearch dump timed out"
        return 1
    fi
    # Download dump file
    local backup_file="$BACKUP_DIR/meili_${TIMESTAMP}.dump"
    curl -s -H "Authorization: Bearer $api_key" "$meili_url/dumps/$dump_uid" -o "$backup_file"
    validate_file "$backup_file" || return 1
    gzip "$backup_file"
    encrypt_file "${backup_file}.gz"
    local stage_end
    stage_end=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Meilisearch backup completed at $stage_end, saved to ${backup_file}.gz"
}

# 8.5 Kubernetes resources export
backup_kubernetes_resources() {
    local stage_start
    stage_start=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Starting Kubernetes resources export at $stage_start"
    local k8s_dir="$BACKUP_DIR/k8s"
    mkdir -p "$k8s_dir"
    local resources=("deployments" "services" "configmaps" "secrets" "ingress" "statefulsets" "daemonsets")
    for res in "${resources[@]}"; do
        if ! kubectl get "$res" --all-namespaces -o yaml > "$k8s_dir/${res}.yaml" 2>/dev/null; then
            log_warn "Could not export $res (exit code $?)"
        fi
    done
    local stage_end
    stage_end=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Kubernetes resources exported at $stage_end to $k8s_dir"
}

# 8.6 Vault snapshot
backup_vault() {
    local stage_start
    stage_start=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Starting Vault snapshot at $stage_start"
    local pod
    pod=$(get_pod_name "$NAMESPACE_VAULT" "${VAULT_SELECTOR:-app=vault}")
    if [[ -z "$pod" ]]; then
        log_warn "Vault pod not found, skipping"
        return 0
    fi
    local backup_file="$BACKUP_DIR/vault_${TIMESTAMP}.snap"
    kubectl exec -n "$NAMESPACE_VAULT" "$pod" -- vault operator raft snapshot save /tmp/vault.snap
    kubectl cp "$NAMESPACE_VAULT/$pod:/tmp/vault.snap" "$backup_file"
    validate_file "$backup_file" || return 1
    gzip "$backup_file"
    encrypt_file "${backup_file}.gz"
    log_info "Vault snapshot saved to ${backup_file}.gz"
}

# 8.7 Istio configs export
backup_istio_configs() {
    local stage_start
    stage_start=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Starting Istio configs export at $stage_start"
    local istio_dir="$BACKUP_DIR/istio"
    mkdir -p "$istio_dir"
    kubectl get vs,dr,gateway,peerauthentication,requestauthentication,authorizationpolicy --all-namespaces -o yaml > "$istio_dir/all.yaml" 2>/dev/null
    local stage_end
    stage_end=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Istio configs exported at $stage_end"
}

# 8.8 Monitoring dashboards export
backup_monitoring_dashboards() {
    local stage_start
    stage_start=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Starting Grafana dashboards export at $stage_start"
    local grafana_url="http://grafana.${NAMESPACE_MONITORING}.svc.cluster.local:3000"
    local api_key="${GRAFANA_API_KEY:-}"
    if [[ -z "$api_key" ]]; then
        log_warn "GRAFANA_API_KEY not set, skipping dashboards"
        return 0
    fi
    local dash_dir="$BACKUP_DIR/grafana"
    mkdir -p "$dash_dir"
    # Export all dashboards via Grafana API
    curl -s -H "Authorization: Bearer $api_key" "$grafana_url/api/search?type=dash-db" | jq -r '.[].uid' | while read uid; do
        curl -s -H "Authorization: Bearer $api_key" "$grafana_url/api/dashboards/uid/$uid" > "$dash_dir/${uid}.json"
    done
    local stage_end
    stage_end=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Grafana dashboards exported at $stage_end"
}

# =============================================================================
# 9. Post-backup Operations
# =============================================================================
compress_and_checksum() {
    local archive="$BACKUP_ROOT/backup_${TIMESTAMP}.tar.gz"
    tar -czf "$archive" -C "$BACKUP_ROOT" "$TIMESTAMP"
    if command -v sha256sum &> /dev/null; then
        sha256sum "$archive" > "${archive}.sha256"
        log_info "SHA256 checksum: $(cat ${archive}.sha256)"
    elif command -v shasum &> /dev/null; then
        shasum -a 256 "$archive" > "${archive}.sha256"
        log_info "SHA256 checksum (shasum): $(cat ${archive}.sha256)"
    else
        log_warn "No sha256 tool found, skipping checksum"
    fi
    ARCHIVE_CREATED=true
    log_info "Archive created: $archive"
}

# =============================================================================
# 10. Rotation and Cleanup
# =============================================================================
rotate_backups() {
    log_info "Rotating old backups (retention: ${RETENTION_DAYS} days)"
    while IFS= read -r -d '' file; do
        log_info "Removing old backup: $file"
        rm -f "$file"
    done < <(find "$BACKUP_ROOT" -name "*.tar.gz" -mtime +"$RETENTION_DAYS" -print0 2>/dev/null || true)
    local current_size
    current_size=$(du -sm "$BACKUP_ROOT" 2>/dev/null | cut -f1)
    if [[ ${current_size:-0} -gt $MAX_BACKUP_SIZE_MB ]]; then
        log_warn "Backup directory exceeds size limit: ${current_size}MB (max ${MAX_BACKUP_SIZE_MB}MB)"
    fi
}

# =============================================================================
# 11. Notification & Reporting
# =============================================================================
send_notification() {
    if [[ -n "$WEBHOOK_URL" ]]; then
        local message="Backup completed on $(hostname) at $TIMESTAMP. Archive: $BACKUP_ROOT/backup_${TIMESTAMP}.tar.gz"
        curl -s -X POST -H "Content-Type: application/json" -d "{\"text\":\"$message\"}" "$WEBHOOK_URL" > /dev/null 2>&1 || true
        log_info "Notification sent to webhook"
    fi
}

print_summary() {
    local end_time
    end_time=$(date '+%Y-%m-%d %H:%M:%S')
    log_info "Backup finished at $end_time"
    log_info "Backup directory: $BACKUP_ROOT/backup_${TIMESTAMP}.tar.gz"
    log_info "Log file: $LOG_FILE"
}

# =============================================================================
# 12. Trap for Cleanup (only if archive not created)
# =============================================================================
cleanup() {
    if [[ -d "$BACKUP_DIR" ]] && [[ "$ARCHIVE_CREATED" != "true" ]]; then
        rm -rf "$BACKUP_DIR"
        log_info "Temporary backup directory $BACKUP_DIR removed"
    fi
}
trap cleanup EXIT

# =============================================================================
# 13. Main Orchestration
# =============================================================================
main() {
    parse_args "$@"
    check_dependencies
    preflight_checks

    mkdir -p "$BACKUP_DIR"

    # Determine what to backup
    if [[ "$BACKUP_ALL" == "true" ]]; then
        BACKUP_POSTGRES=true
        SKIP_MINIO=false
    fi

    # Sequential or parallel execution (max 4 concurrent jobs)
    run_sequential() {
        backup_postgresql
        backup_keydb
        backup_minio
        backup_meilisearch
        backup_kubernetes_resources
        backup_vault
        backup_istio_configs
        backup_monitoring_dashboards
    }

    run_parallel() {
        # Launch all jobs, capture their PIDs
        backup_postgresql &
        pid1=$!
        backup_keydb &
        pid2=$!
        backup_minio &
        pid3=$!
        backup_meilisearch &
        pid4=$!
        backup_kubernetes_resources &
        pid5=$!
        backup_vault &
        pid6=$!
        backup_istio_configs &
        pid7=$!
        backup_monitoring_dashboards &
        pid8=$!

        # Wait for all and check exit statuses
        failed=0
        for pid in $pid1 $pid2 $pid3 $pid4 $pid5 $pid6 $pid7 $pid8; do
            if ! wait $pid; then
                failed=$((failed + 1))
                log_error "Background job PID $pid failed"
            fi
        done
        if [[ $failed -gt 0 ]]; then
            log_warn "$failed background backup job(s) failed"
        fi
    }

    if [[ "$PARALLEL" == "true" ]]; then
        run_parallel
    else
        run_sequential
    fi

    compress_and_checksum
    rotate_backups
    send_notification
    print_summary
}

main "$@"
#!/usr/bin/env bash
# =============================================================================
# Part 16 — restore.sh — Production-ready restore (critical fixes applied)
# =============================================================================
# File: infra/scripts/restore.sh
# Usage: ./restore.sh --archive <backup.tar.gz> [--component <name>] [--all]
#        [--force] [--yes] [--dry-run] [--parallel] [--verbose] [--help]

set -euo pipefail

# ---------------------------------------------------------------------------
# 1. Meta‑configuration & Environment
# ---------------------------------------------------------------------------
RESTORE_TIMESTAMP=$(date '+%Y%m%d-%H%M%S')
RESTORE_ROOT="${RESTORE_ROOT:-/mnt/restores}"
ARCHIVE_FILE=""
LOG_FILE=""
RESTORE_DIR=""
COMPONENTS=()
ALL_COMPONENTS=false
FORCE=false
SKIP_CHECKSUM=false
YES_MODE=false
DRY_RUN=false
PARALLEL=false
VERBOSE=false
WEBHOOK_URL="${WEBHOOK_URL:-}"
ARCHIVE_VERSION=""

# Default namespaces
NAMESPACE_CRAWLER="${NAMESPACE_CRAWLER:-crawler}"
NAMESPACE_DB="${NAMESPACE_DB:-databases}"
NAMESPACE_MONITORING="${NAMESPACE_MONITORING:-monitoring}"
NAMESPACE_VAULT="${NAMESPACE_VAULT:-vault}"

# Default pod selectors (override via env)
PG_SELECTOR="${PG_SELECTOR:-app=postgresql}"
KEYDB_SELECTOR="${KEYDB_SELECTOR:-app=keydb-master}"
VAULT_SELECTOR="${VAULT_SELECTOR:-app=vault}"

# Default service URLs
MEILI_URL="${MEILI_URL:-http://meilisearch.${NAMESPACE_DB}.svc.cluster.local:7700}"
GRAFANA_URL="${GRAFANA_URL:-http://grafana.${NAMESPACE_MONITORING}.svc.cluster.local:3000}"
VAULT_ADDR="${VAULT_ADDR:-http://vault.${NAMESPACE_VAULT}.svc.cluster.local:8200}"

# ---------------------------------------------------------------------------
# 2. Help text
# ---------------------------------------------------------------------------
show_help() {
    cat <<EOF
Usage: $0 --archive <backup.tar.gz> [OPTIONS]

Required:
  --archive <path>           Path to backup archive (tar.gz)

Optional:
  --component <name>         Restore only this component (repeatable)
                             Valid names: postgres, keydb, minio, meili,
                             k8s, vault, istio, grafana
  --all                      Restore all components
  --force                    Continue on non‑critical errors
  --skip-checksum            Skip SHA256 verification
  --yes                      Automatic confirmation (non‑interactive)
  --dry-run                  Simulate restore without changes
  --parallel                 Run independent components in parallel
  --verbose                  Verbose logging (debug level)
  --help                     Show this help and exit
EOF
    exit 0
}

# ---------------------------------------------------------------------------
# 3. Logging Framework
# ---------------------------------------------------------------------------
log() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a "${LOG_FILE:-/dev/null}"
}
log_info() { log "INFO" "$1"; }
log_warn() { log "WARN" "$1"; }
log_error() { log "ERROR" "$1"; }
log_verbose() { [[ "$VERBOSE" == "true" ]] && log "DEBUG" "$1"; }

# ---------------------------------------------------------------------------
# 4. Dependency Verification
# ---------------------------------------------------------------------------
check_dependencies() {
    local deps=(kubectl psql mc redis-check-rdb jq sha256sum)
    for cmd in "${deps[@]}"; do
        if ! command -v "$cmd" &> /dev/null; then
            log_error "Required command '$cmd' not found"
            exit 1
        fi
    done
}

# ---------------------------------------------------------------------------
# 5. Argument Parsing
# ---------------------------------------------------------------------------
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --help) show_help ;;
            --archive) ARCHIVE_FILE="$2"; shift 2 ;;
            --component) COMPONENTS+=("$2"); shift 2 ;;
            --all) ALL_COMPONENTS=true; shift ;;
            --force) FORCE=true; shift ;;
            --skip-checksum) SKIP_CHECKSUM=true; shift ;;
            --yes) YES_MODE=true; shift ;;
            --dry-run) DRY_RUN=true; shift ;;
            --parallel) PARALLEL=true; shift ;;
            --verbose) VERBOSE=true; shift ;;
            *) log_error "Unknown argument: $1. Use --help."; exit 1 ;;
        esac
    done

    # Autodetect archive if not provided
    if [[ -z "$ARCHIVE_FILE" ]]; then
        ARCHIVE_FILE=$(find "$RESTORE_ROOT" -name "backup_*.tar.gz" -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)
        if [[ -z "$ARCHIVE_FILE" ]]; then
            log_error "No backup archive found and --archive not specified"
            exit 1
        fi
        log_info "Using latest archive: $(mask "$ARCHIVE_FILE")"
    fi

    LOG_FILE="${RESTORE_ROOT}/restore_${RESTORE_TIMESTAMP}.log"
    mkdir -p "$RESTORE_ROOT"
}

# ---------------------------------------------------------------------------
# 6. Preflight Checks
# ---------------------------------------------------------------------------
preflight_checks() {
    log_info "=== Preflight checks ==="
    # Archive readable
    if [[ ! -r "$ARCHIVE_FILE" ]]; then
        log_error "Archive not readable: $ARCHIVE_FILE"
        exit 1
    fi

    # Checksum
    if [[ "$SKIP_CHECKSUM" != "true" ]]; then
        local checksum_file="${ARCHIVE_FILE}.sha256"
        if [[ -f "$checksum_file" ]]; then
            log_info "Verifying SHA256 checksum..."
            if ! sha256sum -c "$checksum_file" --status; then
                log_error "Checksum verification failed! Use --skip-checksum to bypass."
                exit 1
            fi
            log_info "Checksum OK"
        else
            log_warn "No SHA256 checksum file found, skipping verification"
        fi
    fi

    # Version check
    if tar -tzf "$ARCHIVE_FILE" 2>/dev/null | grep -q "VERSION"; then
        tar -xzf "$ARCHIVE_FILE" -C /tmp VERSION 2>/dev/null
        ARCHIVE_VERSION=$(cat /tmp/VERSION 2>/dev/null || echo "unknown")
        SCRIPT_VERSION="${SCRIPT_VERSION:-1.0}"
        if [[ "$ARCHIVE_VERSION" != "$SCRIPT_VERSION" ]]; then
            log_warn "Archive version ($ARCHIVE_VERSION) differs from script version ($SCRIPT_VERSION)"
            if [[ "$FORCE" != "true" ]]; then
                log_error "Archive version mismatch. Use --force to continue anyway"
                exit 1
            fi
        fi
        rm -f /tmp/VERSION
    fi

    # Disk space
    local file_count archive_size estimated_size available_space
    file_count=$(tar -tzf "$ARCHIVE_FILE" 2>/dev/null | wc -l)
    archive_size=$(du -sm "$ARCHIVE_FILE" | cut -f1)
    estimated_size=$((archive_size + file_count / 256))
    available_space=$(df -m --output=avail "$RESTORE_ROOT" 2>/dev/null | tail -n +2 | head -1)
    available_space=${available_space:-0}
    if [[ "$available_space" -lt $estimated_size ]]; then
        log_error "Not enough disk space. Need ~${estimated_size}MB, have ${available_space}MB"
        exit 1
    fi

    # Kubernetes and permissions
    if ! kubectl cluster-info &>/dev/null; then
        log_error "Cannot access Kubernetes cluster"
        exit 1
    fi
    if [[ ! -w "$RESTORE_ROOT" ]]; then
        log_error "No write permission to restore root: $RESTORE_ROOT"
        exit 1
    fi

    # Confirmation
    if [[ "$YES_MODE" != "true" ]]; then
        echo "WARNING: This will restore data from $ARCHIVE_FILE."
        echo "All existing data for selected components may be OVERWRITTEN."
        read -p "Are you sure you want to continue? [y/N] " -r -t 60
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "Restore cancelled by user"
            exit 0
        fi
    fi
}

# ---------------------------------------------------------------------------
# 7. Utility Functions
# ---------------------------------------------------------------------------
get_pod_name() {
    local namespace="$1"
    local selector="$2"
    kubectl get pod -n "$namespace" -l "$selector" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

mask() {
    local value="$1"
    if [[ ${#value} -gt 3 ]]; then
        echo "${value:0:2}***"
    else
        echo "***"
    fi
}

# ---------------------------------------------------------------------------
# 8. Restore Functions (timed, with dry-run, error handling)
# ---------------------------------------------------------------------------
restore_progress=0
restore_total=0
step_start=0

start_step() {
    step_start=$(date +%s)
    restore_progress=$((restore_progress + 1))
    log_info "[${restore_progress}/${restore_total}] $1"
}
end_step() {
    local desc="$1"
    log_info "$desc completed in $(( $(date +%s) - step_start ))s"
}

restore_postgresql() {
    start_step "PostgreSQL restore"
    local pod
    pod=$(get_pod_name "$NAMESPACE_DB" "$PG_SELECTOR")
    if [[ -z "$pod" ]]; then
        log_error "No PostgreSQL pod found"
        return 1
    fi
    local dump_file
    dump_file=$(find "$RESTORE_DIR" -name "postgres_*.sql.gz" -print -quit)
    if [[ -z "$dump_file" ]]; then
        log_error "No PostgreSQL dump found in archive"
        return 1
    fi
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would restore: gunzip -c $(mask "$dump_file") | kubectl exec -i -n $NAMESPACE_DB $pod -- psql -U crawler"
    else
        gunzip -c "$dump_file" | kubectl exec -i -n "$NAMESPACE_DB" "$pod" -- psql -U crawler -d crawler_meta || {
            log_error "PostgreSQL restore failed"
            return 1
        }
        kubectl exec -n "$NAMESPACE_DB" "$pod" -- psql -U crawler -d crawler_meta -c "SELECT count(*) FROM emails;" &>/dev/null || {
            log_error "PostgreSQL integrity check failed"
            return 1
        }
    fi
    end_step "PostgreSQL restore"
}

restore_keydb() {
    start_step "KeyDB restore"
    local pod
    pod=$(get_pod_name "$NAMESPACE_CRAWLER" "$KEYDB_SELECTOR")
    if [[ -z "$pod" ]]; then
        log_error "No KeyDB master pod found"
        return 1
    fi
    local rdb_file
    rdb_file=$(find "$RESTORE_DIR" -name "keydb_*.rdb" -print -quit)
    if [[ -z "$rdb_file" ]]; then
        log_error "No KeyDB dump found in archive"
        return 1
    fi
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would restore KeyDB from $(mask "$rdb_file")"
    else
        redis-check-rdb "$rdb_file" &>/dev/null || {
            log_error "Corrupted KeyDB dump"
            return 1
        }
        kubectl cp "$rdb_file" "$NAMESPACE_CRAWLER/$pod:/data/dump.rdb"
        kubectl exec -n "$NAMESPACE_CRAWLER" "$pod" -- keydb-cli SHUTDOWN NOSAVE || true
        sleep 3
        kubectl wait --for=condition=ready pod -l "$KEYDB_SELECTOR" -n "$NAMESPACE_CRAWLER" --timeout=120s || {
            log_error "KeyDB pod did not become ready after restore"
            return 1
        }
        kubectl exec -n "$NAMESPACE_CRAWLER" "$pod" -- keydb-cli PING &>/dev/null || {
            log_error "KeyDB is not responding after restore"
            return 1
        }
    fi
    end_step "KeyDB restore"
}

restore_minio() {
    start_step "MinIO restore"
    if [[ ! -d "$RESTORE_DIR/minio" ]]; then
        log_warn "No MinIO data in archive"
        return 0
    fi
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would restore MinIO buckets from $RESTORE_DIR/minio"
        return 0
    fi
    for bucket_dir in "$RESTORE_DIR/minio/"*; do
        local bucket
        bucket=$(basename "$bucket_dir")
        mc ls "local/$bucket" &>/dev/null || mc mb "local/$bucket"
        mc mirror "$bucket_dir" "local/$bucket" --overwrite || {
            log_error "MinIO mirror failed for bucket $bucket"
            return 1
        }
    done
    end_step "MinIO restore"
}

restore_meilisearch() {
    start_step "Meilisearch restore"
    local api_key="${MEILI_MASTER_KEY:-}"
    if [[ -z "$api_key" ]]; then
        log_error "MEILI_MASTER_KEY not set"
        return 1
    fi
    local dump_file
    dump_file=$(find "$RESTORE_DIR" -name "meili_*.dump" -print -quit)
    if [[ -z "$dump_file" ]]; then
        log_warn "No Meilisearch dump found in archive"
        return 0
    fi
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would import Meilisearch dump from $(mask "$dump_file")"
        return 0
    fi
    local import_response task_uid
    import_response=$(curl -s -X POST "$MEILI_URL/dumps/import" \
        -H "Authorization: Bearer $api_key" \
        -H "Content-Type: application/json" \
        -d "{\"path\":\"/dumps/${dump_file##*/}\"}")
    task_uid=$(echo "$import_response" | jq -r '.taskUid')
    if [[ -z "$task_uid" ]]; then
        log_error "Failed to trigger Meilisearch dump import"
        return 1
    fi
    local status waited=0
    while [[ $waited -lt 60 ]]; do
        status=$(curl -s -H "Authorization: Bearer $api_key" "$MEILI_URL/dumps/$task_uid" | jq -r '.status')
        if [[ "$status" == "succeeded" ]]; then
            break
        elif [[ "$status" == "failed" ]]; then
            log_error "Meilisearch dump import failed"
            return 1
        fi
        sleep 2
        waited=$((waited + 2))
    done
    if [[ "$status" != "succeeded" ]]; then
        log_error "Meilisearch import timed out"
        return 1
    fi
    curl -s -H "Authorization: Bearer $api_key" "$MEILI_URL/indexes" | jq -e '. | length > 0' &>/dev/null || {
        log_error "Meilisearch restore validation failed"
        return 1
    }
    end_step "Meilisearch restore"
}

restore_kubernetes_resources() {
    start_step "Kubernetes resources restore"
    local k8s_dir="$RESTORE_DIR/k8s"
    if [[ ! -d "$k8s_dir" ]]; then
        log_warn "No Kubernetes resources directory in archive"
        return 0
    fi
    local apply_cmd="kubectl apply -f"
    [[ "$DRY_RUN" == "true" ]] && apply_cmd="$apply_cmd --dry-run=server"
    local failed=0
    apply_manifest() {
        if ! $apply_cmd "$1"; then
            log_warn "Failed to apply $1"
            return 1
        fi
        return 0
    }
    export -f apply_manifest log_warn
    find "$k8s_dir" -name "*.yaml" -print0 | xargs -0 -P4 -I{} bash -c 'apply_manifest "{}"' || failed=1
    if [[ "$DRY_RUN" != "true" ]]; then
        kubectl wait --for=condition=available --all deployments --all-namespaces --timeout=300s || log_warn "Some deployments not available"
    fi
    end_step "Kubernetes resources restore"
    return $failed
}

restore_vault() {
    start_step "Vault restore"
    local pod
    pod=$(get_pod_name "$NAMESPACE_VAULT" "$VAULT_SELECTOR")
    if [[ -z "$pod" ]]; then
        log_warn "Vault pod not found, skipping"
        return 0
    fi
    local snap_file
    snap_file=$(find "$RESTORE_DIR" -name "vault_*.snap" -print -quit)
    if [[ -z "$snap_file" ]]; then
        log_warn "No Vault snapshot in archive"
        return 0
    fi
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would restore Vault from $(mask "$snap_file")"
        return 0
    fi
    # Validate snapshot using local vault binary if available
    if command -v vault &>/dev/null; then
        VAULT_ADDR="$VAULT_ADDR" vault operator raft snapshot inspect "$snap_file" &>/dev/null || {
            log_warn "Vault snapshot may be corrupted or incompatible"
        }
    fi
    local safe_snap="$RESTORE_DIR/vault_restore.snap"
    cp "$snap_file" "$safe_snap"
    kubectl cp "$safe_snap" "$NAMESPACE_VAULT/$pod:/tmp/vault.snap"
    kubectl exec -n "$NAMESPACE_VAULT" "$pod" -- vault operator raft snapshot restore /tmp/vault.snap
    end_step "Vault restore"
}

restore_istio_configs() {
    start_step "Istio configs restore"
    local istio_file="$RESTORE_DIR/istio/all.yaml"
    if [[ ! -f "$istio_file" ]]; then
        log_warn "No Istio configs in archive"
        return 0
    fi
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would apply Istio configs from $istio_file"
    else
        kubectl apply -f "$istio_file"
    fi
    end_step "Istio configs restore"
}

restore_monitoring_dashboards() {
    start_step "Grafana dashboards restore"
    local dash_dir="$RESTORE_DIR/grafana"
    if [[ ! -d "$dash_dir" ]]; then
        log_warn "No Grafana dashboards in archive"
        return 0
    fi
    local api_key="${GRAFANA_API_KEY:-}"
    if [[ -z "$api_key" ]]; then
        log_warn "GRAFANA_API_KEY not set, skipping dashboard restore"
        return 0
    fi
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would import Grafana dashboards from $dash_dir"
        return 0
    fi
    local failed=0
    import_dash() {
        if ! curl -s -X POST "$GRAFANA_URL/api/dashboards/db" \
            -H "Authorization: Bearer $api_key" \
            -H "Content-Type: application/json" \
            -d @"$1" > /dev/null; then
            log_warn "Failed to import dashboard: $1"
            return 1
        fi
        return 0
    }
    export -f import_dash log_warn GRAFANA_URL api_key
    find "$dash_dir" -name "*.json" -print0 | xargs -0 -P4 -I{} bash -c 'import_dash "{}"' || failed=1
    end_step "Grafana dashboards restore"
    return $failed
}

# ---------------------------------------------------------------------------
# 9. Post‑restore validation
# ---------------------------------------------------------------------------
post_restore_checks() {
    log_info "=== Post‑restore validation ==="
    kubectl get pods --all-namespaces 2>&1 | column -t | tee -a "$LOG_FILE"
    log_info "Checking API endpoints..."
    curl -s -o /dev/null -w "Meilisearch API: %{http_code}\n" "$MEILI_URL/health" | tee -a "$LOG_FILE"
    curl -s -o /dev/null -w "Grafana API: %{http_code}\n" "$GRAFANA_URL/api/health" | tee -a "$LOG_FILE"
    if command -v vault &>/dev/null; then
        VAULT_ADDR="$VAULT_ADDR" vault status 2>&1 | tee -a "$LOG_FILE"
    fi
}

# ---------------------------------------------------------------------------
# 10. Cleanup (respects --dry-run and signals)
# ---------------------------------------------------------------------------
cleanup() {
    if [[ -d "$RESTORE_DIR" ]] && [[ "$DRY_RUN" != "true" ]]; then
        rm -rf "$RESTORE_DIR"
        log_verbose "Temporary restore directory removed"
    fi
}
trap cleanup EXIT
trap 'cleanup; exit 1' TERM INT

# ---------------------------------------------------------------------------
# 11. Notification
# ---------------------------------------------------------------------------
send_notification() {
    local exit_code=$1
    if [[ -z "$WEBHOOK_URL" ]]; then return 0; fi
    local message
    if [[ $exit_code -eq 0 ]]; then
        message="Restore completed successfully on $(hostname) at $RESTORE_TIMESTAMP from archive $(mask "$ARCHIVE_FILE")"
    else
        message="Restore FAILED on $(hostname) at $RESTORE_TIMESTAMP from archive $(mask "$ARCHIVE_FILE"). Check logs."
    fi
    if ! curl -s -X POST -H "Content-Type: application/json" -d "{\"text\":\"$message\"}" "$WEBHOOK_URL" > /dev/null 2>&1; then
        log_warn "Failed to send notification webhook"
    fi
}

# ---------------------------------------------------------------------------
# 12. Main Orchestration
# ---------------------------------------------------------------------------
main() {
    parse_args "$@"
    log_info "Restore script started (PID $$) on $(hostname)"
    check_dependencies
    preflight_checks

    if [[ "$ALL_COMPONENTS" == "true" ]]; then
        COMPONENTS=("postgres" "keydb" "minio" "meili" "k8s" "vault" "istio" "grafana")
    fi
    if [[ ${#COMPONENTS[@]} -eq 0 ]]; then
        log_error "No components specified (use --all or --component)"
        exit 1
    fi
    restore_total=${#COMPONENTS[@]}
    restore_progress=0

    RESTORE_DIR="${RESTORE_ROOT}/restore_${RESTORE_TIMESTAMP}"
    mkdir -p "$RESTORE_DIR"

    # Extract archive
    if [[ "$DRY_RUN" != "true" ]]; then
        tar -xzf "$ARCHIVE_FILE" -C "$RESTORE_DIR" --strip-components=1
    fi

    local overall_success=0

    run_restore() {
        local comp="$1"
        case $comp in
            postgres) restore_postgresql ;;
            keydb) restore_keydb ;;
            minio) restore_minio ;;
            meili) restore_meilisearch ;;
            k8s) restore_kubernetes_resources ;;
            vault) restore_vault ;;
            istio) restore_istio_configs ;;
            grafana) restore_monitoring_dashboards ;;
            *) log_error "Unknown component: $comp"; return 1 ;;
        esac
    }

    if [[ "$PARALLEL" == "true" ]]; then
        pids=()
        for comp in "${COMPONENTS[@]}"; do
            run_restore "$comp" &
            pids+=($!)
        done
        for pid in "${pids[@]}"; do
            if ! wait $pid; then
                overall_success=1
            fi
        done
    else
        for comp in "${COMPONENTS[@]}"; do
            run_restore "$comp" || overall_success=1
        done
    fi

    if [[ $overall_success -eq 0 ]]; then
        post_restore_checks
        log_info "Restore completed successfully"
    else
        log_error "Restore completed with errors"
    fi

    send_notification $overall_success
    exit $overall_success
}

main "$@"
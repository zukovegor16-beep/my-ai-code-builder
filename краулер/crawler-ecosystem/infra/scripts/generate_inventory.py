#!/usr/bin/env python3
# =============================================================================
# Part 14 — Inventory generation script (production‑grade, v1.3)
# =============================================================================
# File: infra/scripts/generate_inventory.py

import json
import subprocess
import os
import sys
import logging
import ipaddress
import argparse
import time
import re
import shlex
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# ---------------------------------------------------------------------------
# Semantic version comparison (optional)
# ---------------------------------------------------------------------------
try:
    from packaging import version as _version
except ImportError:
    _version = None
    logging.getLogger(__name__).warning(
        "Module 'packaging' not found. Version comparison will be skipped."
    )

# ---------------------------------------------------------------------------
# Configuration (override via environment variables)
# ---------------------------------------------------------------------------
def _validate_path(raw: str, name: str = "path") -> str:
    """Validate that a filesystem path contains only safe characters."""
    if not re.match(r"^[\w/\.\-~\\:]+$", raw):
        raise ValueError(f"Unsafe characters in {name}: {raw}")
    return raw


TF_DIR = _validate_path(os.getenv("TF_DIR", "infra/terraform"), "TF_DIR")
ANSIBLE_INVENTORY = _validate_path(
    os.getenv("ANSIBLE_INVENTORY", "infra/ansible/inventory.ini"),
    "ANSIBLE_INVENTORY",
)
SSH_KEY = os.path.expanduser(
    _validate_path(os.getenv("SSH_KEY", "~/.ssh/id_rsa"), "SSH_KEY")
)
SSH_USER = os.getenv("SSH_USER", "ubuntu")
SSH_PORT = int(os.getenv("SSH_PORT", "22"))
SSH_TIMEOUT = int(os.getenv("SSH_TIMEOUT", "5"))
PING_CHECK = os.getenv("PING_CHECK", "0") == "1"
PING_METHOD = os.getenv("PING_METHOD", "ping")
PING_RETRIES = int(os.getenv("PING_RETRIES", "3"))
PING_RETRY_DELAY = int(os.getenv("PING_RETRY_DELAY", "1"))
CACHE_MAX_AGE_HOURS = int(os.getenv("CACHE_MAX_AGE_HOURS", "24"))
CACHE_MAX_SIZE_MB = int(os.getenv("CACHE_MAX_SIZE_MB", "100"))

CACHE_FILE = Path(".terraform_output_cache.json")

# Validate numerical configuration
if CACHE_MAX_AGE_HOURS < 0:
    print("Error: CACHE_MAX_AGE_HOURS must be non‑negative", file=sys.stderr)
    sys.exit(1)
if CACHE_MAX_SIZE_MB < 0:
    print("Error: CACHE_MAX_SIZE_MB must be non‑negative", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Helper to load JSON environment variables safely
# ---------------------------------------------------------------------------
def _load_json_env(name: str, default: dict) -> dict:
    """Return parsed JSON from environment variable *name*, falling back to *default*."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        log = logging.getLogger(__name__)
        log.error("Invalid JSON in %s: %s. Aborting.", name, e)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Default mappings (validated on load)
# ---------------------------------------------------------------------------
TF_OUTPUT_MAPPING = _load_json_env(
    "TF_OUTPUT_MAPPING",
    {
        "masters": "master_ips",
        "workers": "worker_ips",
        "gpu_workers": "gpu_worker_ips",
    },
)

HOST_PREFIXES = _load_json_env(
    "HOST_PREFIXES",
    {
        "masters": "k3s-master",
        "workers": "k3s-worker",
        "gpu_workers": "k3s-gpu-worker",
    },
)

ANSIBLE_EXTRA_VARS = _load_json_env("ANSIBLE_EXTRA_VARS", {})

if not TF_OUTPUT_MAPPING:
    print("Error: TF_OUTPUT_MAPPING must not be empty", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Command‑line arguments
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse and return command‑line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate Ansible inventory from Terraform outputs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print inventory to stdout without writing file",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Force re‑fetch of Terraform outputs",
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        help="Do not include metadata header in inventory",
    )
    parser.add_argument(
        "--terraform-timeout",
        type=int,
        default=30,
        help="Timeout for Terraform commands in seconds",
    )
    parser.add_argument(
        "--ping-timeout",
        type=int,
        default=3,
        help="Timeout for ping commands in seconds",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level",
    )
    parser.add_argument(
        "--no-duplicate-check",
        action="store_true",
        help="Skip duplicate IP address check between sections",
    )
    parser.add_argument(
        "--cache-debug",
        action="store_true",
        help="Print cache file age and size information",
    )
    parser.add_argument(
        "--no-retries",
        action="store_true",
        help="Disable retry attempts for host reachability checks",
    )
    parser.add_argument(
        "--force-overwrite",
        action="store_true",
        help="Silently overwrite existing inventory file",
    )
    args = parser.parse_args()
    if args.dry_run and args.no_cache:
        log.warning(
            "--dry-run and --no-cache used together – this may be unintended."
        )
    return args


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------
_ip_cache: Dict[str, bool] = {}

def validate_ip(ip: str) -> bool:
    """Check if *ip* is a valid IPv4 or IPv6 address.  Results are cached."""
    if ip in _ip_cache:
        return _ip_cache[ip]
    try:
        ipaddress.ip_address(ip)
        _ip_cache[ip] = True
        return True
    except ValueError:
        _ip_cache[ip] = False
        return False


def check_ssh_key() -> None:
    """Validate SSH private key path and permissions."""
    key_path = Path(SSH_KEY)
    if key_path.is_dir():
        raise IsADirectoryError(f"SSH key path is a directory: {SSH_KEY}")
    if not key_path.exists():
        raise FileNotFoundError(f"SSH key not found: {SSH_KEY}")
    permissions = key_path.stat().st_mode & 0o777
    if permissions not in (0o600, 0o400):
        log.warning(
            "SSH key permissions (%o) are not secure. Consider chmod 600 %s",
            permissions,
            SSH_KEY,
        )
    log.info("SSH key validated: %s", SSH_KEY)


_terraform_version_checked = False

def check_terraform_version(min_version: str = "1.0") -> None:
    """Ensure Terraform is installed and optionally check minimum version."""
    global _terraform_version_checked
    if _terraform_version_checked:
        return

    try:
        result = subprocess.run(
            ["terraform", "version"], capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            raise RuntimeError("Terraform not found or not executable")
        match = re.search(r"v?(\d+\.\d+\.\d+)", result.stdout)
        if not match:
            log.warning("Could not parse Terraform version from output")
            return
        current = match.group(1)
        if _version and _version.parse(current) < _version.parse(min_version):
            log.warning(
                "Terraform version %s is below recommended %s",
                current,
                min_version,
            )
        _terraform_version_checked = True
        log.info("Terraform version %s validated.", current)
    except Exception as e:
        log.error("Terraform check failed: %s", e)
        raise


def cleanup_old_cache() -> None:
    """Remove cache file if it is too old or too large."""
    if not CACHE_FILE.exists():
        return

    mtime = CACHE_FILE.stat().st_mtime
    age = datetime.now() - datetime.fromtimestamp(mtime)

    try:
        if age > timedelta(hours=CACHE_MAX_AGE_HOURS):
            CACHE_FILE.unlink()
            log.info("Old cache file removed: %s", CACHE_FILE)
        elif CACHE_FILE.stat().st_size / (1024 * 1024) > CACHE_MAX_SIZE_MB:
            CACHE_FILE.unlink()
            log.info("Cache file too large, removed: %s", CACHE_FILE)
    except (PermissionError, OSError) as e:
        log.warning("Failed to remove cache file %s: %s", CACHE_FILE, e)


def load_cached_outputs(cache_debug: bool = False) -> Optional[Dict[str, Any]]:
    """Load Terraform outputs from cache file, returning None on any failure."""
    if not CACHE_FILE.exists():
        return None

    if cache_debug:
        mtime = CACHE_FILE.stat().st_mtime
        age = datetime.now() - datetime.fromtimestamp(mtime)
        size_kb = CACHE_FILE.stat().st_size / 1024
        log.info("Cache file age: %s, size: %.2f KB", age, size_kb)

    try:
        with CACHE_FILE.open() as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError, PermissionError) as e:
        log.warning("Cache file corrupted or unreadable (%s), ignoring.", e)
    return None


def save_cached_outputs(outputs: dict) -> None:
    """Persist Terraform outputs to a local cache file."""
    try:
        with CACHE_FILE.open("w") as f:
            json.dump(outputs, f)
        log.debug("Outputs cached to %s", CACHE_FILE)
    except (IOError, PermissionError) as e:
        log.warning("Unable to write cache file: %s", e)


def run_terraform_output(use_cache: bool = True, timeout: int = 30) -> dict:
    """Return parsed Terraform outputs, optionally using a cached copy."""
    if use_cache:
        cached = load_cached_outputs()
        if cached:
            log.info("Using cached Terraform outputs.")
            return cached

    tf_path = Path(TF_DIR)
    if not tf_path.exists():
        raise FileNotFoundError(f"Terraform directory not found: {TF_DIR}")

    try:
        result = subprocess.run(
            ["terraform", "output", "-json"],
            capture_output=True,
            text=True,
            cwd=str(tf_path),
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        log.error("Terraform output timed out after %d seconds", timeout)
        raise

    if result.returncode != 0:
        log.error(
            "Terraform output failed with code %d: %s",
            result.returncode,
            result.stderr.strip(),
        )
        raise RuntimeError(f"Terraform exited with code {result.returncode}")

    try:
        outputs = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        log.error("Failed to parse Terraform outputs: %s", e)
        raise

    save_cached_outputs(outputs)
    return outputs


def host_reachable(ip: str, timeout: int = 3, retries: int = PING_RETRIES) -> bool:
    """Return True if *ip* is reachable via the configured PING_METHOD."""
    if not validate_ip(ip):
        log.debug("Invalid IP address: %s", ip)
        return False

    is_ipv6 = isinstance(ipaddress.ip_address(ip), ipaddress.IPv6Address)

    try:
        if PING_METHOD == "ping":
            cmd = ["ping", "-c", "1", "-W", str(timeout)]
            if is_ipv6:
                cmd.append("-6")
            cmd.append(ip)
        elif PING_METHOD == "nc":
            cmd = ["nc", "-z", "-w", str(timeout), ip, str(SSH_PORT)]
        elif PING_METHOD == "ssh":
            escaped_user = shlex.quote(SSH_USER)
            escaped_ip = shlex.quote(ip)
            cmd = [
                "ssh",
                "-o", f"ConnectTimeout={SSH_TIMEOUT}",
                "-p", str(SSH_PORT),
                f"{escaped_user}@{escaped_ip}",
                "true",
            ]
        else:
            log.error("Unsupported PING_METHOD: %s", PING_METHOD)
            return False

        for attempt in range(retries):
            try:
                result = subprocess.run(
                    cmd, capture_output=True, timeout=timeout + 2
                )
                if result.returncode == 0:
                    return True
                if attempt < retries - 1:
                    time.sleep(PING_RETRY_DELAY)
            except (subprocess.TimeoutExpired, OSError):
                if attempt < retries - 1:
                    time.sleep(PING_RETRY_DELAY)
            except KeyboardInterrupt:
                log.warning("Host check interrupted by user for %s", ip)
                raise
        log.debug("Host %s unreachable after %d attempts", ip, retries)
        return False
    except Exception as e:
        log.debug("Host %s unreachable: %s", ip, e)
        return False


def filter_valid_ips(raw_ips: List[Any]) -> List[str]:
    """Filter and validate IP addresses from raw input, removing duplicates."""
    valid = [ip for ip in raw_ips if ip and validate_ip(ip)]
    return list(dict.fromkeys(valid))


def filter_reachable_ips(ips: List[str], check_reachability: bool) -> List[str]:
    """Return only reachable IPs if *check_reachability* is True."""
    if not check_reachability:
        return ips
    reachable = [ip for ip in ips if host_reachable(ip)]
    skipped = len(ips) - len(reachable)
    if skipped:
        log.info("Ping check removed %d unreachable host(s)", skipped)
    return reachable


def extract_hosts(
    outputs: dict, mapping: Dict[str, str], check_duplicates: bool = True
) -> Dict[str, List[str]]:
    """Return validated, de‑duplicated hosts grouped by section."""
    hosts: Dict[str, List[str]] = {}
    all_ips: set = set()
    for section, tf_name in mapping.items():
        raw = outputs.get(tf_name, {}).get("value", [])
        if not isinstance(raw, list):
            raw = [raw] if raw else []

        valid_ips = filter_valid_ips(raw)
        reachable_ips = filter_reachable_ips(valid_ips, PING_CHECK)

        if check_duplicates:
            duplicates = all_ips & set(reachable_ips)
            if duplicates:
                log.warning(
                    "Duplicate IPs found in %s: %s", section, duplicates
                )
            all_ips.update(reachable_ips)

        hosts[section] = reachable_ips
    return hosts


def build_inventory(
    hosts: Dict[str, List[str]], add_metadata: bool = True
) -> str:
    """Return the content of an Ansible inventory file."""
    lines: List[str] = []
    if add_metadata:
        lines.append("# Generated by generate_inventory.py v1.3")
        lines.append(f"# Timestamp: {datetime.now().isoformat()}")
        lines.append("")

    # Filter out empty sections
    hosts = {k: v for k, v in hosts.items() if v}

    def add_section(title: str, prefix: str, items: List[str]) -> None:
        if not items:
            return
        lines.append(f"[{title}]")
        for i, ip in enumerate(items, start=1):
            lines.append(
                f"{prefix}-{i} ansible_host={ip} ansible_user={SSH_USER}"
            )
        lines.append("")

    for section, prefix in HOST_PREFIXES.items():
        add_section(section, prefix, hosts.get(section, []))

    lines.append("[all:vars]")
    lines.append(f"ansible_ssh_private_key_file = {os.path.abspath(SSH_KEY)}")
    for key, value in ANSIBLE_EXTRA_VARS.items():
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", key):
            log.error("Invalid Ansible variable name: %s", key)
            sys.exit(1)
        escaped_value = str(value).replace('"', '\\"').replace('\n', '\\n')
        lines.append(f"{key} = {escaped_value}")
    return "\n".join(lines)


def write_inventory(content: str, force: bool = False) -> None:
    """Write *content* to ANSIBLE_INVENTORY, creating parent directories."""
    dest = Path(ANSIBLE_INVENTORY)
    if dest.exists() and not force:
        log.error(
            "Inventory file %s already exists. Use --force-overwrite to replace.",
            dest,
        )
        raise FileExistsError(f"{dest} already exists. Use --force-overwrite.")
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
    except PermissionError as e:
        log.error("Cannot create directory %s: permission denied", dest.parent)
        raise
    try:
        dest.write_text(content)
        log.info("Inventory written to %s", dest)
    except (PermissionError, IOError) as e:
        log.error("Failed to write inventory: %s", e)
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Entry point."""
    try:
        args = parse_args()
        logging.getLogger().setLevel(getattr(logging, args.log_level))

        log.info("Starting inventory generation...")

        check_ssh_key()
        log.info("SSH key validation completed successfully")
        check_terraform_version()
        log.info("Terraform version check completed successfully")
        cleanup_old_cache()
        log.info("Cache cleanup completed successfully")

        outputs = run_terraform_output(
            use_cache=not args.no_cache, timeout=args.terraform_timeout
        )
        hosts = extract_hosts(
            outputs,
            TF_OUTPUT_MAPPING,
            check_duplicates=not args.no_duplicate_check,
        )

        if not any(hosts.values()):
            log.error("No valid IP addresses found – aborting.")
            sys.exit(1)

        # HA sanity check: if masters are present, ensure at least 3
        if hosts.get("masters") and len(hosts["masters"]) < 3:
            log.warning(
                "Less than 3 master nodes (%d). High availability may be compromised.",
                len(hosts["masters"]),
            )

        inventory = build_inventory(hosts, add_metadata=not args.no_metadata)

        log.info(
            "Inventory generated with: %d masters, %d workers, %d gpu workers",
            len(hosts.get("masters", [])),
            len(hosts.get("workers", [])),
            len(hosts.get("gpu_workers", [])),
        )

        if args.dry_run:
            print(inventory)
        else:
            write_inventory(inventory, force=args.force_overwrite)

        log.info("Inventory generation finished.")

    except KeyboardInterrupt:
        log.error("Inventory generation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        log.exception("Fatal error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
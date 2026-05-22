# =============================================================================
# Part 17 — Makefile — Enterprise-Grade Ecosystem Orchestrator
# =============================================================================
# Version 2.3 — Production-Hardened, Final
# =============================================================================

SHELL := /bin/bash
.SILENT:
.ONESHELL:
.DELETE_ON_ERROR:

# ---------------------------------------------------------------------------
# Environment & Configuration
# ---------------------------------------------------------------------------
GREEN  := \033[0;32m
YELLOW := \033[1;33m
RED    := \033[0;31m
BLUE   := \033[0;34m
NC     := \033[0m

ENVIRONMENT ?= production
TF_DIR      := infra/terraform
ANSIBLE_DIR := infra/ansible
SCRIPTS_DIR := infra/scripts
HELM_DB     := infra/helm-charts/databases
HELM_APP    := infra/helm-charts/crawler-core
KUBECONFIG  ?= $(HOME)/.kube/config
HELM_TIMEOUT ?= 10m
PARALLEL_LEVEL := $(shell nproc 2>/dev/null || echo 4)
VERBOSE ?= 0
DRY_RUN ?= 0
MAKEFILE_VERSION := 2.3

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
define log_info
    echo -e "$(GREEN)[INFO]$(NC)  $(1)"
endef

define log_warn
    echo -e "$(YELLOW)[WARN]$(NC)  $(1)"
endef

define log_error
    echo -e "$(RED)[ERROR]$(NC) $(1)"
endef

define check_bin
    @command -v $(1) >/dev/null 2>&1 || { $(call log_error,"$(1) is required but not installed."); exit 1; }
endef

# Mask sensitive paths in logs (show only last component)
mask_path = $(shell echo "$(1)" | sed 's/.*\///g; s/^\(.\{2\}\).*/\1***/')

# Dry-run prefix
ifeq ($(DRY_RUN),1)
    DRY_PREFIX = [DRY-RUN]
else
    DRY_PREFIX =
endif

# ---------------------------------------------------------------------------
# Dependency checks (cached)
# ---------------------------------------------------------------------------
.DUMMY: check-terraform check-ansible check-kubectl check-helm check-python3 check-jq check-curl check-sha256sum

check-%:
	$(call check_bin,$*)

REQUIRED_BINS := terraform ansible kubectl helm python3 jq curl sha256sum
CHECK_REQS   := $(addprefix check-,$(REQUIRED_BINS))

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------
.PHONY: help init-project plan apply deploy deploy-parallel status backup restore rollback validate clean

help: ## Show this help
	@echo "$(BLUE)Mega Crawler Ecosystem — Enterprise Orchestrator v$(MAKEFILE_VERSION)$(NC)"
	@echo ""
	@echo "Available targets:"
	@echo "  init-project      Initialize all dependencies and configurations"
	@echo "  plan              Show Terraform execution plan"
	@echo "  apply             Apply infrastructure (VMs, network, DNS)"
	@echo "  deploy            Full deployment (uses parallel by default)"
	@echo "  deploy-sequential Full sequential deployment"
	@echo "  deploy-parallel   Full parallel deployment (fastest)"
	@echo "  status            System health overview"
	@echo "  backup            Create full ecosystem backup"
	@echo "  restore           Restore from backup archive (RESTORE_ARCHIVE=...)"
	@echo "  validate          Validate all configurations and manifests"
	@echo "  rollback          Rollback last deployment to previous revision"
	@echo "  clean             Remove all temporary files and caches"
	@echo "  help              Show this help message"
	@echo ""
	@echo "Environment variables:"
	@echo "  ENVIRONMENT       Deployment environment (production, staging, dev) [default: production]"
	@echo "  RESTORE_ARCHIVE   Path to backup archive for restore"
	@echo "  VERBOSE           Set to 1 for detailed output"
	@echo "  HELM_TIMEOUT      Helm install/upgrade timeout [default: 10m]"
	@echo "  DRY_RUN           Set to 1 to simulate (dry-run) all operations"
	@echo ""
	@echo "Debugging tips:"
	@echo "  Run 'make VERBOSE=1' to see verbose output"
	@echo "  Run 'make DRY_RUN=1' to simulate without changes"
	@echo ""
	@echo "Bash completion:"
	@echo "  Add 'complete -W \"\$$(make _complete)\" make' to your .bashrc"
	@echo ""

# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------
init-project: $(CHECK_REQS)  ## Alias for 'init'
	@$(call log_info,"Initializing ecosystem...")
	@cd $(TF_DIR) && terraform init -reconfigure -upgrade && { \
		$(SCRIPTS_DIR)/generate_inventory.py; \
		$(call log_info,"Initialization completed."); \
	} || { \
		$(call log_error,"Terraform init failed. Inventory not generated."); \
		exit 1; \
	}

init: init-project  ## Backward compatibility

# ---------------------------------------------------------------------------
# Terraform lifecycle
# ---------------------------------------------------------------------------
plan: check-terraform
	@$(call log_info,"$(DRY_PREFIX) Generating Terraform plan...")
	cd $(TF_DIR) && terraform plan -out=plan.tfplan $(if $(filter 1,$(DRY_RUN)),-detailed-exitcode)
	@$(call log_info,"$(DRY_PREFIX) Plan saved to $(TF_DIR)/plan.tfplan")

apply: check-terraform
	@$(call log_info,"$(DRY_PREFIX) Applying Terraform changes...")
	@if [ "$(DRY_RUN)" = "1" ]; then \
		cd $(TF_DIR) && terraform apply -auto-approve -dry-run plan.tfplan; \
		$(call log_info,"[DRY-RUN] Inventory generation skipped."); \
	else \
		cd $(TF_DIR) && terraform apply -auto-approve plan.tfplan || { \
			$(call log_error,"Terraform apply failed. Check state and logs."); \
			exit 1; \
		}; \
		$(SCRIPTS_DIR)/generate_inventory.py; \
		$(call log_info,"Infrastructure applied."); \
	fi

# ---------------------------------------------------------------------------
# Deployment (parallel by default)
# ---------------------------------------------------------------------------
deploy: deploy-parallel  ## Default deployment uses parallel mode

deploy-sequential: check-ansible check-helm check-kubectl
	@$(call log_info,"Starting sequential deployment...")
	@$(MAKE) deploy-k3s
	@$(MAKE) deploy-databases
	@$(MAKE) deploy-app
	@$(MAKE) deploy-istio
	@$(MAKE) deploy-vault
	@$(call log_info,"Full sequential deployment completed.")

# ---------------------------------------------------------------------------
# Parallel deployment with error handling and signal trapping
# ---------------------------------------------------------------------------
deploy-parallel: check-ansible check-helm check-kubectl
	@$(call log_info,"$(DRY_PREFIX) Starting parallel deployment (level $(PARALLEL_LEVEL))...")
	@bash -c '
		trap "echo \"$(RED)[ERROR]$(NC) Parallel deployment interrupted.\"; kill 0; cleanup_temp; exit 1" INT TERM
		pids=()
		step=0
		total=5
		progress() { step=$$((step+1)); echo -e "$(BLUE)[$$step/$$total]$(NC) $$1"; }

		progress "Deploying K3s cluster"
		$(MAKE) deploy-k3s &
		pids+=($$!)
		sleep 30

		progress "Deploying databases"
		$(MAKE) deploy-databases &
		pids+=($$!)

		progress "Deploying application"
		$(MAKE) deploy-app &
		pids+=($$!)

		# Wait for first batch
		failed=0
		for pid in $${pids[@]}; do
			if ! wait $$pid; then
				echo "$(YELLOW)[WARN]$(NC) Process PID $$pid failed"
				failed=1
			fi
		done
		if [ $$failed -eq 1 ]; then
			echo "$(RED)[ERROR]$(NC) One or more critical deployments failed"
			cleanup_temp
			exit 1
		fi

		progress "Applying Istio configs"
		$(MAKE) deploy-istio &
		ISTIO_PID=$$!
		progress "Initializing Vault"
		$(MAKE) deploy-vault &
		VAULT_PID=$$!
		wait $$ISTIO_PID || echo "$(YELLOW)[WARN]$(NC) Istio configuration may have issues"
		wait $$VAULT_PID || echo "$(YELLOW)[WARN]$(NC) Vault initialization skipped"

		echo -e "$(GREEN)[INFO]$(NC) $(DRY_PREFIX) Parallel deployment completed."
		cleanup_temp
	'

# ---------------------------------------------------------------------------
# Individual steps
# ---------------------------------------------------------------------------
deploy-k3s: check-ansible
	@$(call log_info,"$(DRY_PREFIX) Deploying K3s cluster...")
	@if [ "$(DRY_RUN)" = "1" ]; then \
		ansible-playbook -i $(ANSIBLE_DIR)/inventory.ini $(ANSIBLE_DIR)/playbooks/install-k3s.yml --check; \
	else \
		[[ -r "$(ANSIBLE_DIR)/inventory.ini" ]] || { $(call log_error,"Inventory file not found."); exit 1; }; \
		ansible-playbook -i $(ANSIBLE_DIR)/inventory.ini $(ANSIBLE_DIR)/playbooks/install-k3s.yml || { \
			$(call log_error,"K3s deployment failed."); exit 1; \
		}; \
	fi
	@$(call log_info,"$(DRY_PREFIX) K3s cluster deployed.")

deploy-databases: check-helm
	@$(call log_info,"$(DRY_PREFIX) Deploying databases...")
	@helm upgrade --install databases $(HELM_DB) --values $(HELM_DB)/values.yaml --wait --timeout $(HELM_TIMEOUT) $(if $(filter 1,$(DRY_RUN)),--dry-run --debug) || { \
		$(call log_error,"Database deployment failed."); exit 1; \
	}
	@$(call log_info,"$(DRY_PREFIX) Databases deployed.")

deploy-app: check-helm
	@$(call log_info,"$(DRY_PREFIX) Deploying crawler core application...")
	@helm upgrade --install crawler-core $(HELM_APP) --values $(HELM_APP)/values.yaml --wait --timeout $(HELM_TIMEOUT) $(if $(filter 1,$(DRY_RUN)),--dry-run --debug) || { \
		$(call log_error,"Application deployment failed."); exit 1; \
	}
	@$(call log_info,"$(DRY_PREFIX) Application deployed.")

deploy-istio: check-kubectl
	@$(call log_info,"$(DRY_PREFIX) Applying Istio service mesh configuration...")
	@kubectl apply -f infra/istio/ $(if $(filter 1,$(DRY_RUN)),--dry-run=client) || { \
		$(call log_error,"Istio configuration failed."); exit 1; \
	}
	@$(call log_info,"$(DRY_PREFIX) Istio configuration applied.")

deploy-vault:
	@$(call log_info,"$(DRY_PREFIX) Initializing Vault...")
	@if [ "$(DRY_RUN)" != "1" ]; then \
		[[ -x "$(SCRIPTS_DIR)/vault-init.sh" ]] || { $(call log_error,"vault-init.sh not found or not executable"); exit 1; }; \
		bash $(SCRIPTS_DIR)/vault-init.sh || $(call log_warn,"Vault initialization skipped."); \
	fi
	@$(call log_info,"$(DRY_PREFIX) Vault ready.")

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
validate: check-terraform check-ansible check-helm check-kubectl
	@$(call log_info,"Validating all configurations...")
	cd $(TF_DIR) && terraform validate
	ansible-playbook --syntax-check $(ANSIBLE_DIR)/playbooks/*.yml
	helm lint $(HELM_DB)
	helm lint $(HELM_APP)
	kubectl apply --dry-run=client -f infra/istio/
	@$(call log_info,"All configurations valid.")

# ---------------------------------------------------------------------------
# Status overview
# ---------------------------------------------------------------------------
status: check-kubectl
	@echo -e "$(BLUE)=== Cluster Nodes ===$(NC)"
	kubectl get nodes -o wide
	@echo -e "$(BLUE)=== Pods (crawler) ===$(NC)"
	kubectl get pods -n crawler -o wide --show-labels
	@echo -e "$(BLUE)=== Pods (databases) ===$(NC)"
	kubectl get pods -n databases -o wide --show-labels
	@echo -e "$(BLUE)=== Services/Ingresses ===$(NC)"
	kubectl get svc,ingress --all-namespaces | grep -E 'crawler|grafana|prometheus|meili'
	@echo -e "$(BLUE)=== API Health ===$(NC)"
	curl -s -o /dev/null -w 'API Status: %{http_code}\n' https://api.crawler-ecosystem.com/api/health || true

# ---------------------------------------------------------------------------
# Backup & Restore
# ---------------------------------------------------------------------------
backup:
	@$(call log_info,"Initiating full backup...")
	@[[ -x "$(SCRIPTS_DIR)/backup.sh" ]] || { $(call log_error,"backup.sh not found or not executable"); exit 1; }
	bash $(SCRIPTS_DIR)/backup.sh --full || { \
		$(call log_error,"Backup failed"); exit 1; \
	}
	@$(call log_info,"Backup completed.")

restore:
	@test -n "$(RESTORE_ARCHIVE)" || { $(call log_error,"Specify RESTORE_ARCHIVE=/path/to/backup.tar.gz"); exit 1; }
	@test -r "$(RESTORE_ARCHIVE)" || { $(call log_error,"Archive not readable: $(call mask_path,$(RESTORE_ARCHIVE))"); exit 1; }
	@$(call log_info,"Restoring from $(call mask_path,$(RESTORE_ARCHIVE))...")
	@[[ -x "$(SCRIPTS_DIR)/restore.sh" ]] || { $(call log_error,"restore.sh not found or not executable"); exit 1; }
	bash $(SCRIPTS_DIR)/restore.sh --archive $(RESTORE_ARCHIVE) --all --yes || { \
		$(call log_error,"Restore failed"); exit 1; \
	}
	@$(call log_info,"Restore completed.")

# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------
rollback:
	@$(call log_info,"Rolling back to previous deployment...")
	@echo "Current revisions:"
	@helm history databases --max 5 || true
	@helm history crawler-core --max 5 || true
	@if [ "$(DRY_RUN)" = "1" ]; then \
		helm rollback databases 1 --wait --timeout $(HELM_TIMEOUT) --dry-run; \
		helm rollback crawler-core 1 --wait --timeout $(HELM_TIMEOUT) --dry-run; \
	else \
		helm rollback databases 1 --wait --timeout $(HELM_TIMEOUT) || $(call log_warn,"Database rollback failed"); \
		helm rollback crawler-core 1 --wait --timeout $(HELM_TIMEOUT) || $(call log_warn,"Application rollback failed"); \
	fi
	@$(call log_info,"Rollback executed. Verify with 'make status'.")

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
clean: clean-terraform clean-ansible
	@$(call log_info,"Cleanup complete.")

clean-terraform:
	@$(call log_info,"Cleaning Terraform temporary files...")
	@find . -type d -name ".terraform" -exec rm -rf {} + -print 2>/dev/null || true
	@find . -name ".terraform_output_cache.json" -delete -print

clean-ansible:
	@$(call log_info,"Cleaning Ansible temporary files...")
	@find . -name "*.retry" -delete -print
	@rm -f $(ANSIBLE_DIR)/inventory.ini

# ---------------------------------------------------------------------------
# Bash completion helper
# ---------------------------------------------------------------------------
_complete:
	@echo "init-project plan apply deploy deploy-sequential deploy-parallel status backup restore rollback validate clean help"
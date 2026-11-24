#!/usr/bin/env bash
set -euo pipefail
source /opt/agentteams/scripts/lib/agentteams-env.sh
# Claude emits SDK spans directly. Its short-lived stdio and preparation processes
# inherit the shared libraries without bootstrapping Python model instrumentation.
export PYTHONPATH="${PYTHONPATH#/opt/agentteams/scripts/lib/telemetry:}"

worker=${AGENTTEAMS_WORKER_NAME:?}
export TEAMHARNESS_RUNTIME_CONFIG="${AGENTTEAMS_WORKER_HOME:-${HOME}}/runtime/runtime.yaml"
member_dir=$(dirname "$(dirname "${TEAMHARNESS_RUNTIME_CONFIG}")")
export TEAMHARNESS_WORKSPACE="${member_dir}/workspace"
export TEAMHARNESS_SHARED_DIR="${AGENTTEAMS_SHARED_DIR:-${member_dir}/shared}"
export CLAUDE_CONFIG_DIR="${member_dir}/.claude"
mkdir -p "${member_dir}/runtime" "${CLAUDE_CONFIG_DIR}/projects" "${TEAMHARNESS_WORKSPACE}" "${TEAMHARNESS_SHARED_DIR}"

if ! ensure_mc_credentials || ! agentteams_mc_host_configured; then
    if [[ "${AGENTTEAMS_STORAGE_PROVIDER:-minio}" == oss ]]; then
        echo "Cannot resolve member OSS credentials" >&2
        exit 1
    fi
    mc alias set "${AGENTTEAMS_STORAGE_ALIAS}" "${AGENTTEAMS_FS_ENDPOINT:?}" \
        "${AGENTTEAMS_FS_ACCESS_KEY:?}" "${AGENTTEAMS_FS_SECRET_KEY:?}" >/dev/null
fi

exec python3 /opt/agentteams/claude-code/bridge.py

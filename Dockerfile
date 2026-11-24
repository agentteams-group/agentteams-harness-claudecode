ARG AGENTTEAMS_CONTROLLER_IMAGE
FROM ${AGENTTEAMS_CONTROLLER_IMAGE} AS controller
FROM python:3.12-slim-bookworm

ARG APT_MIRROR=
RUN if [ -n "$APT_MIRROR" ]; then sed -i "s|http://deb.debian.org|https://$APT_MIRROR|g" /etc/apt/sources.list.d/debian.sources; fi \
    && apt-get update && apt-get install -y --no-install-recommends ca-certificates git jq \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY --from=controller /usr/local/bin/agt /usr/local/bin/agt
RUN agt task begin --help && agt task submit --help
COPY --from=controller /usr/local/bin/mc /usr/local/bin/mc.bin
COPY --from=controller /opt/agentteams/agent/ /opt/agentteams/agent/
COPY --from=agentteams /shared/lib /opt/agentteams/scripts/lib
RUN chmod 0755 /opt/agentteams/scripts/lib/mc-wrapper.sh \
    && ln -s /opt/agentteams/scripts/lib/mc-wrapper.sh /usr/local/bin/mc \
    && mc --version
COPY --from=agentteams /plugins/teamharness /opt/agentteams/plugins/teamharness
COPY --from=agentteams /deepseek-harness/scripts/matrix_channel.py /deepseek-harness/scripts/matrix_bridge.py /deepseek-harness/scripts/bridge_state.py /deepseek-harness/scripts/runtime_env.py /opt/agentteams/transport/
COPY *.py entrypoint.sh /opt/agentteams/claude-code/
ENV PYTHONPATH=/opt/agentteams/scripts/lib:/opt/agentteams/transport \
    AGENTTEAMS_PLUGIN_DIR=/opt/agentteams/plugins/teamharness \
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
ENTRYPOINT ["python3", "/opt/agentteams/scripts/lib/runtime_supervisor.py", "--runtime", "claudecode", "--", "bash", "/opt/agentteams/claude-code/entrypoint.sh"]

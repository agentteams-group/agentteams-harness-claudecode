from unittest.mock import patch

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import telemetry


def test_task_identity_is_queryable_on_native_turn_and_child_observations(monkeypatch):
    monkeypatch.setenv('AGENTTEAMS_WORKER_NAME', 'team-developer')
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    task = {'identity': {'task_id': 'exec-one', 'team_id': 'team-one', 'generation': 2},
            'coordination': {'main_task_id': 'main-one'}, 'room': '!task:test'}
    with patch('telemetry.trace.get_tracer', provider.get_tracer):
        with telemetry.turn_span('worker', task, 'test-model') as root:
            with trace.use_span(root, end_on_exit=False):
                with provider.get_tracer('fixture').start_as_current_span('tool', attributes=telemetry.identity_attributes()):
                    pass
    spans = exporter.get_finished_spans()
    for span in spans:
        assert span.attributes['langfuse.trace.metadata.agentteams.main_task.id'] == 'main-one'
        assert span.attributes['langfuse.observation.metadata.agentteams.task.id'] == 'exec-one'
        assert span.attributes['agentteams.team.id'] == 'team-one'
        assert span.attributes['gen_ai.agent.id'] == 'team-developer'
    assert len({span.context.trace_id for span in spans}) == 1
    provider.shutdown()

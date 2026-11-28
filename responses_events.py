"""Normalize gateway Responses reasoning into the converter's native event form."""
from litellm.integrations.custom_logger import CustomLogger


class ResponsesReasoningEvents(CustomLogger):
    async def async_post_call_streaming_deployment_hook(self, request_data, response_chunk, call_type):
        kind = (response_chunk.get("type") if isinstance(response_chunk, dict)
                else getattr(response_chunk, "type", None))
        if kind not in {"response.reasoning_text.delta", "response.reasoning_text.done"}:
            return response_chunk
        update = {"type": kind.replace("reasoning_text", "reasoning_summary_text")}
        if isinstance(response_chunk, dict):
            return {**response_chunk, **update}
        return response_chunk.model_copy(update=update)

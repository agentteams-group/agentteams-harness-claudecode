"""Use the shared Matrix cursor and pending inbox for managed Workers."""
from matrix_bridge import managed_rooms
from worker_matrix_state import WorkerMatrix, matrix_event_key
from worker_matrix_delivery import confirm_bound
from worker_matrix_raw import RawWorkerSync, publish_raw_saved


def scan(client, record, transport, rooms, agents, ready, admitted):
    member = record["member"]
    own = member["matrixUserId"]
    matrix = WorkerMatrix("claudecode", device_delivery=True)
    joined = set()
    policy = record["desired"].get("channelPolicy") or {}

    def observe(batch):
        joined.clear()
        joined.update(managed_rooms(client, batch, rooms, own, transport))
        return joined

    def classify(room, raw):
        event = admitted(raw, room, rooms, member, agents, transport, policy)
        if event and not event.get("control"):
            packet = (event.get("_task_protocol_meta") or {}).get("teamharness_coordination") or {}
            if packet.get("kind") == "task_assigned":
                return "task_notification", packet["task_id"]
            return "message", None

    receiver = RawWorkerSync(matrix, client, own, classify, observe)
    receiver.ready = ready
    snapshot, failures = receiver.poll(10000 if ready else 0)
    if failures:
        raise RuntimeError("Matrix history backfill is incomplete")
    pending, assignments = [], {}
    for row in matrix.pending(snapshot):
        room = row["room_id"]
        if room not in joined or snapshot["state"]["rooms"].get(room, {}).get("gaps"):
            continue
        event = admitted({**row["input"], "event_id": row["event_id"]}, room, rooms, member, agents, transport, policy)
        if not event or event.get("control"):
            continue
        if row["intent"] == "task_notification":
            task_id = (row.get("work") or {}).get("task_id")
            packet = (event.get("_task_protocol_meta") or {}).get("teamharness_coordination") or {}
            if task_id and task_id == packet.get("task_id"):
                assignments[task_id] = event
        elif row["intent"] == "message":
            claim = matrix.message_claim(matrix_event_key(room, row["event_id"]))
            if claim["recovery"] == "deliver":
                deliver_message(client, matrix, claim, event, own, claim["saved_result"])
            elif claim["recovery"] == "resume":
                pending.append((event, claim))
    return matrix, pending, assignments


def deliver_message(client, matrix, claim, event, own, answer):
    identity = claim["identity"]

    def bind(checkpoint):
        saved = matrix.message_checkpoint(identity, checkpoint)
        confirm_bound(saved["execution"], identity, "claudecode", checkpoint)

    reply = publish_raw_saved(client, event["room_id"], answer, claim["execution"]["checkpoint"],
                              own, bind, content={"m.relates_to": {"m.in_reply_to": {"event_id": event["event_id"]}}})
    return matrix.message_delivered(identity, reply)

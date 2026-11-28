"""Use the shared Matrix cursor and pending inbox for managed Workers."""
from task_coordination import include_task_rooms, requires_room_history
from matrix_bridge import managed_rooms
from worker_matrix_state import WorkerMatrix, matrix_event_key
from worker_matrix_delivery import confirm_bound
from worker_matrix_raw import RawWorkerSync, publish_raw_saved


def scan(client, record, transport, rooms, agents, ready, admitted, events):
    member = record["member"]
    own = member["matrixUserId"]
    matrix = WorkerMatrix("claudecode", device_delivery=True)
    joined = set()
    policy = record["desired"].get("channelPolicy") or {}

    def observe(batch):
        joined.clear()
        joined.update(managed_rooms(client, batch, rooms, own, transport))
        include_task_rooms(rooms, joined)
        return joined

    def classify(room, raw):
        event = admitted(raw, room, rooms, member, agents, transport, policy)
        if event and not event.get("control"):
            packet = (event.get("_task_protocol_meta") or {}).get("teamharness_coordination") or {}
            if packet.get("kind") == "task_assigned":
                return "task_notification", packet["task_id"]
            return "message", None

    receiver = RawWorkerSync(matrix, client, own, classify, observe, event_types=("m.room.message", "org.agentteams.coordination"))
    receiver.ready = ready
    snapshot, failures = receiver.poll(10000 if ready else 0)
    pending, assignments = [], {}
    for row in matrix.pending(snapshot):
        room = row["room_id"]
        if (room not in joined or ((room in failures or snapshot["state"]["rooms"].get(room, {}).get("gaps"))
                and requires_room_history(row, own))):
            continue
        event = admitted({**row["input"], "event_id": row["event_id"]}, room, rooms, member, agents, transport, policy)
        if not event or event.get("control"):
            continue
        packet = (event.get("_task_protocol_meta") or {}).get("teamharness_coordination") or {}
        if packet.get("kind") == "task_assigned" and row["intent"] == "message":
            # Older inputs retained the ordinary-message classification. The
            # authenticated notification belongs to recover_work, never to a
            # second inference path. Retire only an unused ordinary claim.
            claim = matrix.message_claim(matrix_event_key(room, row["event_id"]))
            if claim["recovery"] == "resume" and not claim["execution"].get("checkpoint"):
                matrix.message_cancel(claim["identity"])
            assignments[packet["task_id"]] = event
            continue
        if row["intent"] == "task_notification":
            task_id = (row.get("work") or {}).get("task_id")
            if task_id and task_id == packet.get("task_id"):
                assignments[task_id] = event
        elif row["intent"] == "message":
            claim = matrix.message_claim(matrix_event_key(room, row["event_id"]))
            if claim["recovery"] == "deliver":
                slot = events.get(event["event_id"]) or {}
                if slot.get("silent_receipt"):
                    if slot.get("answer") != claim["saved_result"]:
                        raise RuntimeError("Saved coordination receipt differs from its native result")
                    matrix.message_complete(claim["identity"])
                else:
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
                              own, bind, content={"org.agentteams.run_id": event["event_id"], "m.relates_to": {"m.in_reply_to": {"event_id": event["event_id"]}}})
    return matrix.message_delivered(identity, reply)

from __future__ import annotations
import hashlib
import json
from typing import List, Tuple
from django.db import transaction
from django.utils import timezone
from audit.models import AuditEvent, Block, BlockMembership


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canon_json(payload: dict) -> bytes:
    # детерминированная сериализация: ключи по алфавиту, без лишних пробелов
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False
    ).encode("utf-8")


def compute_event_hash(
        *,
        actor_type: str,
        actor_id: str|None,
        entity_type: str,
        entity_id: str,
        action: str,
        before: dict|None,
        after: dict|None,
        meta: dict|None,
        created_at_iso: str
) -> str:

    payload = {
        "actor_type": actor_type,
        "actor_id": actor_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "action": action,
        "before": before,
        "after": after,
        "meta": meta,
        "created_at": created_at_iso,
    }
    return _sha256_hex(_canon_json(payload))


def record_event(
        *,
        actor_type: str,
        actor_id: str|None,
        entity_type: str,
        entity_id: str,
        action: str,
        before: dict|None,
        after: dict|None,
        meta: dict|None
) -> AuditEvent:

    timestamp = timezone.now()
    actor_id_str = str(actor_id) if actor_id is not None else None
    entity_id_str = str(entity_id)

    event_hash = compute_event_hash(
        actor_type=actor_type,
        actor_id=actor_id_str,
        entity_type=entity_type,
        entity_id=entity_id_str,
        action=action,
        before=before,
        after=after,
        meta=meta,
        created_at_iso=timestamp.isoformat(),
    )
    return AuditEvent.objects.create(
        created_at=timestamp,
        actor_type=actor_type,
        actor_id=actor_id_str,
        entity_type=entity_type,
        entity_id=entity_id_str,
        action=action,
        before=before,
        after=after,
        meta=meta,
        event_hash=event_hash,
        in_block=False,
    )


def _pair_up(level: List[str]) -> List[str]:
    out: List[str] = []
    i, n = 0, len(level)
    while i < n:
        a = level[i]
        b = level[i+1] if i+1 < n else a
        out.append(_sha256_hex((a+b).encode("utf-8")))
        i += 2
    return out


def merkle_root_for_leaves(leaves: List[str]) -> Tuple[str, List[str]]:
    if not leaves:
        return _sha256_hex(b""), []
    level = leaves[:]
    debug_levels = [";".join(level)]
    while len(level) > 1:
        level = _pair_up(level)
        debug_levels.append(";".join(level))
    return level[0], debug_levels


@transaction.atomic
def seal_block(max_events: int = 512) -> Block | None:
    # забираем часть неподписанных событий
    events = list(
        AuditEvent.objects.select_for_update(skip_locked=True)
        .filter(in_block=False).order_by("id")[:max_events]
    )
    if not events:
        return None

    leaves = [event.event_hash for event in events]
    root, _ = merkle_root_for_leaves(leaves)

    last = Block.objects.select_for_update().order_by("-index").first()
    index = 0 if last is None else last.index + 1
    prev_hash = None if last is None else last.block_hash
    timestamp = timezone.now()

    header = {
        "index": index,
        "created_at": timestamp.isoformat(),
        "prev": prev_hash,
        "merkle": root
    }
    block_hash = _sha256_hex(_canon_json(header))

    block = Block.objects.create(
        index=index,
        prev_block_hash=prev_hash,
        merkle_root=root,
        block_hash=block_hash,
        created_at=timestamp
    )

    for position, event in enumerate(events):
        BlockMembership.objects.create(
            block=block,
            event=event,
            leaf_index=position,
            leaf_hash=event.event_hash
        )
    AuditEvent.objects.filter(
        id__in=[event.id for event in events]
    ).update(in_block=True)
    return block


def verify_chain() -> dict:
    blocks = list(
        Block.objects.order_by("index")
        .prefetch_related("items", "items__event"))
    prev = None
    for block in blocks:
        expected = _sha256_hex(
            _canon_json(
                {
                    "index": block.index,
                    "created_at": block.created_at.isoformat(),
                    "prev": block.prev_block_hash, "merkle": block.merkle_root,
                }
            )
        )
        if expected != block.block_hash:
            return {
                "ok": False,
                "where": f"block_hash mismatch at index {block.index}"
            }
        if (prev is None and block.prev_block_hash is not None) \
                or (prev and block.prev_block_hash != prev.block_hash):
            return {
                "ok": False,
                "where": f"prev_link mismatch at index {block.index}"
            }

        leaves = [item.leaf_hash for item in block.items.order_by("leaf_index")]
        root, _ = merkle_root_for_leaves(leaves)
        if root != block.merkle_root:
            return {
                "ok": False,
                "where": f"merkle_root mismatch at index {block.index}"
            }

        for item in block.items.all():
            event = item.event
            recomputed = compute_event_hash(
                actor_type=event.actor_type,
                actor_id=event.actor_id,
                entity_type=event.entity_type,
                entity_id=event.entity_id,
                action=event.action,
                before=event.before,
                after=event.after,
                meta=event.meta,
                created_at_iso=event.created_at.isoformat(),
            )
            if recomputed != event.event_hash \
                    or item.leaf_hash != event.event_hash:
                return {
                    "ok": False,
                    "where": f"event hash mismatch event#{event.id} in block {block.index}"
                }
        prev = block
    return {"ok": True, "blocks": len(blocks)}

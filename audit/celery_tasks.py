from __future__ import annotations

from pathlib import Path

from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.utils import timezone

from audit.services import seal_block, verify_chain

logger = get_task_logger("audit.celery_tasks")

VERIFY_LOG_PATH = Path(settings.BASE_DIR) / "audit_verify_results.txt"


def _append_verify_result_line(line: str) -> None:
    VERIFY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with VERIFY_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True,
             retry_backoff_max=60, retry_jitter=True, max_retries=5)
def seal_block_tick(self, max_events: int = 512) -> int:
    blk = seal_block(max_events=max_events)
    if blk:
        logger.info(
            f"🧱 sealed block index={blk.index} "
            f"hash={blk.block_hash[:10]}... "
            f"root={blk.merkle_root[:10]}..."
        )
        return 1

    logger.info("🧱 no pending audit events to seal")
    return 0


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True,
             retry_backoff_max=60, retry_jitter=True, max_retries=5)
def verify_chain_tick(self) -> dict:
    checked_at = timezone.now().isoformat()
    res = verify_chain()

    if res.get("ok"):
        line = f"[{checked_at}] OK | blocks={res['blocks']}"
        logger.info(f"🔎 audit chain OK: verified {res['blocks']} blocks")
    else:
        where = res.get("where", "unknown")
        line = f"[{checked_at}] FAIL | where={where}"
        logger.error(f"🚨 audit chain FAIL: {res}")

    _append_verify_result_line(line)
    return res
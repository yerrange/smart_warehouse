from __future__ import annotations
from celery import shared_task
from celery.utils.log import get_task_logger
from audit.services import seal_block

logger = get_task_logger("audit.celery_tasks")

@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=True,
             retry_backoff_max=60, retry_jitter=True, max_retries=5)
def seal_block_tick(self, max_events: int = 512) -> int:
    blk = seal_block(max_events=max_events)
    if blk:
        logger.info(f"🧱 sealed block index={blk.index} hash={blk.block_hash[:10]}... root={blk.merkle_root[:10]}...")
        return 1
    return 0

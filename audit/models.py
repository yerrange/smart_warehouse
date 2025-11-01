from __future__ import annotations
from django.db import models
from django.utils import timezone


class AuditEvent(models.Model):
    """
    Бизнес-событие: кто/что/когда изменил.
    Строки не редактируются и не удаляются.
    """
    created_at = models.DateTimeField(default=timezone.now, db_index=True)

    actor_type = models.CharField(max_length=32)  # "user" | "system" | "celery"
    actor_id = models.CharField(max_length=64, null=True, blank=True)

    entity_type = models.CharField(max_length=32)  # "Task" | "Shift" | "Cargo" | ...
    entity_id = models.CharField(max_length=64)

    action = models.CharField(max_length=32)  # "CREATE" | "ASSIGN" | "UPDATE" | "CLOSE" | ...

    before = models.JSONField(null=True, blank=True)
    after = models.JSONField(null=True, blank=True)
    meta = models.JSONField(null=True, blank=True) # request_id, celery_task_id и т. п.

    event_hash = models.CharField(max_length=64, db_index=True)  # SHA-256 отпечаток события
    in_block = models.BooleanField(default=False, db_index=True)

    class Meta:
        ordering = ["id"]

    def __str__(self) -> str:
        return f"""{self.created_at:%Y-%m-%d %H:%M:%S} {self.entity_type}#{self.entity_id} {self.action}"""


class Block(models.Model):
    """
    Блок цепочки: ссылка на прошлый блок,
    Merkle-корень и собственный хэш.
    """
    index = models.BigIntegerField(unique=True, db_index=True)
    created_at = models.DateTimeField(db_index=True)

    prev_block_hash = models.CharField(max_length=64, null=True, blank=True)
    merkle_root = models.CharField(max_length=64)
    block_hash = models.CharField(max_length=64, unique=True, db_index=True)

    class Meta:
        ordering = ["index"]

    def __str__(self) -> str:
        return f"Block {self.index} {self.block_hash[:10]}..."


class BlockMembership(models.Model):
    """
    Связь события с блоком
    + позиция листа в Merkle-дереве.
    """
    block = models.ForeignKey(
        Block,
        on_delete=models.CASCADE,
        related_name="items"
    )
    event = models.OneToOneField(
        AuditEvent,
        on_delete=models.CASCADE,
        related_name="membership"
    )
    leaf_index = models.IntegerField()
    leaf_hash = models.CharField(max_length=64)

    class Meta:
        unique_together = [("block", "leaf_index")]
        ordering = ["leaf_index"]

    def __str__(self) -> str:
        return f"Block#{self.block.index} leaf#{self.leaf_index} event#{self.event_id}"

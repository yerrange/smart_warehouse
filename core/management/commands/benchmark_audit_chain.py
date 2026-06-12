import gc
import statistics
import time
import tracemalloc

from django.core.management.base import BaseCommand
from django.db import connection

from audit.models import AuditEvent, Block, BlockMembership
from audit.services import verify_chain


class Command(BaseCommand):
    help = "Measures current audit chain verification time and memory usage."

    def add_arguments(self, parser):
        parser.add_argument(
            "--runs",
            type=int,
            default=5,
            help="Number of verification runs. Default: 5.",
        )

    def handle(self, *args, **options):
        runs = options["runs"]

        events_count = AuditEvent.objects.count()
        blocks_count = Block.objects.count()
        memberships_count = BlockMembership.objects.count()

        self.stdout.write("=== Audit chain benchmark ===")
        self.stdout.write(f"Audit events: {events_count}")
        self.stdout.write(f"Blocks: {blocks_count}")
        self.stdout.write(f"Block memberships: {memberships_count}")
        self.stdout.write(f"Runs: {runs}")
        self.stdout.write("")

        times = []
        peak_memories = []

        # Прогрев, чтобы первый запуск не искажал картину
        verify_chain()

        for i in range(1, runs + 1):
            gc.collect()
            connection.queries_log.clear()

            tracemalloc.start()
            start = time.perf_counter()

            result = verify_chain()

            elapsed = time.perf_counter() - start
            current_memory, peak_memory = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            times.append(elapsed)
            peak_memories.append(peak_memory)

            self.stdout.write(
                f"Run {i}: "
                f"time={elapsed:.4f} sec, "
                f"peak_memory={peak_memory / 1024 / 1024:.2f} MB, "
                f"result={result}"
            )

        self.stdout.write("")
        self.stdout.write("=== Summary ===")
        self.stdout.write(f"Min time: {min(times):.4f} sec")
        self.stdout.write(f"Max time: {max(times):.4f} sec")
        self.stdout.write(f"Avg time: {statistics.mean(times):.4f} sec")
        self.stdout.write(f"Median time: {statistics.median(times):.4f} sec")
        self.stdout.write(
            f"Avg peak memory: {statistics.mean(peak_memories) / 1024 / 1024:.2f} MB"
        )

        if events_count > 0:
            avg_time = statistics.mean(times)
            self.stdout.write("")
            self.stdout.write("=== Relative indicators ===")
            self.stdout.write(
                f"Time per 1000 events: {avg_time / events_count * 1000:.6f} sec"
            )

        if blocks_count > 0:
            avg_time = statistics.mean(times)
            self.stdout.write(
                f"Time per block: {avg_time / blocks_count:.6f} sec"
            )
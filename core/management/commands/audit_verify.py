from django.core.management.base import BaseCommand
from audit.services import verify_chain

class Command(BaseCommand):
    help = "Verify audit blockchain integrity"

    def handle(self, *args, **options):
        res = verify_chain()
        if res.get("ok"):
            self.stdout.write(self.style.SUCCESS(f"OK: {res['blocks']} blocks verified"))
        else:
            self.stdout.write(self.style.ERROR(f"FAIL: {res}"))
            raise SystemExit(1)
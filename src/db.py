from __future__ import annotations

from supabase import create_client, Client
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings
from src.models import Property


def get_client() -> Client:
    if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def upsert_properties(properties: list[Property]) -> int:
    """Upsert properties to Supabase. Returns count of upserted rows."""
    if not properties:
        return 0

    client = get_client()

    rows = [p.model_dump(mode="json") for p in properties]

    # Remove fields managed by DB defaults
    for row in rows:
        for key in ("views_count", "saves_count", "created_at", "updated_at"):
            row.pop(key, None)

    BATCH_SIZE = 50
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        result = (
            client.table("properties")
            .upsert(batch, on_conflict="source,source_id")
            .execute()
        )
        total += len(result.data) if result.data else 0

    return total

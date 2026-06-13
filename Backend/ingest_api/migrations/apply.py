#!/usr/bin/env python3
"""
Backend/ingest_api/migrations/apply.py

Applies Sentinel database migrations to ClickHouse and PostgreSQL.

Usage (from repo root):
    python Backend/ingest_api/migrations/apply.py

Reads credentials from .env in the repository root.
All SQL uses IF NOT EXISTS — safe to re-run.
"""

import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# -----------------------------------------------------------------------
# Load .env from repo root
# -----------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[3]
_MIGRATIONS_DIR = Path(__file__).parent


def _load_dotenv() -> None:
    env_file = _REPO_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())


_load_dotenv()


# -----------------------------------------------------------------------
# PostgreSQL URL parser
# Handles special characters (@ [ ]) in the password by splitting on
# the LAST @ sign — that's the one separating user info from the host.
# -----------------------------------------------------------------------

def _parse_pg_url(url: str) -> dict:
    """
    Parse a postgresql:// URL robustly.
    - Splits on the LAST @ so @ signs inside the password are handled.
    - Strips surrounding [] if present (some Supabase dashboards show [password]).
    - Supabase requires ssl='require'.
    """
    url = url.removeprefix("postgresql://").removeprefix("postgres://")
    last_at = url.rfind("@")
    if last_at == -1:
        raise ValueError("Invalid DATABASE_URL — no @ found")
    user_info = url[:last_at]
    host_part = url[last_at + 1:]
    colon = user_info.index(":")
    user = user_info[:colon]
    password = user_info[colon + 1:]
    # Strip surrounding brackets that some dashboards add around passwords
    if password.startswith("[") and password.endswith("]"):
        password = password[1:-1]
    host_db, _, database = host_part.partition("/")
    if ":" in host_db:
        host, port_str = host_db.rsplit(":", 1)
        port = int(port_str)
    else:
        host, port = host_db, 5432
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database or "postgres",
        "ssl": "require",   # Supabase / Railway require SSL
    }


# -----------------------------------------------------------------------
# ClickHouse migration
# -----------------------------------------------------------------------

def apply_clickhouse() -> None:
    import clickhouse_connect

    host = os.environ.get("CLICKHOUSE_HOST")
    if not host:
        print("[clickhouse] SKIP — CLICKHOUSE_HOST not set")
        return

    port = int(os.environ.get("CLICKHOUSE_PORT", 8443))
    user = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    database = os.environ.get("CLICKHOUSE_DB", "default")

    print(f"[clickhouse] Connecting to {host}:{port} …")
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database,
        secure=True,
    )

    sql = (_MIGRATIONS_DIR / "001_clickhouse_traces.sql").read_text()
    client.command(sql)
    print("[clickhouse] 001_clickhouse_traces.sql — applied")

    # Verify: insert a sample trace and read it back
    sample_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    client.insert(
        "traces",
        [[
            sample_id,
            "cust_migration_test",
            "sess_migration",
            None,
            "migration_test",
            "dev",
            "gpt-4",
            "Migration test prompt",
            "Migration test response",
            120,
            15,
            "[]",
            "[]",
            "[]",
            now,
        ]],
        column_names=[
            "trace_id", "customer_id", "session_id", "user_id",
            "flow_name", "environment", "model_name",
            "user_prompt", "llm_response",
            "latency_ms", "token_total",
            "retrieved_context", "tool_calls", "message_history",
            "timestamp",
        ],
    )

    result = client.query(
        "SELECT trace_id, customer_id FROM traces WHERE trace_id = {id:String}",
        parameters={"id": sample_id},
    )
    assert len(result.result_rows) == 1, "Sample trace not found after insert"
    print(f"[clickhouse] Sample trace inserted and queried: {sample_id[:8]}…  OK")

    # Show table info
    cols = client.query("DESCRIBE TABLE traces")
    col_names = [row[0] for row in cols.result_rows]
    print(f"[clickhouse] traces columns ({len(col_names)}): {', '.join(col_names)}")

    client.close()


# -----------------------------------------------------------------------
# PostgreSQL migration
# -----------------------------------------------------------------------

async def apply_postgres() -> None:
    import asyncpg

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("[postgres] SKIP — DATABASE_URL not set")
        return

    params = _parse_pg_url(db_url)
    print(f"[postgres] Connecting to {params['host']}:{params['port']} …")

    conn = await asyncpg.connect(**params)
    try:
        sql = (_MIGRATIONS_DIR / "002_postgres_core.sql").read_text()
        await conn.execute(sql)
        print("[postgres] 002_postgres_core.sql — applied")

        # Verify: list created tables
        rows = await conn.fetch(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname = 'public' "
            "ORDER BY tablename"
        )
        tables = [row["tablename"] for row in rows]
        sentinel_tables = [t for t in tables if t in
                           {"api_keys", "customers", "scores", "golden_dataset"}]
        print(f"[postgres] Sentinel tables present: {sentinel_tables}")

        # Verify index creation
        idx_rows = await conn.fetch(
            "SELECT indexname FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename IN "
            "('scores','golden_dataset') ORDER BY indexname"
        )
        print(f"[postgres] Indexes: {[r['indexname'] for r in idx_rows]}")
    finally:
        await conn.close()


# -----------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------

async def main() -> None:
    print("=" * 60)
    print("  Sentinel — Database Migration Runner")
    print("=" * 60)
    print()

    try:
        apply_clickhouse()
    except Exception as exc:
        print(f"[clickhouse] ERROR: {exc}")
        sys.exit(1)

    print()

    try:
        await apply_postgres()
    except Exception as exc:
        print(f"[postgres] ERROR: {exc}")
        sys.exit(1)

    print()
    print("=" * 60)
    print("  All migrations applied successfully.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

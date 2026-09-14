"""Integration smoke test proving store address/city survive an existing-row upsert."""

import os
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from remotes.short_term.postgres_db import PostgresUploader


url = os.environ["POSTGRESQL_URL"]
connection = psycopg2.connect(url)
connection.autocommit = True
cursor = connection.cursor()
cursor.execute(
    """
    CREATE TEMP TABLE store_upsert_guard_test (
      chain_id TEXT,
      store_id TEXT,
      address TEXT,
      city TEXT,
      created_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ,
      UNIQUE (chain_id, store_id)
    )
    """
)
cursor.execute(
    "INSERT INTO store_upsert_guard_test VALUES (%s, %s, %s, %s, NOW(), NOW())",
    ("chain", "store", "adresse conservee", "ville conservee"),
)

uploader = PostgresUploader.__new__(PostgresUploader)
uploader.conn = connection
uploader.db_url = url
uploader._upsert_batch(
    "store_upsert_guard_test",
    [{
        "chain_id": "chain",
        "store_id": "store",
        "address": "nouvelle adresse source",
        "city": "nouvelle ville source",
        "created_at": None,
        "updated_at": None,
    }],
    on_conflict="chain_id,store_id",
    preserve_existing={"city", "address"},
)
cursor.execute("SELECT address, city FROM store_upsert_guard_test")
result = cursor.fetchone()
assert result == ("adresse conservee", "ville conservee"), result
uploader._upsert_batch(
    "store_upsert_guard_test",
    [{
        "chain_id": "new-chain", "store_id": "new-store",
        "address": "adresse nouvelle", "city": "ville nouvelle",
        "created_at": None, "updated_at": None,
    }],
    on_conflict="chain_id,store_id",
    preserve_existing={"city", "address"},
)
cursor.execute("SELECT address, city FROM store_upsert_guard_test WHERE chain_id = 'new-chain'")
inserted = cursor.fetchone()
assert inserted == ("adresse nouvelle", "ville nouvelle"), inserted
print("STORE_UPSERT_GUARD_OK", {"existing": result, "inserted": inserted})
cursor.close()
connection.close()

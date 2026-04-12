"""Compatibility module for legacy Supabase naming.

The project now uses direct PostgreSQL ingestion logic from postgres_db.
Importing from this module remains supported to avoid breaking old call sites.
"""

from .postgres_db import PostgresUploader

SupabaseUploader = PostgresUploader

__all__ = ["PostgresUploader", "SupabaseUploader"]

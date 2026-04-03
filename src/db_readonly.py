"""PostgreSQL connections restricted to read-only SQL.

Defense in depth:
1. Client-side: only a single SELECT or WITH statement is allowed in helpers.
2. Server-side: session starts with default_transaction_read_only=on.
3. libpq: set_session(readonly=True).

Put secrets in .env (never commit). Rotate any credentials exposed in chat or tickets.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import psycopg2
import sqlparse
from psycopg2.extensions import connection as PGConnection
from psycopg2.extensions import cursor as PGCursor


class WriteQueryNotAllowed(ValueError):
    """Raised when SQL is not a single read-only statement."""


def assert_read_only_sql(sql: str) -> None:
    statements = [
        s for s in sqlparse.parse(sql) if str(s).strip()
    ]
    if len(statements) != 1:
        raise WriteQueryNotAllowed(
            "Exactly one SQL statement is allowed per call."
        )
    first = statements[0].token_first(skip_ws=True, skip_cm=True)
    if first is None:
        raise WriteQueryNotAllowed("Empty SQL.")
    keyword = (first.value or "").strip().upper()
    if keyword not in ("SELECT", "WITH"):
        raise WriteQueryNotAllowed(
            f"Only SELECT or WITH queries are allowed (got {keyword!r})."
        )


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls, prefix: str) -> PostgresConfig:
        _DB_NAME_DEFAULTS = {
            "UNIFIED_DB": "unified_datastore",
            "TAO_DB": "tao_db",
        }

        def _req(name: str) -> str:
            v = os.environ.get(name)
            if not v or not str(v).strip():
                raise OSError(f"Missing required environment variable: {name}")
            return str(v).strip()

        port_raw = os.environ.get(f"{prefix}_PORT", "5432")
        db_name = os.environ.get(f"{prefix}_NAME", _DB_NAME_DEFAULTS.get(prefix, ""))
        if not db_name:
            raise OSError(f"Missing required environment variable: {prefix}_NAME")
        return cls(
            host=_req(f"{prefix}_HOST"),
            port=int(port_raw),
            database=db_name,
            user=_req(f"{prefix}_USER"),
            password=_req(f"{prefix}_PASSWORD"),
        )


def connect_readonly_session(cfg: PostgresConfig, *, connect_timeout: int = 30) -> PGConnection:
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.user,
        password=cfg.password,
        connect_timeout=connect_timeout,
        options="-c default_transaction_read_only=on",
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn


@contextmanager
def readonly_connection(cfg: PostgresConfig) -> Iterator[PGConnection]:
    conn = connect_readonly_session(cfg)
    try:
        yield conn
    finally:
        conn.close()


def fetch_all(
    conn: PGConnection,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = None,
) -> list[tuple[Any, ...]]:
    assert_read_only_sql(sql)
    cur: PGCursor = conn.cursor()
    try:
        cur.execute(sql, params)
        return list(cur.fetchall())
    finally:
        cur.close()


def fetch_one(
    conn: PGConnection,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = None,
) -> tuple[Any, ...] | None:
    assert_read_only_sql(sql)
    cur: PGCursor = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchone()
    finally:
        cur.close()


def unified_config_from_env() -> PostgresConfig:
    return PostgresConfig.from_env("UNIFIED_DB")


def tao_config_from_env() -> PostgresConfig:
    return PostgresConfig.from_env("TAO_DB")

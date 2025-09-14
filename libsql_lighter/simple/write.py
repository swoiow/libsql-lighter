from __future__ import annotations

import asyncio
import math
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

import libsql
import pandas as pd


@dataclass
class WriteOptions:
    table: str
    if_exists: str = "append"  # "append" | "replace" | "fail"
    index: bool = False
    chunksize: int = 1000
    primary_key: Optional[Sequence[str]] = None
    unique_together: Optional[Sequence[Sequence[str]]] = None
    indexes: Optional[Sequence[Tuple[str, Sequence[str], bool]]] = None
    upsert_conflict_cols: Optional[Sequence[str]] = None
    upsert_update_cols: Optional[Sequence[str]] = None


_SQLITE_TYPE_MAP = {
    "int": "INTEGER",
    "int32": "INTEGER",
    "int64": "INTEGER",
    "uint32": "INTEGER",
    "uint64": "INTEGER",
    "float": "REAL",
    "float32": "REAL",
    "float64": "REAL",
    "bool": "INTEGER",
    "datetime64[ns]": "TEXT",
    "object": "TEXT",
    "category": "TEXT",
}


def _quote_ident(name: str) -> str:
    return f"\"{name.replace('\"', '\"\"')}\""


def _infer_sqlite_type(dtype: Any) -> str:
    if isinstance(dtype, pd.CategoricalDtype):
        return "TEXT"
    name = str(dtype)
    for key, sqlt in _SQLITE_TYPE_MAP.items():
        if name.startswith(key):
            return sqlt
    return "TEXT"


def _coerce_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if pd.isna(v):
        return None
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.isoformat()
    return v


def _iter_rows(df: "pd.DataFrame", include_index: bool) -> Iterable[Tuple[Any, ...]]:
    cols = list(df.columns)
    if include_index:
        for idx, row in df.iterrows():
            yield tuple([_coerce_value(idx)] + [_coerce_value(row[c]) for c in cols])
    else:
        for _, row in df.iterrows():
            yield tuple(_coerce_value(row[c]) for c in cols)


async def _ensure_table(conn, df: "pd.DataFrame", opts: WriteOptions) -> None:
    cur = await asyncio.to_thread(
        conn.execute,
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (opts.table,),
    )
    exists = cur.fetchone() is not None

    if exists and opts.if_exists == "fail":
        raise RuntimeError(f"Table {opts.table} already exists")

    if exists and opts.if_exists == "replace":
        await asyncio.to_thread(conn.execute, f"DROP TABLE IF EXISTS {_quote_ident(opts.table)}")
        exists = False

    if not exists:
        cols_def: List[str] = []
        if opts.index:
            idx_sqltype = _infer_sqlite_type(df.index.dtype)
            cols_def.append(f"{_quote_ident('index')} {idx_sqltype}")

        for c in df.columns:
            sqlt = _infer_sqlite_type(df[c].dtype)
            cols_def.append(f"{_quote_ident(str(c))} {sqlt}")

        table_constraints: List[str] = []
        if opts.primary_key:
            pk_cols = ", ".join(_quote_ident(c) for c in opts.primary_key)
            table_constraints.append(f"PRIMARY KEY ({pk_cols})")
        if opts.unique_together:
            for group in opts.unique_together:
                uq_cols = ", ".join(_quote_ident(c) for c in group)
                table_constraints.append(f"UNIQUE ({uq_cols})")

        ddl_body = ",\n  ".join(cols_def + table_constraints)
        ddl = f"CREATE TABLE IF NOT EXISTS {_quote_ident(opts.table)} (\n  {ddl_body}\n)"
        await asyncio.to_thread(conn.execute, ddl)

        if opts.indexes:
            for name, cols, is_unique in opts.indexes:
                cols_sql = ", ".join(_quote_ident(c) for c in cols)
                unique_sql = "UNIQUE " if is_unique else ""
                await asyncio.to_thread(
                    conn.execute,
                    f"CREATE {unique_sql}INDEX IF NOT EXISTS {_quote_ident(name)} "
                    f"ON {_quote_ident(opts.table)} ({cols_sql})"
                )


def _build_insert_sql(
    table: str,
    col_idents: List[str],
    upsert_conflict_cols: Optional[Sequence[str]],
    upsert_update_cols: Optional[Sequence[str]],
) -> str:
    placeholders = ",".join(["?"] * len(col_idents))
    base = f"INSERT INTO {_quote_ident(table)} ({', '.join(col_idents)}) VALUES ({placeholders})"

    if not upsert_conflict_cols:
        return base

    conflict_cols_sql = ", ".join(_quote_ident(c) for c in upsert_conflict_cols)

    if not upsert_update_cols:
        update_targets = [c for c in col_idents if c.strip("\"") not in set(upsert_conflict_cols)]
    else:
        update_targets = [_quote_ident(c) for c in upsert_update_cols]

    if not update_targets:
        return f"{base} ON CONFLICT ({conflict_cols_sql}) DO NOTHING"

    set_clause = ", ".join(f"{col} = excluded.{col}" for col in update_targets)
    return f"{base} ON CONFLICT ({conflict_cols_sql}) DO UPDATE SET {set_clause}"


async def write_df_commit(
    df: "pd.DataFrame",
    db_path: str | Path,
    *,
    table: str,
    sync_url: Optional[str] = None,
    auth_token: Optional[str] = None,
    if_exists: str = "append",
    index: bool = False,
    chunksize: int = 1000,
    primary_key: Optional[Sequence[str]] = None,
    unique_together: Optional[Sequence[Sequence[str]]] = None,
    indexes: Optional[Sequence[Tuple[str, Sequence[str], bool]]] = None,
    upsert_conflict_cols: Optional[Sequence[str]] = None,
    upsert_update_cols: Optional[Sequence[str]] = None,
) -> None:
    if isinstance(db_path, Path):
        db_path = str(db_path)

    sync_url = sync_url or os.getenv("LIBSQL_URL")
    auth_token = auth_token or os.getenv("LIBSQL_AUTH_TOKEN")

    print(f"syncing with {sync_url}")

    conn = libsql.connect(db_path, sync_url=sync_url, auth_token=auth_token)
    try:
        opts = WriteOptions(
            table=table,
            if_exists=if_exists,
            index=index,
            chunksize=chunksize,
            primary_key=primary_key,
            unique_together=unique_together,
            indexes=indexes,
            upsert_conflict_cols=upsert_conflict_cols,
            upsert_update_cols=upsert_update_cols,
        )

        await _ensure_table(conn, df, opts)

        col_idents: List[str] = []
        if index:
            col_idents.append(_quote_ident("index"))
        col_idents.extend(_quote_ident(str(c)) for c in df.columns)

        sql = _build_insert_sql(table, col_idents, upsert_conflict_cols, upsert_update_cols)

        rows_iter = _iter_rows(df, include_index=index)
        batch: List[Tuple[Any, ...]] = []

        for row in rows_iter:
            batch.append(row)
            if len(batch) >= chunksize:
                await asyncio.to_thread(conn.executemany, sql, batch)
                batch.clear()

        if batch:
            await asyncio.to_thread(conn.executemany, sql, batch)

        await asyncio.to_thread(conn.commit)
        await asyncio.to_thread(conn.sync)
    finally:
        conn.close()


def write_df_commit_sync(*args, **kwargs) -> None:
    """同步接口，内部用 asyncio.run() 包装"""
    return asyncio.run(write_df_commit(*args, **kwargs))

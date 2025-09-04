from __future__ import annotations

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

    # ✅ 新增：约束与索引
    primary_key: Optional[Sequence[str]] = None  # 例: ("id",)
    unique_together: Optional[Sequence[Sequence[str]]] = None  # 例: (("a","b"),)
    indexes: Optional[Sequence[Tuple[str, Sequence[str], bool]]] = None
    # indexes: [("idx_name", ("col1","col2"), False/True-is-unique), ...]

    # ✅ 新增：UPSERT
    upsert_conflict_cols: Optional[Sequence[str]] = None  # 例: ("id",)
    upsert_update_cols: Optional[Sequence[str]] = None  # 为空=默认更新所有非冲突列
    # 注意：SQLite 的 ON CONFLICT 需要冲突目标有 PK/UNIQUE 约束或 UNIQUE 索引


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


def _infer_sqlite_type(
    dtype: pd.api.types.CategoricalDtype | pd.Series | pd.api.extensions.ExtensionDtype | Any) -> str:
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


def _ensure_table(conn: "libsql.Connection", df: "pd.DataFrame", opts: WriteOptions) -> None:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (opts.table,),
    )
    exists = cur.fetchone() is not None

    if exists and opts.if_exists == "fail":
        raise RuntimeError(f"Table {opts.table} already exists")

    if exists and opts.if_exists == "replace":
        conn.execute(f"DROP TABLE IF EXISTS {_quote_ident(opts.table)}")
        exists = False

    if not exists:
        cols_def: List[str] = []
        # index 列
        if opts.index:
            idx_dtype = df.index.dtype
            idx_sqltype = _infer_sqlite_type(idx_dtype)
            cols_def.append(f"{_quote_ident('index')} {idx_sqltype}")

        # data 列
        for c in df.columns:
            sqlt = _infer_sqlite_type(df[c].dtype)
            cols_def.append(f"{_quote_ident(str(c))} {sqlt}")

        # ✅ 主键/唯一组合约束
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
        conn.execute(ddl)

        # ✅ 普通/唯一索引（非表级约束的索引在建表后创建）
        if opts.indexes:
            for name, cols, is_unique in opts.indexes:
                cols_sql = ", ".join(_quote_ident(c) for c in cols)
                unique_sql = "UNIQUE " if is_unique else ""
                conn.execute(
                    f"CREATE {unique_sql}INDEX IF NOT EXISTS {_quote_ident(name)} "
                    f"ON {_quote_ident(opts.table)} ({cols_sql})"
                )


def _build_insert_sql(
    table: str, col_idents: List[str], upsert_conflict_cols: Optional[Sequence[str]],
    upsert_update_cols: Optional[Sequence[str]]) -> str:
    placeholders = ",".join(["?"] * len(col_idents))
    base = f"INSERT INTO {_quote_ident(table)} ({', '.join(col_idents)}) VALUES ({placeholders})"

    if not upsert_conflict_cols:
        return base

    conflict_cols_sql = ", ".join(_quote_ident(c) for c in upsert_conflict_cols)

    # 需要更新的列集合：默认=所有非冲突列
    if upsert_update_cols is None or len(upsert_update_cols) == 0:
        update_targets = [c for c in col_idents if c.strip("\"") not in set(upsert_conflict_cols)]
    else:
        update_targets = [_quote_ident(c) for c in upsert_update_cols]

    if not update_targets:
        # 没有可更新列则转为 DO NOTHING
        return f"{base} ON CONFLICT ({conflict_cols_sql}) DO NOTHING"

    set_clause = ", ".join(f"{col} = excluded.{col}" for col in update_targets)
    return f"{base} ON CONFLICT ({conflict_cols_sql}) DO UPDATE SET {set_clause}"


def write_df_commit_sync(
    df: "pd.DataFrame",
    db_path: str | Path,
    *,
    table: str,
    sync_url: Optional[str] = None,
    auth_token: Optional[str] = None,
    if_exists: str = "append",
    index: bool = False,
    chunksize: int = 1000,

    # ✅ 新增能力
    primary_key: Optional[Sequence[str]] = None,
    unique_together: Optional[Sequence[Sequence[str]]] = None,
    indexes: Optional[Sequence[Tuple[str, Sequence[str], bool]]] = None,
    upsert_conflict_cols: Optional[Sequence[str]] = None,
    upsert_update_cols: Optional[Sequence[str]] = None,
) -> None:
    """
    将 DataFrame 写入本地 libsql/SQLite 数据库，支持：
    - 主键 / 唯一组合约束
    - 普通/唯一索引
    - UPSERT (ON CONFLICT DO UPDATE)
    并在一次事务中 commit + sync。
    """
    if isinstance(db_path, Path):
        db_path = str(db_path)

    if sync_url is None:
        sync_url = os.getenv("LIBSQL_URL")
    if auth_token is None:
        auth_token = os.getenv("LIBSQL_AUTH_TOKEN")

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

        _ensure_table(conn, df, opts)

        # 组装列清单
        col_idents: List[str] = []
        if index:
            col_idents.append(_quote_ident("index"))
        col_idents.extend(_quote_ident(str(c)) for c in df.columns)

        # ✅ 构造 INSERT / UPSERT 语句
        sql = _build_insert_sql(
            table=table,
            col_idents=col_idents,
            upsert_conflict_cols=upsert_conflict_cols,
            upsert_update_cols=upsert_update_cols,
        )

        # 批量写入
        rows_iter = _iter_rows(df, include_index=index)
        batch: List[Tuple[Any, ...]] = []

        for row in rows_iter:
            batch.append(row)
            if len(batch) >= chunksize:
                conn.executemany(sql, batch)
                batch.clear()

        if batch:
            conn.executemany(sql, batch)

        conn.commit()
        conn.sync()
    finally:
        conn.close()


if __name__ == '__main__':
    df = pd.DataFrame()
    # 1) 唯一约束 + UPSERT（按 url 去重更新）
    write_df_commit_sync(
        df,
        db_path="hello.db",
        table="food_safety",
        if_exists="append",
        # 确保冲突目标存在唯一约束/索引
        unique_together=[("url",)],  # 或 primary_key=("url",)
        # UPSERT：冲突时更新
        upsert_conflict_cols=("url",),
        # 为空表示更新所有非冲突列；也可以指定：
        # upsert_update_cols=("problem", "publish_time", "food_safety_hazard"),
    )
    # 2) 复合唯一约束 + 普通索引
    write_df_commit_sync(
        df,
        db_path="hello.db",
        table="food_safety",
        if_exists="append",
        unique_together=[("publish_time", "problem")],
        indexes=[
            ("idx_pubtime", ("publish_time",), False),  # 普通索引
            ("uq_url", ("url",), True),  # 唯一索引（等效于 unique_together=[("url",)])
        ],
        upsert_conflict_cols=("publish_time", "problem"),
    )
    # 3) 主键 + 指定更新列
    write_df_commit_sync(
        df,
        db_path="hello.db",
        table="events",
        if_exists="append",
        primary_key=("id",),  # PK
        upsert_conflict_cols=("id",),
        upsert_update_cols=("updated_at", "status"),  # 仅更新部分列
    )

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple

import libsql
import pandas as pd


@dataclass
class WriteOptions:
    table: str
    if_exists: str = "append"  # "append" | "replace" | "fail"
    index: bool = False  # 是否写入 df.index
    chunksize: int = 1000  # 批量插入大小


_SQLITE_TYPE_MAP = {
    "int": "INTEGER",
    "int32": "INTEGER",
    "int64": "INTEGER",
    "uint32": "INTEGER",
    "uint64": "INTEGER",
    "float": "REAL",
    "float32": "REAL",
    "float64": "REAL",
    "bool": "INTEGER",  # SQLite 没有原生 BOOL，用 0/1
    "datetime64[ns]": "TEXT",  # 用 ISO8601 字符串
    "object": "TEXT",
    "category": "TEXT",
}


def _quote_ident(name: str) -> str:
    # 双引号包裹标识符，内部双引号用 "" 转义
    return f"\"{name.replace('\"', '\"\"')}\""


def _infer_sqlite_type(
    dtype: pd.api.types.CategoricalDtype | pd.Series | pd.api.extensions.ExtensionDtype | Any) -> str:
    # dtype -> SQLite 列类型
    if isinstance(dtype, pd.CategoricalDtype):
        return "TEXT"
    name = str(dtype)
    for key, sqlt in _SQLITE_TYPE_MAP.items():
        if name.startswith(key):
            return sqlt
    return "TEXT"


def _coerce_value(v: Any) -> Any:
    # 将 pandas 的 NaN/NaT 转为 None；datetime 转 ISO8601 字符串
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if pd.isna(v):
        return None
    if isinstance(v, (pd.Timestamp, datetime)):
        # 统一用 ISO8601，避免时区歧义
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

    if not exists or opts.if_exists == "replace":
        cols_def: List[str] = []
        if opts.index:
            # index 列类型推断
            idx_dtype = df.index.dtype
            idx_sqltype = _infer_sqlite_type(idx_dtype)
            cols_def.append(f"{_quote_ident('index')} {idx_sqltype}")
        for c in df.columns:
            sqlt = _infer_sqlite_type(df[c].dtype)
            cols_def.append(f"{_quote_ident(str(c))} {sqlt}")

        ddl = f"CREATE TABLE IF NOT EXISTS {_quote_ident(opts.table)} (\n  " + ",\n  ".join(cols_def) + "\n)"
        conn.execute(ddl)


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
) -> None:
    """
    将 DataFrame 写入本地 libsql/SQLite 数据库文件，并在一次事务中 commit + sync。

    - db_path: 本地库文件路径，如 "hello.db"
    - table: 目标表名（自动建表/替换，受 if_exists 控制）
    - sync_url / auth_token: 远端同步参数（可从环境变量传入）
    - if_exists: "append" | "replace" | "fail"
    - index: 是否写入 df.index
    - chunksize: executemany 的批量大小
    """
    if isinstance(db_path, Path):
        db_path = str(db_path)

    if sync_url is None:
        sync_url = os.getenv("LIBSQL_URL")
    if auth_token is None:
        auth_token = os.getenv("LIBSQL_AUTH_TOKEN")

    print(f"syncing with {sync_url}")

    conn = libsql.connect(
        db_path,
        sync_url=sync_url,
        auth_token=auth_token,
    )
    try:
        opts = WriteOptions(table=table, if_exists=if_exists, index=index, chunksize=chunksize)

        # 建表/重建
        _ensure_table(conn, df, opts)

        # 组装 INSERT 语句
        col_idents: List[str] = []
        if index:
            col_idents.append(_quote_ident("index"))
        col_idents.extend(_quote_ident(str(c)) for c in df.columns)

        placeholders = ",".join(["?"] * len(col_idents))
        sql = f"INSERT INTO {_quote_ident(table)} ({', '.join(col_idents)}) VALUES ({placeholders})"

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

        # 一次性提交 + 同步
        conn.commit()
        conn.sync()
    finally:
        conn.close()


if __name__ == "__main__":
    df = pd.DataFrame(
        {
            "problem": ["A", "B", None],
            "publish_time": [pd.Timestamp("2025-09-01"), pd.Timestamp("2025-09-02"), pd.NaT],
            "score": [1.5, 2.0, float("nan")],
            "ok": [True, False, True],
        }
    )

    write_df_commit_sync(
        df,
        db_path="hello.db",
        table="users",
        if_exists="append",  # 首次不存在会自动建表；改成 "replace" 会重建
        index=False,  # 如需写入索引改 True
        chunksize=1000,
        # 默认从环境变量读取 LIBSQL_URL/LIBSQL_AUTH_TOKEN；也可在这里显式传入：
        # sync_url=DB_URI,
        # auth_token=DB_TOKEN,
    )

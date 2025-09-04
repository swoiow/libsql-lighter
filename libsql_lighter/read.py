from __future__ import annotations

import os
from pathlib import Path
from typing import Any, List, Optional, Sequence

import libsql
import pandas as pd


def _maybe_env(val: Optional[str], key: str) -> Optional[str]:
    return val if val is not None else os.getenv(key)


def read_sql_df(
    sql: str,
    db_path: str | Path,
    *,
    params: Optional[Sequence[Any]] = None,  # 使用 ? 占位符的参数序列
    sync_url: Optional[str] = None,
    auth_token: Optional[str] = None,
    parse_dates: Optional[List[str]] = None,  # 需要转为 datetime 的列名列表
) -> "pd.DataFrame":
    """
    用 libsql 执行任意 SQL 并返回 DataFrame。
    - 若提供 sync_url（或设了 LIBSQL_URL 环境变量），读取前会先 conn.sync()。
    - params 采用 SQLite 风格的 '?' 占位符序列。
    """
    if isinstance(db_path, Path):
        db_path = str(db_path)

    sync_url = _maybe_env(sync_url, "LIBSQL_URL")
    auth_token = _maybe_env(auth_token, "LIBSQL_AUTH_TOKEN")

    conn = libsql.connect(db_path, sync_url=sync_url, auth_token=auth_token)
    try:
        # 读取前拉取远端最新（如果配置了同步）
        if sync_url:
            conn.sync()

        cur = conn.execute(sql, params or [])
        rows = cur.fetchall()
        # DB-API: cursor.description[i][0] 是列名
        cols = [d[0] for d in (cur.description or [])]
        df = pd.DataFrame(rows, columns=cols)

        if parse_dates:
            for c in parse_dates:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors="coerce")
        return df
    finally:
        conn.close()


def _quote_ident(name: str) -> str:
    return f"\"{str(name).replace('\"', '\"\"')}\""


def read_table_df(
    db_path: str | Path,
    table: str,
    *,
    columns: Optional[Sequence[str]] = None,  # None = *
    where: Optional[str] = None,  # 例如: "publish_time >= ? AND ch_classify = ?"
    where_params: Optional[Sequence[Any]] = None,  # 与 where 中的 ? 对应
    order_by: Optional[str] = None,  # 例如: "publish_time DESC"
    limit: Optional[int] = None,
    sync_url: Optional[str] = None,
    auth_token: Optional[str] = None,
    parse_dates: Optional[List[str]] = None,
) -> "pd.DataFrame":
    """
    便捷读取整表/部分列，并可附带 WHERE / ORDER BY / LIMIT。
    - where 使用 '?' 占位符，并通过 where_params 传参。
    """
    col_sql = "*"
    if columns:
        col_sql = ", ".join(_quote_ident(c) for c in columns)

    sql_parts: List[str] = [f"SELECT {col_sql} FROM {_quote_ident(table)}"]
    params: List[Any] = []

    if where:
        sql_parts.append(f"WHERE {where}")
        if where_params:
            params.extend(where_params)

    if order_by:
        sql_parts.append(f"ORDER BY {order_by}")

    if limit is not None:
        sql_parts.append("LIMIT ?")
        params.append(int(limit))

    sql = " ".join(sql_parts)
    return read_sql_df(
        sql,
        db_path=db_path,
        params=params,
        sync_url=sync_url,
        auth_token=auth_token,
        parse_dates=parse_dates,
    )


if __name__ == '__main__':
    # 1) 任意 SQL
    df = read_sql_df(
        "SELECT * FROM users WHERE DESC LIMIT ?",
        db_path="hello.db",
        params=[10],
        # 若未显式传入，会从环境变量 LIBSQL_URL / LIBSQL_AUTH_TOKEN 读取
        # sync_url="wss://xxx.turso.io",
        # auth_token="xxxxx",
        parse_dates=["publish_time"],  # 可选：将该列转成 datetime
    )

    # 2) 读整表/部分列
    df2 = read_table_df(
        db_path="hello.db",
        table="users",
        columns=["id", "publish_time"],
        where="publish_time >= ?",
        where_params=["2025-09-01T00:00:00"],
        order_by="publish_time DESC",
        limit=100,
        parse_dates=["publish_time"],
    )

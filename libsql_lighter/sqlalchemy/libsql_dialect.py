# libsql_dialect.py
import libsql
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite


class LibSQLDialect(SQLiteDialect_pysqlite):
    driver = "libsql"

    @classmethod
    def dbapi(cls):
        return libsql

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        db = opts.get("database", ":memory:")

        connect_args = {}
        if url.host:
            scheme = url.drivername.split("+")[-1] if "+" in url.drivername else "libsql"
            port = f":{url.port}" if url.port else ""
            connect_args["sync_url"] = f"{scheme}://{url.host}{port}"
        if url.username and url.password:
            connect_args["auth_token"] = url.password

        return (db,), connect_args

    # ✅ 自动 commit + sync
    def do_commit(self, dbapi_connection):
        super().do_commit(dbapi_connection)
        if hasattr(dbapi_connection, "sync"):
            dbapi_connection.sync()


# 注册 dialect
registry.register("libsql", __name__, "LibSQLDialect")

### 使用示例

#### 同步模式

```python
from sqlalchemy import create_engine, text

engine = create_engine("libsql:///hello.db?host=myremote.com:8080&password=TOKEN")

with engine.begin() as conn:
    conn.execute(text("CREATE TABLE IF NOT EXISTS food_safety (id INTEGER PRIMARY KEY, problem TEXT)"))
    conn.execute(text("INSERT INTO food_safety(problem) VALUES ('sync test')"))
    # 提交时自动 sync()
```

#### 异步模式

异步只需要用 `create_async_engine`，它内部还是用 `LibSQLDialect`，只不过运行在 `asyncio` 包装里。

```python
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

async def main():
    engine = create_async_engine("libsql+aiosqlite:///hello.db?host=myremote.com:8080&password=TOKEN")

    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, status TEXT)"))
        await conn.execute(text("INSERT INTO events(status) VALUES ('ok')"))
        # commit 时 dialect.do_commit() 自动调用 sync()

asyncio.run(main())
```

> 注意：这里用的是 `libsql+async:///`，因为 SQLAlchemy 的 `AsyncEngine` 需要一个 **异步驱动** 名称。
> 我们可以继续用 `LibSQLDialect`，SQLAlchemy 会自动找到它。

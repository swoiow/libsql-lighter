# libsql-lighter

A lightweight adapter for bridging **pandas DataFrame** with **libsql**.

- ✅ One-step **commit + sync**
- ✅ Simple `write_df_commit_sync(df, ...)`
- ✅ Simple `read_sql_df(...)` / `read_table_df(...)`

---

## Quick Example

```python
import pandas as pd
from libsql_lighter import write_df_commit_sync, read_sql_df

df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

# Write DataFrame into libsql, commit & sync
write_df_commit_sync(df, db_path="hello.db", table="users", if_exists="replace")

# Read it back
df2 = read_sql_df("SELECT * FROM users", db_path="hello.db")
print(df2)
```

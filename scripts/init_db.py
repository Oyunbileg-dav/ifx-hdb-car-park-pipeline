
from pathlib import Path
from etl.db import executescript

sql = Path("sql/001_schema.sql").read_text()
executescript(sql)
print("DB schema initialized")

from pathlib import Path

AEDA_DIR = Path(__file__).parent.absolute()

SQLITE3_DB_DIR = AEDA_DIR / "metadata" / "aeda_metadata.db"
SUPPORTED_DB_ENGINES = ["sqlite3"]
SQL_SCRIPTS_DIR = AEDA_DIR / "sql_scripts"
SQL_CREATE_SCRIPTS = {"sqlite3": SQL_SCRIPTS_DIR / "sqlite3" / "sqlite3.sql"}

"""Catalog SQLite database — schema and batch operations."""

import json
import platform
import sqlite3
from datetime import datetime, timezone

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS catalog_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS folders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER REFERENCES folders(id),
    name TEXT NOT NULL,
    rel_path TEXT NOT NULL UNIQUE,
    hidden INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    folder_id INTEGER REFERENCES folders(id),
    filename TEXT NOT NULL,
    rel_path TEXT NOT NULL UNIQUE,
    file_size INTEGER NOT NULL,
    file_type_high TEXT,
    file_type_low TEXT,
    hash_partial TEXT,
    hash_fast TEXT,
    hash_strong TEXT,
    created_date TEXT,
    modified_date TEXT,
    hidden INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_cat_files_folder ON files(folder_id);
CREATE INDEX IF NOT EXISTS idx_cat_files_rel_path ON files(rel_path);
"""


class CatalogDB:
    """Manages the catalog SQLite database."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: sqlite3.Connection | None = None
        self._folder_cache: dict[str, int] = {}  # rel_path -> folder_id

    def open(self, resume: bool = False):
        """Open or create the catalog database."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        for stmt in SCHEMA.split(";"):
            s = stmt.strip()
            if s:
                self.conn.execute(s)
        self.conn.commit()

        if resume:
            # Rebuild folder cache from existing data
            rows = self.conn.execute("SELECT id, rel_path FROM folders").fetchall()
            for r in rows:
                self._folder_cache[r["rel_path"]] = r["id"]

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def set_meta(self, key: str, value: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO catalog_meta (key, value) VALUES (?, ?)",
            (key, value),
        )
        self.conn.commit()

    def get_meta(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM catalog_meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def write_initial_meta(self, root_path: str):
        """Write metadata for a new catalog."""
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.set_meta("root_path", root_path)
        self.set_meta("created_at", now)
        self.set_meta("hostname", platform.node())
        self.set_meta("platform", platform.system())
        self.set_meta("version", "1")

    def mark_complete(self):
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.set_meta("completed_at", now)

    def save_traversal_state(self, dirs: list[str]):
        """Persist remaining BFS queue for resume support."""
        self.set_meta("traversal_state", json.dumps(dirs))

    def load_traversal_state(self) -> list[str] | None:
        """Load saved BFS queue. Returns None if not found."""
        raw = self.get_meta("traversal_state")
        if raw is None:
            return None
        return json.loads(raw)

    def get_visited_dirs(self) -> set[str]:
        """Return set of rel_paths for already-visited directories."""
        rows = self.conn.execute("SELECT DISTINCT rel_path FROM folders").fetchall()
        return {r["rel_path"] for r in rows}

    def get_or_create_folder(self, rel_path: str, name: str, hidden: bool) -> int:
        """Get or create a folder, returning its ID. Uses an in-memory cache."""
        cached = self._folder_cache.get(rel_path)
        if cached is not None:
            return cached

        # Determine parent
        parent_path = "/".join(rel_path.split("/")[:-1]) if "/" in rel_path else ""
        parent_id = self._folder_cache.get(parent_path) if parent_path else None

        cursor = self.conn.execute(
            "INSERT INTO folders (parent_id, name, rel_path, hidden) VALUES (?, ?, ?, ?)",
            (parent_id, name, rel_path, 1 if hidden else 0),
        )
        folder_id = cursor.lastrowid
        self._folder_cache[rel_path] = folder_id
        return folder_id

    def insert_files_batch(self, files: list[tuple]):
        """Bulk insert files. Each tuple:
        (folder_id, filename, rel_path, file_size, file_type_high,
         file_type_low, hash_partial, hash_fast, hash_strong,
         created_date, modified_date, hidden)
        """
        self.conn.executemany(
            "INSERT OR IGNORE INTO files "
            "(folder_id, filename, rel_path, file_size, file_type_high, "
            "file_type_low, hash_partial, hash_fast, hash_strong, "
            "created_date, modified_date, hidden) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            files,
        )
        self.conn.commit()

    def file_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM files").fetchone()
        return row[0]

    def folder_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM folders").fetchone()
        return row[0]

    def total_size(self) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(file_size), 0) FROM files"
        ).fetchone()
        return row[0]

"""Import a catalog DB into a File Hunter server database."""

import json
import os
import sqlite3
import time


BATCH_SIZE = 5000


def import_catalog(
    catalog_path: str,
    server_db_path: str,
    agent_id: int,
    root_path: str,
    location_name: str | None = None,
):
    """Bulk import a catalog into the server DB.

    - Creates or matches a location
    - Inserts folders with correct parent chains
    - Bulk inserts files in batches
    - Recalculates location sizes at the end
    """
    cat = sqlite3.connect(catalog_path)
    cat.row_factory = sqlite3.Row

    srv = sqlite3.connect(server_db_path)
    srv.row_factory = sqlite3.Row
    srv.execute("PRAGMA journal_mode=WAL")
    srv.execute("PRAGMA busy_timeout=30000")
    srv.execute("PRAGMA foreign_keys=ON")

    # Validate catalog
    meta_row = cat.execute(
        "SELECT value FROM catalog_meta WHERE key = 'completed_at'"
    ).fetchone()
    if not meta_row or not meta_row["value"]:
        print("Warning: catalog was not marked as complete (may be partial)")

    cat_root = cat.execute(
        "SELECT value FROM catalog_meta WHERE key = 'root_path'"
    ).fetchone()
    if cat_root:
        print(f"Catalog root: {cat_root['value']}")

    # Resolve or create location
    location_id = _resolve_location(srv, agent_id, root_path, location_name)
    print(f"Location ID: {location_id}")

    t0 = time.monotonic()

    # Import folders — sorted by rel_path length so parents come first
    cat_folders = cat.execute(
        "SELECT id, parent_id, name, rel_path, hidden FROM folders ORDER BY length(rel_path)"
    ).fetchall()

    # Build catalog_folder_id -> server_folder_id mapping
    folder_map: dict[int, int] = {}
    cat_parent_map: dict[int, int | None] = {}  # catalog_id -> catalog_parent_id

    for f in cat_folders:
        cat_parent_map[f["id"]] = f["parent_id"]

    folders_created = 0
    folders_existing = 0
    for f in cat_folders:
        cat_id = f["id"]
        rel_path = f["rel_path"]

        # Check if folder already exists in server
        existing = srv.execute(
            "SELECT id FROM folders WHERE location_id = ? AND rel_path = ?",
            (location_id, rel_path),
        ).fetchone()

        if existing:
            folder_map[cat_id] = existing["id"]
            folders_existing += 1
            continue

        # Determine server parent_id
        cat_parent = f["parent_id"]
        server_parent = folder_map.get(cat_parent) if cat_parent else None

        cursor = srv.execute(
            "INSERT INTO folders (location_id, parent_id, name, rel_path, hidden) "
            "VALUES (?, ?, ?, ?, ?)",
            (location_id, server_parent, f["name"], rel_path, f["hidden"]),
        )
        folder_map[cat_id] = cursor.lastrowid
        folders_created += 1

    srv.commit()
    print(f"Folders: {folders_created:,} created, {folders_existing:,} existing")

    # Import files in batches
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    total_files = cat.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    print(f"Importing {total_files:,} files...")

    files_imported = 0
    offset = 0

    while True:
        rows = cat.execute(
            "SELECT folder_id, filename, rel_path, file_size, "
            "file_type_high, file_type_low, hash_partial, hash_fast, hash_strong, "
            "created_date, modified_date, hidden "
            "FROM files LIMIT ? OFFSET ?",
            (BATCH_SIZE, offset),
        ).fetchall()

        if not rows:
            break

        batch = []
        for r in rows:
            cat_folder_id = r["folder_id"]
            server_folder_id = folder_map.get(cat_folder_id) if cat_folder_id else None
            full_path = os.path.join(root_path, r["rel_path"])

            batch.append(
                (
                    r["filename"],
                    full_path,
                    r["rel_path"],
                    location_id,
                    server_folder_id,
                    r["file_type_high"],
                    r["file_type_low"],
                    r["file_size"],
                    r["hash_partial"],
                    r["hash_fast"],
                    r["hash_strong"],
                    "",  # description
                    "",  # tags
                    r["created_date"],
                    r["modified_date"],
                    now,  # date_cataloged
                    now,  # date_last_seen
                    r["hidden"],
                )
            )

        srv.executemany(
            "INSERT OR IGNORE INTO files "
            "(filename, full_path, rel_path, location_id, folder_id, "
            "file_type_high, file_type_low, file_size, "
            "hash_partial, hash_fast, hash_strong, "
            "description, tags, created_date, modified_date, "
            "date_cataloged, date_last_seen, hidden) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        srv.commit()

        files_imported += len(batch)
        offset += BATCH_SIZE

        elapsed = time.monotonic() - t0
        rate = files_imported / elapsed if elapsed > 0 else 0
        print(
            f"\r  {files_imported:,}/{total_files:,} files | {rate:.0f} files/sec",
            end="",
            flush=True,
        )

    print()

    # Recalculate location sizes
    print("Recalculating sizes...")
    _recalculate_location_sizes(srv, location_id)

    elapsed = time.monotonic() - t0
    print(f"Import complete in {elapsed:.1f}s")
    print(f"  Files imported: {files_imported:,}")
    print(f"  Folders: {folders_created:,} new, {folders_existing:,} existing")

    cat.close()
    srv.close()


def _resolve_location(
    srv: sqlite3.Connection,
    agent_id: int,
    root_path: str,
    location_name: str | None,
) -> int:
    """Find or create the target location."""
    # Try to match existing
    row = srv.execute(
        "SELECT id, name FROM locations WHERE agent_id = ? AND root_path = ?",
        (agent_id, root_path),
    ).fetchone()

    if row:
        print(f"Matched existing location: {row['name']} (id={row['id']})")
        return row["id"]

    # Create new
    name = location_name or os.path.basename(root_path) or root_path
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    cursor = srv.execute(
        "INSERT INTO locations (name, root_path, agent_id, date_added, total_size) "
        "VALUES (?, ?, ?, ?, 0)",
        (name, root_path, agent_id, now),
    )
    srv.commit()
    loc_id = cursor.lastrowid
    print(f"Created location: {name} (id={loc_id})")
    return loc_id


def _recalculate_location_sizes(srv: sqlite3.Connection, location_id: int):
    """Rebuild all stored counters for a location. Synchronous version of
    file_hunter.services.sizes.recalculate_location_sizes.
    """
    # Direct per-folder aggregates
    direct_rows = srv.execute(
        "SELECT folder_id, SUM(file_size) AS total, COUNT(*) AS cnt "
        "FROM files WHERE location_id = ? AND stale = 0 GROUP BY folder_id",
        (location_id,),
    ).fetchall()

    direct_size: dict[int | None, int] = {}
    direct_count: dict[int | None, int] = {}
    root_file_size = 0
    root_file_count = 0
    for r in direct_rows:
        fid = r["folder_id"]
        if fid is None:
            root_file_size = r["total"] or 0
            root_file_count = r["cnt"] or 0
        else:
            direct_size[fid] = r["total"] or 0
            direct_count[fid] = r["cnt"] or 0

    # Type counts per folder
    type_rows = srv.execute(
        "SELECT folder_id, file_type_high, COUNT(*) AS cnt "
        "FROM files WHERE location_id = ? AND stale = 0 "
        "GROUP BY folder_id, file_type_high",
        (location_id,),
    ).fetchall()

    direct_types: dict[int | None, dict[str, int]] = {}
    root_types: dict[str, int] = {}
    for r in type_rows:
        fid = r["folder_id"]
        ft = r["file_type_high"] or "other"
        cnt = r["cnt"] or 0
        if fid is None:
            root_types[ft] = root_types.get(ft, 0) + cnt
        else:
            if fid not in direct_types:
                direct_types[fid] = {}
            direct_types[fid][ft] = direct_types[fid].get(ft, 0) + cnt

    # Dup counts per folder
    dup_rows = srv.execute(
        "SELECT folder_id, COUNT(*) AS cnt "
        "FROM files WHERE location_id = ? AND dup_count > 0 AND stale = 0 "
        "GROUP BY folder_id",
        (location_id,),
    ).fetchall()

    direct_dups: dict[int | None, int] = {}
    root_dup_count = 0
    for r in dup_rows:
        fid = r["folder_id"]
        if fid is None:
            root_dup_count = r["cnt"] or 0
        else:
            direct_dups[fid] = r["cnt"] or 0

    # Build folder tree
    folder_rows = srv.execute(
        "SELECT id, parent_id FROM folders WHERE location_id = ?",
        (location_id,),
    ).fetchall()

    children_of: dict[int | None, list[int]] = {}
    all_folder_ids = []
    for f in folder_rows:
        fid = f["id"]
        pid = f["parent_id"]
        all_folder_ids.append(fid)
        if pid not in children_of:
            children_of[pid] = []
        children_of[pid].append(fid)

    # Bottom-up accumulation
    cum_size: dict[int, int] = {}
    cum_count: dict[int, int] = {}
    cum_types: dict[int, dict[str, int]] = {}
    cum_dups: dict[int, int] = {}

    def accumulate(fid):
        size = direct_size.get(fid, 0)
        count = direct_count.get(fid, 0)
        types = dict(direct_types.get(fid, {}))
        dups = direct_dups.get(fid, 0)
        for child_id in children_of.get(fid, []):
            accumulate(child_id)
            size += cum_size[child_id]
            count += cum_count[child_id]
            dups += cum_dups[child_id]
            for t, c in cum_types[child_id].items():
                types[t] = types.get(t, 0) + c
        cum_size[fid] = size
        cum_count[fid] = count
        cum_types[fid] = types
        cum_dups[fid] = dups

    for root_id in children_of.get(None, []):
        accumulate(root_id)

    # Batch update folders
    srv.executemany(
        "UPDATE folders SET total_size = ?, file_count = ?, "
        "type_counts = ?, duplicate_count = ? WHERE id = ?",
        [
            (
                cum_size.get(fid, 0),
                cum_count.get(fid, 0),
                json.dumps(cum_types.get(fid, {})),
                cum_dups.get(fid, 0),
                fid,
            )
            for fid in all_folder_ids
        ],
    )

    # Update location totals
    loc_total_size = root_file_size + sum(
        direct_size.get(fid, 0) for fid in all_folder_ids
    )
    loc_total_count = root_file_count + sum(
        direct_count.get(fid, 0) for fid in all_folder_ids
    )
    loc_types = dict(root_types)
    for fid in all_folder_ids:
        for t, c in direct_types.get(fid, {}).items():
            loc_types[t] = loc_types.get(t, 0) + c
    loc_dup_count = root_dup_count + sum(
        direct_dups.get(fid, 0) for fid in all_folder_ids
    )

    srv.execute(
        "UPDATE locations SET total_size = ?, file_count = ?, "
        "type_counts = ?, duplicate_count = ? WHERE id = ?",
        (
            loc_total_size,
            loc_total_count,
            json.dumps(loc_types),
            loc_dup_count,
            location_id,
        ),
    )
    srv.commit()

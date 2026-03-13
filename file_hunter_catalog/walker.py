"""Recursive directory walker with hashing and batched DB writes."""

import os
import stat
import time
from collections import deque
from datetime import datetime, timezone

from file_hunter_catalog.catalog_db import CatalogDB
from file_hunter_catalog.classify import classify_file, format_elapsed, format_size
from file_hunter_catalog.hasher import hash_file_partial_sync

BATCH_SIZE = 5000
TRAVERSAL_SAVE_INTERVAL = 30  # seconds


def walk_and_catalog(
    root_path: str,
    db: CatalogDB,
    *,
    no_hash: bool = False,
    resume: bool = False,
):
    """Walk the file tree and write everything to the catalog DB.

    Returns (files_cataloged, files_skipped, total_bytes).
    """
    root_path = os.path.abspath(root_path)

    # Resume or start fresh
    if resume:
        saved = db.load_traversal_state()
        if saved:
            dirs_to_visit = deque(saved)
            print(f"Resuming: {len(dirs_to_visit)} directories remaining")
        else:
            dirs_to_visit = deque([root_path])
            print("No saved state, starting fresh")
        visited = db.get_visited_dirs()
    else:
        dirs_to_visit = deque([root_path])
        visited = set()

    files_cataloged = 0
    files_skipped = 0
    total_bytes = 0
    batch: list[tuple] = []
    t0 = time.monotonic()
    last_report = t0
    last_save = t0

    while dirs_to_visit:
        dirpath = dirs_to_visit.popleft()
        rel_dir = os.path.relpath(dirpath, root_path)
        if rel_dir == ".":
            rel_dir = ""

        # Skip already-visited directories on resume
        if resume and rel_dir and rel_dir in visited:
            continue

        # Determine hidden status from path
        parent_hidden = (
            any(p.startswith(".") for p in rel_dir.split(os.sep)) if rel_dir else False
        )

        # Create folder record (except for root)
        if rel_dir:
            folder_name = os.path.basename(dirpath)
            dir_hidden = parent_hidden or folder_name.startswith(".")
            folder_id = db.get_or_create_folder(rel_dir, folder_name, dir_hidden)
        else:
            folder_id = None

        # Scan directory
        try:
            entries = os.listdir(dirpath)
        except (PermissionError, OSError):
            continue

        for name in entries:
            full_path = os.path.join(dirpath, name)

            try:
                is_link = os.path.islink(full_path)
            except OSError:
                continue

            if is_link:
                continue

            try:
                is_dir = os.path.isdir(full_path)
            except OSError:
                continue

            if is_dir:
                dirs_to_visit.append(full_path)
                continue

            # File — stat it
            try:
                st = os.stat(full_path)
            except OSError:
                files_skipped += 1
                continue

            if not stat.S_ISREG(st.st_mode):
                continue

            hidden = 1 if (parent_hidden or name.startswith(".")) else 0
            rel_path = os.path.join(rel_dir, name) if rel_dir else name
            type_high, type_low = classify_file(name)

            # Hash — partial only (full hashes computed later via backfill)
            hash_partial = None
            if not no_hash and st.st_size > 0:
                try:
                    hash_partial = hash_file_partial_sync(full_path)
                except OSError:
                    files_skipped += 1
                    continue

            created = datetime.fromtimestamp(
                st.st_birthtime if hasattr(st, "st_birthtime") else st.st_ctime,
                tz=timezone.utc,
            ).isoformat(timespec="seconds")
            modified = datetime.fromtimestamp(
                st.st_mtime,
                tz=timezone.utc,
            ).isoformat(timespec="seconds")

            batch.append(
                (
                    folder_id,
                    name,
                    rel_path,
                    st.st_size,
                    type_high,
                    type_low,
                    hash_partial,
                    created,
                    modified,
                    hidden,
                )
            )
            files_cataloged += 1
            total_bytes += st.st_size

            # Flush batch
            if len(batch) >= BATCH_SIZE:
                db.insert_files_batch(batch)
                batch.clear()

            # Progress reporting
            now = time.monotonic()
            if now - last_report >= 2:
                elapsed = now - t0
                rate = files_cataloged / elapsed if elapsed > 0 else 0
                print(
                    f"\r  {files_cataloged:,} files | "
                    f"{format_size(total_bytes)} | "
                    f"{rate:.0f} files/sec | "
                    f"{format_elapsed(elapsed)}\033[K",
                    end="",
                    flush=True,
                )
                last_report = now

                # Save traversal state periodically
                if now - last_save >= TRAVERSAL_SAVE_INTERVAL:
                    db.save_traversal_state(list(dirs_to_visit))
                    last_save = now

    # Flush remaining
    if batch:
        db.insert_files_batch(batch)

    # Clear progress line
    print()

    return files_cataloged, files_skipped, total_bytes

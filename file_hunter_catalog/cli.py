"""CLI interface for file-hunter-catalog."""

import argparse
import os
import sys
import time

from file_hunter_catalog.classify import format_elapsed, format_size


def main():
    """Catalog a directory tree into a SQLite database."""
    parser = argparse.ArgumentParser(
        prog="file-hunter-catalog",
        description="Catalog a directory tree into a portable SQLite database",
    )
    parser.add_argument("path", help="Directory to catalog")
    parser.add_argument(
        "-o", "--output", help="Output SQLite file (default: catalog-<timestamp>.db)"
    )
    parser.add_argument(
        "--no-hash", action="store_true", help="Skip hash computation (metadata only)"
    )
    parser.add_argument(
        "--resume", action="store_true", help="Resume an interrupted catalog"
    )
    args = parser.parse_args()

    root_path = os.path.abspath(os.path.expanduser(args.path))
    if not os.path.isdir(root_path):
        print(f"Error: {root_path} is not a directory", file=sys.stderr)
        sys.exit(1)

    output = args.output
    if not output:
        ts = time.strftime("%Y%m%d-%H%M%S")
        output = f"catalog-{ts}.db"

    resume = args.resume and os.path.exists(output)
    updating = not resume and os.path.exists(output)

    print(f"Cataloging: {root_path}")
    print(f"Output:     {output}")
    if args.no_hash:
        print("Hashing:    disabled")
    if resume:
        print("Mode:       resume")
    elif updating:
        print("Mode:       update")
    print()

    from file_hunter_catalog.catalog_db import CatalogDB
    from file_hunter_catalog.walker import walk_and_catalog, hash_catalog_files

    db = CatalogDB(output)
    db.open(resume=resume or updating)

    # Validate root_path on existing DB
    saved_root = db.get_meta("root_path")
    if saved_root and saved_root != root_path:
        print(
            f"Error: catalog root_path is {saved_root}, but you specified {root_path}",
            file=sys.stderr,
        )
        db.close()
        sys.exit(1)

    db.write_initial_meta(root_path)

    t0 = time.monotonic()

    print("Pass 1: metadata")
    files, skipped, total_bytes = walk_and_catalog(
        root_path,
        db,
        resume=resume,
    )

    hash_count = 0
    hash_skipped = 0
    if not args.no_hash:
        print("Pass 2: hashing (inode-sorted)")
        hash_count, hash_skipped = hash_catalog_files(root_path, db)

    elapsed = time.monotonic() - t0

    db.mark_complete()
    folders = db.folder_count()
    db.close()

    db_size = os.path.getsize(output)

    print(f"Complete in {format_elapsed(elapsed)}")
    print(f"  Files:   {files:,}")
    print(f"  Folders: {folders:,}")
    print(f"  Size:    {format_size(total_bytes)}")
    if skipped:
        print(f"  Skipped: {skipped:,} (walk)")
    if hash_skipped:
        print(f"  Skipped: {hash_skipped:,} (hash)")
    print(f"  Catalog: {output} ({format_size(db_size)})")

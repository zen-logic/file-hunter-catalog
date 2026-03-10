"""CLI interface for file-hunter-catalog."""

import argparse
import os
import sys
import time

from file_hunter_catalog.classify import format_size


def cmd_catalog(argv):
    """Catalog a directory tree into a SQLite database."""
    parser = argparse.ArgumentParser(
        prog="file-hunter-catalog",
        description="Catalog a directory tree into a portable SQLite database",
    )
    parser.add_argument("path", help="Directory to catalog")
    parser.add_argument("-o", "--output", help="Output SQLite file (default: catalog-<timestamp>.db)")
    parser.add_argument("--no-hash", action="store_true", help="Skip hash computation (metadata only)")
    parser.add_argument("--resume", action="store_true", help="Resume an interrupted catalog")
    args = parser.parse_args(argv)

    root_path = os.path.abspath(os.path.expanduser(args.path))
    if not os.path.isdir(root_path):
        print(f"Error: {root_path} is not a directory", file=sys.stderr)
        sys.exit(1)

    output = args.output
    if not output:
        ts = time.strftime("%Y%m%d-%H%M%S")
        output = f"catalog-{ts}.db"

    resume = args.resume and os.path.exists(output)

    print(f"Cataloging: {root_path}")
    print(f"Output:     {output}")
    if args.no_hash:
        print("Hashing:    disabled")
    if resume:
        print("Mode:       resume")
    print()

    from file_hunter_catalog.catalog_db import CatalogDB
    from file_hunter_catalog.walker import walk_and_catalog

    db = CatalogDB(output)
    db.open(resume=resume)

    if not resume:
        db.write_initial_meta(root_path)
    else:
        saved_root = db.get_meta("root_path")
        if saved_root and saved_root != root_path:
            print(
                f"Error: catalog root_path is {saved_root}, "
                f"but you specified {root_path}",
                file=sys.stderr,
            )
            db.close()
            sys.exit(1)

    t0 = time.monotonic()
    files, skipped, total_bytes = walk_and_catalog(
        root_path, db, no_hash=args.no_hash, resume=resume,
    )
    elapsed = time.monotonic() - t0

    db.mark_complete()
    folders = db.folder_count()
    db.close()

    db_size = os.path.getsize(output)

    print(f"Complete in {elapsed:.1f}s")
    print(f"  Files:   {files:,}")
    print(f"  Folders: {folders:,}")
    print(f"  Size:    {format_size(total_bytes)}")
    if skipped:
        print(f"  Skipped: {skipped:,}")
    print(f"  Catalog: {output} ({format_size(db_size)})")


def cmd_import(argv):
    """Import a catalog into a File Hunter server database."""
    parser = argparse.ArgumentParser(
        prog="file-hunter-catalog import",
        description="Import a catalog into a File Hunter server database",
    )
    parser.add_argument("catalog", help="Catalog SQLite file to import")
    parser.add_argument("--db", required=True, help="Path to File Hunter server database")
    parser.add_argument("--agent", required=True, help="Name of the agent that manages the files")
    parser.add_argument("--root-path", help="Override the root path (default: from catalog)")
    parser.add_argument("--location-name", help="Name for a new location (default: directory name)")
    args = parser.parse_args(argv)

    if not os.path.exists(args.catalog):
        print(f"Error: {args.catalog} not found", file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(args.db):
        print(f"Error: {args.db} not found", file=sys.stderr)
        sys.exit(1)

    import sqlite3

    cat = sqlite3.connect(args.catalog)
    cat.row_factory = sqlite3.Row
    row = cat.execute(
        "SELECT value FROM catalog_meta WHERE key = 'root_path'"
    ).fetchone()
    cat.close()

    if not row:
        print("Error: catalog has no root_path in metadata", file=sys.stderr)
        sys.exit(1)

    root_path = args.root_path or row["value"]

    srv = sqlite3.connect(args.db)
    srv.row_factory = sqlite3.Row
    agent_row = srv.execute(
        "SELECT id, name FROM agents WHERE name = ?", (args.agent,)
    ).fetchone()

    if not agent_row:
        agents = srv.execute("SELECT id, name FROM agents ORDER BY id").fetchall()
        srv.close()
        print(f"Error: no agent named '{args.agent}'", file=sys.stderr)
        if agents:
            print("Available agents:", file=sys.stderr)
            for a in agents:
                print(f"  {a['name']}", file=sys.stderr)
        sys.exit(1)

    agent_id = agent_row["id"]
    srv.close()

    if args.root_path:
        print(f"Root path override: {root_path}")

    from file_hunter_catalog.importer import import_catalog

    import_catalog(
        catalog_path=args.catalog,
        server_db_path=args.db,
        agent_id=agent_id,
        root_path=root_path,
        location_name=args.location_name,
    )


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "import":
        cmd_import(sys.argv[2:])
    elif len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
        print("usage: file-hunter-catalog <path> [-o OUTPUT] [--no-hash] [--resume]")
        print("       file-hunter-catalog import <catalog.db> --db <server.db> --agent <name>")
        print()
        print("Offline file cataloger and import tool for File Hunter.")
        print()
        print("Run 'file-hunter-catalog <path> --help' or 'file-hunter-catalog import --help' for details.")
    else:
        cmd_catalog(sys.argv[1:])

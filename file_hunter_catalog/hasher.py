"""File hashing — partial and full.

Matches file_hunter_core.hasher exactly.
If file_hunter_core is installed, uses it directly.
"""

import os

CHUNK_SIZE = 1024 * 1024  # 1 MB
PARTIAL_SIZE = 4 * 1024 * 1024  # 4 MB

_impl_partial = None
_impl_full = None


def hash_file_partial_sync(path: str) -> str:
    """xxHash64 of first 4MB + last 4MB. For files <= 8MB, reads everything."""
    global _impl_partial
    if _impl_partial is None:
        _impl_partial = _load_partial_impl()
    return _impl_partial(path)


def hash_file_sync(path: str) -> tuple[str, str]:
    """Read file once, return (xxhash64_hex, sha256_hex)."""
    global _impl_full
    if _impl_full is None:
        _impl_full = _load_full_impl()
    return _impl_full(path)


def _load_partial_impl():
    try:
        from file_hunter_core.hasher import hash_file_partial_sync as fn
        return fn
    except ImportError:
        pass

    try:
        import xxhash
    except ImportError:
        print(
            "Error: xxhash is required. Install it with: pip install xxhash",
            file=__import__("sys").stderr,
        )
        raise SystemExit(1)

    def _hash(path: str) -> str:
        xx = xxhash.xxh64()
        file_size = os.path.getsize(path)
        with open(path, "rb") as f:
            if file_size <= PARTIAL_SIZE * 2:
                while chunk := f.read(CHUNK_SIZE):
                    xx.update(chunk)
            else:
                read = 0
                while read < PARTIAL_SIZE:
                    chunk = f.read(min(CHUNK_SIZE, PARTIAL_SIZE - read))
                    if not chunk:
                        break
                    xx.update(chunk)
                    read += len(chunk)
                f.seek(-PARTIAL_SIZE, 2)
                while chunk := f.read(CHUNK_SIZE):
                    xx.update(chunk)
        return xx.hexdigest()

    return _hash


def _load_full_impl():
    try:
        from file_hunter_core.hasher import hash_file_sync as fn
        return fn
    except ImportError:
        pass

    import hashlib

    try:
        import xxhash
    except ImportError:
        print(
            "Error: xxhash is required. Install it with: pip install xxhash",
            file=__import__("sys").stderr,
        )
        raise SystemExit(1)

    def _hash(path: str) -> tuple[str, str]:
        xx = xxhash.xxh64()
        sha = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                xx.update(chunk)
                sha.update(chunk)
        return xx.hexdigest(), sha.hexdigest()

    return _hash

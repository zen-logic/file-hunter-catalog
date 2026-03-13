"""File hashing — partial only.

Matches file_hunter_core.hasher exactly.
If file_hunter_core is installed, uses it directly.
"""

import os

CHUNK_SIZE = 1024 * 1024  # 1 MB
PARTIAL_SIZE = 64 * 1024  # 64 KB

_impl_partial = None


def hash_file_partial_sync(path: str) -> str:
    """xxHash64 of first 64KB + last 64KB. For files <= 128KB, reads everything."""
    global _impl_partial
    if _impl_partial is None:
        _impl_partial = _load_partial_impl()
    return _impl_partial(path)


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

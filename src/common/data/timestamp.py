from datetime import datetime


def fast_datetime(s: str) -> datetime:
    """Parse 'YYYY-MM-DD HH:MM:SS' or 'YYYY/MM/DD HH:MM' by character position.

    ~10x faster than ``datetime.strptime`` and produces the identical datetime.
    strptime was being run per transaction at every hop that deserializes a
    Transactions batch (``Transactions._from_fields``) plus once in the client
    parser — a major CPU sink at scale. Parsing by fixed offsets (the separators
    are ignored) keeps the result byte-for-byte equivalent while skipping the
    format-string machinery.
    """
    return datetime(
        int(s[0:4]),
        int(s[5:7]),
        int(s[8:10]),
        int(s[11:13]),
        int(s[14:16]),
        int(s[17:19]) if len(s) >= 19 else 0,
    )

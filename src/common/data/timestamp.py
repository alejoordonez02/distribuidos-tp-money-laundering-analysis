from datetime import datetime


def fast_datetime(s: str) -> datetime:
    """Parse 'YYYY-MM-DD HH:MM:SS' or 'YYYY/MM/DD HH:MM' by character position.

    Faster than ``datetime.strptime`` (skips the format-string machinery) and
    produces an identical datetime.
    """
    return datetime(
        int(s[0:4]),
        int(s[5:7]),
        int(s[8:10]),
        int(s[11:13]),
        int(s[14:16]),
        int(s[17:19]) if len(s) >= 19 else 0,
    )

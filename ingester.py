# ingester.py
# T1 - pulls 4+ RSS feeds and writes one JSONL file per tick into data/incoming/
import os
import ssl
import json
import time
import socket
from datetime import datetime, timezone

# Cap every network call so a slow feed can't hang the whole tick.
socket.setdefaulttimeout(8)

# macOS + python.org Python ship without a usable cert bundle; force certifi.
try:
    import certifi
    ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
except ImportError:
    pass

import feedparser

INCOMING = "data/incoming"
os.makedirs(INCOMING, exist_ok=True)

FEEDS = {
    "bbc":      "https://feeds.bbci.co.uk/news/world/rss.xml",
    "reuters":  "https://feeds.reuters.com/reuters/topNews",
    "nyt":      "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
    "guardian": "https://www.theguardian.com/world/rss",
    "aljazeera":"https://www.aljazeera.com/xml/rss/all.xml",
    "npr":      "https://feeds.npr.org/1001/rss.xml",
}


def _iso(entry) -> str:
    """Return a parseable ISO-8601 timestamp for the entry, falling back to now."""
    for field in ("published_parsed", "updated_parsed"):
        t = entry.get(field)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
            except Exception:
                pass
    return datetime.now(timezone.utc).isoformat()


def pull_once(tick: int) -> int:
    rows = []
    for source, url in FEEDS.items():
        try:
            parsed = feedparser.parse(url)
            # tolerate dead feeds: parsed.bozo will be truthy on parse errors
            if parsed.bozo and not parsed.entries:
                print(f"[tick {tick}] feed {source} dead/empty: {parsed.bozo_exception}")
                continue
            for e in parsed.entries:
                title = (e.get("title") or "").strip()
                link  = (e.get("link")  or "").strip()
                if not title:
                    continue
                rows.append({
                    "source": source,
                    "title":  title,
                    "url":    link,
                    "ts":     _iso(e),
                })
        except Exception as ex:
            # one bad feed must not kill the tick
            print(f"[tick {tick}] feed {source} raised {ex!r}; skipping")
            continue

    if not rows:
        print(f"[tick {tick}] no rows pulled; skipping write")
        return 0

    path = os.path.join(INCOMING, f"batch_{tick}.json")
    # write to a temp file then rename so Spark's file-source never sees half-written files
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")  # JSONL: one record per line
    os.replace(tmp, path)
    print(f"[tick {tick}] wrote {len(rows)} rows -> {path}")
    return len(rows)


if __name__ == "__main__":
    tick = 0
    while True:
        try:
            pull_once(tick)
        except Exception as ex:
            print(f"[tick {tick}] top-level error: {ex!r}")
        tick += 1
        time.sleep(45)  # 30-60s window per spec

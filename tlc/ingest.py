"""Download NYC TLC green-taxi monthly parquet files with resume + retries and a SHA-256 manifest."""
import hashlib, json, pathlib, time
import requests

BASE = "https://d37ci6vzurychx.cloudfront.net"
RAW = pathlib.Path("data/raw")

def url_for(month: str) -> str:
    return f"{BASE}/trip-data/green_tripdata_{month}.parquet"

def sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def download(url: str, dest: pathlib.Path, retries: int = 20, session=requests) -> pathlib.Path:
    """Resumable download: re-requests with a Range header after dropped connections."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    head = session.head(url, timeout=60, headers={"Accept-Encoding": "identity"})
    if "content-length" not in head.headers:  # small files served chunked: plain GET
        if not dest.exists():
            r = session.get(url, timeout=120); r.raise_for_status(); dest.write_bytes(r.content)
        return dest
    total = int(head.headers["content-length"])
    for attempt in range(retries):
        have = dest.stat().st_size if dest.exists() else 0
        if have == total:
            return dest
        try:
            with session.get(url, headers={"Range": f"bytes={have}-"}, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(dest, "ab" if have else "wb") as f:
                    for chunk in r.iter_content(1 << 16):
                        f.write(chunk)
        except requests.RequestException:
            time.sleep(min(2 ** attempt, 30))
    if dest.stat().st_size != total:
        raise IOError(f"incomplete download {url}: {dest.stat().st_size}/{total}")
    return dest

def fetch_month(month: str) -> dict:
    dest = RAW / f"green_tripdata_{month}.parquet"
    download(url_for(month), dest)
    entry = {"month": month, "file": str(dest), "bytes": dest.stat().st_size, "sha256": sha256(dest), "url": url_for(month)}
    man = RAW / "manifest.json"
    m = json.loads(man.read_text()) if man.exists() else {}
    m[month] = entry
    man.write_text(json.dumps(m, indent=2, sort_keys=True))
    return entry

def fetch_zones() -> pathlib.Path:
    return download(f"{BASE}/misc/taxi_zone_lookup.csv", RAW / "taxi_zone_lookup.csv")

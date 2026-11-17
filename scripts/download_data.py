"""Download ONE snapshot of the unofficial BMTC GTFS feed (Vonter/bmtc-gtfs).

Saves the zip, unzips it, and writes data/raw/snapshot_meta.json with the
download date, file sizes and SHA-256 checksums. Also saves the repo's LICENSE
and README so DATA_CARD.md can quote what is really stated.
If the network is blocked, put bmtc.zip in data/raw/ yourself and re-run.
"""
import hashlib, json, sys, urllib.request, zipfile, datetime
from pathlib import Path

BASE = "https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/"
RAW = Path(__file__).resolve().parent.parent / "data" / "raw"


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(rel: str, dest: Path):
    print(f"downloading {rel} ...")
    urllib.request.urlretrieve(BASE + rel, dest)


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    zip_path = RAW / "bmtc.zip"
    if not zip_path.exists():
        try:
            fetch("gtfs/bmtc.zip", zip_path)
        except Exception as e:  # network blocked etc.
            sys.exit(f"Download failed ({e}). Place bmtc.zip in {RAW} and re-run.")
    for name in ("LICENSE", "README.md"):
        if not (RAW / f"repo_{name}").exists():
            try:
                fetch(name, RAW / f"repo_{name}")
            except Exception as e:
                print(f"warning: could not fetch {name}: {e}")
    gtfs_dir = RAW / "gtfs"
    if not gtfs_dir.exists():
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(gtfs_dir)
    meta = {
        "downloaded_on": datetime.date.today().isoformat(),
        "source": BASE + "gtfs/bmtc.zip",
        "zip_bytes": zip_path.stat().st_size,
        "zip_sha256": sha256(zip_path),
        "files": {p.name: {"bytes": p.stat().st_size, "sha256": sha256(p)}
                  for p in sorted(gtfs_dir.rglob("*")) if p.is_file()},
    }
    (RAW / "snapshot_meta.json").write_text(json.dumps(meta, indent=2))
    print(json.dumps({k: v for k, v in meta.items() if k != "files"}, indent=2))
    for n, v in meta["files"].items():
        print(f"  {n}: {v['bytes']:,} bytes")


if __name__ == "__main__":
    main()

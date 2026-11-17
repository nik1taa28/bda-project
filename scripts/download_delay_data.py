"""Download the public MBTA (Boston) 'Bus Arrival Departure Times 2024' dataset (real scheduled vs actual times).

Source: MBTA Open Data Portal, ArcGIS item 96c77138c3144906bce93d0257531b6a (licence shown on the item: CC0).
Writes data/raw_delay/delay_snapshot_meta.json (date, sizes, SHA-256, licence, item description).
If the download is blocked, download the zip manually from the item page and place it as
data/raw_delay/mbta_bus_2024.zip, then re-run (nothing is ever fabricated).
"""
import datetime, hashlib, json, re, sys, urllib.request, zipfile
from pathlib import Path

ITEM = "96c77138c3144906bce93d0257531b6a"
RAW = Path(__file__).resolve().parent.parent / "data" / "raw_delay"
ZIP = RAW / "mbta_bus_2024.zip"


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    if not ZIP.exists():
        try:
            print("downloading ~315 MB ...")
            urllib.request.urlretrieve(f"https://www.arcgis.com/sharing/rest/content/items/{ITEM}/data", ZIP)
        except Exception as e:
            sys.exit(f"Download failed ({e}). Place the zip at {ZIP} and re-run.")
    out = RAW / "extracted"
    if not out.exists():
        with zipfile.ZipFile(ZIP) as z:
            z.extractall(out)
    info = json.load(urllib.request.urlopen(f"https://www.arcgis.com/sharing/rest/content/items/{ITEM}?f=json", timeout=30))
    meta = {"downloaded_on": datetime.date.today().isoformat(), "item_id": ITEM, "title": info.get("title"),
            "licence_on_item": info.get("licenseInfo"), "owner_org": info.get("accessInformation"),
            "description_text": re.sub(r"<[^>]+>", " ", info.get("description") or "")[:3000],
            "zip_bytes": ZIP.stat().st_size, "zip_sha256": sha(ZIP),
            "files": {p.name: {"bytes": p.stat().st_size, "sha256": sha(p)} for p in sorted(out.rglob("*.csv"))}}
    (RAW / "delay_snapshot_meta.json").write_text(json.dumps(meta, indent=2))
    print(json.dumps({k: v for k, v in meta.items() if k not in ("files", "description_text")}, indent=2))


if __name__ == "__main__":
    main()

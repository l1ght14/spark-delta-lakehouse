"""Fetch MovieLens ml-latest-small and unpack it into data/raw.

Deliberately stdlib-only: no unzip binary, no requests, no pandas. A pipeline's
ingest step should not need anything the runtime does not already have.

Idempotent - skips the download if the CSVs are already present.
"""

import pathlib
import shutil
import sys
import urllib.request
import zipfile

URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
WANTED = ("ratings.csv", "movies.csv")

PROJ = pathlib.Path(__file__).resolve().parent.parent
RAW = PROJ / "data" / "raw"


def main() -> int:
    RAW.mkdir(parents=True, exist_ok=True)

    if all((RAW / name).exists() for name in WANTED):
        print(f"  skip  {', '.join(WANTED)} already present in {RAW}")
        return 0

    archive = RAW / "ml-latest-small.zip"
    print(f"  get   {URL}")
    try:
        with urllib.request.urlopen(URL, timeout=300) as response:
            with open(archive, "wb") as handle:
                shutil.copyfileobj(response, handle)
    except Exception as error:
        archive.unlink(missing_ok=True)
        print(f"  FAIL  download failed: {error}", file=sys.stderr)
        return 1

    size_mb = archive.stat().st_size / 1e6
    print(f"  got   {size_mb:.1f} MB")

    # The archive nests everything under ml-latest-small/. Take only what we
    # use and write it flat, so the ingest path stays simple.
    try:
        with zipfile.ZipFile(archive) as zf:
            for name in WANTED:
                member = next(
                    m for m in zf.namelist() if m.endswith(f"/{name}")
                )
                with zf.open(member) as src, open(RAW / name, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                print(f"  unpack {name}")
    except Exception as error:
        print(f"  FAIL  unpack failed: {error}", file=sys.stderr)
        return 1
    finally:
        archive.unlink(missing_ok=True)

    print("\n=== data/raw ===")
    for path in sorted(RAW.iterdir()):
        rows = ""
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                rows = f"  ({sum(1 for _ in handle):,} lines)"
        except Exception:
            pass
        print(f"  {path.name:<16} {path.stat().st_size / 1e6:6.2f} MB{rows}")

    print("\n=== headers ===")
    for name in WANTED:
        with open(RAW / name, encoding="utf-8", errors="replace") as handle:
            header = handle.readline().strip()
            first = handle.readline().strip()
        print(f"  {name}\n    {header}\n    {first}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
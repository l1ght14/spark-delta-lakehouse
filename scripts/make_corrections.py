"""Generate late-arriving rating corrections.

Synthetic on purpose. Demonstrating an upsert needs data that supersedes an
earlier record for the same key, and real datasets will not reliably hand you
that - you would have to wait for it, or discover it by accident. Generating it
deterministically means the MERGE demonstration is reproducible, and it means
the exact expected outcome can be stated in advance and then checked.

The corrections:
  * re-rate some existing (user, movie) pairs to a different value
  * always with a LATER timestamp than the original, so "latest wins" is
    unambiguous
  * include a handful of rows for movies that do not exist, so the
    referential-integrity gate has something real to catch
  * include one out-of-range rating, so the range check is exercised too
"""

import csv
import pathlib
import random
import sys

RAW = pathlib.Path(__file__).resolve().parent.parent / "data" / "raw"
CORRECTIONS = RAW / "rating_corrections.csv"

# Seeded so the file is byte-identical on every run - a pipeline whose test data
# changes between runs cannot be debugged.
SEED = 20240115
N_CORRECTIONS = 40
N_UNKNOWN_MOVIE = 3
OUT_OF_RANGE = 1

# Far enough ahead of the MovieLens data (which ends 2018) that every
# correction is unambiguously newer than the row it supersedes.
CORRECTION_EPOCH = 1_700_000_000


def main() -> int:
    rng = random.Random(SEED)
    ratings_path = RAW / "ratings.csv"

    if not ratings_path.exists():
        print(f"  FAIL  {ratings_path} missing - run scripts/fetch_data.py first")
        return 1

    pairs = []
    with open(ratings_path, encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            pairs.append((row["userId"], row["movieId"], float(row["rating"])))

    rng.shuffle(pairs)

    rows = []
    for user_id, movie_id, original in pairs[:N_CORRECTIONS]:
        # Deliberately different from the original, and still inside the valid
        # range, so the correction is visible in the output rather than
        # indistinguishable from a no-op.
        new_rating = 5.0 if original < 4.0 else 1.0
        rows.append([user_id, movie_id, str(new_rating), str(CORRECTION_EPOCH + len(rows))])

    # Ratings for movies that are not in movies.csv. The gate must catch these
    # rather than the fact table silently dropping them.
    for i in range(N_UNKNOWN_MOVIE):
        rows.append(
            [str(pairs[i][0]), "99999999", "4.0", str(CORRECTION_EPOCH + 900 + i)]
        )

    # One impossible rating, to exercise the range check.
    rows.append([str(pairs[0][0]), pairs[0][1], "9.9", str(CORRECTION_EPOCH + 950)])

    CORRECTIONS.parent.mkdir(parents=True, exist_ok=True)
    with open(CORRECTIONS, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["userId", "movieId", "rating", "timestamp"])
        writer.writerows(rows)

    print(f"  wrote {CORRECTIONS.name}: {len(rows)} correction rows")
    print(f"    {N_CORRECTIONS} re-ratings of existing pairs (latest-wins)")
    print(f"    {N_UNKNOWN_MOVIE} ratings for a movie_id that does not exist")
    print(f"    {OUT_OF_RANGE} rating outside the valid 0.5-5.0 range")
    print(f"    deterministic: seed={SEED}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
"""Generate a Delta Lake table with multiple versions, partitioning, CDF, and diverse types."""

import argparse
import random
import shutil
import string
from datetime import datetime, timezone, timedelta
from pathlib import Path

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

# Output path
TABLE_PATH = "test_data/partitioned_cdf_table"
LARGE_TABLE_PATH = "test_data/large_table"

# Schema with all required types
schema = pa.schema([
    ("id", pa.int64()),
    ("category", pa.string()),  # partition column
    ("value_int", pa.int32()),
    ("value_float", pa.float32()),
    ("value_double", pa.float64()),
    ("name", pa.string()),
    ("tags", pa.list_(pa.string())),
    ("address", pa.struct([
        ("city", pa.string()),
        ("zip", pa.string()),
    ])),
    ("created_at", pa.timestamp("us", tz="UTC")),
    ("updated_at", pa.timestamp("us")),
])

base_ts = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
base_naive = datetime(2025, 1, 15, 10, 0, 0)


def make_row(id, category, value_int, value_float, value_double, name, tags, city, zip_code, hours_offset=0):
    return {
        "id": id,
        "category": category,
        "value_int": value_int,
        "value_float": value_float,
        "value_double": value_double,
        "name": name,
        "tags": tags,
        "city": city,
        "zip": zip_code,
        "created_at": base_ts + timedelta(hours=hours_offset),
        "updated_at": base_naive + timedelta(hours=hours_offset),
    }


def rows_to_table(rows):
    return pa.table({
        "id": [r["id"] for r in rows],
        "category": [r["category"] for r in rows],
        "value_int": pa.array([r["value_int"] for r in rows], type=pa.int32()),
        "value_float": pa.array([r["value_float"] for r in rows], type=pa.float32()),
        "value_double": [r["value_double"] for r in rows],
        "name": [r["name"] for r in rows],
        "tags": [r["tags"] for r in rows],
        "address": pa.array(
            [{"city": r["city"], "zip": r["zip"]} for r in rows],
            type=pa.struct([("city", pa.string()), ("zip", pa.string())]),
        ),
        "created_at": pa.array([r["created_at"] for r in rows], type=pa.timestamp("us", tz="UTC")),
        "updated_at": pa.array([r["updated_at"] for r in rows], type=pa.timestamp("us")),
    }, schema=schema)


def main():
    # Clean up
    shutil.rmtree(TABLE_PATH, ignore_errors=True)
    Path(TABLE_PATH).parent.mkdir(parents=True, exist_ok=True)

    # ── Version 0: Pure INSERT (10 rows) ──
    initial_rows = [
        make_row(1,  "A", 10, 1.1, 100.001, "Alice",   ["python", "rust"],  "NYC",    "10001", 0),
        make_row(2,  "B", 20, 2.2, 200.002, "Bob",     ["java"],            "LA",     "90001", 1),
        make_row(3,  "A", 30, 3.3, 300.003, "Charlie", ["go", "c++"],       "Chicago","60601", 2),
        make_row(4,  "C", 40, 4.4, 400.004, "Diana",   ["rust"],            "Miami",  "33101", 3),
        make_row(5,  "B", 50, 5.5, 500.005, "Eve",     ["python", "js"],    "Seattle","98101", 4),
        make_row(6,  "A", 60, 6.6, 600.006, "Frank",   ["scala"],           "Denver", "80201", 5),
        make_row(7,  "C", 70, 7.7, 700.007, "Grace",   ["kotlin", "java"],  "Boston", "02101", 6),
        make_row(8,  "B", 80, 8.8, 800.008, "Hank",    ["c#"],              "Austin", "73301", 7),
        make_row(9,  "A", 90, 9.9, 900.009, "Ivy",     ["ruby"],            "Portland","97201", 8),
        make_row(10, "C", 100,10.0,1000.01, "Jack",    ["elixir", "erlang"],"Dallas", "75201", 9),
    ]
    table = rows_to_table(initial_rows)
    write_deltalake(
        TABLE_PATH, table, mode="overwrite", partition_by=["category"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    print("v0: Inserted 10 rows")

    # ── Version 1: Pure UPDATE (update 5 rows) ──
    dt = DeltaTable(TABLE_PATH)
    dt.merge(
        source=rows_to_table([
            make_row(1,  "A", 11, 1.11, 100.111, "Alice Updated",   ["python", "rust", "sql"], "NYC",    "10001", 10),
            make_row(3,  "A", 31, 3.33, 300.333, "Charlie Updated", ["go", "c++", "wasm"],     "Chicago","60601", 11),
            make_row(5,  "B", 51, 5.55, 500.555, "Eve Updated",     ["python", "js", "ts"],    "Seattle","98101", 12),
            make_row(7,  "C", 71, 7.77, 700.777, "Grace Updated",   ["kotlin"],                "Boston", "02101", 13),
            make_row(9,  "A", 91, 9.99, 900.999, "Ivy Updated",     ["ruby", "crystal"],       "Portland","97201",14),
        ]),
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_update_all().execute()
    print("v1: Updated 5 rows")

    # ── Version 2: Pure DELETE (delete 3 rows: ids 2, 4, 6) ──
    dt = DeltaTable(TABLE_PATH)
    dt.merge(
        source=pa.table({"id": [2, 4, 6]}, schema=pa.schema([("id", pa.int64())])),
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_delete().execute()
    print("v2: Deleted 3 rows (ids 2,4,6)")

    # ── Version 3: INSERT + DELETE ──
    # Insert 3 new rows, delete 2 existing
    dt = DeltaTable(TABLE_PATH)
    new_rows = rows_to_table([
        make_row(11, "B", 110, 11.1, 1100.011, "Karen",  ["haskell"],       "Phoenix", "85001", 15),
        make_row(12, "C", 120, 12.2, 1200.012, "Leo",    ["lua", "zig"],    "Atlanta", "30301", 16),
        make_row(13, "A", 130, 13.3, 1300.013, "Mona",   ["swift"],         "SanFran", "94101", 17),
    ])
    dt.merge(
        source=new_rows,
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t",
    ).when_not_matched_insert_all().execute()

    dt = DeltaTable(TABLE_PATH)
    dt.merge(
        source=pa.table({"id": [8, 10]}, schema=pa.schema([("id", pa.int64())])),
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_delete().execute()
    print("v3-4: Inserted 3 (ids 11,12,13), Deleted 2 (ids 8,10)")

    # ── Version 5: INSERT + UPDATE ──
    dt = DeltaTable(TABLE_PATH)
    merge_data = rows_to_table([
        # Updates (existing ids)
        make_row(1,  "A", 15, 1.15, 100.150, "Alice Revised",   ["python", "rust", "sql", "dbt"], "NYC",     "10002", 20),
        make_row(5,  "B", 55, 5.56, 500.560, "Eve Revised",     ["python", "ts", "react"],        "Seattle", "98102", 21),
        # Inserts (new ids)
        make_row(14, "B", 140, 14.4, 1400.014, "Nina",  ["perl"],          "Houston", "77001", 22),
        make_row(15, "A", 150, 15.5, 1500.015, "Oscar", ["r", "julia"],    "SanDiego","92101", 23),
    ])
    dt.merge(
        source=merge_data,
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_update_all().when_not_matched_insert_all().execute()
    print("v5: Updated 2 (ids 1,5), Inserted 2 (ids 14,15)")

    # ── Version 6: UPDATE + DELETE ──
    dt = DeltaTable(TABLE_PATH)
    # Update ids 3, 11; Delete ids 12, 13
    update_rows = rows_to_table([
        make_row(3,  "A", 35, 3.35, 300.350, "Charlie Final", ["go", "wasm", "rust"], "Chicago",  "60602", 25),
        make_row(11, "B", 115,11.15,1100.115,"Karen Final",   ["haskell", "purescript"],"Phoenix","85002", 26),
    ])
    dt.merge(
        source=update_rows,
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_update_all().execute()

    dt = DeltaTable(TABLE_PATH)
    dt.merge(
        source=pa.table({"id": [12, 13]}, schema=pa.schema([("id", pa.int64())])),
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_delete().execute()
    print("v6-7: Updated 2 (ids 3,11), Deleted 2 (ids 12,13)")

    # ── Version 8: INSERT + UPDATE + DELETE ──
    dt = DeltaTable(TABLE_PATH)
    # First: insert + update via merge
    merge_data = rows_to_table([
        # Update existing
        make_row(7,  "C", 75, 7.75, 700.750, "Grace Final",  ["kotlin", "compose"],    "Boston",  "02102", 30),
        make_row(9,  "A", 95, 9.95, 900.950, "Ivy Final",    ["ruby", "crystal", "go"],"Portland","97202", 31),
        # Insert new
        make_row(16, "C", 160,16.6, 1600.016,"Pat",          ["dart", "flutter"],       "Raleigh", "27601", 32),
        make_row(17, "A", 170,17.7, 1700.017,"Quinn",        ["zig", "nim"],            "Nashville","37201",33),
    ])
    dt.merge(
        source=merge_data,
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_update_all().when_not_matched_insert_all().execute()

    dt = DeltaTable(TABLE_PATH)
    dt.merge(
        source=pa.table({"id": [14]}, schema=pa.schema([("id", pa.int64())])),
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t",
    ).when_matched_delete().execute()
    print("v8-9: Updated 2 (ids 7,9), Inserted 2 (ids 16,17), Deleted 1 (id 14)")

    # Print final state
    dt = DeltaTable(TABLE_PATH)
    print(f"\nFinal table version: {dt.version()}")
    print(f"History entries: {len(dt.history())}")
    print(f"Row count: {dt.to_pyarrow_table().num_rows}")
    print(f"Partitions: category = A, B, C")
    print(f"\nFinal rows:")
    print(dt.to_pyarrow_table().sort_by("id"))


CATEGORIES = list(string.ascii_uppercase[:10])  # A-J
CITIES = [
    ("NYC", "10001"), ("LA", "90001"), ("Chicago", "60601"), ("Miami", "33101"),
    ("Seattle", "98101"), ("Denver", "80201"), ("Boston", "02101"), ("Austin", "73301"),
    ("Portland", "97201"), ("Dallas", "75201"), ("Phoenix", "85001"), ("Atlanta", "30301"),
    ("Houston", "77001"), ("SanDiego", "92101"), ("Nashville", "37201"), ("Raleigh", "27601"),
]
FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack",
    "Karen", "Leo", "Mona", "Nina", "Oscar", "Pat", "Quinn", "Rose", "Sam", "Tina",
]
TAGS_POOL = [
    "python", "rust", "go", "java", "js", "ts", "c++", "c#", "ruby", "scala",
    "kotlin", "swift", "haskell", "elixir", "perl", "lua", "zig", "nim", "dart", "r",
]


def generate_large_row(row_id):
    cat = CATEGORIES[row_id % len(CATEGORIES)]
    city, zip_code = CITIES[row_id % len(CITIES)]
    name = FIRST_NAMES[row_id % len(FIRST_NAMES)] + str(row_id)
    num_tags = random.randint(1, 4)
    tags = random.sample(TAGS_POOL, num_tags)
    return make_row(
        id=row_id,
        category=cat,
        value_int=row_id * 10 + random.randint(0, 9),
        value_float=round(random.uniform(0, 1000), 2),
        value_double=round(random.uniform(0, 100000), 4),
        name=name,
        tags=tags,
        city=city,
        zip_code=zip_code,
        hours_offset=row_id % 10000,
    )


def generate_large_table(num_rows, num_versions, output_path, seed=42):
    """Generate a large Delta table with synthetic data and multiple versions."""
    random.seed(seed)
    shutil.rmtree(output_path, ignore_errors=True)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # ── Version 0: bulk insert ──
    print(f"Generating {num_rows} rows...")
    rows = [generate_large_row(i) for i in range(1, num_rows + 1)]
    table = rows_to_table(rows)
    write_deltalake(
        output_path, table, mode="overwrite", partition_by=["category"],
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    print(f"v0: Inserted {num_rows} rows")

    next_id = num_rows + 1
    all_ids = list(range(1, num_rows + 1))

    for v in range(1, num_versions):
        dt = DeltaTable(output_path)
        batch_size = max(1, num_rows // 20)  # ~5% of rows per version

        if v % 3 == 1:
            # UPDATE
            update_ids = random.sample(all_ids, min(batch_size, len(all_ids)))
            update_rows = [generate_large_row(rid) for rid in update_ids]
            dt.merge(
                source=rows_to_table(update_rows),
                predicate="s.id = t.id", source_alias="s", target_alias="t",
            ).when_matched_update_all().execute()
            print(f"v{v}: Updated {len(update_ids)} rows")

        elif v % 3 == 2:
            # DELETE
            delete_count = min(batch_size // 2, len(all_ids))
            delete_ids = random.sample(all_ids, delete_count)
            dt.merge(
                source=pa.table({"id": delete_ids}, schema=pa.schema([("id", pa.int64())])),
                predicate="s.id = t.id", source_alias="s", target_alias="t",
            ).when_matched_delete().execute()
            for did in delete_ids:
                all_ids.remove(did)
            print(f"v{v}: Deleted {delete_count} rows")

        else:
            # INSERT + UPDATE
            insert_rows = [generate_large_row(next_id + i) for i in range(batch_size)]
            new_ids = list(range(next_id, next_id + batch_size))
            next_id += batch_size

            update_ids = random.sample(all_ids, min(batch_size, len(all_ids)))
            update_rows = [generate_large_row(rid) for rid in update_ids]

            merge_rows = insert_rows + update_rows
            dt.merge(
                source=rows_to_table(merge_rows),
                predicate="s.id = t.id", source_alias="s", target_alias="t",
            ).when_matched_update_all().when_not_matched_insert_all().execute()
            all_ids.extend(new_ids)
            print(f"v{v}: Inserted {batch_size}, Updated {len(update_ids)} rows")

    dt = DeltaTable(output_path)
    print(f"\nFinal table version: {dt.version()}")
    print(f"History entries: {len(dt.history())}")
    print(f"Row count: {dt.to_pyarrow_table().num_rows}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Delta Lake test tables")
    parser.add_argument("--large", action="store_true", help="Generate a large table with synthetic data")
    parser.add_argument("--rows", type=int, default=100_000, help="Number of rows for large table (default: 100000)")
    parser.add_argument("--versions", type=int, default=6, help="Number of versions for large table (default: 6)")
    parser.add_argument("--output", type=str, default=None, help="Output path (overrides default)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    args = parser.parse_args()

    if args.large:
        output = args.output or LARGE_TABLE_PATH
        generate_large_table(args.rows, args.versions, output, seed=args.seed)
    else:
        if args.output:
            TABLE_PATH = args.output
        main()

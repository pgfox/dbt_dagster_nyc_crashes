"""
Quick DuckDB exploration of the NYC Motor Vehicle Collisions CSVs.
Run with: .venv/bin/python explore_data.py
"""

import duckdb
from tabulate import tabulate

DATA = "data"
CRASHES  = f"{DATA}/Motor_Vehicle_Collisions_-_Crashes.csv"
PERSONS  = f"{DATA}/Motor_Vehicle_Collisions_-_Person.csv"
VEHICLES = f"{DATA}/Motor_Vehicle_Collisions_-_Vehicles.csv"

# Vehicles CSV uses double-quote quoting; force it so embedded commas parse correctly.
VEHICLES_READ = f"read_csv('{VEHICLES}', quote='\"', header=true)"

con = duckdb.connect()

# ------------------------------------------------------------------
# 1. Row counts
# ------------------------------------------------------------------
print("=== Row counts ===")
for label, src in [
    ("crashes",  f"read_csv_auto('{CRASHES}')"),
    ("persons",  f"read_csv_auto('{PERSONS}')"),
    ("vehicles", VEHICLES_READ),
]:
    n = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
    print(f"  {label}: {n:,}")

# ------------------------------------------------------------------
# 2. Top 10 boroughs by total people killed (crashes file)
# ------------------------------------------------------------------
print("\n=== Top boroughs by total persons killed ===")
result = con.execute(f"""
    SELECT
        COALESCE(BOROUGH, '(unknown)') AS borough,
        SUM("NUMBER OF PERSONS KILLED")  AS total_killed,
        SUM("NUMBER OF PERSONS INJURED") AS total_injured,
        COUNT(*)                          AS num_crashes
    FROM read_csv_auto('{CRASHES}')
    GROUP BY borough
    ORDER BY total_killed DESC
    LIMIT 10
""").fetchall()
print(tabulate(result, headers=[d[0] for d in con.description], tablefmt="simple"))

# ------------------------------------------------------------------
# 3. Top 10 contributing factors (crashes file)
# ------------------------------------------------------------------
print("\n=== Top 10 contributing factors ===")
result = con.execute(f"""
    SELECT
        "CONTRIBUTING FACTOR VEHICLE 1" AS factor,
        COUNT(*) AS occurrences
    FROM read_csv_auto('{CRASHES}')
    WHERE "CONTRIBUTING FACTOR VEHICLE 1" NOT IN ('Unspecified', '')
      AND "CONTRIBUTING FACTOR VEHICLE 1" IS NOT NULL
    GROUP BY factor
    ORDER BY occurrences DESC
    LIMIT 10
""").fetchall()
print(tabulate(result, headers=[d[0] for d in con.description], tablefmt="simple"))

# ------------------------------------------------------------------
# 4. Crashes per year (crashes file)
# ------------------------------------------------------------------
print("\n=== Crashes per year ===")
result = con.execute(f"""
    SELECT
        YEAR("CRASH DATE") AS crash_year,
        COUNT(*) AS num_crashes,
        SUM("NUMBER OF PERSONS KILLED") AS total_killed
    FROM read_csv_auto('{CRASHES}')
    WHERE "CRASH DATE" IS NOT NULL
    GROUP BY crash_year
    ORDER BY crash_year
""").fetchall()
print(tabulate(result, headers=[d[0] for d in con.description], tablefmt="simple"))

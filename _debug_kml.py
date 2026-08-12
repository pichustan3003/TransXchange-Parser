import os
import sqlite3
import time

from timetable import (
    collect_route_line_coordinates,
    extract_route_placemarks,
    list_route_definitions,
    service_metadata_from_db_path,
)

path = ".out/ARVA/ARVA 29.db"
if os.path.exists(path):
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    tables = [row[0] for row in cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    print("tables:", tables)
    routes = list_route_definitions(cursor)
    print("routes:", len(routes))
    for route_id, description, section_refs in routes[:5]:
        coordinates = collect_route_line_coordinates(cursor, section_refs)
        print(" ", route_id, description[:45], "coords", len(coordinates))
    operator_code, line_name = service_metadata_from_db_path(path)
    placemarks = extract_route_placemarks(cursor, operator_code, line_name, out_dir=".out")
    print("placemarks:", len(placemarks))
    conn.close()

total = 0
with_coords = 0
without_coords = 0
start = time.time()
for root, _, files in os.walk(".out"):
    for filename in files:
        if not filename.endswith(".db") or filename in ("stops.db", "Operators.db"):
            continue
        db_path = os.path.join(root, filename)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        has_any = False
        for _, _, section_refs in list_route_definitions(cursor):
            if collect_route_line_coordinates(cursor, section_refs):
                has_any = True
                break
        if has_any:
            with_coords += 1
        else:
            without_coords += 1
        operator_code, line_name = service_metadata_from_db_path(db_path)
        placemarks = extract_route_placemarks(
            cursor, operator_code, line_name, out_dir=".out"
        )
        total += len(placemarks)
        conn.close()
print("dbs with coords:", with_coords, "without:", without_coords)
print("total placemarks:", total, "seconds:", round(time.time() - start, 1))

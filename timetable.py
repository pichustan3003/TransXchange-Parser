import argparse
import csv
import json
import math
import os
import re
import time
import traceback
import base64
import io
from collections import defaultdict
from html import escape
from concurrent.futures import ProcessPoolExecutor
from PIL import Image, ImageDraw, ImageFont
import requests
import xmltodict
import sqlite3
import datetime
from datetime import date, timedelta
from tqdm import tqdm

from cleaner import parse_tnds_filename

KML_SHOW_LABELS = True
PROCESS_QUIET = False
STOPS_CSV_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    ".data",
    "Stops.csv",
)
_NAPTAN_STOP_INDEX = None

def time_to_seconds(timestr):
    h, m, s = map(int, timestr.split(":"))
    return h * 3600 + m * 60 + s


def format_time(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    return f"{hours:02}:{minutes:02}:{secs:02}"


def process_log(message):
    if not PROCESS_QUIET:
        print(message)


SQLITE_BUSY_TIMEOUT_MS = 60000


def configure_sqlite_connection(conn, write=True):
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-64000")
    if write:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")


def open_sqlite_database(db_path, readonly=False):
    timeout_seconds = SQLITE_BUSY_TIMEOUT_MS / 1000
    if readonly:
        uri_path = os.path.abspath(db_path).replace("\\", "/")
        conn = sqlite3.connect(
            f"file:{uri_path}?mode=ro",
            uri=True,
            timeout=timeout_seconds,
        )
        configure_sqlite_connection(conn, write=False)
        return conn

    conn = sqlite3.connect(db_path, timeout=timeout_seconds)
    configure_sqlite_connection(conn, write=True)
    return conn


def database_build_lock_path(db_path):
    return db_path + ".lock"


def claim_database_build(db_path):
    """Atomically claim building a route database in a worker process."""
    if os.path.exists(db_path):
        return False

    lock_path = database_build_lock_path(db_path)
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False


def release_database_build(db_path):
    lock_path = database_build_lock_path(db_path)
    try:
        os.remove(lock_path)
    except FileNotFoundError:
        pass


def cleanup_failed_database(db_path):
    release_database_build(db_path)
    for path in (db_path, db_path + "-wal", db_path + "-shm"):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
    for sidecar_ext in (".json", ".csv"):
        sidecar_path = os.path.splitext(db_path)[0] + sidecar_ext
        try:
            os.remove(sidecar_path)
        except FileNotFoundError:
            pass


def db_path_from_tnds_filename(file_path, out_dir=".out"):
    """Guess the output DB path from a standard TNDS filename without parsing XML."""
    parsed = parse_tnds_filename(os.path.basename(file_path))
    if parsed is None:
        return None

    service_key, _ = parsed
    line, operator_code, _, _, _ = service_key.split("_")
    db_name = f"{operator_code} {line}"
    return os.path.join(out_dir, operator_code, db_name + ".db")


def select_tnds_files_to_process(files, today=None):
    """
    Keep one XML file per registered service instead of parsing every duplicate
    registration period.
    """
    today = today or date.today()
    grouped = defaultdict(list)
    ungrouped = []

    for path in files:
        parsed = parse_tnds_filename(os.path.basename(path))
        if parsed is None:
            ungrouped.append(path)
            continue

        service_key, validity_start = parsed
        grouped[service_key].append((path, validity_start))

    selected = list(ungrouped)
    for entries in grouped.values():
        active = [(path, start) for path, start in entries if start <= today]
        candidates = active or entries
        selected.append(max(candidates, key=lambda item: item[1])[0])

    return select_best_file_per_service_output(selected)


def quick_output_db_path(file_path, out_dir=".out"):
    """Return the route DB path a TNDS XML file would produce."""
    try:
        with open(file_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            if file_size <= 500000:
                f.seek(0)
                data = f.read()
            else:
                data = b""
                for start_ratio in (0.79, 0.81, 0.83):
                    start = max(0, int(file_size * start_ratio) - 65536)
                    f.seek(start)
                    chunk = f.read(131072)
                    data += chunk
                    if (
                        b"<NationalOperatorCode>" in chunk
                        or b"<OperatorCode>" in chunk
                    ) and b"<LineName>" in chunk:
                        break
    except OSError:
        return None

    operator_match = re.search(
        rb"<NationalOperatorCode>([^<]+)</NationalOperatorCode>", data
    )
    if operator_match is None:
        operator_match = re.search(rb"<OperatorCode>([^<]+)</OperatorCode>", data)
    line_match = re.search(rb"<LineName>([^<]+)</LineName>", data)
    if operator_match is None or line_match is None:
        return None

    operator_code = operator_match.group(1).decode("utf-8", errors="ignore").strip()
    line_name = line_match.group(1).decode("utf-8", errors="ignore").strip()
    if not operator_code or not line_name:
        return None

    db_name = f"{operator_code} {line_name}"
    return os.path.join(out_dir, operator_code, db_name + ".db")


def tnds_file_score(file_path):
    """Prefer the largest registration file for the same output route DB."""
    try:
        return (os.path.getsize(file_path), 0, 0)
    except OSError:
        return (0, 0, 0)


def _tnds_file_index_path(out_dir=".out"):
    return os.path.join(out_dir, "tnds_file_index.json")


def load_tnds_file_index(out_dir=".out"):
    index_path = _tnds_file_index_path(out_dir)
    if not os.path.exists(index_path):
        return {}

    try:
        with open(index_path, encoding="utf-8") as index_file:
            payload = json.load(index_file)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}

    entries = payload.get("entries", {})
    return {
        file_path: entry
        for file_path, entry in entries.items()
        if isinstance(entry, dict)
    }


def save_tnds_file_index(entries, out_dir=".out"):
    index_path = _tnds_file_index_path(out_dir)
    try:
        os.makedirs(out_dir, exist_ok=True)
        with open(index_path, "w", encoding="utf-8") as index_file:
            json.dump({"entries": entries}, index_file, indent=2)
    except OSError:
        pass


def cached_output_db_path(file_path, out_dir=".out", index=None, index_updates=None):
    abs_path = os.path.abspath(file_path)
    try:
        mtime = os.path.getmtime(file_path)
        size = os.path.getsize(file_path)
    except OSError:
        return None

    if index is not None:
        cached = index.get(abs_path)
        if cached and cached.get("mtime") == mtime and cached.get("size") == size:
            return cached.get("db_path")

    db_path = quick_output_db_path(file_path, out_dir)
    if index_updates is not None:
        index_updates[abs_path] = {
            "mtime": mtime,
            "size": size,
            "db_path": db_path,
        }
    return db_path


def select_best_file_per_service_output(files, out_dir=".out"):
    """
    When several registrations map to the same output DB, keep the richest file.

    Parallel parsing previously let whichever worker finished first win, which
    could leave sparse registrations (for example NXMT YEL) in place of the
    main dataset.
    """
    grouped = defaultdict(list)
    ungrouped = []
    index = load_tnds_file_index(out_dir)
    index_updates = {}

    for path in tqdm(files, desc="Grouping duplicate registrations", unit="file"):
        db_path = cached_output_db_path(
            path, out_dir, index=index, index_updates=index_updates,
        )
        if db_path is None:
            ungrouped.append(path)
            continue
        grouped[db_path].append(path)

    selected = []
    duplicate_groups = [paths for paths in grouped.values() if len(paths) > 1]
    if duplicate_groups:
        for paths in tqdm(
            duplicate_groups,
            desc="Choosing richest registration",
            unit="route",
        ):
            selected.append(max(paths, key=tnds_file_score))

    for paths in grouped.values():
        if len(paths) == 1:
            selected.append(paths[0])

    selected.extend(ungrouped)

    if index_updates:
        index.update(index_updates)
        save_tnds_file_index(index, out_dir)

    return selected


def load_naptan_stop_index(stops_csv_path=None):
    global _NAPTAN_STOP_INDEX
    if _NAPTAN_STOP_INDEX is not None:
        return _NAPTAN_STOP_INDEX

    stops_csv_path = stops_csv_path or STOPS_CSV_PATH
    index = {}
    if os.path.exists(stops_csv_path):
        with open(stops_csv_path, encoding="utf-8", newline="") as stops_file:
            for row in csv.DictReader(stops_file):
                atco_code = (row.get("ATCOCode") or "").strip()
                if not atco_code:
                    continue

                longitude = latitude = None
                try:
                    if row.get("Longitude"):
                        longitude = float(row["Longitude"])
                    if row.get("Latitude"):
                        latitude = float(row["Latitude"])
                except (TypeError, ValueError):
                    longitude = latitude = None

                index[atco_code] = {
                    "naptan": (row.get("NaptanCode") or "").strip(),
                    "name": (row.get("CommonName") or "").strip(),
                    "longitude": longitude,
                    "latitude": latitude,
                }

    _NAPTAN_STOP_INDEX = index
    return index


def load_stop_display_names(cursor):
    table_names = database_table_names(cursor)
    if "StopPoints" not in table_names:
        return {}

    return {
        stop_ref: common_name
        for stop_ref, common_name in cursor.execute(
            "SELECT StopNameRef, CommonName FROM StopPoints"
        ).fetchall()
        if stop_ref
    }


def write_timetable_csv(csv_path, timetable, stop_names, operator, route,
                        naptan_index=None):
    naptan_index = naptan_index or load_naptan_stop_index()

    def journey_sort_key(journey_code):
        return int(journey_code) if str(journey_code).isdigit() else journey_code

    with open(csv_path, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([
            "Operator",
            "Route",
            "Direction",
            "JourneyCode",
            "StopSequence",
            "StopCode",
            "AtcoCode",
            "StopName",
            "Arrival",
            "Departure",
        ])

        for journey_code in sorted(timetable.keys(), key=journey_sort_key):
            journey = timetable[journey_code]
            direction = journey.get("Direction", "")
            stop_sequence = 0

            for stop_ref, times in journey.items():
                if stop_ref == "Direction":
                    continue

                stop_sequence += 1
                naptan_row = naptan_index.get(stop_ref, {})
                stop_name = (
                    stop_names.get(stop_ref)
                    or naptan_row.get("name")
                    or stop_ref
                )
                stop_code = naptan_row.get("naptan") or stop_ref
                writer.writerow([
                    operator,
                    route,
                    direction,
                    journey_code,
                    stop_sequence,
                    stop_code,
                    stop_ref,
                    stop_name,
                    times.get("Arrival", ""),
                    times.get("Departure", ""),
                ])


def export_timetable_csv_for_database(db_path, naptan_index=None):
    json_path = os.path.splitext(db_path)[0] + ".json"
    csv_path = os.path.splitext(db_path)[0] + ".csv"
    if not os.path.exists(json_path):
        return False

    operator, route = service_metadata_from_db_path(db_path)
    with open(json_path, encoding="utf-8") as json_file:
        timetable = json.load(json_file)

    conn = open_sqlite_database(db_path, readonly=True)
    try:
        stop_names = load_stop_display_names(conn.cursor())
    finally:
        conn.close()

    write_timetable_csv(
        csv_path,
        timetable,
        stop_names,
        operator,
        route,
        naptan_index=naptan_index,
    )
    return True


def export_all_timetable_csv(out_dir=".out"):
    naptan_index = load_naptan_stop_index()
    written = 0
    for db_path in sorted(iter_service_databases(out_dir)):
        if export_timetable_csv_for_database(db_path, naptan_index=naptan_index):
            written += 1
    if written:
        process_log(f"Exported {written} timetable CSV files from {out_dir}")
    return written

# txc = parsed TransXChange dict

def splitHMStoS(duration):
    """Parse ISO 8601 duration (e.g. PT1H2M30S, PT45S) to seconds."""
    if not duration:
        return 0

    body = duration.strip()
    if not body.startswith("PT"):
        return 0

    body = body[2:]
    hours = minutes = seconds = 0

    match = re.search(r"(\d+)H", body)
    if match:
        hours = int(match.group(1))
    match = re.search(r"(\d+)M", body)
    if match:
        minutes = int(match.group(1))
    match = re.search(r"(\d+)S", body)
    if match:
        seconds = int(match.group(1))

    return hours * 3600 + minutes * 60 + seconds


def parse_wait_time(from_point):
    wait_time = from_point.get("WaitTime")
    if wait_time is None:
        return 0
    try:
        return splitHMStoS(wait_time)
    except (TypeError, ValueError):
        return 0


def parse_location_coordinates(location):
    """Return (longitude, latitude) from a TransXChange Mapping Location."""
    translation = location.get("Translation") or location
    longitude = translation.get("Longitude")
    latitude = translation.get("Latitude")
    if longitude is None or latitude is None:
        return None

    try:
        return float(longitude), float(latitude)
    except (TypeError, ValueError):
        return None


_OSG_TO_WGS84 = None


def get_osgb_to_wgs84_transformer():
    global _OSG_TO_WGS84
    if _OSG_TO_WGS84 is None:
        try:
            from pyproj import Transformer

            _OSG_TO_WGS84 = Transformer.from_crs(
                "EPSG:27700", "EPSG:4326", always_xy=True
            )
        except ImportError:
            _OSG_TO_WGS84 = False
    return _OSG_TO_WGS84 if _OSG_TO_WGS84 is not False else None


def parse_stop_location(location):
    """Return (longitude, latitude) from a stop Location element."""
    if not location:
        return None

    coordinates = parse_location_coordinates(location)
    if coordinates is not None:
        return coordinates

    translation = location.get("Translation") or location
    easting = translation.get("Easting")
    northing = translation.get("Northing")
    if easting is None or northing is None:
        return None

    try:
        easting = float(easting)
        northing = float(northing)
    except (TypeError, ValueError):
        return None

    transformer = get_osgb_to_wgs84_transformer()
    if transformer is None:
        return None

    longitude, latitude = transformer.transform(easting, northing)
    return longitude, latitude

def daterange(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def parse_date(date_str):
    # Handles:
    # 2026-12-18
    # 2026-12-18T00:00:00
    return datetime.datetime.fromisoformat(
        date_str.replace("Z", "")
    ).date()


def ensure_list(value):
    if value is None:
        return []

    if isinstance(value, list):
        return value

    return [value]


def get_service(txc):
    services = ensure_list(txc.get("Services", {}).get("Service"))
    if not services:
        return {}
    return services[0]


def get_service_line_names(txc):
    """Return unique line names from a service (Line may be a dict or list)."""
    line_names = []
    seen = set()
    for line in ensure_list(get_service(txc).get("Lines", {}).get("Line")):
        line_name = line.get("LineName")
        if line_name is None:
            continue
        line_name = str(line_name).strip()
        if not line_name:
            continue
        key = line_name.casefold()
        if key in seen:
            continue
        seen.add(key)
        line_names.append(line_name)
    return line_names


def get_service_line_label(line_names):
    if not line_names:
        return "Unknown"
    if len(line_names) == 1:
        return line_names[0]
    return "-".join(line_names)


def build_journey_pattern_link_index(data):
    """Map journey pattern ids to ordered JourneyPatternTimingLink ids."""
    section_links = {}
    for section in ensure_list(
        data["TransXChange"]["JourneyPatternSections"]["JourneyPatternSection"]
    ):
        links = ensure_list(section["JourneyPatternTimingLink"])
        section_links[section["@id"]] = [link["@id"] for link in links]

    pattern_section_refs = {}
    for service in ensure_list(data["TransXChange"]["Services"]["Service"]):
        standard_service = service.get("StandardService") or {}
        for journey_pattern in ensure_list(
            standard_service.get("JourneyPattern", [])
        ):
            pattern_section_refs[journey_pattern["@id"]] = ensure_list(
                journey_pattern.get("JourneyPatternSectionRefs", [])
            )

    return pattern_section_refs, section_links


def build_journey_pattern_metadata(data):
    """Map journey pattern ids to route refs and travel direction."""
    pattern_route_refs = {}
    pattern_directions = {}
    service_origin = None
    service_destination = None

    for service in ensure_list(data["TransXChange"]["Services"]["Service"]):
        standard_service = service.get("StandardService") or {}
        if service_origin is None:
            service_origin = standard_service.get("Origin")
            service_destination = standard_service.get("Destination")

        for journey_pattern in ensure_list(
            standard_service.get("JourneyPattern", [])
        ):
            pattern_id = journey_pattern["@id"]
            if "RouteRef" in journey_pattern:
                pattern_route_refs[pattern_id] = journey_pattern["RouteRef"]
            if "Direction" in journey_pattern:
                pattern_directions[pattern_id] = journey_pattern["Direction"]

    return pattern_route_refs, pattern_directions, service_origin, service_destination


def get_journey_direction_description(cursor, journey_pattern_ref, pattern_route_refs,
                                    pattern_directions, service_origin,
                                    service_destination):
    route_ref = pattern_route_refs.get(journey_pattern_ref, journey_pattern_ref)
    direction_row = cursor.execute(
        "SELECT Description FROM Routes WHERE RouteId = ?",
        (route_ref,),
    ).fetchone()
    if direction_row:
        return direction_row[0]

    direction = pattern_directions.get(journey_pattern_ref)
    if direction and service_origin and service_destination:
        if direction == "outbound":
            return f"{service_origin} - {service_destination}"
        if direction == "inbound":
            return f"{service_destination} - {service_origin}"

    return "Unknown"


def get_timing_sections(cursor, vehicle_journey_code, journey_pattern_ref,
                        pattern_section_refs, section_links):
    """
    Return ordered timing rows:
    (orderCol, from_stop, to_stop, runtime, waittime)
    """
    rows = cursor.execute("""
        SELECT
            j.orderCol,
            jp.FromStopPointRef,
            jp.ToStopPointRef,
            jp.Runtime,
            jp.Waittime
        FROM Journeys j
        JOIN JourneyPatterns jp
            ON jp.ExternalId = j.JourneyPatternTimingLinkRef
        WHERE j.vehicleJourneyCode = ?
        ORDER BY j.orderCol
    """, (vehicle_journey_code,)).fetchall()

    if rows:
        return rows

    sections = []
    order_col = 0
    for section_ref in pattern_section_refs.get(journey_pattern_ref, []):
        for link_id in section_links.get(section_ref, []):
            row = cursor.execute("""
                SELECT FromStopPointRef, ToStopPointRef, Runtime, Waittime
                FROM JourneyPatterns
                WHERE ExternalId = ?
            """, (link_id,)).fetchone()
            if row is None:
                continue
            order_col += 1
            sections.append((order_col, row[0], row[1], row[2], row[3]))

    return sections


def journey_stop_refs_ordered(sections):
    """Ordered stop point refs visited on a journey (first departure + each leg destination)."""
    if not sections:
        return []

    ordered = [sections[0][1]]
    for section in sections:
        ordered.append(section[2])
    return ordered


def collect_route_stop_ids(journey_stops_by_code):
    """Unique stop point refs served by any journey on this route."""
    stop_ids = set()
    for ordered_refs in journey_stops_by_code.values():
        for stop_ref in ordered_refs:
            if stop_ref:
                stop_ids.add(stop_ref)
    return stop_ids


def parse_service_ref(service_ref):
    """Split a service ref such as 'ARVA 38' into operator and route name."""
    if " " in service_ref:
        operator, route_name = service_ref.split(" ", 1)
        return operator, route_name
    return service_ref, service_ref


def service_ref_path(service_ref, out_dir=".out", extension=".db"):
    operator, _ = parse_service_ref(service_ref)
    return os.path.join(out_dir, operator, service_ref + extension)


def normalize_xml_text(value, default=None):
    """Flatten xmltodict leaf values (strings, numbers, or {#text: ...} dicts)."""
    if value is None:
        return default
    if isinstance(value, dict):
        return value.get("#text", value.get("@text", default))
    return value


def get_operator_code(txc):
    operators = ensure_list((txc.get("Operators") or {}).get("Operator"))
    if not operators:
        return None

    operator = operators[0]
    if "NationalOperatorCode" in operator:
        return operator["NationalOperatorCode"]
    return operator.get("OperatorCode")


def write_route_bus_runs(cursor, journey_stops_by_code):
    """Store journey stop visits in the route database."""
    rows = []
    for vehicle_journey_code, ordered_refs in journey_stops_by_code.items():
        if not vehicle_journey_code or not ordered_refs:
            continue

        visited_sequences = {}
        for sequence, stop_ref in enumerate(ordered_refs, start=1):
            if not stop_ref or stop_ref in visited_sequences:
                continue
            visited_sequences[stop_ref] = sequence

        for stop_ref, sequence in visited_sequences.items():
            rows.append((vehicle_journey_code, stop_ref, sequence))

    cursor.execute("""
        create table if not exists busRuns (
            vehicleJourneyCode text not null,
            stopPointRef text not null,
            stopSequence integer not null,
            primary key (vehicleJourneyCode, stopPointRef)
        )
    """)
    cursor.execute(
        "create index if not exists idx_busruns_stop on busRuns (stopPointRef)"
    )
    cursor.execute("delete from busRuns")
    if rows:
        cursor.executemany(
            "insert into busRuns values (?,?,?)",
            rows,
        )


def bulk_write_busstops_rows(stops_db_path, rows):
    if not rows:
        return 0

    conn = open_sqlite_database(stops_db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        chunk_size = 50000
        for start in range(0, len(rows), chunk_size):
            conn.executemany(
                "insert or replace into busstops values (?,?,?)",
                rows[start:start + chunk_size],
            )
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def busstops_rows_for_database(db_path):
    operator, route_name = service_metadata_from_db_path(db_path)
    conn = open_sqlite_database(db_path, readonly=True)
    try:
        cursor = conn.cursor()
        if "busRuns" not in database_table_names(cursor):
            return []

        return [
            (stop_id, operator, route_name)
            for (stop_id,) in cursor.execute(
                """
                SELECT DISTINCT stopPointRef
                FROM busRuns
                WHERE stopPointRef IS NOT NULL AND stopPointRef != ''
                """
            ).fetchall()
        ]
    finally:
        conn.close()


def init_busstops_index(stops_db_path):
    conn = open_sqlite_database(stops_db_path)
    c = conn.cursor()
    c.execute("drop table if exists busRuns")
    c.execute("drop table if exists busstops")
    c.execute("""
        create table busstops (
            StopID text not null,
            Operator text not null,
            RouteName text not null,
            primary key (StopID, Operator, RouteName)
        )
    """)
    c.execute(
        "create index if not exists idx_busstops_stop_id on busstops (StopID)"
    )
    conn.commit()
    conn.close()


def update_busstops_index(stops_db_path, operator, route_name, stop_ids):
    """Record which routes call at each stop in the shared stops index."""
    rows = [
        (stop_id, operator, route_name)
        for stop_id in stop_ids
        if stop_id
    ]
    bulk_write_busstops_rows(stops_db_path, rows)


def rebuild_busstops_index(out_dir=".out", workers=None):
    """Rebuild the shared stop index from per-route busRuns tables."""
    stops_db_path = os.path.join(out_dir, "stops.db")
    init_busstops_index(stops_db_path)

    db_paths = list(iter_service_databases(out_dir))
    rows = []
    workers = workers or os.cpu_count()

    with ProcessPoolExecutor(max_workers=workers) as executor:
        for db_rows in tqdm(
            executor.map(busstops_rows_for_database, db_paths),
            total=len(db_paths),
            desc="Building stop index",
        ):
            rows.extend(db_rows)

    written = bulk_write_busstops_rows(stops_db_path, rows)
    if written:
        process_log(f"Stop index: {written:,} rows in {stops_db_path}")
    return written


def migrate_legacy_bus_runs(stops_db_path, out_dir=".out"):
    """Move rows from the old shared busRuns table into route databases."""
    if not os.path.exists(stops_db_path):
        return False

    conn = open_sqlite_database(stops_db_path, readonly=True)
    try:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='busRuns'"
        ).fetchone()
        if not row or "serviceRef" not in (row[0] or ""):
            return False

        legacy_rows = conn.execute(
            """
            SELECT serviceRef, vehicleJourneyCode, stopPointRef, stopSequence
            FROM busRuns
            """
        ).fetchall()
    finally:
        conn.close()

    if not legacy_rows:
        return False

    journeys_by_service = defaultdict(lambda: defaultdict(dict))
    for service_ref, vehicle_journey_code, stop_ref, stop_sequence in legacy_rows:
        journeys_by_service[service_ref][vehicle_journey_code][stop_ref] = stop_sequence

    migrated = 0
    for service_ref, journeys in journeys_by_service.items():
        db_path = service_ref_path(service_ref, out_dir)
        if not os.path.exists(db_path):
            continue

        journey_stops_by_code = {}
        for vehicle_journey_code, stop_sequences in journeys.items():
            ordered = [
                stop_ref
                for stop_ref, _ in sorted(
                    stop_sequences.items(),
                    key=lambda item: item[1],
                )
            ]
            journey_stops_by_code[vehicle_journey_code] = ordered

        route_conn = open_sqlite_database(db_path)
        try:
            write_route_bus_runs(route_conn.cursor(), journey_stops_by_code)
            route_conn.commit()
            migrated += 1
        finally:
            route_conn.close()

    if migrated:
        conn = open_sqlite_database(stops_db_path)
        try:
            conn.execute("drop table if exists busRuns")
            conn.commit()
        finally:
            conn.close()
    return bool(migrated)


def write_bus_runs(service_ref, journey_stops_by_code, stops_db_path):
    """Legacy wrapper kept for callers that still pass a service ref."""
    operator, route_name = parse_service_ref(service_ref)
    update_busstops_index(
        stops_db_path,
        operator,
        route_name,
        collect_route_stop_ids(journey_stops_by_code),
    )


def route_link_sort_key(route_link_ref):
    prefix, _, suffix = route_link_ref.rpartition("_")
    if suffix.isdigit():
        return (prefix, int(suffix))
    return (route_link_ref, 0)


def database_table_names(cursor):
    return {
        row[0]
        for row in cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def ordered_route_links_for_section(cursor, section_ref, table_names):
    if "RouteLinks" in table_names:
        return [
            row[0]
            for row in cursor.execute(
                """
                SELECT RouteLinkRef
                FROM RouteLinks
                WHERE RouteSectionRef = ?
                ORDER BY LinkOrder
                """,
                (section_ref,),
            ).fetchall()
        ]

    if "RouteLinkPoints" not in table_names:
        return []

    links = [
        row[0]
        for row in cursor.execute(
            """
            SELECT DISTINCT RouteLinkRef
            FROM RouteLinkPoints
            WHERE RouteSectionRef = ?
            """,
            (section_ref,),
        ).fetchall()
    ]
    return sorted(links, key=route_link_sort_key)


def find_longest_listed_route(cursor):
    """
    Pick the route definition with the most links across its sections.
    Ties break on mapping point count, then section count.
    """
    table_names = database_table_names(cursor)
    if "Routes" not in table_names:
        return None

    routes = cursor.execute("""
        SELECT RouteId, Description, RouteSectionRef, CAST(OrderCol AS INTEGER)
        FROM Routes
        ORDER BY RouteId, CAST(OrderCol AS INTEGER)
    """).fetchall()
    if not routes:
        return None

    grouped = {}
    for route_id, description, section_ref, order_col in routes:
        grouped.setdefault(route_id, {"description": description, "sections": []})
        grouped[route_id]["sections"].append((order_col, section_ref))

    best = None
    for route_id, route_info in grouped.items():
        section_refs = [
            section_ref
            for _, section_ref in sorted(route_info["sections"])
        ]

        link_count = 0
        point_count = 0
        for section_ref in section_refs:
            route_links = ordered_route_links_for_section(
                cursor, section_ref, table_names
            )
            link_count += len(route_links)
            for route_link_ref in route_links:
                if "RouteLinkPoints" not in table_names:
                    continue
                point_count += cursor.execute(
                    "SELECT COUNT(*) FROM RouteLinkPoints WHERE RouteLinkRef = ?",
                    (route_link_ref,),
                ).fetchone()[0]

        if link_count == 0:
            continue

        score = (link_count, point_count, len(section_refs))
        candidate = (score, route_id, route_info["description"], section_refs)
        if best is None or candidate[0] > best[0]:
            best = candidate

    if best is None:
        return None

    return best[1], best[2], best[3]


def list_route_definitions(cursor):
    """Return (route_id, description, ordered section refs) for each listed route."""
    table_names = database_table_names(cursor)
    if "Routes" not in table_names:
        return []

    routes = cursor.execute("""
        SELECT RouteId, Description, RouteSectionRef, CAST(OrderCol AS INTEGER)
        FROM Routes
        ORDER BY RouteId, CAST(OrderCol AS INTEGER)
    """).fetchall()
    if not routes:
        return []

    grouped = {}
    for route_id, description, section_ref, order_col in routes:
        grouped.setdefault(route_id, {"description": description, "sections": []})
        grouped[route_id]["sections"].append((order_col, section_ref))

    return [
        (
            route_id,
            route_info["description"],
            [section_ref for _, section_ref in sorted(route_info["sections"])],
        )
        for route_id, route_info in grouped.items()
    ]


def collect_route_stop_sequence(cursor, section_refs):
    """Return ordered stop point refs for a route definition."""
    table_names = database_table_names(cursor)
    if "RouteLinks" not in table_names:
        return []

    stops = []
    for section_ref in section_refs:
        for route_link_ref in ordered_route_links_for_section(
            cursor, section_ref, table_names
        ):
            row = cursor.execute(
                """
                SELECT FromStopPointRef, ToStopPointRef
                FROM RouteLinks
                WHERE RouteLinkRef = ?
                """,
                (route_link_ref,),
            ).fetchone()
            if row is None:
                continue

            from_stop, to_stop = row
            if not stops:
                if from_stop:
                    stops.append(from_stop)
            elif from_stop and from_stop != stops[-1]:
                stops.append(from_stop)
            if to_stop and (not stops or to_stop != stops[-1]):
                stops.append(to_stop)

    return stops


def normalize_stop_name(name):
    if not name:
        return ""
    return " ".join(name.split()).casefold()


def load_stop_common_names(cursor):
    table_names = database_table_names(cursor)
    if "StopPoints" not in table_names:
        return {}

    return {
        stop_ref: normalize_stop_name(common_name)
        for stop_ref, common_name in cursor.execute(
            "SELECT StopNameRef, CommonName FROM StopPoints"
        ).fetchall()
        if stop_ref
    }


def stop_ref_to_name(stop_ref, stop_names):
    if not stop_ref:
        return ""
    common_name = stop_names.get(stop_ref)
    if common_name:
        return common_name
    return stop_ref.casefold()


def collect_route_stop_name_sequence(cursor, section_refs, stop_names=None):
    """Return ordered stop common names for a route definition."""
    if stop_names is None:
        stop_names = load_stop_common_names(cursor)

    names = []
    for stop_ref in collect_route_stop_sequence(cursor, section_refs):
        stop_name = stop_ref_to_name(stop_ref, stop_names)
        if not stop_name:
            continue
        if not names or stop_name != names[-1]:
            names.append(stop_name)

    return names


def route_description_destination(description):
    normalized = " ".join((description or "").split())
    if " - " in normalized:
        return normalized.split(" - ", 1)[1].strip()
    if "-" in normalized:
        return normalized.split("-", 1)[1].strip()
    return normalized.strip()


def route_endpoint_key(candidate):
    stops = candidate.get("stop_names") or candidate.get("stops") or []
    if stops:
        return stops[-1]

    destination = route_description_destination(candidate.get("description", ""))
    if destination:
        return destination.casefold()

    return candidate.get("route_id") or ""


def is_stop_prefix(shorter, longer):
    if len(shorter) >= len(longer):
        return False
    return longer[: len(shorter)] == shorter


def route_redundant(shorter_stops, longer_stops):
    """
    True when shorter_stops is the same journey, a prefix, a reverse,
    or a reverse-prefix of longer_stops.
    """
    shorter = tuple(shorter_stops or [])
    longer = tuple(longer_stops or [])
    if not shorter or not longer or len(shorter) > len(longer):
        return False

    if shorter == longer:
        return True
    if is_stop_prefix(shorter, longer):
        return True

    reverse_longer = tuple(reversed(longer))
    if shorter == reverse_longer:
        return True
    if is_stop_prefix(shorter, reverse_longer):
        return True

    reverse_shorter = tuple(reversed(shorter))
    if is_stop_prefix(reverse_shorter, longer):
        return True

    return False


def select_unique_routes(candidates):
    """
    Keep distinct route shapes for KML.

    Drop variants that are subsets, exact reverses, or reverse-subsets of a
    longer route. Keep a second line only when it diverges to a different place.
    """
    candidates = sorted(
        candidates,
        key=lambda item: (
            len(item.get("stop_names") or item.get("stops") or []),
            len(item.get("coordinates") or []),
        ),
        reverse=True,
    )
    kept = []

    for candidate in candidates:
        candidate_stops = candidate.get("stop_names") or candidate.get("stops") or []
        if not candidate_stops:
            kept.append(candidate)
            continue

        if any(
            route_redundant(candidate_stops, other.get("stop_names") or other.get("stops") or [])
            for other in kept
        ):
            continue

        kept = [
            other
            for other in kept
            if not route_redundant(
                other.get("stop_names") or other.get("stops") or [],
                candidate_stops,
            )
        ]
        kept.append(candidate)

    return sorted(kept, key=route_endpoint_key)


def load_stop_coordinates(cursor, include_naptan_index=True):
    table_names = database_table_names(cursor)
    coordinates = {}
    if "StopPoints" in table_names:
        columns = {
            row[1] for row in cursor.execute("PRAGMA table_info(StopPoints)").fetchall()
        }
        if "Longitude" in columns and "Latitude" in columns:
            coordinates = {
                stop_ref: (longitude, latitude)
                for stop_ref, longitude, latitude in cursor.execute(
                    """
                    SELECT StopNameRef, Longitude, Latitude
                    FROM StopPoints
                    WHERE Longitude IS NOT NULL AND Latitude IS NOT NULL
                    """
                ).fetchall()
                if stop_ref
            }

    if include_naptan_index:
        for atco_code, row in load_naptan_stop_index().items():
            longitude = row.get("longitude")
            latitude = row.get("latitude")
            if longitude is None or latitude is None:
                continue
            coordinates.setdefault(atco_code, (longitude, latitude))

    return coordinates


def append_route_coordinate(coordinates, point):
    if point is None:
        return
    if coordinates and coordinates[-1] == point:
        return
    coordinates.append(point)


def append_route_stop_coordinate(coordinates, stop_coordinates, stop_ref,
                                 min_distance=0.00005):
    if not stop_ref or stop_ref not in stop_coordinates:
        return

    point = stop_coordinates[stop_ref]
    if coordinates:
        last_longitude, last_latitude = coordinates[-1]
        distance = math.hypot(
            point[0] - last_longitude,
            point[1] - last_latitude,
        )
        if distance < min_distance:
            return
    append_route_coordinate(coordinates, point)


def database_has_coordinates(cursor):
    table_names = database_table_names(cursor)
    if "RouteLinkPoints" in table_names:
        if cursor.execute("SELECT COUNT(*) FROM RouteLinkPoints").fetchone()[0]:
            return True

    return bool(load_stop_coordinates(cursor))


def collect_route_line_coordinates_from_stops(cursor, section_refs, stop_coordinates):
    """Build a route line from stop coordinates when track points are unavailable."""
    if not stop_coordinates:
        return []

    table_names = database_table_names(cursor)
    if "RouteLinks" not in table_names:
        return []

    coordinates = []
    for section_ref in section_refs:
        for route_link_ref in ordered_route_links_for_section(
            cursor, section_ref, table_names
        ):
            row = cursor.execute(
                """
                SELECT FromStopPointRef, ToStopPointRef
                FROM RouteLinks
                WHERE RouteLinkRef = ?
                """,
                (route_link_ref,),
            ).fetchone()
            if row is None:
                continue

            for stop_ref in (row[0], row[1]):
                if not stop_ref or stop_ref not in stop_coordinates:
                    continue
                point = stop_coordinates[stop_ref]
                if coordinates and coordinates[-1] == point:
                    continue
                coordinates.append(point)

    return coordinates


def collect_route_line_coordinates(cursor, section_refs, stop_coordinates=None):
    """Return ordered (longitude, latitude) pairs for a route definition."""
    table_names = database_table_names(cursor)
    if stop_coordinates is None:
        stop_coordinates = load_stop_coordinates(cursor)

    if "RouteLinkPoints" in table_names:
        if cursor.execute("SELECT COUNT(*) FROM RouteLinkPoints").fetchone()[0]:
            coordinates = []
            for section_ref in section_refs:
                for route_link_ref in ordered_route_links_for_section(
                    cursor, section_ref, table_names
                ):
                    rows = cursor.execute(
                        """
                        SELECT Longitude, Latitude
                        FROM RouteLinkPoints
                        WHERE RouteLinkRef = ?
                        ORDER BY PointSequence
                        """,
                        (route_link_ref,),
                    ).fetchall()
                    for longitude, latitude in rows:
                        append_route_coordinate(coordinates, (longitude, latitude))

                    if "RouteLinks" in table_names:
                        link_row = cursor.execute(
                            """
                            SELECT ToStopPointRef
                            FROM RouteLinks
                            WHERE RouteLinkRef = ?
                            """,
                            (route_link_ref,),
                        ).fetchone()
                        if link_row is not None:
                            append_route_stop_coordinate(
                                coordinates,
                                stop_coordinates,
                                link_row[0],
                            )

            if coordinates:
                return coordinates

    return collect_route_line_coordinates_from_stops(
        cursor, section_refs, stop_coordinates
    )


BUSTIMES_HEADERS = {"User-Agent": "TransXchangeParser/1.0"}
SERVICE_DB_SKIP = {"stops.db", "Operators.db"}
OPERATOR_BRAND_COLOURS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "operator_brand_colours.json",
)


def service_metadata_from_db_path(db_path):
    operator_code = os.path.basename(os.path.dirname(db_path))
    service_name = os.path.splitext(os.path.basename(db_path))[0]
    prefix = f"{operator_code} "
    if service_name.startswith(prefix):
        line_name = service_name[len(prefix):]
    else:
        line_name = service_name
    return operator_code, line_name


def format_placemark_name(operator_code, line_name, route_description):
    return f"{operator_code}-{line_name} {route_description}"


def format_route_label(operator_code, line_name):
    return f"{operator_code}-{line_name}"


def kml_line_color(hex_colour):
    if not hex_colour:
        return None
    hex_colour = hex_colour.lstrip("#")
    if len(hex_colour) != 6:
        return None
    red = hex_colour[0:2]
    green = hex_colour[2:4]
    blue = hex_colour[4:6]
    return f"ff{blue}{green}{red}"


def route_colours_path(operator_code, out_dir=".out"):
    return os.path.join(out_dir, operator_code, "route_colours.json")


def load_route_colours(operator_code, out_dir=".out"):
    colours_path = route_colours_path(operator_code, out_dir)
    if not os.path.exists(colours_path):
        return None

    with open(colours_path, encoding="utf-8") as colours_file:
        payload = json.load(colours_file)

    return payload.get("routes")


def load_route_colours_payload(operator_code, out_dir=".out"):
    colours_path = route_colours_path(operator_code, out_dir)
    if not os.path.exists(colours_path):
        return None

    with open(colours_path, encoding="utf-8") as colours_file:
        return json.load(colours_file)


def load_operator_brand_colours(path=OPERATOR_BRAND_COLOURS_PATH):
    if not os.path.exists(path):
        return {}

    with open(path, encoding="utf-8") as brand_colours_file:
        payload = json.load(brand_colours_file)

    return {
        str(brand): str(colour).upper()
        for brand, colour in payload.items()
        if brand and colour
    }


def get_operator_name(operator_code, out_dir=".out"):
    payload = load_route_colours_payload(operator_code, out_dir)
    if payload and payload.get("name"):
        return payload["name"]
    return None


def lookup_operator_brand_colour(operator_code, out_dir=".out"):
    brand_colours = load_operator_brand_colours()
    if not brand_colours:
        return None

    operator_name = get_operator_name(operator_code, out_dir)
    if not operator_name:
        return None

    normalized_name = operator_name.casefold()
    for brand, colour in sorted(
        brand_colours.items(),
        key=lambda item: len(item[0]),
        reverse=True,
    ):
        if brand.casefold() in normalized_name:
            return colour

    return None


def lookup_route_colour(colours, line_name):
    if not colours or line_name is None:
        return None

    normalized = str(line_name).strip().casefold()
    if not normalized:
        return None

    for key, value in colours.items():
        if str(key).strip().casefold() == normalized:
            return value
    return None


def resolve_route_colour(operator_code, line_name, out_dir=".out", colours=None, line_names=None):
    if colours is None:
        colours = load_route_colours(operator_code, out_dir)

    candidates = []
    for name in [line_name, *(line_names or [])]:
        if name is None:
            continue
        candidate = str(name).strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    for candidate in candidates:
        route_colour = lookup_route_colour(colours, candidate)
        if route_colour:
            return route_colour

    return lookup_operator_brand_colour(operator_code, out_dir)


def parse_bustimes_operator_colours(html):
    colours_by_id = {}
    for match in re.finditer(
        r"\.colour-(\d+)\s*\{[^}]*background:\s*(#[0-9A-Fa-f]{6})",
        html,
    ):
        colours_by_id[match.group(1)] = match.group(2).upper()

    routes = {}
    for match in re.finditer(
        r'<strong class="name[^"]*colour colour-(\d+)[^"]*">\s*([^<]+?)\s*</strong>',
        html,
        re.S,
    ):
        colour_id = match.group(1)
        line_name = re.sub(r"\s+", " ", match.group(2)).strip()
        background = colours_by_id.get(colour_id)
        if line_name and background:
            routes[line_name] = background

    return routes


def fetch_operator_route_colours(operator, out_dir=".out", raise_on_missing=True, quiet=False):
    """
    Fetch route colours from bustimes.org for one operator only.

    operator may be a National Operator Code (e.g. LOTH) or a bustimes slug
    (e.g. lothian-buses). Colours are saved to
    .out/<NOC>/route_colours.json for use when generating KML files.
    """
    operator_url = f"https://bustimes.org/operators/{operator}"
    response = requests.get(
        operator_url,
        timeout=30,
        headers=BUSTIMES_HEADERS,
        allow_redirects=True,
    )
    if response.status_code == 404:
        if raise_on_missing:
            raise ValueError(f"Operator not found on bustimes.org: {operator}")
        return None

    response.raise_for_status()

    noc = operator
    operator_name = None
    api_response = requests.get(
        f"https://bustimes.org/api/operators/{operator}",
        timeout=30,
        headers=BUSTIMES_HEADERS,
    )
    if api_response.status_code == 200:
        api_data = api_response.json()
        noc = api_data["noc"]
        operator_name = api_data.get("name")

    routes = parse_bustimes_operator_colours(response.text)
    operator_dir = os.path.join(out_dir, noc)
    os.makedirs(operator_dir, exist_ok=True)

    payload = {
        "operator": noc,
        "name": operator_name,
        "source": response.url,
        "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "routes": routes,
    }
    colours_path = route_colours_path(noc, out_dir)
    with open(colours_path, "w", encoding="utf-8") as colours_file:
        json.dump(payload, colours_file, ensure_ascii=False, indent=2)

    if not quiet:
        print(f"{noc}: saved {len(routes)} route colours to {colours_path}")
    return payload


def backfill_operator_name_from_bustimes(operator, out_dir=".out"):
    """Add operator name to route_colours.json when missing (for brand colour matching)."""
    colours_path = route_colours_path(operator, out_dir)
    if not os.path.exists(colours_path):
        return False

    with open(colours_path, encoding="utf-8") as colours_file:
        payload = json.load(colours_file)

    if payload.get("name"):
        return False

    api_response = requests.get(
        f"https://bustimes.org/api/operators/{operator}",
        timeout=30,
        headers=BUSTIMES_HEADERS,
    )
    if api_response.status_code != 200:
        return False

    operator_name = api_response.json().get("name")
    if not operator_name:
        return False

    payload["name"] = operator_name
    with open(colours_path, "w", encoding="utf-8") as colours_file:
        json.dump(payload, colours_file, ensure_ascii=False, indent=2)

    return True


def iter_operator_folders(out_dir=".out"):
    """Yield operator codes for each subdirectory of out_dir."""
    if not os.path.isdir(out_dir):
        return

    for name in sorted(os.listdir(out_dir)):
        if os.path.isdir(os.path.join(out_dir, name)):
            yield name


def fetch_all_operator_route_colours(out_dir=".out", skip_existing=False, delay_seconds=0.25):
    """
    Fetch route colours from bustimes.org for every operator folder in out_dir.

    Uses each folder name as the bustimes operator slug/NOC (e.g. LOTH, ECBU).
    Operators not listed on bustimes.org are skipped. Returns a summary dict.
    """
    results = {
        "saved": [],
        "skipped": [],
        "not_found": [],
        "failed": [],
    }
    operators = list(iter_operator_folders(out_dir))

    for operator in tqdm(operators, desc="Fetching route colours"):
        colours_path = route_colours_path(operator, out_dir)
        if skip_existing and os.path.exists(colours_path):
            backfill_operator_name_from_bustimes(operator, out_dir)
            results["skipped"].append(operator)
            continue

        try:
            payload = fetch_operator_route_colours(
                operator,
                out_dir,
                raise_on_missing=False,
                quiet=True,
            )
            if payload is None:
                results["not_found"].append(operator)
            else:
                results["saved"].append(payload["operator"])
        except Exception as exc:
            results["failed"].append((operator, str(exc)))

        if delay_seconds:
            time.sleep(delay_seconds)

    print(
        f"Route colours: {len(results['saved'])} saved, "
        f"{len(results['skipped'])} skipped, "
        f"{len(results['not_found'])} not on bustimes.org, "
        f"{len(results['failed'])} failed"
    )
    return results


KML_LINE_WIDTH = 6
LABEL_FONT_SIZE = 15
_LABEL_FONT = None
_LABEL_ICON_CACHE = {}


def load_label_font(size=LABEL_FONT_SIZE):
    global _LABEL_FONT
    if _LABEL_FONT is not None:
        return _LABEL_FONT

    font_candidates = [
        os.path.join(os.environ.get("WINDIR", r"C:\Windows"), "Fonts", "arialbd.ttf"),
        os.path.join(os.environ.get("WINDIR", r"C:\Windows"), "Fonts", "arial.ttf"),
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    ]
    for font_path in font_candidates:
        if os.path.exists(font_path):
            try:
                _LABEL_FONT = ImageFont.truetype(font_path, size)
                return _LABEL_FONT
            except OSError:
                continue
    _LABEL_FONT = ImageFont.load_default()
    return _LABEL_FONT


def build_label_icon_data_uri(text, colour):
    """Build a PNG badge icon with a coloured background and black text."""
    cache_key = (text or "Route", (colour or "#808080").lower())
    cached = _LABEL_ICON_CACHE.get(cache_key)
    if cached is not None:
        return cached

    hex_colour = cache_key[1].lstrip("#")
    if len(hex_colour) != 6:
        hex_colour = "808080"

    red = int(hex_colour[0:2], 16)
    green = int(hex_colour[2:4], 16)
    blue = int(hex_colour[4:6], 16)
    label_text = cache_key[0]
    font = load_label_font()

    measure_image = Image.new("RGBA", (1, 1))
    measure_draw = ImageDraw.Draw(measure_image)
    text_bbox = measure_draw.textbbox((0, 0), label_text, font=font)
    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]

    padding_x = 12
    padding_y = 6
    width = max(52, text_width + padding_x * 2)
    height = max(26, text_height + padding_y * 2)

    image = Image.new("RGBA", (width, height), (red, green, blue, 255))
    draw = ImageDraw.Draw(image)
    draw.text(
        (padding_x - text_bbox[0], padding_y - text_bbox[1]),
        label_text,
        fill=(0, 0, 0, 255),
        font=font,
    )

    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    data_uri = f"data:image/png;base64,{encoded}"
    _LABEL_ICON_CACHE[cache_key] = data_uri
    return data_uri


def route_label_points(label_text, coordinates):
    """Return start and end label positions for a route line."""
    if not coordinates:
        return []

    indices = [0]
    if len(coordinates) > 1:
        indices.append(len(coordinates) - 1)

    labels = []
    for index in indices:
        longitude, latitude = coordinates[index]
        if index == 0 and len(coordinates) > 1:
            suffix = " (start)"
        elif index == len(coordinates) - 1 and len(coordinates) > 1:
            suffix = " (end)"
        else:
            suffix = ""
        labels.append({
            "text": f"{label_text}{suffix}",
            "longitude": longitude,
            "latitude": latitude,
        })
    return labels


def assign_label_offsets(labels, coordinate_precision=5):
    """Spread labels that share the same map position so they remain readable."""
    groups = {}
    for index, label in enumerate(labels):
        key = (
            round(label["longitude"], coordinate_precision),
            round(label["latitude"], coordinate_precision),
        )
        groups.setdefault(key, []).append(index)

    lat_step = 0.00018
    lon_step = 0.00012

    for indices in groups.values():
        if len(indices) <= 1:
            continue

        indices.sort(key=lambda label_index: (
            labels[label_index]["text"],
            labels[label_index].get("placemark_index", 0),
        ))
        count = len(indices)
        for stack_index, label_index in enumerate(indices):
            row = stack_index - ((count - 1) / 2)
            labels[label_index]["longitude"] += row * lon_step * 0.35
            labels[label_index]["latitude"] += row * lat_step


def placemark_labels_with_offsets(placemarks):
    labels = []
    for placemark_index, placemark in enumerate(placemarks):
        label_text = placemark.get("label") or placemark["name"]
        for label_point in route_label_points(label_text, placemark["coordinates"]):
            labels.append({
                **label_point,
                "colour": placemark.get("colour"),
                "placemark_index": placemark_index,
            })

    assign_label_offsets(labels)

    grouped = [[] for _ in placemarks]
    for label in labels:
        grouped[label["placemark_index"]].append(label)
    return grouped


def build_inline_line_style(colour):
    kml_colour = kml_line_color(colour) if colour else None
    if not kml_colour:
        return f"""
        <LineStyle>
          <width>{KML_LINE_WIDTH}</width>
        </LineStyle>"""
    return f"""
        <LineStyle>
          <color>{kml_colour}</color>
          <width>{KML_LINE_WIDTH}</width>
        </LineStyle>"""


def build_line_placemark(name, coordinates, line_style_id=None, colour=None):
    coordinate_text = " ".join(
        f"{longitude:.7f},{latitude:.7f},0"
        for longitude, latitude in coordinates
    )
    safe_name = escape(name or "Route")
    if line_style_id:
        style_block = f"\n        <styleUrl>#{line_style_id}</styleUrl>"
    else:
        style_block = f"""
        <Style>{build_inline_line_style(colour)}
        </Style>"""

    return f"""
      <Placemark>
        <name>{safe_name}</name>{style_block}
        <LineString>
          <tessellate>1</tessellate>
          <coordinates>{coordinate_text}</coordinates>
        </LineString>
      </Placemark>"""


def build_point_label_placemark(text, longitude, latitude, colour=None):
    icon_href = build_label_icon_data_uri(text, colour)

    return f"""
      <Placemark>
        <Style>
          <IconStyle>
            <Icon>
              <href>{escape(icon_href)}</href>
            </Icon>
            <hotSpot x="0.5" y="0.5" xunits="fraction" yunits="fraction"/>
          </IconStyle>
          <LabelStyle>
            <scale>0</scale>
          </LabelStyle>
        </Style>
        <Point>
          <coordinates>{longitude:.7f},{latitude:.7f},0</coordinates>
        </Point>
      </Placemark>"""


def build_route_folder_kml(name, coordinates, label_text=None, colour=None, line_style_id=None, labels=None, show_labels=True):
    safe_name = escape(name or "Route")
    label_text = label_text or name
    line_block = build_line_placemark(
        name,
        coordinates,
        line_style_id=line_style_id,
        colour=colour,
    )
    if not show_labels:
        label_blocks = ""
    else:
        if labels is None:
            labels = route_label_points(label_text, coordinates)

        label_blocks = "".join(
            build_point_label_placemark(
                label["text"],
                label["longitude"],
                label["latitude"],
                colour=label.get("colour", colour),
            )
            for label in labels
        )
    return f"""
    <Folder>
      <name>{safe_name}</name>{line_block}{label_blocks}
    </Folder>"""


def build_document_style_blocks(placemarks):
    style_blocks = []
    line_style_ids = []
    style_id_by_colour = {}

    for placemark in placemarks:
        kml_colour = kml_line_color(placemark.get("colour"))
        if not kml_colour:
            line_style_ids.append(None)
            continue

        if kml_colour not in style_id_by_colour:
            style_id = f"line_{len(style_id_by_colour)}"
            style_id_by_colour[kml_colour] = style_id
            style_blocks.append(f"""
    <Style id="{style_id}">
      <LineStyle>
        <color>{kml_colour}</color>
        <width>{KML_LINE_WIDTH}</width>
      </LineStyle>
    </Style>""")

        line_style_ids.append(style_id_by_colour[kml_colour])

    return "".join(style_blocks), line_style_ids


def write_kml_document(kml_path, document_name, placemarks, show_labels=KML_SHOW_LABELS):
    if not placemarks:
        return False

    if show_labels:
        label_groups = placemark_labels_with_offsets(placemarks)
    else:
        label_groups = [[] for _ in placemarks]

    if len(placemarks) == 1:
        style_blocks = ""
        line_style_ids = [None]
    else:
        style_blocks, line_style_ids = build_document_style_blocks(placemarks)

    placemark_blocks = "".join(
        build_route_folder_kml(
            placemark["name"],
            placemark["coordinates"],
            colour=placemark.get("colour"),
            line_style_id=line_style_id,
            labels=label_group,
            show_labels=show_labels,
        )
        for placemark, line_style_id, label_group in zip(
            placemarks, line_style_ids, label_groups
        )
    )

    safe_document_name = escape(document_name or "Routes")

    kml = f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>{safe_document_name}</name>{style_blocks}{placemark_blocks}
  </Document>
</kml>
"""

    with open(kml_path, "w", encoding="utf-8") as kml_file:
        kml_file.write(kml)

    return True


def extract_route_placemarks(cursor, operator_code, line_name, out_dir=".out", colours=None, line_names=None):
    if not database_has_coordinates(cursor):
        return []

    stop_names = load_stop_common_names(cursor)
    stop_coordinates = load_stop_coordinates(cursor)
    candidates = []
    for route_id, description, section_refs in list_route_definitions(cursor):
        coordinates = collect_route_line_coordinates(
            cursor, section_refs, stop_coordinates=stop_coordinates
        )
        if not coordinates:
            continue

        candidates.append({
            "route_id": route_id,
            "description": description,
            "stops": collect_route_stop_sequence(cursor, section_refs),
            "stop_names": collect_route_stop_name_sequence(
                cursor, section_refs, stop_names=stop_names
            ),
            "coordinates": coordinates,
        })

    if not candidates:
        route = find_longest_listed_route(cursor)
        if route is None:
            return []

        _, description, section_refs = route
        coordinates = collect_route_line_coordinates(
            cursor, section_refs, stop_coordinates=stop_coordinates
        )
        if not coordinates:
            return []

        candidates = [{
            "route_id": None,
            "description": description,
            "stops": collect_route_stop_sequence(cursor, section_refs),
            "stop_names": collect_route_stop_name_sequence(
                cursor, section_refs, stop_names=stop_names
            ),
            "coordinates": coordinates,
        }]

    selected_routes = select_unique_routes(candidates)
    route_label = format_route_label(operator_code, line_name)
    placemarks = []
    for route in selected_routes:
        placemarks.append({
            "name": format_placemark_name(operator_code, line_name, route["description"]),
            "label": route_label,
            "coordinates": route["coordinates"],
            "colour": resolve_route_colour(
                operator_code,
                line_name,
                out_dir,
                colours=colours,
                line_names=line_names,
            ),
        })

    return placemarks


def write_route_kml(cursor, kml_path, operator_code=None, line_name=None, colours=None, out_dir=".out", line_names=None):
    if operator_code is None or line_name is None:
        operator_code, line_name = service_metadata_from_db_path(kml_path.replace(".kml", ".db"))

    placemarks = extract_route_placemarks(
        cursor,
        operator_code,
        line_name,
        out_dir=out_dir,
        colours=colours,
        line_names=line_names,
    )
    if not placemarks:
        return False

    document_name = (
        placemarks[0]["name"]
        if len(placemarks) == 1
        else f"{operator_code}-{line_name}"
    )
    return write_kml_document(kml_path, document_name, placemarks)


def generate_kml_for_database(db_path, out_dir=".out"):
    if not os.path.exists(db_path):
        return None

    operator_code, line_name = service_metadata_from_db_path(db_path)
    colours = load_route_colours(operator_code, out_dir)
    kml_path = os.path.splitext(db_path)[0] + ".kml"

    conn = open_sqlite_database(db_path, readonly=True)
    try:
        placemarks = extract_route_placemarks(
            conn.cursor(), operator_code, line_name, out_dir=out_dir, colours=colours
        )
        if not placemarks:
            return None

        document_name = (
            placemarks[0]["name"]
            if len(placemarks) == 1
            else f"{operator_code} {line_name}"
        )
        write_kml_document(kml_path, document_name, placemarks)
    finally:
        conn.close()

    print(f"  KML: {kml_path} ({len(placemarks)} routes)")
    return placemarks


def iter_service_databases(out_dir=".out"):
    for root, _, filenames in os.walk(out_dir):
        for filename in filenames:
            if not filename.endswith(".db") or filename in SERVICE_DB_SKIP:
                continue
            yield os.path.join(root, filename)


def collect_route_placemarks(out_dir=".out", operator_code=None):
    placemarks = []
    colour_cache = {}
    search_root = os.path.join(out_dir, operator_code) if operator_code else out_dir

    for db_path in sorted(iter_service_databases(search_root)):
        db_operator, line_name = service_metadata_from_db_path(db_path)
        if operator_code and db_operator != operator_code:
            continue

        if db_operator not in colour_cache:
            colour_cache[db_operator] = load_route_colours(db_operator, out_dir)

        conn = open_sqlite_database(db_path, readonly=True)
        try:
            db_placemarks = extract_route_placemarks(
                conn.cursor(),
                db_operator,
                line_name,
                out_dir=out_dir,
                colours=colour_cache[db_operator],
            )
            placemarks.extend(db_placemarks)
        finally:
            conn.close()

    return placemarks


def generate_operator_combined_kml(operator_code, out_dir=".out"):
    placemarks = collect_route_placemarks(out_dir, operator_code=operator_code)
    if not placemarks:
        return False

    operator_dir = os.path.join(out_dir, operator_code)
    kml_path = os.path.join(operator_dir, f"{operator_code}.kml")
    written = write_kml_document(kml_path, f"{operator_code} Routes", placemarks)
    if written:
        print(f"  Combined KML: {kml_path}")
    return written


def generate_dataset_combined_kml(out_dir=".out"):
    placemarks = collect_route_placemarks(out_dir)
    if not placemarks:
        return False

    kml_path = os.path.join(out_dir, "all_routes.kml")
    written = write_kml_document(kml_path, "All Routes", placemarks)
    if written:
        print(f"  Dataset KML: {kml_path}")
    return written


def generate_all_kml(out_dir=".out", show_labels=KML_SHOW_LABELS):
    colour_cache = {}
    placemarks_by_operator = defaultdict(list)
    placemarks_by_region = defaultdict(list)
    route_region_index = build_route_region_index(out_dir=out_dir)
    db_paths = list(iter_service_databases(out_dir))

    for db_path in tqdm(db_paths, desc="Generating KML"):
        operator_code, line_name = service_metadata_from_db_path(db_path)
        if operator_code not in colour_cache:
            colour_cache[operator_code] = load_route_colours(operator_code, out_dir)

        conn = open_sqlite_database(db_path, readonly=True)
        try:
            cursor = conn.cursor()
            placemarks = extract_route_placemarks(
                cursor,
                operator_code,
                line_name,
                out_dir=out_dir,
                colours=colour_cache[operator_code],
            )
            if not placemarks:
                continue

            kml_path = os.path.splitext(db_path)[0] + ".kml"
            document_name = (
                placemarks[0]["name"]
                if len(placemarks) == 1
                else f"{operator_code}-{line_name}"
            )
            write_kml_document(
                kml_path, document_name, placemarks, show_labels=show_labels,
            )
            placemarks_by_operator[operator_code].extend(placemarks)

            region = service_region_for_database(
                db_path, cursor, route_region_index=route_region_index,
            )
            if region:
                placemarks_by_region[region].extend(placemarks)
        finally:
            conn.close()

    for operator_code in sorted(placemarks_by_operator):
        operator_dir = os.path.join(out_dir, operator_code)
        kml_path = os.path.join(operator_dir, f"{operator_code}.kml")
        placemarks = placemarks_by_operator[operator_code]
        if write_kml_document(
            kml_path, f"{operator_code} Routes", placemarks, show_labels=show_labels,
        ):
            print(f"  Combined KML: {kml_path} ({len(placemarks)} routes)")

    regions_dir = os.path.join(out_dir, "regions")
    os.makedirs(regions_dir, exist_ok=True)
    for region_code in sorted(placemarks_by_region):
        placemarks = placemarks_by_region[region_code]
        kml_path = os.path.join(regions_dir, f"{region_code}.kml")
        document_name = f"{region_display_name(region_code)} Routes"
        if write_kml_document(
            kml_path, document_name, placemarks, show_labels=show_labels,
        ):
            print(f"  Region KML: {kml_path} ({len(placemarks)} routes)")

    all_placemarks = [
        placemark
        for placemarks in placemarks_by_operator.values()
        for placemark in placemarks
    ]
    if all_placemarks:
        kml_path = os.path.join(out_dir, "all_routes.kml")
        if write_kml_document(
            kml_path, "All Routes", all_placemarks, show_labels=show_labels,
        ):
            print(f"  Dataset KML: {kml_path} ({len(all_placemarks)} routes)")


def build_operating_profile(vehicle_journey):
    """
    Returns:

    {
        "Monday": True,
        "Tuesday": False,
        "2026-12-25": False,
        "2026-12-26": True
    }
    """

    result = {}

    operating_profile = vehicle_journey.get("OperatingProfile", {})

    regular_days = (
        operating_profile
        .get("RegularDayType", {})
        .get("DaysOfWeek", {})
    )

    all_days = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday"
    ]

    # Default all days to False
    for day in all_days:
        result[day] = False

    #
    # Regular operating days
    #

    if regular_days:
        if "MondayToFriday" in regular_days:
            for d in all_days[:5]:
                result[d] = True

        if "MondayToSaturday" in regular_days:
            for d in all_days[:6]:
                result[d] = True

        if "MondayToSunday" in regular_days:
            for d in all_days:
                result[d] = True

        if "Weekend" in regular_days:
            result["Saturday"] = True
            result["Sunday"] = True

        # Individual days
        for d in all_days:
            if d in regular_days:
                result[d] = True

    #
    # Special days / date ranges
    #

    special_days = (
        operating_profile
        .get("SpecialDaysOperation", {})
    )

    #
    # Days of operation
    #
    if special_days:
        for block in ensure_list(
                special_days.get("DaysOfOperation")
        ):

            ranges = ensure_list(
                block.get("DateRange")
            )

            for r in ranges:
                start = parse_date(r["StartDate"])
                end = parse_date(r["EndDate"])

                for d in daterange(start, end):
                    result[d.isoformat()] = True

        #
        # Days of non-operation
        #

        for block in ensure_list(
                special_days.get("DaysOfNonOperation")
        ):

            ranges = ensure_list(
                block.get("DateRange")
            )

            for r in ranges:
                start = parse_date(r["StartDate"])
                end = parse_date(r["EndDate"])

                for d in daterange(start, end):
                    result[d.isoformat()] = False

    #
    # Bank holidays
    #

    global bank_holidays
    bank_holidays = (
        operating_profile
        .get("BankHolidayOperation", {})
    )
    for mode, value in bank_holidays.items():

        enabled = mode == "DaysOfOperation"

        if not isinstance(value, dict):
            continue

        for holiday_name in value.keys():
            result[f"{holiday_name}"] = enabled

    return result


def provider_from_name(name):
    """ECBU_124 -> ECBU, operators.db -> operators."""
    return name.split("_")[0].split(".")[0]

TNDS_DIR = ".data/TNDS"
TNDS_REGION_LABELS = {
    "NE": "North East",
    "NW": "North West",
    "Y": "Yorkshire",
    "EM": "East Midlands",
    "WM": "West Midlands",
    "EA": "East of England",
    "SE": "South East",
    "SW": "South West",
    "S": "Scotland",
    "WA": "Wales",
    "L": "London",
}


def extract_tnds_region(file_path, txc=None):
    """
    Return the TNDS region code for a source XML file.

    Region is not a dedicated TransXChange element; it comes from the TNDS
    folder layout (``.data/TNDS/NE/...``) and the ServiceCode prefix
    (``NE_04_NEX_YEL_2``).
    """
    parts = os.path.normpath(file_path).split(os.sep)
    if "TNDS" in parts:
        region_index = parts.index("TNDS") + 1
        if region_index < len(parts):
            region = parts[region_index].strip()
            if region:
                return region

    if txc is not None:
        service_code = get_service(txc).get("ServiceCode")
        if service_code:
            region = str(service_code).split("_", 1)[0].strip()
            if region:
                return region

    return None


def region_display_name(region_code):
    if not region_code:
        return "Unknown"
    return TNDS_REGION_LABELS.get(region_code, region_code)


def ensure_service_metadata_table(cursor):
    cursor.execute(
        "create table if not exists ServiceMetadata ("
        "key text primary key, value text)"
    )


def write_service_metadata(cursor, metadata):
    ensure_service_metadata_table(cursor)
    rows = [
        (key, value)
        for key, value in metadata.items()
        if key and value is not None
    ]
    if rows:
        cursor.executemany(
            "insert or replace into ServiceMetadata values (?, ?)",
            rows,
        )


def load_service_metadata(cursor, key):
    table_names = database_table_names(cursor)
    if "ServiceMetadata" not in table_names:
        return None

    row = cursor.execute(
        "SELECT value FROM ServiceMetadata WHERE key = ?",
        (key,),
    ).fetchone()
    if row is None:
        return None
    return row[0]


def build_route_region_index(tnds_dir=TNDS_DIR, out_dir=".out"):
    """Map output route DB paths to TNDS region codes."""
    cache_path = os.path.join(out_dir, "route_region_index.json")
    tnds_mtime = _latest_tnds_mtime(tnds_dir)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, encoding="utf-8") as cache_file:
                cached = json.load(cache_file)
            if cached.get("tnds_mtime") == tnds_mtime:
                return {
                    os.path.abspath(db_path): region
                    for db_path, region in cached.get("regions", {}).items()
                }
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    index = {}
    if not os.path.isdir(tnds_dir):
        return index

    file_index = load_tnds_file_index(out_dir)
    if file_index:
        for file_path, entry in file_index.items():
            db_path = entry.get("db_path")
            if not db_path:
                continue
            region = extract_tnds_region(file_path)
            if region:
                index[os.path.abspath(db_path)] = region
    else:
        db_path_index = {}
        index_updates = {}
        xml_files = []
        for root, _, filenames in os.walk(tnds_dir):
            for filename in filenames:
                if filename.lower().endswith(".xml"):
                    xml_files.append(os.path.join(root, filename))

        for file_path in tqdm(xml_files, desc="Indexing route regions", unit="file"):
            db_path = cached_output_db_path(
                file_path,
                out_dir,
                index=db_path_index,
                index_updates=index_updates,
            )
            region = extract_tnds_region(file_path)
            if not db_path or not region:
                continue
            index[os.path.abspath(db_path)] = region

        if index_updates:
            db_path_index.update(index_updates)
            save_tnds_file_index(db_path_index, out_dir)

    try:
        os.makedirs(out_dir, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as cache_file:
            json.dump(
                {
                    "tnds_mtime": tnds_mtime,
                    "regions": {
                        db_path: region
                        for db_path, region in index.items()
                    },
                },
                cache_file,
                indent=2,
            )
    except OSError:
        pass

    return index


def _latest_tnds_mtime(tnds_dir):
    latest = 0.0
    if not os.path.isdir(tnds_dir):
        return latest

    for root, _, filenames in os.walk(tnds_dir):
        for filename in filenames:
            if not filename.lower().endswith(".xml"):
                continue
            file_path = os.path.join(root, filename)
            try:
                latest = max(latest, os.path.getmtime(file_path))
            except OSError:
                continue
    return latest


def service_region_for_database(db_path, cursor, route_region_index=None):
    region = load_service_metadata(cursor, "region")
    if region:
        return region

    if route_region_index:
        return route_region_index.get(os.path.abspath(db_path))

    return None


def parse_cli_args(argv=None):
    parser = argparse.ArgumentParser(description="Parse TransXChange timetables.")
    parser.add_argument(
        "--no-labels",
        action="store_true",
        help="Omit start/end route badges from generated KML files.",
    )
    parser.add_argument(
        "--labels",
        action="store_true",
        help="Include route badges in KML (default).",
    )
    parser.add_argument(
        "--no-kml",
        action="store_true",
        help="Skip KML generation after parsing.",
    )
    parser.add_argument(
        "--kml-only",
        action="store_true",
        help="Only regenerate KML files from existing databases.",
    )
    parser.add_argument(
        "--no-colours",
        action="store_true",
        help="Skip fetching route colours from bustimes.org.",
    )
    parser.add_argument(
        "--stops-only",
        action="store_true",
        help="Only rebuild the shared stop index from route databases.",
    )
    parser.add_argument(
        "--csv-only",
        action="store_true",
        help="Only export timetable CSV files from existing JSON databases.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce per-file logging while parsing.",
    )
    return parser.parse_args(argv)


def execute(args=None):
    if args is None:
        args = parse_cli_args()

    global PROCESS_QUIET, KML_SHOW_LABELS
    PROCESS_QUIET = args.quiet
    if args.no_labels:
        KML_SHOW_LABELS = False
    elif args.labels:
        KML_SHOW_LABELS = True

    out_dir = ".out"
    if args.csv_only:
        export_all_timetable_csv(out_dir)
        return

    if args.kml_only:
        generate_all_kml(out_dir, show_labels=KML_SHOW_LABELS)
        return

    if args.stops_only:
        rebuild_busstops_index(out_dir, workers=os.cpu_count())
        return

    raw_files = []
    for root, dirs, filenames in os.walk(TNDS_DIR):
        for filename in filenames:
            if filename.lower().endswith(".xml"):
                raw_files.append(os.path.join(root, filename))

    print(f"Found {len(raw_files):,} XML files in {TNDS_DIR}")
    files = select_tnds_files_to_process(raw_files)
    skipped = len(raw_files) - len(files)
    print(
        f"Selected {len(files):,} XML files "
        f"(skipped {skipped:,} duplicate registrations)"
    )

    workers = os.cpu_count()
    stops_db_path = os.path.join(out_dir, "stops.db")
    migrate_legacy_bus_runs(stops_db_path)
    with ProcessPoolExecutor(max_workers=workers) as executor:
        list(
            tqdm(
                executor.map(processFile, files),
                total=len(files),
                desc="Parsing timetables",
            )
        )

    rebuild_busstops_index(out_dir, workers=workers)

    if not args.no_colours:
        fetch_all_operator_route_colours(out_dir, skip_existing=True)
    if not args.no_kml:
        generate_all_kml(out_dir, show_labels=KML_SHOW_LABELS)

    export_all_timetable_csv(out_dir)

def processFile(file):

    if not file.lower().endswith(".xml"):
        return

    file_path = file
    OUT_DIR = ".out"

    process_log(f"Processing {file_path}")

    try:
        WRITE_DEBUG_JSON = False
        with open(file_path, encoding="utf-8") as f:
            data = xmltodict.parse(f.read())

        txc = data.get("TransXChange", {})
        vehicle_journeys = ensure_list(
            txc.get("VehicleJourneys", {}).get("VehicleJourney")
        )
        if not vehicle_journeys:
            process_log(f"  Skipping (no vehicle journeys): {file_path}")
            return
        if WRITE_DEBUG_JSON:
            debug_path = os.path.join(OUT_DIR, "test.json")
            with open(debug_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

        provider = get_operator_code(txc)
        if not provider:
            process_log(f"  Skipping (no operator): {file_path}")
            return
        provider_dir = os.path.join(OUT_DIR, provider)
        os.makedirs(provider_dir, exist_ok=True)
        line_names = get_service_line_names(txc)
        line_label = get_service_line_label(line_names)
        dbName = provider + " " + line_label
        db_path = os.path.join(provider_dir, dbName + ".db")
        json_path = os.path.join(provider_dir, dbName + ".json")
        csv_path = os.path.join(provider_dir, dbName + ".csv")
        if os.path.exists(db_path):
            return
        if not claim_database_build(db_path):
            return

        timetable = {}
        conn = open_sqlite_database(db_path)
        try:
            c = conn.cursor()

            stops = {}
            c.execute("drop table if exists StopPoints")
            c.execute(
                "create table if not exists StopPoints("
                "StopNameRef text(50) primary key, CommonName text(50), "
                "Longitude real, Latitude real)"
            )

            def insert_stop_point(stop_ref, common_name, location):
                if not stop_ref:
                    return
                coordinates = parse_stop_location(location) if location else None
                longitude = latitude = None
                if coordinates is not None:
                    longitude, latitude = coordinates
                stops[stop_ref] = common_name or stop_ref
                c.execute(
                    "insert or replace into StopPoints values (?, ?, ?, ?)",
                    (stop_ref, common_name, longitude, latitude),
                )

            stop_points = txc.get("StopPoints") or {}
            for stop in ensure_list(stop_points.get("AnnotatedStopPointRef")):
                insert_stop_point(
                    stop.get("StopPointRef"),
                    stop.get("CommonName"),
                    None,
                )

            for stop in ensure_list(stop_points.get("StopPoint")):
                descriptor = stop.get("Descriptor") or {}
                location = (stop.get("Place") or {}).get("Location")
                insert_stop_point(
                    stop.get("AtcoCode") or stop.get("StopPointRef"),
                    descriptor.get("CommonName"),
                    location,
                )

            c.execute("drop table if exists JourneyPatterns")
            c.execute(
                "create table JourneyPatterns("
                "SectionId text, ExternalId text(30) primary key, "
                "FromSequenceNumber text(4), FromStopPointRef text(50), "
                "ToSequenceNumber text(4), ToStopPointRef text(50), "
                "RouteLinkRef text(50), Runtime text(15), Distance text(15), "
                "Direction text(15), Waittime text(15))"
            )
            journey_pattern_rows = []
            for section in ensure_list(
                txc.get("JourneyPatternSections", {}).get("JourneyPatternSection")
            ):
                section_id = section["@id"]
                for link in ensure_list(section.get("JourneyPatternTimingLink")):
                    from_point = link.get("From") or {}
                    to_point = link.get("To") or {}
                    from_stop = from_point.get("StopPointRef")
                    to_stop = to_point.get("StopPointRef")
                    if not from_stop or not to_stop:
                        continue

                    journey_pattern_rows.append((
                        section_id,
                        link["@id"],
                        from_point.get("@SequenceNumber"),
                        from_stop,
                        to_point.get("@SequenceNumber"),
                        to_stop,
                        link.get("RouteLinkRef"),
                        splitHMStoS(link.get("RunTime", "PT0S")),
                        normalize_xml_text(link.get("Distance"), 0),
                        None,
                        parse_wait_time(from_point),
                    ))
            if journey_pattern_rows:
                c.executemany(
                    "insert or replace into JourneyPatterns (SectionId, ExternalId, "
                    "FromSequenceNumber, FromStopPointRef, ToSequenceNumber, "
                    "ToStopPointRef, RouteLinkRef, Runtime, Distance, Direction, Waittime) "
                    "values (?,?,?,?,?,?,?,?,?,?,?)",
                    journey_pattern_rows,
                )

            c.execute("drop table if exists RouteLinks")
            c.execute("""create table RouteLinks (
                RouteSectionRef text,
                RouteLinkRef text,
                LinkOrder integer,
                FromStopPointRef text,
                ToStopPointRef text,
                primary key (RouteSectionRef, RouteLinkRef)
            )""")

            c.execute("drop table if exists RouteLinkPoints")
            c.execute("""create table RouteLinkPoints (
                RouteSectionRef text,
                RouteLinkRef text,
                PointSequence integer,
                Longitude real,
                Latitude real,
                primary key (RouteLinkRef, PointSequence)
            )""")

            route_link_rows = []
            route_link_point_rows = []
            route_direction_updates = []
            for section in ensure_list(txc.get("RouteSections", {}).get("RouteSection")):
                section_ref = section["@id"]
                link_order = 0
                for route_link in ensure_list(section.get("RouteLink")):
                    route_link_ref = route_link["@id"]
                    link_order += 1
                    from_stop = route_link.get("From", {}).get("StopPointRef")
                    to_stop = route_link.get("To", {}).get("StopPointRef")
                    route_link_rows.append((
                        section_ref, route_link_ref, link_order, from_stop, to_stop,
                    ))
                    route_direction_updates.append((
                        route_link.get("Direction"),
                        route_link_ref,
                    ))

                    mapping = (route_link.get("Track") or {}).get("Mapping") or {}
                    point_sequence = 0
                    for location in ensure_list(mapping.get("Location", [])):
                        coordinates = parse_location_coordinates(location)
                        if coordinates is None:
                            continue

                        point_sequence += 1
                        route_link_point_rows.append((
                            section_ref,
                            route_link_ref,
                            point_sequence,
                            coordinates[0],
                            coordinates[1],
                        ))

            if route_link_rows:
                c.executemany(
                    "insert or replace into RouteLinks values (?,?,?,?,?)",
                    route_link_rows,
                )
            if route_link_point_rows:
                c.executemany(
                    "insert or replace into RouteLinkPoints values (?,?,?,?,?)",
                    route_link_point_rows,
                )
            for direction, route_link_ref in route_direction_updates:
                if direction is None:
                    continue
                c.execute(
                    "update JourneyPatterns set Direction = ? where RouteLinkRef = ?",
                    (direction, route_link_ref),
                )

            c.execute("drop table if exists Routes")
            c.execute(
                "create table Routes (RouteId text, RouteSectionRef text, "
                "Description text, OrderCol text)"
            )

            route_rows = []
            for section in ensure_list(txc.get("Routes", {}).get("Route")):
                local_id = section["@id"]
                description = section["Description"]
                order_col = 0
                for ref in ensure_list(section.get("RouteSectionRef")):
                    order_col += 1
                    route_rows.append((local_id, ref, description, order_col))
            if route_rows:
                c.executemany(
                    "insert into Routes values (?,?,?,?)",
                    route_rows,
                )

            c.execute("drop table if exists Operations")
            c.execute(
                "create table Operations ("
                "VehicleJourneyCode text, DateOfOperation text, Operating int, "
                "primary key (VehicleJourneyCode, DateOfOperation))"
            )

            c.execute("drop table if exists VehicleJourneys")
            c.execute(
                "create table VehicleJourneys ("
                "VehicleJourneyCode text primary key, JourneyPatternRef text, "
                "OperatorRef text, DepartureTime text)"
            )

            c.execute("drop table if exists Journeys")
            c.execute("""create table Journeys (
                localId text,
                vehicleJourneyCode text,
                journeyPatternRef text,
                JourneyPatternTimingLinkRef text,
                orderCol integer,
                primary key (vehicleJourneyCode, orderCol)
            )""")

            pattern_section_refs, section_links = build_journey_pattern_link_index(data)
            (pattern_route_refs, pattern_directions,
             service_origin, service_destination) = build_journey_pattern_metadata(data)

            operation_rows = []
            journey_rows = []
            vehicle_journey_rows = []
            for journey in vehicle_journeys:
                vehicle_journey_code = journey.get("VehicleJourneyCode")
                journey_pattern_ref = journey.get("JourneyPatternRef")
                if not vehicle_journey_code or not journey_pattern_ref:
                    continue

                operator_ref = journey.get("OperatorRef", "None")
                profile = build_operating_profile(journey)
                for operation, operating in profile.items():
                    operation_rows.append((
                        vehicle_journey_code, operation, int(bool(operating)),
                    ))

                if "VehicleJourneyTimingLink" in journey:
                    order_col = 0
                    for journey_section in ensure_list(journey["VehicleJourneyTimingLink"]):
                        local_id = journey_section.get("@id")
                        journey_pattern_timing_link_ref = journey_section.get(
                            "JourneyPatternTimingLinkRef"
                        )
                        if not journey_pattern_timing_link_ref:
                            continue
                        order_col += 1
                        journey_rows.append((
                            local_id,
                            vehicle_journey_code,
                            journey_pattern_ref,
                            journey_pattern_timing_link_ref,
                            order_col,
                        ))

                departure_time = journey.get("DepartureTime")
                if not departure_time:
                    continue

                vehicle_journey_rows.append((
                    vehicle_journey_code,
                    journey_pattern_ref,
                    operator_ref,
                    departure_time,
                ))

            if operation_rows:
                c.executemany(
                    "insert or replace into Operations values (?,?,?)",
                    operation_rows,
                )
            if journey_rows:
                c.executemany(
                    "insert or replace into Journeys values (?,?,?,?,?)",
                    journey_rows,
                )
            if vehicle_journey_rows:
                c.executemany(
                    "insert or replace into VehicleJourneys values (?,?,?,?)",
                    vehicle_journey_rows,
                )

            conn.commit()
            journey_stops_by_code = {}

            for journey in c.execute("""
                SELECT
                    VehicleJourneyCode,
                    JourneyPatternRef,
                    DepartureTime
                FROM VehicleJourneys
            """).fetchall():

                vehicleJourneyCode = journey[0]
                journeyPatternRef = journey[1]
                departure_seconds = time_to_seconds(journey[2])

                timetable[vehicleJourneyCode] = {}
                current_time = departure_seconds

                timetable[vehicleJourneyCode]["Direction"] = get_journey_direction_description(
                    c, journeyPatternRef, pattern_route_refs, pattern_directions,
                    service_origin, service_destination,
                )

                sections = get_timing_sections(
                    c, vehicleJourneyCode, journeyPatternRef,
                    pattern_section_refs, section_links,
                )

                if not sections:
                    journey_stops_by_code[vehicleJourneyCode] = []
                    continue

                journey_stops_by_code[vehicleJourneyCode] = journey_stop_refs_ordered(sections)

                first_stop_ref = sections[0][1]
                timetable[vehicleJourneyCode][first_stop_ref] = {
                    "Departure": format_time(current_time)
                }

                for section in sections:
                    to_stop_ref = section[2]
                    runtime = int(section[3] or 0)
                    waittime = int(section[4] or 0)

                    current_time += runtime
                    arrival_time = current_time
                    departure_time = arrival_time + waittime

                    timetable[vehicleJourneyCode][to_stop_ref] = {
                        "Arrival": format_time(arrival_time),
                        "Departure": format_time(departure_time)
                    }

                    current_time = departure_time

            write_route_bus_runs(c, journey_stops_by_code)
            service = get_service(txc)
            write_service_metadata(c, {
                "region": extract_tnds_region(file_path, txc=txc),
                "service_code": service.get("ServiceCode"),
                "source_file": os.path.basename(file_path),
            })
            conn.commit()

            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(timetable, f, ensure_ascii=False, indent=4)

            write_timetable_csv(
                csv_path,
                timetable,
                stops,
                provider,
                line_label,
            )

            process_log(f"  Done: {db_path}")
            return dbName, journey_stops_by_code
        except sqlite3.Error as exc:
            print(f"  SQL error: {file_path}")
            print(f"    {type(exc).__name__}: {exc}")
            traceback.print_exc()
            cleanup_failed_database(db_path)
            return
        except Exception:
            print(f"  Failed: {file_path}")
            traceback.print_exc()
            cleanup_failed_database(db_path)
            return
        finally:
            conn.close()
            release_database_build(db_path)

    except sqlite3.Error as exc:
        print(f"  SQL error: {file_path}")
        print(f"    {type(exc).__name__}: {exc}")
        traceback.print_exc()
        return
    except Exception:
        print(f"  Failed: {file_path}")
        traceback.print_exc()
        return

def flatten_bank_holidays(raw_data):
    today = date.today()

    flat = {}

    for region, info in raw_data.items():
        for event in info["events"]:

            key = (
                event["title"]
                .lower()
                .replace(" ", "_")
                .replace("’", "")
                .replace("'", "")
                .replace("-", "_")
            )

            holiday_date = event["date"]

            parsed_date = datetime.datetime.strptime(
                holiday_date,
                "%Y-%m-%d"
            ).date()

            # Ignore holidays already in the past
            if parsed_date < today:
                continue

            if key not in flat:
                flat[key] = holiday_date
                continue

            existing_date = datetime.datetime.strptime(
                flat[key],
                "%Y-%m-%d"
            ).date()

            # Keep whichever upcoming holiday is sooner
            if parsed_date < existing_date:
                flat[key] = holiday_date

    return flat

bank_holidays = requests.get("https://www.gov.uk/bank-holidays.json").json()
bank_holidays = flatten_bank_holidays(bank_holidays)
bank_holidays = dict((v, k) for k, v in bank_holidays.items())

def determineIfRouteRunsOnDate(routeName, vehicleJourneycode, check_date):
    """Return whether this vehicle journey operates on the given calendar date."""
    if isinstance(check_date, datetime.datetime):
        check_date = check_date.date()

    dowct = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }

    day_of_week = dowct[check_date.weekday()]
    date_key = check_date.isoformat()

    conn = open_sqlite_database(service_ref_path(routeName), readonly=True)

    bank_hol = None
    if date_key in bank_holidays:
        bank_hol = re.sub("-", "", bank_holidays[date_key]).lower()

    c = conn.cursor()
    c.execute(
        "select * from operations where VehicleJourneyCode=?",
        (vehicleJourneycode,),
    )
    operating = 0
    for info in c.fetchall():
        if info[1] == day_of_week and info[2] == 1:
            operating = 1

        elif info[1] == date_key and info[2] == 0:
            operating = 0
            break

        elif info[1] == date_key and info[2] == 1:
            operating = 1
            break

        elif bank_hol and info[1].lower() == bank_hol and info[2] == 0:
            operating = 0
            break

        elif bank_hol and info[1].lower() == bank_hol and info[2] == 1:
            operating = 1
            break

        elif bank_hol and info[1] == "AllBankHolidays" and info[2] == 0:
            operating = 0
            break

        elif bank_hol and info[1] == "AllBankHolidays" and info[2] == 1:
            operating = 1
            break

    conn.close()
    return bool(operating)


def determineIfRouteRunsToday(routeName, vehicleJourneycode):
    runs = determineIfRouteRunsOnDate(routeName, vehicleJourneycode, date.today())
    if runs:
        print("This service will run today")
    else:
        print("This service will not run today")
    return runs

def whenDoesThisTripGetToThisStopCode(routeName, vehicleJourneycode, stopCode):
    with open(service_ref_path(routeName, extension=".json"), "r", encoding="utf-8") as file:
        data = json.load(file)

    return data[vehicleJourneycode][str(stopCode)]["Departure"]

if __name__ == "__main__":
    execute(parse_cli_args())

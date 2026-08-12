import os
from collections import defaultdict
from datetime import date, datetime


TNDS_FOLDER = r".data/TNDS"


def parse_tnds_filename(filename):
    """
    Parse standard TNDS filenames such as:
    124_MCGL_609_MCGL_124_20260712_20260626_183438.xml

    Returns (service_key, validity_start) or None when the name does not match.
    """
    if not filename.lower().endswith(".xml"):
        return None

    parts = filename[:-4].split("_")
    if len(parts) < 8:
        return None

    line = parts[0]
    operator_code = parts[1]
    dataset_code = parts[2]
    operator_repeat = parts[3]
    line_repeat = parts[4]
    validity_token = parts[5]

    if line != line_repeat or operator_code != operator_repeat:
        return None

    if len(validity_token) != 8 or not validity_token.isdigit():
        return None

    try:
        validity_start = datetime.strptime(validity_token, "%Y%m%d").date()
    except ValueError:
        return None

    service_key = f"{line}_{operator_code}_{dataset_code}_{operator_repeat}_{line_repeat}"
    return service_key, validity_start


def collect_tnds_files(root_dir):
    grouped = defaultdict(list)

    for root, _, files in os.walk(root_dir):
        for filename in files:
            parsed = parse_tnds_filename(filename)
            if parsed is None:
                continue

            service_key, validity_start = parsed
            grouped[service_key].append(
                (os.path.join(root, filename), validity_start, filename)
            )

    return grouped


def files_not_in_use_yet(grouped_files, today=None):
    """Return paths to delete: future registrations for the same service."""
    today = today or date.today()
    to_delete = []

    for service_key, files in grouped_files.items():
        for path, validity_start, filename in files:
            if validity_start > today:
                to_delete.append((path, filename, service_key, validity_start))

    return to_delete


def clean(root_dir=TNDS_FOLDER, today=None, dry_run=False):
    today = today or date.today()
    grouped_files = collect_tnds_files(root_dir)
    to_delete = files_not_in_use_yet(grouped_files, today=today)

    if not to_delete:
        print(f"No future timetable files found under {root_dir}.")
        return 0

    print(f"Reference date: {today.isoformat()}")
    print(f"Future files to remove: {len(to_delete)}")

    for path, filename, service_key, validity_start in sorted(
        to_delete, key=lambda item: (item[2], item[3], item[1])
    ):
        action = "Would delete" if dry_run else "Deleting"
        print(
            f"{action} {filename} "
            f"(service {service_key}, starts {validity_start.isoformat()})"
        )
        if not dry_run:
            os.remove(path)

    kept = sum(len(files) for files in grouped_files.values()) - len(to_delete)
    print(f"Keeping {kept} file(s), removing {len(to_delete)} future file(s).")
    return len(to_delete)


if __name__ == "__main__":
    clean()

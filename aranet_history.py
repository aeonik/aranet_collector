import setproctitle

setproctitle.setproctitle("aranet_history")

import aranet4
import csv
import os

from bleak.exc import BleakError
from asyncio import TimeoutError


# aranet4 mac address
device_mac = "E3:99:D5:D6:06:CA"

# Selection filter. Will export last 50 records
entry_filter = {"last": 250}

# Fetch results
try:
    # Fetch results
    records = aranet4.client.get_all_records(
        device_mac, entry_filter, remove_empty=True
    )
except BleakError as e:
    print("Aranet: Failed to connect to local device", e)
    exit(1)
except TimeoutError as e:
    print("Operation timed out while connecting to aranet sensor", e)
    exit(1)

# File path
file_path = os.path.expanduser("~/data/house/aranet_history.csv")
file_exists = os.path.isfile(file_path)


# Function to read the last line of a file
def read_last_line(filename):
    with open(filename, "rb") as file:
        file.seek(-2, os.SEEK_END)
        while file.read(1) != b"\n":
            file.seek(-2, os.SEEK_CUR)
        return file.readline().decode()


# Get the last record from the file, if it exists
last_record = read_last_line(file_path) if file_exists else None


# Function to convert record to CSV row format
def record_to_row(record):
    return [
        record.date.isoformat(),
        str(record.co2),
        str(record.temperature),
        str(record.humidity),
        str(record.pressure),
    ]


# Find the index in records where the last file record matches
def find_matching_index(last_record, records):
    if last_record:
        for index, record in enumerate(records):
            csv_record = ",".join(record_to_row(record))
            last_record_stripped = last_record.strip()
            if csv_record == last_record_stripped:
                return index
    return None


matching_index = (
    find_matching_index(last_record, records.value) if last_record else None
)

# Write or append to CSV file
with open(file_path, "a+" if file_exists else "w", newline="") as file:
    writer = csv.writer(file)

    # Write header only if the file did not exist or was empty
    if not file_exists:
        header = ["date", "co2", "temperature", "humidity", "pressure"]
        writer.writerow(header)

    # Determine starting point for writing new records
    start_index = matching_index + 1 if matching_index is not None else 0

    # Write CSV rows for new records
    for line in records.value[start_index:]:
        writer.writerow(record_to_row(line))

    # Log if unable to find matching record
    if matching_index is None and last_record:
        print(
            "Warning: Unable to find matching record in the file. Writing all records."
        )

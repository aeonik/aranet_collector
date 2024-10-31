import setproctitle
import aranet4
import csv
import os
import logging
from bleak.exc import BleakError
from asyncio import TimeoutError

# Set process title
setproctitle.setproctitle("aranet_history")

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s - %(message)s")

# Device MAC address
device_mac = "E3:99:D5:D6:06:CA"

# Selection filter - last 250 records
entry_filter = {"last": 250}

# File path
file_path = os.path.expanduser("~/data/house/aranet_history.csv")


def fetch_records(device_mac, entry_filter):
    """Fetch records from the Aranet device."""
    try:
        records = aranet4.client.get_all_records(
            device_mac, entry_filter, remove_empty=True
        )
        logging.info(f"Fetched {len(records.value)} records from the device.")
        return records
    except BleakError as e:
        logging.error("Aranet: Failed to connect to local device")
        exit(1)
    except TimeoutError as e:
        logging.error(
            "Operation timed out while connecting to Aranet sensor")
        exit(1)
    except Exception as e:
        logging.error("Unexpected error occurred")
        exit(1)


def read_last_line(filename):
    """Read the last line of a file."""
    with open(filename, "rb") as file:
        file.seek(-2, os.SEEK_END)
        while file.read(1) != b"\n":
            file.seek(-2, os.SEEK_CUR)
        return file.readline().decode()


def record_to_row(record):
    """Convert record to CSV row format."""
    return [
        record.date.isoformat(),
        str(record.co2),
        str(record.temperature),
        str(record.humidity),
        str(record.pressure),
    ]


def find_matching_index(last_record, records):
    """Find the index in records where the last file record matches."""
    if last_record:
        for index, record in enumerate(records):
            csv_record = ",".join(record_to_row(record))
            last_record_stripped = last_record.strip()
            if csv_record == last_record_stripped:
                logging.info(f"Found matching record at index {index}.")
                return index
    logging.warning("No matching record found.")
    return None


def write_records_to_csv(file_path, records, last_record):
    """Write or append records to a CSV file."""
    file_exists = os.path.isfile(file_path)
    matching_index = (
        find_matching_index(last_record, records.value) if last_record else None
    )

    with open(file_path, "a+" if file_exists else "w", newline="") as file:
        writer = csv.writer(file)

        if not file_exists:
            header = ["date", "co2", "temperature", "humidity", "pressure"]
            writer.writerow(header)
            logging.info("CSV header written.")

        start_index = matching_index + 1 if matching_index is not None else 0
        records_written = 0
        for line in records.value[start_index:]:
            writer.writerow(record_to_row(line))
            records_written += 1

        logging.info(f"{records_written} records written to the CSV file.")

        if matching_index is None and last_record:
            logging.warning(
                "Unable to find matching record in the file. Writing all records."
            )


def main():
    records = fetch_records(device_mac, entry_filter)
    last_record = read_last_line(file_path) if os.path.isfile(file_path) else None
    write_records_to_csv(file_path, records, last_record)


if __name__ == "__main__":
    main()

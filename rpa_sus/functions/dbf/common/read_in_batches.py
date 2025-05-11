import logging
from dbfread import DBF


def read_in_batches(dbf_file_path, batch_size):
    """Read the DBF file in batches and count the records."""
    record_count = 0  # Initialize a counter for the records processed
    try:
        # Open the DBF file and pass the file path to DBF
        reader = DBF(dbf_file_path)  # Pass the file path directly to DBF
        data_batch = []
        for record in reader:
            data_batch.append(record)
            record_count += 1  # Increment the counter for each record processed
            if len(data_batch) == batch_size:
                yield data_batch
                data_batch = []

        # If there are any remaining records in the final batch, yield them
        if data_batch:
            yield data_batch

        # Log the total number of records processed
        logging.info(
            f"Total records processed from {dbf_file_path}: {record_count}")

    except Exception as e:
        logging.error(
            f"ERROR - Failed to read DBF file {dbf_file_path}: {str(e)}")
        raise

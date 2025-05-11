def clean_column_data(record):
    """Clean and preprocess the columns in the record before manipulation."""
    cleaned_record = {}

    for key, value in record.items():
        if value is None:
            # Or assign a default value like "" or 0 if preferred
            cleaned_record[key] = None
            continue

        # Ensure all values are strings, trimming whitespaces
        if isinstance(value, str):
            cleaned_value = value.strip()  # Remove leading/trailing whitespaces
        else:
            cleaned_value = value

        # Add further transformations if needed, for example:
        # - Clean date format, currency formatting, etc.
        # - Normalize text data, e.g., make all text lowercase if required
        # - Remove unwanted characters or standardize codes

        cleaned_record[key] = cleaned_value

    return cleaned_record

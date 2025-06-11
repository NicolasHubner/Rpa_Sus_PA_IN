## I need to read a codigo.db file of a sqlite database and i want to see the first 100 records on a print statement.

# Here is the code I have tried so far:

import sqlite3
import os

def read_db_and_print_records(db_file_path, num_records=100):
    # Check if the file exists
    if not os.path.exists(db_file_path):
        print(f"Error: Database file '{db_file_path}' not found.")
        return None
    
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        
        # Read first 100 records - note the space between LIMIT and ?
        cursor.execute(f"SELECT code, normalized_tax, state_code FROM codes WHERE code = '004321' AND range = '9407-9910' LIMIT ?", (num_records,))
        records = cursor.fetchall()
        
        # Get column names
        column_names = [description[0] for description in cursor.description]
        print("Columns:", column_names)
        print(f"Found {len(records)} records")
        print("-" * 100)
        
        # Print the records
        for i, record in enumerate(records, 1):
            print(f"Record {i}:")
            print(record)
            print("-" * 100)
        
        conn.close()
        return records
        
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None

# Provide the correct path to your database file
# Using both relative and absolute path options
current_dir = os.path.dirname(os.path.abspath(__file__))
db_relative_path = "./codigos.db"
db_absolute_path = os.path.join(current_dir, "codigos.db")

print(f"Trying relative path: {db_relative_path}")
records = read_db_and_print_records(db_relative_path)

if records is None:
    print(f"Trying absolute path: {db_absolute_path}")
    records = read_db_and_print_records(db_absolute_path)

# The problem is that it's not reading the first 100 records from the database.

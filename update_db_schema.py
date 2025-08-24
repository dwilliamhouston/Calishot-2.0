#!/usr/bin/env python3
"""
Script to update the sites database schema by adding missing columns.
Run this script from the same directory as your calishot.py script.
"""

import sqlite3
from pathlib import Path
import sys

def update_database_schema(db_path):
    """
    Update the sites database schema by adding any missing columns.
    
    Args:
        db_path (str): Path to the SQLite database file
    """
    # Columns to add if they don't exist
    columns_to_add = [
        ("error_message", "TEXT"),
        ("failed_attempts", "INTEGER DEFAULT 0"),
        ("last_failed", "TEXT"),
        ("last_success", "TEXT"),
        ("last_scrape", "TEXT"),
        ("scrapes", "INTEGER DEFAULT 0"),
        ("downloads", "INTEGER DEFAULT 0"),
        ("last_download", "TEXT"),
        # Ensure demeter_id and active exist for Demeter host management
        ("demeter_id", "INTEGER UNIQUE"),
        ("active", "INTEGER DEFAULT 0")
    ]
    
    # Change error column type from INTEGER to TEXT if needed
    alter_error_column = """
    PRAGMA foreign_keys=off;
    BEGIN TRANSACTION;
    ALTER TABLE sites RENAME TO _sites_old;
    CREATE TABLE sites (
        uuid TEXT PRIMARY KEY,
        url TEXT,
        hostnames TEXT,
        ports TEXT,
        country TEXT,
        isp TEXT,
        status TEXT,
        last_online TEXT,
        last_check TEXT,
        error TEXT,
        error_message TEXT,
        book_count INTEGER,
        last_book_count INTEGER,
        new_books INTEGER,
        libraries_count INTEGER,
        failed_attempts INTEGER DEFAULT 0,
        last_failed TEXT,
        last_success TEXT,
        last_scrape TEXT,
        scrapes INTEGER DEFAULT 0,
        downloads INTEGER DEFAULT 0,
        last_download TEXT,
        demeter_id INTEGER UNIQUE,
        active INTEGER DEFAULT 0
    );
    INSERT INTO sites (
        uuid, url, hostnames, ports, country, isp, status,
        last_online, last_check, error, error_message, book_count,
        last_book_count, new_books, libraries_count, failed_attempts, last_failed, last_success, last_scrape, scrapes, downloads, last_download, demeter_id, active
    ) SELECT 
        uuid, url, hostnames, ports, country, isp, status,
        last_online, last_check, error, error_message, book_count,
        last_book_count, new_books, libraries_count, failed_attempts, last_failed, last_success, last_scrape, scrapes, downloads, last_download,
        demeter_id, COALESCE(active, 0)
    FROM _sites_old;
    DROP TABLE _sites_old;
    COMMIT;
    PRAGMA foreign_keys=on;
    """
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get the current columns in the sites table
        cursor.execute("PRAGMA table_info(sites)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        
        # Check if we need to alter the error column type
        cursor.execute("PRAGMA table_info(sites)")
        error_col_info = [row for row in cursor.fetchall() if row[1] == 'error']
        need_error_col_update = error_col_info and error_col_info[0][2].upper() != 'TEXT'
        
        if need_error_col_update:
            print("Converting 'error' column from INTEGER to TEXT type...")
            cursor.executescript(alter_error_column)
            print("Successfully converted 'error' column to TEXT type")
        
        # Add any missing columns
        for column, col_type in columns_to_add:
            if column not in existing_columns:
                print(f"Adding missing column: {column} ({col_type})")
                cursor.execute(f"ALTER TABLE sites ADD COLUMN {column} {col_type}")
        
        # Commit changes
        conn.commit()
        print("Database schema updated successfully!")
        
    except sqlite3.Error as e:
        print(f"Error updating database: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
    
    return True

def main():
    # Default database path relative to the script location
    default_db_path = Path(__file__).parent / "data" / "sites.db"
    
    # Use command line argument if provided, otherwise use default
    if len(sys.argv) > 1:
        db_path = Path(sys.argv[1])
    else:
        db_path = default_db_path
    
    print(f"Updating database at: {db_path}")
    
    if not db_path.exists():
        print(f"Error: Database file not found at {db_path}")
        print("Please provide the correct path to your sites.db file as an argument.")
        sys.exit(1)
    
    success = update_database_schema(str(db_path))
    if success:
        print("Database update completed successfully!")
    else:
        print("Failed to update database.")
        sys.exit(1)

if __name__ == "__main__":
    main()

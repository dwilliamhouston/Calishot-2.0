import requests
from pathlib import Path
from urllib.parse import *
import uuid
import datetime
import gevent
from gevent import monkey
from gevent import Timeout
from gevent.pool import Pool
import ipaddress
import pandas as pd
from sqlite_utils import Database
import sqlite3
import sys
import shodan
import os
import time
import re
import shutil
from typing import Dict
import json
from humanize import naturalsize as hsize
import humanize
from langid.langid import LanguageIdentifier, model
import iso639
import time
import unidecode
from requests.adapters import HTTPAdapter
import urllib3
import logging
import configparser
import calishot_logging

# Configure project-wide logging and pin to project CWD
calishot_logging.init_logging(logging.INFO, log_file=Path.cwd() / 'calishot.log')

# Initialize Shodan API
def get_shodan_api_key():
    config = configparser.ConfigParser()
    config_file = Path('config.ini')
    
    if not config_file.exists():
        raise FileNotFoundError("config.ini not found. Please create it with your Shodan API key.")
    
    config.read(config_file)
    
    if 'shodan' not in config or 'api_key' not in config['shodan']:
        raise ValueError("Shodan API key not found in config.ini. Please add it under the [shodan] section.")
    
    api_key = config['shodan']['api_key'].strip()
    if not api_key or api_key == 'your_shodan_api_key_here':
        raise ValueError("Please set your Shodan API key in config.ini")
    
    return api_key

try:
    api = shodan.Shodan(get_shodan_api_key())
except Exception as e:
    logging.error(f"Failed to initialize Shodan API: {str(e)}")
    print(f"Error: {str(e)}")
    print("Please check your config.ini file and ensure it contains a valid Shodan API key.")
    sys.exit(1)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)

global site_conn
data_dir = "./data/"
site_conn = sqlite3.connect(data_dir + "sites.db")
site_cursor = site_conn.cursor()
########################
# Setup Sites Database #
########################
def init_sites_db(dir=data_dir):
    """
    Initializes the sites database.

    Parameters:
    - dir (str): The directory path where the database file will be created. Default is the current directory.

    Returns:
    - db (Database): The initialized database object.
    """
    print("Initializing sites database in directory:", dir)
    logging.info("****Setup Sites Database Function****")
    logging.info("Directory: %s", dir)
    
    # Create the directory if it doesn't exist
    Path(dir).mkdir(parents=True, exist_ok=True)
    
    path = Path(dir) / "sites.db"
    print("Database path:", path) 
    logging.info("Database path: %s", path)
    
    # Initialize the database
    db = Database(path)
    
    # Define the schema for the sites table
    sites_schema = {
        "uuid": str,
        "url": str,
        "hostnames": str,
        "ports": str,
        "country": str,  # Ensure country is in the schema
        "isp": str,
        "status": str,
        "last_online": str,
        "last_check": str,
        "error": int,
        # "schema_version": 1
        # TODO: add the most common formats
    }
    # Example usage (kept for reference):
    # db["sites"].create(sites_schema, pk="uuid")

    # if not "sites" in db.table_names():
    #     db["sites"].create({
    #     "uuid": str
    #     }, pk="uuid",)

    db.table("sites", pk='uuid', batch_size=100, alter=True)
    return db

##############################
# Migration: demeter_id fill #
##############################
def migrate_demeter_ids(db: Database) -> int:
    """
    Backfill sequential demeter_id values for rows where demeter_id is NULL or 0.
    Starts from current MAX(demeter_id) + 1. Returns number of rows updated.
    """
    logging.info("****migrate_demeter_ids****")
    try:
        cur = db.conn.cursor()
        row = cur.execute("SELECT COALESCE(MAX(demeter_id), 0) FROM sites").fetchone()
        start_id = (row[0] or 0) + 1

        # Order rows for stable assignment: non-null last_success/check first, then by url
        rows = list(
            cur.execute(
                """
                SELECT uuid
                FROM sites
                WHERE demeter_id IS NULL OR demeter_id = 0
                ORDER BY (last_success IS NULL), last_success,
                         (last_check IS NULL), last_check,
                         url
                """
            )
        )

        next_id = start_id
        for (uuid_val,) in rows:
            cur.execute("UPDATE sites SET demeter_id = ? WHERE uuid = ?", (next_id, uuid_val))
            next_id += 1

        db.conn.commit()
        updated = len(rows)
        logging.info(f"migrate_demeter_ids updated {updated} rows (start_id={start_id})")
        return updated
    except Exception as e:
        logging.error("migrate_demeter_ids failed: %s", str(e), exc_info=True)
        raise

########################################
# Save sites found into Sites Database #
########################################
def save_site(db: Database, site):
    """
    Saves a site to the database.

    Parameters:
    - db (Database): The database object to save the site to.
    - site (dict): The site to be saved. Should include 'url' and may include 'country'.

    This function saves a site to the specified database. If the site does not have a 'uuid' key, 
    a new UUID will be generated and assigned to the site before saving it. The site is saved using 
    the 'upsert' method of the database object, with the primary key set to 'uuid'.

    Returns:
    - None
    """
    logging.info("****Save Site Function****")


    # # TODO: Check if the site is not alreday present
    # def save_sites(db, sites):
    #     db["sites"].insert_all(sites, alter=True,  batch_size=100)
    if not 'uuid' in site: 
        site['uuid']=str(uuid.uuid4())    
    print("Site: ",site)
    logging.info("Site: %s", site)
    db["sites"].upsert(site, pk='uuid')


##########################
# Validate Site and save #
##########################
def check_and_save_site(db, site):
        """
        Check and save a site.

        Args:
            db (database): The database object.
            site (str): The site to be checked and saved.

        Returns:
            bool: True if the site was processed successfully, False if it was deleted
        """
        logging.info("****Check and Save Function****")

        res = check_calibre_site(site)
        
        # If check_calibre_site returns None, the site was deleted due to too many failures
        if res is None:
            logging.info("Site %s was deleted due to too many failures", site.get('url', 'unknown'))
            return False
            
        print(res)
        logging.info("Result: %s", res)
        
        # Ensure we have the site's UUID for updating
        if 'uuid' not in res and 'url' in res:
            # Try to find the site by URL if UUID is not available
            site_record = db["sites"].get_where(f"url = '{res['url']}'").first()
            if site_record:
                res['uuid'] = site_record['uuid']
        
        # Save the site with updated information
        save_site(db, res)
        
        # Verify the data was saved correctly
        if 'uuid' in res:
            saved_site = db["sites"].get(res['uuid'])
            logging.info("Saved site data: %s", saved_site)
            
        return True

# import pysnooper
# @pysnooper.snoop()

######################
# Check Calibre Site #
######################
def check_calibre_site(site):
    """
    Check the calibre site.

    :param site: A dictionary containing information about the site.
                 It should have the following keys:
                 - "uuid" (str): The UUID of the site.
                 - "url" (str): The URL of the site.
    :return: A dictionary containing the result of the check.
             It has the following keys:
             - "uuid" (str): The UUID of the site.
             - "last_check" (str): The timestamp of the last check.
             - "status" (str): The status of the site, which can be "unauthorized", "down", "online", or "error".
             - "last_online" (str): The timestamp of the last online status if the site is online.
             - "error" (str): The error message if there is an error.
             - "failed_attempts" (int): The number of consecutive failed attempts.
             - "last_failed" (str): The timestamp of the last failure.
    """
    
    logging.info("****Check Calibre Site Function****")
    now = str(datetime.datetime.now())
    
    # Normalize site to a dict (sqlite_utils Row can be dict-like but not a real dict)
    try:
        site = dict(site) if not isinstance(site, dict) else site
    except Exception:
        # Fall back to an empty dict if conversion fails
        site = {}  

    # Get current failed_attempts from site or default to 0
    failed_attempts = site.get('failed_attempts', 0) if isinstance(site, dict) else 0
    
    # Initialize return values with current site data
    ret = {
        'uuid': site.get('uuid'),
        'last_check': now,
        'status': 'unknown',
        'last_online': site.get('last_online'),
        'last_success': site.get('last_success'),
        'error': None,
        'error_message': None,
        'failed_attempts': failed_attempts
    }
    
    # Check if we should delete the site due to too many failed attempts
    if failed_attempts >= 5:
        print(f"Deleting site {site.get('url')} due to {failed_attempts} failed attempts")
        logging.info(f"Deleting site {site.get('url')} due to {failed_attempts} failed attempts")
        
        try:
            # Delete the site from the database
            if 'uuid' in site:
                db = init_sites_db()
                db["sites"].delete(site['uuid'])
                print(f"Successfully deleted site {site.get('url')}")
                logging.info(f"Successfully deleted site {site.get('url')}")
        except Exception as e:
            error_msg = f"Error deleting site {site.get('url')}: {str(e)}"
            print(error_msg)
            logging.error(error_msg, exc_info=True)
        
        # Return None to indicate the site should be removed from any processing
        return None

    # Validate URL early to avoid TypeError during concatenation
    url_value = site.get('url')
    if not isinstance(url_value, str) or not url_value.strip():
        error_msg = f"Invalid site url: {url_value!r}"
        print(error_msg)
        logging.error(error_msg)
        ret.update({
            'status': 'error',
            'error': error_msg,
            'failed_attempts': failed_attempts + 1,
            'last_failed': now,
            'error_message': error_msg
        })
        return ret

    print('URL = ', url_value)
    
    # If we've had 5 or more failed attempts, we should have already deleted the site
    # This is just a safety check in case we somehow get here
    if failed_attempts >= 5:
        return None
        
    api = url_value.rstrip('/') + '/ajax/'
    timeout = 15
    library = ""
    url = api + 'search' + library + '?num=0'
    print()
    print("Getting ebooks count:", site['url'])
    logging.info("Getting ebooks count: %s", site['url'])
    print(url)
    logging.info("URL: %s", url)
    
    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        status_code = getattr(e.response, 'status_code', 0) if 'e' in locals() else 0
        error_msg = f"HTTP error {status_code}"
        logging.error(error_msg)
        
        # Increment failed attempts and update return values
        failed_attempts += 1
        ret.update({
            'error': error_msg,
            'status': 'unauthorized' if status_code == 401 else 'down',
            'failed_attempts': failed_attempts,
            'last_failed': now,
            'error_message': str(e)
        })
        return ret
        
    except requests.RequestException as e:
        error_msg = f"Request failed: {str(e)}"
        print(f"Unable to open site: {url}")
        logging.error(error_msg)
        
        # Update failed attempts and last failed time
        ret.update({
            'status': 'down',
            'error': error_msg,
            'failed_attempts': failed_attempts + 1,  # Increment failed attempts
            'last_failed': now,
            'error_message': str(e)
        })
        return ret
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"Error checking site {url}: {error_msg}")
        logging.error(error_msg, exc_info=True)
        
        # Increment failed attempts and update return values
        failed_attempts += 1
        ret.update({
            'status': 'error',
            'error': error_msg,
            'failed_attempts': failed_attempts,
            'last_failed': now,
            'error_message': str(e)
        })
        return ret

    try: 
        # Parse the response to get total books
        total_books = int(r.json()["total_num"])
        print("Total count=", total_books)
        logging.info("Total count: %s", total_books)
        
        # Get the current book count from the site
        current_book_count = total_books
        
        # Get the previous state
        previous_book_count = site.get('book_count', 0)
        previous_last_book_count = site.get('last_book_count', 0)
        
        # Calculate new books based on the difference between current and last total count
        # Always calculate new_books, even if previous_book_count is 0
        new_books = max(0, current_book_count - previous_book_count)
        last_book_count = previous_book_count if previous_book_count > 0 else 0
        
        # Log the calculation for debugging
        debug_info = (
            f"Book count update for {site.get('url')}: "
            f"Current: {current_book_count}, "
            f"Previous: {previous_book_count}, "
            f"Last: {last_book_count}, "
            f"New books: {new_books}"
        )
        
        # Special case for first run or reset
        if previous_book_count == 0 and previous_last_book_count == 0:
            # First time seeing this site, initialize counts
            new_books = 0
            last_book_count = current_book_count
            debug_info += " (First run, initializing counts)"
        
        logging.info(debug_info)
        
        # Log a warning if we detect a change but new_books is 0
        if current_book_count != previous_book_count and new_books == 0 and previous_book_count > 0:
            logging.warning(
                f"Unexpected state for {site.get('url')}: "
                f"Count changed ({previous_book_count} -> {current_book_count}) "
                f"but new_books is 0"
            )
        
        # Update return values for successful connection
        ret.update({
            'book_count': current_book_count,
            'last_book_count': last_book_count,  # Store the previous total book count
            'new_books': new_books,
            'status': 'online',
            'last_online': now,
            'last_success': now,
            'error': None,
            'error_message': None
        })
        
        logging.info(f"Book counts - Current: {current_book_count}, Previous: {previous_book_count}, Last: {last_book_count}, New: {new_books}")
        print(f"Updated book counts - Current: {current_book_count}, Previous: {previous_book_count}, Last: {last_book_count}, New: {new_books}")
        
        print(f"Book count: {total_books}, Last book count: {last_book_count}, New books: {new_books}")
        logging.info(f"Book count: {total_books}, Last book count: {last_book_count}, New books: {new_books}")
        
        # Reset failed attempts counter on success
        if failed_attempts > 0:
            ret['failed_attempts'] = 0
            logging.info(f"Reset failed_attempts counter for {site['url']}")
        
        # Get library count and calculate total books when site is online
        try:
            libraries = get_libs_from_site(site['url'])
            libraries_count = len(libraries)
            ret['libraries_count'] = libraries_count
            print(f"Found {libraries_count} libraries at {site['url']}")
            logging.info(f"Found {libraries_count} libraries at {site['url']}")
            
            # If there are multiple libraries, compute total strictly from per-library counts
            if libraries_count > 1:
                print(f"Calculating total books across {libraries_count} libraries...")
                logging.info(f"Calculating total books across {libraries_count} libraries")

                total_books = 0
                for library in libraries:
                    try:
                        if not library:
                            continue

                        lib_url = f"{api}search/{library}?num=0"
                        lib_r = requests.get(lib_url, verify=False, timeout=(timeout, 30))
                        lib_r.raise_for_status()
                        lib_count = int(lib_r.json().get("total_num", 0))
                        total_books += lib_count
                        print(f"Library '{library}': {lib_count} books")
                        logging.info(f"Library '{library}': {lib_count} books")
                        # Update per-library tracking in sites.db
                        try:
                            upsert_library_count(site['url'], library, lib_count)
                        except Exception as uple:
                            logging.warning("Failed upserting per-library count for %s/%s: %s", site['url'], library, uple)

                    except Exception as lib_e:
                        error_msg = f"Error getting book count for library '{library}': {str(lib_e)}"
                        print(f"Warning: {error_msg}")
                        logging.warning(error_msg, exc_info=True)

                # Update the total book count in the return value from summed libraries
                ret['book_count'] = total_books
                print(f"Total books across all libraries: {total_books}")
                logging.info(f"Total books across all libraries: {total_books}")
            else:
                # Single library: set book_count from that library and upsert
                for library in libraries:
                    try:
                        if not library:
                            continue
                        lib_url = f"{api}search/{library}?num=0"
                        lib_r = requests.get(lib_url, verify=False, timeout=(timeout, 30))
                        lib_r.raise_for_status()
                        lib_count = int(lib_r.json().get("total_num", 0))
                        ret['book_count'] = lib_count
                        try:
                            upsert_library_count(site['url'], library, lib_count)
                        except Exception as uple:
                            logging.warning("Failed upserting per-library count for %s/%s: %s", site['url'], library, uple)
                    except Exception as lib_e:
                        logging.warning("Error getting book count for library '%s': %s", library, lib_e, exc_info=True)
            
            # If no libraries found, mark status as 'unknown' but keep error fields as None
            if libraries_count == 0:
                print(f"No libraries found at {site['url']}, marking as 'unknown'")
                logging.warning(f"No libraries found at {site['url']}, marking as 'unknown'")
                ret.update({
                    'status': 'unknown',
                    'book_count': 0,
                    'error': None,
                    'error_message': None
                })
                
        except Exception as lib_e:
            error_msg = f"Error getting libraries for {site['url']}: {lib_e}"
            print(f"Warning: {error_msg}")
            logging.warning(error_msg, exc_info=True)
            ret.update({
                'libraries_count': 0,
                'status': 'online',  # Still consider it online even if library check fails
                # Don't set error/error_message here to maintain clean online status
            })
            
        return ret
        
    except Exception as e:
        # Handle errors during response processing
        error_msg = f"Error processing response from {site['url']}: {str(e)}"
        print(f"Error: {error_msg}")
        logging.error(error_msg, exc_info=True)
        
        # Increment failed attempts and update return values
        failed_attempts += 1
        ret.update({
            'book_count': 0,
            'libraries_count': 0,
            'status': 'error',
            'error': error_msg,
            'failed_attempts': failed_attempts,
            'last_failed': now,
            'error_message': str(e)
        })
        return ret

######################
# Get UUID from Site #
######################
def get_site_uuid_from_url(db, url):
    """
    Retrieve the site UUID from a given URL.

    Args:
        db (Database): The database connection.
        url (str): The URL to extract the site UUID from.

    Returns:
        tuple or None: The row from the 'sites' table if a match is found, None otherwise.
    """
    logging.info("****Get Site UUID from url Function****")
    site=urlparse(url)
    hostname=site.hostname
    site=site._replace(path='')
    
    url=urlunparse(site)
    # print (url)

    # print (hostname)
    row=db.conn.execute(f"select * from sites where instr(hostnames, '{hostname}')").fetchone()
    # print(row)
    if row:
        return row

##############################
# Get URL, hostname and Port #
##############################
def map_site_from_url(url, country=None):
    """
    Generates a site map from a given URL.

    Args:
        url (str): The URL to generate the site map from.
        country (str, optional): The country code for the site. Defaults to None.
    Returns:
        dict: A dictionary containing the generated site map. The dictionary has the following keys:
            - 'url' (str): The modified URL with the path removed.
            - 'hostnames' (list): A list containing the hostname extracted from the URL.
            - 'ports' (list): A list containing the port number extracted from the URL as a string.
            - 'country' (str): The country code for the site.
    """
    logging.info("****Map Site from URL Function****")
    ret = {}
    print('*******')
    print('Processing URL:', url)
    print('*******')
    
    try:
        # Remove any whitespace and newlines
        url = url.strip()
        
        # Basic URL validation
        if not url or not url.startswith(('http://', 'https://')):
            print(f"Invalid URL (must start with http:// or https://): {url}")
            logging.warning(f"Invalid URL format: {url}")
            return ret
            
        # Parse the URL
        site = urlparse(url)
        if not site.hostname:
            print(f"Could not extract hostname from URL: {url}")
            logging.warning(f"Could not extract hostname from URL: {url}")
            return ret
            
        # Remove path and rebuild URL
        site = site._replace(path='', params='', query='', fragment='')
        clean_url = urlunparse(site)
        
        # Prepare the return dictionary
        ret = {
            'url': clean_url,
            'hostnames': [site.hostname],
            'ports': [str(site.port) if site.port else '80' if site.scheme == 'http' else '443'],
            'country': country if country else ''
        }
            
        print(f"Successfully processed URL: {url} -> {clean_url}")
        logging.info(f"Mapped URL {url} to {ret}")
        return ret
        
    except Exception as e:
        print(f"Error processing URL {url}: {str(e)}")
        logging.error(f"Error processing URL {url}: {str(e)}", exc_info=True)
        return ret

############################################################
# Update site status with failure tracking #
###########################################
def update_site_status(db, url, status, error_message=None, failed_attempts=None, last_failed=None, last_success=None, book_count=None):
    """
    Update a site's status in the database with failure tracking.
    
    Args:
        db: Database connection
        url (str): The site URL to update
        status (str): New status ('online', 'offline', etc.)
        error_message (str, optional): Error message if any
        failed_attempts (int, optional): Number of consecutive failed attempts
        last_failed (str, optional): ISO format timestamp of last failure
        last_success (str, optional): ISO format timestamp of last success
        book_count (int, optional): Number of books in the library
    """
    try:
        # Get current time for the update
        current_time = datetime.datetime.utcnow().isoformat()
        
        # Get the existing record
        site_record = db["sites"].get_where(f"url = '{url}'").first()
        
        if not site_record:
            print(f"Warning: Site {url} not found in database")
            return
            
        # Prepare update data
        update_data = {
            'status': status,
            'last_check': current_time,
            'error': None if status == 'online' else (error_message or '')
        }
        
        # Update book count if provided
        if book_count is not None:
            update_data['book_count'] = book_count
            print(f"Updating book count to {book_count} for {url}")
            logging.info(f"Updating book count to {book_count} for {url}")
        
        # Update failure tracking fields if provided
        if failed_attempts is not None:
            update_data['failed_attempts'] = failed_attempts
            
        if last_failed:
            update_data['last_failed'] = last_failed
            
        if status == 'online':
            # If marking as online, update last_success time and clear error fields
            update_data['last_success'] = last_success or current_time
            # Reset failed_attempts if it's not already 0
            if site_record.get('failed_attempts', 0) != 0:
                update_data['failed_attempts'] = 0
                print(f"Resetting failed_attempts to 0 for {url}")
                logging.info(f"Resetting failed_attempts to 0 for {url}")
            # Clear error fields
            update_data['error'] = None
            update_data['error_message'] = None
        elif last_success:
            update_data['last_success'] = last_success
        
        # Update the record
        db["sites"].update(site_record['uuid'], update_data)
        
        # Log the update
        update_summary = {
            'status': status,
            'failed_attempts': failed_attempts or site_record.get('failed_attempts', 0),
            'last_check': current_time
        }
        if error_message:
            update_summary['error'] = error_message
            
        print(f"Updated site {url}: {update_summary}")
        logging.info(f"Updated site {url}: {update_summary}")
        
    except Exception as e:
        error_msg = f"Error updating site status for {url}: {str(e)}"
        print(error_msg)
        logging.error(error_msg, exc_info=True)

#############################################################
# Import the URLS from the temp file and write to Database #
###########################################################
def import_urls_from_file(filepath, dir=data_dir, country=None):
    """
    Import URLs from a file and add them to a sites database.

    Args:
        filepath (str): The path to the file containing the URLs.
        dir (str, optional): The directory where the sites database is located. Defaults to '.'.
        country (str, optional): The country code for the sites being imported. Defaults to None.
    Returns:
        None
    """
    logging.info("***Importing URLs from file Function***")
    logging.info("File: %s, Country: %s", filepath, country)
    
    db = init_sites_db(dir)
    
    with open(filepath) as f:
        for url in f.readlines():
            url = url.rstrip()
            if not url:
                continue
                
            logging.info("Processing URL: %s", url)
            
            # Check if URL already exists
            if get_site_uuid_from_url(db, url):
                logging.info("'%s' already present in database", url)
                print(f"'{url}' already present")
                continue
                
            # Map the URL to a site object
            site_data = map_site_from_url(url, country=country)
            logging.info("Mapped site data: %s", site_data)
            
            if not site_data:
                logging.warning("Skipping invalid URL: %s", url)
                continue
                
            # Add the site to the database
            print(f"Adding '{url}' with country: {country}")
            logging.info("Saving site: %s", site_data)
            save_site(db, site_data)
            logging.info("Successfully saved site: %s", url)
    
###################################
# Get list of libraries from site #
###################################
def get_libs_from_site(site):
    """
    Retrieves libraries from a specified website.

    Args:
        site (str): The URL of the website to retrieve libraries from.

    Returns:
        list[str]: A list of libraries retrieved from the website.

    Raises:
        requests.RequestException: If there is an issue making the request to the website.
    """
    logging.info("****Get Libs from site Function****")
    server=site.rstrip('/')
    api=server+'/ajax/'
    timeout=30
    
    print()
    print("Server:", server)
    logging.info("Server: %s", server)
    url=api+'library-info'

    print()
    print("Getting libraries from", server)
    logging.info("Getting libraries from: %s", server)
    # print(url)

    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.RequestException as e: 
        print("Unable to open site:", url)
        logging.error("Unable to open site: %s", url)
        return []
    except Exception as e:
        logging.error("Other issue: %s", e)
        print ("Other issue:", e)
        return []

    # Tolerate servers without library_map or alternative shapes
    try:
        data = r.json()
        libraries = list((data.get("library_map") or {}).keys())
        if not libraries and isinstance(data.get("libraries"), list):
            libraries = data.get("libraries")
    except Exception as e:
        logging.error("Failed parsing library-info JSON: %s", e)
        return []
    libraries_count = len(libraries)
    logging.info("Libraries: %s", libraries)
    print("Libraries:", ", ".join(libraries))
    
    # Update libraries count in sites database
    try:
        sites_db = Database(Path(data_dir) / "sites.db")
        site_record = None
        
        # Try to find the site by URL if UUID is not available
        for record in sites_db["sites"].rows_where(f"url LIKE '%{server}%'"):
            site_record = record
            break
            
        if site_record:
            site_record["libraries_count"] = libraries_count
            sites_db["sites"].update(site_record["uuid"], site_record)
            print(f"Updated libraries count to {libraries_count} in sites database for {server}")
            logging.info(f"Updated libraries count to {libraries_count} in sites database for {server}")
    except Exception as e:
        print(f"Error updating libraries count in sites database: {e}")
        logging.error(f"Error updating libraries count in sites database: {e}")
    
    return libraries

#############################################
# Per-library tracking helpers               #
#############################################
def upsert_library_count(url: str, library: str, count: int, dir=data_dir):
    """
    Upsert per-library counts into data/sites.db 'libraries_per_server' table.
    Maintains book_count_per_library, last_book_count_per_library, new_books_per_library, last_updated.
    Primary key is (url, library).
    """
    try:
        db_path = Path(dir) / "sites.db"
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        # Ensure table exists (migration should create it, but be defensive)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS libraries_per_server (
                url TEXT NOT NULL,
                library TEXT NOT NULL,
                book_count_per_library INTEGER DEFAULT 0,
                last_book_count_per_library INTEGER DEFAULT 0,
                new_books_per_library INTEGER DEFAULT 0,
                last_updated TEXT,
                PRIMARY KEY (url, library)
            )
            """
        )

        # Fetch previous count
        cur.execute(
            "SELECT book_count_per_library FROM libraries_per_server WHERE url = ? AND library = ?",
            (url, library),
        )
        row = cur.fetchone()
        prev = int(row[0]) if row and row[0] is not None else 0
        current = int(count) if count is not None else 0
        new_books = max(0, current - prev)
        now = datetime.datetime.utcnow().isoformat()

        # Upsert with conflict handling
        cur.execute(
            """
            INSERT INTO libraries_per_server (
                url, library, book_count_per_library, last_book_count_per_library, new_books_per_library, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(url, library) DO UPDATE SET
                last_book_count_per_library = excluded.last_book_count_per_library,
                book_count_per_library = excluded.book_count_per_library,
                new_books_per_library = excluded.new_books_per_library,
                last_updated = excluded.last_updated
            """,
            (url, library, current, prev, new_books, now),
        )
        conn.commit()
        logging.info("Upserted library count %s/%s: prev=%d current=%d new=%d", url, library, prev, current, new_books)
    except Exception as e:
        logging.error("upsert_library_count failed for %s/%s: %s", url, library, e)
    finally:
        try:
            conn.close()
        except Exception:
            pass

#############################################
def create_library_records_for_servers(server_urls):
    """
    For each server in server_urls, fetch libraries and populate records in data/library.db.
    Each record will have uuid, server_url, library_name, library_url, library_book_count.
    Args:
        server_urls (list[str]): List of server base URLs (e.g., http://1.2.3.4:8083)
    Returns:
        None
    """
    import sqlite3
    import uuid as uuidlib
    from pathlib import Path
    print(server_urls)
    
    db_path = Path(__file__).parent / 'data' / 'library.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        for server_url in server_urls:
            try:
                # Fetch library info from server
                api = server_url.rstrip('/') + '/ajax/library-info'
                r = requests.get(api, verify=False, timeout=(30, 30))
                r.raise_for_status()
                lib_map = r.json().get('library_map', {})
                for lib_name, lib_info in lib_map.items():
                    print(lib_name)
                    library_url = lib_info.get('url', server_url.rstrip('/') + '/' + lib_name)
                    print(library_url)
                    book_count = lib_info.get('book_count') or lib_info.get('total_books')
                    print(book_count)
                    if book_count is None:
                        # fallback: try get count from summary
                        book_count = 0
                    try:
                        book_count = int(book_count)
                    except Exception:
                        book_count = 0
                    record = (
                        str(uuidlib.uuid4()),
                        server_url,
                        lib_name,
                        library_url,
                        book_count
                    )
                    cursor.execute("""
                        INSERT OR REPLACE INTO Library (uuid, server_url, library_name, library_url, library_book_count)
                        VALUES (?, ?, ?, ?, ?)
                    """, record)
                    logging.info(f"Inserted library record: {record}")
            except Exception as e:
                logging.error(f"Error processing server {server_url}: {e}")
        conn.commit()
    finally:
        conn.close()
    print("Library records created/updated for all servers.")

###################################
# Check the list of sites in file #
###################################
def check_calibre_list(dir=data_dir):    
    """
    Generates a comment for the given function body in a markdown code block with the correct language syntax.

    Parameters:
        dir (str): The directory to search for the sites database. Defaults to the current directory.

    Returns:
        None
    """
    logging.info("****Check Calibre List Function****")
    db=init_sites_db(dir)
    sites=[]
    for row in db["sites"].rows:
        logging.info("Queueing: %s", row['url'])
        print(f"Queueing:{row['url']}")
        sites.append(row)
    print(sites)
    pool = Pool(100)
    pool.map(lambda s: check_and_save_site (db, s), sites)

#################
# Get site UUID #
#################
# example of a fts search sqlite-utils index.db "select * from summary_fts where summary_fts  match 'title:fre*'"
def get_site_db(uuid, data_dir):
        """
        Retrieves the site database based on the given UUID and directory.

        :param uuid: The UUID of the site.
        :type uuid: int or str
        :param dir: The directory where the site database is located.
        :type dir: str
        :return: The site database.
        :rtype: Database
        """
        logging.info("****Get Site DB Function****")
        f_uuid=str(uuid)+".db"
        logging.info(f_uuid)
        print(f_uuid)
        path = Path(dir) / str(f_uuid) 
        return Database(path)

############################
# Initialize Site Database #
############################
def init_site_db(site, _uuid="", dir=data_dir):
    """
    Initializes a site database.

    Parameters:
        site (str): The URL of the site.
        _uuid (str, optional): The UUID for the database. Defaults to an empty string.
        dir (str, optional): The directory where the database will be created. Defaults to ".".

    Returns:
        Database: The initialized database.

    """
    logging.info("****Init Site DB Function****")
    if not _uuid:
        s_uuid=str(uuid.uuid4())
    else:
        s_uuid=str(_uuid)

    f_uuid=s_uuid+".db"
    path = Path(dir) / f_uuid 
    db = Database(path)


    if not "site" in db.table_names():
        s=db["site"]
        s.insert(
            {    
                "uuid": s_uuid,
                "urls": [site],
                "version": "",
                "major": 0,
                "schema_version": 1,
            }
            , pk='uuid'
        )

    if not "ebooks" in db.table_names():
        db["ebooks"].create({
        "uuid": str,
        "id": int,
        "library": str,  #TODO: manage libraries ids as integer to prevent library renam on remote site  
        "title": str,
        "authors": str,
        "series": str,
        "series_index": int,
        # "edition": int, 
        "language": str,
        "desc": str,
        "identifiers": str,
        "tags": str,
        "publisher": str, #Index Ebooks From Library Function
        "pubdate": str,
        "last_modified": str,
        "timestamp": str,
        "formats": str,
        "cover": int,
        # "epub": int,
        # "mobi": int,
        # "pdf": int,
        # TODO: add the most common formats to avoid alter tables
        }, pk="uuid")

    if not "libraries" in db.table_names():
        db["libraries"].create({
        "id": int,    
        "names": str
        }, pk="id")
        # db.table("ebooks", pk="id")
        # db.table("ebooks", pk="id", alter=True
    return db

#################################
# Get Library URL from Database #
#################################
def get_format_url(db, book, format):
    """
    Generate the URL for a specific book format.

    Args:
        db (dict): The database containing the site information.
        book (dict): The book information.
        format (str): The desired book format.

    Returns:
        str: The URL for the specified book format.
    """
    logging.info("****Get Format URL Function****")
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]
    library=book['library']
    id_=str(book['id'])

    f_url = url+"/get/"+format+"/"+id_+"/"+library
    return f_url
    
############################
# Get Library Version Info #
############################
def get_desc_url(db, book):
    """
    Generate the URL for the book description.

    Parameters:
        db (dict): The database containing the site information.
        book (dict): The book object.

    Returns:
        str: The URL for the book description.
    """
    logging.info("****Get Desc URL Function****")
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]

    library=book['library']
    id_=str(book['id'])

    f_urls=[]

    major=  list(db['site'].rows)[0]["major"]

    if major >= 3:
        d_url =url+"#book_id="+id_+"&library_id="+library+"&panel=book_details"
    else:
        d_url =url+"/browse/book/"+id_

    return d_url

###############################
# Write book info to Database #
###############################
def save_books_metadata_from_site(db, books):
    """
    Saves the metadata of books from a website into the database.

    Parameters:
    - db (dict): The database object.
    - books (list): A list of dictionaries containing the metadata of the books.

    Returns:
    - None
    """
    logging.info("****Save Books Metadata From Site Function****")
    uuid = list(db['site'].rows)[0]["uuid"]
    # print(uuid)
    ebooks_t=db["ebooks"]
    # print([c[1] for c in ebooks_t.columns])
    # for b in books:
    #     print(b['title'])
    #     ebooks_t.insert(b, alter=True)
    # ebooks_t.insert_all(books, alter=True)
    ebooks_t.insert_all(books, alter=True,  pk='uuid', batch_size=1000)
    # print([c[1] for c in ebooks_t.columns])

##########################################
# Update Status when book details loaded #
##########################################
def update_done_status(book):
    """
    Update the status of a book based on its source.

    Args:
        book (dict): The book object containing the source information.

    Returns:
        None: This function does not return anything.
    """
    logging.info("****Update Done Status Function****")
    source=book['source']
    if source['status']!='ignored':
        if set(source['formats'].keys()) == set(book['formats']) & set(source['formats'].keys()):
            book['source']['status']="done"
        else: 
            book['source']['status']="todo"

################################
# Index the Site List Sequence #
################################
def index_site_list_seq(file):
    """
    Reads a file line by line and calls the index_ebooks function on each line.

    Parameters:
        file (str): The path to the file to be read.

    Returns:
        None
    """
    logging.info("****Index Site List Sequence Function****")
    with open(file) as f:
        for s in f.readlines():
            # try: 
            #     index_ebooks(s.rstrip())
            # except:
            #     continue
            index_ebooks(s.rstrip())

###################
# Index Site List #
###################
def index_site_list(file):
    """
    Indexes a list of sites in parallel using a pool of worker processes.

    Args:
        file (str): The path to the file containing the list of sites.

    Returns:
        None
    """
    logging.info("****Index Site List Function****")
    pool = Pool(40)

    with open(file) as f:
        sites = f.readlines()
        sites= [s.rstrip() for s in sites]
        logging.info("Sites: "+str(sites))
        print(sites)
        pool.map(index_ebooks_except, sites)

##########################
# Index ebooks Exception #
##########################
#def index_ebooks_except(site):
#    """
#    Indexes ebooks for a given site, except when an error occurs.

#    Args:
#        site (str): The site to index ebooks for.

#    Returns:
#        None
#    """
#    logging.info("****Index ebooks Exception Function****")
#    try:
#        index_ebooks(site)
#    except:
#        print("Error on site")
#        logging.error("Error on site: "+site)

################
# Index Ebooks #
################
def index_ebooks(site, library, start=0, stop=0, dir=data_dir, num=1000, force_refresh=False):
    """
    Retrieves ebooks from a site and indexes them into a library.

    Args:
        site (str): The site from which to retrieve ebooks.
        library (str, optional): The library in which to index the ebooks. Defaults to "".
        start (int, optional): The starting index of ebooks to retrieve. Defaults to 0.
        stop (int, optional): The ending index of ebooks to retrieve. Defaults to 0.
        dir (str, optional): The directory in which to store the ebooks. Defaults to ".".
        num (int, optional): The number of ebooks to retrieve. Defaults to 1000.
        force_refresh (bool, optional): Whether to force a refresh of the ebooks. Defaults to False.

    Returns:
        None
    """

    #TODO old calibres don't manage libraries.  /ajax/library-info endpoint doesn't exist. It would be better to manage calibre version directly 
    logging.info("****Index Ebooks Function****")
    libs=[]
    try:
        libs= get_libs_from_site(site)
    except:
        print("old lib")
        logging.error("Error on site (Old Lib): "+site)
        
    _uuid=str(uuid.uuid4())
    print ('libs = ', libs)
    # Aggregate counts across libraries and update site once
    server = site.rstrip('/')
    lib_list = libs if libs else ['']
    lib_totals = []

    for lib in lib_list:
        print('Index ebooks From Library', site, _uuid, lib, start, stop)
        cnt = index_ebooks_from_library(site=site, _uuid=_uuid, library=lib, start=start, stop=stop, dir=dir, num=num, force_refresh=force_refresh, update_site_count=False)
        try:
            cnt_int = int(cnt) if cnt is not None else 0
        except Exception:
            cnt_int = 0
        print(f"• Library '{lib}' count: {cnt_int}")
        logging.info("Library %s count: %s", lib, cnt_int)
        lib_totals.append((lib, cnt_int))

    site_total = sum(c for _, c in lib_totals)
    print(f"• Aggregated site total across {len(lib_totals)} libraries: {site_total}")
    logging.info("Aggregated site total across %s libraries: %s", len(lib_totals), site_total)

    # Update sites.db once with the aggregated total
    try:
        sites_db = Database(Path(dir) / "sites.db")
        current_time = datetime.datetime.utcnow().isoformat()
        recs = list(sites_db.query("SELECT * FROM sites WHERE url = ? LIMIT 1", [server]))
        if recs:
            rec = dict(recs[0])
            current = int(rec.get('book_count') or 0)
            update = {
                'book_count': site_total,
                'last_book_count': current,
                'new_books': max(0, site_total - current),
                'last_check': current_time,
                'status': 'online'
            }
            sites_db['sites'].update(rec['uuid'], update)
            print(f"✓ Updated site total for {server}: {site_total}")
        else:
            new_uuid = str(uuid.uuid4())
            row = {
                'uuid': new_uuid,
                'url': server,
                'hostnames': server.split('//')[-1].split('/')[0],
                'status': 'online',
                'last_check': current_time,
                'book_count': site_total,
                'last_book_count': 0,
                'new_books': site_total,
                'libraries_count': len(lib_list)
            }
            sites_db['sites'].insert(row, pk='uuid')
            print(f"✓ Created site record for {server} with total {site_total}")
    except Exception as e:
        print("⚠️ Failed to update aggregated site count:", e)
        logging.error("Failed to update aggregated site count for %s: %s", server, e)

    return

#############################
# Index Ebooks from Library #
#############################
def index_ebooks_from_library(site, _uuid="", library='', start=0, stop=0, dir=data_dir, num=1000, force_refresh=False, update_site_count=False):
    """
    Index ebooks from a library on a site.

    Args:
        site (str): The site to index the library from.
        _uuid (str, optional): The UUID of the library. Defaults to "".
        library (str, optional): The library name. Defaults to "".
        start (int, optional): The starting index for indexing. Defaults to 0.
        stop (int, optional): The stopping index for indexing. Defaults to 0.
        dir (str, optional): The directory to save the indexed ebooks. Defaults to data_dir.
        num (int, optional): The number of ebooks to index at a time. Defaults to 1000.
        force_refresh (bool, optional): Whether to force refresh the metadata. Defaults to False.

    Returns:
        None
    """
    print("\n=== INDEX_EBOOKS_FROM_LIBRARY FUNCTION ===")
    print(f"• Site: {site}")
    print(f"• UUID: {_uuid}")
    print(f"• Library: {library}")
    print(f"• Start: {start}, Stop: {stop}")
    print(f"• Directory: {dir}")
    print(f"• Num: {num}, Force Refresh: {force_refresh}")
    print("======================================\n")
    
    logging.info("Indexing ebooks from library")
    offset = 0 if not start else start-1
    server = site.rstrip('/')
    api = server + '/ajax/'
    # Normalize library value for URL construction
    lib = (library or '').strip('/')
    library_path = f"/{lib}" if lib else ""

    # Debug print for the API URL
    print(f"• API URL: {api}")
    print(f"• Library (normalized): '{lib}'")
    print(f"• Library path: '{library_path}'")
    
    # Ensure the directory exists
    os.makedirs(dir, exist_ok=True)
    num=min(1000, num)
    server=server.rstrip('/')
    api=server+'/ajax/'

    timeout=15

    print(f"\nIndexing library: {lib} from server: {server} ")
    logging.info(f"Indexing library: {lib} from server: {server} ")
    # Build count URL without introducing double slashes
    url = f"{api}search{library_path}?num=0"
    print(f"\nGetting ebooks count of library: {lib} from server:{server} ")
    logging.info(f"Getting ebooks count of library: {lib} from server:{server} ")
    # Get the total number of ebooks
    print("\n=== GETTING EBOOK COUNT ===")
    print(f"• Initial Request URL: {url}")

    # Try multiple endpoint variants for compatibility across Calibre-Web versions
    from urllib.parse import quote
    total_num = None
    variants = []

    # Determine candidate representations for the library (ID vs Name)
    lib_candidates = []
    if lib:
        lib_candidates.append(lib)
        try:
            li = requests.get(f"{api}library-info", timeout=20, verify=False)
            if li.ok:
                data = li.json()
                lmap = (data.get("library_map") or {})
                # If lib matches a key, add its value (name)
                if lib in lmap:
                    name = lmap.get(lib)
                    if isinstance(name, str) and name:
                        lib_candidates.append(name)
                # If lib matches a value (name), add its key (id)
                for k, v in lmap.items():
                    if isinstance(v, str) and v == lib:
                        lib_candidates.append(str(k))
                        break
        except Exception as _e:
            print("• library-info lookup failed (non-fatal):", _e)

    # {api}search with and without explicit library
    variants.append(url)  # {api}search{library_path}?num=0
    for L in lib_candidates:
        qL = quote(L)
        variants.append(f"{api}search/{qL}?num=0")
        variants.append(f"{api}search?num=0&library_id={qL}")
        # Some versions require explicit empty query
        variants.append(f"{api}search/{qL}?num=0&query=%5B%5D")
        variants.append(f"{api}search?num=0&library_id={qL}&query=%5B%5D")
    # Global (no library) fallbacks
    variants.append(f"{api}search?num=0")
    variants.append(f"{api}search?num=0&query=%5B%5D")

    last_error = None
    for i, vurl in enumerate(variants, start=1):
        try:
            print(f"• Attempt {i}: {vurl}")
            r = requests.get(vurl, timeout=30, headers=headers)
            print(f"  ↳ Status: {r.status_code}")
            # Keep going even if non-200 to see others
            r.raise_for_status()
            txt = r.text[:300]
            print(f"  ↳ Body[0:300]: {txt}")
            try:
                data = r.json()
            except Exception as je:
                print(f"  ↳ JSON parse failed: {je}")
                last_error = je
                continue

            # Accept several possible keys
            for key in ("total_num", "total", "count"):
                if key in data and data.get(key) is not None:
                    try:
                        total_num = int(data[key])
                    except Exception:
                        try:
                            total_num = int(str(data[key]).strip())
                        except Exception:
                            total_num = None
                    break
            
            if total_num is not None:
                print(f"  ↳ Parsed total_num={total_num} from key set {list(data.keys())}")
                break
            else:
                print(f"  ↳ No total found, keys={list(data.keys())}")
        except Exception as e:
            print(f"  ↳ Attempt {i} failed: {e}")
            last_error = e
            continue

    if total_num is None:
        print("❌ Unable to determine total book count from any endpoint variant.")
        if last_error:
            print(f"Last error: {last_error}")
        logging.error("All count endpoint variants failed for %s (lib=%s)", server, lib, exc_info=True)
        total_num = 0

    # Apply stop limit if specified
    if stop > 0 and stop < total_num:
        total_num = stop
        print(f"• Limited total to {total_num} (stop={stop})")

    print(f"• Final book count: {total_num}")
    logging.info(f"Total count={total_num} from {server}")

    # If we are only collecting the per-library count, return it now
    if not update_site_count:
        return total_num

    total_num = total_num if not stop else stop
    print(f"\n=== EBOOK COUNT ===")
    print(f"Total count={total_num} from {server}")
    print(f"Stop value: {stop}")
    logging.info(f"Total count={total_num} from {server}")
    
    # Debug: Print before updating book count
    print("\n=== STARTING BOOK COUNT UPDATE ===")
    print(f"• Current time: {datetime.datetime.utcnow().isoformat()}")
    print(f"• Site URL: {site}")
    print(f"• UUID: {_uuid}")
    print(f"• Working directory: {os.getcwd()}")
    print("===================================\n")
    
    # Get database path and ensure it exists
    sites_db_path = Path(dir) / "sites.db"
    print(f"\n=== DATABASE OPERATION ===")
    print(f"• Database path: {sites_db_path.absolute()}")
    print(f"• Database exists: {sites_db_path.exists()}")
    print(f"• Current working directory: {os.getcwd()}")
    
    # Ensure directory exists
    os.makedirs(dir, exist_ok=True)
    print(f"• Directory {dir} exists: {os.path.exists(dir)}")
    
    try:
        # Try to connect to the database
        print("• Connecting to database...")
        try:
            sites_db = Database(str(sites_db_path))  # Convert to string for better compatibility
            print("✅ Successfully connected to database")
            
            # Enable foreign keys
            sites_db.conn.execute("PRAGMA foreign_keys = ON")
            print("• Enabled foreign key constraints")
            
        except Exception as conn_err:
            print(f"❌ Failed to connect to database: {str(conn_err)}")
            print("• Checking directory permissions...")
            print(f"• Directory writable: {os.access(dir, os.W_OK)}")
            logging.error("Database connection failed", exc_info=True)
            raise
        
        # Check if table exists
        print("\n=== CHECKING DATABASE SCHEMA ===")
        try:
            tables = sites_db.table_names()
            print(f"• All tables: {tables}")
            table_exists = "sites" in tables
            print(f"• Table 'sites' exists: {table_exists}")
            
            if table_exists:
                # Check table structure
                print("\n=== CHECKING TABLE STRUCTURE ===")
                try:
                    table_info = sites_db.conn.execute("PRAGMA table_info(sites)").fetchall()
                    columns = [col[1] for col in table_info]
                    print(f"• Columns in 'sites' table: {columns}")
                    
                    required_columns = {
                        'uuid', 'url', 'hostnames', 'status', 'last_check',
                        'book_count', 'last_book_count', 'new_books', 'libraries_count'
                    }
                    
                    missing_columns = required_columns - set(columns)
                    if missing_columns:
                        print(f"❌ Missing columns: {missing_columns}")
                        raise Exception(f"Missing required columns: {missing_columns}")
                    
                    print("✅ All required columns present")
                    
                except Exception as schema_err:
                    print(f"❌ Error checking table structure: {str(schema_err)}")
                    logging.error("Table structure check failed", exc_info=True)
                    raise
            
        except Exception as table_err:
            print(f"❌ Error checking tables: {str(table_err)}")
            logging.error("Table check failed", exc_info=True)
            raise
        
        if not table_exists:
            print("\n=== CREATING SITES TABLE ===")
            try:
                print("• Creating 'sites' table with schema...")
                sites_db["sites"].create({
                    "uuid": str,
                    "url": str,
                    "hostnames": str,
                    "status": str,
                    "failed_attempts": int,  # Track consecutive failed connection attempts
                    "last_failed": str,  # Timestamp of last failure
                    "last_success": str,  # Timestamp of last success
                    "last_check": str,
                    "book_count": int,
                    "last_book_count": int,
                    "new_books": int,
                    "libraries_count": int
                }, pk="uuid")
                
                # Verify table was created
                tables_after = sites_db.table_names()
                print(f"• Tables after creation: {tables_after}")
                if "sites" not in tables_after:
                    raise Exception("Failed to create 'sites' table")
                
                print("✅ Created 'sites' table successfully")
                
            except Exception as create_err:
                print(f"❌ Error creating 'sites' table: {str(create_err)}")
                logging.error("Table creation failed", exc_info=True)
                raise
        
        # Add index on URL if it doesn't exist
        try:
            print("\n=== CHECKING INDEXES ===")
            indexes = sites_db.conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='sites';").fetchall()
            print(f"• Current indexes: {indexes}")
            
            if not any('idx_url' in idx[0] for idx in indexes):
                print("• Creating index on 'url' column...")
                sites_db.conn.execute("CREATE INDEX IF NOT EXISTS idx_url ON sites(url);")
                print("✅ Created index on 'url' column")
            else:
                print("• Index on 'url' column already exists")
                
        except Exception as index_err:
            print(f"⚠️ Warning: Could not create index: {str(index_err)}")
            logging.warning("Index creation failed", exc_info=True)
            
    except Exception as e:
        print(f"❌ Fatal database error: {str(e)}")
        logging.critical("Fatal database error", exc_info=True)
        raise
    
    # Get or create site record by URL (not UUID)
    print("\n=== SITE RECORD LOOKUP ===")
    print(f"• Looking up site record for URL: {server}")
    if _uuid:
        print(f"• Fallback UUID: {_uuid}")
    
    site_record = None
    try:
        # First try to find by URL (primary lookup)
        print("• Querying database for site by URL...")
        site_records = list(sites_db.query(
            "SELECT * FROM sites WHERE url = ? LIMIT 1",
            [server]
        ))
        print(f"• Found {len(site_records)} matching records by URL")
        
        if site_records:
            site_record = dict(site_records[0])
            print("\n=== EXISTING SITE RECORD FOUND BY URL ===")
            print(f"• UUID: {site_record.get('uuid')}")
            print(f"• URL: {site_record.get('url')}")
            print(f"• Status: {site_record.get('status')}")
            print(f"• Book Count: {site_record.get('book_count')}")
            print(f"• Last Book Count: {site_record.get('last_book_count')}")
            print(f"• New Books: {site_record.get('new_books')}")
            print(f"• Last Check: {site_record.get('last_check')}")
            print("========================================\n")
        else:
            print(f"ℹ️ No site record found for URL: {server}")
            
            # If no record found by URL, try by UUID as fallback (for backward compatibility)
            if _uuid:
                print(f"• Attempting fallback lookup by UUID: {_uuid}")
                uuid_records = list(sites_db.query(
                    "SELECT * FROM sites WHERE uuid = ? LIMIT 1",
                    [_uuid]
                ))
                print(f"• Found {len(uuid_records)} matching records by UUID")
                
                if uuid_records:
                    site_record = dict(uuid_records[0])
                    print("\n⚠️ FALLBACK: FOUND SITE RECORD BY UUID")
                    print(f"• UUID in DB: {site_record.get('uuid')}")
                    print(f"• Current URL in DB: {site_record.get('url')}")
                    print(f"• New URL being used: {server}")
                    
                    # Update URL to match current server
                    try:
                        with sites_db.conn:
                            cursor = sites_db.conn.cursor()
                            cursor.execute(
                                "UPDATE sites SET url = ? WHERE uuid = ?",
                                [server, _uuid]
                            )
                            if cursor.rowcount > 0:
                                print(f"✓ Updated site record URL from '{site_record.get('url')}' to '{server}'")
                                # Refresh the site record with updated data
                                site_record['url'] = server
                            else:
                                print("⚠️ No rows updated - site record may not exist")
                    except Exception as update_err:
                        print(f"❌ Failed to update site record URL: {str(update_err)}")
                        logging.error("Failed to update site record URL", exc_info=True)
                    
    except Exception as e:
        print(f"❌ Error during site record lookup: {str(e)}")
        logging.error("Site record lookup failed", exc_info=True)
        logging.error(traceback.format_exc())
        site_record = None
    
    current_time = datetime.datetime.utcnow().isoformat()
    print(f"\n• Current time: {current_time}")
    
    if not site_record:
        # Create new record
        print("\n=== CREATING NEW SITE RECORD ===")
        new_uuid = str(uuid.uuid4())
        site_record = {
            'uuid': new_uuid,
            'url': site,
            'hostnames': site.split('//')[-1].split('/')[0],
            'status': 'online',
            'last_check': current_time,
            'book_count': total_num,
            'last_book_count': 0,
            'new_books': 0,
            'libraries_count': 1
        }
        print(f"• New record data: {site_record}")
        
        try:
            print("• Attempting to insert new record...")
            sites_db["sites"].insert(site_record, pk='uuid')
            
            # Verify the record was inserted
            inserted = sites_db["sites"].get(new_uuid)
            if inserted:
                print(f"✅ Successfully created new site record with {total_num} books")
                print(f"• Inserted record: {dict(inserted)}")
                logging.info(f"Created new site record with {total_num} books")
                _uuid = new_uuid  # Update the _uuid variable for later use
            else:
                raise Exception("Failed to verify record creation")
                
        except Exception as e:
            print(f"❌ Failed to create new site record: {str(e)}")
            logging.error(f"Failed to create new site record: {str(e)}", exc_info=True)
            # Try to get more details about the error
            try:
                print(f"• Database error details: {str(e)}")
                if hasattr(e, 'args') and e.args:
                    print(f"• Error args: {e.args}")
            except:
                pass
            raise
    else:
        # Update existing record
        print("\n=== UPDATING EXISTING RECORD ===")
        # Get the current book count before updating
        current_book_count = site_record.get('book_count', 0)
        last_book_count = current_book_count  # This will be the previous count after update
        new_books = max(0, total_num - current_book_count) if total_num > current_book_count else 0
        
        print("\n=== UPDATING SITE RECORD ===")
        print(f"• Current book count in DB: {current_book_count}")
        print(f"• New book count from API: {total_num}")
        print(f"• New books since last check: {new_books}")
        print(f"• Last book count will be set to: {last_book_count}")
        
        try:
            with sites_db.conn:  # Use a transaction
                cursor = sites_db.conn.cursor()
                
                if site_record:
                    # Update existing record
                    site_uuid = site_record["uuid"]
                    print(f"• Updating existing site record for URL: {server}")
                    print(f"• Site UUID: {site_uuid}")
                    
                    # Explicitly set all fields we want to update
                    # First, get current values to ensure we don't miss anything
                    cursor.execute("SELECT * FROM sites WHERE uuid = ?", [site_uuid])
                    current_record = dict(zip([desc[0] for desc in cursor.description], cursor.fetchone()))
                    print(f"• Current record before update: {current_record}")
                    
                    # Prepare update data with explicit values
                    update_data = {
                        "book_count": total_num,           # New total count
                        "last_book_count": current_book_count,  # Previous count becomes last_book_count
                        "new_books": new_books,            # Difference between new and old count
                        "last_check": current_time,
                        "status": "online",
                        "failed_attempts": 0,  # Reset failed attempts on success
                        "last_success": current_time,  # Update last successful check
                        "error": None,  # Clear any previous errors
                        "error_message": None  # Clear any previous error messages
                    }
                    
                    # Ensure all expected columns are in the update
                    for col in ["book_count", "last_book_count", "new_books", "last_check", "status"]:
                        if col not in update_data:
                            update_data[col] = current_record.get(col, None)
                    
                    # Build and execute the update query
                    set_clause = ", ".join([f"{k} = ?" for k in update_data.keys()])
                    values = list(update_data.values()) + [site_uuid]
                    update_sql = f"UPDATE sites SET {set_clause} WHERE uuid = ?"
                    
                    # Add debug logging for the update
                    logging.info(f"Updating site record with SQL: {update_sql}")
                    logging.info(f"Update values: {values}")
                    
                    print("\n=== EXECUTING UPDATE ===")
                    print(f"• SQL: {update_sql}")
                    print(f"• Values: {values}")
                    print(f"• Update data: {update_data}")
                    
                    # Execute the update within a transaction
                    try:
                        cursor.execute("BEGIN TRANSACTION")
                        cursor.execute(update_sql, values)
                        
                        # Verify the update immediately
                        cursor.execute("""
                            SELECT book_count, last_book_count, new_books 
                            FROM sites 
                            WHERE uuid = ?
                        """, [site_uuid])
                        
                        updated = cursor.fetchone()
                        if updated:
                            logging.info(f"After update - book_count: {updated[0]}, last_book_count: {updated[1]}, new_books: {updated[2]}")
                            
                            # Verify the values were set correctly
                            if int(updated[0]) != int(total_num):
                                logging.error(f"book_count mismatch! Expected {total_num}, got {updated[0]}")
                            if int(updated[1]) != int(current_book_count):
                                logging.error(f"last_book_count mismatch! Expected {current_book_count}, got {updated[1]}")
                            if int(updated[2]) != int(new_books):
                                logging.error(f"new_books mismatch! Expected {new_books}, got {updated[2]}")
                        
                        cursor.execute("COMMIT")
                        logging.info("Transaction committed successfully")
                        
                        if cursor.rowcount > 0:
                            print(f"✅ Successfully updated site record. Rows affected: {cursor.rowcount}")
                        
                    except Exception as e:
                        cursor.execute("ROLLBACK")
                        logging.error(f"Error updating site record: {str(e)}")
                        raise
                        
                        # Verify the update immediately
                        cursor.execute("SELECT book_count, last_book_count, new_books FROM sites WHERE uuid = ?", [site_uuid])
                        updated = cursor.fetchone()
                        if updated:
                            print(f"• After update - book_count: {updated[0]}, last_book_count: {updated[1]}, new_books: {updated[2]}")
                            
                            # Check if the values were set correctly
                            if updated[0] != total_num:
                                print(f"❌ book_count mismatch! Expected {total_num}, got {updated[0]}")
                            if updated[1] != current_book_count:
                                print(f"❌ last_book_count mismatch! Expected {current_book_count}, got {updated[1]}")
                            if updated[2] != new_books:
                                print(f"❌ new_books mismatch! Expected {new_books}, got {updated[2]}")
                    else:
                        print("⚠️ No rows were updated - record may not exist")
                        
                        # Try to find out why no rows were updated
                        cursor.execute("SELECT COUNT(*) FROM sites WHERE uuid = ?", [site_uuid])
                        count = cursor.fetchone()[0]
                        print(f"• Records with UUID {site_uuid}: {count}")
                        
                        if count == 0:
                            print("❌ No record found with the specified UUID!")
                        else:
                            cursor.execute("SELECT book_count, last_book_count, new_books FROM sites WHERE uuid = ?", [site_uuid])
                            current = cursor.fetchone()
                            print(f"• Current values: book_count={current[0]}, last_book_count={current[1]}, new_books={current[2]}")
                        
                else:
                    # Create new record
                    site_uuid = _uuid or str(uuid.uuid4())
                    print(f"• Creating new site record for URL: {server}")
                    print(f"• New UUID: {site_uuid}")
                    
                    # For new records, set last_book_count to 0 and new_books to total_num
                    new_record = {
                        "uuid": site_uuid,
                        "url": server,
                        "hostnames": server.replace('https://', '').replace('http://', '').split('/')[0],
                        "status": "online",
                        "last_check": current_time,
                        "book_count": total_num,
                        "last_book_count": 0,  # No previous count for new records
                        "new_books": total_num,  # All books are new for a new record
                        "libraries_count": 1
                    }
                    
                    # Build and execute the insert query
                    columns = ", ".join(new_record.keys())
                    placeholders = ", ".join(["?"] * len(new_record))
                    values = list(new_record.values())
                    insert_sql = f"INSERT INTO sites ({columns}) VALUES ({placeholders})"
                    
                    print(f"• Executing SQL: {insert_sql}")
                    print(f"• With values: {values}")
                    
                    cursor.execute(insert_sql, values)
                    print(f"✅ Successfully created new site record. Row ID: {cursor.lastrowid}")
                    
                    try:
                        sites_db.conn.commit()
                        print("• Transaction committed successfully")
                        
                        # Verify the commit was successful
                        cursor.execute("SELECT book_count, last_book_count, new_books FROM sites WHERE uuid = ?", [site_uuid])
                        after_commit = cursor.fetchone()
                        if after_commit:
                            print(f"• After commit - book_count: {after_commit[0]}, last_book_count: {after_commit[1]}, new_books: {after_commit[2]}")
                            
                            # Check if values match what we tried to set
                            if after_commit[0] != total_num or after_commit[1] != current_book_count or after_commit[2] != new_books:
                                print("❌ WARNING: Values after commit do not match expected values!")
                                print(f"  Expected: book_count={total_num}, last_book_count={current_book_count}, new_books={new_books}")
                                print(f"  Got:      book_count={after_commit[0]}, last_book_count={after_commit[1]}, new_books={after_commit[2]}")
                                
                                # Try a direct update as a last resort
                                print("• Attempting direct update...")
                                cursor.execute("""
                                    UPDATE sites 
                                    SET book_count = ?,
                                        last_book_count = ?,
                                        new_books = ?,
                                        last_check = ?
                                    WHERE uuid = ?
                                """, (total_num, current_book_count, new_books, current_time, site_uuid))
                                sites_db.conn.commit()
                                print("• Direct update committed")
                    except Exception as e:
                        print(f"❌ Error committing transaction: {str(e)}")
                        sites_db.conn.rollback()
                        raise
                
                # Verify the update with a fresh query
                print("\n=== VERIFYING UPDATE ===")
                verify_sql = """
                    SELECT 
                        uuid, 
                        url, 
                        book_count, 
                        last_book_count, 
                        new_books, 
                        last_check,
                        status
                    FROM sites 
                    WHERE uuid = ?
                """
                cursor.execute(verify_sql, [site_uuid])
                updated_record = cursor.fetchone()
                
                if updated_record:
                    print("\n=== VERIFICATION RESULTS ===")
                    print(f"• UUID: {updated_record[0]}")
                    print(f"• URL: {updated_record[1]}")
                    print(f"• Current book count: {updated_record[2]}")
                    print(f"• Last book count: {updated_record[3]}")
                    print(f"• New books: {updated_record[4]}")
                    print(f"• Last check: {updated_record[5]}")
                    print(f"• Status: {updated_record[6]}")
                    
                    # Verify the values match what we tried to set
                    expected_values = {
                        'book_count': total_num,
                        'last_book_count': current_book_count,
                        'new_books': new_books
                    }
                    
                    print("\n=== VERIFYING VALUES ===")
                    for i, field in enumerate(['book_count', 'last_book_count', 'new_books']):
                        actual = updated_record[i+2]  # +2 because first two fields are uuid and url
                        expected = expected_values[field]
                        status = "✅" if actual == expected else "❌"
                        print(f"{status} {field}: Expected {expected}, Got {actual}")
                    
                    # Double-check with a fresh connection
                    try:
                        print("\n=== DOUBLE-CHECKING WITH FRESH CONNECTION ===")
                        fresh_db = Database(str(sites_db_path))
                        fresh_record = fresh_db.query("""
                            SELECT uuid, url, book_count, last_book_count, new_books, last_check 
                            FROM sites 
                            WHERE uuid = ?
                        """, [site_uuid]).fetchone()
                        fresh_db.close()
                        
                        if fresh_record:
                            print(f"• Fresh connection verification successful!")
                            print(f"• Fresh book count: {fresh_record[2]}")
                            print(f"• Fresh last book count: {fresh_record[3]}")
                            print(f"• Fresh new books: {fresh_record[4]}")
                            print(f"• Fresh last check: {fresh_record[5]}")
                            
                            # Verify the counts make sense
                            if fresh_record[2] < fresh_record[3]:
                                print("⚠️ WARNING: Current book count is less than last book count!")
                            if fresh_record[4] != (fresh_record[2] - fresh_record[3]):
                                print(f"⚠️ WARNING: New books count mismatch! Expected {fresh_record[2] - fresh_record[3]} but got {fresh_record[4]}")
                        else:
                            print("⚠️ Could not find record with fresh connection!")
                            print("• Attempting final direct SQL update...")
                            try:
                                final_db = Database(str(sites_db_path))
                                final_cursor = final_db.conn.cursor()
                                final_cursor.execute("""
                                    UPDATE sites 
                                    SET book_count = ?,
                                        last_book_count = ?,
                                        new_books = ?,
                                        last_check = ?,
                                        status = ?
                                    WHERE url = ?
                                """, (
                                    total_num,
                                    last_book_count,
                                    new_books,
                                    current_time,
                                    'online',
                                    server
                                ))
                                final_db.conn.commit()
                                print("✅ Final direct SQL update committed")
                                final_db.close()
                            except Exception as final_err:
                                print(f"❌ Final direct SQL update failed: {str(final_err)}")
                                logging.error("Final direct SQL update failed", exc_info=True)
                    except Exception as final_err:
                        print(f"❌ Error during fresh connection check: {str(final_err)}")
                        logging.error("Fresh connection check failed", exc_info=True)
                else:
                    print("❌ Verification failed - could not find updated record")
                    
        except Exception as e:
            current_time = datetime.datetime.utcnow().isoformat()
            print(f"❌ Error checking site {server}: {str(e)}")
            logging.error(f"Error checking site {server}", exc_info=True)
            
            # Get current failed attempts count with enhanced error handling
            failed_attempts = 1  # Start with 1 for the current failure
            try:
                site_info = sites_db.query(
                    "SELECT failed_attempts FROM sites WHERE url = ?", 
                    [server]
                ).fetchone()
                
                if site_info and len(site_info) > 0 and site_info[0] is not None:
                    failed_attempts = int(site_info[0]) + 1
                    print(f"ℹ️ Incremented failed_attempts to: {failed_attempts} for {server}")
                else:
                    print(f"ℹ️ No previous failed_attempts found, setting to 1 for {server}")
                    
            except Exception as db_err:
                error_msg = f"⚠️ Could not get current failed attempts for {server}: {str(db_err)}"
                print(error_msg)
                logging.error(error_msg, exc_info=True)
                # Continue with failed_attempts = 1
            
            # Update site status with failure information
            update_site_status(
                sites_db, 
                server, 
                "offline", 
                f"{str(e)} (Attempt {failed_attempts})",
                failed_attempts=failed_attempts,
                last_failed=current_time
            )
            
            if hasattr(e, 'args') and e.args:
                print(f"• Error details: {e.args}")
            if hasattr(e, 'sql') and e.sql:
                print(f"• Failed SQL: {e.sql}")
            if hasattr(e, 'params') and e.params:
                print(f"• Failed params: {e.params}")
    
    # Ensure database connection is closed and reopened
    try:
        sites_db.conn.close()
    except:
        pass
        
    # Reopen database for the rest of the function
    sites_db = Database(sites_db_path)
    
    # library=r.json()["base_url"].split('/')[-1]
    # base_url=r.json()["base_url"]

    # cache_db=init_cache_db(dir=dir)
    # _uuid=get_uuid_from_url(cache_db)
    print('Init database for site:', site )
    db=init_site_db(site, _uuid=_uuid, dir=dir)
    r_site = (list(db['site'].rows)[0])
    print('r_site = ', r_site)

    r_site['version']=r.headers['server']
    print('Version =', r_site['version'])
    r_site['major']=int(re.search(r'calibre.(\d).*', r.headers['server']).group(1))
    print('Major = ',r_site['major'])
    db["site"].upsert(r_site, pk='uuid')

    print()

    range=offset+1
    while offset < total_num:
        remaining_num = min(num, total_num - offset)
        # print()
        # print("Downloading ids: offset="+str(offset), "num="+str(remaining_num))
        print ('\r {:180.180}'.format(f'Downloading ids: offset={str(offset)} count={str(remaining_num)} from {server}'), end='')
        logging.info(f"Downloading ids: offset={str(offset)} count={str(remaining_num)} from {server}")

        # url=server+base_url+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'
        url=api+f'search{lib_segment}?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'

        # print("->", url)
        try:
            r = requests.get(url, verify=False, timeout=(timeout, 30))
            r.raise_for_status()
        except requests.RequestException as e:
            error_msg = f"Error fetching book IDs from {url}: {str(e)}"
            print(f"\n❌ {error_msg}")
            logging.error(error_msg, exc_info=True)
            return
        # print("Ids received from:"+str(offset), "to:"+str(offset+remaining_num-1))
        
        # print()
        # print("Downloading metadata from", str(offset+1), "to", str(offset+remaining_num))
        print ('\r {:180.180}'.format(f'Downloading metadata from {str(offset+1)} to {str(offset+remaining_num)}/{total_num} from {server}'), end='')
        logging.info("Downloading metadata from "+str(offset+1)+" to "+str(offset+remaining_num))
        books_s=",".join(str(i) for i in r.json()['book_ids'])
        url=api+f'books{lib_segment}?ids='+books_s
        # url=server+base_url+'/books?ids='+books_s
        # print("->", url)
        # print ('\r{:190.190}'.format(f'url= {url} ...'), end='')

        try:
            r=requests.get(url, verify=False, timeout=(60, 60))
            r.raise_for_status()
        except requests.RequestException as e:
            error_msg = f"Error fetching book details from {url}: {str(e)}"
            print(f"\n❌ {error_msg}")
            logging.error(error_msg, exc_info=True)
            return
        # print(len(r.json()), "received")
        print ('\r {:180.180}'.format(f'{len(r.json())} received'), end='')
        logging.info(f"{len(r.json())} received")
        
        books=[]
        for id, r_book in r.json().items():                
            uuid=r_book['uuid']
            if not uuid:
                print ("No uuid for ebook: ignored")
                logging.info("No uuid for ebook: ignored")
                continue 


            if r_book['authors']:
                desc= f"({r_book['title']} / {r_book['authors'][0]})"
            else:
                desc= f"({r_book['title']})"

            # print (f'\r--> {range}/{total_num} - {desc}', end='')
            # print (f'\r{server}--> {range}/{total_num} - {desc}', end='')
            print ('\r {:180.180} '.format(f'{range}/{total_num} ({server} : {uuid} --> {desc}'), end='')
            logging.info(f"{range}/{total_num} ({server} : {uuid} --> {desc}")

            if not force_refresh:
                # print("Checking local metadata:", uuid)
                try:
                    book = load_metadata(dir, uuid)
                except:
                    print("Unable to get metadata from:", uuid)
                    logging.error("Unable to get metadata from: "+str(uuid))
                    range+=1
                    continue
                if book:
                    print("Metadata already present for:", uuid)
                    logging.error("Metadata already present for: "+str(uuid))
                    range+=1
                    continue

            if not r_book['formats']:
                # print("No format found for {}".format(r_book['uuid']))
                range+=1
                continue

            book={}
            book['uuid']=r_book['uuid']
            book['id']=id
            # Store normalized library identifier (without leading '/')
            book['library']=library_id

            # book['title']=r_book['title']
            book['title']=unidecode.unidecode(r_book['title'])
            # book['authors']=r_book['authors']

            if r_book['authors']:
                book['authors']=[unidecode.unidecode(s) for s in r_book['authors']]
            # book['desc']=""

            book['desc']=r_book['comments']

            if r_book['series']:
                book['series']=unidecode.unidecode(r_book['series'])
                # book['series']=[unidecode.unidecode(s) for s in r_book['series']]
            s_i=r_book['series_index']
            if (s_i): 
                book['series_index']=int(s_i)

            # book['edition']=0

            book['identifiers']=r_book['identifiers']

            # book['tags']=r_book['tags']
            if r_book['tags']:
                book['tags']=[unidecode.unidecode(s) for s in r_book['tags']]

            book['publisher']=r_book['publisher']
            # book['publisher']=unidecode.unidecode(r_book['publisher'])

            book['pubdate']=r_book['pubdate']

            if not r_book['languages']:
            # if True:
                text=r_book['title']+". "
                if r_book['comments']:
                    text=r_book['comments']                    
                s_language, prob=identifier.classify(text)
                if prob >= 0.85:
                    language =  iso639.to_iso639_2(s_language)
                    book['language']=language
                else:
                    book['language']=''
            else:
                book['language']=iso639.to_iso639_2(r_book['languages'][0])

            if r_book['cover']:
                book['cover']= True
            else:
                book['cover']= False

            book['last_modified']=r_book['last_modified']
            book['timestamp']=r_book['timestamp']

            book['formats']=[]
            formats=r_book['formats']
            for f in formats:                    
                if 'size' in r_book['format_metadata'][f]:
                    size=int(r_book['format_metadata'][f]['size'])
                else:
                    # print()
                    # print(f"Size not found for format '{f}'  uuid={uuid}: skipped")
                    pass
                    #TODO query the size when the function to rebuild the full url is ready
                    #   
                    # print("Trying to get size online: {}".format('url'))
                    # try:
                    #     size=get_file_size(s['url'])
                    # except:
                    #     print("Unable to access size for format '{}' : {} skipped".format(f, uuid))
                    #     continue
                book[f]=(size)
                book['formats'].append(f)

            if not book['formats']:
            # if not c_format:
                # print()
                # print(f"No format found for {book['uuid']} id={book['id']} : skipped")
                range+=1
                # continue


            books.append(book)
            range+=1

        # print()
        print("Saving metadata")
        print ('\r {:180.180}'.format(f'Saving metadata from {server}'), end='')
        logging.info("Saving metadata from %s", server)
        try:
            save_books_metadata_from_site(db, books)
            print('\r {:180.180}'.format(f'--> Saved {range-1}/{total_num} ebooks from {server}'), end='')
            logging.info("Saved %s/%s ebooks from %s", range-1, total_num, server)
        except BaseException as err:
            print (err)
            logging.error(err)
        print()
        print()

        # try:
        #     save_metadata(db, books)
        # except:
        #     print("Unable to save book metadata")

        offset=offset+num
    
############################
# Query EBooks in Database #
############################
def query(query_str="", dir=data_dir):
    """
    Generates a function comment for the given function body in a markdown code block with the correct language syntax.

    Parameters:
    - query_str (str): The query string to be used in the function.
    - dir (str): The directory to search for files. Default is the current directory.

    Returns:
    - None
    """
    logging.info("Querying database: %s", query_str)
    dbs=[]
    for path in os.listdir(dir):
        db = Database(path)
        # print (db["ebooks"].count)
        # for row in db["site"].rows:
        #     print (f'{row["urls"]}: {db["ebooks"].count}')
        # db["ebooks"].search(query_str)
        # url=db['site'].get(1)['urls'][0]
        url=db['site'].get(1)
        print (url)
        logging.info("Querying %s", url)

        for ebook in db["ebooks"].rows_where(query_str):
            # print (f"{ebook['title']} ({ebook['uuid']})")
            print (ebook)
            logging.info("%s (%s)", ebook['title'], ebook['uuid'])

##################
# Get Statistics #
##################
def get_stats(dir=data_dir):
    """
    Retrieves statistics about the ebooks in a given directory.

    Parameters:
    - dir (str): The directory path to search for ebooks. Defaults to the current directory.

    Returns:
    None
    """
    logging.info("*** get_stats ***")
    dbs=[]
    size=0
    count=0
    for f in os.listdir(dir):
        if not f.endswith(".db"):
            continue
        if f == "index.db":
            continue
        path = Path(dir) / f 
        dbs.append(Database(path))

    for db in dbs:
        for i, ebook in enumerate(db["ebooks"].rows):
            uuid=ebook['uuid']
            title=ebook['title']
            formats=json.loads(ebook['formats'])
            # print(formats)
            for f in formats:
                if f in ebook:
                    if ebook[f]:
                        size+=ebook[f]
                        count+=1
                        # print (f'\r{count} {f} --> {uuid}: {title}', end ='')
                        # print (f'\r{count} : {uuid} --> {f}', end='')
                        print (f'\r{count} formats - ebook : {uuid}', end='')
                        logging.info("%s : %s", uuid, f)

    print()
    print("Total count of formats:", humanize.intcomma(count)) 
    logging.info("Total count of formats: %s", humanize.intcomma(count))
    print("Total size:", hsize(size)) 
    logging.info("Total size: %s", hsize(size))
    print()

###############################
# Get Temporary Site Database #
###############################
def get_site_db(uuid, data_dir):
        """
        Generate a function comment for the given function body.

        Args:
            uuid (int): The unique identifier for the site database.
            dir (str): The directory where the site database is located.

        Returns:
            Database: The site database corresponding to the given UUID.
        """
        logging.info("*** get_site_db ***")
        f_uuid=str(uuid)+".db"
        print(f_uuid)
        logging.info(f_uuid)
        path = Path(dir) / str(f_uuid) 
        return Database(path)

################################
# Init Temporary Site Database #
################################
def init_site_db(site, _uuid="", dir=data_dir):
    """
    Initializes a site database.

    Args:
        site (str): The site URL.
        _uuid (str, optional): The UUID for the database. Defaults to an empty string.
        dir (str, optional): The directory where the database file will be created. Defaults to ".".

    Returns:
        Database: The initialized site database.
    """
    logging.info("*** init_site_db ***")
    if not _uuid:
        s_uuid=str(uuid.uuid4())
    else:
        s_uuid=str(_uuid)

    f_uuid=s_uuid+".db"
    path = Path(dir) / f_uuid 
    db = Database(path)


    if not "site" in db.table_names():
        s=db["site"]
        s.insert(
            {    
                "uuid": s_uuid,
                "urls": [site],
                "version": "",
                "major": 0,
                "schema_version": 1,
            }
            , pk='uuid'
        )


    if not "ebooks" in db.table_names():
        db["ebooks"].create({
        "uuid": str,
        "id": int,
        "library": str,  #TODO: manage libraries ids as integer to prevent library renam on remote site  
        "title": str,
        "authors": str,
        "series": str,
        "series_index": int,
        # "edition": int, 
        "language": str,
        "desc": str,
        "identifiers": str,
        "tags": str,
        "publisher": str,
        "pubdate": str,
        "last_modified": str,
        "timestamp": str,
        "formats": str,
        "cover": int,
        # "epub": int,
        # "mobi": int,
        # "pdf": int,
        # TODO: add the most common formats to avoid alter tables
        }, pk="uuid")

    if not "libraries" in db.table_names():
        db["libraries"].create({
        "id": int,    
        "names": str
        }, pk="id")


        # db.table("ebooks", pk="id")
        # db.table("ebooks", pk="id", alter=True

    return db

######################
# Format the Get URL #
######################
def get_format_url(db, book, format):
    """
    Generates the URL for a specific format of a book.

    Parameters:
        db (dict): The database containing book information.
        book (dict): The book for which to generate the URL.
        format (str): The desired format of the book.

    Returns:
        str: The URL for the specified format of the book.
    """
    logging.info("****Get Format URL Function****")
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]
    library=book['library']
    id_=str(book['id'])

    f_url = url+"/get/"+format+"/"+id_+"/"+library
    return f_url
    
########################
# Get Book Description #
########################
def get_desc_url(db, book):
    """
    Retrieves the description URL for a given book from the database.

    Parameters:
        db (Database): The database object containing the book information.
        book (dict): The book object containing the book details.

    Returns:
        str: The description URL for the given book.
    """
    logging.info("****Get Description URL Function****")
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]

    library=book['library']
    id_=str(book['id'])

    f_urls=[]

    major=  list(db['site'].rows)[0]["major"]

    if major >= 3:
        d_url =url+"#book_id="+id_+"&library_id="+library+"&panel=book_details"
    else:
        d_url =url+"/browse/book/"+id_

    return d_url

###################################
# Save Books Metadata to Database #
###################################
def save_books_metadata_from_site(db, books):
    """
    Saves the metadata of books from a website into the database.

    Args:
        db (Database): The database object where the metadata will be saved.
        books (List[dict]): A list of dictionaries representing the metadata of the books.

    Returns:
        None
    """
    logging.info("****Save Books Metadata Function****")
    uuid = list(db['site'].rows)[0]["uuid"]

    # print(uuid)
    
    ebooks_t=db["ebooks"]


    # print([c[1] for c in ebooks_t.columns])
    # for b in books:
    #     print(b['title'])
    #     ebooks_t.insert(b, alter=True)

    # ebooks_t.insert_all(books, alter=True)
    ebooks_t.insert_all(books, alter=True,  pk='uuid', batch_size=1000)
    # print([c[1] for c in ebooks_t.columns])

#################
# Load Metadata #
#################
def load_metadata(data_dir, uuid):
    """
    Load metadata from a directory using the specified UUID.

    Args:
        dir (str): The directory from which to load the metadata.
        uuid (str): The UUID of the metadata to load.

    Returns:
        None

    Raises:
        None
    """
    logging.info("****Load Metadata Function****")
    pass
######################
# Update Done Status #
######################
def update_done_status(book):
    """
    Updates the 'status' field of the given book's source based on the availability of formats.

    Args:
        book (dict): The book object containing the source and formats information.

    Returns:
        None
    """
    logging.info("****Update Done Status Function****")
    source=book['source']
    if source['status']!='ignored':
        if set(source['formats'].keys()) == set(book['formats']) & set(source['formats'].keys()):
            book['source']['status']="done"
        else: 
            book['source']['status']="todo"

############################
# Index Site List Sequence #
############################
def index_site_list_seq(file):
    """
    Indexes a list of site sequences from a file.

    Args:
        file (str): The path to the file containing the site sequences.

    Returns:
        None
    """
    logging.info("****Index Site List Sequence Function****")
    with open(file) as f:
        for s in f.readlines():
            # try: 
            #     index_ebooks(s.rstrip())
            # except:
            #     continue
            index_ebooks(s.rstrip())

###################
# Index Site List #
###################
def index_site_list(file):
    """
    Indexes a list of sites from a given file.

    Args:
        file (str): The path to the file containing the list of sites.

    Returns:
        None
    """
    logging.info("****Index Site List Function****")
    pool = Pool(40)

    with open(file) as f:
        sites = f.readlines()
        sites= [s.rstrip() for s in sites]
        print(sites)
        logging.info("Sites: "+str(sites))
        pool.map(index_ebooks_except, sites)

##########################
# Index Ebooks Exception #
##########################
def index_ebooks_except(site):
    """
    Indexes ebooks on the given site, excluding any errors that occur during indexing.

    :param site: The site to index ebooks from.
    :type site: str

    :return: None
    """
    logging.info("****Index Ebooks Exception Function****")
    try:
        index_ebooks(site)
    except Exception as e:
        print(f"Error on site: {site} - {e}")
        logging.exception(f"Error on site: {site}")

################
# Index Ebooks #
################
def index_ebooks(site, library='', start=0, stop=0, dir=data_dir, num=1000, force_refresh=False):
    """
    Generates a function comment for the given function body.

    Args:
        site (str): The site to index ebooks from.
        library (str, optional): The library to index ebooks from. Defaults to "".
        start (int, optional): The starting index. Defaults to 0.
        stop (int, optional): The stopping index. Defaults to 0.
        dir (str, optional): The directory to save the indexed ebooks. Defaults to ".".
        num (int, optional): The number of ebooks to index. Defaults to 1000.
        force_refresh (bool, optional): Whether to force a refresh of the indexed ebooks. Defaults to False.
    
    Returns:
        None
    """
    logging.info("****Index Ebooks Function****")
    #TODO old calibres don't manage libraries.  /ajax/library-info endpoint doesn't exist. It would be better to manage calibre version directly 
    
    print('Index_Ebooks from ',site + ' ' + library)
    libs=[]
    try:
        libs= get_libs_from_site(site)
    except:
        print("old lib")
        logging.error("old lib: %s", site)
        
    _uuid=str(uuid.uuid4())
    
    if libs:
        for lib in libs:
            index_ebooks_from_library(site=site, _uuid=_uuid, library=lib, start=start, stop=stop, dir=dir, num=num, force_refresh=force_refresh)   
    else:
            index_ebooks_from_library(site=site, _uuid=_uuid, start=start, stop=stop, dir=dir, num=num, force_refresh=force_refresh)   

#############################
# Index Ebooks from Library #
#############################
def index_ebooks_from_library(site, _uuid="", library='', start=0, stop=0, dir=data_dir, num=1000, force_refresh=False):
    """
    Indexes ebooks from a library on a specific site.
    
    Parameters:
    - site: The URL of the site.
    - _uuid: The UUID of the library (optional).
    - library: The name of the library (optional).
    - start: The starting index of the ebooks to index (optional).
    - stop: The stopping index of the ebooks to index (optional).
    - dir: The directory to save the indexed ebooks (optional).
    - num: The maximum number of ebooks to index (optional).
    - force_refresh: Whether to force a refresh of the indexed ebooks (optional).
    
    Returns:
    None
    """
    logging.info("****Index Ebooks From Library Function****")
    offset= 0 if not start else start-1
    num=min(1000, num)
    server=site.rstrip('/')
    api=server+'/ajax/'
    print('API = ', api)
    lib=library
    print('lib', lib)
    print('library', library)
    
    library= '/'+library if library else library
    print('library after ', library)
    timeout=15

    print(f"\nIndexing library: {lib} from server: {server} ")
    logging.info("Indexing library: %s from server: %s ", lib, server)
    # Normalize library path segment once
    lib_input = (library or '').strip()
    if lib_input:
        # Accept forms like 'main', '/main', 'search/main', '/ajax/search/main'
        lib_clean = lib_input
        for token in ['ajax/', 'search']:
            lib_clean = lib_clean.replace(token, '')
        lib_clean = lib_clean.strip('/')
        lib_segment = f'/{lib_clean}' if lib_clean else ''
    else:
        lib_segment = ''

    # Normalized library identifier to store with each book
    library_id = lib_segment.strip('/') if lib_segment else ''

    # Some servers require sort params even for count queries
    url=api+f'search{lib_segment}?num=0&sort=timestamp&sort_order=desc'
    print(f"\nGetting ebooks count of library: {lib} from server:{server} ")
    logging.info("Getting ebooks count of library: %s from server: %s ", lib, server)
    print('URL = ',url)
    
    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.RequestException as e: 
        print("Unable to open site:", url, "-", e)
        logging.info("Unable to open site: %s - %s", url, e)
        # Retry once without library segment if it was present
        if lib_segment:
            try:
                fallback_url = api+f'search?num=0&sort=timestamp&sort_order=desc'
                print('Retrying count without library segment: ', fallback_url)
                logging.info('Retrying count without library segment: %s', fallback_url)
                r=requests.get(fallback_url, verify=False, timeout=(timeout, 30))
                r.raise_for_status()
                url = fallback_url
            except Exception as e2:
                print("Fallback also failed:", e2)
                logging.info("Fallback also failed: %s", e2)
                return
        else:
            return
        # pass
    except Exception as e:
        print ("Other issue:", e)
        logging.error("Other issue: %s", e)
        return
    except :
        print("Wazza !!!!")
        logging.error("Wazza !!!!")
        sys.exit(1)
        

    # Get the total number of books from the response
    try:
        response_data = r.json()
        if "total_num" not in response_data:
            error_msg = f"Invalid response format from {url}. Missing 'total_num' in response: {response_data}"
            print(error_msg)
            logging.error(error_msg)
            return
            
        total_num = int(response_data["total_num"])
        total_num = total_num if not stop else min(total_num, stop)
        print()    
        print(f"Total count={total_num} from {server}")
        logging.info("Total count=%s from %s", total_num, server)
    except (ValueError, TypeError) as e:
        error_msg = f"Error parsing 'total_num' from response: {e}. URL: {url}. Response: {response_data}"
        print(error_msg)
        logging.error(error_msg)
        # Retry parsing after requesting without library segment if applicable
        if lib_segment:
            try:
                fallback_url = api+f'search?num=0&sort=timestamp&sort_order=desc'
                print('Retrying parse with request without library segment: ', fallback_url)
                logging.info('Retrying parse with request without library segment: %s', fallback_url)
                r=requests.get(fallback_url, verify=False, timeout=(timeout, 30))
                r.raise_for_status()
                response_data = r.json()
                total_num = int(response_data['total_num']) if 'total_num' in response_data else 0
                print(f"Total count={total_num} from {server} (fallback)")
                logging.info("Total count=%s from %s (fallback)", total_num, server)
            except Exception as e2:
                print("Fallback parse also failed:", e2)
                logging.error("Fallback parse also failed: %s", e2)
                return
        else:
            return

    # If count is zero with library segment, try once without segment to accommodate servers
    if total_num == 0 and lib_segment:
        try:
            fallback_url = api+f'search?num=0&sort=timestamp&sort_order=desc'
            print('Count is 0; retrying count without library segment: ', fallback_url)
            logging.info('Count is 0; retrying count without library segment: %s', fallback_url)
            r=requests.get(fallback_url, verify=False, timeout=(timeout, 30))
            r.raise_for_status()
            response_data = r.json()
            total_num = int(response_data['total_num']) if 'total_num' in response_data else 0
            print(f"Total count (fallback)={total_num} from {server}")
            logging.info("Total count (fallback)=%s from %s", total_num, server)
        except Exception as e2:
            print("Fallback count request failed:", e2)
            logging.error("Fallback count request failed: %s", e2)
    

    # Update book count and calculate new books in sites database
    try:
        sites_db_path = Path(dir) / "sites.db"
        print(f"\nUpdating book count in database: {sites_db_path}")
        logging.info(f"Updating book count in database: {sites_db_path}")
        
        # Check if database file exists
        if not sites_db_path.exists():
            error_msg = f"Database file not found: {sites_db_path}"
            print(error_msg)
            logging.error(error_msg)
            return
            
        try:
            sites_db = Database(sites_db_path)
            print("sites_db = ", sites_db)
            
            # Verify the sites table exists
            if "sites" not in sites_db.table_names():
                error_msg = f"'sites' table does not exist in the database: {sites_db_path}"
                print(error_msg)
                logging.error(error_msg)
                return
                
            base_url = server.rstrip('/')
            print(f"\nSearching for site record with URL: {base_url}")
            
            # Search for matching site by URL
            try:
                # First try exact URL match
                site_record = None
                exact_matches = list(sites_db.query(
                    "SELECT * FROM sites WHERE url = ? OR url = ? LIMIT 1",
                    [base_url, base_url + '/']
                ))
                
                if exact_matches:
                    site_record = dict(exact_matches[0])
                    print(f"Found exact URL match with UUID: {site_record['uuid']}")
                    logging.info(f"Found exact URL match with UUID: {site_record['uuid']}")
                else:
                    # If no exact match, try partial URL match
                    matching_sites = list(sites_db.query(
                        "SELECT * FROM sites WHERE url LIKE ?",
                        [f"%{base_url}%"]
                    ))
                    
                    if matching_sites:
                        # Use the most recently updated matching site
                        site_record = dict(max(matching_sites, key=lambda x: x.get('last_check', '') or ''))
                        print(f"Found matching site by partial URL with UUID: {site_record['uuid']}")
                        logging.info(f"Found matching site by partial URL with UUID: {site_record['uuid']}")
                
                if not site_record:
                    # Create a new site record if none found
                    print(f"No matching site found, creating new record for {base_url}")
                    logging.warning(f"No matching site found, creating new record for {base_url}")
                    
                    site_record = {
                        'uuid': str(uuid.uuid4()),
                        'url': base_url,
                        'hostnames': base_url.split('//')[-1].split('/')[0],
                        'status': 'online',
                        'last_check': datetime.datetime.utcnow().isoformat(),
                        'book_count': 0,
                        'last_book_count': 0,
                        'new_books': 0,
                        'libraries_count': 0
                    }
                    
                    # Insert the new record
                    sites_db["sites"].insert(site_record, pk='uuid', replace=True)
                    print(f"Created new site record with UUID: {site_record['uuid']}")
                    logging.info(f"Created new site record with UUID: {site_record['uuid']}")
                
                # Update _uuid to match the found/created record
                _uuid = site_record['uuid']
                
            except Exception as e:
                error_msg = f"Error searching for site record: {str(e)}"
                print(error_msg)
                logging.error(error_msg, exc_info=True)
                return
                    
                print(f"Found site record: {site_record}")
                logging.info(f"Found site record: {site_record}")
                
                # Get the previous book count if it exists
                last_count = int(site_record.get("book_count", 0))
                print(f"Previous book count: {last_count}, New total: {total_num}")
                
                # Calculate new books (only if this isn't the first run)
                new_books = max(0, total_num - last_count) if last_count > 0 else 0
                print(f"Calculated new books: {new_books}")
                
                # Prepare the update data
                update_data = {
                    "last_book_count": last_count,
                    "book_count": total_num,
                    "new_books": new_books,
                    "last_check": datetime.datetime.utcnow().isoformat()
                }
                print(f"Update data prepared: {update_data}")
                
                # Verify database connection and schema first
                try:
                    print("\n=== Database Verification ===")
                    print(f"Database path: {sites_db.conn}")
                    print("Tables in database:", sites_db.table_names())
                    
                    if 'sites' not in sites_db.table_names():
                        error_msg = "Error: 'sites' table does not exist in the database"
                        print(error_msg)
                        logging.error(error_msg)
                        return
                        
                    # Print table schema
                    print("\nSites table schema:")
                    sites_schema = sites_db['sites'].schema
                    print(sites_schema)
                    print("\nSites table columns:", sites_db['sites'].columns_dict)
                    
                    # Print first few records for verification
                    print("\nFirst few records in sites table:")
                    for row in sites_db.query("SELECT * FROM sites LIMIT 3"):
                        print(dict(row))
                    print("=== End Database Verification ===\n")
                except Exception as db_error:
                    error_msg = f"Error verifying database: {str(db_error)}"
                    print(error_msg)
                    logging.error(error_msg, exc_info=True)
                    return

                # Update the site record with new counts using direct SQL
                try:
                    # Build the SQL update query
                    set_clause = ", ".join([f"{k} = ?" for k in update_data.keys()])
                    query = f"""
                        UPDATE sites 
                        SET {set_clause}
                        WHERE uuid = ?
                    """
                    
                    # Prepare parameters (all update values + the WHERE clause value)
                    params = list(update_data.values()) + [_uuid]
                    
                    print(f"\nExecuting SQL update:\n{query}")
                    print(f"With parameters: {params}")
                    
                    # Execute the update
                    cursor = sites_db.conn.cursor()
                    cursor.execute(query, params)
                    sites_db.conn.commit()
                    
                    # Verify the update
                    cursor.execute("SELECT changes() as changes")
                    changes = cursor.fetchone()['changes']
                    print(f"Update affected {changes} row(s)")
                    
                    if changes == 0:
                        error_msg = "Warning: No rows were updated - record may not exist"
                        print(error_msg)
                        logging.warning(error_msg)
                    
                except Exception as update_error:
                    error_msg = f"Error executing SQL update: {str(update_error)}"
                    print(error_msg)
                    logging.error(error_msg, exc_info=True)
                    # Try to get more details about the error
                    try:
                        print(f"SQLite error code: {update_error.sqlite_errorcode}")
                        print(f"SQLite error name: {update_error.sqlite_name}")
                    except:
                        pass
                    return
                
                # Verify the update was successful using direct SQL
                try:
                    print("\n=== Verifying Update ===")
                    
                    # Get record before update using direct SQL
                    cursor = sites_db.conn.cursor()
                    cursor.execute("SELECT * FROM sites WHERE uuid = ?", (_uuid,))
                    before_record = dict(cursor.fetchone() or {})
                    print("Record before update:", before_record)
                    
                    # Perform the update using direct SQL
                    set_clause = ", ".join([f"{k} = ?" for k in update_data.keys()])
                    query = f"""
                        UPDATE sites 
                        SET {set_clause}
                        WHERE uuid = ?
                        RETURNING *  -- Return the updated record
                    """
                    params = list(update_data.values()) + [_uuid]
                    
                    print(f"\nExecuting update with query:\n{query}")
                    print(f"With parameters: {params}")
                    
                    cursor.execute(query, params)
                    updated_record = dict(cursor.fetchone() or {}) if cursor.description else {}
                    sites_db.conn.commit()
                    
                    if not updated_record:
                        error_msg = "Update failed - no record was updated"
                        print(error_msg)
                        logging.error(error_msg)
                        return
                    
                    print("\nRecord after update:", updated_record)
                    
                    # Verify each field was updated correctly
                    success = True
                    for field, expected_value in update_data.items():
                        actual_value = updated_record.get(field)
                        if str(actual_value) != str(expected_value):
                            print(f"❌ Mismatch in {field}: Expected {expected_value}, got {actual_value}")
                            success = False
                        else:
                            print(f"✅ {field} matches: {actual_value}")
                    
                    if success:
                        success_msg = f"✅ Successfully updated book count: {last_count} → {total_num} (+{new_books} new) for {server}"
                        print(f"\n{success_msg}")
                        logging.info(success_msg)
                    else:
                        error_msg = "❌ Update completed but some fields don't match expected values"
                        print(f"\n{error_msg}")
                        logging.error(error_msg)
                        
                    # Print final state of the record
                    cursor.execute("SELECT * FROM sites WHERE uuid = ?", (_uuid,))
                    final_record = dict(cursor.fetchone() or {})
                    print("\nFinal record state:", final_record)
                    
                except Exception as verify_error:
                    error_msg = f"Error verifying update: {str(verify_error)}"
                    print(error_msg)
                    logging.error(error_msg, exc_info=True)
                    
                    # Print database state for debugging
                    try:
                        cursor = sites_db.conn.cursor()
                        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='sites'")
                        print("\nTable schema:", cursor.fetchone()['sql'])
                        cursor.execute("SELECT * FROM sites WHERE uuid = ?", (_uuid,))
                        print("Current record:", dict(cursor.fetchone() or {}))
                    except Exception as db_err:
                        print(f"Error getting database state: {db_err}")
                        
                except Exception as update_error:
                    error_msg = f"Error updating record in database: {str(update_error)}"
                    print(error_msg)
                    logging.error(error_msg, exc_info=True)
                    
                    # Print database schema for debugging
                    try:
                        print("\nDatabase schema:")
                        schema = sites_db["sites"].schema
                        print(f"Table schema: {schema}")
                        print(f"Table columns: {sites_db['sites'].columns_dict}")
                    except Exception as schema_error:
                        print(f"Error getting schema: {schema_error}")
                    
            except Exception as record_error:
                error_msg = f"Error accessing site record: {str(record_error)}"
                print(error_msg)
                logging.error(error_msg, exc_info=True)
                
        except Exception as db_error:
            error_msg = f"Database error: {str(db_error)}"
            print(error_msg)
            logging.error(error_msg, exc_info=True)
            
    except Exception as e:
        error_msg = f"Unexpected error updating book count in sites database: {str(e)}"
        print(error_msg)
        logging.error(error_msg, exc_info=True)

    db=init_site_db(site, _uuid=_uuid, dir=dir)
    r_site = (list(db['site'].rows)[0])
    r_site['version']=r.headers['server']
    r_site['major']=int(re.search(r'calibre.(\d).*', r.headers['server']).group(1))
    db["site"].upsert(r_site, pk='uuid')

    print()

    range=offset+1
    while offset < total_num:
        remaining_num = min(num, total_num - offset)
        print ('\r {:180.180}'.format(f'Downloading ids: offset={str(offset)} count={str(remaining_num)} from {server}'), end='')
        logging.info("Downloading ids: offset=%s count=%s from %s", str(offset), str(remaining_num), server)
        # url=server+base_url+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'
        url=api+'search'+library+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'

        # print("->", url)
        try:
            r=requests.get(url, verify=False, timeout=(timeout, 30))
            r.raise_for_status()
        except requests.RequestException as e: 
            print ("Connection issue:", e)
            logging.error("Connection issue: %s", e)
            return
            # pass
        except Exception as e:
            print ("Other issue:", e)
            logging.error("Other issue: %s", e)
            return
            # pass
        except :
            print ("Wazza !!!!")
            logging.error("Wazza !!!!")
            return

        print ('\r {:180.180}'.format(f'Downloading metadata from {str(offset+1)} to {str(offset+remaining_num)}/{total_num} from {server}'), end='')
        logging.info("Downloading metadata from %s to %s/%s from %s", str(offset+1), str(offset+remaining_num), total_num, server)
        books_s=",".join(str(i) for i in r.json()['book_ids'])
        url=api+'books'+library+'?ids='+books_s

        try:
            r = requests.get(url, verify=False, timeout=(60, 60))
            r.raise_for_status()
        except requests.RequestException as e:
            error_msg = f"Error fetching book details from {url}: {str(e)}"
            print(f"\n❌ {error_msg}")
            logging.error(error_msg, exc_info=True)
            return
        print ('\r {:180.180}'.format(f'{len(r.json())} received'), end='')
        logging.info("%s received", len(r.json()))        
        
        books=[]
        for id, r_book in r.json().items():                
            uuid=r_book['uuid']
            if not uuid:
                print ("No uuid for ebook: ignored")
                logging.info("No uuid for ebook: ignored")
                continue 


            if r_book['authors']:
                desc= f"({r_book['title']} / {r_book['authors'][0]})"
            else:
                desc= f"({r_book['title']})"

            print ('\r {:180.180} '.format(f'{range}/{total_num} ({server} : {uuid} --> {desc}'), end='')
            logging.info("%s/%s (%s : %s --> %s)", range, total_num, server, uuid, desc)

            if not force_refresh:
                try:
                    book = load_metadata(dir, uuid)
                except Exception as e:
                    error_msg = f"Error loading metadata for {uuid}: {str(e)}"
                    print(f"\n❌ {error_msg}")
                    logging.error(error_msg, exc_info=True)
                    range += 1
                    continue
                if book:
                    print("Metadata already present for:", uuid)
                    logging.error("Metadata already present for: %s", uuid)
                    range+=1
                    continue

            if not r_book['formats']:
                # print("No format found for {}".format(r_book['uuid']))
                range+=1
                continue

            book={}
            book['uuid']=r_book['uuid']
            book['id']=id
            book['library']=lib

            # book['title']=r_book['title']
            book['title']=unidecode.unidecode(r_book['title'])
            # book['authors']=r_book['authors']

            if r_book['authors']:
                book['authors']=[unidecode.unidecode(s) for s in r_book['authors']]
            # book['desc']=""

            book['desc']=r_book['comments']

            if r_book['series']:
                book['series']=unidecode.unidecode(r_book['series'])
                # book['series']=[unidecode.unidecode(s) for s in r_book['series']]
            s_i=r_book['series_index']
            if (s_i): 
                book['series_index']=int(s_i)

            # book['edition']=0

            book['identifiers']=r_book['identifiers']

            # book['tags']=r_book['tags']
            if r_book['tags']:
                book['tags']=[unidecode.unidecode(s) for s in r_book['tags']]

            book['publisher']=r_book['publisher']
            # book['publisher']=unidecode.unidecode(r_book['publisher'])

            book['pubdate']=r_book['pubdate']

            if not r_book['languages']:
            # if True:
                text=r_book['title']+". "
                if r_book['comments']:
                    text=r_book['comments']                    
                s_language, prob=identifier.classify(text)
                if prob >= 0.85:
                    language =  iso639.to_iso639_2(s_language)
                    book['language']=language
                else:
                    book['language']=''
            else:
                book['language']=iso639.to_iso639_2(r_book['languages'][0])

            if r_book['cover']:
                book['cover']= True
            else:
                book['cover']= False

            book['last_modified']=r_book['last_modified']
            book['timestamp']=r_book['timestamp']

            book['formats']=[]
            formats=r_book['formats']
            for f in formats:                    
                if 'size' in r_book['format_metadata'][f]:
                    size=int(r_book['format_metadata'][f]['size'])
                else:
                    # print()
                    # print(f"Size not found for format '{f}'  uuid={uuid}: skipped")
                    pass
                    #TODO query the size when the function to rebuild the full url is ready
                    #   
                    # print("Trying to get size online: {}".format('url'))
                    # try:
                    #     size=get_file_size(s['url'])
                    # except:
                    #     print("Unable to access size for format '{}' : {} skipped".format(f, uuid))
                    #     continue
                book[f]=(size)
                book['formats'].append(f)

            if not book['formats']:
            # if not c_format:
                # print()
                # print(f"No format found for {book['uuid']} id={book['id']} : skipped")
                range+=1
                # continue


            books.append(book)
            range+=1

        # print()
        print("Saving metadata")
        logging.info("Saving metadata")
        print ('\r {:180.180}'.format(f'Saving metadata from {server}'), end='')
        logging.info("Saving metadata from %s", server)
        try:
            save_books_metadata_from_site(db, books)
            print('\r {:180.180}'.format(f'--> Saved {range-1}/{total_num} ebooks from {server}'), end='')
            logging.info("--> Saved %s/%s ebooks from %s", range-1, total_num, server)
        except BaseException as err:
            print (err)
            logging.error(err)

        print()
        print()

        # try:
        #     save_metadata(db, books)
        # except:
        #     print("Unable to save book metadata")

        offset=offset+num
        
###########################
# Query Books in Database #
###########################
def query(query_str="", dir=data_dir):
    """
    This function takes in an optional query string and directory path and performs a query on a list of databases located in the specified directory.
    
    Parameters:
        query_str (str): The query string to be used for searching the databases. Default is an empty string.
        dir (str): The directory path where the databases are located. Default is the current directory.
    
    Returns:
        None
    """
    logging.info("****Query Function****")
    dbs=[]
    for path in os.listdir(dir):
        db = Database(path)
        # print (db["ebooks"].count)
        # for row in db["site"].rows:
        #     print (f'{row["urls"]}: {db["ebooks"].count}')
        # db["ebooks"].search(query_str)
        # url=db['site'].get(1)['urls'][0]
        url=db['site'].get(1)
        print (url)
        logging.info("Querying %s",url)

        for ebook in db["ebooks"].rows_where(query_str):
            # print (f"{ebook['title']} ({ebook['uuid']})")
            print (ebook)
            logging.info("Found %s (%s)", ebook['title'], ebook['uuid'])

###########################
# Get Stats on EBook Type #
###########################
def get_stats(dir=data_dir):
    """
    Retrieves statistics about the ebooks in the specified directory.

    Parameters:
        dir (str): The directory to search for ebooks. Defaults to the current directory.

    Returns:
        None
    """
    logging.info("****Get Stats Function****")
    dbs=[]
    size=0
    count=0
    for f in os.listdir(dir):
        if not f.endswith(".db"):
            continue
        if f == "index.db":
            continue
        path = Path(dir) / f 
        dbs.append(Database(path))

    for db in dbs:
        for i, ebook in enumerate(db["ebooks"].rows):
            uuid=ebook['uuid']
            title=ebook['title']
            formats=json.loads(ebook['formats'])
            # print(formats)
            for f in formats:
                if f in ebook:
                    if ebook[f]:
                        size+=ebook[f]
                        count+=1
                        # print (f'\r{count} {f} --> {uuid}: {title}', end ='')
                        # print (f'\r{count} : {uuid} --> {f}', end='')
                        print (f'\r{count} formats - ebook : {uuid}', end='')
                        logging.info("Found %s (%s)", title, uuid)

    print()
    print("Total count of formats:", humanize.intcomma(count)) 
    logging.info("Total count of formats: %s", humanize.intcomma(count))
    print("Total size:", hsize(size)) 
    logging.info("Total size: %s", hsize(size))
    print()

#######################
# Initialize Index.db #
#######################
def init_index_db(dir=data_dir):
    """
    Initializes an index database in the specified directory.
    
    Args:
        dir (str): The directory where the index database should be created. Defaults to the current directory.
        
    Returns:
        Database: The initialized index database.
    """
    
    logging.info("****Initialize Index Function****")
    path = Path(dir) / "index.db" 
    
    db_index = Database(path)
    if not "summary" in db_index.table_names():
        db_index["summary"].create({
        "uuid": str,
        "cover": str,
        "title": str,
        # "source": str
        "authors": str,
        "year": str,
        "series": str,
        "language": str,
        "links": str,
        # "desc": str,
        "publisher": str,
        "tags": str,
        "identifiers": str,
        "formats": str
        }
        # )
        , pk="uuid")

        # db_index.table("index", pk="uuid")
        # db_index.table("summary").enable_fts(["title"])
        # db_index["summary"].enable_fts(["title", "authors", "series", "uuid", "language", "identifiers", "tags", "publisher", "formats", "pubdate"])
        db_index["summary"].enable_fts(["title", "authors", "series", "language", "identifiers", "tags", "publisher", "formats", "year"])

    return db_index

###################
# Get Book Covers #
###################
def get_img_url(db, book):
    """
    Generates the URL of an image based on the database and book information.

    Parameters:
        db (dict): The database containing the site information.
        book (dict): The book information.

    Returns:
        str: The URL of the image.
    """
    logging.info("****Get Img URL Function****")
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]

    library=book['library']
    id_=str(book['id'])

    f_urls=[]

    major=  list(db['site'].rows)[0]["major"]

    if major >= 3:
        d_url =url+"/get/thumb/"+id_+"/"+library+ "?sz=600x800"
    else:
        # d_url =url+"/get/thumb/"+id_
        d_url =url+"/get/thumb_90_120/"+id_

    return d_url

################
# Build Index  #
################
def build_index(dir=data_dir):
    """
    Builds an index for English ebooks in the given directory.

    Args:
        dir (str): The directory to search for ebook databases. Defaults to the current directory.
        english (bool): Flag to indicate whether to include only English ebooks in the index. Defaults to True.

    Returns:
        None
    """
    dir=data_dir
    logging.info("****Build Index Function****")
    dbs=[]
    for f in os.listdir(dir):
        if not f.endswith(".db"):
            continue
        if f in ("index.db", "sites.db"):
            continue
        p = Path(dir) / f 
        print(f)
        try:
            db = Database(p.resolve())
        except:
            print ("Pb with:", f)
            logging.info("Pb with: %s", f)
        dbs.append(db)
    
    db_index = init_index_db(dir=dir)
    index_t=db_index["summary"]

    batch_size=10000
    count=0
    summaries=[]

    for db in dbs:
        for i, ebook in enumerate(db["ebooks"].rows):
#            if english and (not ebook['language'] or ebook['language'] != "eng"):
#                continue
#            elif not english and ebook['language'] == "eng":
#                continue

            if ebook['authors']: 
                ebook['authors']=formats=json.loads(ebook['authors'])
            # if ebook['series']:    
            #     ebook['series']=formats=json.loads(ebook['series'])
            if ebook['identifiers']:
                ebook['identifiers']=formats=json.loads(ebook['identifiers'])
            if ebook['tags']: 
                ebook['tags']=formats=json.loads(ebook['tags'])
            ebook['formats']=formats=json.loads(ebook['formats'])
            ebook['links']=""
            summary = {k: v for k, v in ebook.items() if k in ("uuid","title", "authors", "series", "language", "formats", "tags", "publisher", "identifiers")}
            # summary = {k: v for k, v in ebook.items() if k in ("uuid","title", "authors", "series", "identifiers", "language", "tags", "publisher", "formats")}
            summary['title']={'href': get_desc_url(db, ebook), 'label': ebook['title']}

            summary["cover"]= {"img_src": get_img_url(db, ebook), "width": 90}

            formats=[]
            for f in ebook['formats']:
                formats.append({'href': get_format_url(db, ebook, f), 'label': f"{f} ({hsize(ebook[f])})"})
            summary['links']=formats
            
            pubdate=ebook['pubdate'] 
            summary['year']=pubdate[0:4] if pubdate else "" 
            summaries.append(summary)
            # print(summary)
            logging.info("Summary: %s", summary)
            count+=1
            print (f"\r{count} - ebook handled: {ebook['uuid']}", end='')
            logging.info(f"\r{count} - ebook handled: {ebook['uuid']}")
            if not count % batch_size:
                # print()
                # print(f"Saving summary by batch: {len(summaries)}")    
                # print(summaries)
                # Prefer upsert to avoid UNIQUE constraint errors when merging multiple site DBs
                try:
                    index_t.upsert_all(summaries, batch_size=batch_size, pk='uuid')
                except Exception as e:
                    print()
                    print("Upsert batch failed; attempting insert_all as fallback (may hit UNIQUE errors)")
                    logging.error("Upsert batch failed: %s", e)
                    try:
                        index_t.insert_all(summaries, batch_size=batch_size)
                    except Exception as e2:
                        print("insert_all also failed:", e2)
                        logging.error("insert_all also failed: %s", e2)
                    print()

                # print("Saved")
                # print()
                summaries=[]

    # print()
    # print("saving summary")    
    # Final flush: upsert remaining rows by uuid
    try:
        index_t.upsert_all(summaries, batch_size=batch_size, pk='uuid')
    except Exception as e:
        print("Final upsert failed; attempting insert_all:", e)
        logging.error("Final upsert failed: %s", e)
        try:
            index_t.insert_all(summaries, batch_size=batch_size)
        except Exception as e2:
            print("Final insert_all failed:", e2)
            logging.error("Final insert_all failed: %s", e2)
    # print("summary done")
    # print()
    
    print()
    print("fts")
    logging.error("fts")
    index_t.populate_fts(["title", "authors", "series", "identifiers", "language", "tags", "publisher", "formats", "year"])
    print("fts done")
    logging.error("fts done")

############################
# Search books in Index.db #
############################
def search(query_str, dir=data_dir, links_only=False):
    """
    Search for ebooks in the specified directory using a query string.
    
    Args:
        query_str (str): The query string to search for.
        dir (str, optional): The directory to search in. Defaults to ".".
        links_only (bool, optional): Whether to only print the download links. Defaults to False.
    
    Returns:
        None
    """
    logging.info("****Search Function****")
    path = Path(dir) / "index.db" 
    db_index = Database(path)
    # table=db_index["summary"]
    # rows=table.search(query_str)
    # print(rows)
    sites=set()
    ebook_ids=[]
    for ebook in db_index["summary"].search(query_str):
        sites.add(ebook[-1])
        ebook_ids.append((ebook[3], ebook[-1]))
        # print (ebook)
    # print("sites:", sites) 
    # print("ebooks:", ebook_ids) 

    site_dbs={}
    for s in sites:
        f_uuid=s+".db"
        path = Path(dir) / f_uuid 
        site_dbs[s]=Database(path)
        # print(site_dbs[s].tables)
    
    for e in ebook_ids:
        # ebook=site_dbs[e[1]]["ebooks"].get(e[0])
        # print("ebook:", ebook)
        db=site_dbs[e[1]] 
        # ebooks=db.conn.execute("select * from ebooks").fetchone()
        ebook=db.conn.execute(f'select * from ebooks where uuid="{e[0]}"').fetchone()
        url=json.loads(db['site'].get(1)['urls'])[0]
        library=db['site'].get(1)['library']
        formats=json.loads(ebook[14])
        id_=str(ebook[0])

        if not links_only:
            print()
            print("Title:", ebook[2])
            logging.info("Title: %s", ebook[2])
            print("Author:", ebook[3])
            logging.info("Author: %s", ebook[3])
            print("Serie:", ebook[4])
            logging.info("Serie: %s", ebook[4])
            print("Formats:", formats)
            logging.info("Formats: %s", formats)

        for f in formats:
            print(url+"get/"+f+"/"+id_+"/"+library)
            logging.info(url+"get/"+f+"/"+id_+"/"+library)
            
#########################
# Index.db to JSON file #
#########################
# https://stackoverflow.com/questions/26692284/how-to-prevent-brokenpipeerror-when-doing-a-flush-in-python
def index_to_json(dir=data_dir):
    """
    Converts the index database to a JSON file.

    Args:
        dir (str): The directory containing the index database file. Defaults to the current directory.

    Returns:
        None
    """
    logging.info("****Index to JSON Function****")
    path = Path(dir) / "index.db" 
    db = Database(path)

    # sys.stdout.flush()

    try: 
        for row in db["summary"].rows:
            if row['title']:
                row['title']=json.loads(row['title'])
            if row['authors']:
                row['authors']=json.loads(row['authors'])
#            if row['series']:
#                row['series']=json.loads(row['series']) 
            if row['links']:
                row['links']=json.loads(row['links'])
            if row['tags']:
                row['tags']=json.loads(row['tags'])
            if row['identifiers']:
                row['identifiers']=json.loads(row['identifiers'])
            if row['formats']:
                row['formats']=json.loads(row['formats'])

            json.dump(row, sys.stdout)
            sys.stdout.flush()
            # return
    except BrokenPipeError:
        logging.error("BrokenPipeError")
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1) 

######################
# Initialize Diff.db #
######################
def init_diff_db(dir=data_dir):
    """
    Initializes a new database for storing diff information.

    Args:
        dir (str): The directory where the database file should be created. Defaults to the current directory.

    Returns:
        Database: The initialized diff database.

    Raises:
        None
    """
    logging.info("****Initialize Diff Function****")
    path = Path(dir) / "diff.db" 
    
    db_diff = Database(path)
    if not "summary" in db_diff.table_names():
        db_diff["summary"].create({
        "uuid": str,
        "title": str,
        # "cover": str,
        # "source": str
        "authors": str,
        "year": str,
        "series": str,
        "language": str,
        "links": str,
        # "desc": str,
        "publisher": str,
        "tags": str,
        "identifiers": str,
        "formats": str,
        "status": str,
        "old_location":str
        }
        # )
        , pk="uuid")

    return db_diff

#######################
# Difference Function #
#######################
def diff(old, new, dir=data_dir, ):
    """
    Generate the function comment for the given function body in a markdown code block with the correct language syntax.

    :param old: The old value.
    :param new: The new value.
    :param dir: The directory where the files are located. Defaults to ".".
    """
    logging.info("****Diff Function****")
    path = Path(dir) / old 
    db_old = Database(path)

    path = Path(dir) /  new 
    db_new = Database(path)

    path = Path(dir) / "diff.db"
    db_diff =init_diff_db(dir)

    for i, n_book in enumerate(db_new["summary"].rows):
        n_uuid = n_book['uuid']
        print(i, n_uuid)
        logging.info("%s %s", i, n_uuid)
        try:
            o_book = db_old["summary"].get(n_uuid)
            # print(n_uuid, '=OK')
            o_loc=json.loads(o_book['title'])['href']
            n_loc=json.loads(n_book['title'])['href']
            if o_loc != n_loc :
                print(n_uuid, 'MOVED')
                logging.info("%s MOVED", n_uuid)
                n_book["status"]="MOVED"
                n_book["old_location"]=o_loc
                n_book.pop ('cover', None)
                db_diff["summary"].insert(n_book, pk='uuid')                  
        except:
            # print(n_uuid, '=NOK')
            n_book.pop ('cover', None)
            logging.error("%s =NOK", n_uuid)
            n_book["status"]="NEW"
            logging.error("%s NEW", n_uuid)
            db_diff["summary"].insert(n_book, pk='uuid')
            logging.error("%s inserted", n_uuid)


############################
# Query Calibre by Country #
############################
def calibre_by_country(country, max_servers=1000, max_retries=5):
    """
    Generates a list of web-Calibre servers by country and saves them to a file.

    Args:
        country (str): The country for which to generate the list of web-Calibre servers.
        max_servers (int): Maximum number of servers to return. Default is 1000.
        max_retries (int): Maximum number of retry attempts for API calls. Default is 5.

    Returns:
        None
    """
    logging.info("****Calibre by Country Function****")
    apiquery = 'calibre http.status:200 has_ipv6:false has_ssl:false ssl.cert.expired:false has_screenshot:false country:"' + country + '"'
    filename = "./data/" + country + ".txt"
    servers_found = 0
    page = 1
    
    print(f"Searching for up to {max_servers} Calibre servers in {country}")
    logging.info("Starting search for country: %s", country)
    
    # Ensure the data directory exists
    Path("./data").mkdir(exist_ok=True)
    
    try:
        with open(filename, 'w') as csvfile:
            while servers_found < max_servers:
                retry_count = 0
                success = False
                
                while retry_count <= max_retries and not success:
                    try:
                        # Add a small delay between API calls
                        if retry_count > 0:
                            delay = 2 ** retry_count  # Exponential backoff
                            print(f"Waiting {delay} seconds before retry...")
                            time.sleep(delay)
                        
                        print(f"Querying Shodan API (page {page}, attempt {retry_count + 1}/{max_retries + 1})")
                        results = api.search(apiquery, page=page)
                        success = True
                        
                    except (shodan.APIError, json.JSONDecodeError) as e:
                        retry_count += 1
                        if retry_count > max_retries:
                            print(f"Error: Max retries reached for page {page}")
                            logging.error("Max retries reached for page %d: %s", page, str(e))
                            raise
                        print(f"Error on page {page}, retrying... ({retry_count}/{max_retries})")
                        logging.warning("Error on page %d (attempt %d): %s", page, retry_count, str(e))
                    except Exception as e:
                        print(f"Unexpected error: {str(e)}")
                        logging.error("Unexpected error: %s", str(e))
                        raise
                
                if not success:
                    break
                
                if not results.get('matches'):
                    print("No more results found.")
                    break
                
                for result in results['matches']:
                    if servers_found >= max_servers:
                        break
                        
                    try:
                        # Check if the server is insecure (not using HTTPS)
                        if 'https' not in result.get('data', ''):
                            ip = result.get('ip_str', 'unknown')
                            port = str(result.get('port', 'unknown'))
                            country_name = result.get('location', {}).get('country_name', 'unknown')
                            
                            server_row = f"http://{ip}:{port}\n"
                            
                            # Log and write the server
                            print(f"Found server {servers_found + 1}/{max_servers}: {server_row.strip()}")
                            logging.info("Found server: %s in %s", server_row.strip(), country_name)
                            
                            csvfile.write(server_row)
                            csvfile.flush()  # Ensure data is written to disk
                            servers_found += 1
                            
                    except Exception as e:
                        logging.error("Error processing server result: %s", str(e))
                        continue
                
                # Move to next page if we haven't found enough servers
                if not results.get('matches') or len(results['matches']) < 100:
                    break
                    
                page += 1
        
        # Import the found servers
        if servers_found > 0:
            print(f"\nFound {servers_found} servers. Importing to database with country code: {country}")
            logging.info("Importing %d servers from %s with country code: %s", 
                        servers_found, filename, country)
            import_urls_from_file(filename, country=country)
        else:
            print("No servers found or an error occurred.")
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        logging.error("Error in calibre_by_country: %s", str(e))
    
    print("Search completed.")
    return()

#############################################
# Query CalibreWeb server from Server Table #
# And write each server to the server table #
#############################################
def book_search(country):
    """
    Function to search for books based on the country.

    Args:
        country (str): The name of the country to search for books.

    Returns:
        None
    """
    print("Starting Book Search for Country: ", country)
    logging.info("Starting Book Search for Country: %s", country)
    logging.info("****Book Search Function****")
    import_urls_from_file(data_dir + country + '.txt')
    print("Ending Book Search for: ", country)
    logging.info("Ending Book Search for: %s", country)
    return()

############################################
# Output to text file all servers that are #
# marked as online in database.            #
############################################
def output_online_db():
    """
    Retrieves the URLs of all online sites from the database and saves them to a file.

    Parameters:
    None

    Returns:
    None
    """
    logging.info("****Output Online DB Function****")
    script = "select url from sites where status='online'"
    print("site_conn = ", site_conn)
    df = pd.read_sql(script, site_conn)
    df.to_csv(data_dir + 'online.txt', header=False, index=False)
    return()
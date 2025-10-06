#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import sqlite3
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import re
import calishot_logging

# --- Build Info ---
VERSION = "1.0.0"
COMMIT = ""
DATE = ""
BUILT_BY = ""

# --- Database Paths ---
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SITES_DB_PATH = os.path.join(_BASE_DIR, "data", "sites.db")
INDEX_DB_PATH = os.path.join(_BASE_DIR, "data", "index.db")

# --- Logging Setup ---
def setup_logging(verbose: bool):
    """Initialize shared logging once with desired verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    calishot_logging.init_logging(level)

# --- Database Connections ---
def get_sites_db_conn():
    return sqlite3.connect(SITES_DB_PATH, timeout=30, check_same_thread=False)

def get_index_db_conn():
    conn = sqlite3.connect(INDEX_DB_PATH, timeout=30, check_same_thread=False)
    try:
        conn.execute('PRAGMA journal_mode=WAL;')
    except Exception:
        pass
    return conn

# --- CLI Handlers ---
def handle_version(args):
    print(f"Demeter {VERSION}")
    print(f"Build date: {DATE}")
    print(f"Commit hash: {COMMIT}")
    print(f"Built by: {BUILT_BY}")

def handle_dl_list(args):
    conn = get_index_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM summary")
        total = cur.fetchone()[0]
        print(f"Total downloads: {total}")
    except Exception as e:
        print(f"Error listing downloads: {e}")
    finally:
        conn.close()

def handle_dl_add(args):
    conn = get_index_db_conn()
    cur = conn.cursor()
    now = datetime.now().isoformat()
    for hash_ in args.bookhash:
        try:
            # Add missing fields as needed for 'summary' table
            cur.execute("INSERT INTO summary (uuid, title, authors, formats) VALUES (?, ?, ?, ?)", (hash_, 'Unknown Title', 'Unknown Author', 'epub'))
            print(f"Book {hash_} has been added to the database")
        except Exception as e:
            print(f"Could not save {hash_}: {e}")
    conn.commit()
    conn.close()

def handle_dl_deleterecent(args):
    conn = get_index_db_conn()
    cur = conn.cursor()
    try:
        cutoff = datetime.now() - timedelta(hours=float(args.hours))
        cur.execute("SELECT uuid, year FROM summary")
        deleted = 0
        scanned = 0
        for row in cur.fetchall():
            scanned += 1
            book_uuid, year = row
            # year field may not be a timestamp; this is a placeholder logic
            # If you have a timestamp field, use it instead
            # Example: added_time = datetime.fromisoformat(added)
            # if added_time > cutoff:
            #     deleted += 1
        print(f"Would have scanned {scanned} books (deletion not implemented, see code comment)")
    except Exception as e:
        print(f"Error deleting recent downloads: {e}")
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="demeter is CLI application for scraping calibre hosts and retrieving books in epub format that are not in your local library.")
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose output')
    subparsers = parser.add_subparsers(dest='command')

    # version
    parser_version = subparsers.add_parser('version', help='Shows version information')
    parser_version.set_defaults(func=handle_version)

    # dl group
    parser_dl = subparsers.add_parser('dl', help='download related commands')
    dl_subparsers = parser_dl.add_subparsers(dest='dl_command')

    parser_dl_list = dl_subparsers.add_parser('list', help='list all downloads')
    parser_dl_list.set_defaults(func=handle_dl_list)

    parser_dl_add = dl_subparsers.add_parser('add', help='add a number of hashes to the database')
    parser_dl_add.add_argument('bookhash', nargs='+', help='book hashes to add')
    parser_dl_add.set_defaults(func=handle_dl_add)

    parser_dl_delrecent = dl_subparsers.add_parser('deleterecent', help='delete all downloads from this time period')
    parser_dl_delrecent.add_argument('hours', help='hours to look back, e.g. 24 for 24h')
    parser_dl_delrecent.set_defaults(func=handle_dl_deleterecent)

    # host group
    parser_host = subparsers.add_parser('host', help='all host related commands')
    host_subparsers = parser_host.add_subparsers(dest='host_command')

    parser_host_list = host_subparsers.add_parser('list', help='list all hosts')
    parser_host_list.set_defaults(func=handle_host_list)

    # list-all shows every row without filtering (debugging)
    parser_host_list_all = host_subparsers.add_parser('list-all', help='list all hosts (no filters)')
    parser_host_list_all.set_defaults(func=handle_host_list_all)

    parser_host_add = host_subparsers.add_parser('add', help='add one or more hosts to the scrape list')
    parser_host_add.add_argument('hosturl', nargs='+', help='host urls to add')
    parser_host_add.set_defaults(func=handle_host_add)

    parser_host_del = host_subparsers.add_parser('rm', help='delete a host')
    parser_host_del.add_argument('hostid', help='host id to remove')
    parser_host_del.set_defaults(func=handle_host_rm)

    parser_host_enable = host_subparsers.add_parser('enable', help='make a host active')
    parser_host_enable.add_argument('--enable-all', action='store_true', help='enable all hosts currently marked online')
    parser_host_enable.add_argument('--enable-country', metavar='CC', help='country abbreviation (e.g., US, DE) to enable all hosts in that country (sets active=1)')
    parser_host_enable.add_argument('hostid', nargs='?', help='host id to enable (omit when using --enable-all/--enable-country)')
    parser_host_enable.set_defaults(func=handle_host_enable)


    parser_host_disable = host_subparsers.add_parser('disable', help='disable a host')
    parser_host_disable.add_argument('--disable-all', action='store_true', help='disable all hosts where active=1')
    parser_host_disable.add_argument('hostid', nargs='?', help='host id to disable (omit when using --disable-all)')
    parser_host_disable.set_defaults(func=handle_host_disable)

    parser_host_detail = host_subparsers.add_parser('stats', help='get host stats')
    parser_host_detail.add_argument('hostid', help='host id for stats')
    parser_host_detail.set_defaults(func=handle_host_stats)

    # scrape group
    parser_scrape = subparsers.add_parser('scrape', help='all scrape related commands')
    scrape_subparsers = parser_scrape.add_subparsers(dest='scrape_command')

    parser_scrape_run = scrape_subparsers.add_parser('run', help='run all scrape jobs')
    parser_scrape_run.add_argument('-e', '--extension', default='epub', help='extension of files to download (default: epub)')
    parser_scrape_run.add_argument('-d', '--outputdir', default='books', help='path to downloaded books (default: books)')
    parser_scrape_run.add_argument('-a', '--authors', default=None, help='SQL LIKE pattern for authors filter (use quotes), e.g. "%King%"')
    parser_scrape_run.add_argument('-t', '--titles', default=None, help='SQL LIKE pattern for title filter (use quotes), e.g. "%Dune%"')
    parser_scrape_run.set_defaults(func=handle_scrape_run)

    parser_scrape_results = scrape_subparsers.add_parser('results', help='show scrape results')
    parser_scrape_results.set_defaults(func=handle_scrape_results)

    args = parser.parse_args()
    setup_logging(args.verbose)

    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()

# --- Host Handlers ---
def handle_host_list(args):
    conn = get_sites_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT demeter_id, uuid, url, status, active, downloads, scrapes
            FROM sites
            WHERE status IN ('online', 'unknown') AND active=1
            ORDER BY demeter_id ASC
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("No active hosts were found.")
        else:
            for row in rows:
                print(f"demeter_id: {row[0]}, UUID: {row[1]}, URL: {row[2]}, Status: {row[3]}, Active: {row[4]}, Downloads: {row[5]}, Scrapes: {row[6]}")
    except Exception as e:
        print(f"Error listing hosts: {e}")
    finally:
        conn.close()

def handle_host_list_all(args):
    conn = get_sites_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT demeter_id, uuid, url, status, active, downloads, scrapes
            FROM sites
            ORDER BY demeter_id ASC
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("No hosts were found in sites table.")
            return
        for row in rows:
            print(f"demeter_id: {row[0]}, UUID: {row[1]}, URL: {row[2]}, Status: {row[3]}, Active: {row[4]}, Downloads: {row[5]}, Scrapes: {row[6]}")
    except Exception as e:
        print(f"Error listing all hosts: {e}")
    finally:
        conn.close()

def handle_host_add(args):
    conn = get_sites_db_conn()
    cur = conn.cursor()
    # Find current max 
    cur.execute("SELECT MAX(demeter_id) FROM sites")
    row = cur.fetchone()
    next_id = (row[0] or 0) + 1
    for hosturl in args.hosturl:
        try:
            # New hosts start as unknown, activation is controlled by the 'active' flag
            cur.execute("INSERT INTO sites (uuid, url, status, demeter_id) VALUES (?, ?, ?, ?)", (hosturl, hosturl, 'unknown', next_id))
            print(f"Host {hosturl} has been added to the database with demeter_id {next_id}")
            next_id += 1
        except Exception as e:
            print(f"Error adding host {hosturl}: {e}")
            continue
    conn.commit()
    conn.close()

def handle_host_rm(args):
    conn = get_sites_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM sites WHERE demeter_id=?", (args.hostid,))
        print(f"Host with demeter_id {args.hostid} has been removed from the database")
    except Exception as e:
        print(f"Error removing host: {e}")
    finally:
        conn.commit()
        conn.close()


def handle_host_enable(args):
    conn = get_sites_db_conn()
    cur = conn.cursor()
    try:
        # Enable all hosts by country code if provided (case-insensitive)
        country_code = getattr(args, 'enable_country', None)
        if country_code:
            cc = country_code.strip().lower()
            cur.execute(
                """
                SELECT demeter_id, uuid, url, country, active
                FROM sites
                WHERE LOWER(TRIM(country)) = ? AND (active IS NULL OR active=0)
                ORDER BY demeter_id ASC
                """,
                (cc,)
            )
            rows = cur.fetchall()
            if not rows:
                print(f"No hosts found with country='{country_code}' needing enable.")
                return
            ids = [r[0] for r in rows]
            placeholders = ",".join(["?"] * len(ids))
            cur.execute(f"UPDATE sites SET active=1 WHERE demeter_id IN ({placeholders})", ids)
            print(f"Enabled {len(ids)} host(s) for country '{country_code}' (active=1).")
            for r in rows:
                print(f"  demeter_id {r[0]} (uuid={r[1]}, url={r[2]}, country={r[3]}) -> active=1")
            return

        # Enable all online hosts if flag set
        if getattr(args, 'enable_all', False):
            cur.execute(
                """
                SELECT demeter_id, uuid, url, status, active
                FROM sites
                WHERE LOWER(TRIM(status))='online' AND (active IS NULL OR active=0)
                ORDER BY demeter_id ASC
                """
            )
            rows = cur.fetchall()
            if not rows:
                print("No hosts with status 'online' to enable.")
                return
            ids = [r[0] for r in rows]
            placeholders = ",".join(["?"] * len(ids))
            # Only toggle the active flag; do not change status
            cur.execute(f"UPDATE sites SET active=1 WHERE demeter_id IN ({placeholders})", ids)
            print(f"Enabled {len(ids)} host(s) with status 'online'.")
            for r in rows:
                print(f"  demeter_id {r[0]} (uuid={r[1]}, url={r[2]}) -> active=1")
            return

        # Otherwise, enable a single host by demeter_id
        if not getattr(args, 'hostid', None):
            print("Please provide a hostid or use --enable-all/--enable-country.")
            return

        # Check if demeter_id exists and get current status
        cur.execute("SELECT uuid, url, status, active FROM sites WHERE demeter_id=?", (args.hostid,))
        row = cur.fetchone()
        if not row:
            print(f"No host found with demeter_id {args.hostid}.")
            return
        uuid, url, current_status, current_active = row

        # Check if already enabled
        if current_active == 1:
            print(f"Host with demeter_id {args.hostid} (uuid={uuid}, url={url}) is already enabled (status='{current_status}', active={current_active})")
            return

        # Enable the host (toggle active flag only)
        cur.execute("UPDATE sites SET active=1 WHERE demeter_id=?", (args.hostid,))
        print(f"Host with demeter_id {args.hostid} (uuid={uuid}, url={url}) has been enabled (active=1)")
    except Exception as e:
        print(f"Error enabling host: {e}")
    finally:
        conn.commit()
        conn.close()

def handle_host_disable(args):
    conn = get_sites_db_conn()
    cur = conn.cursor()
    try:
        # Disable all currently active hosts if flag set
        if getattr(args, 'disable_all', False):
            cur.execute(
                """
                SELECT demeter_id, uuid, url, status, active
                FROM sites
                WHERE active=1
                ORDER BY demeter_id ASC
                """
            )
            rows = cur.fetchall()
            if not rows:
                print("No active hosts to disable.")
                return
            ids = [r[0] for r in rows]
            placeholders = ",".join(["?"] * len(ids))
            # Only set active=0; do not change status in bulk disable
            cur.execute(f"UPDATE sites SET active=0 WHERE demeter_id IN ({placeholders})", ids)
            print(f"Disabled {len(ids)} host(s) that were active (set active=0 only).")
            for r in rows:
                print(f"  demeter_id {r[0]} (uuid={r[1]}, url={r[2]}) -> active=0")
            return

        # Otherwise, disable a single host by demeter_id
        if not getattr(args, 'hostid', None):
            print("Please provide a hostid or use --disable-all.")
            return
        # Check if demeter_id exists
        cur.execute("SELECT uuid, url FROM sites WHERE demeter_id=?", (args.hostid,))
        row = cur.fetchone()
        if not row:
            print(f"No host found with demeter_id {args.hostid}.")
            return
        # Disable the host (toggle active flag only)
        cur.execute("UPDATE sites SET active=0 WHERE demeter_id=?", (args.hostid,))
        print(f"Host with demeter_id {args.hostid} (uuid={row[0]}, url={row[1]}) has been disabled (active=0)")
    except Exception as e:
        print(f"Error disabling host: {e}")
    finally:
        conn.commit()
        conn.close()

def handle_host_stats(args):
    conn = get_sites_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT demeter_id, uuid, url, status, downloads, scrapes, last_scrape, last_download, book_count, libraries_count FROM sites WHERE demeter_id=?", (args.hostid,))
        row = cur.fetchone()
        if row:
            print(f"Host Stats for {row[2]}:")
            print(f"  demeter_id: {row[0]}")
            print(f"  UUID: {row[1]}")
            print(f"  Status: {row[3]}")
            print(f"  Downloads: {row[4]}")
            print(f"  Scrapes: {row[5]}")
            print(f"  Last Scrape: {row[6]}")
            print(f"  Last Download: {row[7]}")
            print(f"  Book Count: {row[8]}")
            print(f"  Libraries Count: {row[9]}")
            print(f"  ")
        else:
            print("No host found with that demeter_id.")
    except Exception as e:
        print(f"Error getting host stats: {e}")
    finally:
        conn.close()

# --- Scrape Handlers ---
def handle_scrape_run(args):
    """
    Scrape all online hosts, fetch new book IDs, download missing books, and update stats in the database.
    """
    # Configurable parameters
    step_size = 50
    workers = 5
    user_agent = 'demeter / v1'
    output_dir = getattr(args, 'outputdir', 'books')
    extension = getattr(args, 'extension', 'epub')
    authors_pattern = getattr(args, 'authors', None)
    titles_pattern = getattr(args, 'titles', None)
    timeout = 30

    os.makedirs(output_dir, exist_ok=True)

    # Connect to DBs
    sites_conn = get_sites_db_conn()
    sites_cur = sites_conn.cursor()
    # index_conn and index_cur are no longer shared; each thread will open its own connection

    # Get all enabled & online hosts
    sites_cur.execute("SELECT demeter_id, url, downloads, scrapes FROM sites WHERE status = 'online' AND active=1")
    hosts = sites_cur.fetchall()
    if not hosts:
        print("No enabled hosts to scrape.")
        return

    import urllib.parse
    def get_book_ids(host_url, extension):
        import urllib.parse
        host = urllib.parse.urlparse(host_url).hostname
        conn = get_index_db_conn()
        try:
            cur = conn.cursor()
            # Prefilter by host; optional authors LIKE filter; do extension filtering in Python for robustness
            sql = "SELECT uuid, links FROM summary WHERE links LIKE ?"
            params = [f"%{host}%"]
            if authors_pattern:
                sql += " AND authors LIKE ?"
                params.append(authors_pattern)
            if titles_pattern:
                sql += " AND title LIKE ?"
                params.append(titles_pattern)
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
        finally:
            conn.close()
        print(f"[DEBUG] Host: {host_url}")
        print(f"[DEBUG] Querying index.db for links LIKE '%{host}%' ...")
        if authors_pattern:
            print(f"[DEBUG] Applying authors LIKE filter: {authors_pattern}")
        if titles_pattern:
            print(f"[DEBUG] Applying title LIKE filter: {titles_pattern}")
        print(f"[DEBUG] Found book UUIDs and links: {rows}")
        import json
        book_links = []
        for uuid, links_json in rows:
            try:
                links = json.loads(links_json)
                # Determine if we should collect all formats
                ext_all = str(extension).strip().lower() in ('all', '*', 'any', '')
                if ext_all:
                    # Collect all available formats (dedupe by file extension) for this book on this host
                    import os
                    from urllib.parse import urlparse
                    seen_exts = set()
                    for link in links:
                        label = (link.get('label') or '')
                        href = (link.get('href') or '')
                        if not href or host not in href:
                            continue
                        try:
                            path = urlparse(href).path
                            ext = os.path.splitext(path)[-1].lower().lstrip('.')
                        except Exception:
                            ext = ''
                        # Dedupe by extension to avoid multiple downloads of same format
                        key_ext = ext or ''
                        if key_ext in seen_exts:
                            continue
                        seen_exts.add(key_ext)
                        book_links.append((uuid, href, label))
                    print(f"[DEBUG] [ALL] uuid={uuid} queued {len(seen_exts)} format(s): {sorted(seen_exts)}")
                else:
                    # Specific extension requested: pick the first matching link on this host
                    wanted = str(extension).strip().lower()
                    for link in links:
                        label = (link.get('label') or '')
                        href = (link.get('href') or '')
                        if not href or host not in href:
                            continue
                        label_ok = wanted in label.lower()
                        href_ok = href.lower().endswith(f".{wanted}")
                        if label_ok or href_ok:
                            book_links.append((uuid, href, label))
                            break
            except Exception as e:
                print(f"[DEBUG] Could not parse links for {uuid}: {e}")
        print(f"[DEBUG] Download targets (uuid, href, label): {book_links}")
        return book_links

    def download_book(host_url, book_tuple):
        uuid, href, label = book_tuple
        try:
            # Each thread opens its own index.db connection
            thread_index_conn = get_index_db_conn()
            try:
                thread_index_cur = thread_index_conn.cursor()
                
                # Get the book title and authors from the summary table
                thread_index_cur.execute("SELECT title, authors FROM summary WHERE uuid = ?", (uuid,))
                result = thread_index_cur.fetchone()
                book_title = (result[0] or uuid) if result else uuid
                book_authors = (result[1] or '') if result and len(result) > 1 else ''
                
                # Always download and overwrite the file, even if uuid is present in index.db
                # Download book file using the actual href
                print(f"[DEBUG] Downloading book {uuid} from href: {href}")
                rf = requests.get(href, headers={"User-Agent": user_agent}, timeout=timeout)
                if rf.status_code == 200:
                    # Determine correct file extension using multiple strategies
                    import os
                    from urllib.parse import urlparse

                    def infer_ext_from_href(h: str) -> str | None:
                        try:
                            p = urlparse(h).path
                            e = os.path.splitext(p)[-1]
                            # accept short, reasonable extensions
                            if e and 1 <= len(e) <= 7:
                                return e.lstrip('.').lower()
                        except Exception:
                            pass
                        return None

                    def infer_ext_from_label(lbl: str) -> str | None:
                        if not lbl:
                            return None
                        tokens = {
                            'epub','epub2','epub3','pdf','mobi','azw','azw1','azw3','kf8','kfx','pdb','prc','tpz',
                            'txt','rtf','html','m4b','mp3','aac','flac','wma','ogg','wav','cbr','cbz','cb7','cbt','cba'
                        }
                        low = lbl.lower()
                        # exact token match inside label
                        for t in sorted(tokens, key=len, reverse=True):
                            if f".{t}" in low or f" {t}" in low or f"-{t}" in low or f"_{t}" in low or low.endswith(t):
                                return t
                        return None

                    def infer_ext_from_headers(resp) -> str | None:
                        try:
                            cd = resp.headers.get('Content-Disposition') or resp.headers.get('content-disposition')
                            if cd and 'filename=' in cd:
                                fname = cd.split('filename=', 1)[1].strip().strip('"\' ')
                                e = os.path.splitext(fname)[-1]
                                if e and 1 <= len(e) <= 7:
                                    return e.lstrip('.').lower()
                        except Exception:
                            pass
                        try:
                            ctype = (resp.headers.get('Content-Type') or resp.headers.get('content-type') or '').lower()
                            mime_map = {
                                'application/epub+zip': 'epub',
                                'application/pdf': 'pdf',
                                'application/x-mobipocket-ebook': 'mobi',
                                'application/x-mobi8-ebook': 'kf8',
                                'application/vnd.amazon.ebook': 'azw',
                                'application/vnd.amazon.mobi8-ebook': 'azw3',
                                'audio/mpeg': 'mp3',
                                'audio/mp4': 'm4b',
                                'audio/aac': 'aac',
                                'audio/flac': 'flac',
                                'audio/x-ms-wma': 'wma',
                                'audio/ogg': 'ogg',
                                'audio/wav': 'wav',
                                'text/plain': 'txt',
                                'text/html': 'html',
                            }
                            for k, v in mime_map.items():
                                if k in ctype:
                                    return v
                        except Exception:
                            pass
                        return None

                    # Try in order: href, label, headers, then fallback
                    file_ext = (
                        infer_ext_from_href(href)
                        or infer_ext_from_label(label)
                        or infer_ext_from_headers(rf)
                    )
                    if not file_ext:
                        # Fallback: if a specific extension was requested, use that; otherwise avoid mislabeling
                        sel = str(extension).strip().lower()
                        file_ext = sel if sel not in ('all', '*', 'any', '') else 'bin'
                    print(f"[DEBUG] Chosen extension for {uuid}: {file_ext} (href={href}, label={label})")
                    
                    # Build filename strictly as bookname_author.ext
                    import re
                    # Helper to remove noisy prefixes from any source string
                    def strip_noise(s: str) -> str:
                        if not s:
                            return s
                        # Remove sequences like: href_http_<host>_<port>_book_id_<id>_library_id_<name>_panel_book_details_(label_)?
                        s = re.sub(r'^href_.*?panel_book_details_(?:label_)?', '', s, flags=re.IGNORECASE)
                        # If still contains explicit 'panel_book_details_label_' anywhere, drop everything up to it
                        if 'panel_book_details_label_' in s:
                            s = s.split('panel_book_details_label_', 1)[1]
                        # Remove leading 'label' markers if present
                        s = re.sub(r'^.*?\blabel[: _-]+', '', s, flags=re.IGNORECASE)
                        return s
                    # Prefer parsing from the link label if it contains a clear Title-Author pattern
                    parsed_title = None
                    parsed_author = None
                    if label:
                        raw_label = str(label).strip()
                        candidate = strip_noise(raw_label) or raw_label
                        # Common separators between title and author
                        for sep in [" - ", " — ", " by "]:
                            if sep in candidate:
                                parts = candidate.split(sep, 1)
                                parsed_title = parts[0].strip()
                                parsed_author = parts[1].strip()
                                break
                    # Fallbacks to DB fields (also clean noise from DB title if it was stored that way)
                    title_for_name = parsed_title or strip_noise(book_title) or uuid
                    author_for_name = parsed_author or book_authors or ""
                    # If multiple authors separated by ';' or ',', prefer first
                    for sep in [';', ',']:
                        if author_for_name and sep in author_for_name:
                            author_for_name = author_for_name.split(sep)[0].strip()
                            break
                    # Sanitize
                    def sanitize_name(s: str) -> str:
                        s = re.sub(r'[^A-Za-z0-9\-_. ]+', '_', s)
                        s = s.strip().replace(' ', '_')
                        s = re.sub(r'_+', '_', s)
                        return s.strip('_')
                    safe_title = sanitize_name(title_for_name) if title_for_name else uuid
                    safe_author = sanitize_name(author_for_name) if author_for_name else ''
                    filename_core = f"{safe_title}_{safe_author}" if safe_author else safe_title
                    # Post-clean: if any residual noise remains, strip it aggressively
                    # Drop anything up to and including 'panel_book_details_(label_)' or 'label_'
                    try:
                        filename_core = re.sub(r'.*?(?:panel_book_details_(?:label_)?|\blabel_)(.*)$', r'\1', filename_core, flags=re.IGNORECASE)
                        # If still begins with 'href_' noise, strip leading href_* until first double underscore-like boundary
                        filename_core = re.sub(r'^href_.*?_(?=[A-Za-z0-9])', '', filename_core, flags=re.IGNORECASE)
                    except TypeError:
                        pass
                    # Debug: show how we derived the filename (verbose only)
                    logging.debug(f"[NAME] raw_label={str(label)} candidate={locals().get('candidate', '')} parsed_title={parsed_title} parsed_author={parsed_author} title_for_name={title_for_name} author_for_name={author_for_name} filename_core={filename_core}")
                    
                    file_path = os.path.join(output_dir, f"{filename_core}.{file_ext}")
                    print(f"[DEBUG] Saving to {file_path}")
                    with open(file_path, 'wb') as f:
                        f.write(rf.content)
                    
                    # Update the summary with the proper title if it wasn't already set
                    thread_index_cur.execute("INSERT OR REPLACE INTO summary (uuid, title, authors, formats) VALUES (?, ?, ?, ?)",
                                     (uuid, book_title, book_authors, file_ext))
                    thread_index_conn.commit()
                    return (uuid, 'downloaded')
                else:
                    return (uuid, f"download_failed_{rf.status_code}")
            finally:
                thread_index_conn.close()
        except Exception as e:
            try:
                thread_index_conn.close()
            except:
                pass
            return (uuid, f"error_{e}")

    for demeter_id, host_url, downloads, scrapes in hosts:
        print(f"Scraping host {host_url} (demeter_id={demeter_id}) ...")
        book_ids = get_book_ids(host_url, extension)
        if not book_ids:
            print(f"  No books found or failed to fetch book list.")
            continue
        new_downloads = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(download_book, host_url, book_tuple): book_tuple for book_tuple in book_ids}
            for future in as_completed(futures):
                book_tuple = futures[future]
                uuid = book_tuple[0]
                try:
                    uuid, status = future.result()
                    if status == 'downloaded':
                        new_downloads += 1
                        print(f"  Downloaded book {uuid}")
                    elif status == 'already_downloaded':
                        pass
                    elif status == 'not_found':
                        print(f"  Book {uuid} not found on remote host.")
                    else:
                        print(f"  Book {uuid} error: {status}")
                except Exception as e:
                    print(f"  Error downloading book {uuid}: {e}")
        # Update stats in sites.db
        now = datetime.now().isoformat()
        sites_cur.execute("UPDATE sites SET downloads = downloads + ?, scrapes = scrapes + 1, last_scrape = ? WHERE demeter_id = ?",
                          (new_downloads, now, demeter_id))
        if new_downloads > 0:
            sites_cur.execute("UPDATE sites SET last_download = ? WHERE demeter_id = ?", (now, demeter_id))
        sites_conn.commit()
        print(f"Host {host_url}: {new_downloads} new books downloaded.")

    sites_conn.close()
    print("Scrape run complete.")

def handle_scrape_results(args):
    print("scrape results called")

if __name__ == '__main__':
    main()

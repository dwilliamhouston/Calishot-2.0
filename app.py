from flask import Flask, render_template, jsonify, send_from_directory, make_response, request
import sqlite3
import os
import sys
import logging
from pathlib import Path
from functools import wraps
import threading
import uuid as uuidlib
import io
import contextlib
import traceback

# When running this file directly, Python sets sys.path[0] to this directory (calishot_web/),
# so add the repository root to import top-level modules like calishot_logging and demeter.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import calishot_logging

def resource_path(relative_path: str) -> Path:
    """Return absolute path to resource, works for dev, installed, and PyInstaller.

    - When frozen by PyInstaller, resources live under sys._MEIPASS.
    - When installed (normal case), use the package directory of this file.
    - In editable/dev mode, this also resolves correctly.
    """
    base = getattr(sys, '_MEIPASS', Path(__file__).resolve().parent)
    return Path(base) / relative_path

app = Flask(
    __name__,
    static_folder=str(resource_path('static')),
    template_folder=str(resource_path('templates')),
)

# Configure project-wide logging: write to repo root regardless of CWD and echo to console during dev
# Use the parent of this package directory as the project root
calishot_logging.init_logging(logging.INFO, log_file=_PROJECT_ROOT / 'calishot.log', add_console=True)
logger = logging.getLogger(__name__)

# Import demeter CLI handlers to reuse logic (after logging is configured)
import demeter  # demeter is shipped as a py-module via packaging

# Simple in-memory job store for long-running demeter operations (e.g., scrape)
JOBS = {}
JOB_THREADS = {}
CANCEL_EVENT = threading.Event()

def _capture_demeter_stdout(callable_func, args_namespace):
    """Run a demeter handler and capture stdout/stderr. Returns dict with output and error info."""
    out_buf = io.StringIO()
    err_buf = io.StringIO()
    error = None
    tb = None
    exc_type = None
    # Capture both stdout and stderr
    with contextlib.redirect_stdout(out_buf), contextlib.redirect_stderr(err_buf):
        try:
            callable_func(args_namespace)
        except Exception as e:
            # Record error; demeter handlers often print errors but we also surface exception details
            error = str(e)
            exc_type = e.__class__.__name__
            tb = traceback.format_exc()
            print(f"Error: {error}")
    return {
        "output": out_buf.getvalue(),
        "stderr": err_buf.getvalue(),
        "error": error,
        "exception_type": exc_type,
        "traceback": tb,
    }

def _mk_ns(**kwargs):
    """Create a simple argparse-like namespace object."""
    return type("NS", (), kwargs)

def get_country_counts():
    """Get count of sites per country from the database."""
    conn = None
    try:
        # Candidate locations for the database
        env_dir = os.getenv('CALISHOT_DATA_DIR')
        repo_sites = _PROJECT_ROOT / 'data' / 'sites.db'
        # Treat "package" path as the repo's data dir to avoid calishot_web/data fallback
        pkg_sites = repo_sites
        cwd_sites = Path.cwd() / 'data' / 'sites.db'

        logger.info(f"CALISHOT_DATA_DIR: {env_dir}")
        logger.info(f"Repo sites.db: {repo_sites}")
        logger.info(f"Package sites.db: {pkg_sites}")
        logger.info(f"CWD sites.db: {cwd_sites}")

        candidates = []
        if env_dir:
            candidates.append(Path(env_dir) / 'sites.db')
        # Prefer repository data directory
        candidates.append(repo_sites)
        # Fallbacks
        candidates.append(pkg_sites)
        candidates.append(cwd_sites)

        # Log candidates and existence to help diagnose selection
        try:
            for i, p in enumerate(candidates):
                logger.info(f"DB candidate[{i}]: {p} exists={p.exists() if p else None}")
        except Exception:
            pass

        db_path = next((p for p in candidates if p and p.exists()), candidates[0])
        if not db_path.exists():
            error_msg = f"Database not found at {db_path.absolute()}"
            logger.error(error_msg)
            return {"error": error_msg}
            
        # Log which database file we're actually using
        logger.info(f"Using sites.db at: {db_path}")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check if the sites table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sites';")
        if not cursor.fetchone():
            error_msg = "The 'sites' table does not exist in the database"
            logger.error(error_msg)
            return {"error": error_msg}
        
        # Get country counts
        cursor.execute('''
            SELECT country, COUNT(*) as count 
            FROM sites 
            WHERE country IS NOT NULL AND country != ''
            GROUP BY country
            ORDER BY count DESC
        ''')
        results = cursor.fetchall()
        
        # Convert to dictionary
        country_data = {row['country']: row['count'] for row in results}
        
        # Log the number of countries found for debugging
        logger.info(f"Found {len(country_data)} countries with servers")
        #if len(country_data) < 1:
        #    sample = dict(list(country_data.items())[:3])
        #    logger.info(f"Sample country data: {sample}")
        #else:
        #    logger.warning("No country data found in the database")
            
        return country_data
        
    except sqlite3.Error as e:
        error_msg = f"Database error: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg}
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": error_msg}
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")

def add_csp_header(response):
    """Add Content Security Policy headers to allow necessary resources."""
    csp_policy = (
        "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob:; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' cdnjs.cloudflare.com cdnjs.com; "
        "style-src 'self' 'unsafe-inline' cdnjs.cloudflare.com cdnjs.com; "
        "img-src 'self' data: blob:; "
        "connect-src 'self' http: https:; "
        "font-src 'self' data:; "
        "frame-src 'self'"
    )
    response.headers['Content-Security-Policy'] = csp_policy
    return response

@app.route('/')
def index():
    try:
        # Log a message when the index page is accessed
        logger.info("Rendering index page")
        response = make_response(render_template('index_clean.html'))
        return add_csp_header(response)
    except Exception as e:
        logger.error(f"Error rendering index: {e}")
        return "An error occurred while loading the page. Please check the logs.", 500

@app.route('/demeter')
def demeter_page():
    try:
        response = make_response(render_template('demeter.html'))
        return add_csp_header(response)
    except Exception as e:
        logger.error(f"Error rendering demeter page: {e}")
        return "Failed to render Demeter UI", 500

# Serve static files
@app.route('/static/<path:path>')
def serve_static(path):
    return send_from_directory(app.static_folder, path)

@app.route('/api/country_counts')
def api_country_counts():
    logger.info("Received request to /api/country_counts")
    try:
        counts = get_country_counts()
        
        # Check if we got an error response
        if isinstance(counts, dict) and 'error' in counts:
            logger.error(f"Error in get_country_counts: {counts['error']}")
            return jsonify({"error": counts['error']}), 500
            
        if not counts:
            logger.warning("No country data available")
            return jsonify({"error": "No country data available"}), 404
            
        logger.info(f"Returning data for {len(counts)} countries")
        logger.debug(f"Sample data: {dict(list(counts.items())[:3])}")
        
        response = jsonify(counts)
        # Enable CORS if needed
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
        
    except Exception as e:
        error_msg = f"Unexpected error in country_counts API: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return jsonify({"error": error_msg}), 500

# --------------------- Demeter API ---------------------
@app.post('/api/demeter/host/list')
def demeter_host_list():
    ns = _mk_ns()
    res = _capture_demeter_stdout(demeter.handle_host_list, ns)
    if res.get("error"):
        logger.error(f"demeter.host.list failed: {res['error']}")
        res["db_paths"] = {"sites": getattr(demeter, 'SITES_DB_PATH', None), "index": getattr(demeter, 'INDEX_DB_PATH', None)}
        return jsonify(res), 500
    return jsonify({"output": (res.get("output") or "").strip() or res.get("stderr")})

@app.post('/api/demeter/host/list_all')
def demeter_host_list_all():
    ns = _mk_ns()
    res = _capture_demeter_stdout(demeter.handle_host_list_all, ns)
    if res.get("error"):
        logger.error(f"demeter.host.list_all failed: {res['error']}")
        res["db_paths"] = {"sites": getattr(demeter, 'SITES_DB_PATH', None), "index": getattr(demeter, 'INDEX_DB_PATH', None)}
        return jsonify(res), 500
    return jsonify({"output": (res.get("output") or "").strip() or res.get("stderr")})

@app.post('/api/demeter/host/add')
def demeter_host_add():
    data = request.get_json(silent=True) or {}
    hosturl = data.get('hosturl') or []
    if isinstance(hosturl, str):
        hosturl = [hosturl]
    ns = _mk_ns(hosturl=hosturl)
    res = _capture_demeter_stdout(demeter.handle_host_add, ns)
    if res.get("error"):
        logger.error(f"demeter.host.add failed: {res['error']}")
        res["db_paths"] = {"sites": getattr(demeter, 'SITES_DB_PATH', None), "index": getattr(demeter, 'INDEX_DB_PATH', None)}
        return jsonify(res), 500
    return jsonify({"output": (res.get("output") or "").strip() or res.get("stderr")})

@app.post('/api/demeter/host/rm')
def demeter_host_rm():
    data = request.get_json(silent=True) or {}
    hostid = data.get('hostid')
    ns = _mk_ns(hostid=str(hostid) if hostid is not None else None)
    res = _capture_demeter_stdout(demeter.handle_host_rm, ns)
    if res.get("error"):
        logger.error(f"demeter.host.rm failed: {res['error']}")
        res["db_paths"] = {"sites": getattr(demeter, 'SITES_DB_PATH', None), "index": getattr(demeter, 'INDEX_DB_PATH', None)}
        return jsonify(res), 500
    return jsonify({"output": (res.get("output") or "").strip() or res.get("stderr")})

@app.post('/api/demeter/host/enable')
def demeter_host_enable():
    data = request.get_json(silent=True) or {}
    ns = _mk_ns(
        enable_all=bool(data.get('enable_all', False)),
        enable_country=data.get('enable_country'),
        hostid=str(data.get('hostid')) if data.get('hostid') is not None else None,
    )
    res = _capture_demeter_stdout(demeter.handle_host_enable, ns)
    if res.get("error"):
        logger.error(f"demeter.host.enable failed: {res['error']}")
        res["db_paths"] = {"sites": getattr(demeter, 'SITES_DB_PATH', None), "index": getattr(demeter, 'INDEX_DB_PATH', None)}
        return jsonify(res), 500
    return jsonify({"output": (res.get("output") or "").strip() or res.get("stderr")})

@app.post('/api/demeter/host/disable')
def demeter_host_disable():
    data = request.get_json(silent=True) or {}
    ns = _mk_ns(
        disable_all=bool(data.get('disable_all', False)),
        hostid=str(data.get('hostid')) if data.get('hostid') is not None else None,
    )
    res = _capture_demeter_stdout(demeter.handle_host_disable, ns)
    if res.get("error"):
        logger.error(f"demeter.host.disable failed: {res['error']}")
        res["db_paths"] = {"sites": getattr(demeter, 'SITES_DB_PATH', None), "index": getattr(demeter, 'INDEX_DB_PATH', None)}
        return jsonify(res), 500
    return jsonify({"output": (res.get("output") or "").strip() or res.get("stderr")})

@app.post('/api/demeter/host/stats')
def demeter_host_stats():
    data = request.get_json(silent=True) or {}
    ns = _mk_ns(hostid=str(data.get('hostid')) if data.get('hostid') is not None else None)
    res = _capture_demeter_stdout(demeter.handle_host_stats, ns)
    if res.get("error"):
        logger.error(f"demeter.host.stats failed: {res['error']}")
        res["db_paths"] = {"sites": getattr(demeter, 'SITES_DB_PATH', None), "index": getattr(demeter, 'INDEX_DB_PATH', None)}
        return jsonify(res), 500
    return jsonify({"output": (res.get("output") or "").strip() or res.get("stderr")})

def _run_scrape_job(job_id, opts):
    # If cancellation requested, exit early
    if CANCEL_EVENT.is_set():
        JOBS[job_id]['status'] = 'cancelled'
        JOBS[job_id]['log'] = (JOBS[job_id].get('log') or '') + "Cancelled before start.\n"
        return

    ns = _mk_ns(
        extension=opts.get('extension', 'epub'),
        outputdir=opts.get('outputdir', 'books'),
        authors=opts.get('authors'),
        titles=opts.get('titles'),
    )
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        try:
            # Note: demeter does not support mid-run cancellation; this is best-effort.
            if not CANCEL_EVENT.is_set():
                demeter.handle_scrape_run(ns)
            else:
                print("Cancellation requested. Skipping run.")
        except Exception as e:
            print(f"Error: {e}")
    # Determine final status
    if CANCEL_EVENT.is_set():
        JOBS[job_id]['status'] = 'cancelled'
    else:
        JOBS[job_id]['status'] = 'completed'
    JOBS[job_id]['log'] = buf.getvalue()

@app.post('/api/demeter/scrape/run')
def demeter_scrape_run():
    data = request.get_json(silent=True) or {}
    job_id = str(uuidlib.uuid4())
    # If cancellation has been requested, do not start
    if CANCEL_EVENT.is_set():
        JOBS[job_id] = {"status": "cancelled", "log": "Cancellation requested. Not starting new scrape.\n"}
        return jsonify({"job_id": job_id, "status": JOBS[job_id]['status']})
    JOBS[job_id] = {"status": "running", "log": "Starting scrape...\n"}
    thread = threading.Thread(target=_run_scrape_job, args=(job_id, data), daemon=True)
    JOB_THREADS[job_id] = thread
    thread.start()
    return jsonify({"job_id": job_id, "status": JOBS[job_id]['status']})

@app.get('/api/demeter/scrape/status')
def demeter_scrape_status():
    job_id = request.args.get('job_id')
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    return jsonify(job)

@app.post('/api/demeter/scrape/cancel_all')
def demeter_scrape_cancel_all():
    # Signal cancellation for future runs and mark running jobs as cancelled (best effort)
    CANCEL_EVENT.set()
    cancelled = 0
    for jid, meta in list(JOBS.items()):
        if meta.get('status') == 'running':
            meta['status'] = 'cancelled'
            meta['log'] = (meta.get('log') or '') + "Cancellation requested. This may not stop immediately.\n"
            cancelled += 1
    return jsonify({"cancelled_jobs": cancelled, "message": "Cancellation requested."})

@app.post('/api/demeter/scrape/reset_cancel')
def demeter_scrape_reset_cancel():
    """Clear the cancellation flag so new scrapes can start."""
    try:
        CANCEL_EVENT.clear()
        return jsonify({"message": "Cancellation flag cleared. New scrapes can start."})
    except Exception as e:
        logger.error(f"Error clearing cancel flag: {e}")
        return jsonify({"error": "Failed to clear cancellation flag"}), 500

# --------------------- Debug Logging Endpoint ---------------------
@app.get('/api/debug/logging')
def debug_logging():
    """Return info about current logging configuration and write a test line."""
    root = logging.getLogger()
    info = {
        "root_level": logging.getLevelName(root.level),
        "handlers": [],
        "resolved_logfile": getattr(root, '_calishot_logfile', None),
    }
    for h in root.handlers:
        item = {
            "type": type(h).__name__,
            "level": logging.getLevelName(h.level),
        }
        # Try to include filename if it's a File/Rotating handler
        for attr in ('baseFilename', 'stream'):
            if hasattr(h, attr):
                try:
                    val = getattr(h, attr)
                    item[attr] = val if isinstance(val, str) else str(val)
                except Exception:
                    pass
        info["handlers"].append(item)
    logger.info("/api/debug/logging hit: emitting test log line")
    return jsonify(info)

@app.route('/api/debug/countries')
def debug_countries():
    """Debug endpoint to list all available country codes in the world map."""
    # This is a sample of country codes that DataMaps expects
    # In a real implementation, you would get this from the actual DataMaps world map data
    sample_countries = [
        'USA', 'DEU', 'GBR', 'FRA', 'CHN', 'JPN', 'BRA', 'CAN', 'AUS', 'IND',
        'RUS', 'ZAF', 'MEX', 'IDN', 'NLD', 'TUR', 'SAU', 'CHE', 'ARG', 'SWE'
    ]
    return jsonify({
        'available_countries': sample_countries,
        'note': 'This is a sample list. In production, use the actual DataMaps world map data.'
    })

if __name__ == '__main__':
    # On Windows with Python 3.13, Werkzeug's watchdog reloader may error.
    # Disable the reloader on Windows to avoid TypeError: 'handle' must be a _ThreadHandle
    use_reloader = False if os.name == 'nt' else True
    app.run(debug=True, port=5003, host='0.0.0.0', use_reloader=use_reloader)

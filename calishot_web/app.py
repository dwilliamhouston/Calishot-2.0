from flask import Flask, render_template, jsonify, send_from_directory, make_response
import sqlite3
import os
import logging
from pathlib import Path
from functools import wraps

app = Flask(__name__, static_folder='static')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_country_counts():
    """Get count of sites per country from the database."""
    conn = None
    try:
        db_path = Path('data/sites.db')
        if not db_path.exists():
            error_msg = f"Database not found at {db_path.absolute()}"
            logger.error(error_msg)
            return {"error": error_msg}
            
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
        if len(country_data) > 0:
            sample = dict(list(country_data.items())[:3])
            logger.info(f"Sample country data: {sample}")
        else:
            logger.warning("No country data found in the database")
            
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

# Serve static files
@app.route('/static/<path:path>')
def serve_static(path):
    return send_from_directory('static', path)

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
    app.run(debug=True, port=5003, host='0.0.0.0')

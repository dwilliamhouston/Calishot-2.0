# Calishot Web

A web interface for visualizing Calibre server distribution by country.

## Setup

1. Install the required dependencies:
   ```
   pip install flask
   ```

2. Make sure you have a `sites.db` file in the `data` directory with the `sites` table.

## Running the Application

1. Start the Flask development server:
   ```
   python app.py
   ```

2. Open your web browser and navigate to:
   ```
   http://localhost:5000
   ```

## Features

- Interactive world map showing the number of Calibre servers per country
- Color-coded countries based on server count
- Click on any country to see detailed information
- Responsive design that works on different screen sizes

## Dependencies

- Python 3.6+
- Flask
- SQLite3 (included in Python standard library)
- Modern web browser with JavaScript enabled

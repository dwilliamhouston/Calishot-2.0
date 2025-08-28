Application to search Shodan for Calibre Web sites that and pull all books into a database and then display on a website.

Some of this code is based on code created by Krazybug - https://github.com/Krazybug/calishot.
Some of this code is based on code created by gnur - https://github.com/gnur/demeter

## Installation

Requirements:

- Python 3.9+ (macOS/Linux tested)
- Optional: use a virtual environment

### Cross-platform installer (Windows/macOS/Linux)

This installer sets up the app and dependencies only. It does not include or copy any files from the repo `books/` or `data/` directories.

### 1) Quick install (recommended)

Installs from GitHub, creates the data directory, and ensures the `demeter` module is present.

```bash
# From the repo root
python3 install_from_github.py
```

This will:

- Upgrade pip/setuptools/wheel
- Install calishot-web from GitHub
- Create data directory at: `~/.calishot/data`
- Ensure `demeter.py` is available in site-packages

Run the web app:

```bash
python3 -m calishot-web.app
```

Place your SQLite DB at one of the recognized locations:

- `~/data/sites.db` (default)
- or set an explicit directory via `CALISHOT_DATA_DIR`:

```bash
export CALISHOT_DATA_DIR=/path/to/dir
calishot-web
```

### 2) Install directly via pip (alternative)

```bash
python3 -m pip install "git+https://github.com/dwilliamhouston/Calishot-2.0.git@main#subdirectory=."
```

Then run:

```bash
calishot-web
```

### 3) Local build and install (contributors)

```bash
# Clean build artifacts
rm -rf build dist *.egg-info

# Build sdist and wheel
python3 -m build

# Install the wheel
python3 -m pip install --force-reinstall --no-deps --no-cache-dir dist/calishot_web-*-py3-none-any.whl
```

Run:

```bash
calishot-web
```

### Data placement

`calishot_web/app.py` looks for the database in this order:

1. `CALISHOT_DATA_DIR/sites.db` if `CALISHOT_DATA_DIR` is set
2. `~/.calishot/data/sites.db`
3. Current working directory: `./data/sites.db`

If the DB is missing, the app returns a helpful error JSON from `/api/country_counts`.

### Troubleshooting

- "No module named 'demeter'":
  - Reinstall the package to ensure `demeter.py` is included:

    ```bash
    python3 -m pip install --force-reinstall --no-deps --no-cache-dir dist/calishot_web-*-py3-none-any.whl
    ```

  - Or rerun the installer:

    ```bash
    python3 install_from_github.py
    ```

- `zsh: no matches found: dist/*.whl`:
  - The glob didn’t match. List dist and install the exact file:

    ```bash
    ls -1 dist
    python3 -m pip install dist/calishot_web-0.1.0-py3-none-any.whl
    ```

- Static/templates path issues when frozen:
  - `resource_path()` in `calishot_web/app.py` handles installed, dev, and PyInstaller modes.

### Uninstall

```bash
python3 -m pip uninstall -y calishot-web
```

## Usage

- Start the app after install:

```bash
calishot-web
# or override host/port
HOST=127.0.0.1 PORT=5003 calishot-web
```

- Place your database at `~/.calishot/data/sites.db` or set `CALISHOT_DATA_DIR`.

## API Endpoints

- `GET /` — main UI (`calishot_web/templates/index_clean.html`)
- `GET /demeter` — Demeter UI
- `GET /api/country_counts` — JSON country->count from `sites.db`

Demeter actions (proxied from `demeter.py`):

- `POST /api/demeter/host/list`
- `POST /api/demeter/host/list_all`
- `POST /api/demeter/host/add` — JSON: `{ "hosturl": ["https://example"] }`
- `POST /api/demeter/host/rm` — JSON: `{ "hostid": 123 }`
- `POST /api/demeter/host/enable` — JSON: `{ "enable_all": true | false, "enable_country": "US", "hostid": 123 }`
- `POST /api/demeter/host/disable` — JSON: `{ "disable_all": true | false, "hostid": 123 }`
- `POST /api/demeter/host/stats` — JSON: `{ "hostid": 123 }`
- `POST /api/demeter/scrape/run` — JSON: `{ "extension": "epub", "outputdir": "books", "authors": [..], "titles": [..] }`
- `GET  /api/demeter/scrape/status?job_id=<id>`
- `POST /api/demeter/scrape/cancel_all`
- `POST /api/demeter/scrape/reset_cancel`

## Docker

Use docker-compose (recommended):

```bash
docker compose up --build -d
```

Defaults from `docker-compose.yml`:

- Port: `5003` exposed
- Volumes:
  - `./data` -> `/app/data` (place `sites.db` here)
  - `./books` -> `/app/books`
  - `./calishot_web/templates` -> `/app/calishot_web/templates`
  - `./calishot_webtemplates` -> `/app/calishot_webtemplates`
- Environment:
  - `DATA_DIR=/data`
  - `FLASK_APP=calishot_web/app.py`
  - `FLASK_ENV=development`

Access the app at http://localhost:5003

<B>Instructions if you want to use existing or downloaded index.db locally (Not using Docker)</B>

  Step 1 - Setup new python environemnt and install datasette. 

  python -m venv shodantest
  ../calishot/bin/activate
  pip install datasette
  pip install datasette-json-html
  pip install datasette-pretty-json

  Step 2 - Execute from command line: pip -r install requirements.txt

  Step 3 - download index.db into your venv directory.

  Step 4 - Execute from command line:
  docker run -d \
  --name=calishot \
  -p 5001:5000 \
  -v /LOCALDIRHERE/app/data:/app/data \
  dwilliamhouston/shodantest:latest datasette -p 5000 -h 0.0.0.0 /app/data/index.db --config sql_time_limit_ms:50000 --config allow_download:off --config max_returned_rows:2000 --config num_sql_threads:10 --config allow_csv_stream:off --metadata metadata.json

<B>Instructions if using Docker rather than setting up your own Python environment:</B>

  Step 1 - Create a directory called /app and then a directory called /app/data and put the index.db file in it. 

  Step 2 Execute from command line:
  docker run -d \
  --name=calishot \
  -p 5001:5000 \
  -v /LOCALDIRHERE/app/data:/app/data \
  dwilliamhouston/shodantest:latest datasette -p 5000 -h 0.0.0.0 /app/data/index.db --config sql_time_limit_ms:50000 --config allow_download:off --config max_returned_rows:2000 --config num_sql_threads:10 --config allow_csv_stream:off --metadata metadata.json

  Step 3 - open the browser to http://localhost:5001
  

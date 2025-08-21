#!/usr/bin/env python3
"""
Calishot one-shot installer from GitHub.

What it does:
- Ensures Python and pip are available.
- Installs calishot-web (and demeter module from the same repo) via pip from GitHub.
- Downloads the runtime database (sites.db) into a user-writable directory: ~/.calishot/data/
- Prints how to start the server.

Usage:
  python install_from_github.py              # installs from default branch (main)
  CALISHOT_GIT_REF=dev python install_from_github.py  # install from a specific branch or tag

After install:
  calishot-web

"""
from __future__ import annotations

import os
import sys
import subprocess
import shutil
from pathlib import Path
from urllib.request import urlopen

REPO = os.getenv("CALISHOT_REPO", "dwilliamhouston/Calishot-2.0")
GIT_REF = os.getenv("CALISHOT_GIT_REF", "main")
RAW_BASE = f"https://raw.githubusercontent.com/{REPO}/{GIT_REF}"

SITES_DB_URL = f"{RAW_BASE}/data/sites.db"
TARGET_DATA_DIR = Path.home() / ".calishot" / "data"
TARGET_DB = TARGET_DATA_DIR / "sites.db"


def run(cmd: list[str]) -> None:
    print("$", " ".join(cmd))
    subprocess.check_call(cmd)


def ensure_python_version() -> None:
    if sys.version_info < (3, 9):
        raise RuntimeError("Python 3.9+ is required")


def ensure_pip() -> None:
    try:
        import pip  # noqa: F401
    except Exception:
        print("pip not found, bootstrapping with ensurepip...")
        import ensurepip

        ensurepip.bootstrap()
    run([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])


def pip_install_from_github() -> None:
    # Prefer VCS install if git is available, otherwise use archive URL
    use_git = shutil.which("git") is not None
    if use_git:
        url = f"git+https://github.com/{REPO}.git@{GIT_REF}#subdirectory=."
    else:
        # Use archive zip (no git required)
        # refs/heads for branches, refs/tags for tags – assume branch unless TAG is specified in ref
        url = f"https://github.com/{REPO}/archive/refs/heads/{GIT_REF}.zip"
    print(f"Installing Calishot from: {url}")
    run([sys.executable, "-m", "pip", "install", url])


def download_sites_db() -> None:
    TARGET_DATA_DIR.mkdir(parents=True, exist_ok=True)
    if TARGET_DB.exists() and TARGET_DB.stat().st_size > 0:
        print(f"sites.db already present at {TARGET_DB}")
        return
    print(f"Downloading sites.db from {SITES_DB_URL}")
    with urlopen(SITES_DB_URL) as resp:  # nosec - trusted GitHub raw URL
        data = resp.read()
    tmp = TARGET_DB.with_suffix(".tmp")
    tmp.write_bytes(data)
    tmp.replace(TARGET_DB)
    print(f"Saved {TARGET_DB} ({TARGET_DB.stat().st_size} bytes)")


def main() -> None:
    print("Calishot installer starting...\n")
    ensure_python_version()
    ensure_pip()
    pip_install_from_github()
    download_sites_db()

    print("\nInstallation complete!\n")
    print("Run the server with:")
    print("  calishot-web")
    print("\nEnvironment options:")
    print("  HOST=127.0.0.1 PORT=5003 calishot-web")
    print("\nData directory:")
    print(f"  {TARGET_DATA_DIR}")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}")
        sys.exit(e.returncode)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

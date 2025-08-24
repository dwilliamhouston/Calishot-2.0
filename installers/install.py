#!/usr/bin/env python3
"""
Cross-platform installer for Calishot suite (Calishot, Demeter, Calishot Web).

What it does (macOS, Windows, Linux):
- Verifies Python 3.9+ and pip are available
- Installs calishot-web (which contains the web app and CLI) from GitHub
- Ensures the 'demeter' module can be imported (drops demeter.py into site-packages if missing)
- Creates a writable data directory at ~/.calishot/data (or %USERPROFILE%\.calishot\data on Windows)
- Does NOT bundle or copy any files from the repository's 'books/' or 'data/' directories

Usage:
  python installers/install.py

Advanced:
  CALISHOT_REPO=dwilliamhouston/Calishot-2.0 CALISHOT_GIT_REF=main python installers/install.py

After install, run:
  calishot-web

Optionally set:
  HOST=127.0.0.1 PORT=5003 calishot-web

"""
from __future__ import annotations

import os
import sys
import shutil
import subprocess
import sysconfig
from pathlib import Path
from urllib.request import urlopen
import tempfile
import zipfile

REPO = os.getenv("CALISHOT_REPO", "dwilliamhouston/Calishot-2.0")
GIT_REF = os.getenv("CALISHOT_GIT_REF", "main")
ARCHIVE_URL = f"https://github.com/{REPO}/archive/refs/heads/{GIT_REF}.zip"

TARGET_DATA_DIR = Path.home() / ".calishot" / "data"


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
    use_git = shutil.which("git") is not None
    if use_git:
        url = f"git+https://github.com/{REPO}.git@{GIT_REF}#subdirectory=."
    else:
        url = f"https://github.com/{REPO}/archive/refs/heads/{GIT_REF}.zip"
    print(f"Installing Calishot from: {url}")
    run([sys.executable, "-m", "pip", "install", url])


def ensure_demeter_module() -> None:
    try:
        import demeter  # noqa: F401
        print("'demeter' module already present.")
        return
    except Exception:
        pass

    site_pkgs = Path(sysconfig.get_paths().get("purelib") or sysconfig.get_paths()["platlib"])  # type: ignore[index]
    dest = site_pkgs / "demeter.py"
    print("'demeter' not found; attempting to extract from repository archive...")

    # Try downloading the repo archive and extract demeter.py (works even if file moves in repo)
    try:
        with tempfile.TemporaryDirectory() as td:
            zip_path = Path(td) / "repo.zip"
            # Prefer the heads URL for branches; callers can change CALISHOT_GIT_REF to a tag if desired
            archive_url = ARCHIVE_URL
            print(f"Downloading archive: {archive_url}")
            with urlopen(archive_url) as resp:  # nosec - GitHub
                zip_path.write_bytes(resp.read())

            with zipfile.ZipFile(zip_path) as zf:
                # Find any entry that ends with '/demeter.py'
                demeter_member = None
                for name in zf.namelist():
                    if name.lower().endswith("/demeter.py") or name.lower().endswith("\\demeter.py") or name.lower().endswith("demeter.py"):
                        demeter_member = name
                        break
                if not demeter_member:
                    raise FileNotFoundError("demeter.py not found inside repository archive")

                print(f"Extracting {demeter_member} -> {dest}")
                data = zf.read(demeter_member)
                tmp = dest.with_suffix(".tmp")
                tmp.write_bytes(data)
                tmp.replace(dest)
                print(f"Installed demeter module at {dest}")
    except Exception as e:
        raise RuntimeError(f"Failed to install 'demeter' module from archive: {e}")


def ensure_data_dir() -> None:
    TARGET_DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Created/verified data directory at: {TARGET_DATA_DIR}")
    print("Note: This installer does NOT copy any repo data/ or books/ files.")


def main() -> None:
    print("Calishot cross-platform installer starting...\n")
    ensure_python_version()
    ensure_pip()
    pip_install_from_github()
    ensure_data_dir()
    ensure_demeter_module()

    print("\nInstallation complete!\n")
    print("Run the server with:")
    print("  calishot-web")
    print("\nEnvironment options:")
    print("  HOST=127.0.0.1 PORT=5003 calishot-web")
    print("\nData directory (place your sites.db here):")
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

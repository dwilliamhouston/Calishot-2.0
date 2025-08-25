import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys
import os

_LOG_FILE_NAME = "calishot.log"


def init_logging(level: int = logging.INFO, log_file: Path | None = None, max_bytes: int = 5_000_000, backup_count: int = 3, add_console: bool = False) -> None:
    """Initialize project-wide logging once.

    - Writes to a single rotating file in the application directory by default.
    - Safe to call multiple times; subsequent calls won't add duplicate handlers.
    - Sets a consistent format across all modules.
    """
    root = logging.getLogger()

    # Determine desired log file path with environment overrides and writable fallback
    app_dir = Path(__file__).resolve().parent
    env_log_file = os.getenv("CALISHOT_LOG_FILE")
    env_log_dir = os.getenv("CALISHOT_LOG_DIR")
    # Priority: explicit arg > CALISHOT_LOG_FILE > CALISHOT_LOG_DIR/calishot.log > default next to module
    lf = Path(log_file) if log_file else (
        Path(env_log_file) if env_log_file else (
            (Path(env_log_dir) / _LOG_FILE_NAME) if env_log_dir else (app_dir / _LOG_FILE_NAME)
        )
    )

    # If already configured, check whether the resolved logfile matches; if so, just adjust level.
    # If different (e.g., caller wants to override location), drop handlers and reconfigure.
    if getattr(root, "_calishot_configured", False):
        current = getattr(root, "_calishot_logfile", None)
        # If same logfile, keep handlers and just tweak level
        if current and str(lf) == str(current):
            root.setLevel(level)
            # Optionally add console if newly requested
            if add_console and not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
                fmt_existing = logging.Formatter(
                    fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
                err_handler = logging.StreamHandler(stream=sys.stdout)
                err_handler.setLevel(level)
                err_handler.setFormatter(fmt_existing)
                root.addHandler(err_handler)
            return
        # Reconfigure: remove existing handlers and proceed to fresh setup
        for h in list(root.handlers):
            root.removeHandler(h)
    try:
        lf.parent.mkdir(parents=True, exist_ok=True)
        # Probe writability early; if not writable, fallback
        with open(lf, 'a', encoding='utf-8'):
            pass
    except Exception:
        # Fallback to user-writable location
        home_fallback = Path.home() / ".calishot" / _LOG_FILE_NAME
        try:
            home_fallback.parent.mkdir(parents=True, exist_ok=True)
            with open(home_fallback, 'a', encoding='utf-8'):
                pass
            lf = home_fallback
        except Exception:
            # Last resort: keep original lf; handler creation may still raise revealing error
            pass

    # Clear any pre-existing basicConfig handlers added elsewhere (if any remain)
    if root.handlers:
        for h in list(root.handlers):
            root.removeHandler(h)

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(str(lf), maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    file_handler.setFormatter(fmt)
    file_handler.setLevel(level)

    root.addHandler(file_handler)
    root.setLevel(level)

    # Mark as configured to avoid duplicates and expose resolved path
    root._calishot_configured = True  # type: ignore[attr-defined]
    try:
        root._calishot_logfile = str(lf)  # type: ignore[attr-defined]
    except Exception:
        pass

    # Optional: Also echo to console for visibility during dev
    if add_console:
        err_handler = logging.StreamHandler(stream=sys.stdout)
        err_handler.setLevel(level)
        err_handler.setFormatter(fmt)
        root.addHandler(err_handler)

    # Emit a startup line indicating the log destination
    try:
        root.info(f"Calishot logging initialized. Writing to: {lf}")
    except Exception:
        # Avoid any issues if handlers are not fully ready
        pass

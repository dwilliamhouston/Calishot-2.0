from __future__ import annotations

import os
from calishot_web import app as flask_app


def main() -> None:
    """Run the Calishot web app locally.

    Environment variables:
      - HOST (default: 0.0.0.0)
      - PORT (default: 5003)
      - FLASK_ENV (development/production)
    """
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "5003"))

    # Respect FLASK_ENV if set, otherwise default to development for local
    debug = os.getenv("FLASK_ENV", "development").lower() == "development"

    # Disable reloader on Windows like in app.py guard
    use_reloader = False if os.name == 'nt' else True

    flask_app.run(host=host, port=port, debug=debug, use_reloader=use_reloader)


if __name__ == "__main__":
    main()

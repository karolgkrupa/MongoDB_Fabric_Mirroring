"""Headless entry point for running the MongoDB -> Fabric mirroring as a service.

This is what the Windows service (WinSW) launches via the project's virtual
environment interpreter:

    .venv\\Scripts\\python.exe service_runner.py

Unlike ``app.py`` (which exposes a Flask health endpoint and is used for the
Azure App Service deployment), this module has no web server and no console
input. It starts the mirroring threads via ``mirror()`` and then blocks until
the process receives a termination signal, so it behaves well under a service
manager that starts it automatically on boot and stops it on shutdown.
"""

import logging
import signal
import threading

from mongodb_generic_mirroring import mirror

logger = logging.getLogger(__name__)

_stop_event = threading.Event()


def _request_stop(signum, _frame):
    logger.info("Received signal %s, shutting down mirroring service...", signum)
    _stop_event.set()


def main() -> None:
    # Handle the signals a service manager (or Ctrl+C) may send so we can exit
    # cleanly instead of being hard-killed. SIGBREAK exists only on Windows.
    for sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                signal.signal(sig, _request_stop)
            except (ValueError, OSError):
                # Some signals can't be set outside the main thread / on every OS.
                pass

    mirror()
    logger.info("Mirroring service is running. Waiting for stop signal...")
    _stop_event.wait()
    logger.info("Mirroring service stopped.")


if __name__ == "__main__":
    main()

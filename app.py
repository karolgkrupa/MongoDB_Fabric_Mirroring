import logging
from flask import Flask
from threading import Thread

from mongodb_generic_mirroring import mirror
from logging_config import setup_logging

logger = logging.getLogger(__name__)


def create_app():
    setup_logging()
    app = Flask(__name__)
    thread_name=Thread(target=mirror).start()

    @app.route("/")
    def home_page():
        import threading
        logger.debug("active threads: %s", [t.name for t in threading.enumerate()])
        return "The MongoDB Fabric Mirroring Service is running..."

    return app

app = create_app()
if __name__ == "__main__":
    app.run()
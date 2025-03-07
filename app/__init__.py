from flask import Flask
from app.config import load_configurations, configure_logging
from .views import webhook_blueprint
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def create_app():
    app = Flask(__name__)

    # Configure logging first to capture all logs
    configure_logging()

    # Load configurations
    load_configurations(app)

    # Register blueprints
    app.register_blueprint(webhook_blueprint)

    return app
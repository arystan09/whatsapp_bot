# app/config.py

import sys
import os
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

def load_configurations(app):
    """
    Load environment variables and set them in Flask's config.
    """
    load_dotenv()

    # Map environment variables to Flask config
    app.config["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
    app.config["OPENAI_ASSISTANT_ID"] = os.getenv("OPENAI_ASSISTANT_ID")
    app.config["MANAGER_WAID"] = os.getenv("MANAGER_WAID")  # Added MANAGER_WAID
    app.config["GREENAPI_IDINSTANCE"] = os.getenv("GREENAPI_IDINSTANCE")
    app.config["GREENAPI_APITOKEN"] = os.getenv("GREENAPI_APITOKEN")

    # Validate essential configurations
    validate_configurations(app)

def validate_configurations(app):
    """
    Ensure all essential configurations are loaded.
    """
    essential_configs = [
        "OPENAI_API_KEY",
        "MANAGER_WAID",
        "GREENAPI_IDINSTANCE",
        "GREENAPI_APITOKEN"
    ]
    missing_configs = [key for key in essential_configs if not app.config.get(key)]
    if missing_configs:
        missing = ", ".join(missing_configs)
        logging.critical(f"Missing essential configuration(s): {missing}")
        sys.exit(1)  # Exit the application if configurations are missing

def configure_logging():
    """
    Настройка логирования без дублирования.
    """
    logger = logging.getLogger()

    # Удаляем все существующие обработчики перед настройкой
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    logger.setLevel(logging.INFO)

    # Обработчик для логов в файл
    file_handler = RotatingFileHandler('app.log', maxBytes=10*1024*1024, backupCount=5, delay=True)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Обработчик для вывода в консоль
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_formatter)
    logger.addHandler(stream_handler)

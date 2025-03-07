import logging
import sys
import os

# Отключаем лишние логи от Flask и Werkzeug
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

# Отключаем режим разработки Flask (убирает WARNING о продакшн-сервере)
os.environ["FLASK_ENV"] = "production"


from app import create_app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)

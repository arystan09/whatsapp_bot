import logging
import json
import requests
import re
import sys
from flask import current_app, jsonify
from app.services.openai_service import generate_response, ChatMode, set_user_mode, detect_language

logging.getLogger().setLevel(logging.WARNING)
# Настройка логирования с поддержкой UTF-8
logger = logging.getLogger()
if not logger.hasHandlers():
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    try:
        console_handler.stream.reconfigure(encoding="utf-8")  # Python 3.7+
    except AttributeError:
        pass  # Если старый Python, игнорируем

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

def log_http_response(response):
    """Логирует HTTP-ответ с сокращенной детализацией."""
    logging.debug(f"HTTP Status: {response.status_code}")
    logging.debug(f"Content-Type: {response.headers.get('content-type')}")
    logging.debug("Response Body: " + json.dumps(response.text, ensure_ascii=False))

def process_text_for_whatsapp(text):
    """Форматирует текст для отправки в WhatsApp."""
    if not text:
        return ""
    text = re.sub(r"\【.*?\】", "", text).strip()  # Удаляет 【...】 скобки
    text = re.sub(r"\*\*(.*?)\*\*", r"*\1*", text)  # Преобразует **жирный** в *жирный*
    return text

def send_greenapi_message(wa_id, text):
    """Отправляет текстовое сообщение через GreenAPI."""
    id_instance = current_app.config.get("GREENAPI_IDINSTANCE")
    api_token = current_app.config.get("GREENAPI_APITOKEN")
    if not id_instance or not api_token:
        logging.error("Отсутствуют учетные данные GreenAPI в конфигурации.")
        return None

    url = f"https://api.green-api.com/waInstance{id_instance}/SendMessage/{api_token}"
    phone_sanitized = wa_id.replace("+", "").strip()
    if not (phone_sanitized.endswith("@c.us") or phone_sanitized.endswith("@g.us")):
        phone_sanitized += "@c.us"

    payload = {"chatId": phone_sanitized, "message": text}
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        log_http_response(response)
        return response
    except requests.Timeout:
        logging.error(f"Timeout при отправке сообщения в {wa_id}")
        return None
    except requests.RequestException as e:
        logging.error(f"Ошибка отправки сообщения в {wa_id}: {e}")
        return None

def process_greenapi_message(body):
    try:
        logging.debug("Webhook received: " + json.dumps(body, indent=2, ensure_ascii=False))

        type_webhook = body.get("typeWebhook", "")
        sender_data = body.get("senderData", {})
        chat_id = sender_data.get("chatId", "")
        sender = sender_data.get("sender", "")
        sender_name = sender_data.get("senderName", sender)

        instance_data = body.get("instanceData", {})
        bot_number = instance_data.get("wid", "")  # Номер бота в WhatsApp

        # Если бот отправил сообщение самому себе, переопределяем тип вебхука
        if sender == bot_number and type_webhook == "outgoingMessageReceived":
            logging.info("Сообщение от бота самому себе обнаружено, обрабатываем как входящее.")
            type_webhook = "incomingMessageReceived"

        # Игнорируем неважные события
        if type_webhook not in ["incomingMessageReceived"]:
            logging.debug(f"Игнорируем webhook типа: {type_webhook}")
            return jsonify({"status": "ignored"}), 200

        # Игнорируем сообщения из групповых чатов
        if chat_id.endswith("@g.us"):
            logging.debug("Групповое сообщение проигнорировано.")
            return jsonify({"status": "ignored", "message": "Group messages are ignored."}), 200

        message_text = None
        message_data = body.get("messageData", {})
        message_type = message_data.get("typeMessage", "")

        # Автоматически переключаем на менеджера, если сообщение не текстовое
        if message_type not in ["textMessage", "extendedTextMessage"]:
            logging.info(f"Неподдерживаемый тип сообщения от {sender} ({message_type}). Переключаем на менеджера.")
            
            # Переключаем пользователя в режим общения с менеджером
            set_user_mode(sender, ChatMode.MANAGER)

            response_ru = " Вы отправили сообщение не в текстовом формате. Переключаю вас на менеджера, он скоро ответит!"
            response_kz = " Сіз мәтін емес хабарлама жібердіңіз. Менеджерге қосамын, ол сізге жауап береді!"

            # Определяем язык пользователя
            lang = detect_language("")
            bot_reply = response_ru if lang == "ru" else response_kz
            
            send_greenapi_message(chat_id, bot_reply)
            
            return jsonify({"status": "switched", "message": "User switched to manager mode due to non-text message."}), 200


        # Получаем текст сообщения
        if "textMessageData" in message_data:
            message_text = message_data["textMessageData"].get("textMessage", "")
        elif "extendedTextMessageData" in message_data:
            message_text = message_data["extendedTextMessageData"].get("text", "")

        if not message_text:
            logging.warning(f"Входящее сообщение от {sender} не содержит текста.")
            return jsonify({"status": "ignored", "message": "Empty text message ignored."}), 200

        logging.debug(f"Входящее сообщение от {sender_name} ({sender}): {message_text}")

        # Определяем режим пользователя
        current_mode = ChatMode.BOT  # По умолчанию бот-режим
        bot_reply = generate_response(message_text, sender, sender_name)

        if current_mode == ChatMode.MANAGER:
            logging.info(f"{sender} в режиме MANAGER. Отправляем автоответ.")
            auto_reply = "Вы на связи с менеджером. Пожалуйста, ожидайте."
            send_greenapi_message(chat_id, auto_reply)
            return jsonify({"status": "success"}), 200

        if bot_reply:
            formatted_reply = process_text_for_whatsapp(bot_reply)
            response = send_greenapi_message(chat_id, formatted_reply)

            if response:
                logging.info(f"Бот ответил {sender_name}: {formatted_reply.encode('utf-8', 'ignore').decode('utf-8')}")
            else:
                logging.error(f"Ошибка отправки ответа {sender_name}")
        else:
            logging.warning(f"Не удалось сгенерировать ответ для {sender_name}.")

        return jsonify({"status": "success"}), 200

    except KeyError as e:
        logging.error("Ошибка структуры сообщения GreenAPI: " + str(e))
        return jsonify({"status": "error", "message": "Invalid structure."}), 400
    except Exception as e:
        logging.error("Непредвиденная ошибка: " + str(e))
        return jsonify({"status": "error", "message": "Internal server error."}), 500


def is_valid_greenapi_message(body):
    """Проверяет, является ли вебхук валидным."""
    if not isinstance(body, dict):
        return False

    valid_types = {
        "incomingMessageReceived",
        "outgoingMessageStatus",
        "outgoingAPIMessageReceived",
        "outgoingMessageReceived",
        "quotaExceeded",
        "stateInstanceChanged",
    }

    return body.get("typeWebhook") in valid_types

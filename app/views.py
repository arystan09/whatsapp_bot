import logging
import json
from flask import Blueprint, request, jsonify

from .utils.whatsapp_utils import process_greenapi_message, is_valid_greenapi_message

logging.getLogger().setLevel(logging.WARNING)

webhook_blueprint = Blueprint("webhook", __name__)

@webhook_blueprint.route("/webhook", methods=["POST"])
def webhook_post():
    """Основной обработчик вебхуков от GreenAPI."""
    try:
        raw_data = request.data.decode("utf-8", errors="ignore")
        logging.debug(f"Raw Request Data: {raw_data}")  # Логируем только в DEBUG

        # Парсим JSON
        try:
            data = json.loads(raw_data)
            logging.debug(f"Parsed Webhook Payload: {json.dumps(data, indent=2, ensure_ascii=False)}")
        except json.JSONDecodeError:
            logging.error("Invalid JSON format in webhook")
            return jsonify({"error": "Invalid JSON"}), 400

        # Проверяем корректность вебхука
        type_webhook = data.get("typeWebhook", "")
        if not is_valid_greenapi_message(data):
            logging.error(f"Invalid GreenAPI webhook format: {type_webhook}")
            return jsonify({"error": "Invalid webhook"}), 400

        # Игнорируем неважные события
        ignored_events = {"outgoingMessageReceived", "outgoingAPIMessage", "outgoingMessageStatus", "stateInstanceChanged"}
        if type_webhook in ignored_events:
            logging.debug(f"Ignored webhook event: {type_webhook}")
            return jsonify({"status": "ok"}), 200

        # Обработка API-сообщений
        if type_webhook == "outgoingAPIMessageReceived":
            message_text = data["messageData"].get("extendedTextMessageData", {}).get("text", "")
            logging.debug(f"API message sent: {message_text}")
            return jsonify({"status": "ok", "message": "API message received"}), 200

        # Обработка исходящих сообщений (от бота)
        if type_webhook == "outgoingMessageReceived":
            message_text = data["messageData"].get("textMessageData", {}).get("textMessage", "")
            chat_name = data["senderData"].get("chatName", "Unknown")
            logging.debug(f"Message sent to {chat_name}: {message_text}")
            return jsonify({"status": "ok", "message": "Outgoing message received"}), 200

        # Передаем дальше обработку входящих сообщений
        return process_greenapi_message(data)

    except Exception as e:
        logging.error(f"Internal server error: {e}")
        return jsonify({"status": "error", "message": "Internal server error"}), 500

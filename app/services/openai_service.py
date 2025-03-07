import openai
import shelve
import os
import logging
import sys
import threading
from threading import Timer
from typing import Optional, Tuple, List
import string

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, String, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import enum

# Для быстрого поиска (RapidFuzz)
from rapidfuzz import process, fuzz

# Модуль для работы с Google Sheets (убедитесь, что он настроен и работает)
from app.services.google_sheets_service import get_sheet_data

# ---------------------------
# Настройка кодировки консоли
# ---------------------------
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

# ---------------------------
# Загрузка переменных окружения
# ---------------------------
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
WHATSAPP_API_URL = os.getenv("WHATSAPP_API_URL", "")
WHATSAPP_API_TOKEN = os.getenv("WHATSAPP_API_TOKEN", "")

openai.api_key = OPENAI_API_KEY

# ---------------------------
# Настройка логирования
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
    encoding="utf-8"
)

# ---------------------------
# Потокобезопасность
# ---------------------------
lock = threading.Lock()

# ---------------------------
# Настройка базы данных SQLite через SQLAlchemy
# ---------------------------
Base = declarative_base()

class ChatMode(enum.Enum):
    BOT = "bot"
    MANAGER = "manager"

class UserState(Base):
    __tablename__ = 'user_states'
    wa_id = Column(String, primary_key=True)
    mode = Column(Enum(ChatMode), default=ChatMode.BOT)

engine = create_engine('sqlite:///bot_database.db')
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)

def get_user_mode(wa_id: str) -> ChatMode:
    session = SessionLocal()
    try:
        user = session.query(UserState).filter_by(wa_id=wa_id).first()
        if user:
            return user.mode
        # Если пользователя нет — создаём запись с режимом BOT
        new_user = UserState(wa_id=wa_id, mode=ChatMode.BOT)
        session.add(new_user)
        session.commit()
        return new_user.mode
    finally:
        session.close()

def set_user_mode(wa_id: str, mode: ChatMode):
    session = SessionLocal()
    try:
        user = session.query(UserState).filter_by(wa_id=wa_id).first()
        if user:
            user.mode = mode
        else:
            user = UserState(wa_id=wa_id, mode=mode)
            session.add(user)
        session.commit()
    finally:
        session.close()

# ---------------------------
# Словари ключевых слов для определения языка
# ---------------------------
RU_KEYWORDS = {"привет", "здравствуйте", "добрый", "день", "вечер", "менеджер", "купить", "заказать", 
               "наличие", "доставка", "бот", "разговор", "флакон", "полный", "объем", "переключить", "связаться"}
KZ_KEYWORDS = {"сәлем", "қайырлы", "күн", "менеджер", "сатып", "алу", "бар", "ма", "жеткізу", 
               "бот", "әңгіме", "флакон", "толық", "көлем", "қосылу", "байланыс"}

def detect_language(message: str) -> str:
    """
    Определяет язык сообщения на основе пересечения ключевых слов.
    Возвращает 'ru' для русского, 'kz' для казахского.
    Если язык не определён однозначно — возвращаем 'ru' по умолчанию.
    """
    # Преобразуем сообщение в набор слов
    words = set(message.lower().split())

    ru_matches = len(words & RU_KEYWORDS)
    kz_matches = len(words & KZ_KEYWORDS)

    if kz_matches > ru_matches:
        return "kz"
    else:
        return "ru"

# ---------------------------
# Конфигурация для Google Sheets
# ---------------------------
JSON_KEYFILE = "data/credentials.json"  # Путь к Google-ключам
SHEET_ID = "Парфюм"
ORIGINAL_SHEET = "original"
SPILLED_SHEET = "spilled"

def load_products_data(sheet_name: str) -> List[dict]:
    try:
        data = get_sheet_data(JSON_KEYFILE, SHEET_ID, sheet_name)
        logging.info(f"Лист '{sheet_name}' загружен ({len(data)} строк).")
        return data
    except Exception as e:
        logging.error(f"Ошибка загрузки листа '{sheet_name}': {e}")
        return []


def load_and_prepare_products(sheet_name: str, product_type: str) -> List[dict]:
    raw_data = get_sheet_data(JSON_KEYFILE, SHEET_ID, sheet_name)
    for item in raw_data:
        # Проставляем тип товара (original/spilled)
        item['type'] = product_type

        # Если это полный флакон (original), приводим volume к нужному виду
        if product_type == 'original':
            vol = item.get('volume', 'N/A')
            if isinstance(vol, (int, float)):
                item['volume'] = f"{vol}ml"
            else:
                item['volume'] = str(vol).strip()
        else:
            # Для разливных всегда '1ml'
            item['volume'] = '1ml'

        item['brand'] = str(item.get('brand', '')).strip()

    return raw_data


def deduplicate_products(products: List[dict]) -> List[dict]:
    seen = set()
    unique = []
    for product in products:
        name_lower = str(product.get('name', '')).strip().lower()
        product_type = str(product.get('type', '')).strip().lower()
        volume = str(product.get('volume', '')).strip().lower()
        
        key = (name_lower, product_type, volume)
        if key not in seen:
            seen.add(key)
            product['name'] = str(product.get('name', '')).title().strip()
            unique.append(product)
        else:
            logging.debug(f"Дубликат пропущен: {product.get('name')} / {volume}")
    return unique


def get_unique_brands(products: List[dict]) -> set:
    brands = set()
    for p in products:
        brand = p.get('brand', '')
        if brand:
            brands.add(brand)
    return brands

products_data: List[dict] = []
unique_brands: set = set()

def refresh_products_data():
    global products_data, unique_brands
    logging.info("Обновляем данные о продуктах из Google Sheets...")
    original_list = load_and_prepare_products(ORIGINAL_SHEET, 'original')
    spilled_list = load_and_prepare_products(SPILLED_SHEET, 'spilled')
    combined = original_list + spilled_list
    products_data = deduplicate_products(combined)
    unique_brands = get_unique_brands(products_data)
    logging.info(f" Всего товаров загружено: {len(products_data)}")
    logging.info(f"Уникальных брендов загружено: {len(unique_brands)}")
    logging.info(" Список загруженных товаров:")
    for product in products_data:
        logging.info(f"- {product.get('name')} ({product.get('type')})")

def periodic_update():
    refresh_products_data()
    Timer(300, periodic_update).start()

periodic_update()


def find_products_by_brand(brand: str, products: List[dict]) -> List[dict]:
    return [
        p for p in products
        if fuzz.token_set_ratio(p.get('brand', '').lower(), brand.lower()) >= 70
    ]

def is_follow_up_question(message: str, items: List[dict]) -> bool:
    keywords = ["цена", "стоимость", "где купить", "наличие", "доступно", "сколько стоит"]
    msg_lower = message.lower()
    has_keyword = any(k in msg_lower for k in keywords)
    has_product_name = any(p['name'].lower() in msg_lower for p in items)
    return has_keyword and not has_product_name

def is_purchase_request(message: str) -> bool:
    buy_keywords = ["купить", "заказать", "оформить заказ", "купить сейчас", "хочу купить", "закажу","сатып алу", "тапсырыс беру" ]
    return any(k in message.lower() for k in buy_keywords)

def save_last_product(wa_id: str, product: dict):
    with shelve.open("last_product_db", writeback=True) as db:
        db[wa_id] = product

def get_last_product(wa_id: str, query: Optional[str] = None) -> Optional[dict]:
    with shelve.open("last_product_db") as db:
        last_product = db.get(wa_id)

        # Если есть запрос, проверяем, соответствует ли последний товар названию
        if query and last_product:
            match_score = fuzz.partial_ratio(query.lower(), last_product["name"].lower())
            if match_score >= 85:  # Порог схожести
                return last_product
            return None  # Если не соответствует
        return last_product


def save_user_conversation(wa_id: str, user_text: str, bot_text: str):
    with shelve.open("conversation_history", writeback=True) as db:
        if wa_id not in db:
            db[wa_id] = []
        db[wa_id].append({"user_message": user_text, "bot_response": bot_text})

def get_user_conversation(wa_id: str, max_messages: int = 10) -> List[dict]:
    with shelve.open("conversation_history") as db:
        full_history = db.get(wa_id, [])
        return full_history[-max_messages:]

# ---------------------------
# Функция определения языка
# ---------------------------
RU_KEYWORDS = {"привет", "здравствуйте", "добрый", "день", "вечер", "менеджер", "купить", "заказать",
               "наличие", "доставка", "бот", "разговор", "флакон", "полный", "объем", "переключить", "связаться"}
KZ_KEYWORDS = {"сәлем", "қайырлы", "күн", "менеджер", "сатып", "алу", "бар", "ма", "жеткізу",
               "бот", "әңгіме", "флакон", "толық", "көлем", "қосылу", "байланыс"}

def detect_language(message: str) -> str:
    """
    Определяет язык сообщения (ru/kz) на основе ключевых слов
    Если язык определить не удалось — возвращаем 'ru' по умолчанию
    """
    words = set(message.lower().split())
    ru_matches = len(words & RU_KEYWORDS)
    kz_matches = len(words & KZ_KEYWORDS)
    if kz_matches > ru_matches:
        return "kz"
    else:
        return "ru"

# ---------------------------
# Основная логика
# ---------------------------
def get_products_list():
    return "\n".join([f"{p['name']} ({p['cost']} KZT)" for p in products_data]) 


def extract_brand_from_message(message: str) -> Tuple[Optional[str], Optional[List[str]], bool]:
    """
    Более гибкий поиск бренда с помощью fuzzy, чтобы 'Armani' находил 'Giorgio Armani'.
    Возвращает (best_match, None, is_spilled).
    """
    message_clean = message.lower().translate(str.maketrans('', '', string.punctuation)).strip()
    is_spilled = any(word in message_clean for word in ["разлив", "разливные", "құйма"])

    best_match = None
    highest_score = 0

    for brand in unique_brands:
        # Тут можно попробовать token_set_ratio
        score = fuzz.token_set_ratio(message_clean, brand.lower())
        if score > highest_score:
            highest_score = score
            best_match = brand

    # Порог лучше подбирать на практике
    if best_match and highest_score >= 60:
        return best_match, None, is_spilled
    else:
        return None, None, is_spilled



def search_product(query: str) -> Optional[dict]:
    query = query.lower().strip()
    logging.info(f"Поиск продукта: {query}")

    # 1. Прямое совпадение по названию или бренду
    for product in products_data:
        if query in product["name"].lower() or query in product["brand"].lower():
            return product

    # 2. Улучшенный поиск по бренду
    extracted_brand, _, _ = extract_brand_from_message(query)
    if extracted_brand:
        brand_products = [p for p in products_data if fuzz.token_sort_ratio(extracted_brand.lower(), p["brand"].lower()) >= 75]
        if brand_products:
            return brand_products[0]

    # 3. Улучшенный fuzzy поиск по названию
    best_match = process.extractOne(query, [p["name"].lower() for p in products_data], scorer=fuzz.token_sort_ratio)
    if best_match and best_match[1] >= 70:
        return next((p for p in products_data if fuzz.token_sort_ratio(p["name"].lower(), best_match[0]) >= 80), None)

    logging.info(f"Продукт '{query}' не найден в базе.")
    return None


def find_best_match(query: str, items: List[dict]) -> Optional[dict]:
    """
    Улучшенный поиск товара с приоритетом на 'original'.
    Если пользователь в тексте явно не просил 'разлив', 'спиллед' и т.п.,
    то сначала ищем среди original, потом — среди spilled.
    """

    query_clean = query.lower().strip()

    # Проверяем, спрашивает ли пользователь о разливе
    spilled_keywords = ["разлив", "разливные", "құйма", "отливант", "sample", "decant", "1ml", "1 мл"]
    user_asks_spilled = any(kw in query_clean for kw in spilled_keywords)

    # Функция для fuzzy-поиска внутри списка
    def fuzzy_search(q, candidates):
        best = process.extractOne(
            q,
            [c["name"].lower() for c in candidates if "name" in c],
            scorer=fuzz.token_sort_ratio
        )
        if best and best[1] >= 70:
            # Находим сам товар
            return next((p for p in candidates if p["name"].lower() == best[0]), None)
        return None

    if user_asks_spilled:
        # Если пользователь явно говорит про разлив
        spilled_only = [p for p in items if p.get("type") == "spilled"]
        return fuzzy_search(query_clean, spilled_only)

    else:
        # Сначала пытаемся найти original
        original_only = [p for p in items if p.get("type") == "original"]
        found_original = fuzzy_search(query_clean, original_only)

        if found_original:
            return found_original
        else:
            # Если в original ничего не нашли, пробуем spilled
            spilled_only = [p for p in items if p.get("type") == "spilled"]
            return fuzzy_search(query_clean, spilled_only)



def is_price_query(message: str) -> bool:
    """
    Проверяет, спрашивает ли пользователь цену.
    """
    price_keywords = ["цена", "сколько стоит", "стоимость", "по чем", "қанша тұрады"]
    text = message.lower()
    return any(kw in text for kw in price_keywords)

def is_general_recommendation_query(message: str) -> bool:
    """
    Проверяет, хочет ли пользователь общую консультацию по выбору аромата,
    а не конкретный товар.
    """
    recommendation_keywords = [
        "подобрать", "помогите выбрать", "посоветуйте", "подскажите",
        "рекомендовать", "лучший аромат", "что выбрать", "совет"
    ]
    
    # Если в сообщении есть слова о выборе, считаем это рекомендацией
    text = message.lower()
    return any(kw in text for kw in recommendation_keywords)


def generate_response(message_body: str, wa_id: str, sender_name: str) -> Optional[str]:
    with lock:
        logging.info(f"Пользователь {wa_id} спрашивает: {message_body}")


        # 1. Проверка пустого сообщения
        if not message_body or not isinstance(message_body, str):
            logging.info(f"Получено не текстовое сообщение от {wa_id}. Переключаем на менеджера.")

            set_user_mode(wa_id, ChatMode.MANAGER)

            response_ru = "Вы отправили сообщение не в текстовом формате. Переключаю вас на менеджера, он скоро ответит!"
            response_kz = "Сіз мәтін емес хабарлама жібердіңіз. Менеджерге қосамын, ол сізге жауап береді!"

            return response_ru if detect_language("") == "ru" else response_kz



        # 2. Приветствие нового пользователя
        with shelve.open("user_sessions") as db:
            if wa_id not in db:  # Если это первое сообщение от пользователя
                db[wa_id] = True  # Записываем в базу, что пользователь уже получил приветствие
                
                welcome_message_ru = (
                    f" Здравствуйте, {sender_name}! \n\n"
                    "Если хотите оформить заказ, напишите *'менеджер'*, и я вас соединю.\n"
                    "Если хотите подобрать парфюм, укажите предпочтения (например: цветочный, свежий, сладкий) "
                    "или название конкретного аромата.\n"
                    "Я помогу вам с ценами, наличием и подбором.\n\n"
                    "Чем могу помочь? "
                )
                welcome_message_kz = (
                    f" Сәлеметсіз бе, {sender_name}! \n\n"
                    "Егер сіз тапсырыс бергіңіз келсе, *'менеджер'* деп жазыңыз, мен сізді қосамын.\n"
                    "Егер сізге хош иіс таңдау қажет болса, өз қалауыңызды айтыңыз "
                    "(мысалы: гүлді, сергіткіш, тәтті) немесе нақты иісті атаңыз.\n"
                    "Мен баға, қол жетімділік және таңдау бойынша көмектесе аламын.\n\n"
                    "Қалай көмектесе аламын? "
                )

                response = welcome_message_ru if detect_language(message_body) == "ru" else welcome_message_kz
                return response  # Отправляем приветственное сообщение и завершаем обработку


        # 3. Определяем язык, проверяем режим (BOT / MANAGER)
        lower_msg = message_body.lower()
        lang = detect_language(lower_msg)  # Определяем язык
        current_mode = get_user_mode(wa_id)

        if current_mode == ChatMode.MANAGER:
            # Если пользователь в режиме MANAGER, но хочет завершить
            if ("завершить разговор" in lower_msg or "бот" in lower_msg) or \
               ("әңгіме" in lower_msg and "аяқтау" in lower_msg):
                set_user_mode(wa_id, ChatMode.BOT)
                response_ru = "Диалог с менеджером завершён, я снова к вашим услугам!"
                response_kz = "Менеджермен сөйлесу аяқталды, мен қайтадан сізге көмектесе аламын!"
                return response_ru if lang == "ru" else response_kz
            return None
        

        # 4. Проверка, хочет ли пользователь менеджера
        if ("переключить" in lower_msg or "менеджер" in lower_msg or "связаться с менеджером" in lower_msg) or \
           ("менеджер" in lower_msg and "байланыс" in lower_msg):
            set_user_mode(wa_id, ChatMode.MANAGER)
            response_ru = "Я переключаю вас на менеджера. Ожидайте, он скоро с вами свяжется!"
            response_kz = "Мен сізді менеджерге қосамын. Ол сізбен жақында байланысады!"
            return response_ru if lang == "ru" else response_kz


        # 5. Проверка, хочет ли пользователь завершить разговор
        if ("завершить разговор" in lower_msg or "бот" in lower_msg) or \
           ("әңгіме" in lower_msg and "аяқтау" in lower_msg):
            set_user_mode(wa_id, ChatMode.BOT)
            response_ru = "Диалог с менеджером завершён, я снова к вашим услугам!"
            response_kz = "Менеджермен сөйлесу аяқталды, мен қайтадан сізге көмектесе аламын!"
            answer = response_ru if lang == "ru" else response_kz
            return answer

        # 6. **Приветствие** (если пользователь просто поздоровался)
        greeting_ru = ["привет", "здравствуйте", "добрый день", "добрый вечер", "салам"]
        greeting_kz = ["сәлем", "қайырлы күн", "қайырлы кеш" "салеметсизбе", "салеметсиз бе", "салем",]

        if (any(word in lower_msg for word in greeting_ru) and lang == "ru") or \
           (any(word in lower_msg for word in greeting_kz) and lang == "kz"):
            resp_ru = ( f" Здравствуйте, {sender_name}!\n\n" 
                       "Если хотите оформить заказ, напишите *'менеджер'*, и я вас соединю.\n" 
                       "Если хотите подобрать парфюм, укажите предпочтения (например: цветочный, свежий, сладкий) " "или название конкретного аромата.\n" 
                       "Я помогу вам с ценами, наличием и подбором.\n\n" 
                       "Чем могу помочь? " )
            resp_kz = (
                    f" Сәлеметсіз бе, {sender_name}! Мен парфюмерия дүкенінің виртуалды көмекшісімін.\n\n"
                    "Егер сіз тапсырыс бергіңіз келсе, *'менеджер'* деп жазыңыз, мен сізді қосамын.\n"
                    "Егер сізге хош иіс таңдау қажет болса, өз қалауыңызды айтыңыз "
                    "(мысалы: гүлді, сергіткіш, тәтті) немесе нақты иісті атаңыз.\n"
                    "Мен баға, қол жетімділік және таңдау бойынша көмектесе аламын.\n\n"
                    "Қалай көмектесе аламын? "
                )
            response = resp_ru if lang == "ru" else resp_kz
            return response
        
        # 7. **Проверка запроса про адрес** 
        if any(word in lower_msg for word in ["адрес", "где вы", "местоположение", "где находится", "как добраться"]) or \
        any(word in lower_msg for word in ["мекенжай", "қай жерде", "орналасқан", "қайда"]):

            resp_ru = (
                "Наш магазин парфюмерии aera находится по адресу: \n"
                "📍 г. Астана, ул. Мангилик Ел 51, 1 этаж.\n\n"
                "Мы работаем ежедневно с 10:00 до 22:00. Будем рады видеть вас!\n"
                "Вы также можете оформить заказ онлайн через наш сайт: aera.kz.\n"
                "Если хотите связаться с менеджером, напишите *'менеджер'*."
            )

            resp_kz = (
                "Біздің aera парфюмерия дүкені келесі мекенжайда орналасқан: \n"
                "📍 Астана қ., Мәңгілік Ел 51, 1-қабат.\n\n"
                "Біз күн сайын 10:00 - 22:00 аралығында жұмыс істейміз. Келіңіз, сізді күтеміз!\n"
                "Сондай-ақ, сіз біздің сайт арқылы онлайн тапсырыс бере аласыз: aera.kz.\n"
                "Менеджермен байланысу үшін *'менеджер'* деп жазыңыз."
            )

            response = resp_ru if lang == "ru" else resp_kz
            return response


        # 8. Шаблонные ответы (пример: доставка)
        if "доставка" in lower_msg or "жеткізу" in lower_msg:
            resp_ru = (
                "Мы доставляем заказы по всему Казахстану:\n"
                "В пределах г. Астана — стандартная доставка.\n"
                "По Казахстану — бесплатная доставка при заказе от 30 000 KZT.\n"
                "В другие города Казахстана (кроме Астаны) доставка через Казпочту — 5 рабочих дней (зависит от региона).\n"
                "Доставка по СНГ — 20 000 KZT.\n\n"
                "Вы можете уточнить точную стоимость и сроки у менеджера при оформлении заказа.\n"
                "Если хотите поговорить с менеджером, напишите *'менеджер'*."
            )

            resp_kz = (
                "Біз Қазақстан бойынша жеткіземіз:\n"
                "Астана қаласы бойынша — стандартты жеткізу.\n"
                "Қазақстан бойынша — 30 000 KZT жоғары тапсырыс болса, тегін жеткізу.\n"
                "Қазақстанның басқа қалаларына (Астанадан басқа) Казпошта арқылы — 5 жұмыс күні (өңірге байланысты).\n"
                "ТМД елдеріне жеткізу — 20 000 KZT.\n\n"
                "Нақты құнын және мерзімін тапсырыс беру кезінде менеджерден біле аласыз.\n"
                "Егер сіз менеджермен сөйлескіңіз келсе, *'менеджер'* деп жазыңыз."
            )

            return resp_ru if lang == "ru" else resp_kz
        

                # 8(2). Проверяем, спрашивает ли пользователь про рассрочку
        installment_keywords = [
            "рассрочка", "оплата частями", "kaspi red", "kaspi рассрочка",
            "можно в рассрочку", "можно ли оплатить частями"
        ]

        if any(word in lower_msg for word in installment_keywords):
            resp_ru = (
                "Мы предоставляем возможность оплаты в рассрочку. "
                "Для уточнения деталей напишите *'менеджер'*, он подскажет все условия!"
            )
            resp_kz = (
                "Біз Kaspi Red арқылы бөліп төлеу мүмкіндігін ұсынамыз. "
                "Толық ақпаратты алу үшін *'менеджер'* деп жазыңыз!"
            )

            response = resp_ru if lang == "ru" else resp_kz
            save_user_conversation(wa_id, message_body, response)
            return response

        # 8(3). Проверяем, спрашивает ли пользователь про оригинал или копию
        originality_keywords = [
            "оригинал", "копия", "реплика", "подделка", "настоящий",
            "сертифицированный", "оригинальная продукция", "реплика или оригинал"
        ]

        if any(word in lower_msg for word in originality_keywords):
            resp_ru = (
                "Вся продукция в нашем магазине является оригинальной и сертифицированной. "
                "Если у вас есть дополнительные вопросы, напишите *'менеджер'*, он предоставит всю информацию!"
            )
            resp_kz = (
                "Біздің дүкендегі барлық өнімдер түпнұсқа және сертификатталған. "
                "Қосымша сұрақтарыңыз болса, *'менеджер'* деп жазыңыз, ол сізге толық ақпарат береді!"
            )

            response = resp_ru if lang == "ru" else resp_kz
            save_user_conversation(wa_id, message_body, response)
            return response


        # 9. Проверка, хочет ли пользователь общую рекомендацию
        if is_general_recommendation_query(lower_msg):
            logging.info(f"Запрос на рекомендацию: {message_body}")

            try:
                conversation = get_user_conversation(wa_id)
                messages = [
                    {
                        "role": "system",
                        "content": (
                        "Ты — ассистент магазина парфюмерии. Отвечай кратко на русском или казахском.\n"
                        "У тебя есть база товаров (ниже), содержащая поля `name`, `volume`, `cost`, `country`.\n"
                        "Ты можешь предоставлять пользователю ТОЛЬКО информацию из этих полей.\n\n"
                        "Если пользователь спрашивает про любой товар, которого нет в этом списке, скажи, "
                        "что его нет в наличии, и предложи обратиться к менеджеру.\n"
                        "Если у товара в базе нет указанных полей (например, нет `volume`), скажи, "
                        "что такой информации в базе нет и предложи обратиться к менеджеру.\n\n"
                        "НЕЛЬЗЯ придумывать или дополнять поля `name`, `volume`, `cost`, `country` "
                        "значениями, которых нет в базе. Никаких гипотез!\n\n"
                        "Вот список товаров:\n"
                        f"{get_products_list()}\n"
                        "Если запрос не относится к товарам или базе, предложи обратиться к менеджеру."
                        "Если у пользователя остались вопросы, предлагай написать *'менеджер'* для связи с сотрудником.\n"
                        "Если не можешь найти товар, просто сообщи, что переключаешь пользователя на менеджера."
                        )
                    }
                ]

                for c in conversation:
                    messages.append({"role": "user", "content": c["user_message"]})
                    messages.append({"role": "assistant", "content": c["bot_response"]})
                messages.append({"role": "user", "content": message_body})

                gpt_response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=messages,
                    max_tokens=500,
                    temperature=0.7
                )
                answer_raw = gpt_response["choices"][0]["message"]["content"].strip()

                # Проверяем, есть ли в ответе упоминание разливных ароматов
                is_spilled_response = any(p['name'].lower() in answer_raw.lower() and p['type'] == 'spilled' for p in products_data)

                # Проверяем, что в ответе есть конкретный продукт и указана цена
                has_price = any(str(p['cost']) in answer_raw for p in products_data if p['cost'])

                # Если ответ точно о разливном аромате и есть цена, добавляем уточнение
                if is_spilled_response and has_price:
                    answer_raw += "\n *Некоторые цены указаны за 1 мл.*"



                save_user_conversation(wa_id, message_body, answer_raw)
                return answer_raw
                
            except Exception as e:
                logging.error(f"Ошибка при обращении к OpenAI: {e}")
                resp_ru = "Извините, не могу найти информацию по вашему вопросу. Если хотите поговорить с менеджером, напишите 'менеджер'."
                resp_kz = "Кешіріңіз, бізде бұл сұраққа қатысты ақпарат жоқ. Егер сіз менеджермен сөйлескіңіз келсе, «менеджер» деп жазыңыз."
                return resp_ru if lang == "ru" else resp_kz


        # 8. Полный флакон
        if any(word in lower_msg for word in ["полный объем", "флакон", "оригинал", "бутылка"]) or \
        any(word in lower_msg for word in ["толық көлем", "құты"]):

            logging.info("Запрос на оригинальный флакон")
            
            # Пытаемся найти продукт по текущему запросу
            original_product = search_product(lower_msg)
            
            # Если по текущему запросу продукт не найден,
            # просим пользователя уточнить название товара.
            if not original_product:
                response = "Пожалуйста, уточните название товара, для которого вас интересует полный флакон."
                save_user_conversation(wa_id, message_body, response)
                return response
            
            # Если продукт найден, формируем ответ с информацией
            response = (
                f"*{original_product.get('name', 'Неизвестно')}*\n"
                f"_{original_product.get('description', 'нет данных')}_\n"
                f"Объём: {original_product.get('volume', 'нет данных')}\n"
                f"Цена: {original_product.get('cost', 'нет данных')} KZT\n"
                f"Страна: {original_product.get('country', 'нет данных')}\n"
                "------------------------------------"
            )
            save_user_conversation(wa_id, message_body, response)
            save_last_product(wa_id, original_product)
            return response


        # 9. Цена (is_price_query)
        if is_price_query(lower_msg):
            # Получаем последний обсуждаемый товар
            last_product = get_last_product(wa_id)

            # Проверяем, действительно ли пользователь спрашивает о последнем товаре
            if last_product and fuzz.partial_ratio(last_product["name"].lower(), lower_msg) >= 85:
                response = (
                    f"Цена на *{last_product['name']}* составляет {last_product.get('cost', 'нет данных')} KZT.\n"
                    "Если у вас есть дополнительные вопросы или хотите оформить заказ, напишите *'менеджер'*."
                )
                save_user_conversation(wa_id, message_body, response)
                return response

            # Если товар не найден, просим уточнить название
            response = (
                "Уточните, пожалуйста, о каком аромате идет речь? "
                "Напишите его название, и я подскажу цену. Если хотите поговорить с менеджером, напишите *'менеджер'*."
            )
            save_user_conversation(wa_id, message_body, response)
            return response
        
    
        # 10. Проверка уточнений (is_follow_up_question)
        if is_follow_up_question(message_body, products_data):
            last_product = get_last_product(wa_id)
            
            # Проверяем, содержит ли запрос название последнего товара
            if last_product and last_product["name"].lower() in message_body.lower():
                response = (
                    f"*{last_product['name']}*\n"
                    f"_{last_product.get('description', 'Описание недоступно')}_\n"
                    f"Объём: {last_product.get('volume', 'Нет данных')}\n"
                    f"Цена: {last_product.get('cost', 'Нет данных')} KZT\n"
                    f"Страна: {last_product.get('country', 'Нет данных')}\n"
                    "------------------------------------"
                    "Если у вас есть вопросы или хотите оформить заказ, напишите *'менеджер'*."
                )
                save_user_conversation(wa_id, message_body, response)
                return response
            
            # Если бот не помнит товар, спрашиваем пользователя уточнить
            return "Можете уточнить, о каком парфюме идет речь? Напишите его название. Если хотите поговорить с менеджером, напишите 'менеджер'."


        # 11. Разлив
        extracted_brand, ambiguity, is_spilled = extract_brand_from_message(message_body)
        
        if extracted_brand == "Нет бренда":
            logging.info("Бренд не найден, продолжаем обработку другим способом.")
        
        if is_spilled or "разлив" in lower_msg or "разливные" in lower_msg or "құйма" in lower_msg:
            if extracted_brand:
                logging.info(f"Запрос на разливную парфюмерию для бренда: {extracted_brand}")

                # Используем fuzzy matching для поиска товаров с типом "spilled"
                brand_products = [
                    p for p in products_data 
                    if p.get('type') == 'spilled' and 
                    fuzz.token_set_ratio(p.get('brand', '').lower(), extracted_brand.lower()) >= 80
                ]
                
                # Если не найдено ни одного товара, просим уточнить запрос, вместо ответа о не наличии
                if not brand_products:
                    response = "Пожалуйста, уточните название разливного аромата, который вас интересует."
                    save_user_conversation(wa_id, message_body, response)
                    return response

                detailed_request = any(word in lower_msg for word in ["все", "показать", "список", "какие", "барлығы", "қандай"])
                if detailed_request:
                    resp_ru = f"Из разливной парфюмерии бренда {extracted_brand} у нас есть:\n" + "\n".join(
                        [f"{i+1}. {p['name']}" for i, p in enumerate(brand_products)]
                    )
                    resp_kz = f"{extracted_brand} брендіне арналған құйма парфюмерия:\n" + "\n".join(
                        [f"{i+1}. {p['name']}" for i, p in enumerate(brand_products)]
                    )
                    answer = resp_ru if lang == "ru" else resp_kz
                else:
                    # Берем первый подходящий товар
                    p = brand_products[0]
                    resp_ru = (
                        f"*{p.get('name', 'Неизвестно')}*\n"
                        f"_{p.get('description', 'нет данных')}_\n"
                        f"Объём: {p.get('volume', 'нет данных')}\n"
                        f"Цена: {p.get('cost', 'нет данных')} KZT за 1 мл\n"
                        f"Страна: {p.get('country', 'нет данных')}\n"
                        "------------------------------------\n"
                        "Если у вас есть вопросы или хотите оформить заказ, напишите *'менеджер'*."
                    )
                    resp_kz = (
                        f"*{p.get('name', 'Белгісіз')}*\n"
                        f"_{p.get('description', 'мәліметтер жоқ')}_\n"
                        f"Көлемі: {p.get('volume', 'мәліметтер жоқ')}\n"
                        f"Бағасы: {p.get('cost', 'мәліметтер жоқ')} KZT 1 мл\n"
                        f"Елі: {p.get('country', 'мәліметтер жоқ')}\n"
                        "------------------------------------"
                    )
                    answer = resp_ru if lang == "ru" else resp_kz
                save_user_conversation(wa_id, message_body, answer)
                return answer
            else:
                resp_ru = "Уточните, пожалуйста, какой бренд разливной парфюмерии вас интересует? Если у вас есть вопросы или хотите оформить заказ, напишите *'менеджер'*."
                resp_kz = "Қай брендтің құйма парфюмериясы керек екенін нақтылаңызшы?"
                answer = resp_ru if lang == "ru" else resp_kz
                save_user_conversation(wa_id, message_body, answer)
                return answer

            
        # Если есть двусмысленность в бренде
        if ambiguity:
            resp_ru = "Уточните, пожалуйста, какой бренд вы имеете в виду: " + ", ".join(ambiguity)
            resp_kz = "Қай брендті айтып тұрғаныңызды нақтылаңыз: " + ", ".join(ambiguity)
            answer = resp_ru if lang == "ru" else resp_kz
            save_user_conversation(wa_id, message_body, answer)
            return answer


        # 12 Бренд (extract_brand_from_message)
        if extracted_brand:
            logging.info(f"Найден бренд: {extracted_brand}")
            
            # --- Определяем, спрашивает ли пользователь разлив
            spilled_keywords = ["разлив", "разливные", "құйма", "sample", "отливант", "1 мл", "1ml"]
            lower_msg_clean = lower_msg.replace("мл.","мл").strip()
            user_asks_spilled = any(kw in lower_msg_clean for kw in spilled_keywords)

            # --- Собираем товары по бренду (original или spilled)
            if user_asks_spilled:
                brand_products = [
                    p for p in products_data
                    if p.get("brand", "").lower() == extracted_brand.lower()
                    and p.get("type") == "spilled"
                ]
            else:
                brand_products_original = [
                    p for p in products_data
                    if fuzz.token_set_ratio(p["brand"].lower(), extracted_brand.lower()) >= 70
                    and p["type"] == "original"
                ]
                brand_products_spilled = [
                    p for p in products_data
                    if p.get("brand", "").lower() == extracted_brand.lower()
                    and p.get("type") == "spilled"
                ]
                brand_products = brand_products_original if brand_products_original else brand_products_spilled

            # --- Вычисляем leftover
            brand_part = extracted_brand.lower()
            leftover = lower_msg_clean.replace(brand_part, "").strip()

            # --- Если leftover короткий (например, < 3 символов) или пустой,
            #     считаем, что пользователь ввёл только бренд → показываем список
            if not leftover or len(leftover) < 3:
                if not brand_products:
                    # Нет товаров вообще
                    resp_ru = (f"Мы не нашли товары по запросу '{extracted_brand}'. "
                            "Возможно, они записаны по-другому. Напишите *'менеджер'* для уточнения.")
                    answer = resp_ru if lang == "ru" else resp_ru  # заглушка
                    save_user_conversation(wa_id, message_body, answer)
                    return answer

                if len(brand_products) > 10:
                    # Если товаров много
                    response = (
                        f"У нас есть более 10 ароматов бренда {extracted_brand}. "
                        "Уточните, пожалуйста, название аромата, и я покажу подходящие варианты."
                    )
                    save_user_conversation(wa_id, message_body, response)
                    return response
                else:
                    # Если товаров <= 10 — сразу показываем список
                    resp_ru = f"Из парфюмерии {extracted_brand} у нас есть:\n"
                    for i, p in enumerate(brand_products, start=1):
                        resp_ru += f"{i}. {p['name']} - {p['volume']} ({p['cost']} KZT)\n"
                    resp_ru += (
                        "\nЕсли вас интересует конкретный аромат, уточните название. "
                        "Для оформления заказа или консультации напишите *'менеджер'*."
                    )

                    answer = resp_ru if lang == "ru" else resp_ru  # заглушка
                    save_user_conversation(wa_id, message_body, answer)
                    return answer

            # --- Если leftover всё же «длинный» (например, > 2-3 символов),
            #     делаем fuzzy-поиск внутри brand_products
            fuzzy_match = process.extractOne(
                leftover,
                [p["name"].lower() for p in brand_products],
                scorer=fuzz.token_sort_ratio
            )
            if fuzzy_match and fuzzy_match[1] >= 60:
                matched_name = fuzzy_match[0]
                matched_item = next((p for p in brand_products if p["name"].lower() == matched_name), None)
                if matched_item:
                    # Возвращаем информацию об этом конкретном товаре
                    cost_text = matched_item.get('cost', 'нет цены')
                    volume_text = matched_item.get('volume', 'нет данных')
                    desc = matched_item.get('description', 'нет данных')

                    resp_ru = (
                        f"*{matched_item.get('name', 'Неизвестно')}*\n"
                        f"_{desc}_\n"
                        f"Объём: {volume_text}\n"
                        f"Цена: {cost_text} KZT\n"
                        "------------------------------------\n"
                        "Если у вас есть вопросы или хотите оформить заказ, напишите *'менеджер'*."
                    )
                    answer = resp_ru if lang == "ru" else resp_ru
                    save_user_conversation(wa_id, message_body, answer)
                    return answer

            if not brand_products:
                resp_ru = (f"Мы не нашли товары по запросу '{extracted_brand}'. "
                        "Возможно, в базе они записаны по-другому. Напишите *'менеджер'* для полного уточнения.")
                resp_kz = f"Кешіріңіз, {extracted_brand} брендін қазір таба алмадық. Менеджермен сөйлесу үшін 'менеджер' деп жазыңыз."
                answer = resp_ru if lang == "ru" else resp_kz
                save_user_conversation(wa_id, message_body, answer)
                return answer

            if len(brand_products) > 10:
                response = (
                    f"У нас есть более 10 ароматов бренда {extracted_brand}. "
                    "Пожалуйста, уточните название аромата, чтобы я мог показать подходящие варианты."
                )
                save_user_conversation(wa_id, message_body, response)
                return response
            else:
                # Если товаров <= 10 — сразу показываем список
                resp_ru = f"Из парфюмерии {extracted_brand} у нас есть:\n"
                for i, p in enumerate(brand_products, start=1):
                    cost_text = p.get('cost', 'нет цены')
                    resp_ru += f"{i}. {p['name']} - {p['volume']} ({p['cost']} KZT)\n"
                resp_ru += (
                    "\nЕсли вас интересует конкретный вариант, уточните, пожалуйста. "
                    "Для оформления заказа или детальной консультации напишите *'менеджер'*."
                )
                
                resp_kz = f"{extracted_brand} бренді бойынша бізде:\n"
                for i, p in enumerate(brand_products, start=1):
                    cost_text = p.get('cost', 'бағасы көрсетілмеген')
                    resp_kz += f"{i}. {p['name']} ({cost_text} KZT)\n"
                resp_kz += (
                    "\nЕгер нақты бір түрі қызықтырса, нақтылаңыз. "
                    "Тапсырыс беру немесе толық ақпарат алу үшін *'менеджер'* деп жазыңыз."
                )
                
                answer = resp_ru if lang == "ru" else resp_kz
                save_user_conversation(wa_id, message_body, answer)
                return answer



        # 13. Покупка (is_purchase_request)
        if is_purchase_request(lower_msg):
            resp_ru = "Я не могу оформить заказ, но передам ваш запрос менеджеру! Напишите *'менеджер'*, и он свяжется с вами."
            resp_kz = "Мен тапсырысты рәсімдей алмаймын, бірақ сізді менеджерге қосамын! *'менеджер'* деп жазыңыз, ол сізбен байланысады."
            
            response = resp_ru if lang == "ru" else resp_kz
            save_user_conversation(wa_id, message_body, response)
            return response
        

        # 14. Ищем конкретный товар
        matched_product = find_best_match(lower_msg, products_data)

        # Проверяем, что matched_product не список и не None
        if isinstance(matched_product, list) and matched_product:
            matched_product = matched_product[0]  # Берём первый элемент списка
        elif not isinstance(matched_product, dict):
            matched_product = None  # Если не dict и не список, устанавливаем None

        if matched_product:
            # Получаем данные с безопасным `.get()`, чтобы избежать `NoneType` ошибки
            name = matched_product.get('name', 'Неизвестно')
            description = matched_product.get('description', 'нет данных')
            volume = matched_product.get('volume', 'нет данных')
            cost = matched_product.get('cost', 'нет данных')
            country = matched_product.get('country', 'нет данных')

            # Форматируем текст в зависимости от языка
            response_text = {
                "ru": (
                    f"*{name}*\n"
                    f"_{description}_\n"
                    f"Объём: {volume}\n"
                    f"Цена: {cost} KZT\n"
                    f"Страна Производства: {country}\n"
                    "------------------------------------\n"
                    "Если у вас есть вопросы или хотите оформить заказ, напишите *'менеджер'*."
                ),
                "kz": (
                    f"*{name}*\n"
                    f"_{description}_\n"
                    f"Көлемі: {volume}\n"
                    f"Бағасы: {cost} KZT\n"
                    f"Өндіріс елі: {country}\n"
                    "------------------------------------\n"
                    "Сұрақтарыңыз болса немесе тапсырыс бергіңіз келсе, *'менеджер'* деп жазыңыз."
                ),
            }

            # Выбираем нужный язык
            response = response_text.get(lang, response_text["ru"])

            # Сохраняем диалог и последний найденный продукт
            save_user_conversation(wa_id, message_body, response)
            save_last_product(wa_id, matched_product)

            return response

        
        if matched_product is None:
            logging.info("Ничего не нашли по find_best_match.") 
        elif matched_product.get('cost') is None:
            logging.info(f"У товара {matched_product['name']} нет цены.")
        else:
            logging.info(f"Цена продукта {matched_product['name']}: {matched_product['cost']} KZT")




        # 15. Если всё остальное не подошло — просим ChatGPT ответить
        answer_raw = None  # Переменная для хранения ответа от ChatGPT

        try:
            conversation = get_user_conversation(wa_id)
            
            # Составляем системное сообщение с жёсткой инструкцией:
            system_message = (
                "Ты — ассистент магазина парфюмерии. Отвечай кратко на русском или казахском.\n"
                "У тебя есть база товаров (ниже), содержащая поля `name`, `volume`, `cost`, `country`.\n"
                "Ты можешь предоставлять пользователю ТОЛЬКО информацию из этих полей.\n\n"
                # ↓↓↓ В ЭТОМ МЕСТЕ меняем инструкцию ↓↓↓
                "Если пользователь спрашивает про любой товар, которого нет в этом списке, НЕ говори, что его нет, "
                "а сразу советуй переключиться на менеджера.\n"
                # ↑↑↑ Вместо «скажи, что нет в наличии», просим «советуй переключиться на менеджера» ↑↑↑
                "Если у товара в базе нет указанных полей (например, нет `volume`), "
                "скажи, что такой информации нет и тоже предложи обратиться к менеджеру.\n\n"
                "НЕЛЬЗЯ придумывать или дополнять поля `name`, `volume`, `cost`, `country` "
                "значениями, которых нет в базе. Никаких гипотез!\n\n"
                "Вот список товаров:\n"
                f"{get_products_list()}\n"
                "Если запрос не относится к товарам или базе, предложи обратиться к менеджеру. "
                "Если у пользователя остались вопросы, предлагай написать *'менеджер'*.\n"
            )

            messages = [
                {"role": "system", "content": system_message}
            ]

            # Добавляем историю диалога
            for c in conversation:
                messages.append({"role": "user", "content": c["user_message"]})
                messages.append({"role": "assistant", "content": c["bot_response"]})
            messages.append({"role": "user", "content": message_body})

            gpt_response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=messages,
                max_tokens=400,
                temperature=0.2, 
            )
            answer_raw = gpt_response["choices"][0]["message"]["content"].strip()
            save_user_conversation(wa_id, message_body, answer_raw)

            # --- Фильтруем "плохие" ответы (если GPT не нашел товар)
            trigger_phrases = [
                "нет в наличии", "не нашел", "не могу помочь", "переключите на менеджера",
                "не уверен", "не распознал", "уточните у менеджера", "нет аромата", "не смог найти",
                "не продаем", "не представлено", "не доступен", "отсутствует", "нет информации",
                "в базе нет", "не реализуем", "не встречается", "недоступно", "не входит в ассортимент",
                "не могу найти информацию", "мы не занимаемся", "такого товара нет", "такого аромата нет",
                "не представлено в каталоге", "в наличии нет", "не могу найти", "нет товара",
                "нет в наличии", "не продаем", "не смог найти", "не могу найти",
                "такого товара нет", "такого аромата нет", "нет товара", "не представлено", "нет информации", 
                "обратитесь к менеджеру", "в нашем ассортименте нет", "нет продукции", "извините, но в нашем ассортименте нет",

            ]

            # Если в ответе GPT встречается какая-то из «плохих» фраз:
            if any(phrase in answer_raw.lower() for phrase in trigger_phrases):
                logging.warning(f"ChatGPT не дал точный ответ. Переключаем пользователя {wa_id} на менеджера.")
                set_user_mode(wa_id, ChatMode.MANAGER)

                response_ru = "Переключаю вас на менеджера, он поможет вам более детально!"
                response_kz = "Мен сізді менеджерге қосамын, ол сізге егжей-тегжейлі көмектеседі!"
                
                final_response = response_ru if lang == "ru" else response_kz
                save_user_conversation(wa_id, message_body, final_response)
                return final_response

            # Если «плохих фраз» нет — возвращаем ответ GPT
            return answer_raw

        except Exception as e:
            logging.error(f"Ошибка при обращении к OpenAI: {e}")
            answer_raw = None

        # 16. Если совсем ничего не сработало — переключаем на менеджера
        set_user_mode(wa_id, ChatMode.MANAGER)
        response = "Извините, я не смог распознать ваш запрос. Переключаю вас на менеджера для более точного ответа."
        save_user_conversation(wa_id, message_body, response)
        return response


def update_products_data():
    global products_data, unique_brands
    try:
        logging.info("Обновляем данные о продуктах...")
        original_list = load_and_prepare_products(ORIGINAL_SHEET, 'original')
        spilled_list = load_and_prepare_products(SPILLED_SHEET, 'spilled')
        combined = original_list + spilled_list
        products_data = deduplicate_products(combined)

        # Проверяем, что бренды загружены корректно
        unique_brands = get_unique_brands(products_data)
        if not unique_brands:
            logging.error("Ошибка: unique_brands пустой после загрузки!")

        logging.info(f"Продукты обновлены: {len(products_data)} шт.")
        logging.info(f"Уникальных брендов загружено: {len(unique_brands)}")
    except Exception as e:
        logging.error(f"Ошибка обновления продуктов: {e}")
    Timer(3000, update_products_data).start()

update_products_data()
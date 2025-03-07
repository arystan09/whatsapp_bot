import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Подключение к Google Sheets через JSON-ключ
def connect_to_google_sheets(json_keyfile, sheet_name):
    try:
        # Авторизация через сервисный аккаунт
        gc = gspread.service_account(filename=json_keyfile)
        # Открытие таблицы по имени
        sheet = gc.open(sheet_name)
        return sheet
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Ошибка: Таблица с именем '{sheet_name}' не найдена.")
    except Exception as e:
        print(f"Произошла ошибка при подключении к таблице: {e}")

# Получение данных из указанного листа
def get_sheet_data(json_keyfile, sheet_name, worksheet_name):
    sheet = connect_to_google_sheets(json_keyfile, sheet_name)
    if sheet:
        try:
            # Получение листа по имени
            worksheet = sheet.worksheet(worksheet_name)
            # Возвращаем все записи
            return worksheet.get_all_records()
        except gspread.exceptions.WorksheetNotFound:
            print(f"Ошибка: Лист с именем '{worksheet_name}' не найден.")
        except Exception as e:
            print(f"Произошла ошибка при получении данных с листа: {e}")


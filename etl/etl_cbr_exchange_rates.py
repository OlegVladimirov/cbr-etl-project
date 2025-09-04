import pandas as pd
import requests
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
import os
from dateutil.relativedelta import relativedelta

# --- Настройки ---
# URL для данных
DYNAMIC_URL = "https://www.cbr.ru/scripts/XML_dynamic.asp"
VAL_CODES_URL = "https://www.cbr.ru/scripts/XML_valFull.asp"

# Коды валют (из ID в XML)
USD_CODE = "R01235"
EUR_CODE = "R01239"

# Подключение к PostgreSQL
DB_URL = os.getenv("DATABASE_URL") # "postgresql+psycopg2://user:password@host:port/dbname"
if not DB_URL:
    raise ValueError("Переменная окружения DATABASE_URL не установлена!")

# Создание движка SQLAlchemy
engine = create_engine(DB_URL)

# --- Функция: загрузка динамических курсов ---
def fetch_exchange_rates(val_code, days_back=30):
    """
    Загружает курсы валюты за последние N дней.
    Возвращает DataFrame с колонками: date, currency_code, rate
    """
    # Определяем даты: с сегодня - N дней по сегодня
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    params = {
        "date_req1": start_date.strftime("%d/%m/%Y"),
        "date_req2": end_date.strftime("%d/%m/%Y"),
        "VAL_NM_RQ": val_code
    }

    try:
        response = requests.get(DYNAMIC_URL, params=params)
        response.raise_for_status()
        response.encoding = 'windows-1251'  # ЦБ РФ использует win-1251
        root = ET.fromstring(response.content)

        data = []
        for record in root.findall('Record'):
            rdate = record.get('Date')  # дата в формате 05.08.2025
            value_str = record.find('Value').text  # "74,56"
            value = float(value_str.replace(',', '.'))

            # Парсим дату
            date_obj = datetime.strptime(rdate, "%d.%m.%Y").date()

            data.append({
                'date': date_obj,
                'currency_code': val_code,
                'rate': value
            })

        return pd.DataFrame(data)
    except Exception as e:
        print(f"Ошибка при загрузке курсов {val_code}: {e}")
        return pd.DataFrame()

# --- Функция: загрузка справочника валют ---
def fetch_currency_reference():
    """
    Загружает полный справочник валют.
    Возвращает DataFrame, отфильтрованный по USD и EUR.
    """
    try:
        response = requests.get(VAL_CODES_URL)
        response.raise_for_status()
        response.encoding = 'windows-1251'
        root = ET.fromstring(response.content)

        data = []
        for item in root.findall('Item'):
            item_id = item.get('ID')
            name = item.find('Name').text if item.find('Name') is not None else None
            eng_name = item.find('EngName').text if item.find('EngName') is not None else None
            nominal = item.find('Nominal').text if item.find('Nominal') is not None else None
            iso_char = item.find('ISO_Char_Code').text if item.find('ISO_Char_Code') is not None else None

            data.append({
                'id': item_id,
                'name': name,
                'eng_name': eng_name,
                'nominal': int(nominal) if nominal else None,
                'iso_char_code': iso_char
            })

        df = pd.DataFrame(data)

        # Фильтруем только USD и EUR
        df_filtered = df[df['id'].isin([USD_CODE, EUR_CODE])].copy()
        print(f"Справочник загружен и отфильтрован: {len(df_filtered)} записей (USD, EUR)")
        return df_filtered

    except Exception as e:
        print(f"Ошибка при загрузке справочника: {e}")
        return pd.DataFrame()

# --- Функция: сохранение в БД ---
def save_to_db(df, table_name, schema='public', if_exists='append'):
    """
    Сохраняет DataFrame в PostgreSQL
    """
    try:
        df.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method='multi'
        )
        print(f"Данные сохранены в таблицу {schema}.{table_name}: {len(df)} строк")
    except Exception as e:
        print(f"Ошибка при сохранении в БД ({schema}.{table_name}): {e}")

# --- Основная логика ---
def main():
    print("Запуск ETL-процесса...")

    # --- 1. Загрузка курсов USD и EUR ---
    print("Загрузка курсов USD...")
    df_usd = fetch_exchange_rates(USD_CODE, days_back=30)

    print("Загрузка курсов EUR...")
    df_eur = fetch_exchange_rates(EUR_CODE, days_back=30)

    # Объединяем курсы
    df_rates = pd.concat([df_usd, df_eur], ignore_index=True)

    # Сохраняем курсы
    if not df_rates.empty:
        save_to_db(df_rates, 'exchange_rates', schema='public', if_exists='replace')  # if_exists='append' - для добавления
    else:
        print("Нет данных для сохранения курсов.")

    # --- 2. Загрузка справочника (1-го числа каждого месяца или инициализация) ---
    today = datetime.now().date()
    should_init_load = os.getenv("INIT_LOAD") is not None and os.getenv("INIT_LOAD").strip() == "1"
    if today.day == 1 or should_init_load:  # Только 1-го числа или иницициализация
        print("Загружаем справочник валют (по расписанию или инициализация).")
        df_ref = fetch_currency_reference()
        if not df_ref.empty:
            save_to_db(df_ref, 'currency_reference', schema='public', if_exists='replace')
        else:
            print("Не удалось загрузить справочник валют.")
    else:
        print("Справочник не обновляется — не первое число и INIT_LOAD != '1'.")

    print("ETL-процесс завершён.")

# --- Запуск ---
if __name__ == "__main__":
    main()
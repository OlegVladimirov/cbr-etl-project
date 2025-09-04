# streamlit_app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import os

# --- Конфигурация БД ---
# Streamlit Cloud прочитает это из st.secrets
DB_URL = st.secrets["DATABASE_URL"]

# --- Подключение к БД ---
@st.cache_data(ttl=3600)  # Кэшируем на 1 час
def load_data():
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            rates = pd.read_sql(""" SELECT r.date, c.iso_char_code, r.rate 
                                    FROM exchange_rates r
                                       join currency_reference c
                                          on c.id = r.currency_code
                                    ORDER BY date
                                """, conn)
            ref = pd.read_sql("SELECT t.id, t.name, t.eng_name, t.nominal, t.iso_char_code FROM currency_reference t", conn)
        return rates, ref
    except Exception as e:
        st.error(f"Ошибка подключения к базе данных: {e}")
        return pd.DataFrame(), pd.DataFrame()

# --- Загрузка данных ---
st.title("💱 Курсы валют ЦБ РФ")
st.markdown("Данные обновляются ежедневно через ETL-пайплайн.")

rates, ref = load_data()

if rates.empty:
    st.warning("Нет данных для отображения.")
else:
    # --- Фильтр по дате ---
    st.sidebar.header("Фильтры")
    min_date = rates['date'].min()
    max_date = rates['date'].max()
    start_date, end_date = st.sidebar.date_input(
        "Период",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    filtered_rates = rates[(rates['date'] >= start_date) & (rates['date'] <= end_date)]

    # --- График курсов ---
    st.subheader("Курсы USD и EUR")
    fig = px.line(
        filtered_rates,
        x='date',
        y='rate',
        color='iso_char_code',
        markers=True,
        labels={'rate': 'Курс (₽)', 'date': 'Дата', 'iso_char_code': 'Валюта'}
    )
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    # --- Таблица последних значений ---
    st.subheader("Последние курсы")
    latest = filtered_rates.sort_values('date').groupby('iso_char_code').tail(1)
    st.dataframe(latest[['iso_char_code', 'rate', 'date']], use_container_width=True)

    # --- Справочник валют ---
    st.subheader("Справочник валют")
    if not ref.empty:
        st.dataframe(ref[['name', 'iso_char_code', 'nominal']], use_container_width=True)
    else:
        st.text("Справочник пуст.")



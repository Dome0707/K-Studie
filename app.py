import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import numpy as np
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials


# streamlit run app.py

# --- 1. Google Sheets Verbindung & Konfiguration ---

# Caching für die Haupt-Datenladefunktion
@st.cache_data(ttl=600)
def get_all_kebaps_as_df():
    """
    Holt alle Daten aus dem Google Sheet und gibt sie als bereinigtes Pandas DataFrame zurück.
    Das Ergebnis wird gecached.
    """
    print("GOOGLE SHEET WIRD GELESEN...")

    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    client = gspread.authorize(creds)

    try:
        spreadsheet = client.open(st.secrets["gcp_sheet_name"])
        sheet = spreadsheet.worksheet("Tabellenblatt1")

    except gspread.exceptions.WorksheetNotFound:
        st.error(f"FEHLER: Konnte Tab 'Tabellenblatt1' im Google Sheet '{st.secrets['gcp_sheet_name']}' nicht finden.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Fehler beim Öffnen des Google Sheets '{st.secrets['gcp_sheet_name']}': {e}")
        return pd.DataFrame()

    df = get_as_dataframe(sheet, header=1, usecols=[0, 1, 2, 3, 4, 5], dtype=str)

    if df.empty or df.columns.empty:
        st.warning("Das Google Sheet-Tab scheint leer zu sein oder hat keine Header-Zeile.")
        return pd.DataFrame()

    expected_cols = ['id', 'datum', 'gewicht_g', 'zubereitet', 'personen', 'uhrzeit']
    actual_cols = [str(col).lower().strip() for col in df.columns]
    df.columns = actual_cols

    if 'id' not in actual_cols:
        st.error(f"SCHWERER FEHLER: 'KeyError: id'")
        st.error(f"Gefundene Spalten: {actual_cols}")
        return pd.DataFrame()

    df = df.dropna(subset=['id'])
    df = df[df['id'] != '']

    try:
        df['id'] = pd.to_numeric(df['id'])
        df['gewicht_g'] = pd.to_numeric(df['gewicht_g'])
        df['personen'] = pd.to_numeric(df['personen'])
    except Exception as e:
        st.error(f"FEHLER bei der Daten-Konvertierung (z.B. Text in 'gewicht_g'-Spalte?): {e}")
        return pd.DataFrame()

    # --- FORMAT-FIX 1: DATEN LESEN ---
    # Konvertiert das DD.MM-Format (und HH:MM) in ein volles Datum,
    # indem wir das Jahr 2025 (aus dem Studientitel) annehmen.
    try:
        # Kombiniere DD.MM und HH:MM
        datetime_str = df['datum'] + ' ' + df['uhrzeit']
        # Sage pandas, wie das Format aussieht und füge das Jahr hinzu
        df['DateTime'] = pd.to_datetime(datetime_str + '.2025', format='%d.%m %H:%M.%Y')
    except Exception as e:
        print(f"Warnung: Datums-Konvertierung fehlgeschlagen: {e}")
        df['DateTime'] = pd.NaT
        # --- ENDE FORMAT-FIX 1 ---

    df['Wochentag'] = df['DateTime'].dt.strftime('%a')
    df['Stunde'] = df['DateTime'].dt.hour + df['DateTime'].dt.minute / 60.0
    df['Zubereitet_Clean'] = df['zubereitet'].str.replace(' ', '').str.upper()
    df['Zubereitet_Clean'] = df['Zubereitet_Clean'].replace({'OG': 'OG1', 'M': 'CHEF'})

    weekday_german = {'Mon': 'Mo', 'Tue': 'Di', 'Wed': 'Mi', 'Thu': 'Do', 'Fri': 'Fr', 'Sat': 'Sa', 'Sun': 'So'}
    df['Wochentag_DE'] = df['Wochentag'].map(weekday_german)

    print("Google Sheet erfolgreich gelesen und aufbereitet.")
    return df


def _connect_to_gsheet():
    """Stellt die Verbindung her und gibt das Sheet-Objekt zurück."""
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    client = gspread.authorize(creds)
    sheet = client.open(st.secrets["gcp_sheet_name"]).worksheet("Tabellenblatt1")
    return sheet


def add_kebap(datum_obj, gewicht, zubereitet, personen, uhrzeit_str_with_seconds):
    """
    Fügt einen neuen Datenpunkt zum Google Sheet hinzu.
    Wandelt Datum/Zeit in das Format DD.MM und HH:MM um.
    """
    sheet = _connect_to_gsheet()

    all_ids = sheet.col_values(1)[1:]
    all_ids = [int(i) for i in all_ids if str(i).isdigit()]
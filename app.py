import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import numpy as np
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
import io


# streamlit run app.py

# --- 1. Google Sheets Verbindung & Konfiguration ---

@st.cache_resource(ttl=3600)
def connect_to_gspread():
    """Stellt EINMALIG die Verbindung zu Google her."""
    print("STELLE NEUE VERBINDUNG ZU GOOGLE HER...")
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"FEHLER bei Authentifizierung: {e}")
        return None


@st.cache_data(ttl=600)
def get_all_kebaps_as_df(_client):
    """Liest Daten direkt per ID."""
    print("GOOGLE SHEET WIRD GELESEN...")

    if _client is None:
        return pd.DataFrame()

    try:
        # --- ÄNDERUNG: Öffne direkt per ID (vermeidet Drive-Suche) ---
        spreadsheet = _client.open_by_key(st.secrets["gcp_sheet_id"])
        worksheet_name = "Tabellenblatt1"
        sheet = spreadsheet.worksheet(worksheet_name)
    except Exception as e:
        st.error(f"Fehler beim Öffnen der Tabelle (ID geprüft?): {e}")
        return pd.DataFrame()

    df = get_as_dataframe(sheet, header=1, usecols=[0, 1, 2, 3, 4, 5], dtype=str)

    if df.empty or df.columns.empty:
        st.warning("Das Google Sheet-Tab scheint leer zu sein.")
        return pd.DataFrame()

    # Header Bereinigung
    df.columns = [str(col).lower().strip() for col in df.columns]

    if 'id' not in df.columns:
        st.error(f"SCHWERER FEHLER: Spalte 'id' fehlt. Gefunden: {df.columns.tolist()}")
        return pd.DataFrame()

    df = df.dropna(subset=['id'])
    df = df[df['id'] != '']

    if df.empty:
        return pd.DataFrame()

    # Typkonvertierung
    try:
        df['id'] = pd.to_numeric(df['id'])
        df['gewicht_g'] = pd.to_numeric(df['gewicht_g'])
        df['personen'] = pd.to_numeric(df['personen'])
    except Exception:
        pass  # Fehler ignorieren, falls einzelne Zellen kaputt sind

    # Datumsverarbeitung (TT.MM.JJJJ)
    try:
        df['DateTime'] = pd.to_datetime(df['datum'] + ' ' + df['uhrzeit'], dayfirst=True)
    except Exception:
        df['DateTime'] = pd.NaT

    df['Wochentag'] = df['DateTime'].dt.strftime('%a')
    df['Stunde'] = df['DateTime'].dt.hour + df['DateTime'].dt.minute / 60.0
    df['Zubereitet_Clean'] = df['zubereitet'].str.replace(' ', '').str.upper().replace({'OG': 'OG1', 'M': 'CHEF'})

    weekday_german = {'Mon': 'Mo', 'Tue': 'Di', 'Wed': 'Mi', 'Thu': 'Do', 'Fri': 'Fr', 'Sat': 'Sa', 'Sun': 'So'}
    df['Wochentag_DE'] = df['Wochentag'].map(weekday_german)

    return df


# --- SCHREIB-FUNKTIONEN (Auch auf open_by_key umgestellt) ---

def add_kebap(client, datum, gewicht, zubereitet, personen, uhrzeit):
    sheet = client.open_by_key(st.secrets["gcp_sheet_id"]).worksheet("Tabellenblatt1")

    all_ids = sheet.col_values(1)[1:]
    all_ids = [int(i) for i in all_ids if str(i).isdigit()]
    next_id = max(all_ids) + 1 if all_ids else 1

    # Format: TT.MM.JJJJ
    datum_str = datum.strftime('%d.%m.%Y')

    new_row = [next_id, datum_str, int(gewicht), str(zubereitet), int(personen), str(uhrzeit)]
    sheet.append_row(new_row)
    st.cache_data.clear()


def get_kebap_row_by_id(client, id):
    sheet = client.open_by_key(st.secrets["gcp_sheet_id"]).worksheet("Tabellenblatt1")
    try:
        cell = sheet.find(str(id), in_column=1)
        return cell.row
    except gspread.exceptions.CellNotFound:
        return None


def update_kebap(client, id, datum, gewicht, zubereitet, personen, uhrzeit):
    row_index = get_kebap_row_by_id(client, id)
    if row_index is None:
        st.error("Eintrag nicht gefunden.")
        return

    sheet = client.open_by_key(st.secrets["gcp_sheet_id"]).worksheet("Tabellenblatt1")

    datum_str = datum.strftime('%d.%m.%Y')

    sheet.update_cell(row_index, 2, datum_str)
    sheet.update_cell(row_index, 3, int(gewicht))
    sheet.update_cell(row_index, 4, str(zubereitet))
    sheet.update_cell(row_index, 5, int(personen))
    sheet.update_cell(row_index, 6, str(uhrzeit))
    st.cache_data.clear()


def delete_kebap(client, id):
    row_index = get_kebap_row_by_id(client, id)
    if row_index is None:
        st.error("Eintrag nicht gefunden.")
        return

    sheet = client.open_by_key(st.secrets["gcp_sheet_id"]).worksheet("Tabellenblatt1")
    sheet.delete_rows(row_index)
    st.cache_data.clear()


# --- 2. Plotting-Funktionen (Unverändert) ---

def plot_weight_distribution(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.histplot(df['gewicht_g'], kde=True, bins=15, ax=ax)
    ax.set_title('Verteilung des Kebapgewichts')
    ax.set_xlabel('Gewicht [g]')
    ax.set_ylabel('Anzahl')
    if not df['gewicht_g'].empty:
        ax.axvline(df['gewicht_g'].mean(), color='red', linestyle='--', label=f"Ø: {df['gewicht_g'].mean():.0f}g")
    ax.legend()
    return fig


def plot_weight_by_preparer(df):
    fig, ax = plt.subplots(figsize=(12, 7))
    order = df['Zubereitet_Clean'].value_counts().index
    sns.boxplot(x='Zubereitet_Clean', y='gewicht_g', data=df, order=order, ax=ax)
    sns.stripplot(x='Zubereitet_Clean', y='gewicht_g', data=df, order=order, color='0.25', alpha=0.7, ax=ax)
    ax.set_title('Kebapgewicht nach Zubereiter')
    return fig


def plot_weight_by_weekday(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    df_cleaned = df.dropna(subset=['Wochentag_DE'])
    weekday_order = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
    sns.boxplot(x='Wochentag_DE', y='gewicht_g', data=df_cleaned, order=weekday_order, ax=ax)
    ax.set_title('Kebapgewicht nach Wochentag')
    return fig


def plot_weight_vs_people(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.regplot(x='personen', y='gewicht_g', data=df, ci=None, scatter_kws={'alpha': 0.7}, ax=ax)
    ax.set_title('Korrelation: Gewicht vs. Personen')
    return fig


def plot_weight_over_time(df):
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.lineplot(x='DateTime', y='gewicht_g', data=df, marker='o', hue='Zubereitet_Clean', ax=ax)
    ax.set_title('Entwicklung über Zeit')
    plt.xticks(rotation=45)
    return fig


def plot_weight_over_time_of_day(df):
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.regplot(x='Stunde', y='gewicht_g', data=df, ci=95, ax=ax)
    ax.set_title('Gewicht vs. Uhrzeit')
    return fig


# --- 3. Haupt-App ---

def main_app():
    st.set_page_config(page_title="Kebapstudie Dashboard", layout="wide")
    st.title("🥙 Kebapstudie 2025 Dashboard")
    sns.set_theme(style="whitegrid")

    client = connect_to_gspread()
    if client is None:
        st.stop()

    df = get_all_kebaps_as_df(client)

    # Zubereiter Liste
    known_preparers = ["OG1", "OG2", "CHEF", "IDIOT", "ANDERE"]
    if not df.empty:
        db_preparers = list(df['zubereitet'].str.upper().unique())
        for p in db_preparers:
            if p not in known_preparers:
                known_preparers.append(p)

    # --- Sidebar ---
    st.sidebar.title("Neuer Eintrag")
    with st.sidebar.form("add_form", clear_on_submit=True):
        datum = st.date_input("Datum", datetime.date.today())
        uhrzeit = st.time_input("Uhrzeit", datetime.datetime.now().time())
        gewicht = st.number_input("Gewicht [g]", 0, 1000, 450)
        personen = st.number_input("Personen", 0, 50, 3)
        zubereitet = st.selectbox("Zubereiter", known_preparers)
        if st.form_submit_button("Speichern"):
            uhr_str = uhrzeit.strftime('%H:%M:%S')
            add_kebap(client, datum, gewicht, zubereitet.upper(), personen, uhr_str)
            st.success("Gespeichert!")
            st.rerun()

    if df.empty:
        st.warning("Keine Daten vorhanden.")
        st.stop()

    # --- Plots ---
    st.header("Analysen")
    plot_options = {
        "Verteilung": plot_weight_distribution,
        "Nach Zubereiter": plot_weight_by_preparer,
        "Nach Wochentag": plot_weight_by_weekday,
        "Gewicht vs Personen": plot_weight_vs_people,
        "Zeitverlauf": plot_weight_over_time,
        "Tageszeit": plot_weight_over_time_of_day
    }
    sel_plot = st.selectbox("Wähle Plot:", list(plot_options.keys()))
    st.pyplot(plot_options[sel_plot](df.copy()))

    # --- Stats ---
    with st.expander("Zahlen & Fakten"):
        st.write(df[['gewicht_g', 'personen']].describe().round(2))

    # --- Edit ---
    with st.expander("Bearbeiten / Löschen"):
        df['id'] = pd.to_numeric(df['id'])
        ids = sorted(df['id'].unique(), reverse=True)
        sel_id = st.selectbox("ID wählen:", ids)

        if st.button("Laden"):
            row = df[df['id'] == sel_id].iloc[0]
            # Zeit parsen robust machen
            try:
                t_val = datetime.time.fromisoformat(str(row['uhrzeit']))
            except:
                t_val = datetime.datetime.strptime(str(row['uhrzeit']), "%H:%M").time()

            # Datum parsen robust machen
            try:
                d_val = datetime.datetime.strptime(str(row['datum']), "%d.%m.%Y").date()
            except:
                d_val = datetime.datetime.strptime(str(row['datum']), "%Y-%m-%d").date()

            st.session_state.edit_data = {
                "id": sel_id,
                "datum": d_val,
                "gewicht": int(row['gewicht_g']),
                "zub": str(row['zubereitet']),
                "pers": int(row['personen']),
                "uhr": t_val
            }

        if "edit_data" in st.session_state:
            with st.form("edit_form"):
                d = st.session_state.edit_data
                new_d = st.date_input("Datum", d['datum'])
                new_t = st.time_input("Uhrzeit", d['uhr'])
                new_g = st.number_input("Gewicht", 0, 1000, d['gewicht'])
                new_p = st.number_input("Personen", 0, 50, d['pers'])

                curr_zub = d['zub'].upper()
                idx = known_preparers.index(curr_zub) if curr_zub in known_preparers else 0
                new_z = st.selectbox("Zubereiter", known_preparers, index=idx)

                c1, c2 = st.columns(2)
                if c1.form_submit_button("Update"):
                    update_kebap(client, d['id'], new_d, new_g, new_z.upper(), new_p, new_t.strftime('%H:%M:%S'))
                    st.success("Aktualisiert!")
                    del st.session_state.edit_data
                    st.rerun()

                if c2.form_submit_button("Löschen"):
                    delete_kebap(client, d['id'])
                    st.warning("Gelöscht!")
                    del st.session_state.edit_data
                    st.rerun()

    st.header("Rohdaten")
    st.dataframe(df.sort_values('id', ascending=False))


if __name__ == "__main__":
    main_app()
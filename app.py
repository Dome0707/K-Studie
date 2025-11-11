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

@st.cache_resource(ttl=3600)  # Cache die Verbindung für 1 Stunde
def connect_to_gspread():
    """
    Stellt EINMALIG die Verbindung zu Google her und gibt den
    autorisierten Client zurück.
    """
    print("STELLE NEUE VERBINDUNG ZU GOOGLE HER...")
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https.www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
        )
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"FEHLER bei Authentifizierung: {e}")
        st.error("Prüfe deine 'gcp_service_account' Secrets.")
        return None


@st.cache_data(ttl=600)  # Cache die DATEN für 10 Minuten
def get_all_kebaps_as_df(_client):
    """
    Holt alle Daten aus dem Google Sheet und gibt sie als bereinigtes Pandas DataFrame zurück.
    Nimmt den gecachten Client als Argument.
    """
    print("GOOGLE SHEET WIRD GELESEN...")

    if _client is None:
        st.error("Verbindung zu Google fehlgeschlagen. App kann nicht laden.")
        return pd.DataFrame()

    try:
        spreadsheet = _client.open(st.secrets["gcp_sheet_name"])
        worksheet_name = "Tabellenblatt1"
        sheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"FEHLER: Konnte Tab '{worksheet_name}' nicht finden.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Fehler beim Öffnen des Google Sheets: {e}")
        return pd.DataFrame()

    df = get_as_dataframe(sheet, header=1, usecols=[0, 1, 2, 3, 4, 5], dtype=str)

    if df.empty or df.columns.empty:
        st.warning("Das Google Sheet-Tab scheint leer zu sein oder hat keine Header-Zeile.")
        return pd.DataFrame()

    expected_cols = ['id', 'datum', 'gewicht_g', 'zubereitet', 'personen', 'uhrzeit']
    actual_cols = [str(col).lower().strip() for col in df.columns]
    df.columns = actual_cols

    if 'id' not in actual_cols:
        st.error(f"SCHWERER FEHLER: 'KeyError: id'. GEFUNDEN: {actual_cols}")
        st.error("Bitte korrigiere Zeile 1 in deinem LIVE Google Sheet.")
        return pd.DataFrame()

    df = df.dropna(subset=['id'])
    df = df[df['id'] != '']

    if df.empty:
        return pd.DataFrame()

    try:
        df['id'] = pd.to_numeric(df['id'])
        df['gewicht_g'] = pd.to_numeric(df['gewicht_g'])
        df['personen'] = pd.to_numeric(df['personen'])
    except Exception as e:
        st.error(f"FEHLER bei der Daten-Konvertierung: {e}")
        return pd.DataFrame()

    try:
        # Flexible Lese-Funktion
        df['DateTime'] = pd.to_datetime(df['datum'] + ' ' + df['uhrzeit'], dayfirst=True)
    except Exception:
        df['DateTime'] = pd.NaT

    df['Wochentag'] = df['DateTime'].dt.strftime('%a')
    df['Stunde'] = df['DateTime'].dt.hour + df['DateTime'].dt.minute / 60.0
    df['Zubereitet_Clean'] = df['zubereitet'].str.replace(' ', '').str.upper()
    df['Zubereitet_Clean'] = df['Zubereitet_Clean'].replace({'OG': 'OG1', 'M': 'CHEF'})

    weekday_german = {'Mon': 'Mo', 'Tue': 'Di', 'Wed': 'Mi', 'Thu': 'Do', 'Fri': 'Fr', 'Sat': 'Sa', 'Sun': 'So'}
    df['Wochentag_DE'] = df['Wochentag'].map(weekday_german)

    print("Google Sheet erfolgreich gelesen und aufbereitet.")
    return df


# --- SCHREIB-FUNKTIONEN (NEHMEN JETZT 'client' ALS ARGUMENT) ---

def add_kebap(client, datum, gewicht, zubereitet, personen, uhrzeit):
    """Fügt einen neuen Datenpunkt hinzu (verwendet bestehenden Client)."""
    sheet = client.open(st.secrets["gcp_sheet_name"]).worksheet("Tabellenblatt1")

    all_ids = sheet.col_values(1)[1:]
    all_ids = [int(i) for i in all_ids if str(i).isdigit()]
    next_id = max(all_ids) + 1 if all_ids else 1

    # Speichert im ISO-Format (YYYY-MM-DD und HH:MM:SS)
    new_row = [next_id, str(datum), int(gewicht), str(zubereitet), int(personen), str(uhrzeit)]
    sheet.append_row(new_row)

    st.cache_data.clear()  # Lösche den DATEN-Cache


def get_kebap_row_by_id(client, id):
    """Findet die Zeilennummer (verwendet bestehenden Client)."""
    sheet = client.open(st.secrets["gcp_sheet_name"]).worksheet("Tabellenblatt1")
    try:
        cell = sheet.find(str(id), in_column=1)
        return cell.row
    except gspread.exceptions.CellNotFound:
        return None


def update_kebap(client, id, datum, gewicht, zubereitet, personen, uhrzeit):
    """Aktualisiert einen Eintrag (verwendet bestehenden Client)."""
    row_index = get_kebap_row_by_id(client, id)
    if row_index is None:
        st.error(f"Konnte Eintrag mit ID {id} zum Aktualisieren nicht finden.")
        return

    sheet = client.open(st.secrets["gcp_sheet_name"]).worksheet("Tabellenblatt1")
    sheet.update_cell(row_index, 1, int(id))
    sheet.update_cell(row_index, 2, str(datum))
    sheet.update_cell(row_index, 3, int(gewicht))
    sheet.update_cell(row_index, 4, str(zubereitet))
    sheet.update_cell(row_index, 5, int(personen))
    sheet.update_cell(row_index, 6, str(uhrzeit))

    st.cache_data.clear()  # Lösche den DATEN-Cache


def delete_kebap(client, id):
    """Löscht einen Eintrag (verwendet bestehenden Client)."""
    row_index = get_kebap_row_by_id(client, id)
    if row_index is None:
        st.error(f"Konnte Eintrag mit ID {id} zum Löschen nicht finden.")
        return

    sheet = client.open(st.secrets["gcp_sheet_name"]).worksheet("Tabellenblatt1")
    sheet.delete_rows(row_index)

    st.cache_data.clear()  # Lösche den DATEN-Cache


# --- 2. Plotting-Funktionen (Unverändert) ---

def plot_weight_distribution(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.histplot(df['gewicht_g'], kde=True, bins=15, ax=ax)
    ax.set_title('Verteilung des Kebapgewichts')
    ax.set_xlabel('Gewicht [g]')
    ax.set_ylabel('Anzahl')
    ax.axvline(df['gewicht_g'].mean(), color='red', linestyle='--',
               label=f"Durchschnitt: {df['gewicht_g'].mean():.0f}g")
    ax.legend()
    plt.tight_layout()
    return fig


def plot_weight_by_preparer(df):
    fig, ax = plt.subplots(figsize=(12, 7))
    order = df['Zubereitet_Clean'].value_counts().index
    sns.boxplot(x='Zubereitet_Clean', y='gewicht_g', data=df, order=order, ax=ax)
    sns.stripplot(x='Zubereitet_Clean', y='gewicht_g', data=df, order=order, color='0.25', alpha=0.7, ax=ax)
    ax.set_title('Kebapgewicht nach Zubereiter (Boxplot)')
    ax.set_xlabel('Zubereiter')
    ax.set_ylabel('Gewicht [g]')
    plt.tight_layout()
    return fig


def plot_weight_by_weekday(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    df_cleaned = df.dropna(subset=['Wochentag_DE'])
    weekday_order_de = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
    sns.boxplot(x='Wochentag_DE', y='gewicht_g', data=df_cleaned, order=weekday_order_de, ax=ax)
    ax.set_title('Kebapgewicht nach Wochentag')
    ax.set_xlabel('Wochentag')
    ax.set_ylabel('Gewicht [g]')
    plt.tight_layout()
    return fig


def plot_weight_vs_people(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.regplot(x='personen', y='gewicht_g', data=df, ci=None, scatter_kws={'alpha': 0.7}, ax=ax)
    ax.set_title('Korrelation: Kebapgewicht vs. Personen (Wartezeit)')
    ax.set_xlabel('Anzahl Personen (Indikator für Wartezeit)')
    ax.set_ylabel('Gewicht [g]')
    plt.tight_layout()
    return fig


def plot_weight_over_time(df):
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.lineplot(x='DateTime', y='gewicht_g', data=df, marker='o', hue='Zubereitet_Clean', ax=ax)
    ax.set_title('Entwicklung des Kebapgewichts über die Zeit')
    ax.set_xlabel('Datum und Uhrzeit')
    ax.set_ylabel('Gewicht [g]')
    plt.xticks(rotation=45)
    ax.legend(title='Zubereiter')
    plt.tight_layout()
    return fig


def plot_weight_over_time_of_day(df):
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.regplot(x='Stunde', y='gewicht_g', data=df, ci=95, scatter_kws={'alpha': 0.7}, ax=ax)
    ax.set_title('Kebapgewicht in Abhängigkeit von der Uhrzeit')
    ax.set_xlabel('Uhrzeit (Stunde des Tages)')
    ax.set_ylabel('Gewicht [g]')

    valid_stunde = df['Stunde'].dropna()
    valid_stunde = valid_stunde[np.isfinite(valid_stunde)]
    if not valid_stunde.empty:
        min_hour = int(np.floor(valid_stunde.min()))
        max_hour = int(np.ceil(valid_stunde.max()))
        ax.set_xticks(ticks=range(min_hour, max_hour + 2))
        ax.set_xlim(min_hour - 1, max_hour + 1)

    plt.tight_layout()
    return fig


# --- 3. Die Streamlit-App (Angepasst, um den 'client' zu übergeben) ---

def main_app():
    st.set_page_config(page_title="Kebapstudie Dashboard", layout="wide")
    st.title("🥙 Kebapstudie 2025 Dashboard")

    sns.set_theme(style="whitegrid")

    # --- ÄNDERUNG: Stelle EINMALIG die Verbindung her ---
    client = connect_to_gspread()

    if client is None:
        # App anhalten, wenn die Authentifizierung fehlschlägt
        st.stop()

        # Lade Daten (gecached) und übergebe den client
    df = get_all_kebaps_as_df(client)

    if df.empty:
        known_preparers = ["OG1", "OG2", "CHEF", "IDIOT", "ANDERE"]
    else:
        known_preparers = list(df['zubereitet'].str.upper().unique())
        if "ANDERE" not in known_preparers:
            known_preparers.append("ANDERE")
        known_preparers.sort()

    # Seitenleiste
    st.sidebar.title("Neuen Datenpunkt hinzufügen")
    with st.sidebar.form("add_form", clear_on_submit=True):
        datum = st.date_input("Datum", datetime.date.today())
        uhrzeit = st.time_input("Uhrzeit", datetime.datetime.now().time())
        gewicht = st.number_input("Gewicht [g]", min_value=0, max_value=1000, value=450)
        personen = st.number_input("Personen (Wartezeit)", min_value=0, max_value=50, value=3)
        zubereitet = st.selectbox("Zubereitet von", known_preparers)
        add_submitted = st.form_submit_button("Speichern")

    if add_submitted:
        uhrzeit_str = uhrzeit.strftime('%H:%M:%S')
        # --- ÄNDERUNG: Übergebe den 'client' ---
        add_kebap(client, datum, gewicht, zubereitet.upper(), personen, uhrzeit_str)
        st.sidebar.success(f"Datenpunkt ({gewicht}g, {zubereitet}) gespeichert!")
        st.rerun()

    if df.empty:
        st.warning("Noch keine Daten in der Datenbank. Bitte links Daten eingeben.")
        st.stop()

    # Plot-Auswahl
    st.header("Statistische Auswertungen")
    plot_options = {
        "Gewichtsverteilung (Histogramm)": plot_weight_distribution,
        "Gewicht nach Zubereiter (Boxplot)": plot_weight_by_preparer,
        "Gewicht nach Wochentag (Boxplot)": plot_weight_by_weekday,
        "Gewicht vs. Personen (Korrelation)": plot_weight_vs_people,
        "Gewicht im Zeitverlauf (Linie)": plot_weight_over_time,
        "Gewicht vs. Uhrzeit (Korrelation)": plot_weight_over_time_of_day
    }
    selected_plot_name = st.selectbox("Wähle eine Abbildung aus:", list(plot_options.keys()))
    plot_function = plot_options[selected_plot_name]
    fig_to_show = plot_function(df.copy())
    st.pyplot(fig_to_show)

    # Numerische Statistiken
    with st.expander("📊 Numerische Zusammenfassung anzeigen"):
        st.subheader("Deskriptive Statistik (Gewicht & Personen)")
        st.dataframe(df[['gewicht_g', 'personen']].describe().round(2))

        st.subheader("Statistik pro Zubereiter")
        avg_weight_preparer = df.groupby('Zubereitet_Clean')['gewicht_g'].agg(['mean', 'count', 'std']).sort_values(
            'mean', ascending=False)
        st.dataframe(avg_weight_preparer.round(2))

        st.subheader("Statistik pro Wochentag")
        avg_weight_weekday = df.groupby('Wochentag_DE')['gewicht_g'].agg(['mean', 'count', 'std']).reindex(
            ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So'])
        st.dataframe(avg_weight_weekday.round(2))

    # Bearbeiten & Löschen
    with st.expander("📝 Daten bearbeiten oder löschen"):
        st.subheader("1. Eintrag zum Bearbeiten laden")

        df['id'] = pd.to_numeric(df['id'])

        all_ids = sorted(df['id'].unique(), reverse=True)
        id_to_edit = st.selectbox("Wähle eine ID zum Bearbeiten:", all_ids,
                                  format_func=lambda x: f"ID: {x} (vom {df[df['id'] == x]['datum'].values[0]})")

        if "loaded_data" not in st.session_state:
            st.session_state.loaded_data = None

        if st.button("Ausgewählte ID laden"):
            try:
                python_id_to_load = int(id_to_edit)
                entry_df = df[df['id'] == python_id_to_load].iloc[0]

                st.session_state.loaded_data = {
                    "id": python_id_to_load,
                    "datum": datetime.date.fromisoformat(entry_df['datum']),
                    "gewicht": int(entry_df['gewicht_g']),
                    "zubereitet": str(entry_df['zubereitet']),
                    "personen": int(entry_df['personen']),
                    "uhrzeit": datetime.time.fromisoformat(entry_df['uhrzeit'])
                }
            except Exception as e:
                st.error(f"Fehler beim Laden von ID {id_to_edit}: {e}")
                st.session_state.loaded_data = None

        if st.session_state.loaded_data:
            st.subheader(f"2. Eintrag ID {st.session_state.loaded_data['id']} bearbeiten")

            with st.form("edit_form"):
                data = st.session_state.loaded_data
                edit_datum = st.date_input("Datum", value=data['datum'])
                edit_uhrzeit = st.time_input("Uhrzeit", value=data['uhrzeit'])
                edit_gewicht = st.number_input("Gewicht [g]", min_value=0, max_value=1000, value=data['gewicht'])
                edit_personen = st.number_input("Personen (Wartezeit)", min_value=0, max_value=50,
                                                value=data['personen'])

                current_preparer = data['zubereitet'].upper()
                if current_preparer not in known_preparers:
                    known_preparers.append(current_preparer)
                    known_preparers.sort()

                default_index = known_preparers.index(current_preparer) if current_preparer in known_preparers else 0
                edit_zubereitet = st.selectbox("Zubereitet von", known_preparers, index=default_index)

                col1, col2 = st.columns(2)
                with col1:
                    update_submitted = st.form_submit_button("Änderungen speichern")
                with col2:
                    delete_submitted = st.form_submit_button("❌ DIESEN EINTRAG LÖSCHEN")

            if update_submitted:
                uhrzeit_str = edit_uhrzeit.strftime('%H:%M:%S')
                # --- ÄNDERUNG: Übergebe den 'client' ---
                update_kebap(client, data['id'], edit_datum, edit_gewicht, edit_zubereitet.upper(), edit_personen,
                             uhrzeit_str)
                st.session_state.loaded_data = None
                st.success(f"Eintrag ID {data['id']} erfolgreich aktualisiert!")
                st.rerun()

            if delete_submitted:
                # --- ÄNDERUNG: Übergebe den 'client' ---
                delete_kebap(client, data['id'])
                st.session_state.loaded_data = None
                st.warning(f"Eintrag ID {data['id']} wurde gelöscht!")
                st.rerun()

    # Rohdaten anzeigen
    st.header("Alle erfassten Daten (Rohdaten)")
    st.dataframe(
        df[['id', 'datum', 'gewicht_g', 'zubereitet', 'personen', 'uhrzeit']].sort_values(by="id", ascending=False))


# --- Skript-Start ---
if __name__ == "__main__":
    main_app()
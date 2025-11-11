import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import numpy as np

# streamlit run app.py

# --- 1. Datenbank-Konfiguration & Funktionen ---
DB_FILE = "kebapstudie.db"


def init_db():
    """Erstellt die Datenbank-Tabelle, falls sie noch nicht existiert."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS kebaps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datum DATE,
        gewicht_g INTEGER,
        zubereitet TEXT,
        personen INTEGER,
        uhrzeit TIME 
    )
    """)
    conn.commit()
    conn.close()


def add_kebap(datum, gewicht, zubereitet, personen, uhrzeit):
    """Fügt einen neuen Datenpunkt zur Datenbank hinzu."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO kebaps (datum, gewicht_g, zubereitet, personen, uhrzeit) VALUES (?, ?, ?, ?, ?)",
        (datum, gewicht, zubereitet, personen, uhrzeit)
    )
    conn.commit()
    conn.close()
    # NEU (a): Cache leeren, da sich die Daten geändert haben
    st.cache_data.clear()


def get_kebap_by_id(id):
    """Holt einen einzelnen Kebap-Eintrag anhand seiner ID."""
    conn = sqlite3.connect(DB_FILE)
    entry = conn.execute("SELECT * FROM kebaps WHERE id = ?", (id,)).fetchone()
    conn.close()
    return entry


def update_kebap(id, datum, gewicht, zubereitet, personen, uhrzeit):
    """Aktualisiert einen bestehenden Eintrag in der Datenbank."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    UPDATE kebaps 
    SET datum = ?, gewicht_g = ?, zubereitet = ?, personen = ?, uhrzeit = ?
    WHERE id = ?
    """, (datum, gewicht, zubereitet, personen, uhrzeit, id))
    conn.commit()
    conn.close()
    # NEU (a): Cache leeren, da sich die Daten geändert haben
    st.cache_data.clear()


def delete_kebap(id):
    """Löscht einen Eintrag aus der Datenbank anhand seiner ID."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM kebaps WHERE id = ?", (id,))
    conn.commit()
    conn.close()
    # NEU (a): Cache leeren, da sich die Daten geändert haben
    st.cache_data.clear()


# NEU (a): Caching für die Haupt-Datenladefunktion
@st.cache_data
def get_all_kebaps_as_df():
    """
    Holt alle Daten aus der DB und gibt sie als bereinigtes Pandas DataFrame zurück.
    Das Ergebnis wird gecached.
    """
    print("DATENBANK WIRD GELESEN...")  # (Für Debugging, siehst du im Terminal)
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM kebaps", conn)
    conn.close()

    if df.empty:
        return df

    # --- Datenaufbereitung (Cleaning) ---
    df['DateTime'] = pd.to_datetime(df['datum'] + ' ' + df['uhrzeit'])
    df['Wochentag'] = df['DateTime'].dt.strftime('%a')
    df['Stunde'] = df['DateTime'].dt.hour + df['DateTime'].dt.minute / 60.0
    df['Zubereitet_Clean'] = df['zubereitet'].str.replace(' ', '').str.upper()
    df['Zubereitet_Clean'] = df['Zubereitet_Clean'].replace({'OG': 'OG1', 'M': 'CHEF'})

    # NEU (b): Wochentag_DE für Statistik-Expander erstellen
    weekday_german = {'Mon': 'Mo', 'Tue': 'Di', 'Wed': 'Mi', 'Thu': 'Do', 'Fri': 'Fr', 'Sat': 'Sa', 'Sun': 'So'}
    df['Wochentag_DE'] = df['Wochentag'].map(weekday_german)

    return df


# --- 2. Plotting-Funktionen (bleiben unverändert) ---

def plot_weight_distribution(df):
    """Erstellt ein Histogramm der Kebapgewichte."""
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
    """Erstellt ein Boxplot des Gewichts pro Zubereiter."""
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
    """Erstellt ein Boxplot des Gewichts pro Wochentag."""
    fig, ax = plt.subplots(figsize=(10, 6))
    # 'Wochentag_DE' wurde bereits in get_all_kebaps_as_df erstellt
    weekday_order_de = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']

    sns.boxplot(x='Wochentag_DE', y='gewicht_g', data=df, order=weekday_order_de, ax=ax)
    ax.set_title('Kebapgewicht nach Wochentag')
    ax.set_xlabel('Wochentag')
    ax.set_ylabel('Gewicht [g]')
    plt.tight_layout()
    return fig


def plot_weight_vs_people(df):
    """Erstellt ein Streudiagramm (Scatter Plot) von Gewicht vs. Personen."""
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.regplot(x='personen', y='gewicht_g', data=df, ci=None, scatter_kws={'alpha': 0.7}, ax=ax)
    ax.set_title('Korrelation: Kebapgewicht vs. Personen (Wartezeit)')
    ax.set_xlabel('Anzahl Personen (Indikator für Wartezeit)')
    ax.set_ylabel('Gewicht [g]')
    plt.tight_layout()
    return fig


def plot_weight_over_time(df):
    """Erstellt ein Liniendiagramm des Gewichts über die Zeit."""
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
    """Erstellt ein Streudiagramm (Scatter Plot) von Gewicht vs. Uhrzeit."""
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.regplot(x='Stunde', y='gewicht_g', data=df, ci=95, scatter_kws={'alpha': 0.7}, ax=ax)
    ax.set_title('Kebapgewicht in Abhängigkeit von der Uhrzeit')
    ax.set_xlabel('Uhrzeit (Stunde des Tages)')
    ax.set_ylabel('Gewicht [g]')
    min_hour = int(np.floor(df['Stunde'].min()))
    max_hour = int(np.ceil(df['Stunde'].max()))
    ax.set_xticks(ticks=range(min_hour, max_hour + 2))
    ax.set_xlim(min_hour - 1, max_hour + 1)
    plt.tight_layout()
    return fig


# --- 3. Die Streamlit-App (mit allen Features) ---

def main_app():
    st.set_page_config(page_title="Kebartudie Dashboard", layout="wide")
    st.title("🥙 Kebartudie 2025 Dashboard")

    # Setze den Plot-Stil für alle Plots
    sns.set_theme(style="whitegrid")

    # --- SCHRITT 1: DATEN GANZ AM ANFANG LADEN (wird gecached) ---
    df = get_all_kebaps_as_df()

    # --- SCHRITT 2 (d): DYNAMISCHE LISTE ERSTELLEN ---
    if df.empty:
        known_preparers = ["OG1", "OG2", "CHEF", "IDIOT", "ANDERE"]
    else:
        known_preparers = list(df['zubereitet'].str.upper().unique())
        if "ANDERE" not in known_preparers:
            known_preparers.append("ANDERE")
        known_preparers.sort()

    # --- Seitenleiste für Dateneingabe ---
    st.sidebar.title("Neuen Datenpunkt hinzufügen")
    with st.sidebar.form("add_form", clear_on_submit=True):
        datum = st.date_input("Datum", datetime.date.today())
        uhrzeit = st.time_input("Uhrzeit", datetime.datetime.now().time())
        gewicht = st.number_input("Gewicht [g]", min_value=0, max_value=1000, value=450)
        personen = st.number_input("Personen (Wartezeit)", min_value=0, max_value=50, value=3)

        # Verwende die dynamische Liste
        zubereitet = st.selectbox("Zubereitet von", known_preparers)

        add_submitted = st.form_submit_button("Speichern")

    if add_submitted:
        uhrzeit_str = uhrzeit.strftime('%H:%M:%S')
        add_kebap(datum, gewicht, zubereitet.upper(), personen, uhrzeit_str)
        st.sidebar.success(f"Datenpunkt ({gewicht}g, {zubereitet}) gespeichert!")
        st.rerun()

        # --- Hauptseite für Plots & Daten ---

    if df.empty:
        st.warning("Noch keine Daten in der Datenbank. Bitte links Daten eingeben.")
        st.stop()

        # 2. Plot-Auswahl
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
    fig_to_show = plot_function(df.copy())  # .copy() verhindert Caching-Warnungen
    st.pyplot(fig_to_show)

    # --- NEU (b): Numerische Statistiken anzeigen ---
    with st.expander("📊 Numerische Zusammenfassung anzeigen"):

        st.subheader("Deskriptive Statistik (Gewicht & Personen)")
        st.dataframe(df[['gewicht_g', 'personen']].describe().round(2))

        st.subheader("Statistik pro Zubereiter")
        avg_weight_preparer = df.groupby('Zubereitet_Clean')['gewicht_g'].agg(['mean', 'count', 'std']).sort_values(
            'mean', ascending=False)
        st.dataframe(avg_weight_preparer.round(2))

        st.subheader("Statistik pro Wochentag")
        # 'Wochentag_DE' wurde in get_all_kebaps_as_df erstellt
        avg_weight_weekday = df.groupby('Wochentag_DE')['gewicht_g'].agg(['mean', 'count', 'std']).reindex(
            ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So'])
        st.dataframe(avg_weight_weekday.round(2))
    # --- ENDE NEU (b) ---

    # 3. Bearbeiten & Löschen
    with st.expander("📝 Daten bearbeiten oder löschen"):

        st.subheader("1. Eintrag zum Bearbeiten laden")
        all_ids = sorted(df['id'].unique(), reverse=True)
        # format_func hilft, einen lesbaren Namen im Dropdown anzuzeigen
        id_to_edit = st.selectbox("Wähle eine ID zum Bearbeiten:", all_ids,
                                  format_func=lambda x: f"ID: {x} (vom {df[df['id'] == x]['datum'].values[0]})")

        if "loaded_data" not in st.session_state:
            st.session_state.loaded_data = None

        if st.button("Ausgewählte ID laden"):
            try:
                python_id_to_load = int(id_to_edit)
            except ValueError:
                st.error("Ungültige ID ausgewählt.")
                st.session_state.loaded_data = None
                st.stop()

            entry = get_kebap_by_id(python_id_to_load)

            if entry is None:
                st.error(
                    f"Fehler: Eintrag mit ID {python_id_to_load} konnte nicht gefunden werden. (Wurde er vielleicht gelöscht?)")
                st.session_state.loaded_data = None
            else:
                st.session_state.loaded_data = {
                    "id": entry[0],
                    "datum": datetime.date.fromisoformat(entry[1]),
                    "gewicht": entry[2],
                    "zubereitet": entry[3],
                    "personen": entry[4],
                    "uhrzeit": datetime.time.fromisoformat(entry[5])
                }

        # Wenn Daten geladen wurden, zeige das Bearbeitungsformular an
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
                edit_zubereitet = st.selectbox("Zubereitet von", known_preparers,
                                               index=default_index)

                col1, col2 = st.columns(2)
                with col1:
                    update_submitted = st.form_submit_button("Änderungen speichern")
                with col2:
                    delete_submitted = st.form_submit_button("❌ DIESEN EINTRAG LÖSCHEN")

            if update_submitted:
                uhrzeit_str = edit_uhrzeit.strftime('%H:%M:%S')
                update_kebap(data['id'], edit_datum, edit_gewicht, edit_zubereitet.upper(), edit_personen, uhrzeit_str)
                st.session_state.loaded_data = None
                st.success(f"Eintrag ID {data['id']} erfolgreich aktualisiert!")
                st.rerun()

            if delete_submitted:
                delete_kebap(data['id'])
                st.session_state.loaded_data = None
                st.warning(f"Eintrag ID {data['id']} wurde gelöscht!")
                st.rerun()

    # 4. Rohdaten anzeigen
    st.header("Alle erfassten Daten (Rohdaten)")
    st.dataframe(df.sort_values(by="id", ascending=False))


# --- Skript-Start ---
if __name__ == "__main__":
    init_db()  # Stellt sicher, dass die DB-Datei und Tabelle existiert
    main_app()  # Startet die Streamlit-App
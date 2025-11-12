import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap  # NEU: Für das Eigelb
import seaborn as sns
import datetime
import numpy as np
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
import scipy.stats as stats
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import matplotlib.image as mpimg


# --- 1. Google Sheets Verbindung & Konfiguration ---

@st.cache_resource(ttl=3600)
def connect_to_gspread():
    """Stellt EINMALIG die Verbindung zu Google her."""
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
    if _client is None: return pd.DataFrame()

    try:
        # Öffnet das Sheet per ID (nicht per Name, um Fehler zu vermeiden)
        spreadsheet = _client.open_by_key(st.secrets["gcp_sheet_id"])
        worksheet_name = "Tabellenblatt1"
        sheet = spreadsheet.worksheet(worksheet_name)
    except Exception as e:
        st.error(f"Fehler beim Öffnen der Tabelle: {e}")
        return pd.DataFrame()

    df = get_as_dataframe(sheet, header=1, usecols=[0, 1, 2, 3, 4, 5], dtype=str)

    if df.empty or df.columns.empty:
        return pd.DataFrame()

    df.columns = [str(col).lower().strip() for col in df.columns]
    if 'id' not in df.columns:
        st.error("Spalte 'id' fehlt!")
        return pd.DataFrame()

    df = df.dropna(subset=['id'])
    df = df[df['id'] != '']

    if df.empty: return pd.DataFrame()

    try:
        df['id'] = pd.to_numeric(df['id'])
        df['gewicht_g'] = pd.to_numeric(df['gewicht_g'])
        df['personen'] = pd.to_numeric(df['personen'])
    except Exception:
        pass

    try:
        df['DateTime'] = pd.to_datetime(df['datum'] + ' ' + df['uhrzeit'], dayfirst=True)
    except Exception:
        df['DateTime'] = pd.NaT

    df['Wochentag'] = df['DateTime'].dt.strftime('%a')
    df['Stunde'] = df['DateTime'].dt.hour + df['DateTime'].dt.minute / 60.0
    df['Stunde_Ganz'] = df['DateTime'].dt.hour

    df['Zubereitet_Clean'] = df['zubereitet'].str.replace(' ', '').str.upper().replace({'OG': 'OG1', 'M': 'CHEF'})

    weekday_german = {'Mon': 'Mo', 'Tue': 'Di', 'Wed': 'Mi', 'Thu': 'Do', 'Fri': 'Fr', 'Sat': 'Sa', 'Sun': 'So'}
    df['Wochentag_DE'] = df['Wochentag'].map(weekday_german)

    return df


# --- SCHREIB-FUNKTIONEN ---

def add_kebap(client, datum, gewicht, zubereitet, personen, uhrzeit):
    sheet = client.open_by_key(st.secrets["gcp_sheet_id"]).worksheet("Tabellenblatt1")
    all_ids = sheet.col_values(1)[1:]
    all_ids = [int(i) for i in all_ids if str(i).isdigit()]
    next_id = max(all_ids) + 1 if all_ids else 1

    new_row = [next_id, datum.strftime('%d.%m.%Y'), int(gewicht), str(zubereitet), int(personen), str(uhrzeit)]
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
    if row_index is None: return

    sheet = client.open_by_key(st.secrets["gcp_sheet_id"]).worksheet("Tabellenblatt1")
    sheet.update_cell(row_index, 2, datum.strftime('%d.%m.%Y'))
    sheet.update_cell(row_index, 3, int(gewicht))
    sheet.update_cell(row_index, 4, str(zubereitet))
    sheet.update_cell(row_index, 5, int(personen))
    sheet.update_cell(row_index, 6, str(uhrzeit))
    st.cache_data.clear()


def delete_kebap(client, id):
    row_index = get_kebap_row_by_id(client, id)
    if row_index is None: return
    sheet = client.open_by_key(st.secrets["gcp_sheet_id"]).worksheet("Tabellenblatt1")
    sheet.delete_rows(row_index)
    st.cache_data.clear()


# --- 2. Plotting-Funktionen ---

def plot_weight_distribution(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.histplot(df['gewicht_g'], kde=True, bins=15, ax=ax)
    ax.set_title('Verteilung des Kebapgewichts')
    ax.set_xlabel('Gewicht [g]')
    if not df['gewicht_g'].empty:
        ax.axvline(df['gewicht_g'].mean(), color='red', linestyle='--', label=f"Ø: {df['gewicht_g'].mean():.0f}g")
    return fig


def plot_weight_by_preparer(df):
    fig, ax = plt.subplots(figsize=(12, 7))
    order = df['Zubereitet_Clean'].value_counts().index
    sns.boxplot(x='Zubereitet_Clean', y='gewicht_g', hue='Zubereitet_Clean', data=df, order=order, dodge=False, ax=ax,
                palette="Set2")
    sns.stripplot(x='Zubereitet_Clean', y='gewicht_g', data=df, order=order, color='0.25', alpha=0.7, ax=ax)
    ax.set_title('Kebapgewicht nach Zubereiter')
    if ax.get_legend(): ax.get_legend().remove()
    return fig


def plot_weight_by_weekday(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    df_cleaned = df.dropna(subset=['Wochentag_DE'])
    weekday_order = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
    sns.boxplot(x='Wochentag_DE', y='gewicht_g', hue='Wochentag_DE', data=df_cleaned, order=weekday_order, dodge=False,
                ax=ax, palette="pastel")
    ax.set_title('Kebapgewicht nach Wochentag')
    if ax.get_legend(): ax.get_legend().remove()
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


def plot_heatmap(df):
    fig, ax = plt.subplots(figsize=(8, 6))
    corr_data = df[['gewicht_g', 'personen', 'Stunde']].dropna()
    corr_data.columns = ['Gewicht', 'Wartezeit', 'Uhrzeit']
    if not corr_data.empty:
        sns.heatmap(corr_data.corr(), annot=True, cmap='coolwarm', fmt=".2f", vmin=-1, vmax=1, ax=ax)
        ax.set_title('Korrelations-Heatmap')
    return fig


def plot_moving_average(df):
    fig, ax = plt.subplots(figsize=(12, 6))
    df_sorted = df.sort_values('DateTime')
    df_sorted['MA_5'] = df_sorted['gewicht_g'].rolling(window=5).mean()
    sns.lineplot(x='DateTime', y='gewicht_g', data=df_sorted, alpha=0.3, label='Einzelwerte', ax=ax)
    sns.lineplot(x='DateTime', y='MA_5', data=df_sorted, linewidth=3, color='red', label='Trend (Ø 5 Kebaps)', ax=ax)
    ax.set_title('Gewichtstrend (Gleitender Durchschnitt)')
    plt.xticks(rotation=45)
    return fig


def plot_cumulative_weight(df):
    """Summen-Plot mit Döner-Icon am Ende"""
    fig, ax = plt.subplots(figsize=(12, 6))
    df_sorted = df.sort_values('DateTime')
    df_sorted['Cumulative_KG'] = df_sorted['gewicht_g'].cumsum() / 1000.0

    # Die Linie zeichnen
    sns.lineplot(x='DateTime', y='Cumulative_KG', data=df_sorted, drawstyle='steps-post', ax=ax, color='green',
                 linewidth=3)
    ax.fill_between(df_sorted['DateTime'], df_sorted['Cumulative_KG'], color='green', alpha=0.1)

    # --- KEBAP ICON LOGIK ---
    if not df_sorted.empty:
        try:
            # 1. Koordinaten des letzten Punktes holen (Aktueller Stand)
            last_x = df_sorted['DateTime'].iloc[-1]
            last_y = df_sorted['Cumulative_KG'].iloc[-1]

            # 2. Bild laden (muss im GitHub Repo liegen!)
            # 'zoom' steuert die Größe des Döners. Probier 0.1 bis 0.3 aus.
            img = mpimg.imread("kebap_icon.png")
            imagebox = OffsetImage(img, zoom=0.15)

            # 3. Bild an die Koordinaten heften
            ab = AnnotationBbox(imagebox, (last_x, last_y), frameon=False)
            ax.add_artist(ab)
        except Exception:
            # Falls das Bild fehlt, stürzt die App nicht ab, sondern macht einfach weiter
            pass
    # ------------------------

    ax.set_title('Gesamter Kebapkonsum (in kg)')
    ax.set_ylabel('Summe (kg)')
    plt.xticks(rotation=45)
    return fig

# --- 3. Erweiterte Statistik-Funktion ---

def show_advanced_stats(df):
    st.subheader("⚔️ 1-gegen-1 Vergleich")
    st.markdown("Vergleiche zwei Zubereiter direkt miteinander.")

    preparers = list(df['Zubereitet_Clean'].unique())
    preparers.sort()

    col1, col2 = st.columns(2)
    with col1:
        option_a = st.selectbox("Zubereiter A", preparers, index=0)
    with col2:
        default_idx = 1 if len(preparers) > 1 else 0
        option_b = st.selectbox("Zubereiter B", preparers, index=default_idx)

    if option_a == option_b:
        st.info("Wähle unterschiedliche Zubereiter.")
    else:
        data_a = df[df['Zubereitet_Clean'] == option_a]['gewicht_g']
        data_b = df[df['Zubereitet_Clean'] == option_b]['gewicht_g']

        c1, c2 = st.columns(2)
        c1.metric(f"Ø {option_a}", f"{data_a.mean():.1f} g", f"n={len(data_a)}")
        c2.metric(f"Ø {option_b}", f"{data_b.mean():.1f} g", f"n={len(data_b)}")

        if len(data_a) < 2 or len(data_b) < 2:
            st.warning("Zu wenig Daten für T-Test.")
        else:
            t_stat, p_val = stats.ttest_ind(data_a, data_b, equal_var=False)
            st.write(f"**p-Wert (Welch-Test):** `{p_val:.5f}`")
            if p_val < 0.05:
                st.success(
                    f"✅ Signifikant! {option_a if data_a.mean() > data_b.mean() else option_b} macht schwerere Kebaps.")
            else:
                st.info("❌ Nicht signifikant (Unterschied könnte Zufall sein).")

    st.divider()
    st.subheader("🔬 Globale Tests (ANOVA)")

    try:
        groups = df.groupby('Zubereitet_Clean')['gewicht_g'].apply(list)
        if len(groups) > 1:
            f_val, p_val = stats.f_oneway(*groups)
            if p_val < 0.05:
                st.success(f"✅ **Zubereiter:** Signifikant (p=`{p_val:.4f}`). Wer ihn macht, zählt.")
            else:
                st.info(f"❌ **Zubereiter:** Nicht signifikant (p=`{p_val:.4f}`).")
    except:
        st.write("Zu wenige Daten.")

    try:
        groups_day = df.groupby('Wochentag_DE')['gewicht_g'].apply(list)
        if len(groups_day) > 1:
            f_val, p_val = stats.f_oneway(*groups_day)
            if p_val < 0.05:
                st.success(f"✅ **Wochentag:** Signifikant (p=`{p_val:.4f}`). Der Tag ist wichtig.")
            else:
                st.info(f"❌ **Wochentag:** Nicht signifikant (p=`{p_val:.4f}`).")
    except:
        pass

    try:
        groups_hour = df.groupby('Stunde_Ganz')['gewicht_g'].apply(list)
        if len(groups_hour) > 1:
            f_val, p_val = stats.f_oneway(*groups_hour)
            if p_val < 0.05:
                st.success(f"✅ **Uhrzeit:** Signifikant (p=`{p_val:.4f}`). Die Uhrzeit beeinflusst das Gewicht.")
            else:
                st.info(f"❌ **Uhrzeit:** Nicht signifikant (p=`{p_val:.4f}`).")
    except:
        pass

    st.divider()
    st.subheader("📅 Arbeitsplan-Analyse")

    df_active = df[df['Zubereitet_Clean'] != "IDIOT"]

    try:
        ct = pd.crosstab(df_active['Wochentag_DE'], df_active['Zubereitet_Clean'])
        chi2, p_val, dof, ex = stats.chi2_contingency(ct)

        st.write(f"**Wer arbeitet wann?** (p=`{p_val:.4f}`)")

        st.write("Wahrscheinlichkeit (in %), dass ein Zubereiter an einem Tag arbeitet:")
        ct_prob = pd.crosstab(df_active['Wochentag_DE'], df_active['Zubereitet_Clean'], normalize='index') * 100

        days_order = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']
        ct_prob = ct_prob.reindex(days_order).dropna(how='all')

        # UPDATE: Custom Eigelb-Ampel (Rot -> Eigelb -> Grün)
        # 0% = Rot (#FF4B4B), 50% = Eigelb (#FFD700), 100% = Grün (#2ECC71)
        colors = ["#FF4B4B", "#FFD700", "#2ECC71"]
        cmap_custom = LinearSegmentedColormap.from_list("EigelbAmpel", colors)

        st.dataframe(ct_prob.style.format("{:.1f}%").background_gradient(cmap=cmap_custom, vmin=0, vmax=100))

    except Exception as e:
        st.warning(f"Zu wenig Daten für Analyse: {e}")


# --- 4. Haupt-App ---

def main_app():
    st.set_page_config(page_title="Kebapstudie Dashboard", layout="wide")
    st.title("🥙 Kebapstudie 2025 Dashboard")
    sns.set_theme(style="whitegrid")

    client = connect_to_gspread()
    if client is None: st.stop()

    df = get_all_kebaps_as_df(client)

    known_preparers = ["OG1", "OG2", "CHEF", "IDIOT", "ANDERE"]
    if not df.empty:
        db_preparers = list(df['zubereitet'].str.upper().unique())
        for p in db_preparers:
            if p not in known_preparers: known_preparers.append(p)
        known_preparers.sort()

    # --- Sidebar ---
    st.sidebar.title("Neuer Eintrag")
    with st.sidebar.form("add_form", clear_on_submit=True):
        datum = st.date_input("Datum", datetime.date.today())
        uhrzeit = st.time_input("Uhrzeit", datetime.datetime.now().time())
        gewicht = st.number_input("Gewicht [g]", 0, 1000, 450)
        personen = st.number_input("Personen", 0, 50, 3)
        zubereitet = st.selectbox("Zubereiter", known_preparers)
        if st.form_submit_button("Speichern"):
            add_kebap(client, datum, gewicht, zubereitet.upper(), personen, uhrzeit.strftime('%H:%M:%S'))
            st.success("Gespeichert!")
            st.rerun()

    if df.empty:
        st.warning("Keine Daten vorhanden.")
        st.stop()

    # --- Plots ---
    st.header("Analysen")
    plot_options = {
        "Verteilung (Histogramm)": plot_weight_distribution,
        "Nach Zubereiter (Farbig)": plot_weight_by_preparer,
        "Nach Wochentag (Farbig)": plot_weight_by_weekday,
        "Korrelations-Heatmap": plot_heatmap,
        "Gewicht vs Personen": plot_weight_vs_people,
        "Zeitverlauf": plot_weight_over_time,
        "Tageszeit": plot_weight_over_time_of_day
    }
    sel_plot = st.selectbox("Wähle Plot:", list(plot_options.keys()))
    st.pyplot(plot_options[sel_plot](df.copy()))

    # --- Stats in Tabs ---
    tab1, tab2 = st.tabs(["📊 Zahlen & Fakten", "🧪 Wissenschaftliche Tests"])

    with tab1:
        st.metric("Gesamtanzahl Kebaps", len(df))
        col1, col2, col3 = st.columns(3)
        with col1:
            st.caption("Top Zubereiter (Gewicht)")
            st.dataframe(df.groupby('Zubereitet_Clean')['gewicht_g'].mean().sort_values(ascending=False).round(1))
        with col2:
            st.caption("Top Wochentage (Gewicht)")
            st.dataframe(df.groupby('Wochentag_DE')['gewicht_g'].mean().sort_values(ascending=False).round(1))
        with col3:
            st.caption("Top Uhrzeit (Ø Gewicht)")
            hourly_stats = df.groupby('Stunde_Ganz')['gewicht_g'].mean().sort_values(ascending=False).round(1)
            hourly_stats.index = [f"{int(h):02d}:00" for h in hourly_stats.index]
            st.dataframe(hourly_stats)

        st.divider()
        st.subheader("📈 Langzeit-Trends")
        st.pyplot(plot_moving_average(df))
        st.pyplot(plot_cumulative_weight(df))

    with tab2:
        show_advanced_stats(df)

        # --- Edit ---
    with st.expander("Bearbeiten / Löschen"):
        df['id'] = pd.to_numeric(df['id'])
        ids = sorted(df['id'].unique(), reverse=True)
        sel_id = st.selectbox("ID wählen:", ids)

        if st.button("Laden"):
            row = df[df['id'] == sel_id].iloc[0]
            try:
                t_val = datetime.time.fromisoformat(str(row['uhrzeit']))
            except:
                try:
                    t_val = datetime.datetime.strptime(str(row['uhrzeit']), "%H:%M").time()
                except:
                    t_val = datetime.time(12, 0)
            try:
                d_val = datetime.datetime.strptime(str(row['datum']), "%d.%m.%Y").date()
            except:
                d_val = datetime.datetime.strptime(str(row['datum']), "%Y-%m-%d").date()

            st.session_state.edit_data = {
                "id": sel_id, "datum": d_val, "gewicht": int(row['gewicht_g']),
                "zub": str(row['zubereitet']), "pers": int(row['personen']), "uhr": t_val
            }

        if "edit_data" in st.session_state:
            with st.form("edit_form"):
                d = st.session_state.edit_data
                new_d = st.date_input("Datum", d['datum'])
                new_t = st.time_input("Uhrzeit", d['uhr'])
                new_g = st.number_input("Gewicht", 0, 1000, d['gewicht'])
                new_p = st.number_input("Personen", 0, 50, d['pers'])
                curr = d['zub'].upper()
                idx = known_preparers.index(curr) if curr in known_preparers else 0
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
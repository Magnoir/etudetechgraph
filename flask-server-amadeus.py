import streamlit as st
import pandas as pd
from datetime import datetime
import altair as alt
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

import os
# --- Connexion à Cosmos DB avec gestion d'erreur ---
load_dotenv()
url = os.getenv("URL_AZURE_COSMOS")
key = os.getenv("KEY_AZURE_COSMOS")

print(url, key)  # Vérifier que les valeurs sont bien définies


try:
    client = CosmosClient(url, credential=key)
    database = client.get_database_client("test")
    container = database.get_container_client("test-d")
except Exception as e:
    st.error(f"Erreur de connexion à Cosmos DB: {e}")
    st.stop()

# Fonction pour récupérer les différents search_id
@st.cache_data(show_spinner=False)
def get_search_ids():
    query = "SELECT DISTINCT c.search_id FROM c"
    items = list(container.query_items(query=query, enable_cross_partition_query=True))
    return [item['search_id'] for item in items]

# Fonction pour charger les données en fonction du search_id sélectionné
@st.cache_data(show_spinner=False)
def load_data_by_search_id(search_id):
    query = f"SELECT * FROM c WHERE c.search_id = '{search_id}'"
    items = list(container.query_items(query=query, enable_cross_partition_query=True))
    return items

# Fonction pour rafraîchir les données
def refresh_data():
    st.cache_data.clear()
    st.success("Données mises à jour avec succès!")

# --- Interface Streamlit ---
st.sidebar.title("Filtres de recherche")

# Bouton pour mettre à jour les données
if st.sidebar.button("🔄 Rafraîchir les données"):
    refresh_data()

# Récupérer les search_id disponibles
search_ids = get_search_ids()

if search_ids:
    # Sélectionner un search_id
    selected_search_id = st.sidebar.selectbox("Sélectionner un Search ID", search_ids)

    # Charger les données associées à ce search_id
    data = load_data_by_search_id(selected_search_id)

    if data:
        record = data[0]
    else:
        st.error(f"Aucune donnée trouvée pour le search_id {selected_search_id}.")
        st.stop()

    # --- Traitement des données ---
    recos = record.get("recos", [])
    df_recos = pd.DataFrame(recos)

    # Calcul de l'avance d'achat (booking lead time) en jours
    if not df_recos.empty:
        try:
            search_date = datetime.strptime(record.get("search_date"), "%Y-%m-%d")
            request_dep_date = datetime.strptime(record.get("request_dep_date"), "%Y-%m-%d")
            booking_lead_time = (request_dep_date - search_date).days
        except Exception as e:
            booking_lead_time = None
        df_recos["booking_lead_time"] = booking_lead_time
    else:
        df_recos["booking_lead_time"] = None

    # Création d'un DataFrame détaillé pour chaque vol
    flights_data = []
    for reco in recos:
        for flight in reco.get("flights", []):
            flight_copy = flight.copy()
            flight_copy["price"] = reco.get("price")
            flight_copy["main_airline"] = reco.get("main_marketing_airline")
            flights_data.append(flight_copy)
    df_flights = pd.DataFrame(flights_data)

    # --- Affichage des données dans Streamlit ---
    st.title("Dashboard de comparaison des tarifs")

    st.header("Recommandations")
    if not df_recos.empty:
        st.dataframe(df_recos)
    else:
        st.write("Aucune recommandation trouvée.")

    # --- Indicateurs clés (KPIs) ---
    st.header("Indicateurs clés")
    if not df_recos.empty:
        min_price = df_recos["price"].min()
        max_price = df_recos["price"].max()
        median_price = df_recos["price"].median()
        avg_price = df_recos["price"].mean()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Prix Min", f"{min_price:.2f}")
        col2.metric("Prix Max", f"{max_price:.2f}")
        col3.metric("Prix Médian", f"{median_price:.2f}")
        col4.metric("Prix Moyen", f"{avg_price:.2f}")
    else:
        st.write("Aucune donnée pour les indicateurs clés.")

    # --- Comparaison des prix par compagnie aérienne ---
    st.header("Comparaison des prix par compagnie aérienne")
    if not df_recos.empty:
        airline_prices = df_recos.groupby("main_marketing_airline")["price"].agg(["min", "median", "max"]).reset_index()
        
        chart = alt.Chart(airline_prices).mark_bar().encode(
            x=alt.X("main_marketing_airline:N", title="Compagnie aérienne"),
            y=alt.Y("median:Q", title="Prix médian"),
            color=alt.Color("main_marketing_airline:N", legend=None),
            tooltip=["main_marketing_airline", "min", "median", "max"]
        ).properties(width=600, height=400)
        
        st.altair_chart(chart, use_container_width=True)
        
        st.write("Tableau comparatif des prix par compagnie:")
        st.dataframe(airline_prices)
    else:
        st.write("Données insuffisantes pour la comparaison des prix.")

else:
    st.error("Aucun search_id disponible.")
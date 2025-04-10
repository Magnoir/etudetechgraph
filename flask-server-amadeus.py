import streamlit as st
import pandas as pd
from datetime import datetime
import altair as alt
from azure.cosmos import CosmosClient
from dotenv import load_dotenv
import os

# Configuration de la page
st.set_page_config(page_title="Comparaison Tarifaire Amadeus", layout="wide")

# --- Connexion à Cosmos DB avec gestion d'erreur ---
load_dotenv()
url = os.getenv("URL_AZURE_COSMOS")
key = os.getenv("KEY_AZURE_COSMOS")

try:
    client = CosmosClient(url, credential=key)
    database = client.get_database_client("amadeus-system-cosmosdbcontainer")
    container = database.get_container_client("amadeus-system-cosmosdbcontainer")
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

# Fonction pour formater un vol
def format_flight_info(flight):
    dep_date = flight.get('dep_date', '')
    dep_time = flight.get('dep_time', '')
    arr_date = flight.get('arr_date', '')
    arr_time = flight.get('arr_time', '')
    
    return {
        'airline': flight.get('marketing_airline', ''),
        'flight_nb': flight.get('flight_nb', ''),
        'dep': f"{flight.get('dep_airport', '')} ({dep_date} {dep_time})",
        'arr': f"{flight.get('arr_airport', '')} ({arr_date} {arr_time})",
        'cabin': flight.get('cabin', '')
    }

# Fonction pour créer un dataframe de synthèse des itinéraires
def create_itinerary_summary(recos):
    summary_list = []
    
    for i, reco in enumerate(recos):
        flights = reco.get('flights', [])
        
        # Information de base sur l'itinéraire
        itinerary = {
            'itinerary_id': i + 1,
            'price': float(reco.get('price', 0)),
            'taxes': float(reco.get('taxes', 0)),
            'fees': float(reco.get('fees', 0)),
            'total_segments': len(flights),
            'total_price': float(reco.get('price', 0)) + float(reco.get('fees', 0))
        }
        
        if flights:
            # Récupérer les points de départ et d'arrivée globaux
            first_flight = flights[0]
            last_flight = flights[-1]
            
            itinerary.update({
                'origin': first_flight.get('dep_airport', ''),
                'destination': last_flight.get('arr_airport', ''),
                'departure_date': first_flight.get('dep_date', ''),
                'departure_time': first_flight.get('dep_time', ''),
                'arrival_date': last_flight.get('arr_date', ''),
                'arrival_time': last_flight.get('arr_time', ''),
                'airlines': ', '.join(set([f.get('marketing_airline', '') for f in flights]))
            })
            
            # Calculer la durée totale si possible
            try:
                departure = datetime.strptime(f"{first_flight.get('dep_date')} {first_flight.get('dep_time')}", "%Y-%m-%d %H:%M")
                arrival = datetime.strptime(f"{last_flight.get('arr_date')} {last_flight.get('arr_time')}", "%Y-%m-%d %H:%M")
                duration = (arrival - departure).total_seconds() / 3600  # en heures
                itinerary['duration'] = f"{int(duration)}h {int((duration % 1) * 60)}m"
            except:
                itinerary['duration'] = "N/A"
        
        summary_list.append(itinerary)
    
    return pd.DataFrame(summary_list)

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
    
    # Calcul de l'avance d'achat (booking lead time) en jours
    if recos:
        try:
            search_date = datetime.strptime(record.get("search_date"), "%Y-%m-%d")
            request_dep_date = datetime.strptime(record.get("request_dep_date"), "%Y-%m-%d")
            booking_lead_time = (request_dep_date - search_date).days
        except Exception as e:
            booking_lead_time = None
    
    # --- Affichage de l'en-tête ---
    st.title("Comparaison des tarifs de vol")
    
    # --- Affichage des informations de voyage ---
    origin_city = record.get("origin_city", "Inconnu")
    destination_city = record.get("destination_city", "Inconnu")
    request_dep_date = record.get("request_dep_date", "Inconnu")
    request_return_date = record.get("request_return_date", "Inconnu")
    
    # Informations sur la recherche et le voyage
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### Itinéraire")
        st.markdown(f"**De:** {origin_city}")
        st.markdown(f"**À:** {destination_city}")
    
    with col2:
        st.markdown("### Dates")
        st.markdown(f"**Départ:** {request_dep_date}")
        st.markdown(f"**Retour:** {request_return_date}")
    
    with col3:
        st.markdown("### Informations")
        st.markdown(f"**Search ID:** {selected_search_id}")
        #st.markdown(f"**Date de recherche:** {record.get('search_date', 'Inconnu')}")
        #st.markdown(f"**Avance de réservation:** {booking_lead_time} jours")
    
    # --- Créer un résumé des itinéraires ---
    if recos:
        # Créer un DataFrame de résumé des itinéraires
        summary_df = create_itinerary_summary(recos)
        
        # --- Affichage des indicateurs clés (KPIs) ---
        st.header("Indicateurs clés")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Prix Min", f"{summary_df['price'].min():.2f} €")
        col2.metric("Prix Max", f"{summary_df['price'].max():.2f} €")
        col3.metric("Prix Médian", f"{summary_df['price'].median():.2f} €")
        col4.metric("Prix Moyen", f"{summary_df['price'].mean():.2f} €")
        
        # --- Tableau de résumé des itinéraires ---
        st.header("Résumé des itinéraires")
        # Filtrer les colonnes à afficher
        display_cols = ['itinerary_id', 'price', 'airlines', 'total_segments', 
                        'departure_date', 'departure_time', 'arrival_date', 'arrival_time', 'duration']
        
        st.dataframe(summary_df[display_cols], use_container_width=True)
        
        # --- Vue détaillée des itinéraires ---
        st.header("Détails des itinéraires")
        
        # Créer des onglets pour chaque itinéraire
        tabs = st.tabs([f"Itinéraire {i+1} ({reco['price']} €)" for i, reco in enumerate(recos)])
        
        for i, (tab, reco) in enumerate(zip(tabs, recos)):
            with tab:
                flights = reco.get('flights', [])
                
                st.subheader(f"Prix: {reco['price']} € (Taxes: {reco.get('taxes', '0')} €)")
                
                # Afficher les vols dans un format plus lisible
                for j, flight in enumerate(flights):
                    flight_info = format_flight_info(flight)
                    
                    st.markdown(f"#### Vol {j+1}: {flight_info['airline']} {flight_info['flight_nb']}")
                    cols = st.columns(2)
                    cols[0].markdown(f"**Départ:** {flight_info['dep']}")
                    cols[1].markdown(f"**Arrivée:** {flight_info['arr']}")
                    st.markdown("---")
        
        # --- Visualisations ---
        st.header("Visualisations")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Comparaison des prix par compagnie aérienne")
            airline_chart = alt.Chart(summary_df).mark_bar().encode(
                x=alt.X('airlines:N', title='Compagnie(s) aérienne(s)'),
                y=alt.Y('mean(price):Q', title='Prix moyen (€)'),
                color=alt.Color('airlines:N', legend=None),
                tooltip=['airlines', 'mean(price)', 'min(price)', 'max(price)']
            ).properties(height=400)
            
            st.altair_chart(airline_chart, use_container_width=True)
        
        with col2:
            st.subheader("Distribution des prix")
            hist_chart = alt.Chart(summary_df).mark_bar().encode(
                alt.X('price:Q', bin=True, title='Prix (€)'),
                y='count()',
                tooltip=['count()']
            ).properties(height=400)
            
            st.altair_chart(hist_chart, use_container_width=True)
        
        # --- Tableau comparatif des compagnies aériennes ---
        st.header("Comparaison des prix par compagnie aérienne")
        airline_stats = summary_df.groupby('airlines').agg({
            'price': ['min', 'max', 'mean', 'median', 'count']
        }).reset_index()
        
        airline_stats.columns = ['airlines', 'min_price', 'max_price', 'avg_price', 'median_price', 'count']
        airline_stats = airline_stats.sort_values('avg_price')
        
        st.dataframe(airline_stats, use_container_width=True)
    else:
        st.warning("Aucune recommandation trouvée pour ce Search ID.")
else:
    st.error("Aucun search_id disponible.")
"""
Application web interactive pour visualiser les résultats du benchmark NoSQL depuis InfluxDB
Usage: streamlit run visualize_results.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import numpy as np

# Configuration
st.set_page_config(
    page_title="Benchmark NoSQL - Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Charger les variables d'environnement
load_dotenv()

# Configuration InfluxDB
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ensa")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "bench")

# Noms des bases de données
DATABASES = ["MongoDB", "Redis", "Cassandra", "Neo4j"]

# Configurations des scénarios
SCENARIOS = {
    "scenario1_crud": {
        "name": "Opérations CRUD",
        "description": "Analyse des opérations de base sur les données : INSERTION, LECTURE, MISE À JOUR, SUPPRESSION",
        "fields": ["latency_ms", "total_time", "cpu_percent", "memory_percent"],
        "operations": ["insert", "read", "update", "delete"]
    },
    "scenario2_iot": {
        "name": "IoT/Journaux (Time-Series)",
        "description": "Analyse de l'ingestion de données de séries temporelles et des performances de requêtage",
        "fields": ["insert_time", "insert_throughput", "range_query_time", "insert_cpu", "insert_mem"]
    },
    "scenario3_graph": {
        "name": "Requêtes Graphiques",
        "description": "Analyse des relations de type réseau social et de la traversée de graphes",
        "fields": ["create_users_time", "create_friendships_time", "friends_of_friends_time", "three_level_time"]
    },
    "scenario4_keyvalue": {
        "name": "Vitesse Clé-Valeur",
        "description": "Analyse des opérations GET/SET ultra-rapides",
        "fields": ["set_latency_ms", "get_latency_ms", "throughput_ops", "cpu_usage"]
    },
    "scenario5_fulltext": {
        "name": "Recherche Full-Text",
        "description": "Analyse de l'indexation de texte et des opérations de recherche",
        "fields": ["insert_time", "index_build_time", "search_latency", "cpu_usage"]
    },
    "scenario6_scalability": {
        "name": "Test de Scalabilité",
        "description": "Analyse des opérations multi-thread et des charges concurrentes",
        "fields": ["create_time", "read_time", "update_time", "delete_time", "throughput_ops"]
    }
}

@st.cache_resource
def get_influx_client():
    """Initialiser le client InfluxDB avec cache"""
    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        return client, client.query_api()
    except Exception as e:
        st.error(f"Erreur de connexion à InfluxDB: {e}")
        return None, None

def query_scenario_data(_query_api, scenario_name, time_range="-24h"):
    """Interroger les données d'un scénario depuis InfluxDB"""
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {time_range})
      |> filter(fn: (r) => r["_measurement"] == "{scenario_name}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    try:
        tables = _query_api.query(query, org=INFLUX_ORG)
        results = []
        for table in tables:
            for record in table.records:
                results.append(record.values)
        return results
    except Exception as e:
        st.error(f"Erreur lors de la requête {scenario_name}: {e}")
        return []

def convert_to_dataframe(data):
    """Convertir les données en DataFrame"""
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    
    # Convertir les timestamps
    if '_time' in df.columns:
        df['_time'] = pd.to_datetime(df['_time'])
        df = df.sort_values('_time')
    
    return df

def create_comparison_chart(data, title, y_label, lower_better=True):
    """Créer un graphique de comparaison"""
    if data.empty:
        return None
    
    fig = go.Figure()
    
    # Couleurs pour chaque base de données
    colors = {
        'MongoDB': '#4CAF50',
        'Redis': '#F44336',
        'Cassandra': '#2196F3',
        'Neo4j': '#9C27B0'
    }
    
    for db in DATABASES:
        db_data = data[data['database'] == db]
        if not db_data.empty:
            fig.add_trace(go.Scatter(
                x=db_data['_time'],
                y=db_data[y_label],
                name=db,
                mode='lines+markers',
                line=dict(color=colors.get(db, '#666'), width=2),
                marker=dict(size=8),
                hovertemplate=f'<b>{db}</b><br>Temps: %{{x}}<br>{y_label}: %{{y:.2f}}<extra></extra>'
            ))
    
    fig.update_layout(
        title=dict(text=title, font=dict(size=20)),
        xaxis_title="Temps",
        yaxis_title=y_label,
        hovermode='x unified',
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )
    
    return fig

def create_bar_comparison(data, metric, title):
    """Créer un graphique à barres de comparaison"""
    if data.empty:
        return None
    
    # Calculer la moyenne par base de données
    summary = data.groupby('database')[metric].mean().reset_index()
    
    fig = px.bar(
        summary,
        x='database',
        y=metric,
        color='database',
        title=title,
        text_auto='.2f',
        color_discrete_map={
            'MongoDB': '#4CAF50',
            'Redis': '#F44336',
            'Cassandra': '#2196F3',
            'Neo4j': '#9C27B0'
        }
    )
    
    fig.update_layout(
        xaxis_title="Base de données",
        yaxis_title=metric,
        template="plotly_white",
        showlegend=False,
        height=400
    )
    
    fig.update_traces(
        textposition='outside',
        textfont=dict(size=12)
    )
    
    return fig

def display_summary_stats():
    """Afficher les statistiques récapitulatives"""
    client, query_api = get_influx_client()
    if not query_api:
        return
    
    st.header("📊 Vue d'ensemble")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Compter les scénarios avec données
    scenarios_with_data = 0
    total_measurements = 0
    
    with st.spinner("Chargement des données..."):
        for scenario_key in SCENARIOS.keys():
            data = query_scenario_data(query_api, scenario_key)
            if data:
                scenarios_with_data += 1
                total_measurements += len(data)
        
        with col1:
            st.metric("Scénarios avec données", f"{scenarios_with_data}/{len(SCENARIOS)}")
        
        with col2:
            st.metric("Mesures totales", total_measurements)
        
        with col3:
            st.metric("Bases testées", len(DATABASES))
        
        with col4:
            st.metric("Source", "InfluxDB")

def display_scenario_analysis():
    """Afficher l'analyse par scénario"""
    client, query_api = get_influx_client()
    if not query_api:
        return
    
    st.header("📈 Analyse par scénario")
    
    # Sélecteur de scénario
    scenario_options = {f"{info['name']} ({key})": key for key, info in SCENARIOS.items()}
    selected_scenario_label = st.selectbox(
        "Sélectionner un scénario:",
        options=list(scenario_options.keys())
    )
    selected_scenario = scenario_options[selected_scenario_label]
    scenario_info = SCENARIOS[selected_scenario]
    
    st.subheader(scenario_info['name'])
    st.write(scenario_info['description'])
    
    # Charger les données
    data = query_scenario_data(query_api, selected_scenario, "-24h")
    
    if not data:
        st.warning("⚠️ Aucune donnée disponible pour ce scénario")
        return
    
    df = convert_to_dataframe(data)
    
    # Afficher les données brutes
    with st.expander("📋 Données brutes"):
        st.dataframe(df, use_container_width=True)
    
    # Métriques clés
    st.subheader("📊 Métriques clés")
    
    if selected_scenario == "scenario1_crud":
        # Pour CRUD, afficher par opération
        operations = scenario_info['operations']
        
        cols = st.columns(len(operations))
        for idx, op in enumerate(operations):
            with cols[idx]:
                op_data = df[df['operation'] == op]
                if not op_data.empty:
                    avg_latency = op_data['latency_ms'].mean()
                    st.metric(
                        label=f"Latence {op}",
                        value=f"{avg_latency:.2f} ms",
                        delta=f"Moyenne sur {len(op_data)} mesures"
                    )
    else:
        # Pour les autres scénarios
        cols = st.columns(3)
        metrics_to_show = scenario_info['fields'][:3]
        
        for idx, metric in enumerate(metrics_to_show):
            if metric in df.columns:
                with cols[idx]:
                    avg_value = df[metric].mean()
                    unit = "ms" if "latency" in metric else "s" if "time" in metric else "ops/sec" if "throughput" in metric else "%"
                    st.metric(
                        label=metric.replace('_', ' ').title(),
                        value=f"{avg_value:.2f} {unit}"
                    )
    
    # Graphiques
    st.subheader("📈 Visualisations")
    
    if selected_scenario == "scenario1_crud":
        # Graphique CRUD par opération
        fig = make_subplots(
            rows=1, cols=len(scenario_info['operations']),
            subplot_titles=[op.upper() for op in scenario_info['operations']]
        )
        
        for idx, op in enumerate(scenario_info['operations'], 1):
            op_data = df[df['operation'] == op]
            if not op_data.empty:
                for db in DATABASES:
                    db_op_data = op_data[op_data['database'] == db]
                    if not db_op_data.empty:
                        fig.add_trace(
                            go.Bar(
                                x=[db],
                                y=[db_op_data['latency_ms'].mean()],
                                name=f"{db} - {op}",
                                showlegend=(idx == 1)
                            ),
                            row=1, col=idx
                        )
        
        fig.update_layout(
            title="Latence CRUD par opération",
            height=400,
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)
    
    else:
        # Graphique pour les autres scénarios
        col1, col2 = st.columns(2)
        
        with col1:
            # Graphique temporel
            if 'latency_ms' in df.columns or 'search_latency' in df.columns:
                metric = 'latency_ms' if 'latency_ms' in df.columns else 'search_latency'
                fig_time = create_comparison_chart(df, "Évolution dans le temps", metric)
                if fig_time:
                    st.plotly_chart(fig_time, use_container_width=True)
        
        with col2:
            # Graphique à barres
            if 'throughput_ops' in df.columns or 'insert_throughput' in df.columns:
                metric = 'throughput_ops' if 'throughput_ops' in df.columns else 'insert_throughput'
                fig_bar = create_bar_comparison(df, metric, "Débit de traitement")
                if fig_bar:
                    st.plotly_chart(fig_bar, use_container_width=True)

def display_comparison_dashboard():
    """Tableau de bord de comparaison"""
    client, query_api = get_influx_client()
    if not query_api:
        return
    
    st.header("🏆 Tableau de bord de comparaison")
    
    # Sélectionner les métriques à comparer
    col1, col2 = st.columns(2)
    
    with col1:
        scenario_options = list(SCENARIOS.keys())
        selected_scenario = st.selectbox(
            "Scénario:",
            options=scenario_options,
            format_func=lambda x: SCENARIOS[x]['name']
        )
    
    with col2:
        scenario_info = SCENARIOS[selected_scenario]
        metric_options = scenario_info['fields']
        selected_metric = st.selectbox(
            "Métrique:",
            options=metric_options,
            format_func=lambda x: x.replace('_', ' ').title()
        )
    
    # Charger les données
    data = query_scenario_data(query_api, selected_scenario, "-24h")
    
    if not data:
        st.warning("Aucune donnée disponible pour cette comparaison")
        return
    
    df = convert_to_dataframe(data)
    
    # Tableau de comparaison
    st.subheader("📊 Tableau comparatif")
    
    if selected_scenario == "scenario1_crud":
        # Pour CRUD, afficher par opération
        operation = st.selectbox("Opération:", scenario_info['operations'])
        op_data = df[df['operation'] == operation]
        
        if not op_data.empty:
            summary = op_data.groupby('database')[selected_metric].agg(['mean', 'min', 'max', 'std']).round(2)
            st.dataframe(summary, use_container_width=True)
            
            # Graphique radar
            radar_data = op_data.groupby('database')[selected_metric].mean().reset_index()
            fig_radar = px.line_polar(
                radar_data,
                r=selected_metric,
                theta='database',
                line_close=True,
                title=f"Comparaison {selected_metric} - {operation.upper()}"
            )
            st.plotly_chart(fig_radar, use_container_width=True)
    else:
        # Pour les autres scénarios
        summary = df.groupby('database')[selected_metric].agg(['mean', 'min', 'max', 'std']).round(2)
        st.dataframe(summary, use_container_width=True)
        
        # Graphique en boîte
        fig_box = px.box(
            df,
            x='database',
            y=selected_metric,
            color='database',
            title=f"Distribution de {selected_metric}"
        )
        st.plotly_chart(fig_box, use_container_width=True)

def display_recommendations():
    """Afficher les recommandations"""
    st.header("💡 Recommandations")
    
    recommendations = {
        "Opérations simples à haute fréquence": {
            "recommandation": "Redis",
            "raison": "Offre la meilleure latence pour les opérations clé-valeur",
            "icon": "⚡"
        },
        "Stockage de documents flexibles": {
            "recommandation": "MongoDB",
            "raison": "Équilibre optimal entre performance et flexibilité",
            "icon": "📄"
        },
        "Données time-series et écritures massives": {
            "recommandation": "Cassandra",
            "raison": "Excelle dans le débit d'écriture et la scalabilité horizontale",
            "icon": "📈"
        },
        "Requêtes de relations complexes": {
            "recommandation": "Neo4j",
            "raison": "Inégalé pour les traversées de graphes et analyses de relations",
            "icon": "🕸️"
        },
        "Recherche full-text avancée": {
            "recommandation": "MongoDB",
            "raison": "Intégration native avec moteurs de recherche",
            "icon": "🔍"
        }
    }
    
    for use_case, info in recommendations.items():
        with st.expander(f"{info['icon']} {use_case}"):
            st.markdown(f"**Recommandé :** {info['recommandation']}")
            st.markdown(f"*{info['raison']}*")
            
            # Afficher les alternatives
            alternatives = [db for db in DATABASES if db != info['recommandation']]
            st.markdown(f"**Alternatives :** {', '.join(alternatives)}")

def main():
    """Fonction principale"""
    # Sidebar
    with st.sidebar:
        st.title("📊 Benchmark NoSQL")
        st.markdown("---")
        
        # Menu de navigation
        page = st.radio(
            "Navigation",
            ["Vue d'ensemble", "Analyse par scénario", "Comparaison", "Recommandations", "À propos"]
        )
        
        st.markdown("---")
        
        # Informations de connexion
        st.caption(f"**Source :** InfluxDB")
        st.caption(f"**Bucket :** {INFLUX_BUCKET}")
        st.caption(f"**Bases :** {', '.join(DATABASES)}")
        
        # Rafraîchir les données
        if st.button("🔄 Rafraîchir les données"):
            st.cache_resource.clear()
            st.rerun()
    
    # Contenu principal
    if page == "Vue d'ensemble":
        display_summary_stats()
        
        # Aperçu rapide de tous les scénarios
        st.header("🎯 Aperçu des scénarios")
        
        cols = st.columns(3)
        for idx, (scenario_key, scenario_info) in enumerate(SCENARIOS.items()):
            with cols[idx % 3]:
                with st.container():
                    st.markdown(f"##### {scenario_info['name']}")
                    st.markdown(f"*{scenario_info['description'][:100]}...*")
                    if st.button(f"Analyser →", key=f"btn_{scenario_key}"):
                        st.session_state['auto_scenario'] = scenario_key
                        st.rerun()
    
    elif page == "Analyse par scénario":
        # Si un scénario a été sélectionné depuis l'aperçu
        if 'auto_scenario' in st.session_state:
            scenario_key = st.session_state['auto_scenario']
            del st.session_state['auto_scenario']
            
            # Sélectionner automatiquement le scénario
            scenario_name = f"{SCENARIOS[scenario_key]['name']} ({scenario_key})"
            st.experimental_set_query_params(scenario=scenario_key)
        
        display_scenario_analysis()
    
    elif page == "Comparaison":
        display_comparison_dashboard()
    
    elif page == "Recommandations":
        display_recommendations()
    
    elif page == "À propos":
        st.header("ℹ️ À propos")
        st.markdown("""
        ### Dashboard d'analyse de benchmark NoSQL
        
        Cette application permet de visualiser et d'analyser les résultats des tests de performance
        sur les bases de données NoSQL suivantes :
        
        - **MongoDB** - Base de données orientée documents
        - **Redis** - Base de données clé-valeur en mémoire
        - **Cassandra** - Base de données orientée colonnes
        - **Neo4j** - Base de données orientée graphe
        
        ### Fonctionnalités
        
        1. **Vue d'ensemble** - Statistiques récapitulatives
        2. **Analyse par scénario** - Détails de chaque test
        3. **Comparaison** - Tableau de bord interactif
        4. **Recommandations** - Conseils de sélection
        
        ### Données
        
        Les données sont récupérées en temps réel depuis **InfluxDB**, une base de données
        optimisée pour les séries temporelles.
        
        ### Technologies utilisées
        
        - **Streamlit** - Interface web interactive
        - **Plotly** - Visualisations dynamiques
        - **InfluxDB** - Stockage des métriques
        - **Pandas** - Analyse des données
        
        ---
        
        *Développé pour l'analyse comparative des performances NoSQL*
        """)

if __name__ == "__main__":
    main()
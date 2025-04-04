import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
from metrics import PortfolioMetrics
from strategies import Strategies
import numpy as np
import matplotlib.ticker as mtick
from datetime import datetime, timedelta
from io import StringIO  # Ajout de l'import manquant

# Configuration de la page Streamlit
st.set_page_config(
    page_title="Dashboard de Performance - Portefeuilles",
    page_icon="📈",
    layout="wide"
)

# Titre et introduction
st.title("Dashboard de Performance des Portefeuilles")


# Fonction pour récupérer la liste des stratégies disponibles
def get_available_strategies(db_file="fund.db"):
    with sqlite3.connect(db_file) as conn:
        df = pd.read_sql_query(
            "SELECT DISTINCT type FROM Portfolios WHERE produits IS NOT NULL", 
            conn)
    return df['type'].tolist()

# Récupération des stratégies disponibles
strategies = get_available_strategies()

# Sélection de la stratégie
col1, col2 = st.columns([1, 2])
with col1:
    selected_strategy = st.selectbox(
        "Sélectionnez une stratégie",
        options=strategies,
        index=0
    )
    
    # Sélection de la période d'analyse
    st.subheader("Période d'analyse")
    
    # Récupération des dates disponibles pour la stratégie sélectionnée
    @st.cache_data
    def get_strategy_dates(strategy, db_file="fund.db"):
        metrics = PortfolioMetrics(strategy, db_file)
        returns = metrics.returns()
        return returns['date'].min(), returns['date'].max()
    
    min_date, max_date = get_strategy_dates(selected_strategy)
    
    start_date = st.date_input(
        "Date de début",
        value=min_date,
        min_value=min_date,
        max_value=max_date
    )
    
    end_date = st.date_input(
        "Date de fin",
        value=max_date,
        min_value=min_date,
        max_value=max_date
    )
    
    # Métriques à afficher
    st.subheader("Métriques")
    metrics_selection = st.multiselect(
        "Sélectionnez les métriques à afficher",
        options=["Rendement moyen", "Rendement total", "Volatilité", "Ratio de Sharpe", "Drawdown maximum"],
        default=["Rendement total", "Volatilité", "Ratio de Sharpe"]
    )


with col2:
    metrics = PortfolioMetrics(selected_strategy)
    
    # Affichage des métriques sélectionnées
    metrics_data = {}
    if "Rendement moyen" in metrics_selection:
        metrics_data["Rendement moyen"] = f"{metrics.mean_return() * 100:.2f}%"
    if "Rendement total" in metrics_selection:
        metrics_data["Rendement total"] = f"{metrics.total_return() * 100:.2f}%"
    if "Volatilité" in metrics_selection:
        metrics_data["Volatilité"] = f"{metrics.volatility() * 100:.2f}%"
    if "Ratio de Sharpe" in metrics_selection:
        metrics_data["Ratio de Sharpe"] = f"{metrics.sharpe_ratio():.2f}"
    if "Drawdown maximum" in metrics_selection:
        metrics_data["Drawdown maximum"] = f"{metrics.max_drawdown() * 100:.2f}%"
    
    # Affichage des métriques en cards
    metrics_cols = st.columns(len(metrics_data))
    for i, (metric_name, metric_value) in enumerate(metrics_data.items()):
        with metrics_cols[i]:
            st.metric(metric_name, metric_value)

# Visualisation des rendements cumulés
st.header("Rendements cumulés")
fig_returns = metrics.plot(plot_type='return', start_date=start_date, end_date=end_date)
st.pyplot(fig_returns)

# Visualisation de la volatilité
st.header("Drawdown")
fig_drawdown = metrics.plot(plot_type='drawdown', start_date=start_date, end_date=end_date)
st.pyplot(fig_drawdown)

# Comparaison des stratégies
st.header("Comparaison des stratégies")
compare = st.checkbox("Comparer avec d'autres stratégies")

if compare:
    strategies_to_compare = st.multiselect(
        "Sélectionnez les stratégies à comparer",
        options=[s for s in strategies if s != selected_strategy],
        default=[]
    )
    
    if strategies_to_compare:
        # Préparation des données pour la comparaison
        plt.figure(figsize=(12, 6))
        
        # Tracé de la stratégie principale
        main_metrics = PortfolioMetrics(selected_strategy)
        main_returns = main_metrics.returns()
        main_returns = main_returns[(main_returns['date'] >= pd.to_datetime(start_date)) & 
                                   (main_returns['date'] <= pd.to_datetime(end_date))]
        main_cumulative = (1 + main_returns['return']).cumprod() - 1
        plt.plot(main_returns['date'], main_cumulative, label=selected_strategy)
        
        # Tracé des stratégies à comparer
        for strategy in strategies_to_compare:
            comp_metrics = PortfolioMetrics(strategy)
            comp_returns = comp_metrics.returns()
            comp_returns = comp_returns[(comp_returns['date'] >= pd.to_datetime(start_date)) & 
                                       (comp_returns['date'] <= pd.to_datetime(end_date))]
            comp_cumulative = (1 + comp_returns['return']).cumprod() - 1
            plt.plot(comp_returns['date'], comp_cumulative, label=strategy)
        
        plt.title("Comparaison des rendements cumulés")
        plt.ylabel("Rendement cumulé")
        plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
        plt.grid(alpha=0.3)
        plt.legend()
        plt.tight_layout()
        
        st.pyplot(plt.gcf())
        
        # Tableau de comparaison des métriques
        comparison_data = []
        
        # Métriques pour la stratégie principale
        main_data = {
            "Stratégie": selected_strategy,
            "Rendement moyen": f"{main_metrics.mean_return() * 100:.2f}%",
            "Rendement total": f"{main_metrics.total_return() * 100:.2f}%",
            "Volatilité": f"{main_metrics.volatility() * 100:.2f}%",
            "Ratio de Sharpe": f"{main_metrics.sharpe_ratio():.2f}",
            "Drawdown maximum": f"{main_metrics.max_drawdown() * 100:.2f}%"
        }
        comparison_data.append(main_data)
        
        # Métriques pour les stratégies à comparer
        for strategy in strategies_to_compare:
            comp_metrics = PortfolioMetrics(strategy)
            comp_data = {
                "Stratégie": strategy,
                "Rendement moyen": f"{comp_metrics.mean_return() * 100:.2f}%",
                "Rendement total": f"{comp_metrics.total_return() * 100:.2f}%",
                "Volatilité": f"{comp_metrics.volatility() * 100:.2f}%",
                "Ratio de Sharpe": f"{comp_metrics.sharpe_ratio():.2f}",
                "Drawdown maximum": f"{comp_metrics.max_drawdown() * 100:.2f}%"
            }
            comparison_data.append(comp_data)
        
        st.dataframe(pd.DataFrame(comparison_data).set_index("Stratégie"))

# Informations détaillées sur la stratégie
st.header(f"Détails de la stratégie : {selected_strategy}")

if selected_strategy == "low_risk":
    st.markdown("""
    **Stratégie Low Risk**
    
    Cette stratégie vise à minimiser la volatilité du portefeuille à 10% par an avec les contraintes suivantes:
    - La somme des poids doit être égale à 1
    - La somme des poids attribués aux actifs de la catégorie "bond" doit être >= 0.6
    - La vente à découvert est interdite
    """)
elif selected_strategy == "low_turnover":
    st.markdown("""
    **Stratégie Low Turnover**
    
    Cette stratégie utilise un modèle de régression linéaire pour prédire le rendement suivant de chaque actif.
    Les parts des actifs sont normalisées (rendement/|somme des rendements|).
    La vente à découvert est autorisée dans cette stratégie.
    """)
elif selected_strategy == "high_yield_equity_only":
    st.markdown("""
    **Stratégie High Yield Equity Only**
    
    Cette stratégie vise à maximiser le rendement moyen sur les deux dernières semaines avec les contraintes suivantes:
    - La somme des poids doit être égale à 1
    - La vente à découvert est autorisée
    - Seuls les actifs de la catégorie "equity" sont considérés
    """)

# Section pour examiner les transactions (deals)
st.header("Transactions récentes")

@st.cache_data
def get_deals(db_file="fund.db"):
    with sqlite3.connect(db_file) as conn:
        query = f"""
        SELECT date, "{selected_strategy}" as deals
        FROM Deals
        WHERE "{selected_strategy}" IS NOT NULL
        ORDER BY date DESC
        LIMIT 5
        """
        deals = pd.read_sql_query(query, conn)
    return deals

deals = get_deals()
if not deals.empty:
    for _, deal in deals.iterrows():
        with st.expander(f"Transaction du {deal['date']}"):
            try:
                deals_json = deal['deals']
                if deals_json:
                    deals_data = pd.read_json(StringIO(deals_json), orient='index')
                    st.dataframe(deals_data)
                else:
                    st.write("Aucune donnée disponible pour cette transaction.")
            except:
                st.write("Erreur lors du traitement des données de cette transaction.")
else:
    st.write("Aucune transaction récente trouvée pour cette stratégie.")





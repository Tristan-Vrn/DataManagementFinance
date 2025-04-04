import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import StringIO
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PortfolioMetrics:
    def __init__(self, portfolio_type, db_file="fund.db"):
        self.db_file = db_file
        self.portfolio_type = portfolio_type
        self._load_returns()
        
    def _load_returns(self):
        """Charge les rendements du portefeuille depuis la base de données."""
        with sqlite3.connect(self.db_file) as conn:
            #on récupère les données de la table portfolios
            portfolios = pd.read_sql_query(
                """SELECT date_creation, produits FROM Portfolios 
                WHERE type = ? AND produits IS NOT NULL ORDER BY date_creation ASC""", 
                conn, params=(self.portfolio_type,))
            
            portfolios['date_creation'] = pd.to_datetime(portfolios['date_creation'])

            #on initialise les vecteurs de rendements et de dates
            all_returns = []
            all_dates = []
            
            for _, portfolio in portfolios.iterrows():
                start_date = portfolio['date_creation']
                end_date = start_date + timedelta(days=6)
                
                #on récupère les poids
                weights = pd.read_json(StringIO(portfolio['produits']), orient='index')
                
                # Vérification et normalisation des poids
                weights_sum = weights['weight'].sum()
                if not np.isclose(weights_sum, 1.0, atol=1e-5):
                    logger.warning(f"Portefeuille du {start_date.strftime('%Y-%m-%d')}: Somme des poids = {weights_sum:.4f}, normalisation appliquée")
                    
                    # Normalisation des poids pour qu'ils s'additionnent à 1
                    if weights_sum > 0:  # Éviter division par zéro
                        weights['weight'] = weights['weight'] / weights_sum
                    else:
                        logger.error(f"Portefeuille du {start_date.strftime('%Y-%m-%d')}: Somme des poids = {weights_sum:.4f}, impossible de normaliser")
                        continue  # Skip this portfolio
                
                product_ids = weights.index.tolist()
                
                if not product_ids:  # Skip if no products in portfolio
                    continue
                    
                #on récupère les rendements
                product_id_list = ", ".join(f"'{id}'" for id in product_ids)  # Utilisation de guillemets pour éviter les problèmes de type
                query = f"""
                    SELECT product_id, date, value 
                    FROM Returns 
                    WHERE product_id IN ({product_id_list}) 
                    AND date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
                """

                #on exécute la requête pour récupérer les rendements
                returns_data = pd.read_sql_query(query, conn)
                returns_data['date'] = pd.to_datetime(returns_data['date'])
                # Filtrer les valeurs infinies dans 'value'
                returns_data['value'] = pd.to_numeric(returns_data['value'], errors='coerce')
                returns_data = returns_data.replace([np.inf, -np.inf], np.nan).dropna(subset=['value'])
                
                if returns_data.empty:  # Skip if no returns data found
                    continue
                    
                returns_data['date'] = pd.to_datetime(returns_data['date'])
                
                # Création d'un DataFrame pour les rendements quotidiens de ce portefeuille
                date_range = pd.date_range(start=start_date, end=end_date)
                portfolio_returns = pd.DataFrame(index=date_range, columns=['return'])
                
                for date, group in returns_data.groupby('date'):
                    # S'assurer que les types de product_id sont cohérents pour le merge
                    product_id_col = group['product_id'].astype(str)
                    weight_index = weights.index.astype(str)
                    
                    # Créer un dataframe temporaire avec les poids pour faciliter le merge
                    weights_df = pd.DataFrame({'weight': weights['weight']})
                    weights_df.index = weight_index
                    
                    # Merge des rendements avec les poids
                    merged = pd.DataFrame({'product_id': product_id_col, 'value': group['value']})
                    merged = merged.merge(weights_df, left_on='product_id', right_index=True, how='inner')
                    
                    # Calculer le rendement pondéré
                    if not merged.empty:
                        daily_return = (merged['value'] * merged['weight']).sum()
                        portfolio_returns.loc[date, 'return'] = daily_return
                
                # Ajouter les rendements non-nuls aux listes globales
                valid_returns = portfolio_returns.dropna()
                for date, row in valid_returns.iterrows():
                    all_returns.append(row['return'])
                    all_dates.append(date)
                
                # Logging des informations utiles pour le débogage
                logger.info(f"Portefeuille du {start_date.strftime('%Y-%m-%d')}: {len(product_ids)} produits, {valid_returns.shape[0]} jours de rendements")
            
            self._returns = pd.DataFrame({'date': all_dates, 'return': all_returns})
            self._returns.sort_values('date', inplace=True)
            
            # Si aucun rendement n'a été calculé, créer un DataFrame vide avec les bonnes colonnes
            if self._returns.empty:
                self._returns = pd.DataFrame(columns=['date', 'return'])

    
    def returns(self):
        return self._returns
    
    def mean_return(self):
        return self._returns['return'].mean()
    
    def total_return(self):
        return (1 + self._returns['return']).prod() - 1
    
    def volatility(self):
        return self._returns['return'].std()
    
    def sharpe_ratio(self, risk_free_rate=0):
        mean = self.mean_return()
        vol = self.volatility()
        return (mean - risk_free_rate) / vol
    
    def max_drawdown(self):
        cumulative = (1 + self._returns['return']).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        return drawdown.min()
    
    def plot(self, plot_type='return', start_date=None, end_date=None):
        
        df = self._returns.copy()
        if start_date: 
            df = df[df['date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['date'] <= pd.to_datetime(end_date)]
        
        df = df.sort_values('date')
    
        plt.figure(figsize=(10, 5))
        
        if plot_type == 'return':
            dates = df['date'].values
            cum_return = (1 + df['return']).cumprod().values - 1
            

            plt.plot(dates, cum_return) # on trace le graphique
            plt.title(f'Rendement cumulé - {self.portfolio_type}')
            plt.ylabel('Rendement')
            
        elif plot_type == 'drawdown':
            dates = df['date'].values
            cum_return = (1 + df['return']).cumprod().values
            running_max = np.maximum.accumulate(cum_return)
            drawdown = (cum_return / running_max) - 1
            
            plt.fill_between(dates, drawdown, 0, color='red', alpha=0.3)
            plt.title(f'Drawdown - {self.portfolio_type}')
            plt.ylabel('Drawdown')
        
        #Formatage axe y
        plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
        plt.grid(alpha=0.3)
        plt.tight_layout()
        
        return plt.gcf()


def calculate_portfolio_returns(portfolio_type, db_file="fund.db"):
    metrics = PortfolioMetrics(portfolio_type, db_file)
    return metrics.returns()
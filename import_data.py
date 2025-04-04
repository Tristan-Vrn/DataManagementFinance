import yfinance as yf

import pandas as pd

import sqlite3

import os

import time
import numpy as np

class DataImporter:

    def __init__(self, db_file="fund.db"):

        self.db_file = db_file

    def fill_returns(self, start_date, end_date):
        """
        Télécharge les données de prix et calcule les rendements hebdomadaires pour tous les produits.
        Gère les cas d'erreurs comme les rendements nuls ou extrêmes.
        """
        
        returns_data = []
        try:
            # Chargement des produits depuis la table Products
            with sqlite3.connect(self.db_file) as conn:
                products = pd.read_sql_query("SELECT product_id, ticker FROM Products", conn)
            if products.empty:
                print("Aucun produit trouvé dans la table Products.")
                return
            
            # Télécharger les données pour tous les tickers en une seule fois
            tickers = products["ticker"].tolist()
            time.sleep(0.1)
            data = yf.download(tickers, start=start_date, end=end_date, progress=False, group_by="ticker")

            if data is None or data.empty:
                print("Aucune donnée téléchargée depuis Yahoo Finance.")
                return

            # Pour chaque produit, on calcule les rendements 
            for product_id, ticker in products[["product_id", "ticker"]].itertuples(index=False):
                try:
                    # Les données pour un ticker sont stockées dans data[ticker]
                    ticker_data = data[ticker].copy() if isinstance(data, pd.DataFrame) and ticker in data else data.loc[:, (ticker, slice(None))].copy()
                except Exception as e:
                    print(f"Pas de données pour le ticker {ticker}: {e}")
                    continue

                # Assurons-nous que nous avons des données
                if ticker_data.empty:
                    print(f"Pas de données pour {ticker}")
                    continue
                
                # Réinitialisation de l'index pour obtenir la date dans une colonne
                ticker_data.reset_index(inplace=True)
                
                # Pour un calcul hebdomadaire simple, nous pouvons calculer les rendements
                # sur une base de vendredi à vendredi ou de semaine à semaine
                
                # Méthode 1 (plus simple): calculer le rendement pour chaque jour par rapport à la semaine précédente (même jour)
                ticker_data["weekly_return"] = ticker_data["Close"].pct_change(5, fill_method=None)
              
                ticker_data = ticker_data[np.isfinite(ticker_data["weekly_return"])]
                


                ticker_data["Date_str"] = ticker_data["Date"].dt.strftime("%Y-%m-%d")

                # Préparation des tuples pour chaque ligne
                returns = list(zip([product_id]*len(ticker_data), ticker_data["Date_str"], ticker_data["weekly_return"]))
                returns_data.extend(returns)
            
            # Insertion des données dans la table Returns (uniquement si returns_data n'est pas vide)
            if returns_data:
                with sqlite3.connect(self.db_file) as conn:
                    cursor = conn.cursor()
                    cursor.executemany(
                        """
                        INSERT INTO Returns (product_id, date, value)
                        VALUES (?, ?, ?)
                        """, 
                        returns_data
                    )
                    conn.commit()
                print(f"Returns hebdomadaires ajoutés avec succès: {len(returns_data)} entrées.")
            else:
                print("Aucun return à insérer (les valeurs étaient NULL, inf ou données manquantes).")
        except Exception as e:
            print(f"Erreur lors de la génération des returns : {e}")

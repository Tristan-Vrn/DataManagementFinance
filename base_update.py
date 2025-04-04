import sqlite3
import json
import pandas as pd
from datetime import timedelta
from io import StringIO  # Ajout de l'import StringIO

def update_portfolio(date_str, risk_profile, weight_df, db_file="fund.db"):
    """
    Met à jour la table Portfolios
    
    Paramètres :
      date_str     : Date de création du portefeuille
      risk_profile : Profil de risque du portefeuille 
      weight_df    : DataFrame contenant les poids des produits du portefeuille
      db_file      : Chemin vers la base de données SQLite (défaut "fund.db")
                      
    """

    produits_json = weight_df.to_json(orient="index")
    
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute("""INSERT INTO Portfolios (type, date_creation, produits)
        VALUES (?, ?, ?)""", (risk_profile, date_str,produits_json))
        conn.commit()


def update_deals(date_str, risk_profile, new_weight_df=None, db_file="fund.db"):
    """
    Met à jour la table Deals en calculant la différence entre les poids du nouveau portefeuille 
    et ceux du dernier portefeuille enregistré pour le même profil de risque

    Paramètres :
      date_str      : Date du deal
      risk_profile  : Profil de risque du portefeuille ('low_risk', 'low_turnover', 'high_yield_equity_only')
      new_weight_df : DataFrame contenant les nouveaux poids (index = product_id, colonne "weight").
                      Par défaut, None, ce qui signifie qu'il n'y a pas d'investissement
      db_file       : Chemin vers la base de données SQLite (défaut "fund.db")
    """
    
    with sqlite3.connect(db_file) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # 1. Recherche du dernier portefeuille avec le même profil de risque 
        cursor.execute("""
            SELECT produits 
            FROM Portfolios 
            WHERE type = ?
            ORDER BY date_creation DESC
            LIMIT 1
        """, (risk_profile,))
        row = cursor.fetchone()
        if row is None:
            old_weight_df = pd.DataFrame(columns=["weight"])
        else:
            old_json = row["produits"]
            if old_json:
                # Envelopper la chaîne JSON dans un objet StringIO
                old_weight_df = pd.read_json(StringIO(old_json), orient="index")
            else:
                old_weight_df = pd.DataFrame(columns=["weight"])
        
        # 2. Gestion de l'absence de nouveau portefeuille : new_weight_df est None
        if new_weight_df is None:
            diff_json = None
        else:
            new_weight_df = new_weight_df.copy()
            if "weight" not in new_weight_df.columns:
                print("La DataFrame new_weight_df doit contenir une colonne 'weight'.")
                return
            diff_df = new_weight_df.subtract(old_weight_df, fill_value=0)
            diff_json = diff_df.to_json(orient="index")
        
        # 3. Mise à jour de la table Deals pour la date donnée, dans la colonne correspondant à risk_profile
        cursor.execute("SELECT * FROM Deals WHERE date = ?", (date_str,))
        existing = cursor.fetchone()
        if existing:
            query = f"UPDATE Deals SET \"{risk_profile}\" = ? WHERE date = ?"
            cursor.execute(query, (diff_json, date_str))
        else:
            query = f"INSERT INTO Deals (date, \"{risk_profile}\") VALUES (?, ?)"
            cursor.execute(query, (date_str, diff_json))
            
        conn.commit()

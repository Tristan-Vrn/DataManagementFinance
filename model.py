import sqlite3
import pandas as pd
import numpy as np
import pickle
from sklearn.linear_model import LinearRegression

def fit_model(start_date, end_date, db_file="fund.db", window_size=10, model_path="model.pkl"):
    """
    Fonction qui entraîne un modèle de régression linéaire sur les rendements de la table Returns
    de start_date à end_date.

    Le modèle utilise une fenêtre glissante de 'window_size' observations comme features pour prédire le rendement suivant.

    Paramètres:
        start_date : date de début de la période de formation
        end_date   : date de fin de la période de formation
        db_file    : chemin vers la base de données
        window_size: nombre d'observations à utiliser pour la prédiction
        model_path : chemin de sauvegarde du modèle entraîné
    """
    # Récupération des rendements depuis la table Returns
    with sqlite3.connect(db_file) as conn:
        query = "SELECT product_id, date, value FROM Returns WHERE date BETWEEN ? AND ? ORDER BY date ASC"
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    df['date'] = pd.to_datetime(df['date'])
    
    # Génération des features (X) et cibles (y) à partir d'une fenêtre glissante pour chaque produit
    X_list = []
    y_list = []
    
    for ticker, group in df.groupby("product_id"):
        group = group.sort_values("date")
        values = group['value'].values
        if len(values) > window_size:
            # Pour chaque position possible, on prend window_size valeurs comme features et la valeur suivante comme cible
            for i in range(len(values) - window_size):
                feature_window = values[i:i+window_size]
                target = values[i+window_size]
                if np.isnan(feature_window).any() or np.isnan(target):
                    continue
                X_list.append(feature_window)
                y_list.append(target)
    
    if not X_list:
        print("Pas suffisamment de données pour entraîner le modèle.")
        return None
    
    X = np.array(X_list)
    y = np.array(y_list)
    
    # Nettoyage des données pour enlever les valeurs infinies ou trop grandes
    finite = np.all(np.isfinite(X), axis=1) & np.isfinite(y)
    X = X[finite]
    y = y[finite]
    
    if len(X) == 0:
        print("Aucune donnée valide après nettoyage.")
        return None
    
    # Entraînement du modèle de régression linéaire
    model = LinearRegression()
    model.fit(X, y)
    
    # Sauvegarde du modèle dans un fichier pickle
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    
    print("Le modèle a été entraîné et sauvegardé dans", model_path)
    return model

if __name__ == "__main__":
    fit_ml_model(start_date="2019-01-01", end_date="2022-12-31")
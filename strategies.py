import sqlite3
import pandas as pd
import numpy as np
import pickle
from scipy.optimize import minimize
from scipy.stats import kurtosis

class Strategies:

    def __init__(self, db_file="fund.db"):

        self.db_file = db_file

    # Fonction permettant de calculer les portefeuilles optimaux pour le profil "low_risk"
    # en minimisant la volatilité du portefeuille à 10% par an
    # Les contraintes de minimisation sont :
    # - La somme des poids doit être égale à 1
    # - La somme des poids attribués aux actifs de la catégorie "bond" doit être >= 0.6
    # - La vente à découvert est interdite

    def low_risk(self, target_volatility=0.10):

        # Chargement des returns historiques depuis la table Returns
        with sqlite3.connect(self.db_file) as conn:
            df = pd.read_sql_query("SELECT product_id, date, value FROM Returns ORDER BY date DESC", conn)
        df['date'] = pd.to_datetime(df['date'])



        # Pour chaque produit, on récupère les 252 dernières valeurs (les dates les plus récentes)
        series_list = []
        product_ids = []
        for prod_id, grp in df.groupby("product_id"):

            if len(grp) >= 252:

                # Récupération des 252 observations les plus récentes et remise en ordre chronologique
                s = grp.sort_values("date", ascending=False).head(252)
                s = s.sort_values("date", ascending=True)
                s = s['value'].reset_index(drop=True)
                series_list.append(s)
                product_ids.append(prod_id)

        if not series_list:
            print("Aucun actif avec 252 retours disponibles.")
            return None

        # Création d'un DataFrame où chaque colonne correspond aux 252 retours d'un actif
        returns_data = pd.concat(series_list, axis=1)
        returns_data.columns = product_ids

        # Calcul de la matrice de covariance des rendements et annualisation par 252 jours
        cov_matrix = returns_data.cov()

        # Fonction objectif : minimiser l'écart entre la volatilité annualisée du portefeuille et target_volatility
        def objective(weights):
            port_vol = np.sqrt(np.dot(weights.T, np.dot(cov_matrix * 252, weights)))
            return (port_vol - target_volatility) ** 2

        n = len(product_ids)
        # Contrainte : la somme des poids doit être égale à 1
        constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]

        # Chargement de la table Products pour définir le masque (bond)
        with sqlite3.connect(self.db_file) as conn:
            products_df = pd.read_sql_query("SELECT product_id, category FROM Products", conn)

        # Détection si l'actif est de la catégorie "bond"
        bond = []
        for prod_id in product_ids:
            row = products_df[products_df["product_id"] == prod_id]
            if not row.empty:
                is_bond = "bond" in row["category"].iloc[0].lower()
                bond.append(1.0 if is_bond else 0.0)
            else:
                bond.append(0.0)
        bond = np.array(bond)

        # Contrainte : la somme des poids attribués aux bonds doit être >= 0.6
        constraints.append({'type': 'ineq', 'fun': lambda w: np.dot(bond, w) - 0.6})

        # Bornes des poids : entre 0 et 1 (vente à découvert interdite)
        bounds = [(0, 1)] * n
        # Répartition initiale égale
        initial_guess = np.array([1/n] * n)
        result = minimize(objective, initial_guess, method='SLSQP', bounds=bounds, constraints=constraints)

        if not result.success:
            print("Échec de l'optimisation:", result.message)
            return None
        
        # Retourne un DataFrame avec les parts investies par actif
        return pd.DataFrame(result.x, index=product_ids, columns=["weight"])



######################################################################################################################
    # Fonction permettant de calculer les portefeuilles optimaux pour le profil "low_turnover" 
    # ici la vente à découverte est autoriséee

    # Pour obtenir les parts des actifs on les normalisent (return/|somme des returns|)

    def linear_strategy(self, target_date, window_size=10, model_path="model.pkl"):

        """
        Prédit le rendement suivant pour chaque actif en utilisant le modèle de régression linéaire pré-entraîné.
        Pour chaque actif, on extrait les dernières 'window_size' observations antérieures à target_date
        afin de construire le vecteur de features utilisé pour la prédiction.

        Paramètres:
            target_date : date cible à partir de laquelle on effectue la prédiction
            window_size : nombre d'observations (rendements) à utiliser comme entrée du modèle
            model_path  : chemin vers le fichier pickle contenant le modèle pré-entraîné
        """

        target_date = pd.to_datetime(target_date)

        # Chargement du modèle pré-entraîné
        try:
            with open(model_path, "rb") as f:
                model_path = pickle.load(f)
        except Exception as e:
            print("Erreur lors du chargement du modèle :", e)
            return None

        # Chargement des rendements depuis la table Returns
        with sqlite3.connect(self.db_file) as conn:
            df = pd.read_sql_query("SELECT product_id, date, value FROM Returns ORDER BY date DESC", conn)
        df['date'] = pd.to_datetime(df['date'])

        predictions = {}

        # Pour chaque actif, on récupère les window_size dernières valeurs antérieures à target_date
        for prod_id, group in df.groupby("product_id"):
            group = group[group['date'] < target_date]
            if len(group) < window_size:
                continue
            # Récupération des 'window_size' retours (les plus récents) et tri chronologique
            features = group.sort_values("date", ascending=False).head(window_size)
            features = features.sort_values("date", ascending=True)['value'].values
            features = features.reshape(1, -1)
            try:
                pred = model_path.predict(features)[0]
            except Exception as e:
                print(f"Erreur lors de la prédiction pour le produit {prod_id} :", e)
                continue
            predictions[prod_id] = pred

        if not predictions:
            print("Aucun actif avec suffisamment d'observations pour la prédiction.")
            return None
        
        # Calcul de la somme des valeurs absolues des rendements prédits
        total_pred = sum(abs(pred) for pred in predictions.values())
        mean_abs_pred = total_pred / len(predictions)   
        if total_pred == 0:
            print("La somme des valeurs absolues des rendements prédits est égale à zéro, impossible de normaliser.")
            return None
        # Calcul des parts à investir pour chaque actif : on conserve le signe de la prédiction
        if mean_abs_pred >0.0025:
            weights = {prod_id: pred / total_pred for prod_id, pred in predictions.items()}
            #print(mean_abs_pred)
        else:
            print("La moyenne des rendements prédits est trop faible pour investir")
            return None

        return pd.DataFrame.from_dict(weights, orient='index', columns=["weight"])

######################################################################################################################

    # Fonction permettant de calculer les portefeuilles optimaux pour le profil "high_yield"
    # en maximisant le rendement moyen sur les deux dernières semaines  
    # Les contraintes de maximisation sont :
    # - La somme des poids doit être égale à 1
    # - La vente à découvert est autorisée
    # - seuls les actifs de la catégorie "equity" sont considérés

    def high_yield(self, days=14):

        # Chargement de la table Products et récupération uniquement des produits "equity"
        with sqlite3.connect(self.db_file) as conn:
            products_df = pd.read_sql_query("SELECT product_id, category FROM Products", conn)
        products_df['category'] = products_df['category'].str.lower()
        equity_ids = products_df.loc[products_df['category'] == 'equity', 'product_id'].tolist()

        if not equity_ids:
            print("Aucun produit de catégorie equity trouvé.")
            return None

        # Chargement des rendements historiques pour les produits equity
        with sqlite3.connect(self.db_file) as conn:
            query = ("SELECT product_id, date, value FROM Returns WHERE product_id IN ("
                    + ",".join(map(str, equity_ids)) + ") ORDER BY date DESC")
            df = pd.read_sql_query(query, conn)
        df['date'] = pd.to_datetime(df['date'])

        # Pour chaque produit, on récupère les rendements des deux dernières semaines
        series_list = []
        product_ids = []

        for prod_id, group in df.groupby("product_id"):
            latest_date = group['date'].max()
            days_ago = latest_date - pd.Timedelta(days=days)
            group_days = group[group['date'] >= days_ago]
            if group_days.empty:
                continue
            s = group_days.sort_values("date", ascending=True)['value'].reset_index(drop=True)
            series_list.append(s)
            product_ids.append(prod_id)

        if not series_list:
            print("Aucun actif avec des retours sur les deux dernières semaines disponibles.")
            return None

        # Calcul de la moyenne des rendements pour chaque actif
        mu = np.array([s.mean() for s in series_list])
        n = len(mu) 

        # Objectif : maximiser le rendement moyen (équivalent à minimiser l'opposé)
        def objective(weights):
            return -np.dot(mu, weights)

        constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]
        bounds = [(-1, 1)] * n
        initial_guess = np.array([1/n] * n)

        result = minimize(objective, initial_guess, method='SLSQP', bounds=bounds,
                        constraints=constraints,
                        options={'maxiter': 5000, 'ftol': 1e-8, 'disp': False})

        if not result.success:
            print("Échec de l'optimisation:", result.message)
            return None

        return pd.DataFrame(result.x, index=product_ids, columns=["weight"])
import pandas as pd
from faker import Faker
import sqlite3
import json
import random
from dicoo import tickers_brut, full_categories_dict
fake = Faker()
db_file = "fund.db"
from datetime import date, timedelta
import yfinance as yf

# Création des tables dans l'ordre hiérarchique
def create_tables():
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Table Clients
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Clients (
            client_id INTEGER PRIMARY KEY AUTOINCREMENT,
            profil_risque TEXT,
            nom TEXT,
            prenom TEXT,
            date_naissance DATE,
            adresse TEXT,
            telephone TEXT,
            email TEXT,
            date_inscription DATE
        );""")
        
        # Table Portfolios avec client_id stockant une liste d'IDs en JSON
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Portfolios (
            portfolio_id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT CHECK(type IN ('low_risk', 'low_turnover', 'high_yield_equity_only')),
            date_creation DATE,
            produits TEXT DEFAULT '[]'
        );""")
                    
        # Table Managers
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Managers (
            manager_id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL,
            nom TEXT,
            prenom TEXT,
            date_naissance DATE,
            FOREIGN KEY (portfolio_id) REFERENCES Portfolios(portfolio_id)
        );""")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Products (
                product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                category TEXT);
                       """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Deals (
            deal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE,
            low_risk TEXT,
            low_turnover TEXT,  
            high_yield_equity_only TEXT        
        );""")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Returns (
            product_id INTEGER NOT NULL,
            date DATE,
            value REAL,
            FOREIGN KEY (product_id) REFERENCES Products(product_id) ON DELETE CASCADE
        );""")
        
        conn.commit()
        print("Tables créées avec succès.")

    except sqlite3.Error as e:
        print(f"Erreur SQLite lors de la création des tables : {e}")
    finally:
        if conn:
            conn.close()

# Création des portefeuillessous forme de JSON vide
def create_initial_portfolios():
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Insertion des 3 portefeuilles sous forme de liste JSON
        portfolios = [
            ('low_risk', '2020-01-01', None),
            ('low_turnover', '2020-01-01', None),
            ('high_yield_equity_only','2020-01-01', None)
        ]
        
        cursor.executemany("""
        INSERT INTO Portfolios (type, date_creation, produits)
        VALUES (?, ?, ?)""", portfolios)
        
        conn.commit()
        print("3 portefeuilles de base créés avec succès.")
    except sqlite3.Error as e:
        print(f"Erreur SQLite lors de la création des portefeuilles : {e}")
    finally:
        if conn:
            conn.close()

# Génération de clients avec un profil de risque assigné aléatoirement
def generate_clients(n: int = 10):
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        clients = []
        for _ in range(n):
            # Assigner aléatoirement un profil de risque
            profil_risque = random.choice(['low_risk', 'low_turnover', 'high_yield_equity_only'])
            
            # Générer les informations du client
            client_data = (
                profil_risque,
                fake.last_name(),
                fake.first_name(),
                fake.date_of_birth(minimum_age=25, maximum_age=65).strftime('%Y-%m-%d'),
                fake.address().replace("\n", ", "),
                fake.phone_number(),
                fake.email(),
                fake.date_this_decade().strftime('%Y-%m-%d')
            )
            clients.append(client_data)
            
            # Insertion du client dans la table Clients
            cursor.execute("""
            INSERT INTO Clients (profil_risque, nom, prenom, date_naissance, adresse, telephone, email, date_inscription)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, client_data)
      
        conn.commit()
        print(f"{n} clients générés avec un profil de risque assigné aléatoirement.")
    except sqlite3.Error as e:
        print(f"Erreur SQLite lors de la génération des clients : {e}")
    finally:
        if conn:
            conn.close()

# Génération de managers liés aux portefeuilles
def generate_managers(n: int = 5):
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Récupération des IDs de portefeuille
        cursor.execute("SELECT portfolio_id FROM Portfolios")
        portfolio_ids = [row[0] for row in cursor.fetchall()]
        
        managers = []
        for _ in range(n):
            managers.append((
                random.choice(portfolio_ids),
                fake.last_name(),
                fake.first_name(),
                fake.date_of_birth(minimum_age=25, maximum_age=55).strftime('%Y-%m-%d')
            ))
        
        cursor.executemany("""
        INSERT INTO Managers (portfolio_id, nom, prenom, date_naissance)
        VALUES (?, ?, ?, ?)""", managers)
        
        conn.commit()
        print(f"{n} managers générés avec succès.")
    except sqlite3.Error as e:
        print(f"Erreur SQLite lors de la génération des managers : {e}")
    finally:
        if conn:
            conn.close()
 
def generate_products():
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Insertion des produits
        products = [(k, v) for k, v in full_categories_dict.items()]
        
        cursor.executemany("""
        INSERT INTO Products (ticker, category)
        VALUES (?, ?)""", products)
        
        conn.commit()
        print("Produits ajoutés avec succès.")
    except sqlite3.Error as e:
        print(f"Erreur SQLite lors de l'ajout des produits : {e}")
    finally:
        if conn:
            conn.close()

# Exécution du script
if __name__ == "__main__":
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
   
    cursor.execute("DROP TABLE IF EXISTS Clients;")
    cursor.execute("DROP TABLE IF EXISTS Portfolios;")
    cursor.execute("DROP TABLE IF EXISTS Managers;")
    cursor.execute("DROP TABLE IF EXISTS Products;")
    conn.commit()
    
    create_tables()
    create_initial_portfolios()
    generate_clients(10)  
    generate_managers()
    generate_products()
    conn.close()
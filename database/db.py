import sqlite3
import pandas as pd
import numpy as np
import json

#importo csv e creo due datframe in base al colore del vino
df_red_wine=pd.read_csv('https://raw.githubusercontent.com/FabioGagliardiIts/datasets/main/wine_quality/winequality-red.csv', sep=';', encoding='latin1')
df_white_wine=pd.read_csv('https://raw.githubusercontent.com/FabioGagliardiIts/datasets/main/wine_quality/winequality-white.csv', sep= ';', encoding='latin1')

#aggiungo in entrambi i dataframe una colonna per il colore del vino 
df_red_wine.insert(0,'type','red')
df_white_wine.insert(0,'type','white')

#unisco i due dataframe tramite la concatenazione 
wine=[df_red_wine,df_white_wine]
df_wine=pd.concat(wine)
df_wine.insert(0, "id", range(0, 0+len(df_wine)))

#rinomino le colonne
df_wine=df_wine.rename(columns={ 'fixed acidity':'fixed_acidity',
                                 'volatile acidity':'volatile_acidity',
                                 'citric acid':'citric_acid',
                                 'residual sugar':'residual_sugar',
                                 'free sulfur dioxide': 'free_sulfur_dioxide',
                                 'total sulfur dioxide': 'total_sulfur_dioxide',})

#creo db
db = sqlite3.connect('database/sqlite/db.sqlite')
cursor = db.cursor()
db.commit()

#creo una tabella a partire dal dataframe df_wine
df_wine.to_sql('wine', db, if_exists='replace', index=False)
db.commit()

##########
#        #
#  CRUD  #
#        # 
##########


#CREATE
def create_wine(wine:wine):
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO wine(id, type, fixed_acidity, volatile_acidity, critic_acid, residual_sugar, clhorides, free_sulfur_dioxide, total_sulfur_dioxide, density, ph, sulphates, alcohol, quality)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
    (wine.id, wine.type, wine.fixed_acidity, wine.volatile_acidity, wine.critic_acid, wine.resiudal_sugar, wine.clhorides, wine.free_sulfur_dioxide, wine.total_sulfur_dioxide, wine.density, wine.ph, wine.sulphates, wine.alcohol, wine.quality))
    connection.commit()
    connection.close()

#creo la connesione con il database
def create_connection():
    connection = sqlite3.connect('database/sqlite/db.sqlite')
    return connection

###############################################################

#READ (visualizzare i dati presenti nel db)
def read_wines():
    try:
        connection = create_connection()
        cursor = connection.cursor()
        query = "SELECT * FROM wine"
        cursor.execute(query,)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        result = []
        for row in rows:
            wine_data = dict(zip(column_names, row))
            result.append(wine_data)

        return result
    #gestione delle eccezioni
    except Exception as e:
        print(f"Errore lettura dati: {str(e)}")
        return None
    #chiusura connesione con il db
    finally:
        if connection:
            connection.close()

###############################################################

#READ filtrare i dati per type (tipologia dei vini)
def read_wines_by_type(type: str):
    try:
        connection = create_connection()
        cursor = connection.cursor()
        query = "SELECT * FROM wine WHERE type = ?"
        cursor.execute (query, (type,))
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        result = []
        for row in rows:
            wine_data = dict(zip(column_names, row))
            result.append(wine_data)

            return result
    #gestione delle eccezioni 
    except Exception as e:
        print(f"Errore lettura dati: {str(e)}")
        return None
    #chiusura connessione con il db
    finally:
        if connection:
            connection.close()

###############################################################

#READ filtrando i vini per qualità e calcolandone la media 
def read_mean_wine_by_quality():
    try:
        connection = create_connection()
        cursor = connection.cursor()
        query = "SELECT quality FROM wine"
        cursor.execute(query,)
        result = [value[0] for value in cursor.fetchall()]
        connection.close()

        if result:
            mean_quality = np.mean(result)
            return mean_quality
        else:
            print("Nessun dato trovato.")
            return None
    #gestione delle eccezioni 
    except Exception as e:
     print(f"Errore lettura dati: {str(e)}")
     return None
    
############################################################### 

#READ filtrando i vini per tipo e calcolando la media della qualità
def read_mean_wines_quality_by_type(type:str):
    try:
        connection = create_connection()
        cursor = connection.cursor()
        query = "SELECT quality FROM wine WHERE type = ?"
        cursor.execute(query,(type,))
        result = [value[0] for value in cursor.fetchall()]
        connection.close()

        if result:
            mean_quality = np.mean(result)
            return mean_quality
        else:
            print("Nessun dato trovato.")
            return None
    #gestione delle eccezioni 
    except Exception as e:
     print(f"Errore lettura dati: {str(e)}")
     return None

###############################################################    
    
#READ calcola la media dei vini filtrati per ph
def read_mean_wines_quality_by_ph(ph:float):
    try:
        connection = create_connection()
        cursor = connection.cursor()
        query = "SELECT quality FROM wine WHERE ph = ?"
        cursor.execute(query,(ph,))
        result = [value[0] for value in cursor.fetchall()]
        connection.close()

        if result:
            mean_quality = np.mean(result)
            return mean_quality
        else:
            print("Nessun dato trovato con questo valore di ph.")
            return None
    #gestione delle eccezioni 
    except Exception as e:
     print(f"Errore lettura dati: {str(e)}")
     return None
    
###############################################################

#READ vini superiori ad un determinato ph
def read_wines_by_ph(ph:float):
    try:
        connection = create_connection()
        cursor = connection.cursor()
        query = "SELECT * FROM wine WHERE ph >?"
        cursor.execute(query,ph,)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        result = []
        for row in rows:
            wine_data = dict(zip(column_names, row))
            result.append(wine_data)

        return result
    #chiusura connesione con il db
    finally:
        if connection:
            connection.close()

###############################################################

#READ vini superiori ad un determinato valore di alcohol
def read_wines_by_alcohol(alcohol:float):
    try:
        connection = create_connection()
        cursor = connection.cursor()
        query = "SELECT * FROM wine WHERE alcohol >?"
        cursor.execute(query,alcohol,)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        result = []
        for row in rows:
            wine_data = dict(zip(column_names, row))
            result.append(wine_data)

        return result
    #chiusura connesione con il db
    finally:
        if connection:
            connection.close()

###############################################################

#DELETE elimina un record passando come valore l'id
def delete_wines_by_id(id:int):
    try:
        connection = create_connection()
        cursor = connection.cursor()
        query = "DELETE FROM wine WHERE id = ?"
        cursor.execute(query, (id,))
        connection.commit()
        connection.close()

        print(f"Vino con ID {id} eliminato con successo.")
    #gestione delle eccezioni
    except Exception as e:
        print(f"Errore durante l'eliminazione del vino: {str(e)}")


        




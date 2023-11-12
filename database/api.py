
#importo le libreire necessarie per svolgere le API
import numpy as np
from fastapi import FastAPI
#importo il database e il model presenti nei file db. e model.
from database.db import *
from database.model import *

# uvicorn database:app --reload --> x avviare le api
# http://127.0.0.1:8000/docs. Swagger


#creo app
app = FastAPI()

#API post che crea un record da inserire nel db
@app.post("/addWine")
def new_wine(wine:Wine):
    newWine=create_wine(wine)
    return{"id":newWine, **wine.model_dump()}


#API get che chiama la funzione read_wines per leggere tutti i vini presenti nel database
@app.get("/wine")
def read_wine_endpoint():
    return read_wines()

#API get che chiama la funzione read_wines_by_type per filtra i vini per tipo(bianco/rosso)
@app.get("/wine/{type}")
def read_wines_by_type_endpoint(type:str):
    return read_wines_by_type(type)

#API get cehchiama la funzione read_wines_by_ph che restituisce tutti i vini superiori ad un determinato valore di ph (valore scelto dall'utente)
@app.get("/wine/{ph}")
def read_wines_by_ph_endpoint(ph:float):
    return read_wines_by_ph(ph)

#API get che chiama la funzione read_wines_by_alcohol per restituire tutti i vini superiori ad un determinato valore di alcohol (valore scelto dall'utente)
@app.get("/wine/{alcohol}")
def read_wines_by_alcohol_endpoint(alcohol:float):
    return read_wines_by_alcohol(alcohol)

#API get che richiama la funzione read_mean_wines_by_quality che restituisce la media della qualità dei vini
@app.get("/quality")
def read_mean_wines_by_quality_endpoint():
    return read_mean_wine_by_quality

#API get che richiama la funzione read_mean_quality_by_type che restituisce la media della qualità dei vini filtrati per tipo
@app.get("/quality/{type}")
def read_mean_wines_quality_by_type_endpoint(type):
    return read_mean_wines_quality_by_type(type)


#API delete che richiama la funzione delete_wines_by_id che permette di eliminare un record passand come paramentro l'id
@app.delete("/deleteWine/{id}")
def delete_wines_by_id_endpoint(id:int):
    delete_wines_by_id(id)
    return{"message": "Dato eliminato"}
#importo le librerie necessarie
from pydantic import BaseModel
import pickle
from sklearn.linear_model import LinearRegression
from fastapi import FastAPI, Request


#creo model per poter fare delle api POST
class Wine(BaseModel):
    id:int
    type:str
    fixed_acidity: float
    volatile_acidity: float
    citric_acid: float
    residual_sugar: float
    chlorides: float
    free_sulfur_dioxide: float   
    total_sulfur_dioxide: float
    density: float
    pH: float
    sulphates: float
    alcohol: float
    quality: int



app = FastAPI()

#ho caricato il training model
model = pickle.load(open(r"C:\Users\sebas\OneDrive\Desktop\Exam_python\training", "rb"))

app = FastAPI()

#ho creato l'API post 
@app.post("/predict")
async def predict(request: Request, wine_quality_prediction_input: Wine):
    # Non è necessario chiamare await request.json() se wine_quality_prediction_input è già un oggetto JSON
    prediction = model.predict([wine_quality_prediction_input])
    return {"prediction": prediction}

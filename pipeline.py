import requests
import schedule
import time
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import threading
from flask import Flask

# =========================
# CARGAR VARIABLES .ENV
# =========================
load_dotenv()
API_KEY = os.getenv("WEATHER_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")

# =========================
# CONEXIÓN A MONGODB ATLAS
# =========================
client = MongoClient(MONGO_URI)
db = client["Clima"]
collection = db["ciudad"]

# =========================
# LISTA DE CIUDADES EN COLOMBIA
# =========================
CIUDADES_CO = [
    "Bogota", "Medellin", "Cali", "Barranquilla", "Cartagena",
    "Bucaramanga", "Pereira", "Cucuta", "Manizales", "Santa Marta",
    "Armenia", "Popayan", "Monteria", "Neiva", "Villavicencio"
]
indice_ciudad = 0  # índice global

# =========================
# FUNCIÓN PRINCIPAL DE PIPELINE
# =========================
def obtener_datos_clima():
    global indice_ciudad

    # Seleccionar ciudad actual
    ciudad_actual = CIUDADES_CO[indice_ciudad]

    try:
        print(f"\n[{datetime.now()}] 🔄 Consultando clima de {ciudad_actual}...")

        URL = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={ciudad_actual},Colombia&aqi=no"
        response = requests.get(URL)
        data = response.json()

        location = data["location"]
        current = data["current"]

        registro = {
            "fecha_insercion": datetime.now(),
            "ubicacion": {
                "ciudad": location["name"],
                "region": location["region"],
                "pais": location["country"],
                "lat": location["lat"],
                "lon": location["lon"],
                "hora_local": location["localtime"]
            },
            "clima": {
                "ultima_actualizacion": current["last_updated"],
                "temperatura_c": current["temp_c"],
                "sensacion_c": current["feelslike_c"],
                "humedad": current["humidity"],
                "presion_mb": current["pressure_mb"],
                "viento_kph": current["wind_kph"],
                "direccion_viento": current["wind_dir"],
                "precipitacion_mm": current["precip_mm"],
                "nubes": current["cloud"],
                "uv": current["uv"],
                "condicion": current["condition"]["text"],
                "icono": "https:" + current["condition"]["icon"]
            }
        }

        collection.insert_one(registro)

        print(f"✅ Guardado ({ciudad_actual}) | Temp: {registro['clima']['temperatura_c']}°C | "
              f"Humedad: {registro['clima']['humedad']}% | "
              f"Condición: {registro['clima']['condicion']}")

    except Exception as e:
        print(f"❌ Error al obtener o guardar datos de {ciudad_actual}: {e}")

    # Avanzar a la siguiente ciudad (cíclico)
    indice_ciudad = (indice_ciudad + 1) % len(CIUDADES_CO)


# =========================
# FUNCIÓN PARA EJECUTAR EL PIPELINE DE FORMA CONTINUA
# =========================
def run_pipeline():
    schedule.every(10).seconds.do(obtener_datos_clima)
    print("🚀 Pipeline de WeatherAPI iniciado... (captura cada 10 segundos y cambia de ciudad)")

    while True:
        schedule.run_pending()
        time.sleep(0.2)

# =========================
# SERVIDOR FLASK (NECESARIO PARA RENDER)
# =========================
app = Flask(__name__)

@app.route('/')
def home():
    return "☁️ Weather pipeline is running successfully and rotating cities across Colombia!"

# Ejecuta el pipeline en un hilo separado
threading.Thread(target=run_pipeline, daemon=True).start()

if __name__ == '__main__':
    # Render necesita que se exponga un puerto (por defecto usa el 8080)
    app.run(host='0.0.0.0', port=8080)

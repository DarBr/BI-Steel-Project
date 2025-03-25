import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Verzeichnis des aktuellen Skripts
data_path = os.path.join(BASE_DIR, "/energy_prices_data.csv")
model_path = os.path.join(BASE_DIR, "/energy_price_model.h5")

print(data_path)
print(model_path)
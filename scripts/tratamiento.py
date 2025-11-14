import pandas as pd
import os

def parse_amazon_old_format(filename):
    entry = {}
    with open(filename, 'r', encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                if entry:
                    yield entry
                    entry = {}
                continue

            colon = line.find(':')
            if colon != -1:
                key = line[:colon]
                value = line[colon+2:]
                entry[key] = value

        if entry:
            yield entry

# ESTE DATASET ES SUPER PEQUEÑO PARA LAS PRUEBAS 
# LUEGO CAMBAIMOS AQUI LA RUTA SEGUN EL NUEVO DATASET 
# EL CODIGO ES EL QUE BRINDA AMAZON PARA EL PARSEO 
# Y LLEVARLO A TABLAS

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ruta = os.path.join(BASE_DIR, "local_data", "Arts.txt")

print("Leyendo archivo desde:", ruta)

reviews = list(parse_amazon_old_format(ruta))
df = pd.DataFrame(reviews)

print(df.head())
print("\nTotal de registros:", len(df))

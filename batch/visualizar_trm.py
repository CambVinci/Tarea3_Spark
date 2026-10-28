import csv
from datetime import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

archivo = "/tmp/rows.csv"
carpeta = Path("/home/hadoop/Tarea3_Spark/resultados")
carpeta.mkdir(parents=True, exist_ok=True)

datos = []

with open(archivo, "r", encoding="utf-8-sig", newline="") as f:
    lector = csv.DictReader(f)

    for fila in lector:
        try:
            if fila["UNIDAD"].strip() != "COP":
                continue

            valor = float(fila["VALOR"])
            fecha = datetime.strptime(
                fila["VIGENCIADESDE"].strip(), "%d/%m/%Y"
            )

            if valor > 0:
                datos.append((fecha, valor))
        except (ValueError, KeyError):
            continue

if not datos:
    raise SystemExit("No se encontraron registros válidos.")

datos.sort(key=lambda x: x[0])
fechas = [dato[0] for dato in datos]
valores = [dato[1] for dato in datos]

# Gráfica 1: evolución histórica
plt.figure(figsize=(12, 6))
plt.plot(fechas, valores, linewidth=1)
plt.title("Evolución histórica de la TRM en Colombia")
plt.xlabel("Fecha")
plt.ylabel("TRM (COP por USD)")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(carpeta / "trm_evolucion_temporal.png", dpi=150)
plt.close()

# Gráfica 2: distribución de los valores
plt.figure(figsize=(10, 6))
plt.hist(valores, bins=30, edgecolor="black")
plt.title("Distribución histórica de la TRM")
plt.xlabel("TRM (COP por USD)")
plt.ylabel("Frecuencia")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()
plt.savefig(carpeta / "trm_distribucion.png", dpi=150)
plt.close()

print(f"Registros graficados: {len(datos)}")
print(f"Fecha inicial: {fechas[0].strftime('%d/%m/%Y')}")
print(f"Fecha final: {fechas[-1].strftime('%d/%m/%Y')}")
print(f"TRM mínima: {min(valores):.2f} COP")
print(f"TRM máxima: {max(valores):.2f} COP")
print(f"Promedio: {sum(valores) / len(valores):.2f} COP")
print("Gráficas guardadas en:", carpeta)

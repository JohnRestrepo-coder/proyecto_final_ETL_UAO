# ============================================================
# PROYECTO FINAL - ETL + Analítica de Negocios
# Colombianos detenidos en el exterior
# Autor: John Restrepo Aparicio
# ============================================================

import pandas as pd
import requests
from io import StringIO
import matplotlib.pyplot as plt
import schedule
import time
from datetime import datetime
import os
import unicodedata

# ============================================================
# 1️⃣ EXTRACCIÓN - Descarga desde la API (con paginación)
# ============================================================

def descargar_datos_completos():
    print("⏳ Descargando datos completos desde la API de Datos Abiertos Colombia...")
    
    base_url = "https://www.datos.gov.co/resource/e97j-vuf7.csv"
    all_data = []
    limit = 50000  # máximo permitido por la API
    offset = 0

    while True:
        params = {"$limit": limit, "$offset": offset}
        response = requests.get(base_url, params=params)

        if response.status_code != 200:
            print(f"❌ Error en la descarga ({response.status_code}). Deteniendo proceso.")
            break

        df_temp = pd.read_csv(StringIO(response.text), encoding="latin1")

        if df_temp.empty:
            break

        all_data.append(df_temp)
        offset += limit
        print(f"   → {offset} registros descargados...")

    df = pd.concat(all_data, ignore_index=True)
    print(f"✅ Descarga completa: {len(df)} registros obtenidos.\n")
    return df


# ============================================================
# 2️⃣ TRANSFORMACIÓN Y LIMPIEZA
# ============================================================

def limpiar_datos(df):
    print("🧹 Iniciando limpieza del dataset...")

    # Corregir texto mal codificado
    for col in df.select_dtypes(include='object').columns:
        try:
            df[col] = df[col].apply(
                lambda x: x.encode('latin1').decode('utf-8') if isinstance(x, str) else x
            )
        except Exception:
            pass

    # Eliminar filas vacías y duplicadas
    df = df.dropna(how='all')
    df = df.drop_duplicates()

    # Reemplazar valores vacíos comunes
    df = df.replace({'N/A': None, 'n/a': None, '': None})

    # --- Normalizar nombres de columnas ---
    def normalizar_columna(nombre):
        nfkd_form = unicodedata.normalize('NFKD', nombre)
        solo_ascii = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
        return solo_ascii.lower().strip().replace(" ", "_")

    df.columns = [normalizar_columna(c) for c in df.columns]

    # Formatear fechas si existe la columna de publicación
    if 'fecha_publicacion' in df.columns:
        df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], errors='coerce')
        meses = {
            1: 'enero', 2: 'febrero', 3: 'marzo', 4: 'abril', 5: 'mayo', 6: 'junio',
            7: 'julio', 8: 'agosto', 9: 'septiembre', 10: 'octubre', 11: 'noviembre', 12: 'diciembre'
        }
        df['fecha_texto'] = df['fecha_publicacion'].apply(
            lambda x: f"{x.day} de {meses[x.month]} de {x.year}" if pd.notnull(x) else None
        )

    print(f"📋 Columnas normalizadas: {df.columns.tolist()}")
    print("✅ Limpieza completada.\n")
    return df


# ============================================================
# 3️⃣ VISUALIZACIONES (muestran y guardan resultados)
# ============================================================

def generar_visualizaciones(df, rapido=True):
    print("📊 Generando visualizaciones...")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "graficos")
    os.makedirs(output_dir, exist_ok=True)

    if rapido and len(df) > 50000:
        df = df.sample(30000, random_state=42)
        print("⚡ Modo rápido activado: muestra de 30,000 filas.\n")

    plt.rcParams.update({
        "figure.dpi": 100,
        "axes.titlesize": 12,
        "axes.labelsize": 10
    })

    # 1️⃣ Top 10 países con más detenciones
    if 'pais' in df.columns:
        top_paises = df['pais'].value_counts().head(10)
        if not top_paises.empty:
            plt.figure(figsize=(10, 5))
            top_paises.plot(kind='bar', color='skyblue')
            plt.title("Top 10 países con más colombianos detenidos")
            plt.xlabel("País")
            plt.ylabel("Número de detenciones")
            plt.tight_layout()
            ruta = os.path.join(output_dir, "top_paises.png")
            plt.savefig(ruta)
            plt.show()
            plt.close()
            print(f"✅ Gráfico 'Top Países' guardado en: {ruta}")
        else:
            print("⚠️ No hay datos válidos en 'pais'.")
    else:
        print("⚠️ No se encontró la columna 'pais'.")

    # 2️⃣ Distribución por género
    if 'genero' in df.columns:
        genero_counts = df['genero'].value_counts()
        if not genero_counts.empty:
            plt.figure(figsize=(5, 5))
            genero_counts.plot(kind='pie', autopct='%1.1f%%', startangle=90, colors=['#ffb347', '#77dd77'])
            plt.title("Distribución por género")
            plt.ylabel("")
            plt.tight_layout()
            ruta = os.path.join(output_dir, "distribucion_genero.png")
            plt.savefig(ruta)
            plt.show()
            plt.close()
            print(f"✅ Gráfico 'Distribución Género' guardado en: {ruta}")
        else:
            print("⚠️ No hay datos válidos en 'genero'.")
    else:
        print("⚠️ No se encontró la columna 'genero'.")

    # 3️⃣ Evolución anual de detenciones
    if 'fecha_publicacion' in df.columns:
        df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'], errors='coerce')
        if df['fecha_publicacion'].notnull().sum() > 0:
            df['anio'] = df['fecha_publicacion'].dt.year
            casos_anuales = df['anio'].value_counts().sort_index()
            plt.figure(figsize=(10, 5))
            casos_anuales.plot(kind='line', marker='o', color='coral')
            plt.title("Evolución anual de detenciones")
            plt.xlabel("Año")
            plt.ylabel("Número de casos")
            plt.tight_layout()
            ruta = os.path.join(output_dir, "evolucion_anual.png")
            plt.savefig(ruta)
            plt.show()
            plt.close()
            print(f"✅ Gráfico 'Evolución Anual' guardado en: {ruta}")
        else:
            print("⚠️ No hay fechas válidas en 'fecha_publicacion'.")
    else:
        print("⚠️ No se encontró la columna 'fecha_publicacion'.")

    print(f"🎨 Visualizaciones guardadas en: {output_dir}\n")


# ============================================================
# 4️⃣ PIPELINE COMPLETO
# ============================================================

def ejecutar_pipeline(rapido=True):
    print(f"🚀 Ejecutando pipeline ETL ({'modo rápido' if rapido else 'modo completo'})...\n")
    df = descargar_datos_completos()
    df_limpio = limpiar_datos(df)

    salida = os.path.join(os.getcwd(), "colombianos_detenidos_limpio.csv")
    df_limpio.to_csv(salida, index=False, encoding='utf-8-sig')

    print(f"💾 Archivo limpio guardado en: {salida}")
    print(f"Total de registros finales: {len(df_limpio)}\n")

    generar_visualizaciones(df_limpio, rapido)
    print("🎯 Pipeline completado exitosamente.\n")


# ============================================================
# 5️⃣ PREFETCH Y AUTOMATIZACIÓN MENSUAL
# ============================================================

def prefetch_mensual():
    salida = os.path.join(os.getcwd(), "colombianos_detenidos_limpio.csv")
    
    if not os.path.exists(salida):
        print("📥 No se encontró archivo previo. Descargando por primera vez...")
        ejecutar_pipeline(rapido=False)
        return
    
    ultima_modificacion = datetime.fromtimestamp(os.path.getmtime(salida))
    dias_desde_actualizacion = (datetime.now() - ultima_modificacion).days

    if dias_desde_actualizacion >= 30:
        print(f"📆 Han pasado {dias_desde_actualizacion} días desde la última actualización. Prefetch activado.")
        ejecutar_pipeline(rapido=False)
    else:
        print(f"✅ El archivo está actualizado ({dias_desde_actualizacion} días desde la última descarga).")


# ============================================================
# 6️⃣ EJECUCIÓN PRINCIPAL
# ============================================================

if __name__ == "__main__":
    print("🕒 Iniciando ejecución del pipeline ETL con prefetch mensual...\n")

    prefetch_mensual()
    ejecutar_pipeline(rapido=True)

    # Si se dese correr de forma automática,quitar comemnts
    # schedule.every(30).days.do(prefetch_mensual)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(3600)

    print("✅ Proceso finalizado. Archivos y gráficos generados correctamente.")

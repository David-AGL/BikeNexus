"""
Dashboard de alertas — BikeNexus (RF-07, RNF-02)
================================================

Qué muestra:
  - La lista de estaciones VACIAS, LLENAS o EN_RIESGO, ordenada por el tiempo
    estimado que les queda para quedar vacias o llenas (de menor a mayor),
    para que los operadores decidan a cuales enviar camiones primero (RF-07).
  - Marca como DESACTUALIZADA cualquier estacion sin reportes en mas de 2 min,
    comparando su ultimo reporte con la hora actual (RNF-02).

De donde lee:
  De la tabla DynamoDB "bikenexus-estado-estaciones", usando el indice
  secundario (GSI) "gsi-clasificacion-tiempo" para traer cada categoria ya
  ordenada por urgencia, SIN escanear toda la tabla.

Como se ejecuta:
  pip install -r requirements.txt
  streamlit run dashboard.py

  Variables de entorno (opcionales):
    DYNAMODB_TABLA  (default: bikenexus-estado-estaciones)
    AWS_REGION      (default: us-east-1)
"""

import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import streamlit as st

TABLA = os.environ.get("DYNAMODB_TABLA", "bikenexus-estado-estaciones")
REGION = os.environ.get("AWS_REGION", "us-east-1")
# Endpoint opcional: para pruebas locales con LocalStack usar
# DYNAMODB_ENDPOINT=http://localhost:4566. En AWS real se deja vacio.
ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT") or None
GSI = "gsi-clasificacion-tiempo"

# Minutos sin reportar tras los cuales una estacion se marca desactualizada (RNF-02).
UMBRAL_DESACTUALIZADA_MIN = 2

# Categorias que son ALERTA (se muestran en el dashboard). NORMAL no es alerta.
CATEGORIAS_ALERTA = ["VACIA", "LLENA", "EN_RIESGO"]


@st.cache_resource
def _tabla_dynamodb():
    """Crea el recurso de DynamoDB una sola vez (AWS real o LocalStack)."""
    return boto3.resource(
        "dynamodb", region_name=REGION, endpoint_url=ENDPOINT
    ).Table(TABLA)


def consultar_categoria(clasificacion: str) -> list[dict]:
    """
    Trae todas las estaciones de una categoria, YA ORDENADAS por
    min_estimado_restante (ascendente) gracias al GSI. Esto evita escanear
    toda la tabla: es una Query sobre el indice (RF-07).
    """
    tabla = _tabla_dynamodb()
    resp = tabla.query(
        IndexName=GSI,
        KeyConditionExpression=Key("clasificacion").eq(clasificacion),
        ScanIndexForward=True,  # orden ascendente por el sort key (tiempo restante)
    )
    return resp.get("Items", [])


def minutos_desde(iso_ts: str) -> float | None:
    """Minutos transcurridos desde un timestamp ISO hasta ahora (RNF-02)."""
    if not iso_ts:
        return None
    try:
        t = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        ahora = datetime.now(timezone.utc)
        return (ahora - t).total_seconds() / 60.0
    except (ValueError, TypeError):
        return None


def construir_tabla_alertas() -> pd.DataFrame:
    """Consulta las categorias de alerta y arma un DataFrame para mostrar."""
    filas = []
    for categoria in CATEGORIAS_ALERTA:
        for item in consultar_categoria(categoria):
            min_rest = float(item.get("min_estimado_restante", 0))
            antiguedad = minutos_desde(item.get("ultimo_reporte", ""))
            desactualizada = (
                antiguedad is not None and antiguedad > UMBRAL_DESACTUALIZADA_MIN
            )
            filas.append({
                "Estación": item.get("estacion_id"),
                "Estado": categoria,
                "Min. restantes": None if min_rest >= 999999 else round(min_rest, 1),
                "Bicis": int(item.get("bikes_disponibles", 0)),
                "Docks": int(item.get("docks_libres", 0)),
                "Capacidad": int(item.get("capacidad", 0)),
                "Último reporte": item.get("ultimo_reporte", ""),
                "Desactualizada": "⚠️ SÍ" if desactualizada else "No",
            })

    df = pd.DataFrame(filas)
    if not df.empty:
        # Orden global por urgencia: menor tiempo restante primero (RF-07).
        # Las que no tienen estimacion (None) van al final.
        df = df.sort_values(
            by="Min. restantes", ascending=True, na_position="last"
        ).reset_index(drop=True)
    return df


# --- Interfaz Streamlit -------------------------------------------------------
def main():
    st.set_page_config(page_title="BikeNexus — Alertas", page_icon="🚲", layout="wide")
    st.title("🚲 BikeNexus — Dashboard de alertas")
    st.caption(
        "Estaciones vacías, llenas o en riesgo, ordenadas por urgencia. "
        "Fuente: DynamoDB (RF-07, RNF-02)."
    )

    # Auto-refresco: el operador ve el estado casi en vivo.
    with st.sidebar:
        st.header("Opciones")
        refrescar = st.button("🔄 Refrescar ahora")
        st.write(f"Tabla: `{TABLA}`")
        st.write(f"Región: `{REGION}`")

    try:
        df = construir_tabla_alertas()
    except Exception as e:  # noqa: BLE001
        st.error(f"No se pudo consultar DynamoDB: {e}")
        st.info("Verifica el nombre de la tabla, la región y los permisos.")
        return

    if df.empty:
        st.success("✅ No hay estaciones en alerta en este momento.")
        return

    # Métricas rápidas arriba.
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total en alerta", len(df))
    col2.metric("Vacías", int((df["Estado"] == "VACIA").sum()))
    col3.metric("Llenas", int((df["Estado"] == "LLENA").sum()))
    col4.metric("En riesgo", int((df["Estado"] == "EN_RIESGO").sum()))

    # Colorea las filas segun el estado para lectura rapida.
    def _color_estado(fila):
        colores = {
            "VACIA": "background-color: #ffd6d6",      # rojo claro
            "LLENA": "background-color: #d6e4ff",      # azul claro
            "EN_RIESGO": "background-color: #fff3cd",  # amarillo
        }
        return [colores.get(fila["Estado"], "")] * len(fila)

    st.subheader("Estaciones ordenadas por urgencia")
    st.dataframe(
        df.style.apply(_color_estado, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        f"⚠️ 'Desactualizada' = sin reportes en más de "
        f"{UMBRAL_DESACTUALIZADA_MIN} min (consistencia eventual, RNF-02)."
    )


if __name__ == "__main__":
    main()

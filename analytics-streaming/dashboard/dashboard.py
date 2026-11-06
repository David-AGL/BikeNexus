"""
Dashboard de alertas — BikeNexus (RF-07, RNF-02)
================================================

Panel para los OPERADORES: muestra qué estaciones están vacías, llenas o en
riesgo, ordenadas por urgencia, para decidir a cuáles enviar camiones primero
(RF-07). Marca como desactualizada cualquier estación sin reportes en >2 min
(RNF-02).

Lee de la tabla DynamoDB usando el índice secundario (GSI) para traer cada
categoría ya ordenada por urgencia, sin escanear toda la tabla.

Cómo se ejecuta:
  pip install -r requirements.txt
  streamlit run dashboard.py

  Variables de entorno:
    DYNAMODB_TABLA     (default: bikenexus-estado-estaciones)
    AWS_REGION         (default: us-east-1)
    DYNAMODB_ENDPOINT  (para LocalStack: http://localhost:4566; en AWS se deja vacío)
"""

import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import streamlit as st

TABLA = os.environ.get("DYNAMODB_TABLA", "bikenexus-estado-estaciones")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT") or None
GSI = "gsi-clasificacion-tiempo"

UMBRAL_DESACTUALIZADA_MIN = 2
CATEGORIAS_ALERTA = ["VACIA", "LLENA", "EN_RIESGO"]
TODAS_CATEGORIAS = ["VACIA", "LLENA", "EN_RIESGO", "NORMAL"]

# Colores por estado (consistentes en toda la app).
COLOR = {
    "VACIA": "#e74c3c",      # rojo
    "LLENA": "#3498db",      # azul
    "EN_RIESGO": "#f39c12",  # naranja
    "NORMAL": "#2ecc71",     # verde
}
EMOJI = {"VACIA": "🔴", "LLENA": "🔵", "EN_RIESGO": "🟠", "NORMAL": "🟢"}


@st.cache_resource
def _tabla_dynamodb():
    return boto3.resource(
        "dynamodb", region_name=REGION, endpoint_url=ENDPOINT
    ).Table(TABLA)


def consultar_categoria(clasificacion: str) -> list[dict]:
    """Trae las estaciones de una categoría, ordenadas por urgencia (GSI)."""
    tabla = _tabla_dynamodb()
    resp = tabla.query(
        IndexName=GSI,
        KeyConditionExpression=Key("clasificacion").eq(clasificacion),
        ScanIndexForward=True,
    )
    items = resp.get("Items", [])
    # Paginar por si hay muchas estaciones.
    while "LastEvaluatedKey" in resp:
        resp = tabla.query(
            IndexName=GSI,
            KeyConditionExpression=Key("clasificacion").eq(clasificacion),
            ScanIndexForward=True,
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))
    return items


def minutos_desde(iso_ts: str):
    if not iso_ts:
        return None
    try:
        t = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - t).total_seconds() / 60.0
    except (ValueError, TypeError):
        return None


@st.cache_data(ttl=20)
def cargar_datos() -> pd.DataFrame:
    """Carga TODAS las estaciones (incluye NORMAL) para los gráficos."""
    filas = []
    for categoria in TODAS_CATEGORIAS:
        for item in consultar_categoria(categoria):
            min_rest = float(item.get("min_estimado_restante", 999999))
            cap = int(item.get("capacidad", 0) or 0)
            bikes = int(item.get("bikes_disponibles", 0) or 0)
            antiguedad = minutos_desde(item.get("ultimo_reporte", ""))
            filas.append({
                "estacion_id": item.get("estacion_id"),
                "estado": categoria,
                "min_restantes": None if min_rest >= 999999 else round(min_rest, 1),
                "bikes": bikes,
                "docks": int(item.get("docks_libres", 0) or 0),
                "capacidad": cap,
                "ocupacion_pct": round(100 * bikes / cap, 1) if cap > 0 else 0,
                "ultimo_reporte": item.get("ultimo_reporte", ""),
                "lat": float(item["lat"]) if item.get("lat") is not None else None,
                "lon": float(item["lon"]) if item.get("lon") is not None else None,
                "desactualizada": (antiguedad is not None and antiguedad > UMBRAL_DESACTUALIZADA_MIN),
            })
    return pd.DataFrame(filas)


# -----------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="BikeNexus — Alertas", page_icon="🚲", layout="wide")

    # Encabezado
    st.markdown(
        "<h1 style='margin-bottom:0'>🚲 BikeNexus — Panel de operaciones</h1>"
        "<p style='color:#888;margin-top:4px'>Estado en tiempo real de las estaciones · "
        "ordenado por urgencia (RF-07) · Nueva York</p>",
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.header("⚙️ Opciones")
        if st.button("🔄 Refrescar ahora", use_container_width=True):
            st.cache_data.clear()
        st.divider()
        st.caption(f"Tabla: `{TABLA}`")
        st.caption(f"Región: `{REGION}`")
        st.caption(f"Actualizado: {datetime.now().strftime('%H:%M:%S')}")

    try:
        df = cargar_datos()
    except Exception as e:  # noqa: BLE001
        st.error(f"No se pudo consultar DynamoDB: {e}")
        st.info("¿Corriste el productor y el pipeline? ¿LocalStack está arriba?")
        return

    if df.empty:
        st.warning("No hay datos todavía. Ejecuta el productor y el pipeline.")
        return

    total = len(df)
    alertas = df[df["estado"].isin(CATEGORIAS_ALERTA)]

    # --- KPIs (tarjetas grandes) ---------------------------------------------
    st.subheader("Resumen")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🚲 Estaciones", total)
    c2.metric("⚠️ En alerta", len(alertas),
              delta=f"{round(100*len(alertas)/total)}%" if total else None,
              delta_color="inverse")
    c3.metric("🔴 Vacías", int((df["estado"] == "VACIA").sum()))
    c4.metric("🔵 Llenas", int((df["estado"] == "LLENA").sum()))
    c5.metric("🟠 En riesgo", int((df["estado"] == "EN_RIESGO").sum()))

    st.divider()

    # --- Gráficos ------------------------------------------------------------
    g1, g2 = st.columns([1, 1])

    with g1:
        st.markdown("**Distribución por estado**")
        conteo = (
            df["estado"].value_counts()
            .reindex(TODAS_CATEGORIAS, fill_value=0)
            .rename_axis("estado").reset_index(name="cantidad")
        )
        # Barras con color por estado.
        st.bar_chart(
            conteo.set_index("estado"),
            color="#888888",
            height=280,
        )
        # Leyenda de colores legible.
        st.markdown(" &nbsp; ".join(
            f"{EMOJI[e]} {e.replace('_',' ').title()}" for e in TODAS_CATEGORIAS
        ))

    with g2:
        st.markdown("**Distribución del % de ocupación**")
        # Histograma de ocupación: ¿cuántas estaciones están casi vacías/llenas?
        bins = pd.cut(
            df["ocupacion_pct"],
            bins=[-1, 0, 20, 40, 60, 80, 99, 100],
            labels=["0% (vacía)", "1-20%", "21-40%", "41-60%",
                    "61-80%", "81-99%", "100% (llena)"],
        )
        hist = bins.value_counts().sort_index().rename_axis("rango").reset_index(name="estaciones")
        st.bar_chart(hist.set_index("rango"), height=280)

    # --- Mapa (si hay coordenadas) -------------------------------------------
    con_coords = df.dropna(subset=["lat", "lon"])
    if not con_coords.empty:
        st.divider()
        st.markdown("**🗺️ Mapa de estaciones en alerta**")
        mapa_alertas = con_coords[con_coords["estado"].isin(CATEGORIAS_ALERTA)]
        if not mapa_alertas.empty:
            st.map(mapa_alertas[["lat", "lon"]], size=30, color="#e74c3c")
        else:
            st.info("No hay estaciones en alerta con coordenadas para el mapa.")

    st.divider()

    # --- Tabla de alertas (legible, con badges) ------------------------------
    st.subheader("🚨 Estaciones en alerta — ordenadas por urgencia")
    if alertas.empty:
        st.success("✅ No hay estaciones en alerta ahora mismo.")
        return

    tabla = alertas.copy().sort_values("min_restantes", na_position="last")
    tabla["Estado"] = tabla["estado"].map(lambda e: f"{EMOJI[e]} {e.replace('_',' ').title()}")
    tabla["Urgencia"] = tabla["min_restantes"].map(
        lambda m: "🔴 Crítica" if (m is not None and m <= 0)
        else (f"⏱️ {m} min" if m is not None else "—")
    )
    tabla["Ocupación"] = tabla["ocupacion_pct"].map(lambda p: f"{p:.0f}%")
    tabla["Actualización"] = tabla["desactualizada"].map(
        lambda d: "⚠️ Desactualizada" if d else "✅ Al día"
    )

    vista = tabla[[
        "estacion_id", "Estado", "Urgencia", "bikes", "docks",
        "capacidad", "Ocupación", "Actualización",
    ]].rename(columns={
        "estacion_id": "Estación",
        "bikes": "Bicis 🚲",
        "docks": "Espacios libres 🅿️",
        "capacidad": "Capacidad",
    })

    st.dataframe(
        vista,
        use_container_width=True,
        hide_index=True,
        height=420,
        column_config={
            "Ocupación": st.column_config.TextColumn(help="% de la capacidad ocupada por bicis"),
        },
    )

    st.caption(
        f"Mostrando {len(alertas)} estaciones en alerta de {total} totales. "
        f"'Desactualizada' = sin reportar hace más de {UMBRAL_DESACTUALIZADA_MIN} min (RNF-02)."
    )


if __name__ == "__main__":
    main()

"""
Dashboard de alertas — BikeNexus (RF-07, RNF-02)
================================================

Panel profesional (estilo BI) para los OPERADORES: muestra qué estaciones están
vacías, llenas o en riesgo, ordenadas por urgencia (RF-07), y marca las
desactualizadas (>2 min sin reporte, RNF-02).

Lee de DynamoDB usando el índice secundario (GSI) para traer cada categoría ya
ordenada por urgencia, sin escanear toda la tabla.

Cómo se ejecuta:
  pip install -r requirements.txt
  streamlit run dashboard.py

  Variables de entorno:
    DYNAMODB_TABLA     (default: bikenexus-estado-estaciones)
    AWS_REGION         (default: us-east-1)
    DYNAMODB_ENDPOINT  (LocalStack: http://localhost:4566; en AWS se deja vacío)
"""

import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

TABLA = os.environ.get("DYNAMODB_TABLA", "bikenexus-estado-estaciones")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT") or None
GSI = "gsi-clasificacion-tiempo"

UMBRAL_DESACTUALIZADA_MIN = 2
CATEGORIAS_ALERTA = ["VACIA", "LLENA", "EN_RIESGO"]
TODAS_CATEGORIAS = ["VACIA", "LLENA", "EN_RIESGO", "NORMAL"]

# Paleta de colores (consistente en toda la app, estilo BI).
COLOR = {
    "VACIA": "#ef4444",      # rojo
    "LLENA": "#3b82f6",      # azul
    "EN_RIESGO": "#f59e0b",  # ámbar
    "NORMAL": "#10b981",     # verde
}
LABEL = {"VACIA": "Vacía", "LLENA": "Llena", "EN_RIESGO": "En riesgo", "NORMAL": "Normal"}


@st.cache_resource
def _tabla_dynamodb():
    return boto3.resource(
        "dynamodb", region_name=REGION, endpoint_url=ENDPOINT
    ).Table(TABLA)


def consultar_categoria(clasificacion: str) -> list[dict]:
    """Trae las estaciones de una categoría, ordenadas por urgencia (GSI)."""
    tabla = _tabla_dynamodb()
    kwargs = dict(
        IndexName=GSI,
        KeyConditionExpression=Key("clasificacion").eq(clasificacion),
        ScanIndexForward=True,
    )
    resp = tabla.query(**kwargs)
    items = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = tabla.query(ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
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
    filas = []
    for categoria in TODAS_CATEGORIAS:
        for item in consultar_categoria(categoria):
            min_rest = float(item.get("min_estimado_restante", 999999))
            cap = int(item.get("capacidad", 0) or 0)
            bikes = int(item.get("bikes_disponibles", 0) or 0)
            antiguedad = minutos_desde(item.get("ultimo_reporte", ""))
            filas.append({
                "estacion_id": item.get("estacion_id"),
                "nombre": item.get("nombre", ""),
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


# --- Estilos (CSS) para look profesional -------------------------------------
def inyectar_css():
    st.markdown(
        """
        <style>
          /* Más aire arriba para que el título no quede recortado por el header */
          .block-container { padding-top: 3rem; padding-bottom: 1rem; }
          header[data-testid="stHeader"] { height: 0; }
          /* Tarjetas KPI */
          .kpi {
            background: linear-gradient(145deg, #1e293b, #0f172a);
            border: 1px solid #334155; border-radius: 14px;
            padding: 18px 20px; text-align: left;
            box-shadow: 0 4px 14px rgba(0,0,0,0.3);
          }
          .kpi .label { color:#94a3b8; font-size:0.82rem; font-weight:600;
                        text-transform:uppercase; letter-spacing:0.5px; }
          .kpi .value { color:#f1f5f9; font-size:2.1rem; font-weight:800; line-height:1.1; }
          .kpi .sub   { font-size:0.8rem; margin-top:2px; }
          .titulo { font-size:1.9rem; font-weight:800; color:#f8fafc; margin-bottom:0; }
          .subtitulo { color:#94a3b8; margin-top:2px; font-size:0.95rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def tarjeta_kpi(label, value, sub="", sub_color="#64748b", borde="#334155"):
    return (
        f"<div class='kpi' style='border-left:5px solid {borde}'>"
        f"<div class='label'>{label}</div>"
        f"<div class='value'>{value}</div>"
        f"<div class='sub' style='color:{sub_color}'>{sub}</div>"
        f"</div>"
    )


# --- Gráficos Plotly ---------------------------------------------------------
def grafico_dona(df):
    conteo = df["estado"].value_counts().reindex(TODAS_CATEGORIAS, fill_value=0)
    fig = go.Figure(go.Pie(
        labels=[LABEL[e] for e in conteo.index],
        values=conteo.values,
        hole=0.62,
        marker=dict(colors=[COLOR[e] for e in conteo.index]),
        textinfo="percent",
        hovertemplate="%{label}: %{value} estaciones<extra></extra>",
    ))
    total = int(conteo.sum())
    fig.update_layout(
        annotations=[dict(text=f"<b>{total}</b><br>estaciones", x=0.5, y=0.5,
                          font_size=16, showarrow=False, font_color="#e2e8f0")],
        showlegend=True, legend=dict(orientation="h", y=-0.1, font_color="#cbd5e1"),
        margin=dict(t=10, b=10, l=10, r=10), height=300,
        paper_bgcolor="rgba(0,0,0,0)", font_color="#cbd5e1",
    )
    return fig


def _color_urgencia(min_rest, estado):
    """Color según cuán urgente: rojo = muy urgente, ámbar = medio, verde = con margen."""
    if min_rest is None or min_rest <= 2:
        return "#ef4444"   # rojo: crítico / ya en el límite
    if min_rest <= 7:
        return "#f59e0b"   # ámbar: urgente
    return "#eab308"       # amarillo: en riesgo pero con algo de margen


def grafico_top_criticas(alertas):
    """
    Barras horizontales con las 10 estaciones más urgentes por TIEMPO RESTANTE
    estimado (menor = más urgente). La más crítica queda arriba. El color indica
    el nivel de urgencia. Solo tiene sentido con estaciones EN_RIESGO (que tienen
    estimación de minutos); si no hay, se muestra un aviso.
    """
    con_tiempo = alertas.dropna(subset=["min_restantes"])
    con_tiempo = con_tiempo[con_tiempo["min_restantes"] < 999999]

    if con_tiempo.empty:
        return None  # el caller mostrará un mensaje explicativo

    top = con_tiempo.nsmallest(10, "min_restantes").iloc[::-1]  # más urgente arriba

    def _etq(row):
        nom = (row["nombre"] or "").strip()
        base = nom if nom else f"Estación {row['estacion_id']}"
        return (base[:24] + "…") if len(base) > 25 else base

    etiquetas = top.apply(_etq, axis=1)
    valores = top["min_restantes"].astype(float)
    colores = [_color_urgencia(v, e) for v, e in zip(valores, top["estado"])]
    texto = [f"  {v:.0f} min" for v in valores]

    fig = go.Figure(go.Bar(
        x=valores,
        y=etiquetas,
        orientation="h",
        marker=dict(color=colores, line=dict(width=0), cornerradius=6),
        text=texto,
        textposition="outside",
        textfont=dict(color="#f1f5f9", size=13, family="Arial Black"),
        hovertemplate="<b>%{y}</b><br>Se vacía/llena en ~%{x:.0f} min<extra></extra>",
    ))
    fig.update_layout(
        height=360, margin=dict(t=10, b=40, l=10, r=50),
        xaxis=dict(
            title="⏱️ Minutos restantes para vaciarse / llenarse",
            gridcolor="rgba(148,163,184,0.15)", zeroline=False,
            title_font=dict(size=12, color="#94a3b8"),
        ),
        yaxis=dict(tickfont=dict(size=13, color="#e2e8f0")),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color="#cbd5e1", bargap=0.4,
    )
    fig.update_xaxes(range=[0, max(valores.max() * 1.25, 5)])
    return fig


def mapa_estaciones(df):
    con_coords = df.dropna(subset=["lat", "lon"])
    alertas = con_coords[con_coords["estado"].isin(CATEGORIAS_ALERTA)]
    if alertas.empty:
        return None
    fig = px.scatter_mapbox(
        alertas, lat="lat", lon="lon", color="estado",
        color_discrete_map=COLOR,
        hover_name="estacion_id",
        hover_data={"bikes": True, "docks": True, "lat": False, "lon": False},
        zoom=10.5, height=420,
    )
    fig.update_traces(marker=dict(size=11))
    fig.update_layout(
        mapbox_style="carto-darkmatter",
        margin=dict(t=0, b=0, l=0, r=0),
        legend=dict(font_color="#cbd5e1", bgcolor="rgba(15,23,42,0.6)"),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# -----------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="BikeNexus — Operaciones", page_icon="🚲", layout="wide")
    inyectar_css()

    st.markdown(
        "<div class='titulo'>🚲 BikeNexus · Centro de Operaciones</div>"
        "<div class='subtitulo'>Monitoreo en tiempo real de estaciones · Nueva York · "
        "ordenado por urgencia (RF-07)</div>",
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.header("⚙️ Controles")
        if st.button("🔄 Refrescar datos", use_container_width=True):
            st.cache_data.clear()
        st.divider()
        st.caption(f"📦 Tabla: `{TABLA}`")
        st.caption(f"🌎 Región: `{REGION}`")
        st.caption(f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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
    n_vacias = int((df["estado"] == "VACIA").sum())
    n_llenas = int((df["estado"] == "LLENA").sum())
    n_riesgo = int((df["estado"] == "EN_RIESGO").sum())

    # --- Fila de KPIs (tarjetas estilizadas) ---------------------------------
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.markdown(tarjeta_kpi("Estaciones", total, "monitoreadas", "#94a3b8", "#64748b"),
                unsafe_allow_html=True)
    k2.markdown(tarjeta_kpi("En alerta", len(alertas),
                            f"{round(100*len(alertas)/total)}% de la red", "#f87171", "#ef4444"),
                unsafe_allow_html=True)
    k3.markdown(tarjeta_kpi("Vacías", n_vacias, "sin bicis", "#f87171", COLOR["VACIA"]),
                unsafe_allow_html=True)
    k4.markdown(tarjeta_kpi("Llenas", n_llenas, "sin espacio", "#93c5fd", COLOR["LLENA"]),
                unsafe_allow_html=True)
    k5.markdown(tarjeta_kpi("En riesgo", n_riesgo, "próximas a fallar", "#fcd34d", COLOR["EN_RIESGO"]),
                unsafe_allow_html=True)

    st.write("")

    # --- Fila de gráficos ----------------------------------------------------
    g1, g2 = st.columns([1, 1.6])
    with g1:
        st.markdown("**Distribución por estado**")
        st.plotly_chart(grafico_dona(df), use_container_width=True, config={"displayModeBar": False})
    with g2:
        st.markdown("**🏆 Top 10 estaciones más urgentes** "
                    "<span style='color:#64748b;font-size:0.8rem'>(por tiempo estimado para fallar)</span>",
                    unsafe_allow_html=True)
        fig_top = grafico_top_criticas(alertas) if not alertas.empty else None
        if fig_top is not None:
            st.plotly_chart(fig_top, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info(
                "ℹ️ Aún no hay estaciones **en riesgo** con predicción de tiempo.\n\n"
                "La predicción se calcula comparando reportes consecutivos. "
                "Deja corriendo `run_loop_local.py` unos minutos para que se "
                "acumule tendencia y aparezcan aquí las estaciones próximas a fallar."
            )

    # --- Mapa ----------------------------------------------------------------
    fig_mapa = mapa_estaciones(df)
    if fig_mapa is not None:
        st.markdown("**🗺️ Ubicación de estaciones en alerta**")
        st.plotly_chart(fig_mapa, use_container_width=True, config={"displayModeBar": False})

    # --- Tabla ---------------------------------------------------------------
    st.markdown("**🚨 Detalle de estaciones en alerta — ordenadas por urgencia**")
    if alertas.empty:
        st.success("✅ No hay estaciones en alerta ahora mismo.")
        return

    tabla = alertas.copy().sort_values("min_restantes", na_position="last")
    tabla["Estado"] = tabla["estado"].map(LABEL)
    # Columna "Actualizada": 🟢 verde = dato reciente (<2 min), ⚪ gris = viejo (RNF-02).
    tabla["Actualizada"] = tabla["desactualizada"].map(
        lambda d: "⚪ Sin reporte" if d else "🟢 Al día"
    )
    vista = tabla[[
        "estacion_id", "Estado", "min_restantes", "bikes", "docks",
        "capacidad", "ocupacion_pct", "Actualizada",
    ]].rename(columns={
        "estacion_id": "Estación", "min_restantes": "Min. restantes",
        "bikes": "Bicis", "docks": "Espacios", "capacidad": "Capacidad",
        "ocupacion_pct": "% Ocupación",
    })

    st.dataframe(
        vista, use_container_width=True, hide_index=True, height=380,
        column_config={
            "% Ocupación": st.column_config.ProgressColumn(
                "% Ocupación", format="%.0f%%", min_value=0, max_value=100),
            "Min. restantes": st.column_config.NumberColumn(
                "Min. restantes", format="%.1f", help="Minutos estimados para vaciarse/llenarse"),
            "Actualizada": st.column_config.TextColumn(
                "Actualizada",
                help="🟢 Al día = reportó hace menos de 2 min · ⚪ Sin reporte = dato posiblemente viejo (RNF-02)"),
        },
    )
    st.caption(
        f"Mostrando {len(alertas)} estaciones en alerta de {total} totales · "
        f"🟢 Al día = dato reciente · ⚪ Sin reporte = sin datos hace >{UMBRAL_DESACTUALIZADA_MIN} min (RNF-02) · "
        f"actualizado {datetime.now().strftime('%H:%M:%S')}"
    )


if __name__ == "__main__":
    main()

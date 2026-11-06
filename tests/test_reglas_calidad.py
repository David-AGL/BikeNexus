"""
Pruebas unitarias de las reglas de calidad y clasificación (RF-04, RF-06)
=========================================================================

Replican, en Python puro (sin Spark), la LOGICA de las reglas para verificar
que son correctas. La implementacion real corre en Spark, pero las reglas de
negocio se validan aqui de forma rapida y sin dependencias.

Se ejecutan con:  python -m pytest tests/test_reglas_calidad.py -v
"""


# --- Regla de validez de Silver (RF-04) --------------------------------------
def es_valido(bikes, capacidad):
    """
    Replica la regla de RF-04:
      invalido si capacidad <= 0 (o None) o si bikes > capacidad.
    """
    if capacidad is None or capacidad <= 0:
        return False
    if bikes > capacidad:
        return False
    return True


def test_capacidad_cero_es_invalido():
    assert es_valido(bikes=0, capacidad=0) is False


def test_bikes_mayor_que_capacidad_es_invalido():
    assert es_valido(bikes=30, capacidad=25) is False


def test_registro_normal_es_valido():
    assert es_valido(bikes=5, capacidad=25) is True


def test_capacidad_nula_es_invalido():
    assert es_valido(bikes=5, capacidad=None) is False


# --- Regla de clasificación (RF-06) ------------------------------------------
UMBRAL_RIESGO_MIN = 15


def clasificar(bikes, docks, min_para_vaciarse=None, min_para_llenarse=None):
    """
    Replica la clasificacion de RF-06, en orden de prioridad:
      VACIA -> LLENA -> EN_RIESGO -> NORMAL.
    """
    if bikes == 0:
        return "VACIA"
    if docks == 0:
        return "LLENA"
    en_riesgo = (
        (min_para_vaciarse is not None and min_para_vaciarse < UMBRAL_RIESGO_MIN)
        or (min_para_llenarse is not None and min_para_llenarse < UMBRAL_RIESGO_MIN)
    )
    if en_riesgo:
        return "EN_RIESGO"
    return "NORMAL"


def test_clasifica_vacia():
    assert clasificar(bikes=0, docks=20) == "VACIA"


def test_clasifica_llena():
    assert clasificar(bikes=10, docks=0) == "LLENA"


def test_clasifica_en_riesgo_por_vaciarse():
    # Le quedan 5 minutos para vaciarse -> EN_RIESGO.
    assert clasificar(bikes=3, docks=22, min_para_vaciarse=5) == "EN_RIESGO"


def test_clasifica_normal():
    # Cambia despacio: 40 min para vaciarse -> NORMAL.
    assert clasificar(bikes=10, docks=15, min_para_vaciarse=40) == "NORMAL"


def test_prioridad_vacia_sobre_riesgo():
    # Aunque tuviera estimacion de riesgo, 0 bikes manda: VACIA.
    assert clasificar(bikes=0, docks=5, min_para_vaciarse=2) == "VACIA"

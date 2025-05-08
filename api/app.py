from fastapi import FastAPI, APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
from typing import Dict, List
from fastapi.responses import FileResponse
import subprocess
import requests
import logging
import json
import tempfile
import os
import pandas as pd
import psycopg2
from requests.auth import HTTPBasicAuth
import re

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI(root_path="/apiopenproject")
api_router = APIRouter(prefix="/apiopenproject")
# Configuración de seguridad
security = HTTPBasic()
VALID_USERNAME = "admin"
VALID_PASSWORD = "password123"  # ⚠️ Reemplaza esto con credenciales seguras
OPENPROJECT_API_URL = "https://sucs.eacomsa.com/api/v3/work_packages"

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    """Verifica las credenciales del usuario."""
    if credentials.username != VALID_USERNAME or credentials.password != VALID_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# APScheduler
scheduler = BackgroundScheduler()

def run_update():
    """Ejecuta update.py y registra la salida."""
    logging.info("Ejecutando update.py")
    try:
        result = subprocess.run(["python", "tasks/update.py"], capture_output=True, text=True)
        with open("cron_log.txt", "a") as f:
            f.write(f"\n[{datetime.now()}] STDOUT:\n{result.stdout}\n")
            f.write(f"[{datetime.now()}] STDERR:\n{result.stderr}\n")
        logging.info("update.py finalizado")
    except Exception as e:
        logging.error(f"Error al ejecutar update.py: {e}")

def get_all_work_packages():
    all_wps = []
    offset = 1
    page_size = 1000
    API_KEY = "1c8305e1e0a17b93e361923c280c81cba0ca0c5abefc684e0ab9dbc1dbb17425"
    while True:
        resp = requests.get(f"{OPENPROJECT_API_URL}?offset={offset}&pageSize={page_size}", auth=HTTPBasicAuth('apikey', API_KEY))
        try:
            json_data = resp.json()
            logging.info("📦 Respuesta JSON:")
            logging.info(json.dumps(json_data, indent=2))
            data = json_data["_embedded"]["elements"]
        except ValueError:
            logging.info("❌ Error al parsear JSON. Respuesta:")
            logging.info(resp.text)
            break
        except KeyError:
            logging.info("❌ La clave '_embedded' no está en la respuesta. Respuesta:")
            logging.info(json.dumps(json_data, indent=2))
            break

        all_wps.extend(data)

        if len(data) < page_size:
            break
        offset += 1

    return all_wps

def matches_all_filters(wp, filters: Dict[str, List[str]]):
    for field, expected_values in filters.items():
        content = wp.get(field, "")
        # si es un dict con campo "raw"
        if isinstance(content, dict):
            content = content.get("raw", "")
        # si no es dict, asumimos que es un string u otro valor plano

        if str(content) not in map(str, expected_values):
            return False
    return True

def clean_text(text):
    if isinstance(text, str):
        # Reemplazar caracteres no imprimibles (como \u0002) por un espacio vacío o por otro carácter
        return re.sub(r'[^\x20-\x7E]', '', text)  # Esto eliminará caracteres no ASCII imprimibles
    return text


# Programar la ejecución diaria con APScheduler
scheduler.add_job(run_update, CronTrigger(hour=5, minute=30))
scheduler.add_job(run_update, CronTrigger(hour=13, minute=30))
scheduler.start()

@api_router.get("/", dependencies=[Depends(verify_credentials)])
async def index():
    return {"message": "API protegida con autenticación básica."}

@api_router.get("/force/update", dependencies=[Depends(verify_credentials)])
async def force_update():
    """Endpoint para forzar la ejecución de update.py."""
    run_update()
    return {"status": "update.py ejecutado manualmente"}

@api_router.post("/filter-and", dependencies=[Depends(verify_credentials)])
async def filter_with_and(filters: Dict[str, List[str]]):
    work_packages = get_all_work_packages()
    filtered = [wp for wp in work_packages if matches_all_filters(wp, filters)]

    if not filtered:
        raise HTTPException(status_code=404, detail="No se encontraron tareas con esos filtros.")

    customfield_mapping = {
        1: "ANULADA",
        2: "ANULADA CON COSTE",
        3: "AR Y MD FACILITADAS",
        4: "AR Y MD INCORRECTA",
        5: "AVISO DE NO OCUPACION",
        6: "AVISO PARA RECIBIR AR Y MD",
        7: "BAJA CONFIRMADA",
        8: "BAJA SOLICITADA",
        9: "CITA DE REPLANTEO ACEPTADA",
        10: "CITA DE REPLANTEO PROPUESTA",
        11: "CITA DE REPLANTEO RECHAZADA",
        12: "CONCESIÓN DEL PERMISO",
        13: "EJECUCION EN OBRAS",
        14: "EJECUCION TENDIDO",
        15: "FIN DE LAS OBRAS",
        16: "INCIDENCIA",
        17: "INCIDENCIA DE REPLANTEO",
        18: "INICIO DE LAS OBRAS",
        19: "NO CONFIRMADA",
        20: "OCUPACION",
        21: "OCUPACION RECTIFICADA",
        22: "PDTE. ANULADA CON COSTE",
        23: "PERMISOS",
        24: "PROYECTO ESPECIFICO",
        25: "PROYECTO INVIABLE. PDTE OPERADOR",
        26: "PROYECTO INVIABLE. PDTE REVISION",
        27: "PTE. REPLANTEO AUTÓNOMO",
        28: "REPLANTEO REALIZADO. VIABLE",
        29: "RESERVADA SIN OCUPAR",
        30: "TENDIDO",
        31: "CÓDIGO Y PLANO SUC",
        32: "MUNICIPIO",
        33: "CODIGO MIGA",
        34: "REPLANTEO AUTONOMO",
        35: "REPLANTEO CONJUNTO",
        36: "HORA REPLANTEO",
        37: "INICIO EJECUCION",
        38: "LIMITE EJECUCION",
        50: "F. INICIO OBRA",
        51: "F. SOLIC. PERMISOS",
        52: "F. FINAL OBRA",
        53: "SOLICITADO POR",
        55: "Gestor corrección AR y MD INCORRECTA",
        56: "FECHA AR Y MD INCORRECTA",
        57: "FECHA RESOLUCIÓN AR Y MD INCORRECTA",
        39: "MALLA GEO. (m)",
        42: "Nª CR 'USO-0'",
        58: "REGISTROS FINALES TRAS AR Y MD FACILITADAS",
        87: "FECHA POSIBLE ANULACIÓN",
        88: "NOMBRE ARCHIVO ORIGINAL",
        62: "ESTADO ACTUAL NEON",
        89: "ANOTACION GESTOR",
        54: "AR Y MD ENVIADAS",
        90: "ID SUC",
        60: "ES INVIABLE OCULTA",
        61: "OBSERVACIONES NEON",
        63: "CODIGO ANOTACION",
        64: "ANOTACION",
        65: "Gestor elaboración MD",
        66: "GESTOR PPAL",
        91: "CONSULTADO_DEVUELTO_ANULADO",
        92: "CORREO",
        93: "DEFINE SECTOR_CLUSTER",
        70: "ENCARGO",
        71: "PARTNER",
        72: "TIPO REPLANTEO (CONJUNTO/AUTÓNOMO)",
        73: "MD AR",
        76: "APROBACIÓN COSTES PMO",
        43: "Nª CR",
        45: "Nª PEDESTAL",
        46: "Nª AISLADA",
        47: "Nª POSTES",
        48: "COSTE SUSTITUCION",
        74: "CANALIZADO (m)",
        94: "DOCUMENTACION ADJUNTA",
        95: "ENTIDAD SINGULAR",
        96: "NUMERO TOTAL DE REGISTROS A SOLICITAR",
        97: "NUMERO TOTAL DE SUC A SOLICITAR",
        98: "PRIORIDAD",
        80: "SUSTITUCION POSTES",
        82: "SUSTITUCION POSTES BOOL",
        41: "SUB UTILIZADO (m)",
        68: "SE PUEDE AVANZAR(SI/NO)",
        99: "SEMANA SOLICITADA",
        75: "SAL. LAT. (m)",
        40: "SUB INSTALADO (m)",
        69: "ES PRIO TESA",
        59: "ES DE POSTE (SI/NO)",
        83: "PROVINCIA",
        84: "CENTRAL",
        85: "PTE. DATOS REPLANTEO AUTÓNOMO",
        44: "Nª ARQ",
        86: "AP-DN",
        100: "SUC YA REPLANTEADA",
        101: "TIPO DE INFRAESTRUCTURA",
        102: "TIPO DE RED",
        103: "TIPO DE SUC",
        104: "TASK ID",
        105: "DOC ARCHIVADA",
        106: "EN CURSO",
        107: "ESTIMACION TIEMPO DEDICADO (H)",
        77: "ALTA SOLICITUD",
        108: "FECHA DE ANULACION",
        109: "FECHA DE CONSULTA",
        110: "FECHA DE DEVOLUCION",
        111: "FECHA PREVISION DESPLIEGUE",
        112: "PINTADO",
        113: "REGISTROS FINALES",
        114: "SOLICITADO",
        115: "OBSERVACIONES GESTOR SUC INSDO/EACOM",
        117: "FECHA REGISTRO INV. OPE",
        118: "ESTADO GESTORES INV. OPE",
        119: "DISCONFORMIDAD",
        120: "FECHA RECLAMACION",
        121: "MOTIVO",
        124: "HOJAS",
        67:  "SE PUEDE ANULAR (SI/NO)",
        126: "ANÁLISIS PARTNER ANULAR (SI/NO)",
        127: "ESTADO GESTORES INV. OPE NEW",
        128: "DINSCONFORMIDAD NEW",
        129: "CODIGO ANOTACION NEW"
    }
    df = pd.DataFrame([{
        "ID": wp["id"],
        "Asunto": wp["subject"]["raw"] if isinstance(wp["subject"], dict) else wp["subject"],
        "Estado": wp["status"]["name"] if wp.get("status") else None,
        "Tipo": wp["type"]["name"] if wp.get("type") else None,
        **{
           customfield_mapping[num]: wp.get(f"customField{num}", None)
           for num in customfield_mapping
        }
    } for wp in filtered])

    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        df.to_excel(tmp.name, index=False, engine="openpyxl")
        tmp_path = tmp.name

# Enviar el archivo como respuesta
    return FileResponse(
        path=tmp_path,
        filename="tareas_filtradas.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@api_router.get("/get-all-tasks", dependencies=[Depends(verify_credentials)])
async def get_all_tasks():
    try:
        conn = None
        # Conexión a PostgreSQL
        conn = psycopg2.connect(
            host="openproject_sucs_postgres",
            port="5432",
            database="openproject",
            user="postgres",
            password="p4ssw0rd"
        )

        # Ejecutar la consulta
        query = "SELECT subject, custom_fields FROM public.vw_work_packages_custom"

        df = pd.read_sql(query, conn)

        print("FETA QUERY")

        # Convertir la columna custom_fields (JSON) en columnas
        custom_df = df['custom_fields'].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x else x
        ).apply(pd.Series)

        print("SEPARAT PER CUSTOM_FIELDS")

        # Limpiar los datos en custom_df (aplicar la limpieza de caracteres no válidos)
        custom_df_cleaned = custom_df.applymap(clean_text)

        # Limpiar la columna 'subject' también
        df['subject'] = df['subject'].apply(clean_text)

        # Combinar con la columna subject
        final_df = pd.concat([df['subject'], custom_df_cleaned], axis=1)

        print("1")

        # Guardar los datos en un archivo Excel temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            final_df.to_excel(tmp.name, index=False, engine="openpyxl")
            tmp_path = tmp.name

        print("GUARDAR EXC")

        # Enviar el archivo como respuesta
        return FileResponse(
            path=tmp_path,
            filename="work_packages.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {str(e)}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener las tareas: {str(e)}")

    finally:
        if conn:
            conn.close()

# Incluir el router en la aplicación
app.include_router(api_router)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
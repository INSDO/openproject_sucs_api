from fastapi import FastAPI, APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import subprocess
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI(root_path="/apiopenproject")
api_router = APIRouter(prefix="/apiopenproject")

# Configuración de seguridad
security = HTTPBasic()
VALID_USERNAME = "admin"
VALID_PASSWORD = "password123"  # ⚠️ Reemplaza esto con credenciales seguras

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

# Programar la ejecución diaria con APScheduler
now = datetime.now()
next_run_time = now.replace(hour=6, minute=0, second=0, microsecond=0)

if now.hour >= 6:  # Si ya pasó hoy a las 6 AM, programar para mañana
    next_run_time += timedelta(days=1)

next_run_time_3pm = now.replace(hour=15, minute=0, second=0, microsecond=0)

if now.hour >= 15:  # Si ya pasó hoy a las 3 PM, programar para mañana
    next_run_time_3pm += timedelta(days=1)

# Programar la ejecución diaria con intervalo de 24 horas
scheduler.add_job(run_update, 'interval', days=1, next_run_time=next_run_time)
scheduler.add_job(run_update, 'interval', days=1, next_run_time=next_run_time_3pm)
scheduler.start()

@api_router.get("/", dependencies=[Depends(verify_credentials)])
async def index():
    return {"message": "API protegida con autenticación básica."}

@api_router.get("/force/update", dependencies=[Depends(verify_credentials)])
async def force_update():
    """Endpoint para forzar la ejecución de update.py."""
    run_update()
    return {"status": "update.py ejecutado manualmente"}

# Incluir el router en la aplicación
app.include_router(api_router)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)

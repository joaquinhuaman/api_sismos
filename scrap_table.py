import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import os
import json
from datetime import datetime, timezone
import traceback

# -------- Configuración global --------
HEADERS = ['ubicacion', 'fecha_hora', 'magnitud']  # ajusta si hay más columnas
USER_AGENT = "Mozilla/5.0 (compatible; LambdaScraper/1.0; +https://example.com)"

dynamodb = boto3.resource('dynamodb')  # cliente reutilizable

def log_json(tipo: str, datos: dict):
    """Imprime un JSON estandarizado en CloudWatch."""
    print(json.dumps({
        "tipo": tipo,
        "log_datos": datos,
        "ts": datetime.now(timezone.utc).isoformat()
    }))

def lambda_handler(event, context):
    try:
        # ------- 1. Leer parámetros / entorno -------
        url        = os.environ.get('SCRAP_URL', "https://sgonorte.bomberosperu.gob.pe/24horas/?criterio=/")
        table_name = os.environ['TABLE_NAME']              # debe existir
        css_table  = os.environ.get('TABLE_SELECTOR', 'table.table.table-hover.table-bordered')

        log_json("INFO", {"msg": "Scraping inicia", "url": url, "selector": css_table})

        # ------- 2. Descarga de la página -------
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.content, 'html.parser')
        table_html = soup.select_one(css_table)
        if not table_html:
            raise ValueError("No se encontró la tabla con el selector CSS proporcionado")

        # ------- 3. Extraer filas -------
        rows = []
        for tr in table_html.select('tr'):
            celdas = [td.get_text(strip=True) for td in tr.select('td')]
            if not celdas:
                continue                       # salta tr vacíos (header u otros)
            # Emparejar con los encabezados (descarta celdas sobrantes)
            celdas = celdas[:len(HEADERS)]
            fila = dict(zip(HEADERS, celdas))
            fila['id'] = str(uuid.uuid4())
            rows.append(fila)

        if not rows:
            raise ValueError("La tabla se encontró pero no contiene filas de datos")

        # ------- 4. Limpiar y cargar DynamoDB -------
        table = dynamodb.Table(table_name)

        # Elimina todos los ítems existentes (cuidado si quieres histórico)
        scan = table.scan(ProjectionExpression='id')
        with table.batch_writer() as batch:
            for item in scan.get('Items', []):
                batch.delete_item(Key={'id': item['id']})

        # Inserta nuevas filas
        with table.batch_writer() as batch:
            for fila in rows:
                batch.put_item(Item=fila)

        log_json("INFO", {"msg": "Scraping exitoso", "total_filas": len(rows)})

        return {
            "statusCode": 200,
            "body": rows
        }

    except Exception as exc:
        log_json("ERROR", {
            "mensaje": str(exc),
            "traceback": traceback.format_exc()
        })
        return {
            "statusCode": 500,
            "error": str(exc)
        }

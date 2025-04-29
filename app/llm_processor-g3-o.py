#!/usr/bin/env python3
# llm_processor.py
# Procesa lotes de notas usando Ollama (gemma3:12b) con JSON Schema y reintentos

import os
import json
import logging
import argparse
from ollama import Client
from jsonschema import validate, ValidationError
from sqlalchemy.orm import Session
from models import SessionLocal, NotasXUsuario, NotasXUsuarioGemma

# ▶ Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("llm_processor.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ▶ Configuración Ollama
#OLLAMA_HOST = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_HOST = os.getenv("OLLAMA_BASE_URL", "https://ollama.curza.com.ar")
MODEL = os.getenv("GEMMA3_MODEL", "gemma3:12b")
client = Client(host=OLLAMA_HOST)

# ▶ Definición de tickers y JSON Schema
TICKERS = ["AAPLD","DESPD","KOD","MELID","MSFTD","NVDAD","TEND","VISTD","XOMD"]

SCHEMA = {
    "type": "object",
    "properties": {
        "valoracion_llm": {
            "type": "string",
            "enum": ["positive", "negative", "neutral"]
        },
        "relevante_economia": {
            "type": "integer",
            "enum": [0, 1]
        },
        **{tk: {"type": "integer", "enum": [0, 1]} for tk in TICKERS}
    },
    "required": ["valoracion_llm", "relevante_economia", *TICKERS],
    "additionalProperties": False
}

EXAMPLE = {
    "valoracion_llm": "neutral",
    "relevante_economia": 0,
    **{tk: 0 for tk in TICKERS}
}

def build_strict_prompt(text: str) -> str:
    """Construye prompt con esquema y ejemplo para garantizar JSON válido."""
    schema_str = json.dumps(SCHEMA, indent=2)
    example_str = json.dumps(EXAMPLE, indent=2)
    return (
        "You are an assistant. Validate and output STRICT valid JSON **only**.\n"
        "Here is the JSON Schema for your response:\n"
        f"```json\n{schema_str}\n```\n"
        "Example of valid JSON:\n"
        f"```json\n{example_str}\n```\n"
        "Now, given the following post, output a JSON conforming exactly to the schema:\n\n"
        f"{text}\n"
        "Respond using JSON."
    )

def validate_json(data: dict) -> bool:
    """Valida la respuesta con jsonschema."""
    try:
        validate(instance=data, schema=SCHEMA)
        return True
    except ValidationError as e:
        logger.warning(f"JSON validation error: {e.message}")
        return False

def send_with_retry(prompt: str, retries: int = 2) -> dict:
    """
    Envía el prompt a Ollama, valida contra el esquema y reintenta si falla.
    """
    for attempt in range(retries + 1):
        resp = client.chat(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            format="json"
        )
        content = resp.message.content
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.warning(f"Decode error: {e.msg}")
            data = None

        if isinstance(data, dict) and validate_json(data):
            return data

        logger.info(f"Retrying prompt ({attempt+1}/{retries})")
    raise ValueError("Failed to produce valid JSON after retries")

def process_batch(batch_size: int = 10):
    """Procesa lotes de notas pendientes y almacena resultados."""
    with SessionLocal() as db:  # tipo: Session
        subq = db.query(NotasXUsuarioGemma.id_nota)
        pendientes = (
            db.query(NotasXUsuario)
              .filter(~NotasXUsuario.id_nota.in_(subq))
              .order_by(NotasXUsuario.fecha_publicacion)
              .limit(batch_size)
              .all()
        )

        for nota in pendientes:
            try:
                prompt = build_strict_prompt(nota.contenido)
                data = send_with_retry(prompt, retries=2)

                row = NotasXUsuarioGemma(
                    id_nota=nota.id_nota,
                    valoracion_llm=data["valoracion_llm"],
                    relevante_economia=bool(data["relevante_economia"]),
                    **{tk: bool(data[tk]) for tk in TICKERS}
                )
                db.add(row)
                db.commit()
                logger.info(f"✔️ note {nota.id_nota} -> {data}")

            except Exception as e:
                db.rollback()
                logger.error(f"❌ note {nota.id_nota} failed: {e}")

def main():
    parser = argparse.ArgumentParser(description="Process notes via Ollama with JSON Schema")
    parser.add_argument("-n", "--batch-size", type=int, default=10)
    args = parser.parse_args()
    process_batch(args.batch_size)

if __name__ == "__main__":
    main()

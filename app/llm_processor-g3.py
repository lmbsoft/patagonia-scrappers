#!/usr/bin/env python3
# llm_processor.py
# Procesa lotes de notas_x_usuario pendientes y guarda resultados Gemma en GPU

import os
import logging
import json
import re
import argparse
from transformers import AutoTokenizer, Gemma3ForCausalLM
import torch
from sqlalchemy.orm import Session
from models import SessionLocal, NotasXUsuario, NotasXUsuarioGemma

# ▶ Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("llm_processor.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ▶ Device (GPU si está disponible)
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logger.info(f"Using device: {DEVICE}")

# ▶ Modelo (usar versión 4B por defecto)
MODEL_ID = os.getenv("GEMMA3_MODEL", "google/gemma-3-12b-it")


def load_model():
    tok = AutoTokenizer.from_pretrained(MODEL_ID)
    # Usar float16 en GPU para eficiencia, float32 en CPU
    dtype = torch.float16 if DEVICE.type == "cuda" else torch.float32
    mdl = (
        Gemma3ForCausalLM.from_pretrained(MODEL_ID, torch_dtype=dtype)
        .to(DEVICE)
        .eval()
    )
    return tok, mdl

# ▶ Prompt
TICKERS = ["AAPLD", "DESPD", "KOD", "MELID", "MSFTD", "NVDAD", "TEND", "VISTD", "XOMD"]


def build_prompt(text: str) -> str:
    tickers_str = ", ".join(TICKERS)
    return (
        "You are an assistant that must output STRICT valid JSON **only**.\n"
        "Given the post below, return a JSON object with exactly these keys:\n"
        "  valoracion_llm          – one of: positive, negative, neutral\n"
        "  relevante_economia      – 1 if the post is about stocks/economics, else 0\n"
        + "\n".join([f"  {t} – 1 if the post mentions {t}, else 0" for t in TICKERS])
        + "\nDo NOT wrap the JSON in markdown code fences, and do NOT include any extra keys.\n"
        f"Post: {text}"
    )

# ▶ Utilidades
json_braces = re.compile(r"{.*}", re.S)  # greedy – primera { … última }

def extract_clean_json(text: str) -> dict:
    """
    - Elimina fences ```json … ``` si los hubiese.
    - Extrae la primera a última llave { … } para robustez.
    - Convierte arrays de tickers en booleans.
    """
    # quitar ```json ... ``` o ``` ... ```
    text = re.sub(r"```.*?```", lambda m: m.group(0).strip("```json\n\r "), text, flags=re.S)
    m = json_braces.search(text)
    if not m:
        raise ValueError("No JSON object found")
    obj = json.loads(m.group(0))

    # Si viene lista de tickers, mapear
    if isinstance(obj.get("ticker"), list):
        for tk in TICKERS:
            obj[tk] = 1 if tk in obj["ticker"] else 0
    if isinstance(obj.get("ticker_relevancia"), list):
        for tk in TICKERS:
            obj[tk] = 1 if tk in obj["ticker_relevancia"] else 0

    # normalizar strings "0"/"1"
    obj["relevante_economia"] = int(obj.get("relevante_economia", 0))
    for tk in TICKERS:
        obj[tk] = int(obj.get(tk, 0))
    return obj

# ▶ Procesamiento por lotes
def process_batch(batch_size: int = 10):
    tokenizer, model = load_model()

    with SessionLocal() as db:
        subq = db.query(NotasXUsuarioGemma.id_nota)
        notas = (
            db.query(NotasXUsuario)
            .filter(~NotasXUsuario.id_nota.in_(subq))
            .order_by(NotasXUsuario.fecha_publicacion)
            .limit(batch_size)
            .all()
        )

        for nota in notas:
            try:
                prompt = build_prompt(nota.contenido)

                messages = [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt},
                ]

                inputs = tokenizer.apply_chat_template(
                    messages,
                    add_generation_prompt=True,
                    tokenize=True,
                    return_dict=True,
                    return_tensors="pt",
                )
                # Mover tensores al dispositivo adecuado
                inputs = {k: v.to(DEVICE) for k, v in inputs.items()}

                with torch.inference_mode():
                    gen = model.generate(
                        input_ids=inputs["input_ids"],
                        attention_mask=inputs.get("attention_mask"),
                        max_new_tokens=200,
                        do_sample=False,  # greedy
                    )

                seq_len = inputs["input_ids"].shape[1]
                gen_ids = gen[0, seq_len:]
                raw_out = tokenizer.decode(gen_ids, skip_special_tokens=True).strip()

                data = extract_clean_json(raw_out)

                row = NotasXUsuarioGemma(
                    id_nota=nota.id_nota,
                    valoracion_llm=data.get("valoracion_llm"),
                    relevante_economia=bool(data["relevante_economia"]),
                    **{tk: bool(data[tk]) for tk in TICKERS},
                )
                db.add(row)
                db.commit()
                logger.info(f"✔️ note {nota.id_nota} processed -> {data}")

            except Exception as e:
                db.rollback()
                logger.error(f"❌ note {nota.id_nota} failed: {e}")

# ▶ CLI
def main():
    parser = argparse.ArgumentParser(description="Process batches of notes with Gemma3 on GPU/CPU")
    parser.add_argument("-n", "--batch-size", type=int, default=10)
    args = parser.parse_args()
    process_batch(args.batch_size)

if __name__ == "__main__":
    main()


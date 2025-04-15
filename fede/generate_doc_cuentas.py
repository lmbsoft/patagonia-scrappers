import requests
import json
from pathlib import Path
import pandas as pd
from langdetect import detect


def identify_lang(texto: str):
    return detect(texto)


def get_profile(handle, URL_BASE):
    url = f"{URL_BASE}actor={handle}"
    response = requests.get(url)
    return response.json()



def generate_csv(path_json: Path, output_csv: Path, URL_BASE):
    with open(path_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = []

    for cod_tipo_usuario, users in data.items():
        for nombre, handle in users.items():
            handle_limpio = handle.lstrip("@")
            response = get_profile(handle_limpio, URL_BASE)

            results.append({
                "id_usuario": response["did"],
                "nombre": response["displayName"],
                "handle": handle_limpio,
                "cod_tipo_usuario": cod_tipo_usuario,
                "verificado": None,
                "seguidores": response["followersCount"],
                "cod_pais": None,
                "idioma_principal": identify_lang(response['description']),
                "Score_credibilidad": None
            })

    df = pd.DataFrame(results)
    df.to_csv(output_csv, index=False)


if __name__ == "__main__":
    URL_BASE = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?"
    ruta_json = Path("seguir.json")
    ruta_salida = Path("usuarios_bluesky.csv")
    generate_csv(ruta_json, ruta_salida, URL_BASE)

import os
import requests
from base64 import b64encode
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import pandas_gbq
from google.oauth2 import service_account
import json

# se estiver no ambiente local, carrega as variaveis de ambiente
load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

PROJECT_ID = "my-spotify-475819"
DATASET = "landing"
TABLE = "landing_spotify_tracks"

# credenciais do BigQuery
def get_gcp_credentials():
    json_key = os.getenv("GCP_CREDENTIALS_JSON") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if json_key:
        try:
            info = json.loads(json_key)
            return service_account.Credentials.from_service_account_info(info)
        except json.JSONDecodeError:
            if os.path.exists(json_key):
                return service_account.Credentials.from_service_account_file(json_key)
    raise EnvironmentError(
        "Credenciais do GCP não encontradas. Defina GCP_CREDENTIALS_JSON ou GOOGLE_APPLICATION_CREDENTIALS."
    )

credentials = get_gcp_credentials()

# === Funções principais ===

def get_access_token():
    # Obtém token de acesso do Spotify via refresh token
    url = "https://accounts.spotify.com/api/token"
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = b64encode(auth_str.encode()).decode()

    headers = {"Authorization": f"Basic {b64_auth}"}
    data = {"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN}

    r = requests.post(url, headers=headers, data=data)
    r.raise_for_status()
    return r.json()["access_token"]


def get_last_played_from_bq():
    # Retorna a data que a ultima musica foi executada no BigQuery
    query = f"""
        SELECT MAX(played_at) AS last_played
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """
    try:
        df = pandas_gbq.read_gbq(query, project_id=PROJECT_ID, credentials=credentials)
        if not df.empty and pd.notnull(df.iloc[0]["last_played"]):
            return pd.to_datetime(df.iloc[0]["last_played"])
    except Exception as e:
        print(f"Tabela ainda não existe ou erro ao consultar BQ: {e}")
    return None


def get_recently_played(access_token, after_ts=None):
    # Obtém as últimas 50 músicas tocadas (após after_ts)
    headers = {"Authorization": f"Bearer {access_token}"}
    url = "https://api.spotify.com/v1/me/player/recently-played?limit=50"
    if after_ts:
        url += f"&after={after_ts}"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["items"]


def filter_tracks(tracks):
    # transforma os dados da API em df
    df = pd.json_normalize(tracks, sep='_')
    
    for col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        
    return df

def load_to_bigquery(df):
    # mandando os dados para o bq
    if df.empty:
        print("Nenhum dado para carregar no BigQuery.")
        return

    df["load_datetime"] = pd.Timestamp.now(tz="UTC")

    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=f"{DATASET}.{TABLE}",
        project_id=PROJECT_ID,
        credentials=credentials,
        if_exists="append"
    )
    print(f"{len(df)} linhas adicionadas em {DATASET}.{TABLE}.")

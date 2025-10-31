import os
import pandas as pd
import logging
from spotify_api import get_access_token, get_recently_played, filter_tracks, load_to_bigquery, get_last_played_from_bq

logging.basicConfig(level=logging.INFO)

PROJECT_ID = "my-spotify-475819"
DATASET = "landing"
TABLE = "landing_spotify_tracks"

def main():

    token = get_access_token()
    
    last_played = get_last_played_from_bq()
    after_ts = int(last_played.timestamp() * 1000) if last_played else None

    # buscar músicas novas
    try:
        tracks = get_recently_played(token, after_ts=after_ts)
    except Exception as e:
        logging.error(f"Erro ao buscar músicas recentes: {e}")
        return
    
    df = filter_tracks(tracks)
    
    if df.empty:
        logging.info("Nenhuma música nova encontrada.")
        return
    
    print(df.dtypes)
    # enviar novas músicas para o BigQuery
    load_to_bigquery(df)
    logging.info(f"{len(df)} novas músicas adicionadas.")

if __name__ == "__main__":
    main()

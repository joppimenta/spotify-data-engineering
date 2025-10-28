import os
import pandas as pd
from spotify_api import get_access_token, get_recently_played, filter_tracks, load_to_bigquery, get_last_played_from_bq

PROJECT_ID = "my-spotify"
DATASET = "staging"
TABLE = "songs_played"

def main():

    token = get_access_token()
    
    # pegar o último played_at já salvo no BQ
    last_played = get_last_played_from_bq()
    after_ts = int(last_played.timestamp() * 1000) if last_played else None

    # buscar músicas novas
    tracks = get_recently_played(token, after_ts=after_ts)
    
    df = filter_tracks(tracks)
    
    if df.empty:
        print("Nenhuma música nova encontrada.")
        return
    
    # enviar novas músicas para o BigQuery
    load_to_bigquery(df)
    print(f"{len(df)} novas músicas adicionadas.")

if __name__ == "__main__":
    main()

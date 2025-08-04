import os
import pandas as pd
import sqlite3
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
from dotenv import load_dotenv

load_dotenv()
CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")

auth_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)

def search_kenyan_artists(query="Kenya", limit=20):
    """Search for artists related to Kenya."""
    results = sp.search(q=query, type="artist", limit=limit)
    artists = []
    for item in results['artists']['items']:
        artists.append({
            "artist_id": item['id'],
            "artist_name": item['name'],
            "popularity": item['popularity'],
            "followers": item['followers']['total'],
            "genres": ", ".join(item['genres'])
        })
    return pd.DataFrame(artists)


def get_artist_top_tracks(artist_id, country="KE"):
    """Get top tracks for an artist."""
    tracks_data = []
    results = sp.artist_top_tracks(artist_id, country=country)
    for track in results['tracks']:
        tracks_data.append({
            "artist_id": artist_id,
            "track_name": track['name'],
            "album_name": track['album']['name'],
            "release_date": track['album']['release_date'],
            "track_popularity": track['popularity'],
            "preview_url": track['preview_url']
        })
    return pd.DataFrame(tracks_data)


def save_to_sqlite(df, db_name="spotify_kenya.db"):
    conn = sqlite3.connect(db_name)
    df.to_sql("spotify_data", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Data saved to {db_name} (table: spotify_data)")


def run_pipeline():
    print("Extracting Kenyan artists...")
    artists_df = search_kenyan_artists(limit=30)

    print("Extracting top tracks for each artist...")
    all_tracks = []
    for artist_id in artists_df['artist_id']:
        tracks_df = get_artist_top_tracks(artist_id)
        all_tracks.append(tracks_df)

    tracks_df = pd.concat(all_tracks, ignore_index=True)

    print("Merging artist and track data...")
    final_df = tracks_df.merge(artists_df, on="artist_id", how="left")


    save_to_sqlite(final_df)

if __name__ == "__main__":
    run_pipeline()

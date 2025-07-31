import os
import pandas as pd
from dotenv import load_dotenv
import sqlite3
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy

load_dotenv()

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")

auth_manager = SpotipyClientCredentials(client_id = CLIENT_ID, client_secret = CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager = auth_manager)

def search_kenyan_artists(query = "Kenya", limit =20):
    """
    Search for Kenyan artists using the Spotify API.
    
    Args:
        query (str): The search query, default is "Kenya".
        limit (int): The number of results to return, default is 20.
    
    Returns:
        list: A list of artist names.
    """
    results = sp.search(q=query, type='artist', limit=limit)
    artists = []
    for item in results['artists']['items']:
        artists.append({
            "artist_id":item['id'],
            "artist_name": item['name'],
            "popularity": item['popularity'],
            "followers": item['followers']['total'],
            "genres": item['genres']
        })
    df = pd.DataFrame(artists)
    print(f"Found {len(df)} artists")
    return df
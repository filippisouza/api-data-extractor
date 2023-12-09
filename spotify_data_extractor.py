import spotipy
import pandas as pd
from math import ceil
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

load_dotenv()

SCOPES = ["user-library-read", "user-read-recently-played"]

def get_authentication(scopes):
    spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scopes))
    return spotify


#GET SAVED TRACKS SONGS
def extract_save_tracks(spotify, saved_tracks_file):
    limit = 20 
    iteration = 0
    dfs = []
    results = spotify.current_user_saved_tracks(limit=limit)

    while len(results['items'])>0:
        df = pd.json_normalize(results['items'])
        dfs.append(df)
        iteration += limit 
        results = spotify.current_user_saved_tracks(limit=limit, offset=iteration)

    saved_tracks = pd.concat(dfs)
    saved_tracks.reset_index()
    saved_tracks.to_csv(saved_tracks_file, index=False)
    print('Successful extraction')


# GET SAVED TRACKS FEATURES
def extract_track_features(spotify,saved_tracks_file, audio_features_file):
    saved_tracks = pd.read_csv(saved_tracks_file)
    saved_tracks.reset_index(inplace=True)
    
    n_iterations = ceil(len(saved_tracks)/100)
    dfs = []

    for i in range(0, n_iterations): 
        start = i*100
        stop = (i*100) + 99
        ids = saved_tracks.loc[start:stop, 'track.id'].to_list()  
        results = spotify.audio_features(tracks=ids)
        df = pd.DataFrame(results)
        dfs.append(df)

    audio_features = pd.concat(dfs)
    audio_features.reset_index(inplace=True)
    audio_features.to_csv(audio_features_file, index=False)
    print('Successful extraction')

if __name__ == '__main__':
    spotify = get_authentication(scopes=SCOPES)

    extract_save_tracks(spotify=spotify,
                         saved_tracks_file='./data/spotify_saved_tracks.csv')

    extract_track_features(saved_tracks_file='./data/spotify_saved_tracks.csv',
                       audio_features_file='./data/spotify_audio_features.csv')
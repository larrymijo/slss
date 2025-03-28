import spotipy
from spotipy.oauth2 import SpotifyOAuth
from collections import defaultdict
from datetime import datetime, timedelta
import time
from tenacity import retry, stop_after_attempt, wait_exponential
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# Spotify API credentials
# Replace these with environment variables
SPOTIPY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SPOTIPY_REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI', 'http://127.0.0.1:8080')
SCOPE = 'user-library-read playlist-modify-public playlist-modify-private'

# Add these constants near the top
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 5
DELAY_BETWEEN_BATCHES = 2  # seconds

# Enhance create_spotify_client with better rate limit handling
retry_strategy = Retry(
    total=MAX_RETRIES,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
    respect_retry_after_header=True
)

def create_spotify_client():
    """Create Spotify client with custom timeout and retry settings"""
    print("🔌 Connecting to Spotify API...")
    try:
        auth_manager = SpotifyOAuth(
            client_id=SPOTIPY_CLIENT_ID,
            client_secret=SPOTIPY_CLIENT_SECRET,
            redirect_uri=SPOTIPY_REDIRECT_URI,
            scope=SCOPE)
        
        # Configure more robust retry strategy
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=2,  # Increased from 1 for better rate limit handling
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]
        )
        
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        
        # Add to create_spotify_client()
        client = spotipy.Spotify(
        auth_manager=auth_manager,
        requests_timeout=REQUEST_TIMEOUT,
        retries=MAX_RETRIES,
        requests_session=session,
        max_retries=3,  # Additional protection
        status_retries=3
        )
        print("✅ Successfully connected to Spotify API")
        return client
    except Exception as e:
        print(f"❌ Failed to connect to Spotify API: {str(e)}")
        raise

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_artist_genres(sp, artist_ids):
    """Get genres with caching"""
    global cache_last_updated
    
    # Check if cache needs to be cleared
    if cache_last_updated and (datetime.now() - cache_last_updated).total_seconds() > CACHE_EXPIRY:
        artist_genre_cache.clear()
    
    uncached = [aid for aid in artist_ids if aid not in artist_genre_cache]
    if uncached:
        artists_data = sp.artists(uncached)['artists']
        for artist in artists_data:
            if artist:
                artist_genre_cache[artist['id']] = artist.get('genres', [])
        cache_last_updated = datetime.now()
    
    return {aid: artist_genre_cache.get(aid, []) for aid in artist_ids}

# Update get_genres_for_tracks to use this
# artist_genres = get_artist_genres(sp, list(artists))

# Modify get_genres_for_tracks to include caching
def get_genres_for_tracks(sp, track_ids, batch_num, total_batches):
    """Get genres for multiple tracks with retry logic"""
    print(f"\n🔍 Processing batch {batch_num}/{total_batches} ({len(track_ids)} tracks)...")
    try:
        start_time = time.time()
        # Remove timeout parameter - using session-level timeout instead
        tracks_data = sp.tracks(track_ids)['tracks']
        
        print("📡 Fetching track artist data...")
        artists = set()
        
        # Skip None tracks (sometimes happens with deleted tracks)
        valid_tracks = [t for t in tracks_data if t is not None]
        if len(valid_tracks) != len(track_ids):
            print(f"⚠️ Warning: {len(track_ids) - len(valid_tracks)} tracks not found")
        
        for track in valid_tracks:
            for artist in track['artists']:
                artists.add(artist['id'])
        
        print(f"🎤 Found {len(artists)} unique artists, fetching genres...")
        genres = defaultdict(list)
        # Remove timeout parameter here too
        artist_data = sp.artists(list(artists))['artists']
        
        # Add batch timeout check (5 minutes max per batch)
        if time.time() - start_time > 300:  
            raise Exception("Batch processing timeout (5 minutes exceeded)")
            
        print("🔗 Matching genres to tracks...")
        # Current artist genre matching (lines 86-93)
        # Replace with more efficient matching:
        # In get_genres_for_tracks:
        track_artist_map = {t['id']: [a['id'] for a in t['artists']] for t in valid_tracks}
        genre_tracks = defaultdict(list)
        
        for track_id, artist_ids in track_artist_map.items():
            for artist_id in artist_ids:
                for genre in artist_genres.get(artist_id, []):
                    genre_tracks[genre].append(track_id)
        
        return genre_tracks
        for artist in artist_data:
            if artist:
                for genre in artist.get('genres', []):
                    for track_id, artist_ids in track_artist_map.items():
                        if artist['id'] in artist_ids:
                            genres[genre].append(track_id)
        return genres
    except Exception as e:
        print(f"⚠️ Error processing batch: {str(e)}")
        time.sleep(5)
        raise

def generate_report(all_genres, playlists_created, playlists_updated):
    """Generate a detailed report of changes made and save to file"""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_filename = f"spotify_genre_report_{timestamp}.txt"
    
    try:
        report = f"""
=== Spotify Genre Organizer Report ===
Timestamp: {timestamp.replace('_', ' ')}
Total genres processed: {len(all_genres)}
Total playlists created: {len(playlists_created)}
Total playlists updated: {len(playlists_updated)}

Playlists Created:
"""
        for playlist in playlists_created:
            report += f" - {playlist}\n"
        
        report += "\nPlaylists Updated:\n"
        for playlist in playlists_updated:
            report += f" - {playlist}\n"
        
        report += "\nGenre Distribution:\n"
        for genre, tracks in all_genres.items():
            report += f" - {genre}: {len(tracks)} tracks\n"
        
        # Ensure directory exists and save to file
        import os
        os.makedirs(os.path.dirname(report_filename) or '.', exist_ok=True)
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"\n📄 Report successfully saved to: {os.path.abspath(report_filename)}")
        return report
        
    except Exception as e:
        print(f"\n⚠️ Failed to save report: {str(e)}")
        return None

def update_genre_playlists(sp, genres):
    """Create or update playlists for each genre"""
    user_id = sp.current_user()['id']
    playlists_created = []
    playlists_updated = []
    
    for genre, track_ids in genres.items():
        playlist_name = f"Genre: {genre.title()}"
        action = "updated"
        
        # Check if playlist exists
        playlists = sp.current_user_playlists()
        playlist_id = None
        for pl in playlists['items']:
            if pl['name'] == playlist_name:
                playlist_id = pl['id']
                break
        
        # Create new playlist if needed
        if not playlist_id:
            playlist = sp.user_playlist_create(
                user_id, playlist_name, public=False)
            playlist_id = playlist['id']
            action = "created"
            playlists_created.append(playlist_name)
        else:
            playlists_updated.append(playlist_name)
            
        # Clear existing tracks and add new ones
        sp.playlist_replace_items(playlist_id, [])
        sp.playlist_add_items(playlist_id, track_ids)
        
        print(f"Playlist {playlist_name} {action} with {len(track_ids)} tracks")
    
    return playlists_created, playlists_updated

def main():
    # Clear cache at startup
    import os
    cache_path = os.path.join(os.path.dirname(__file__), '.cache')
    if os.path.exists(cache_path):
        os.remove(cache_path)
        print("🗑️ Cleared existing Spotify API cache")
    
    # Replace authentication with:
    sp = create_spotify_client()
    
    # Get all liked tracks with better progress tracking
    print("\n🔎 Fetching your liked songs from Spotify...")
    liked_tracks = []
    results = sp.current_user_saved_tracks(limit=50)
    total_songs = results['total']
    print(f"📊 Total liked songs to process: {total_songs}")
    
    while results:
        liked_tracks.extend([item['track']['id'] for item in results['items']])
        print(f"📥 Retrieved {len(liked_tracks)}/{total_songs} songs ({len(liked_tracks)/total_songs:.1%})...")
        results = sp.next(results) if results['next'] else None
    
    # Adjust batch size based on library size
    # Adjust these constants at the top of the file
    REQUEST_TIMEOUT = 45  # Increased from 30
    MAX_RETRIES = 7       # Increased from 5
    DELAY_BETWEEN_BATCHES = 3  # Increased from 2
    
    # Modify the batch size calculation to be more aggressive for large libraries
    # Current batch size calculation (line 206)
    # Replace current batch size calculation with:
    def calculate_batch_size(total_songs):
        """Dynamically calculate batch size based on library size"""
        base_size = 50  # Spotify's max tracks per request
        if total_songs > 3000:
            return min(base_size, 20)  # Smaller batches for very large libraries
        elif total_songs > 1000:
            return min(base_size, 30)
        return min(base_size, 50)  # Default to max allowed
    
    # In main():
    batch_size = calculate_batch_size(total_songs)
    
    # Optimized version considering API limits (100 tracks/artists per call)
    batch_size = min(
    50,  # Spotify's max tracks per request
    max(20, 100 - (total_songs // 100))  # Balance between size and API limits
    )
    
    # In get_genres_for_tracks, add this timeout check at the start
    # Remove these lines (210-212):
    # if time.time() - start_time > 120:  # 2 minute timeout per batch
    #     raise Exception("Batch timeout exceeded")
    total_batches = (len(liked_tracks) + batch_size - 1) // batch_size
    
    print(f"\n🎵 Processing {len(liked_tracks)} songs in {total_batches} batches (~{batch_size} tracks/batch)...")
    all_genres = defaultdict(list)
    
    # Remove these lines (222-224) since they reference undefined variables:
    # del tracks_data  # Free memory after processing
    # del artist_data
    
    for i in range(0, len(liked_tracks), batch_size):
        batch = liked_tracks[i:i+batch_size]
        print(f"\n🔍 Processing batch {i//batch_size+1}/{total_batches} with IDs: {batch}")
        try:
            start_time = time.time()
            # Remove redundant timeout check here (232-233)
            genres = get_genres_for_tracks(sp, batch, (i//batch_size)+1, total_batches)
            
            for genre, ids in genres.items():
                all_genres[genre].extend(ids)
            
            # Dynamic delay based on processing time
            elapsed = time.time() - start_time
            delay = max(DELAY_BETWEEN_BATCHES, elapsed * 0.5)  # Wait at least half the processing time
            print(f"⏳ Waiting {delay:.1f}s before next batch...")
            time.sleep(delay)
            
        except Exception as e:
            print(f"⚠️ Skipping batch due to unrecoverable error: {str(e)}")
            continue
    
    print("✅ Successfully authenticated with Spotify")
    print(f"🔍 Found {len(liked_tracks)} liked songs to process...")
    
    # Update playlists and get change report
    playlists_created, playlists_updated = update_genre_playlists(sp, all_genres)
    generate_report(all_genres, playlists_created, playlists_updated)
    
    print("🎉 All done! Check your Spotify account for the changes")
    print(f"Found {len(all_genres)} distinct genres in your library")
    for genre in all_genres:
        print(f" - {genre}: {len(all_genres[genre])} songs")
    
    print("✅ Playlist update complete! Check your Spotify account")

if __name__ == '__main__':
    main()


def process_batch(sp, batch, batch_num, total_batches):
    """Get genres for multiple tracks with retry logic"""
    print(f"\n🔍 Processing batch {batch_num}/{total_batches} ({len(track_ids)} tracks)...")
    try:
        start_time = time.time()
        # Remove timeout parameter - using session-level timeout instead
        tracks_data = sp.tracks(track_ids)['tracks']
        
        print("📡 Fetching track artist data...")
        artists = set()
        
        # Skip None tracks (sometimes happens with deleted tracks)
        valid_tracks = [t for t in tracks_data if t is not None]
        if len(valid_tracks) != len(track_ids):
            print(f"⚠️ Warning: {len(track_ids) - len(valid_tracks)} tracks not found")
        
        for track in valid_tracks:
            for artist in track['artists']:
                artists.add(artist['id'])
        
        print(f"🎤 Found {len(artists)} unique artists, fetching genres...")
        genres = defaultdict(list)
        # Remove timeout parameter here too
        artist_data = sp.artists(list(artists))['artists']
        
        # Add batch timeout check (5 minutes max per batch)
        if time.time() - start_time > 300:  
            raise Exception("Batch processing timeout (5 minutes exceeded)")
            
        print("🔗 Matching genres to tracks...")
        for artist in artist_data:
            if artist is None:  # Skip None artists
                continue
            for genre in artist.get('genres', []):
                for track_id in track_ids:
                    track_info = sp.track(track_id)
                    if track_info and artist['id'] in [a['id'] for a in track_info['artists']]:
                        genres[genre].append(track_id)
        return genres
    except Exception as e:
        print(f"⚠️ Error processing batch: {str(e)}")
        time.sleep(5)
        raise

# Add this near imports
from concurrent.futures import ThreadPoolExecutor, as_completed

# Replace the batch loop in main() with:
with ThreadPoolExecutor(max_workers=2) as executor:
    futures = {
        executor.submit(
            get_genres_for_tracks,
            sp,
            liked_tracks[i:i+batch_size],
            (i//batch_size)+1,
            total_batches
        ): i for i in range(0, len(liked_tracks), batch_size)
    }
    
    for future in as_completed(futures):
        try:
            batch_genres = future.result()
            for genre, ids in batch_genres.items():
                all_genres[genre].extend(ids)
        except Exception as e:
            print(f"⚠️ Batch failed: {str(e)}")


def validate_credentials(client_id, client_secret):
    if not client_id or not client_secret:
        raise ValueError("Spotify API credentials must be provided")
    if len(client_id) != 32 or len(client_secret) != 32:
        raise ValueError("Invalid Spotify credential format")
import psycopg2
import requests
import time
import logging
from tqdm import tqdm
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("chess_ratings_update.log"),
    ]
)

def ensure_table_columns(cursor):
    """Ensure all required columns exist in the players table"""
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'players'
    """)
    existing_columns = {row[0] for row in cursor.fetchall()}
    
    required_columns = {
        'chesscom_bullet_rating', 'chesscom_blitz_rating', 'chesscom_rapid_rating',
        'chesscom_bullet_last_played', 'chesscom_blitz_last_played', 'chesscom_rapid_last_played',
        'chesscom_bullet_rd', 'chesscom_blitz_rd', 'chesscom_rapid_rd'
    }
    
    for col in required_columns:
        if col not in existing_columns:
            logging.info(f"Adding column {col} to players table")
            cursor.execute(f"""
                ALTER TABLE players
                ADD COLUMN {col} INTEGER
            """)
    
    # ensure timestamp column exists
    if 'last_chesscom_update' not in existing_columns:
        cursor.execute("""
            ALTER TABLE players
            ADD COLUMN last_chesscom_update TIMESTAMP
        """)
    
    return True

def fetch_chesscom_data(username):
    """Fetch Chess.com ratings for a given username"""
    url = f'https://api.chess.com/pub/player/{username}/stats'
    try:
        response = requests.get(
            url,
            headers={'User-Agent': 'RatingAnalyzer/1.0 (https://astral.lol/)'},
            timeout=10
        )
        
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            logging.error(f"HTTP {response.status_code} for {username}")
            return None

        data = response.json()
        
        ratings = {
            'bullet_rating': None, 'bullet_last_played': None, 'bullet_rd': None,
            'blitz_rating': None, 'blitz_last_played': None, 'blitz_rd': None,
            'rapid_rating': None, 'rapid_last_played': None, 'rapid_rd': None
        }
        
        game_types = {
            'chess_bullet': 'bullet',
            'chess_blitz': 'blitz',
            'chess_rapid': 'rapid'
        }
        
        for chess_type, prefix in game_types.items():
            if chess_type in data and 'last' in data[chess_type]:
                last_data = data[chess_type]['last']
                ratings[f'{prefix}_rating'] = last_data.get('rating')
                ratings[f'{prefix}_last_played'] = last_data.get('date')
                ratings[f'{prefix}_rd'] = last_data.get('rd')
        
        logging.info(f"Found user {username}, might have blitz rating {ratings['blitz_rating']}")

        return ratings
        
    except Exception as e:
        logging.error(f"Error fetching {username}: {str(e)}")
        return None

def update_player_ratings(batch_size=100, rate_limit_delay=0.1):
    """Update PostgreSQL database with Chess.com ratings"""
    try:
        conn = psycopg2.connect(
            user="postgres",
            password="postgres",
            host="localhost",
            dbname="astral_db"
        )
        cursor = conn.cursor()
        
        ensure_table_columns(cursor)
        conn.commit()
        
        # get players ordered by last update time
        cursor.execute("""
            SELECT username FROM players
            ORDER BY last_chesscom_update NULLS FIRST
        """)
        players = [row[0] for row in cursor.fetchall()]
        
        total_players = len(players)
        logging.info(f"Starting update for {total_players} players")
        
        batch = []
        start_time = time.time()
        
        for i, username in enumerate(tqdm(players, desc="Updating players")):
            if rate_limit_delay > 0 and i > 0:
                time.sleep(rate_limit_delay)
            
            ratings = fetch_chesscom_data(username)
            if not ratings:
                continue
            
            batch.append((
                username,
                ratings['bullet_rating'],
                ratings['bullet_last_played'],
                ratings['bullet_rd'],
                ratings['blitz_rating'],
                ratings['blitz_last_played'],
                ratings['blitz_rd'],
                ratings['rapid_rating'],
                ratings['rapid_last_played'],
                ratings['rapid_rd'],
                datetime.now()
            ))
            
            if len(batch) >= batch_size:
                execute_batch_update(cursor, batch)
                conn.commit()
                batch = []
        
        if batch:
            execute_batch_update(cursor, batch)
            conn.commit()
        
        elapsed_time = time.time() - start_time
        logging.info(f"Updated {total_players} players in {elapsed_time:.2f}s")
        
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        conn.rollback()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if conn:
            conn.close()

def execute_batch_update(cursor, batch):
    """Execute batched UPSERT operation"""
    query = """
        INSERT INTO players (
            username, 
            chesscom_bullet_rating, chesscom_bullet_last_played, chesscom_bullet_rd,
            chesscom_blitz_rating, chesscom_blitz_last_played, chesscom_blitz_rd,
            chesscom_rapid_rating, chesscom_rapid_last_played, chesscom_rapid_rd,
            last_chesscom_update
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (username) DO UPDATE SET
            chesscom_bullet_rating = EXCLUDED.chesscom_bullet_rating,
            chesscom_bullet_last_played = EXCLUDED.chesscom_bullet_last_played,
            chesscom_bullet_rd = EXCLUDED.chesscom_bullet_rd,
            chesscom_blitz_rating = EXCLUDED.chesscom_blitz_rating,
            chesscom_blitz_last_played = EXCLUDED.chesscom_blitz_last_played,
            chesscom_blitz_rd = EXCLUDED.chesscom_blitz_rd,
            chesscom_rapid_rating = EXCLUDED.chesscom_rapid_rating,
            chesscom_rapid_last_played = EXCLUDED.chesscom_rapid_last_played,
            chesscom_rapid_rd = EXCLUDED.chesscom_rapid_rd,
            last_chesscom_update = EXCLUDED.last_chesscom_update
    """
    cursor.executemany(query, batch)

if __name__ == "__main__":
    update_player_ratings()

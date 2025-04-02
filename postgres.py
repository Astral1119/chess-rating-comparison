import psycopg2

def process_data():
    conn = psycopg2.connect(
        user="postgres",
        password="postgres",
        host="localhost",
        dbname="astral_db"
    )
    cursor = conn.cursor()

    # create table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS players (
        username TEXT PRIMARY KEY,
        bullet_rating INT,
        blitz_rating INT,
        rapid_rating INT,
        last_updated TIMESTAMP
    )
    """)
    conn.commit()

    # create temporary table
    cursor.execute("""
    CREATE TEMPORARY TABLE temp_ratings (
        username TEXT,
        time_control INT,
        rating INT,
        entry_seq BIGINT
    )
    """)
    conn.commit()

    print("beginning copy")

    with open('/home/chriscarpenter/PycharmProjects/chess-ratings/temp_data.csv', 'r') as f:
        cursor.copy_expert("COPY temp_ratings FROM STDIN WITH (FORMAT csv, HEADER true)", f)
    conn.commit()

    print("copy successful")

    # create index for performance
    cursor.execute("""
    CREATE INDEX ON temp_ratings (username, time_control, entry_seq DESC)
    """)
    conn.commit()

    print("adding to main table")

    # upsert most recent into main table
    cursor.execute("""
    INSERT INTO players (username, bullet_rating, blitz_rating, rapid_rating)
    SELECT 
        username,
        MAX(CASE WHEN time_control = 1 THEN rating::int END) AS bullet_rating,
        MAX(CASE WHEN time_control = 2 THEN rating::int END) AS blitz_rating,
        MAX(CASE WHEN time_control = 3 THEN rating::int END) AS rapid_rating
    FROM (
        SELECT DISTINCT ON (username, time_control) 
            username, time_control, rating
        FROM temp_ratings
        ORDER BY username, time_control, entry_seq DESC
    ) AS latest
    GROUP BY username
    ON CONFLICT (username) DO UPDATE SET
        bullet_rating = COALESCE(EXCLUDED.bullet_rating, players.bullet_rating),
        blitz_rating = COALESCE(EXCLUDED.blitz_rating, players.blitz_rating),
        rapid_rating = COALESCE(EXCLUDED.rapid_rating, players.rapid_rating),
        last_updated = GREATEST(EXCLUDED.last_updated, players.last_updated)
    """)
    conn.commit()

    print("success")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    process_data()

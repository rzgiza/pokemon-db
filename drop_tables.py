# Drop all tables created from create_db.py
import psycopg2
import hidden

# Load the secrets
secrets = hidden.secrets()

conn = psycopg2.connect(
    host=secrets['host'],
    port=secrets['port'],
    database=secrets['database'],
    user=secrets['user'],
    password=secrets['pass'],
    connect_timeout=3
)
cur = conn.cursor()

sql = """
DROP TABLE IF EXISTS js_pokemon;
DROP TABLE IF EXISTS js_species;
DROP TABLE IF EXISTS pokedex;
"""
cur.execute(sql)
print(sql)

conn.commit()

cur.close()
conn.close()


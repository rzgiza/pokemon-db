# Drop all tables created from create_db.py
import psycopg2
import hidden


# Load the secrets
secrets = hidden.secrets()
conn = psycopg2.connect(host=secrets['host'], port=secrets['port'], database=secrets['database'],
                        user=secrets['user'], password=secrets['pass'], connect_timeout=3)
cur = conn.cursor()

# Drop all tables created in create_db.py
sql = r"""
DROP TABLE IF EXISTS js_pokemon;
DROP TABLE IF EXISTS js_species;
DROP TABLE IF EXISTS js_types;
DROP TABLE IF EXISTS js_evo;
DROP TABLE IF EXISTS js_moves;
DROP TABLE IF EXISTS js_abilities;
DROP TABLE IF EXISTS pokedex CASCADE;
DROP TABLE IF EXISTS types CASCADE;
DROP TABLE IF EXISTS pokemon_moves;
DROP TABLE IF EXISTS pokemon_abilities;
DROP TABLE IF EXISTS moves CASCADE;
DROP TABLE IF EXISTS abilities CASCADE;
DROP TABLE IF EXISTS trainer CASCADE;
DROP TABLE IF EXISTS trainer_moves;
"""
cur.execute(sql)
print(sql)

conn.commit()

cur.close()
conn.close()


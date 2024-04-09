# python3 create_db.py
# Pulls data from the https://pokeapi.co API and creates a Pokemon database
import psycopg2
import hidden
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from pprint import pprint


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
print("Connection opened to PGSQL database.")

sql = """
DROP TABLE IF EXISTS js_pokemon;
DROP TABLE IF EXISTS pokedex;
"""
cur.execute(sql)
print(sql)

sql = """
CREATE TABLE IF NOT EXISTS js_pokemon (
    id INTEGER, 
    body JSONB
);

CREATE TABLE IF NOT EXISTS pokedex (
    id INTEGER PRIMARY KEY, name VARCHAR(20) UNIQUE, height NUMERIC, weight NUMERIC, hp NUMERIC,
    attack NUMERIC, defense NUMERIC, s_attack NUMERIC, s_defense NUMERIC, speed NUMERIC, 
    type TEXT [], info TEXT, evo_set INTEGER
);
"""
cur.execute(sql)
print(sql)

print("Async scraping pokemon data from: https://pokeapi.co/api/v2/pokemon/")
print("Results dumped into js_pokemon for further processing.")

futures = []
id_texts = []
with FuturesSession(max_workers=10) as session:
    for i in range(1, 1026):
        url = 'https://pokeapi.co/api/v2/pokemon/' + str(i)
        future = session.get(url)
        future.i = i
        futures.append(future)

    for future in as_completed(futures):
        response = future.result()
        pprint({'pokemon_id': future.i})
        id_texts.append((future.i, response.text))

args = ','.join([cur.mogrify("(%s,%s)", tup).decode('utf-8') for tup in id_texts])
cur.execute("INSERT INTO js_pokemon VALUES " + args)

conn.commit()

print("Records inserted into js_pokemon: ", len(id_texts))

sql = """
INSERT INTO pokedex (
    id, name, height, weight, hp, attack, defense, s_attack, s_defense, speed, type
)
SELECT (body->'id')::int as id, 
        body->'species'->>'name',
       (body->'height')::numeric,
       (body->'weight')::numeric, 
       (body->'stats'->0->'base_stat')::numeric,
       (body->'stats'->1->'base_stat')::numeric,
       (body->'stats'->2->'base_stat')::numeric,
       (body->'stats'->3->'base_stat')::numeric,
       (body->'stats'->4->'base_stat')::numeric,
       (body->'stats'->5->'base_stat')::numeric,
        translate(jsonb_path_query_array(body->'types', '$.type.name')::text, '[]', '{}')::text[]
FROM js_pokemon ORDER BY id ASC;
"""
cur.execute(sql)
print(sql)

conn.commit()

cur.close()
conn.close()
print("Connection closed to PGSQL database.")

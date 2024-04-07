# python3 papi.py
# Pulls data from the https://pokeapi.co API and creates Pokemon database

import psycopg2
import hidden
import requests


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
    id INTEGER PRIMARY KEY, 
    name VARCHAR(20) UNIQUE,
    height NUMERIC,
    weight NUMERIC,
    hp NUMERIC,
    attack NUMERIC,
    defense NUMERIC,
    s_attack NUMERIC,
    s_defense NUMERIC,
    speed NUMERIC,
    type TEXT [],
    info TEXT,
    evo_set INTEGER
);
"""
cur.execute(sql)
print(sql)

sql = 'INSERT INTO js_pokemon (id, body) VALUES (%s, %s);'
for i in range(1, 11):
    url = 'https://pokeapi.co/api/v2/pokemon/' + str(i)
    response = requests.get(url)
    text = response.text   # json string
    cur.execute(sql, (i, text))
    print('Exec: ' + url)

conn.commit()

sql = "SELECT count(*) FROM js_pokemon;"
print(sql)
cur.execute(sql)

row = cur.fetchone()
if row is None:
    print('Row not found')
else:
    print('Found', row)

sql = """
INSERT INTO pokedex (
    id, name, height, weight, hp, attack, defense, s_attack, s_defense, speed, type
)
SELECT (body->'id')::int, 
        body->>'name',
       (body->'height')::numeric,
       (body->'weight')::numeric, 
       (body->'stats'->0->'base_stat')::numeric,
       (body->'stats'->1->'base_stat')::numeric,
       (body->'stats'->2->'base_stat')::numeric,
       (body->'stats'->3->'base_stat')::numeric,
       (body->'stats'->4->'base_stat')::numeric,
       (body->'stats'->5->'base_stat')::numeric,
        translate(jsonb_path_query_array(body->'types', '$.type.name')::text, '[]', '{}')::text[]
FROM js_pokemon;
"""
cur.execute(sql)
print(sql)

conn.commit()

cur.close()
conn.close()


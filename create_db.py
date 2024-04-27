# python3 create_db.py
# Pulls data from the https://pokeapi.co API and creates a Pokemon database
import psycopg2
from psycopg2.extensions import AsIs
import hidden
from get_url import get_url


# Number of Pokemon desired (or list/iterable object of integers [1:1025] possible)
NUM_OF_POKE = 1025

# Load the credentials, connect to PGSQL database, and create cursor
secrets = hidden.secrets()
conn = psycopg2.connect(host=secrets['host'], port=secrets['port'], database=secrets['database'],
                        user=secrets['user'], password=secrets['pass'], connect_timeout=3)
cur = conn.cursor()
print("Connection opened to PGSQL database.")

# Drop existing tables created in this program to start from scratch.
sql = """
DROP TABLE IF EXISTS js_pokemon;
DROP TABLE IF EXISTS js_species;
DROP TABLE IF EXISTS js_types;
DROP TABLE IF EXISTS js_evo;
DROP TABLE IF EXISTS js_moves;
DROP TABLE IF EXISTS js_abilities;
DROP TABLE IF EXISTS pokedex;
DROP TABLE IF EXISTS types;
DROP TABLE IF EXISTS pokemon_moves;
DROP TABLE IF EXISTS pokemon_abilities;
"""
cur.execute(sql)
print(sql)

conn.commit()

# Create json helper tables (js_<name>) and final database tables
sql = """
CREATE TABLE IF NOT EXISTS js_pokemon (id INTEGER PRIMARY KEY, body JSONB);
CREATE TABLE IF NOT EXISTS js_species (id INTEGER PRIMARY KEY, body JSONB);
CREATE TABLE IF NOT EXISTS js_types (id INTEGER PRIMARY KEY, body JSONB);
CREATE TABLE IF NOT EXISTS js_evo (id INTEGER PRIMARY KEY, body JSONB);
CREATE TABLE IF NOT EXISTS js_moves (id INTEGER PRIMARY KEY, body JSONB);
CREATE TABLE IF NOT EXISTS js_abilities (id INTEGER PRIMARY KEY, body JSONB);
CREATE TABLE IF NOT EXISTS pokedex (
    id INTEGER PRIMARY KEY, name VARCHAR(20) UNIQUE, height NUMERIC, weight NUMERIC, hp NUMERIC,
    attack NUMERIC, defense NUMERIC, s_attack NUMERIC, s_defense NUMERIC, speed NUMERIC, 
    type TEXT [], evo_set INTEGER, info TEXT
);
CREATE TABLE IF NOT EXISTS types (id INTEGER PRIMARY KEY, name VARCHAR(20) UNIQUE);
CREATE TABLE IF NOT EXISTS pokemon_moves (
    poke_id INTEGER, move_id INTEGER, PRIMARY KEY (poke_id, move_id), UNIQUE (move_id, poke_id)
);
CREATE TABLE IF NOT EXISTS pokemon_abilities (
    poke_id INTEGER, ability_id INTEGER, PRIMARY KEY (poke_id, ability_id), UNIQUE (ability_id, poke_id)
);
"""
cur.execute(sql)
print(sql)

conn.commit()

# Get initial json data from pokeapi and insert into json tables (js_<name>)
# Note some pages in evolution-chain data are missing from api (look at page = 210), data is still complete though
url_paths = ['https://pokeapi.co/api/v2/pokemon/', 'https://pokeapi.co/api/v2/pokemon-species/',
             'https://pokeapi.co/api/v2/type/', 'https://pokeapi.co/api/v2/evolution-chain/']
json_tables = ['js_pokemon', 'js_species', 'js_types', 'js_evo']
indexes = [NUM_OF_POKE, NUM_OF_POKE, None, 549]
for i in range(len(url_paths)):
    id_texts = get_url(url_paths[i], indexes[i])
    values = ','.join([cur.mogrify("(%s,%s)", tup).decode('utf-8') for tup in id_texts])
    cur.execute("INSERT INTO %s VALUES %s;", (AsIs(json_tables[i]), AsIs(values)))

conn.commit()

# Insert data into the pokedex table from js_pokemon, js_evo, js_species
sql = r"""
WITH cte AS (
    SELECT (body->'id')::int as id, 
            unnest(translate(jsonb_path_query_array(body->'flavor_text_entries', '$.language.name')::text, 
                   '[]', '{}')::text[]) as language,
            unnest(translate(regexp_replace(jsonb_path_query_array(body->'flavor_text_entries', '$.flavor_text')::text, 
                   '\\n|\\f', ' ', 'g'), '[]', '{}')::text[]) as info
    FROM js_species
), rownum AS (
    SELECT row_number() over(order by (select NULL)) as rn, * FROM cte
), engchk AS (
    SELECT row_number() over(partition by id order by rn) as first_eng, * FROM rownum
), infotb AS (
    SELECT rank() over(partition by id order by first_eng) as rk, * FROM engchk WHERE language = 'en'
)
INSERT INTO pokedex (
    id, name, height, weight, hp, attack, defense, s_attack, s_defense, speed, type, evo_set, info
)
SELECT pd.*, es.evo_set, it.info
FROM (
    SELECT (body->'id')::int as id, 
            body->'species'->>'name' as name,
           (body->'height')::numeric,
           (body->'weight')::numeric, 
           (body->'stats'->0->'base_stat')::numeric,
           (body->'stats'->1->'base_stat')::numeric,
           (body->'stats'->2->'base_stat')::numeric,
           (body->'stats'->3->'base_stat')::numeric,
           (body->'stats'->4->'base_stat')::numeric,
           (body->'stats'->5->'base_stat')::numeric,
            translate(jsonb_path_query_array(body->'types', '$.type.name')::text, '[]', '{}')::text[]
    FROM js_pokemon
) AS pd
LEFT JOIN (
    SELECT (body->'id')::int as evo_set, 
            unnest(translate((jsonb_path_query_array(body->'chain', '$.species.name') || 
                   jsonb_path_query_array(body->'chain', '$.*.species.name') || 
                   jsonb_path_query_array(body->'chain', '$.*.*.species.name'))::text, '[]', '{}')::text[]) as name
    FROM js_evo
) AS es 
    ON pd.name = es.name
LEFT JOIN (SELECT id, info FROM infotb WHERE rk = 1) AS it 
    ON pd.id = it.id
ORDER BY pd.id;
"""
cur.execute(sql)
print(sql)

# Insert data into the types table from js_types
sql = r"""
INSERT INTO types (id, name)
SELECT substring(unnest(translate(jsonb_path_query_array(body->'results', '$.url')::text, 
                 '[]', '{}')::text[]) from '.+/([0-9]+)/$')::int,
       unnest(translate(jsonb_path_query_array(body->'results', '$.name')::text, '[]', '{}')::text[])
FROM js_types; 
"""
cur.execute(sql)
print(sql)

# Insert data into the pokemon_moves table from js_pokemon
sql = r"""
INSERT INTO pokemon_moves (poke_id, move_id)
SELECT (body->'id')::int,
        substring(unnest(translate(jsonb_path_query_array(body->'moves', '$.move.url')::text, 
                  '[]', '{}')::text[]) from '.+/([0-9]+)/$')::int
FROM js_pokemon;
"""
cur.execute(sql)
print(sql)

# Insert data into the pokemon_abilities table from js_pokemon
# Some Pokemon have duplicate abilities listed so SELECT DISTINCT is used (look at pokemon_id = 948 or 949)
sql = r"""
INSERT INTO pokemon_abilities (poke_id, ability_id)
SELECT DISTINCT (body->'id')::int,
                 substring(unnest(translate(jsonb_path_query_array(body->'abilities', '$.ability.url')::text, 
                           '[]', '{}')::text[]) from '.+/([0-9]+)/$')::int
FROM js_pokemon;
"""
cur.execute(sql)
print(sql)

conn.commit()

# Get move_ids from pokemon_moves table
sql = r"SELECT DISTINCT move_id FROM pokemon_moves ORDER BY move_id;"
cur.execute(sql)
print(sql)
move_ids = cur.fetchall()

conn.commit()

# Get ability_ids from pokemon_abilities table
sql = r"SELECT DISTINCT ability_id FROM pokemon_abilities ORDER BY ability_id;"
cur.execute(sql)
print(sql)
ability_ids = cur.fetchall()

conn.commit()

# Get final json data from pokeapi and insert into json tables (js_<name>)
MOVES_INDEX = [x[0] for x in move_ids]
ABILITIES_INDEX = [y[0] for y in ability_ids]
url_paths = ['https://pokeapi.co/api/v2/move/', 'https://pokeapi.co/api/v2/ability/']
json_tables = ['js_moves', 'js_abilities']
indexes = [MOVES_INDEX, ABILITIES_INDEX]
for i in range(len(url_paths)):
    id_texts = get_url(url_paths[i], indexes[i])
    values = ','.join([cur.mogrify("(%s,%s)", tup).decode('utf-8') for tup in id_texts])
    cur.execute("INSERT INTO %s VALUES %s;", (AsIs(json_tables[i]), AsIs(values)))

conn.commit()

cur.close()
conn.close()
print("Connection closed to PGSQL database.")

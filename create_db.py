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

# Create json helper tables (js_<name>) and final database tables
sql = r"""
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
CREATE TABLE IF NOT EXISTS moves (
    id INTEGER PRIMARY KEY, name VARCHAR(50) UNIQUE, pp INTEGER, damage NUMERIC, accuracy NUMERIC,
    type INTEGER REFERENCES types (id) ON DELETE CASCADE, info TEXT
);
CREATE TABLE IF NOT EXISTS abilities (id INTEGER PRIMARY KEY, name VARCHAR(50) UNIQUE, info TEXT);
CREATE TABLE IF NOT EXISTS trainer (
    id SERIAL PRIMARY KEY, poke_id INTEGER NOT NULL REFERENCES pokedex (id) ON DELETE CASCADE,
    ability_id INTEGER REFERENCES abilities (id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS trainer_moves (
    trainer_id INTEGER REFERENCES trainer (id) ON DELETE CASCADE,
    move_id INTEGER REFERENCES moves (id) ON DELETE CASCADE, PRIMARY KEY (trainer_id, move_id)
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

# Insert data into moves table from js_moves
sql = r"""
WITH cte AS (
    SELECT (body->'id')::int as id, 
            unnest(translate(jsonb_path_query_array(body->'effect_entries', '$.language.name')::text, 
                   '[]', '{}')::text[]) as language,
            unnest(translate(regexp_replace(jsonb_path_query_array(body->'effect_entries', '$.effect')::text, 
                   '\\n|\\n\\n|\\f|  ', ' ', 'g'), '[]', '{}')::text[]) as effect
    FROM js_moves
), rownum AS (
    SELECT row_number() over(order by (select NULL)) as rn, * FROM cte
), engchk AS (
    SELECT row_number() over(partition by id order by rn) as first_eng, * FROM rownum
), effctb AS (
    SELECT rank() over(partition by id order by first_eng) as rk, * FROM engchk WHERE language = 'en'
), cte1 AS (
    SELECT (body->'id')::int as id, 
            unnest(translate(jsonb_path_query_array(body->'flavor_text_entries', '$.language.name')::text, 
                   '[]', '{}')::text[]) as language,
            unnest(translate(regexp_replace(jsonb_path_query_array(body->'flavor_text_entries', '$.flavor_text')::text, 
                   '\\n|\\f', ' ', 'g'), '[]', '{}')::text[]) as ftext
    FROM js_moves
), rownum1 AS (
    SELECT row_number() over(order by (select NULL)) as rn, * FROM cte1
), engchk1 AS (
    SELECT row_number() over(partition by id order by rn) as first_eng, * FROM rownum1
), flavtb AS (
    SELECT rank() over(partition by id order by first_eng) as rk, * FROM engchk1 WHERE language = 'en'
)
INSERT INTO moves (id, name, pp, damage, accuracy, type, info)
SELECT mt.*, coalesce(ft.ftext, et.effect) 
FROM (
    SELECT (body->'id')::int as id,
            body->>'name',
           (body->'pp')::int,
            CASE WHEN (body->'power') = 'null' THEN NULL 
                 ELSE (body->'power')::numeric
            END,
            CASE WHEN (body->'accuracy') = 'null' THEN NULL 
                 ELSE (body->'accuracy')::numeric
            END,
            substring(body->'type'->>'url' from '.+/([0-9]+)/$')::int
    FROM js_moves
) AS mt
LEFT JOIN (SELECT id, ftext FROM flavtb WHERE rk = 1) AS ft
    ON mt.id = ft.id
LEFT JOIN (SELECT id, effect FROM effctb WHERE rk = 1) AS et
    ON mt.id = et.id
ORDER BY mt.id;
"""
cur.execute(sql)
print(sql)

# Insert data into abilities table from js_abilities
sql = r"""
WITH cte AS (
    SELECT (body->'id')::int as id, 
            unnest(translate(jsonb_path_query_array(body->'effect_entries', '$.language.name')::text, 
                   '[]', '{}')::text[]) as language,
            unnest(translate(regexp_replace(jsonb_path_query_array(body->'effect_entries', '$.effect')::text, 
                   '\\n|\\n\\n|\\f|  ', ' ', 'g'), '[]', '{}')::text[]) as effect
    FROM js_abilities
), rownum AS (
    SELECT row_number() over(order by (select NULL)) as rn, * FROM cte
), engchk AS (
    SELECT row_number() over(partition by id order by rn) as first_eng, * FROM rownum
), effctb AS (
    SELECT rank() over(partition by id order by first_eng) as rk, * FROM engchk WHERE language = 'en'
), cte1 AS (
    SELECT (body->'id')::int as id, 
            unnest(translate(jsonb_path_query_array(body->'flavor_text_entries', '$.language.name')::text, 
                   '[]', '{}')::text[]) as language,
            unnest(translate(regexp_replace(jsonb_path_query_array(body->'flavor_text_entries', '$.flavor_text')::text, 
                   '\\n|\\f', ' ', 'g'), '[]', '{}')::text[]) as ftext
    FROM js_abilities
), rownum1 AS (
    SELECT row_number() over(order by (select NULL)) as rn, * FROM cte1
), engchk1 AS (
    SELECT row_number() over(partition by id order by rn) as first_eng, * FROM rownum1
), flavtb AS (
    SELECT rank() over(partition by id order by first_eng) as rk, * FROM engchk1 WHERE language = 'en'
)
INSERT INTO abilities (id, name, info)
SELECT abt.*, coalesce(ft.ftext, et.effect) 
FROM (
    SELECT (body->'id')::int as id,
            body->>'name'
    FROM js_abilities
) AS abt
LEFT JOIN (SELECT id, ftext FROM flavtb WHERE rk = 1) AS ft
    ON abt.id = ft.id
LEFT JOIN (SELECT id, effect FROM effctb WHERE rk = 1) AS et
    ON abt.id = et.id
ORDER BY abt.id;
"""
cur.execute(sql)
print(sql)

conn.commit()

# Add remaining foreign keys and create GIN indexes on text/text[] columns
sql = r"""
ALTER TABLE pokemon_moves ADD FOREIGN KEY (poke_id) REFERENCES pokedex (id) ON DELETE CASCADE;
ALTER TABLE pokemon_moves ADD FOREIGN KEY (move_id) REFERENCES moves (id) ON DELETE CASCADE;
ALTER TABLE pokemon_abilities ADD FOREIGN KEY (poke_id) REFERENCES pokedex (id) ON DELETE CASCADE;
ALTER TABLE pokemon_abilities ADD FOREIGN KEY (ability_id) REFERENCES abilities (id) ON DELETE CASCADE;
CREATE INDEX gin_pd_type ON pokedex USING gin (type array_ops);
CREATE INDEX gin_pd_info ON pokedex USING gin (to_tsvector('english', info));    
CREATE INDEX gin_mv_info ON moves USING gin (to_tsvector('english', info));    
CREATE INDEX gin_ab_info ON abilities USING gin (to_tsvector('english', info));    
"""
cur.execute(sql)
print(sql)

conn.commit()

# The json tables used to dump the data into are no longer required but are not dropped by default.
# To drop the json tables created in this program simply uncomment the lines of code below by deleting the "# " portion.

# sql = r"""
# DROP TABLE IF EXISTS js_pokemon;
# DROP TABLE IF EXISTS js_species;
# DROP TABLE IF EXISTS js_types;
# DROP TABLE IF EXISTS js_evo;
# DROP TABLE IF EXISTS js_moves;
# DROP TABLE IF EXISTS js_abilities;
# """
# cur.execute(sql)
# print(sql)
#
# conn.commit()

cur.close()
conn.close()
print("Connection closed to PGSQL database.")

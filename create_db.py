# python3 create_db.py
# Pulls data from the https://pokeapi.co API and creates a Pokemon database
import psycopg2
import hidden
from get_url import get_url


#Number of Pokemon desired (or list possible)
NUM_OF_POKE = 1025

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
DROP TABLE IF EXISTS js_species;
DROP TABLE IF EXISTS js_types;
DROP TABLE IF EXISTS js_evo;
DROP TABLE IF EXISTS pokedex;
"""
cur.execute(sql)
print(sql)

conn.commit()

sql = """
CREATE TABLE IF NOT EXISTS js_pokemon (
    id INTEGER PRIMARY KEY, body JSONB
);

CREATE TABLE IF NOT EXISTS js_species (
    id INTEGER PRIMARY KEY, body JSONB
);

CREATE TABLE IF NOT EXISTS js_types (
    id INTEGER PRIMARY KEY, body JSONB
);

CREATE TABLE IF NOT EXISTS js_evo (
    id INTEGER PRIMARY KEY, body JSONB
);

CREATE TABLE IF NOT EXISTS pokedex (
    id INTEGER PRIMARY KEY, name VARCHAR(20) UNIQUE, height NUMERIC, weight NUMERIC, hp NUMERIC,
    attack NUMERIC, defense NUMERIC, s_attack NUMERIC, s_defense NUMERIC, speed NUMERIC, 
    type TEXT [], evo_set INTEGER, info TEXT
);
"""
cur.execute(sql)
print(sql)

conn.commit()

url_paths = ['https://pokeapi.co/api/v2/pokemon/', 'https://pokeapi.co/api/v2/pokemon-species/',
             'https://pokeapi.co/api/v2/type/', 'https://pokeapi.co/api/v2/evolution-chain/']
json_tables = ['js_pokemon', 'js_species', 'js_types', 'js_evo']
indexes = [NUM_OF_POKE, NUM_OF_POKE, None, 549]
for i in range(len(url_paths)):
    id_texts = get_url(url_paths[i], indexes[i])
    values = ','.join([cur.mogrify("(%s,%s)", tup).decode('utf-8') for tup in id_texts])
    cur.execute("INSERT INTO " + json_tables[i] + " VALUES " + values)

conn.commit()

sql = r"""
WITH cte AS (
    SELECT (body->'id')::int as id, 
            unnest(translate(jsonb_path_query_array(body->'flavor_text_entries', 
                   '$.language.name')::text, '[]', '{}')::text[]) as language,
            unnest(translate(regexp_replace(jsonb_path_query_array(body->'flavor_text_entries', 
                   '$.flavor_text')::text, '\\n|\\f', ' ', 'g'), '[]', '{}')::text[]) as info
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

conn.commit()

cur.close()
conn.close()
print("Connection closed to PGSQL database.")

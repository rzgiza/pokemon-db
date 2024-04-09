import psycopg2
import hidden
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from pprint import pprint

futures = []
id_texts = []
with FuturesSession(max_workers=10) as session:
	for i in range(1, 152):
		url = 'https://pokeapi.co/api/v2/pokemon/' + str(i)
		future = session.get(url)
		future.i = i
		futures.append(future)

	for future in as_completed(futures):
		response = future.result()
		pprint({
			'id': future.i
		})
		id_texts.append((future.i, response.text))

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
CREATE TABLE IF NOT EXISTS js_pokemon (
	id INTEGER, 
	body JSONB
);
"""
cur.execute(sql)
print(sql)

args = ','.join([cur.mogrify("(%s,%s)", tup).decode('utf-8') for tup in id_texts])

cur.execute("INSERT INTO js_pokemon VALUES " + args)

conn.commit()
print("Values inserted into js_pokemon.")

cur.close()
conn.close()

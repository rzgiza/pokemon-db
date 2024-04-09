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
			'pokemon': response.json()['name'],
			'id': future.i
		})
		id_texts.append((future.i, response.text))

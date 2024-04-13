# get_url.py creates function to get data async
# Pass url_path without index (page number).
# Add index as number to get that many pages, a +int list to get specific pages,
# or none when no pages are needed.
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from pprint import pprint


def get_url(url_path, index=None):
    futures = []
    id_texts = []

    if index is None:
        with FuturesSession(max_workers=1) as session:
            url = url_path
            future = session.get(url)
            future.i = 1
            future.geturl = url
            futures.append(future)

            for future in as_completed(futures):
                response = future.result()
                pprint({'id': future.i, 'url': future.geturl})
                id_texts.append((future.i, response.text))
    else:
        try:
            _ = int(index)
        except TypeError:
            iterable = index
        else:
            iterable = range(1, index + 1)

        with FuturesSession(max_workers=10) as session:
            for i in iterable:
                url = url_path + str(i)
                future = session.get(url)
                future.i = i
                future.geturl = url
                futures.append(future)

            for future in as_completed(futures):
                response = future.result()
                pprint({'id': future.i, 'url': future.geturl})
                id_texts.append((future.i, response.text))

    return id_texts

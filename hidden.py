# Credentials to connect to postgresql database
import json


f = open("/home/rzg/Documents/postgres/pokemon/credentials.json")
cred = json.load(f)


def secrets(host=cred['host'], port=cred['port'], database=cred['database'], user=cred['user'], password=cred['pass']):
    return {"host": host,
            "port": port,
            "database": database,
            "user": user,
            "pass": password}

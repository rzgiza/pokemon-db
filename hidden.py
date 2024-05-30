# Credentials to connect to postgresql database
import json

# Load the credentials from a json file.
f = open("/home/rzg/Documents/postgres/pokemon/credentials.json")
cred = json.load(f)


# Function to return credentials used to connect with the PGSQL Pokemon database.
def secrets(host=cred['host'], port=cred['port'], database=cred['database'],
            user=cred['user'], password=cred['pass']):
    return {"host": host,
            "port": port,
            "database": database,
            "user": user,
            "pass": password}

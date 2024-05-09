import psycopg2


class PGSQLConnection:
    """Create class to wrap psycopg2.connect and support with statements."""
    def __init__(self, host, port, database, user, password, connect_timeout=3):
        """Initialize parameters to PGSQL connection."""
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connect_timeout = connect_timeout

    def __enter__(self):
        """Open psycopg2 connection (conn) and creates cursor (cur) using with."""
        self.conn = psycopg2.connect(host=self.host, port=self.port, database=self.database,
                                     user=self.user, password=self.password, connect_timeout=self.connect_timeout)
        self.cur = self.conn.cursor()
        print("Connection opened to PGSQL database.\n")
        return self.conn, self.cur

    def __exit__(self, exc_type, exc_value, tb):
        """Commits and closes PGSQL cursor, connection using with."""
        self.conn.commit()
        self.cur.close()
        self.conn.close()
        print("\nConnection closed to PGSQL database.")


class TrainerPack:
    """Handles postgres connection (from PGSQLConnection) using with statements
    to perform CRUD operations on the pokemon database created in create_db.py."""
    def __init__(self, pgsql_connection):
        """Pass PGSQLConnection object."""
        self.pgsql_connection = pgsql_connection

if __name__ == "__main__":
    print("Running as main.")

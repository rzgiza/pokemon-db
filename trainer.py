# Perform CRUD operations on trainer and trainer_moves tables in Pokemon database.
import psycopg2
import pandas as pd


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
        print("Connection opened to PGSQL database.")
        return self.conn, self.cur

    def __exit__(self, exc_type, exc_value, tb):
        """Commits and closes PGSQL cursor, connection using with."""
        self.conn.commit()
        self.cur.close()
        self.conn.close()
        print("Connection closed to PGSQL database.\n")


class TrainerPack:
    """Handles postgres connection (from PGSQLConnection) using with statements
    to perform CRUD operations on the Pokemon database created in create_db.py."""
    MAX_POKEMON = 6
    MAX_MOVES = 4

    def __init__(self, pgsql_connection):
        """Pass PGSQLConnection object."""
        self.pgsql_connection = pgsql_connection
        self.trainer_count = self.get_trainer_count()
        print("Current trainer Pokemon count is:", self.trainer_count)
        if self.trainer_count > TrainerPack.MAX_POKEMON:
            print("Warning: Change has caused current number of trainer pokemon to exceed MAX_POKEMON =", TrainerPack.MAX_POKEMON)

    @classmethod
    def change_trainer_max(cls, new_max):
        """Change the default max number of Pokemon for the trainer table."""
        cls.MAX_POKEMON = new_max

    @classmethod
    def change_moves_max(cls, new_max):
        """Change the default max number of Pokemon moves for the trainer_moves table."""
        cls.MAX_MOVES = new_max

    def get_trainer_count(self):
        """Get number of pokemon in trainer table. -> count as int."""
        with self.pgsql_connection as conn_cur:
            sql = r"SELECT count(*) FROM trainer;"
            conn_cur[1].execute(sql)
            count = conn_cur[1].fetchall()[0][0]
            return count

    def show_pokemon(self):
        """Show list of pokemon id, names and info. -> pd dataframe."""
        with self.pgsql_connection as conn_cur:
            sql = r"SELECT id, name, info FROM pokedex;"
            conn_cur[1].execute(sql)
            pokemon = conn_cur[1].fetchall()
            pokemon = pd.DataFrame(pokemon, columns=['id', 'name', 'info'])
            return pokemon


# Code to run when module runs as main. Does not run when module is imported.
# Used to create a quick random setup of trainer and trainer_moves.
if __name__ == "__main__":
    import hidden
    print("Running as main.")
    secrets = hidden.secrets()
    pgsql_conn = PGSQLConnection(host=secrets['host'], port=secrets['port'], database=secrets['database'],
                                 user=secrets['user'], password=secrets['pass'])
    tr_pack = TrainerPack(pgsql_connection=pgsql_conn)


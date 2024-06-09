# Pokemon-DB
This project uses Python to create a Pokemon PostgreSQL database. 

The database is created by scraping JSON data from the https://pokeapi.co API and organizing the data 
into a relational format. The main file for the project is *create_db.py*, which uses 
[*requests-futures*](https://github.com/ross/requests-futures/tree/main) and *psycopg2* to make asynchronous requests 
from the API and insert the results into your chosen PostgreSQL database. A diagram for the schema can be 
found below.

![Pokemon DB](/images/pokemon_db.jpg?raw=true "Pokemon-DB")

## Getting Started
The workflow demonstrated below uses a Linux terminal to set up and run the project from the command line. 
It also uses the conda package manager to create an environment called *webscrape* that has all the necessary 
dependencies. Note that this project has been tested on PostgreSQL versions 16.2 and 14.3.

Begin by logging in as the Postgres superuser and creating a database named "pokemon". The password for the user you 
select as the owner will be the one to use in the credentials file. 
```console
[rob@fedora ~]$ sudo su - postgres
[postgres@fedora ~]$ psql
psql (16.2)
Type "help" for help.

postgres=# CREATE DATABASE pokemon WITH OWNER rob;
CREATE DATABASE
postgres=# \q
[postgres@fedora ~]$ exit
logout
```
Next, switch into the directory where you wish to clone the *pokemon-db* repository.
```console
[rob@fedora ~]$ cd Dev
[rob@fedora Dev]$ git clone https://github.com/rzgiza/pokemon-db.git
[rob@fedora Dev]$ ls
pokemon-db
```
In this example, *pokemon-db* is installed in the Dev directory. You can now access the *hidden.py* file, which is used
to point Python to your database credentials file. 

To create a credentials file make a text file containing the information in a JSON format.
```
{
"host": "127.0.0.1",
"port": 5432,
"database": "pokemon",
"user": "rob",
"pass": "user_password"
}
```
Once you have created the credentials file, edit the *hidden.py* file to change the path to the correct one on your system.
So, change the path from
```python
# Load the credentials from a json file.
f = open("/home/rzg/Documents/postgres/pokemon/credentials.json")
```
to the correct one below.
```python
# Load the credentials from a json file.
f = open("path_to_your_credentials_file")
```
With the initial setup complete, you are now ready to catch them all!

![Squirtle](/images/squirtle.png?raw=true "Squirtle")

## Creating The Pokemon Database
The *pokemon-db* directory on your system contains a yaml file that the conda package manager will use to create the 
*webscrape* environment. Once the environment is created, you can activate it and check which Python is being referenced.
```console
[rob@fedora Dev]$ cd pokemon-db
[rob@fedora pokemon-db]$ conda env create -f webscrape.yml
[rob@fedora pokemon-db]$ conda activate webscrape
(webscrape) [rob@fedora pokemon-db]$ which python
~/anaconda3/envs/webscrape/bin/python
```
With the environment still active, execute the *create_db.py* file using Python. This will retrieve and store data for all 1025 Pokemon.
```console
(webscrape) [rob@fedora pokemon-db]$ python create_db.py
```
There will be output displaying the asynchronous requests. Note that the request ids are not sequentially ordered. Finally, run the
*trainer* module as main to finish the database by inserting some random input into the **trainer** and **trainer_moves** tables. The *trainer* 
module also provides additional functionality when it is imported. 
```console
(webscrape) [rob@fedora pokemon-db]$ python -m trainer
(webscrape) [rob@fedora pokemon-db]$ conda deactivate
```
To verify that the database has been properly created, log back into Postgres and enter the database as the owner.
Then, print a list of all the available tables with the `\dt` command.
```console
[rob@fedora pokemon-db]$ cd ~
[rob@fedora ~]$ sudo su - postgres
[postgres@fedora ~]$ psql pokemon rob
psql (16.2)
Type "help" for help.

pokemon=> \dt
             List of relations
 Schema |       Name        | Type  | Owner 
--------+-------------------+-------+-------
 public | abilities         | table | rob
 public | js_abilities      | table | rob
 public | js_evo            | table | rob
 public | js_moves          | table | rob
 public | js_pokemon        | table | rob
 public | js_species        | table | rob
 public | js_types          | table | rob
 public | moves             | table | rob
 public | pokedex           | table | rob
 public | pokemon_abilities | table | rob
 public | pokemon_moves     | table | rob
 public | trainer           | table | rob
 public | trainer_moves     | table | rob
 public | types             | table | rob
(14 rows)

pokemon=> \q
[postgres@fedora ~]$ exit
logout
```
You will notice six additional tables prefixed with "js_" that are not included in the schema diagram. These are intermediary tables used to store
the JSON data from the website in JSONB columns. These records are then parsed and organized into the remaining tables. The default is to
keep the intermediary tables, but this can be changed by uncommenting the following lines of code at the end of the *create_db.py* file.
```python
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
```
This concludes the main section of the project. You now have a Pokemon database!

![Cubone](/images/cubone.png?raw=true "Cubone")

## Accessing The Webscrape Enviornment From Jupyter Notebooks
If you wish to use the *webscrape* environment from a jupyter notebook, you can use `ipython kernel install`
from inside the *webscrape* environment to finish the setup (*ipykernel* is already installed in the *webscrape* environment).
To verify, check the list of available kernels with `kernelspec list`.
```console
[rob@fedora ~]$ conda activate webscrape
(webscrape) [rob@fedora ~]$ ipython kernel install --user --name webscrape --display-name "webscrape"
Installed kernelspec webscrape in /home/rob/.local/share/jupyter/kernels/webscrape
(webscrape) [rob@fedora ~]$ jupyter kernelspec list
Available kernels:
  python3      /home/rob/anaconda3/envs/webscrape/share/jupyter/kernels/python3
  webscrape    /home/rob/.local/share/jupyter/kernels/webscrape
```
Now you should see an option to attach the *webscrape* kernel when creating a new notebook or working with
a pre-existing one.
```console
(webscrape) [rob@fedora ~]$ conda deactivate
[rob@fedora ~]$ conda activate
(base) [rob@fedora ~]$ jupyter notebook
```

![Jigglypuff](/images/jigglypuff.jpg?raw=true "Jigglypuff")

## Additional Help
Here are a some resources I found useful while working with PostgreSQL and Python.

1. Peer Authentication Error
   
   When first setting up Postgres, you log in as the superuser to create a user with their own password
   `postgres=# CREATE USER rob WITH PASSWORD 'user_password';` and create a database with the user as the owner
   `postgres=# CREATE DATABASE pokemon WITH OWNER rob;`. When attempting to enter the database as the user `[postgres@fedora ~]$ psql pokemon rob`, you may
   encounter a "peer authentication error". You will need to edit a few lines in the *pg_ident.conf* and *pg_hba.conf* Postgres files.
   Their locations can be  found with
   
   - `postgres=# SHOW ident_file;`
   - `postgres=# SHOW hba_file;`

   and a detailed description of the edits to be made can be found
   [here](https://stackoverflow.com/questions/69676009/psql-error-connection-to-server-on-socket-var-run-postgresql-s-pgsql-5432).
   In summary,
   
   - Open the *pg_ident.conf* file and edit it to map your system user name to the desired Postgres user name.
   - Open the *pg_hba.conf* file and edit it to add the Postgres user name with "method = md5" (instead of "method = peer" worked for me).
   - Finally, `[postgres@fedora ~]$ systemctl restart postgresql-16` (or whatever version you have).

   Now you should be able to enter the database as the user.
   
2. Ident Authentication Error

   The module *create_db* uses psycopg2 to make a connection from Python to your PostgreSQL server. For users connecting from certain hosts Postgres defines          "Ident" as the protocol used to connect to the database. Since *create_db* attempts to use your user password to make the connection, you may encounter an
   "Ident authentication error" when running *create_db* with `(webscrape) [rob@fedora pokemon-db]$ python create_db.py`. To avoid this issue open
   the *pg_hba.conf* file and edit the appropriate line (different from the edit in [1]) to change the method from "ident" to "md5". A thorough discussion can
   be found [here](https://serverfault.com/questions/406606/postgres-error-message-fatal-ident-authentication-failed-for-user) (look at solution #4). 
   
3. PSQL Client and Postgres are Different Versions Warning

   If you encounter this warning, the information
   [here](https://stackoverflow.com/questions/34052046/how-do-i-correct-the-mismatch-of-psql-version-and-postgresql-version)
   may be helpful.

![Snorlax](/images/snorlax.jpg?raw=true "Snorlax")

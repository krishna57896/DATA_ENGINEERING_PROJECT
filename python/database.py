import psycopg2
from sqlalchemy import create_engine


def confirm_postgres_connectable(config):
    # first see if postgres is running
    conn_string = f"host='{config['postgres_host']}' " \
        + f"user='{config['postgres_user']}' " \
        + f"password='{config['postgres_password']}'"
    try:
        conn = psycopg2.connect(conn_string)
    except psycopg2.OperationalError as e:
        # if you can't connect to postgres, well you can't really put data there, not much sense to keep going - so quit()
        logging.error(e)
        quit()
    except:
        quit()
    return conn


def confirm_database_connection(config):
    conn = confirm_postgres_connectable(config)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SELECT datname FROM pg_database;")
    db_available = any([db[0] == config['postgres_db'] for db in cursor.fetchall()])
    if not db_available:
        cursor.execute(f"CREATE DATABASE {config['postgres_db']};")
    cursor.close()
    conn.close()


def build_tables(cursor):
    create_tables = open('../sql/create_tables.sql', 'r')
    cursor.execute(create_tables.read())


def build_database(config):
    confirm_database_connection(config)
    conn_string = f"host='{config['postgres_host']}' " \
        + f"user='{config['postgres_user']}' " \
        + f"password='{config['postgres_password']}' " \
        + f"dbname={config['postgres_db']}"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    build_tables(cursor)


def write_df_to_sql(config, df, table):
    conn_string = f"postgresql://{config['postgres_user']}" \
        + f":{config['postgres_password']}" \
        + f"@{config['postgres_host']}:5432" \
        + f"/{config['postgres_db']}"
    engine = create_engine(conn_string)
    df.to_sql(table, engine)

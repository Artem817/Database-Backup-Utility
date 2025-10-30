
#FIXME --- IGNORE ---
def fetch_version_database(connection):
    with connection.cursor() as cur:
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        database_version = version.split(',')[0]
    return database_version
import psycopg2


def get_conn():
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host="34.171.40.65",
        port="5432",
        database="cexplorer",
        user="postgres",
        password="postgres"
    )
    return conn


def query_data(conn, sql):
    # Query the data from the specified table and retrieve the entity and resource columns
    cursor = conn.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    return data


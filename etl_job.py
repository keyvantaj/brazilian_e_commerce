from pyspark.sql import SparkSession
from ingest import *

jdbc_url = "jdbc:postgresql://{}:{}/{}".format(DB_HOST, DB_PORT, DB_NAME)


def extract_from_postgresql(table, conn_properties):
    """Extract data from PostgreSQL"""

    # Read data from a table
    df = spark.read \
        .jdbc(url=jdbc_url,
              table=table,
              properties=conn_properties)

    return df

if __name__ == "__main__":
    # Initialize Spark Session with PostgreSQL JDBC driver
    spark = SparkSession.builder. \
        appName("PostgreSQL ETL Job"). \
        config("spark.jars.packages", "org.postgresql:postgresql:42.2.24"). \
        getOrCreate()

    connection_properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver",
        "fetchsize": "1000"  # Fetch rows in batches
    }


    for key,value in csv_paths.items():
        # Charger les données
        print(key)
        print('****************************************************')
        print('****************************************************')
        print('****************************************************')
        print('****************************************************')
        df = extract_from_postgresql(table=key, conn_properties=connection_properties)
        df.show(5)



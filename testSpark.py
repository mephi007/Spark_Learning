import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from Postgres_Pipeline import Postgres_Pipeline





# spark = SparkSession.builder.master("local")\
#         .config("spark.jars", "/Users/sumitroy/Downloads/postgresql-42.6.0.jar") \
#         .appName("PySpark_Postgres_source_sink").getOrCreate()
# schema = StructType([ \
#     StructField("ORIGIN_COUNTRY_NAME",StringType(),True), \
#     StructField("DEST_COUNTRY_NAME",StringType(),True), \
#     StructField("count",IntegerType(),True)
#   ])
# df = spark.read.format('json').schema(schema).load('./flight_data.json')
# df.printSchema()
# df.show(truncate=False)

# df.write.format("jdbc")\
#         .option("url", URL)\
#         .option("dbtable", TABLE_NAME)\
#         .option("user", PSQL_USRRNAME)\
#         .option("password", PSQL_PASSWORD)\
#         .option("driver", "org.postgresql.Driver")\
#         .mode("overwrite").save()


# sql_df = spark.read\
#         .format("jdbc")\
#         .option("url", URL)\
#         .option("dbtable", TABLE_NAME)\
#         .option("user", PSQL_USRRNAME)\
#         .option("password", PSQL_PASSWORD)\
#         .option("driver", "org.postgresql.Driver")\
#         .load()
# sql_df.printSchema()
# sql_df.show()

def pipeline_creation():
    PSQL_SERVERNAME = "localhost"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "spark_learn"
    PSQL_USRRNAME = "sumitroy"
    PSQL_PASSWORD = "admin"
    TABLE_NAME = "flight_data"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}/{PSQL_DBNAME}"
    jar_path = "/Users/sumitroy/Downloads/postgresql-42.6.0.jar"
    postgres_pipeline = Postgres_Pipeline(PSQL_SERVERNAME, PSQL_PORTNUMBER, PSQL_DBNAME, PSQL_USRRNAME, PSQL_PASSWORD, TABLE_NAME, jar_path)
    spark = postgres_pipeline.create_spark_session()
    query = 'select * from flight_data where "ORIGIN_COUNTRY_NAME"=\'India\''
    df = postgres_pipeline.read_dbtable(spark, query)
    # postgres_pipeline.write_files(df=df, format='csv', mode='overwrite',path='./flight_data/',include_headers=True,delimiter='|')
    # df = postgres_pipeline.read_files(spark=spark, format='csv', path='./flight_data/part-*.csv')
    df.show(truncate=False)

def period_trend_data_to_db():
    PSQL_SERVERNAME = "localhost"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "spark_learn"
    PSQL_USRRNAME = "sumitroy"
    PSQL_PASSWORD = "admin"
    TABLE_NAME = "period_trend_data"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}/{PSQL_DBNAME}"
    jar_path = "/Users/sumitroy/Downloads/postgresql-42.6.0.jar"
    postgres_pipeline = Postgres_Pipeline(PSQL_SERVERNAME, PSQL_PORTNUMBER, PSQL_DBNAME, PSQL_USRRNAME, PSQL_PASSWORD, TABLE_NAME, jar_path)
    spark = postgres_pipeline.create_spark_session()
    df = postgres_pipeline.read_files(spark=spark, format='csv', path='./period_trend_data_6M.csv', header=True, delimiter=",")
    postgres_pipeline.write_to_table(df=df)
    # df.show(truncate=False)

def period_trend_data_from_db():
    PSQL_SERVERNAME = "localhost"
    PSQL_PORTNUMBER = 5432
    PSQL_DBNAME = "spark_learn"
    PSQL_USRRNAME = "sumitroy"
    PSQL_PASSWORD = "admin"
    TABLE_NAME = "period_trend_data"
    URL = f"jdbc:postgresql://{PSQL_SERVERNAME}/{PSQL_DBNAME}"
    jar_path = "/Users/sumitroy/Downloads/postgresql-42.6.0.jar"
    postgres_pipeline = Postgres_Pipeline(PSQL_SERVERNAME, PSQL_PORTNUMBER, PSQL_DBNAME, PSQL_USRRNAME, PSQL_PASSWORD, TABLE_NAME, jar_path)
    spark = postgres_pipeline.create_spark_session()
    # query = 'select DATE(max(date)) as HIGH_DATE from period_trend_data'
    low_date = '2022-11-28'
    high_date = '2023-05-26'
    batchSize = 10
    query_in_range = 'select * from period_trend_data where DATE(date) <= \'{high_date}\' and DATE(date) > \'{low_date}\''
    # postgres_pipeline.get_high_time(spark=spark, query=query)
    postgres_pipeline.read_query_in_range_date(spark=spark, query=query_in_range, low_date=low_date, high_date=high_date, batchSize=batchSize)
    # df.show(truncate=False)

if __name__ == '__main__':
    # pipeline_creation()
    # period_trend_data_to_db()
    period_trend_data_from_db()

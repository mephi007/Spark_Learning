import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType


class Postgres_Pipeline:
    def __init__(self, servername, port, dbname, username, password, tablename, jar_path):
        self.PSQL_SERVERNAME = servername
        self.PSQL_PORTNUMBER = port
        self.PSQL_DBNAME = dbname
        self.PSQL_USRRNAME = username
        self.PSQL_PASSWORD = password
        self.TABLE_NAME = tablename
        self.jar_path = jar_path
        self.URL = f"jdbc:postgresql://{self.PSQL_SERVERNAME}/{self.PSQL_DBNAME}"
        self.spark = None
    
    def create_spark_session(self):
        spark = SparkSession.builder.master("local")\
                                    .config("spark.jars", self.jar_path) \
                                    .appName("PySpark_Postgres_source_sink").getOrCreate()
        
        return spark
    
    def read_dbtable(self,spark):
        sql_df = spark.read\
                    .format("jdbc")\
                    .option("url", self.URL)\
                    .option("dbtable", self.TABLE_NAME)\
                    .option("user", self.PSQL_USRRNAME)\
                    .option("password", self.PSQL_PASSWORD)\
                    .option("driver", "org.postgresql.Driver")\
                    .load()
        return sql_df
    
    def write_to_table(self, df):
        df.write.format("jdbc")\
        .option("url", self.URL)\
        .option("dbtable", self.TABLE_NAME)\
        .option("user", self.PSQL_USRRNAME)\
        .option("password", self.PSQL_PASSWORD)\
        .option("driver", "org.postgresql.Driver")\
        .mode("overwrite").save()

    def read_files(self, spark, format, path):
        df = spark.read.format(format).option("header", True)\
                                      .option("delimiter", "|")\
                                      .load(path)
        return df
    
    def write_files(self, df, format, mode, path, include_headers, delimiter):
        df.write.format(format).mode(mode).options(header = include_headers, delimiter = delimiter).save(path)

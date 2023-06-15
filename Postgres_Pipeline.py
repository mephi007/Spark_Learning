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
        self.high_time = None
        self.low_time = None
        self.state = None
    
    def create_spark_session(self):
        spark = SparkSession.builder.master("local")\
                                    .config("spark.jars", self.jar_path) \
                                    .appName("PySpark_Postgres_source_sink").getOrCreate()
        
        return spark
    
    def read_dbtable(self,spark, query, table_name):
        if query:
            sql_df = spark.read\
                        .format("jdbc")\
                        .option("url", self.URL)\
                        .option("user", self.PSQL_USRRNAME)\
                        .option("password", self.PSQL_PASSWORD)\
                        .option("query", query)\
                        .option("driver", "org.postgresql.Driver")\
                        .load()
        if table_name:
            sql_df = spark.read\
                        .format("jdbc")\
                        .option("url", self.URL)\
                        .option("user", self.PSQL_USRRNAME)\
                        .option("password", self.PSQL_PASSWORD)\
                        .option("dbtable", table_name)\
                        .option("driver", "org.postgresql.Driver")\
                        .load()  
            
        self.state = 'RUNNING'
        return sql_df
    
    def write_to_table(self, df):
        df.write.format("jdbc")\
        .option("url", self.URL)\
        .option("dbtable", self.TABLE_NAME)\
        .option("user", self.PSQL_USRRNAME)\
        .option("password", self.PSQL_PASSWORD)\
        .option("driver", "org.postgresql.Driver")\
        .mode("overwrite").save()

    def read_files(self, spark, format, path, header, delimiter):
        df = spark.read.format(format).option("header", header)\
                                      .option("delimiter", delimiter)\
                                      .load(path)
        return df
    
    def write_files(self, df, format, mode, path, include_headers, delimiter):
        df.write.format(format).mode(mode).options(header = include_headers, delimiter = delimiter).save(path)

    def get_high_time(self,spark, query):
        df = self.read_dbtable(spark=spark,query=query, table_name=None)
        df.select(df['high_date']).show()
        high_date = df.select(df['high_date']).collect()[0].high_date
        # print(high_date)
        self.high_time = high_date
        return str(high_date)

    # def read_query_in_range_date(self, spark, query, low_date, high_date,batchSize):
    #     query = query.format(low_date=self.low_date, high_date=self.high_date)
    #     self.low_date = high_date
    #     temp =high_date+batchSize
    #     self.high_date = high_date
    #     self.batchSize = batchSize
    #     df = self.read_dbtable(spark=spark, query=query, table_name=None)
    #     print(df.count())

    def read_query_in_range_date(self, spark, query, low_date, high_date):
        query = query.format(low_date=low_date, high_date=high_date)
        return self.read_dbtable(spark=spark, query=query, table_name=None)
    
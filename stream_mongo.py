import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import findspark
import pymongo
 
findspark.init()
import pyspark
findspark.find()
 
def create_mongo_connection():
    try:
        client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.ns1e9kq.mongodb.net/?retryWrites=true&w=majority")
        db = client["sparkProject"]
        logging.info("Connected to MongoDB successfully!")
        return db
    except Exception as e:
        logging.error(f"Could not create MongoDB connection due to {e}")
        return None
 
def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
                    "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config('spark.mongodb.input.uri', 'mongodb://localhost:27017/spark_streams.created_users') \
            .config('spark.mongodb.output.uri', 'mongodb://localhost:27017/spark_streams.created_users') \
            .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        print('✅ Spark connection created successfully!')
    except Exception as e:
        logging.error(f"❌ Couldn't create the Spark session due to exception {e}")

    return s_conn

 
def connect_to_kafka(spark_conn):
    try:
        print("Attempting to connect to Kafka...")
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        print("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
        return None

 
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')) \
            .select("data.*")
    logging.info("Selection DataFrame created successfully!")
    print(sel)
    print(sel.printSchema())
    return sel
    # sel = spark_df.selectExpr("CAST(value AS STRING)") \
    #     .select(from_json(col('value'), schema).alias('data')) \
    #     .selectExpr("data.id", "data.first_name", "data.last_name",
    #                 "data.gender", "data.address", "data.post_code",
    #                 "data.email", "data.username","data.dob",
    #                 "data.registered_date", "data.phone", "data.picture")
    # print(sel)
    # return sel
 
def write_to_mongo(batch_df, batch_id):
    batch_df.write \
        .format("mongo") \
        .mode("append") \
        .option('uri', 'mongodb+srv://admin:admin@cluster0.ns1e9kq.mongodb.net/sparkProject.details') \
        .save()
   
if __name__ == "__main__":
    spark_conn = create_spark_connection()
    spark_conn.conf.set("spark.sql.streaming.schemaInference", "true")
    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        db = create_mongo_connection()
        if db is not None:
            logging.info("Streaming is being started...")
            streaming_query = (selection_df.writeStream
                               .foreachBatch(write_to_mongo)  # Use foreachBatch
                               .option("checkpointLocation", "./checkpoint")
                               .start())
            streaming_query.awaitTermination()


# python stream_mongo.py
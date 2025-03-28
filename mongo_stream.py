import logging
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configure logging
logging.basicConfig(level=logging.INFO)

def create_mongo_connection():
    try:
        client = MongoClient("mongodb://root:example@mongodb:27017/")
        db = client["spark_streams"]
        logging.info("Connected to MongoDB successfully!")
        return db
    except Exception as e:
        logging.error(f"Could not create MongoDB connection due to: {e}")
        return None

def insert_data(db, data):
    try:
        collection = db["created_users"]
        collection.insert_one(data)
        logging.info(f"Data inserted for {data['first_name']} {data['last_name']}")
    except Exception as e:
        logging.error(f"Could not insert data due to {e}")

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to: {e}")
        return None

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully!")
        return spark_df
    except Exception as e:
        logging.error(f"Kafka DataFrame could not be created due to: {e}")
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

    try:
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')) \
            .select("data.*")
        logging.info("Selection DataFrame created successfully!")
        return sel
    except Exception as e:
        logging.error(f"Could not create selection DataFrame due to: {e}")
        return None

if __name__ == "__main__":
    import os
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)

        if spark_df is not None:
            db = create_mongo_connection()

            if db is not None:
                selection_df = create_selection_df_from_kafka(spark_df)

                if selection_df is not None:
                    logging.info("Starting streaming...")

                    streaming_query = (selection_df.writeStream
                                       .foreachBatch(lambda batch_df, _: batch_df.collect().foreach(lambda row: insert_data(db, row.asDict())))
                                       .start())

                    streaming_query.awaitTermination()
                else:
                    logging.error("Failed to create selection DataFrame.")
            else:
                logging.error("Failed to create MongoDB connection.")
        else:
            logging.error("Failed to create Kafka DataFrame.")
    else:
        logging.error("Failed to create Spark connection.")


# docker exec -it spark-master spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
#   /opt/bitnami/spark/mongo_stream.py
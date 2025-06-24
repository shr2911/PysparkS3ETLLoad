from utils.config_loader import load_config
from pyspark.sql import SparkSession
from extract_s3 import extract_s3_data
from transform_s3 import transfomed_s3_data
from load_s3 import write_to_s3
import logging

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")

def create_spark_session(config):

    spark = SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .master(config['spark']['master']) \
        .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.529")\
        .config("spark.hadoop.fs.s3a.access.key", config['s3']['aws_access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['s3']['aws_secret_key']) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.timeout", str(60 * 1000))\
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", str(60 * 1000))\
        .config("spark.hadoop.fs.s3a.connection.request.timeout", str(60 * 1000)) \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")\
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")\
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")\
        .config("spark.hadoop.fs.s3a.multipart.purge.enabled", "true")\
        .config("spark.sql.shuffle.partitions", "200")\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()
    return spark




def main():
    config = load_config("config/application.yml")
    import pprint
    pprint.pprint(config)
    pprint.pprint(config['s3']['aws_access_key'])
    try:
        spark = create_spark_session(config)
        # spark._jsc.hadoopConfiguration().iterator().forEachRemaining(lambda x: print(f"{x.key} = {x.value}"))
        # conf = spark._jsc.hadoopConfiguration()
        # iterator = conf.iterator()
        # while iterator.hasNext():
        #     entry = iterator.next()
        #     key = entry.getKey()
        #     value = entry.getValue()
        #     if "s3a" in key:
        #         print(f"{key} = {value}")
                # df = spark.read.option("header", True).csv("s3a://cddshr/raw/load_date=2025_06_24/raw_input.csv")
        # df.show(False)
        data_raw_df = extract_s3_data(spark,config)
        data_raw_df.show(10,False)
        transformed_df = transfomed_s3_data(spark, data_raw_df)
        transformed_df.show(10,False)
        write_to_s3(transformed_df,config)
        logging.info("ETL job load to S3 completed")
    except Exception as e:
        logging.error(f"ETL job load from s3 and write to S3 failed with exception {str(e)}")
        raise
    spark.stop()



if __name__ == "__main__":
    main()

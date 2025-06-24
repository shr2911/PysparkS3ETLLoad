
import logging

def extract_s3_data(spark,config):
    logging.info("reading data from S3")
    input_path = config['s3']['input_path']
    schema_path = config['s3']['schema_path']
    raw_df = spark.read.format("csv").option("inferSchema",True)\
        .option("header",True).load(input_path)
    return raw_df

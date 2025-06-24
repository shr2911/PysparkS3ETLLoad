
import logging

from pyspark.sql.functions import current_date
def write_to_s3(trans_df,config):

    out_put_path = config['s3']['output_path']
    logging.info("Writing data to s3 path")
    trans_df.withColumn("load_date",current_date()).coalesce(1).write.format("orc").partitionBy("load_date").mode("overwrite").save(out_put_path)
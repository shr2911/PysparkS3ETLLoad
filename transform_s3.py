import logging
from pyspark.sql.functions import to_date,col,split
def transfomed_s3_data(spark,raw_df):
    logging.info("Transforming data")
    transformed_df=raw_df\
                    .withColumn("signup_date",to_date(col("signup_date"),"YYYY-MM-DD"))\
                    .withColumn("email", split(col("email"),"@").getItem(1))
    
    return transformed_df
    
    

from pyspark.sql import SparkSession, functions as f
from pyspark import SparkConf
from pyspark.sql.types import (
    StringType, StructType, StructField, IntegerType, DoubleType
)
from pyspark.sql.window import Window

if __name__ == '__main__':
    conf = SparkConf() \
        .set('spark.executor.memory', '2g') \
        .set("spark.submit.deployMode", "client") \
        .set("spark.hadoop.fs.s3a.access.key", "<YOUR_AWS_ACCESS_KEY_ID>") \
        .set("spark.hadoop.fs.s3a.secret.key", "<YOUR_AWS_SECRET_ACCESS_KEY>") \
        .set("spark.hadoop.fs.s3a.path.style.access", "true") \
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark = SparkSession.builder \
        .appName('fraud_analysis') \
        .master('spark://<spark-master-host>:<spark-master-port>') \
        .config(conf=conf) \
        .getOrCreate()
    # spark-master-host и spark-master-port нужно поменять на хост и порт своего spark мастера

    spark.sparkContext.setLogLevel('ERROR')

    schema_transactions = StructType([
        StructField('id', IntegerType()),
        StructField('date', StringType()),
        StructField('client_id', IntegerType()),
        StructField('card_id', IntegerType()),
        StructField('amount', StringType()),
        StructField('use_chip', StringType()),
        StructField('merchant_id', IntegerType()),
        StructField('merchant_city', StringType()),
        StructField('merchant_state', StringType()),
        StructField('zip', StringType()),
        StructField('mcc', StringType()),
        StructField('errors', StringType())
    ])

    schema_users = StructType([
        StructField('id', IntegerType()),
        StructField('current_age', IntegerType()),
        StructField('retirement_age', IntegerType()),
        StructField('birth_year', IntegerType()),
        StructField('birth_month', IntegerType()),
        StructField('gender', StringType()),
        StructField('address', StringType()),
        StructField('latitude', DoubleType()),
        StructField('longitude', DoubleType()),
        StructField('per_capita_income', DoubleType()),
        StructField('yearly_income', DoubleType()),
        StructField('total_debt', DoubleType()),
        StructField('credit_score', IntegerType()),
        StructField('num_credit_cards', IntegerType())
    ])

    schema_cards = StructType([
        StructField('id', IntegerType()),
        StructField('client_id', IntegerType()),
        StructField('card_brand', StringType()),
        StructField('card_type', StringType()),
        StructField('card_number', StringType()),
        StructField('expires', StringType()),
        StructField('cvv', StringType()),
        StructField('has_chip', StringType()),
        StructField('num_cards_issued', IntegerType()),
        StructField('credit_limit', DoubleType()),
        StructField('acct_open_date', StringType()),
        StructField('year_pin_last_changed', IntegerType()),
        StructField('card_on_dark_web', StringType())
    ])

    bucket = "<bucket-name>"  # change me

    sdf_transactions = spark.read.csv(f's3a://{bucket}/transactions_data.csv',
                                      header=True, schema=schema_transactions)
    sdf_users = spark.read.csv(f's3a://{bucket}/users_data.csv',
                               header=True, schema=schema_users)
    sdf_cards = spark.read.csv(f's3a://{bucket}/cards_data.csv',
                               header=True, schema=schema_cards)

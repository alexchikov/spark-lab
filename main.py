'''
Пример начала программы для запуска в spark-submit
'''
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DateType, DoubleType
from pyspark.sql.window import Window


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('my_cool_application') \
        .master("local[*]") \
        .getOrCreate()  # создаем spark-сессию
    # необходимо изменить master, если вы работаете на кластере 

    spark.sparkContext.setLogLevel('ERROR')
    
    
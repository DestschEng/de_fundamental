from datetime import datetime
from pyspark.sql import SparkSession, SQLContext

spark =(
    SparkSession.builder.appName("pyspark-rdd-demo-{}".format(datetime.today()))
    .master("spark://spark-master:7077")
    .getOrCreate())
sqlContext = SQLContext(spark)
sc = spark.sparkContext
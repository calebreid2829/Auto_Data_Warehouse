from clover.warehouse.manager import Manager
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Learning") \
    .config("spark.master", "local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
rootDir = "../../warehouse_setup/warehouse"
man = Manager(spark, rootDir)
man.showAll()
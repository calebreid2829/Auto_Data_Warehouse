from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


class Creator:
    __df: DataFrame
    __spark: SparkSession

    def __init__(self, spark, options=None):
        if options is None:
            options = {'': ''}
        self.__spark = spark

        tempdf = self.__spark.read.format(options["format"]) \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("infer.schema", "true") \
            .load(options["path"])
        self.setDF(tempdf)

    def setDF(self, df):
        self.__df = df

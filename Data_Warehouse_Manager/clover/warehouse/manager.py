from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
import re


def _createSpark():
    spark = SparkSession.builder \
        .appName("Learning") \
        .config("spark.master", "local[*]") \
        .getOrCreate()
    return spark


class Manager:
    __rootdir = ''
    __fact: DataFrame
    __dimensions = []
    __spark: SparkSession
    __target = None

    def __init__(self, spark=None, rootdir=''):
        if spark is None:
            self.__spark = _createSpark()
        else:
            self.__spark = spark
        if rootdir != '':
            self.__rootdir = rootdir
            self.__initialSetup()

    def load(self, rootdir):
        self.__rootdir = rootdir
        self.__initialSetup()

    def __initialSetup(self):
        for file in os.listdir(self.__rootdir):
            d = os.path.join(self.__rootdir, file)
            if os.path.isdir(d):
                if re.search(r"\\fact$", d) is not None:
                    self.__fact = self.__makeTable(d)
                else:
                    self.__dimensions.append(self.__makeTable(d))

    def __makeTable(self, path):
        return self.__spark.read.format("parquet") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("infer.schema", "true") \
            .load(path)

    def showAll(self):
        self.__fact.show()
        for df in self.__dimensions:
            df.show()

    def getFact(self):
        return self.__fact

    def getDimension(self, index):
        return self.__dimensions[index]

    def getAllDimensions(self):
        return self.__dimensions

    def select(self, *cols):
        df = self.__fact.select(*cols)
        self.__clearTarget()
        return df

    def fact(self):
        self.__target = self.__fact

    def dimension(self, index):
        self.__target = self.__dimensions[index]

    def __clearTarget(self):
        self.__target = None



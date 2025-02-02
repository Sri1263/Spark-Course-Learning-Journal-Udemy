from pyspark.sql import *


def main():

    spark = SparkSession.builder \
                                .appName("HelloSpark") \
                                .master("local[2]") \
                                .getOrCreate()
                                # .config("spark.driver.memory", "4g") \

    data_list = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
    df = spark.createDataFrame(data_list, ["Language", "Users"])
    df.show()


if __name__ == "__main__":
    main()

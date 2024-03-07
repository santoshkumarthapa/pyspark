from pyspark.sql import *
if __name__ == "__main__":

    spark = SparkSession.builder \
            .appName("hello spark") \
            .master("local[2]") \
            .getOrCreate()
    data_list = [( "ravi", 10),
                 ( "balaya", 20)
                 ]
    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.printSchema()
    df.show()
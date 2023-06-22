from pyspark.sql import *

spark = SparkSession.builder.config("spark.jars", "/home/ec2-user/postgresql-42.6.0.jar").master("local") \
    .appName("MiniProj").enableHiveSupport().getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432"
                                             "/testdb") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "emp_info") \
    .option("user", "consultants").option("password", "WelcomeItc@2022").load()
df.printSchema()
df.show(10)


from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432"
                                             "/testdb") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "emp_info") \
    .option("user", "consultants").option("password", "WelcomeItc@2022").load()
df.printSchema()

# Define the calculation of age
df_age = df.withColumn("DOB", to_date(col("DOB"), "M/d/yyyy")) \
    .withColumn("age", floor(datediff(current_date(), col("DOB")) / 365))
df_age.show(10)

# Define the increments based on departments and gender
department_increment_expr = when(col("dept") == "IT", 0.1) \
    .when(col("dept") == "Marketing", 0.12) \
    .when(col("dept") == "Purchasing", 0.15) \
    .when(col("dept") == "Operations", 0.18) \
    .when(col("dept") == "Finance", 0.2) \
    .when(col("dept") == "Management", 0.25) \
    .when(col("dept") == "Research and Development", 0.15) \
    .when(col("dept") == "Sales", 0.18) \
    .when(col("dept") == "Accounting", 0.15) \
    .when(col("dept") == "Human Resources", 0.12) \
    .otherwise(0)

# Calculate the increment based on department and gender
increment_expr = when(col("gender") == "Female", department_increment_expr + 0.1).otherwise(department_increment_expr)

# Calculate the incremented salary based on department and gender
df_increment = df_age.withColumn("increment", col("salary") * increment_expr) \
    .withColumn("new_salary", col("salary") + col("increment"))

# Show the updated DataFrame
df_increment.show(10)

# Sort the DataFrame by salary and department
sorted_df = df_increment.orderBy(col("salary").desc(), col("dept"))
sorted_df.show(10)


# df1.write.mode("overwrite").saveAsTable("product.dummy")

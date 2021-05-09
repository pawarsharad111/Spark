from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import Row

spark=SparkSession.builder.appName('SparkSQl').getOrCreate()

df=spark.read.option("header","true").option("inferSchema","true").csv(r'C:\Users\hp\Desktop\Spark\DataFrame\fakefriends-header (1).csv')

df.printSchema()
df.select("name").show()
df.filter(df.age<21).show()
df.groupBy("age").count().show()
df.select(['name','age']).show()
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,FloatType

spark=SparkSession.builder.appName("MinTemp").getOrCreate()

schema=StructType([StructField("stationID",StringType(),True),StructField("date",IntegerType(),True),
                   StructField("measure_type",StringType(),True),StructField("temprature",FloatType(),True)
                   ])

df=spark.read.schema(schema=schema).csv(r"C:/Users/hp/Desktop/Spark/DataFrame/1800 (1).csv")
df.show(10)
df.printSchema()

minTemp=df.filter(df.measure_type=="TMIN")
minTemp.show(10)

stationTemps=minTemp.select(["StationId","Temprature"])
minTempsByStation=stationTemps.groupBy("stationID").min("temprature")
minTempsByStation.show()

minTempsByStationF=minTempsByStation.withColumn("temprature",func.round(func.col("min(temprature)")*0.1*(9.0/5.0)+32.0,2))\
    .select("stationID","temprature").sort("temprature")

results=minTempsByStationF.collect()
for result in results:
    print(f"{result[0]} \t {result[1]}F")
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark=SparkSession.builder.appName('WordCount').getOrCreate()

inputDf=spark.read.text(r"C:/Users/hp/Desktop/Spark/DataFrame/book.txt")
words=inputDf.select(func.explode(func.split(inputDf.value, "\\W+")).alias("word"))
Words=words.filter(words.word != "")

lowercase=words.select(func.lower(words.word).alias("word"))
wordcounts=lowercase.groupBy("Word").count()
wordCountSorted=wordcounts.sort("count")
wordCountSorted.show(wordCountSorted.count())
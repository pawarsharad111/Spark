import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile(r"C:\Users\hp\Desktop\Spark\Spark Data/Book")
words = input.flatMap(normalizeWords)
a=words.collect()

for i in a:
    print(i)
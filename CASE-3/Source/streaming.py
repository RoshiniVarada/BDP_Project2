import sys
import os

import findspark
findspark.init('C:\spark-3.0.0-preview2-bin-hadoop2.7')

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("localhost", 8085)
Eachwords = lines.flatMap(lambda line: line.split(" "))
KeyPair = Eachwords.map(lambda word: (word, 1))
ResultCount = KeyPair.reduceByKey(lambda x, y: x + y)
ResultCount.pprint()

ssc.start()
ssc.awaitTermination()
from amaterasu.pyspark.runtime import AmaContext
import os
from pyspark import Row

ama_context: AmaContext = AmaContext.builder().build()

data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(os.listdir(os.getcwd()))
rdd = ama_context.sc.parallelize(data)
numDS = rdd.map(Row).toDF()

def g(x):
    print(x)

rdd.foreach(g)

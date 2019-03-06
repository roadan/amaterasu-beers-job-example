from amaterasu.pyspark.runtime import ama_context
import os
from pyspark import Row

data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(os.listdir(os.getcwd()))
rdd = ama_context.sc.parallelize(data)
numDS = rdd.map(Row).toDF()

def g(x):
    print(x)

rdd.foreach(g)

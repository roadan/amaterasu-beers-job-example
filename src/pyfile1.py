import os
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(os.listdir(os.getcwd()))
rdd = sc.parallelize(data)
numDS = rdd.map(Row).toDF()

def g(x):
    print(x)

rdd.foreach(g)

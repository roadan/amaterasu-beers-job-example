val data = 1 to 1000 

val rdd = sc.parallelize(data)

val odd = rdd.filter(n => n%2 != 0).toDF("number")

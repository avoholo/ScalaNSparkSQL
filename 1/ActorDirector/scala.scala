def rmHeader(line: String) = {
    val fields = line.split(",")
    (fields)
}

def rmCol(line: String) = {
	val fields = line.split(",")
	val actor = fields(0).toInt
	val director = fields(1).toInt
	(actor, director)
}

val actordirector = sc.textFile("file:////data/sample_data/workspace/SQL/Data/actordirector/actordirector.csv")

actordirector.collect().foreach(println)

var header = actordirector.first()
val data = actordirector.filter(row => row != header)
val rdd = data.map(rmHeader)
rdd.take(3)

val pair = data.map(rmCol)
val tmp = pair.map(x => (x,1))
val pair_count = tmp.reduceByKey(_ + _)
val answer = pair_count.collect().filter(count => count._2 > 2)(0)._1

// answer: (Int, Int) = (1,1)

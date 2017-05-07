// creating user rdd
val data_user1=sc.textFile("hdfs://cshadoop1/yelp/user/user.csv").map(line=>line.split("\\^"))

// creathing review rdd
val data_review=sc.textFile("hdfs://cshadoop1/yelp/review/review.csv).map(line=>line.split("\\^"))


val data_user=data_user1.map(line=>(line(0),line(1).toString))


val count=data_review.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b).distinct

val joinres=data_user.join(count).distinct.collect()

val sortedres=joinres.sortWith(_._2._2>_._2._2).take(10)

sortedres.foreach(line=>println(line._1,line._2._1,line._2._2))
System.exit(0)

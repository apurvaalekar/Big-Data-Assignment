
// creating user rdd
val data_user=sc.textFile("hdfs://cshadoop1/yelp/user/user.csv").map(line=>line.split("\\^"))

// creathing review rdd
val data_review=sc.textFile("hdfs://cshadoop1/yelp/review/review.csv").map(line=>line.split("\\^"))

val sumratings=data_review.map(line=>(line(1),line(3).toDouble)).reduceByKey((a,b)=>a+b).distinct 
val count=data_review.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b).distinct 
val mergecolums=sumratings.join(count) 
val review=mergecolums.map(a=>(a._1,a._2._1/a._2._2))
println("Enter the User name:(e.g. Matt J.)")
val user_name1=Console.readLine()



val check=data_user.filter(line=>line(1).contains(user_name1)).map(line=>(line(0).toString,line(1).toString))
val userData=data_user.map(line=>(line(0).toString,line(1).toString))
val res=review.join(userData)
val finalres=res.join(check)


finalres.foreach(line=>println(line._1,line._2._1._1,line._2._2))
System.exit(0)
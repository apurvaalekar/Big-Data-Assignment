// creating business rdd
val data_business=sc.textFile("hdfs://cshadoop1/yelp/business/business.csv").map(line=>line.split("\\^"))

// creathing review rdd
val data_review=sc.textFile("hdfs://cshadoop1/yelp/review/review.csv").map(line=>line.split("\\^"))


val check=data_business.filter(line=>line(1).contains("Stanford")).map(line=>(line(0).toString,line(1).toString))



val jtable = data_review.map(line=>(line(2).toString,(line(1).toString,line(3).toDouble)))
val res=jtable.join(check) 


res.foreach(line=>println(line._2._1._1,line._2._1._2))
System.exit(0)
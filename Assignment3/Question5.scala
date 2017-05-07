// creating business rdd
val data_business=sc.textFile("hdfs://cshadoop1/yelp/business/business.csv").map(line=>line.split("\\^"))

// creathing review rdd
val data_review=sc.textFile("hdfs://cshadoop1/yelp/review/review.csv").map(line=>line.split("\\^"))



val filter=data_business.filter(line=>line(1).contains("TX")).map(line=>(line(0).toString,line(1).toString))
val count=data_review.map(line=>(line(2),1)).reduceByKey((a,b)=>a+b)
val joinres=filter.join(count)
joinres.foreach(line=>println(line._1,line._2._2))
System.exit(0)
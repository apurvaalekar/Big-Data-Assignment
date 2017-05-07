val data_business=sc.textFile("hdfs://cshadoop1/yelp/business/business.csv").map(line=>line.split("\\^"));
     
val data_review=sc.textFile("hdfs://cshadoop1/yelp/review/review.csv").map(line=>line.split("\\^"))
     // creating key value pairs for businees where key is (business id) and value is (address and category)

		val dataKV_business=data_business.map(line=>(line(0),line(1).toString+line(2).toString))

// creating table for sum of rating for distinct business
val sum=data_review.map(line=>(line(2),line(3).toDouble)).reduceByKey((a,b)=>a+b).distinct


// creating distinct business id count
val count=data_review.map(line=>(line(2),1)).reduceByKey((a,b)=>a+b).distinct



val sum_count_join=sum.join(count)

//creating table for business id and avg rating count
val avg=sum_count_join.map(a=>(a._1,a._2._1/a._2._2))


// combining the results with business rdd
val res=dataKV_business.join(avg).distinct.collect()


// sorting counts
val sorted_res=res.sortWith(_._2._2>_._2._2).take(10)


// printing results
sorted_res.foreach(line=>println(line._1,line._2._1,line._2._2))

System.exit(0)
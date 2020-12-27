import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

//最受年轻人关注的<商家>
object YoungSellSpark {
   def main(args: Array[String]) {
     if (args.length < 3) {
       System.err.println("Usage: <input1 path> <input2 path> <output path>")
       System.exit(1)
     }

     val conf = new SparkConf().setAppName("Scala_YoungSell")
     val sc = new SparkContext(conf)
     val inputLog = sc.textFile(args(0))
     val inputInfo = sc.textFile(args(1))

     //old
     val infoClean = inputInfo.na.fill(value="0".toInt, Array("gender"))
     val infoClean = inputInfo.na.fill(value="2".toInt, Array("age_range"))
     
     val old = infoClean.filter(x=>(x.split(",")(1).equals("1")==false && x.split(",")(1).equals("2")==false && x.split(",")(1).equals("3")==false))
     val oldUsers = old.map(x=>(x.split(",")(0),1))//<user_id, 1>

     
     val rdd = inputLog.filter(x=>x.split(",")(5).equals("1111"))
     val rdd2 = rdd.filter(x=>x.split(",")(6).equals("0")==false)
     val users = rdd2.map(x=>(x.split(",")(0),x.split(",")(3)))//<user_id, seller>

     val pair = users.subtractByKey(oldUsers)
     val counts = pair.map(x=>(x._2, 1)).reduceByKey(_+_)//<seller, 1>

     val result = counts.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1)).take(100)
     val r = sc.parallelize(result)
     r.saveAsTextFile(args(2))
     sc.stop()
    
    }
}
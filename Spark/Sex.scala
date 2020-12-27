import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

//双十一购买了商品的男女比例
object Sex {
   def main(args: Array[String]) {
     if (args.length < 3) {
       System.err.println("Usage: <input1 path> <input2 path> <output1 path> <output2 path>")
       System.exit(1)
     }

     val conf = new SparkConf().setAppName("Sex")
     val sc = new SparkContext(conf)
     val inputLog = sc.textFile(args(0))
     val inputInfo = sc.textFile(args(1))

     //sex
     val infoClean = inputInfo.na.fill(value="0".toInt, Array("gender"))
     val infoClean = inputInfo.na.fill(value="2".toInt, Array("age_range"))


     val man = infoClean.filter(x=>(x.split(",")(2).equals("1"))).map(x=>(x.split(",")(0),1))
     val woman = infoClean.filter(x=>(x.split(",")(2).equals("0"))).map(x=>(x.split(",")(0),1))

     
     val rdd = inputLog.filter(x=>x.split(",")(5).equals("1111"))
     val rdd2 = rdd.filter(x=>x.split(",")(6).equals("2")) //purchase
     val users = rdd2.map(x=>(x.split(",")(0),1))//<user_id, 1>
     val log = inputLog.map(x=>(x.split(",")(0),1))
     val notUsers = log.subtractByKey(users)

     val manUsers = man.subtractByKey(notUsers).map(x=>("man",1))
     val manCounts = manUsers.reduceByKey(_+_).map(x=>x._2)

     val womanUsers = woman.subtractByKey(notUsers).map(x=>("woman",1))
     val womanCounts = womanUsers.reduceByKey(_+_).map(x=>x._2)
     

     manCounts.saveAsTextFile(args(2))
     womanCounts.saveAsTextFile(args(3))

     sc.stop()

    }
}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

//2.购买了商品的买家 年龄段比例

object Age {
   def main(args: Array[String]) {
     if (args.length < 3) {
       System.err.println("Usage: <input1 path> <input2 path> <output path>")
       System.exit(1)
     }

     val conf = new SparkConf().setAppName("Age")
     val sc = new SparkContext(conf)
     val inputLog = sc.textFile(args(0))
     val inputInfo = sc.textFile(args(1))

     //age range
     val infoClean = inputInfo.na.fill(value="0".toInt, Array("gender"))
     val infoClean = inputInfo.na.fill(value="2".toInt, Array("age_range"))


     val ageLess18 = infoClean.filter(x=>(x.split(",")(1).equals("1"))).map(x=>(x.split(",")(0),1))//<user_id, 1>
     val age18to24 = infoClean.filter(x=>(x.split(",")(1).equals("2"))).map(x=>(x.split(",")(0),1))//<user_id, 1>
     val age25to29 = infoClean.filter(x=>(x.split(",")(1).equals("3"))).map(x=>(x.split(",")(0),1))//<user_id, 1>
     val age30to34 = infoClean.filter(x=>(x.split(",")(1).equals("4"))).map(x=>(x.split(",")(0),1))//<user_id, 1>
     val age35to39 = infoClean.filter(x=>(x.split(",")(1).equals("5"))).map(x=>(x.split(",")(0),1))//<user_id, 1>
     val age40to49 = infoClean.filter(x=>(x.split(",")(1).equals("6"))).map(x=>(x.split(",")(0),1))//<user_id, 1>
     val ageMore50 = infoClean.filter(x=>(x.split(",")(1).equals("7") || x.split(",")(1).equals("8"))).map(x=>(x.split(",")(0),1))//<user_id, 1>

     
     val rdd = inputLog.filter(x=>x.split(",")(5).equals("1111"))
     val rdd2 = rdd.filter(x=>x.split(",")(6).equals("2"))
     val users = rdd2.map(x=>(x.split(",")(0),1))//<user_id, 1>
     val log = inputLog.map(x=>(x.split(",")(0),1))
     val notUsers = log.subtractByKey(users)

     val ageLess18Users = ageLess18.subtractByKey(notUsers).map(x=>("ageLess18",1)).reduceByKey(_+_).map(x=>x._2)//<user_id, 1> -> <cate1, 1>
     val age18to24Users = age18to24.subtractByKey(notUsers).map(x=>("age18to24",1)).reduceByKey(_+_).map(x=>x._2)//<user_id, 1> -> <cate2, 1>
     val age25to29Users = age25to29.subtractByKey(notUsers).map(x=>("age25to29",1)).reduceByKey(_+_).map(x=>x._2)//<user_id, 1> -> <cate3, 1>
     val age30to34Users = age30to34.subtractByKey(notUsers).map(x=>("age30to34",1)).reduceByKey(_+_).map(x=>x._2)//<user_id, 1> -> <cate4, 1>
     val age35to39Users = age35to39.subtractByKey(notUsers).map(x=>("age35to39",1)).reduceByKey(_+_).map(x=>x._2)//<user_id, 1> -> <cate5, 1>
     val age40to49Users = age40to49.subtractByKey(notUsers).map(x=>("age40to49",1)).reduceByKey(_+_).map(x=>x._2)//<user_id, 1> -> <cate6, 1>
     val ageMore50Users = ageMore50.subtractByKey(notUsers).map(x=>("ageMore50",1)).reduceByKey(_+_).map(x=>x._2)//<user_id, 1> -> <cate7, 1>

     val r1 = ageMore50Users.union(age40to49Users)
     val r2 = age35to39Users.union(r1)
     val r3 = age30to34Users.union(r2)
     val r4 = age25to29Users.union(r3)
     val r5 = age18to24Users.union(r4)
     val r6 = ageLess18Users.union(r5)

     r6.saveAsTextFile(args(2))
     sc.stop()
    
    }
}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object HotSellSpark {
   def main(args: Array[String]) {
     if (args.length < 2) {
       System.err.println("Usage: <input path> <output path>")
       System.exit(1)
     }

     val conf = new SparkConf().setAppName("Scala_HotSell")
     val sc = new SparkContext(conf)
     val input = sc.textFile(args(0))

     val rdd = input.filter(x=>x.split(",")(5).equals("1111"))

     val rdd2 = rdd.filter(x=>x.split(",")(6).equals("0")==false)
     val counts = rdd2.map(x=>(x.split(",")(1),1)).reduceByKey(_+_)


     val result = counts.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1)).take(100)
     val r = sc.parallelize(result)
     r.saveAsTextFile(args(1))
     sc.stop()
    
    }
   
}
   


/*

spark-submit --class "HotSellSpark" --master spark://jcn181250057-master:7077 target/scala-2.11/hotsell_2.11-1.0.jar hdfs://jcn181250057-master:9000/user/root/inputLogPart/user_log_part.csv hdfs://jcn181250057-master:9000/user/root/output

*/


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object RDD_Demo {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("RDD_Demo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // creating rdd1
//    val rdd1 = sc.parallelize(Array(1,2,3,4,5))
//    println("elements of rdd1: ")
//    rdd1.foreach(x => print(x + ","))
//    println()
//
//    // creating rdd2
//    val rdd2 = sc.parallelize(Array(6,7,8,9,10))
//    println("elements of rdd2: ")
//    rdd1.foreach(x => print(x + ","))
//    println()
//
//    val rdd3 = rdd1.union(rdd2)
//    println("elements of rdd3: ")
//    rdd3.foreach(x=>print(x + ","))
//    println()

    val text_file = sc.textFile("/home/saurabh/Desktop/Spafka_RW/Read/SR_To_Do-list")
    val rd_text = text_file.flatMap(_.split(" "))
    val wordCounts = rd_text.map((_, 1)).reduceByKey(_ + _)

    // Warning: always change the file name before running this application, otherwise it return with exit code 1 instead of 0.
    wordCounts.saveAsTextFile("/home/saurabh/Desktop/Spafka_RW/Write/SR_To_Do-list_1")

  }
}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object RDD_Demo {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("RDD_Demo")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

//    val text_file = sc.textFile("/home/saurabh/Desktop/Spafka_RW/Read/SR_To_Do-list")
    val text_file = sc.textFile(args(0))
    println("Input Source: " + args(0))
    val rd_text = text_file.flatMap(_.split(" "))
    val wordCounts = rd_text.map((_, 1)).reduceByKey(_ + _)
//    wordCounts.saveAsTextFile("/home/saurabh/Desktop/Spafka_RW/Write/SR_To_Do-list_1")
    wordCounts.saveAsTextFile(args(1))
    println("Output Source: " + args(1))

  }
}


//spark-submit --class RDD_Demo target/scala-2.12/sparkdemo_2.12-1.0.jar local output /home/saurabh/Desktop/Spafka_RW/Read/SR_To_Do-list.txt
//spark-submit --class RDD_Demo target/scala-2.12/sparkdemo_2.12-1.0.jar /home/saurabh/Desktop/Spafka_RW/Read/SR_To_Do-list.txt /home/saurabh/Desktop/Spafka_RW/Write/SR_To_Do-list_1


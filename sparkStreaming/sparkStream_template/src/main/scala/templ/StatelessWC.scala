package templ

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatelessWC {
  def main(args: Array[String]) {
    /*入口
    // 可以通过ssc.sparkContext 来访问SparkContext        ssc=>sc

    // 或者通过已经存在的SparkContext来创建StreamingContext
    val sc = ...
    val ssc = new StreamingContext(sc, Seconds(1))       sc=>ssc */

    val ssc = new StreamingContext(
      new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
      , Seconds(5)) //一个接收器要有一个线程  至少要  local[2]

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("hadoop102", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map((_, 1))
    val resultDStream = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    resultDStream.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

    //ssc.stop()            // don't
  }
}

package windowOperations

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

object RBKAWOptimize {
  def main(args: Array[String]) {
    //Program arguments:    hadoop102 9999 20 10
    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // 创建StreamingContext，batch interval为5秒
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./sparkStreaming/checkpoint/RBKAWOptimize")


    //Socket为数据源
    val lines = ssc.socketTextStream("hadoop102", 9999, StorageLevel.MEMORY_ONLY_SER)

    val words = lines.flatMap(_.split(" "))

    //以过去20秒钟为一个输入窗口，每10秒统计一下WordCount
    //把新加入的窗口进行方法, 再对过期的窗口进行方法
    val wordCounts = words.map(x => (x, 1))
      .reduceByKeyAndWindow(
        _ + _,
        _ - _,
        Seconds(20),
        Seconds(10)
      )

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
package windowOperations

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

object CBWWordC {
  def main(args: Array[String]) {
    //hadoop102 9999 20 10
    if (args.length != 4) {
      System.err.println("Usage: WindowWorldCount <hostname> <port> <windowDuration> <slideDuration>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./sparkStreaming/checkpoint/CBWWordC")


    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(" "))

    //countByWindow 方法计算基于滑动窗口的DStream中的元素的数量。
    val countByWindow=words.countByWindow(Seconds(args(2).toInt), Seconds(args(3).toInt))

    countByWindow.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

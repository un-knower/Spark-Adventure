package windowOperations

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

object RBWWordC {
  def main(args: Array[String]) {
    //Program arguments:    hadoop102 9999 20 10
    if (args.length != 4) {
      System.err.println("Usage: WindowWorldCount <hostname> <port> <windowDuration> <slideDuration>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // 创建StreamingContext，batch interval为5秒
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./sparkStreaming/checkpoint/RBWWordC")


    //Socket为数据源
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER)

    val words = lines.flatMap(_.split(" "))

    //windows操作，对窗口中的单词进行计数
    //以过去5秒钟为一个输入窗口，每1秒统计一下WordCount，本方法会将过去20秒钟的WordCount都进行统计 10秒统计一次
    //然后进行叠加，得出这个窗口中的单词统计。 这种方式被称为叠加方式，如下图左边所示
    val wordCounts = words
      .reduceByWindow(
        _ + _,
      //_ - _,
        Seconds(args(2).toInt),
        Seconds(args(3).toInt)
      )

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

/*
input:  Spark is a fast Spark is a fast and
-------------------------------------------
Time: 1538190245000 ms
-------------------------------------------

-------------------------------------------
Time: 1538190255000 ms
-------------------------------------------
SparkisafastSparkisafastand

-------------------------------------------
Time: 1538190265000 ms
-------------------------------------------
SparkisafastSparkisafastandSparkisafastSparkisafastand
*/
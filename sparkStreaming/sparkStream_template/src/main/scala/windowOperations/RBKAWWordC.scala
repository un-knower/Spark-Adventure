package windowOperations

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

object RBKAWWordC {
  def main(args: Array[String]) {
    //Program arguments:    hadoop102 9999 20 10
    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // 创建StreamingContext，batch interval为5秒
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./sparkStreaming/checkpoint/RBKAWWordC")


    //Socket为数据源
    val lines = ssc.socketTextStream("hadoop102", 9999, StorageLevel.MEMORY_ONLY_SER)

    val words = lines.flatMap(_.split(" "))

    //windows操作，对窗口中的单词进行计数
    //以过去5秒钟为一个输入窗口，每1秒统计一下WordCount，本方法会将过去20秒钟的WordCount都进行统计 10秒统计一次
    //然后进行叠加，得出这个窗口中的单词统计。 这种方式被称为叠加方式，如下图左边所示
    val wordCounts = words.map(x => (x, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(20),
        Seconds(10)
      )

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
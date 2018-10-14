package AccumulatorNBroadcastVariables

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator

object AccumulatorNBroadcast extends App {

  //单例 广播变量
  object WordBlacklist {
    @volatile private var instance: Broadcast[Seq[String]] = _

    def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            val wordBlacklist = Seq("a", "b", "c")
            instance = sc.broadcast(wordBlacklist)
          }
        }
      }
      instance
    }
  }

  //单例 累加器
  object DroppedWordsCounter {
    @volatile private var instance: LongAccumulator = _

    def getInstance(sc: SparkContext): LongAccumulator = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = sc.longAccumulator("WordsInBlacklistCounter")
          }
        }
      }
      instance
    }
  }

  val ssc = new StreamingContext(
    new SparkConf().setMaster("local[*]").setAppName("AccumulatorNBroadcast")
    , Seconds(5)) //一个接收器要有一个线程  至少要  local[2]

  ssc.checkpoint("./sparkStreaming/checkpoint/AccumulatorNBroadcast")


  val lines = ssc.socketTextStream("hadoop102", 9999)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map((_, 1))


  val wordCounts = pairs.reduceByKey(_ + _)

  //foreachRDD 将DStream的输出转换成了RDD的输出
  wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: Time) =>
    // Get or register the blacklist Broadcast  广播变量黑名单list
    val blacklist = WordBlacklist.getInstance(rdd.sparkContext)

    // Get or register the droppedWordsCounter Accumulator  黑名单过滤出来的词的次数 累加器
    val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)

    // Use blacklist to drop words and use droppedWordsCounter to count them
    val counts = rdd.filter { case (word, count) =>
      if (blacklist.value.contains(word)) {
        droppedWordsCounter.add(count)
        false
      } else {
        true
      }
    }.collect().mkString("[", ", ", "]")

    val output = "Counts at time " + time + " " + counts + ", droppedAccumulator:" + droppedWordsCounter.value
    println(output)
  }

  ssc.start()
  ssc.awaitTermination()
}

/** 有问题  累加器的次数总数  一直加上 每次的黑名单数量
  * */
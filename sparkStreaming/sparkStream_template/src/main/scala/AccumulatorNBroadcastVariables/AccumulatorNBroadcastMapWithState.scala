package AccumulatorNBroadcastVariables

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorNBroadcastMapWithState extends App {

  //单例 广播变量
  object WordBlacklist {
    private var instance: Broadcast[Seq[String]] = _

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
    private var instance: LongAccumulator = _

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
  val sc = ssc.sparkContext

  val lines = ssc.socketTextStream("hadoop102", 9999)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map((_, 1))

  // Get or register the blacklist Broadcast  广播变量黑名单list
  val blacklist = WordBlacklist.getInstance(sc)

  // Get or register the droppedWordsCounter Accumulator  黑名单过滤出来的词的次数 累加器
  val droppedWordsCounter = DroppedWordsCounter.getInstance(sc)

  // Use blacklist to drop words and use droppedWordsCounter to count them
  val counts = pairs.filter { case (word, count) =>
    if (blacklist.value.contains(word)) {
      droppedWordsCounter.add(count)
      false
    } else {
      true
    }
  }

  //自定义mappingFunction，累加单词出现的次数并更新状态
  val mappingFunc = (word: String, count: Option[Int], state: State[Long]) => {
    val sum = count.getOrElse(0) + state.getOption.getOrElse(0L)
    val output = (word, sum)
    state.update(sum)
    output
  }

  //调用mapWithState进行管理流数据的状态
  val wordCounts = counts.mapWithState(StateSpec.function[String, Int, Long, (String, Long)](mappingFunc))


  //foreachRDD 将DStream的输出转换成了RDD的输出
  /** 不能把累加器逻辑放在 foreachRDD函数里 */
  wordCounts.foreachRDD { (rdd: RDD[(String, Long)], time: Time) => {
    val output = "Counts at time " +
      time + " " +
      rdd.collect().mkString("[", ", ", "]") +
      ", droppedAccumulator:" + droppedWordsCounter.value
    println(output)
  }
  }

  ssc.start()
  ssc.awaitTermination()
}

/** 有问题  累加器的次数总数  一直加上 每次的黑名单数量
  * */
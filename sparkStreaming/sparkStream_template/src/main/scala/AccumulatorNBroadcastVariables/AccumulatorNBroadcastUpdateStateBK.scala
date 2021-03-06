package AccumulatorNBroadcastVariables

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorNBroadcastUpdateStateBK extends App {

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
  val sc: SparkContext = ssc.sparkContext


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

  // 定义一个更新方法，values是当前批次RDD中相同key的value集合，state是框架提供的上次state的值
  // values在这里是所有word 里相同word 的所有的1
  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    // 计算当前批次相同key的单词总数
    val currentCount = values.sum
    // 获取上一次保存的单词总数
    val previousCount = state.getOrElse(0)
    // 返回新的单词总数
    Some(currentCount + previousCount)
  }
  // 使用updateStateByKey方法，类型参数是状态的类型，后面传入一个更新方法。
  val wordCounts = counts.updateStateByKey[Int](updateFunc)

  wordCounts.foreachRDD { (rdd: RDD[(String, Int)], time: Time) => {
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
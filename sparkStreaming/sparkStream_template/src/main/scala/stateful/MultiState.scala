package stateful

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MultiState extends App {

  // 需要创建一个StreamingContext
  val ssc = new StreamingContext(
    new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount"),
    Seconds(5))

  // 需要设置一个checkpoint的目录。
  ssc.checkpoint("./sparkStreaming/checkpoint/MultiState")

  // 通过StreamingContext来获取master01机器上9999端口传过来的语句
  val lines = ssc.socketTextStream("hadoop102", 9999)

  // 需要通过空格将语句中的单词进行分割DStream[RDD[String]]
  val words = lines.flatMap(_.split(" "))

  // 需要将每一个单词都映射成为一个元组（word,1）
  val pairs = words.map((_, 1))


  // 定义一个更新方法，values是当前批次RDD中相同key的value集合，state是框架提供的上次state的值
  // values在这里是所有word 里相同word 的所有的1
  //                                        总数  本次数  上次数
  val stateDstream = pairs.updateStateByKey[(Int, Int, Int)] {
    (values: Seq[Int], state: Option[(Int, Int, Int)]) =>
      // 获取上一次保存的单词总数
      val previousCount = state.getOrElse(0, 0, 0)
      // 返回新的单词总数
      Some(values.sum + previousCount._1, values.sum, previousCount._1)  //三个状态
  }

  stateDstream.print()

  ssc.start() // Start the computation
  ssc.awaitTermination() // Wait for the computation to terminate
}

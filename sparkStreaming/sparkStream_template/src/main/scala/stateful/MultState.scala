package stateful

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MultState extends App {

  // 需要创建一个StreamingContext
  val ssc = new StreamingContext(
    new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount"),
    Seconds(5))

  // 需要设置一个checkpoint的目录。
  ssc.checkpoint("./sparkStreaming/checkpoint")

  // 通过StreamingContext来获取master01机器上9999端口传过来的语句
  val lines = ssc.socketTextStream("hadoop102", 9999)

  // 需要通过空格将语句中的单词进行分割DStream[RDD[String]]
  val words = lines.flatMap(_.split(" "))

  //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
  // 需要将每一个单词都映射成为一个元组（word,1）
  val pairs = words.map((_, 1))


  //                                        总数  个数  次数
  val stateDstream = pairs.updateStateByKey[(Int, Int, Int)] {
    (values: Seq[Int], state: Option[(Int, Int, Int)]) =>
      // 计算当前批次相同key的单词总数
      // 获取上一次保存的单词总数
      // 返回新的单词总数
      Some(values.sum + state.get._1, 0, 0)  //三个状态
  }

  stateDstream.print()

  ssc.start() // Start the computation
  ssc.awaitTermination() // Wait for the computation to terminate
}

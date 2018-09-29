package stateful

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyWC extends App {

  // 需要创建一个StreamingContext
  val ssc = new StreamingContext(
    new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount"),
    Seconds(5))

  // 需要设置一个checkpoint的目录。
  ssc.checkpoint("./sparkStreaming/checkpoint/UpdateStateByKeyWC")

  // 通过StreamingContext来获取master01机器上9999端口传过来的语句
  val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

  // 需要通过空格将语句中的单词进行分割DStream[RDD[String]]
  val words: DStream[String] = lines.flatMap(_.split(" "))

  //import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
  // 需要将每一个单词都映射成为一个元组（word,1）
  val pairs: DStream[(String, Int)] = words.map(word => (word, 1))


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
  val stateDStream = pairs.updateStateByKey[Int](updateFunc)
  //输出
  stateDStream.print()
//  stateDstream.saveAsTextFiles("hdfs://hadoop102:9000/statful/","abc")

  ssc.start() // Start the computation
  ssc.awaitTermination() // Wait for the computation to terminate
}

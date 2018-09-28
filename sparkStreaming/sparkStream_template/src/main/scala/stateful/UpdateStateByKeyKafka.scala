package stateful

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyKafka extends App {

  // 需要创建一个StreamingContext
  val ssc = new StreamingContext(
    new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount"),
    Seconds(5))

  // 需要设置一个checkpoint的目录。
  ssc.checkpoint("./sparkStreaming/chkUpdateStateByKeyKafka")

  val params = Map("bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    , "group.id" -> "kafka")
  val topic = Set("from1")
  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, params, topic)
  val pairs: DStream[(String, Int)] = messages.flatMap(_._2.split(" ")).map { x => (x, 1) }

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

  stateDStream.print()
//  stateDStream.saveAsTextFiles("hdfs://hadoop102:9000/statful/","abc")

  ssc.start() // Start the computation
  ssc.awaitTermination() // Wait for the computation to terminate
}

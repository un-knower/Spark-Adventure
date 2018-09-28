package stateful

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object MapWithStateKafka extends App {
  val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  ssc.checkpoint("./sparkStreaming/chkMapWithStateWC")

  val params = Map("bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    , "group.id" -> "kafka")
  val topic = Set("from1")
  val initialRDD = ssc.sparkContext.parallelize(List[(String, Long)]())
  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, params, topic)
  val word = messages.flatMap(_._2.split(" ")).map { x => (x, 1) }

  //自定义mappingFunction，累加单词出现的次数并更新状态
  val mappingFunc = (word: String, count: Option[Int], state: State[Long]) => {
    val sum = count.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    state.update(sum)
    output
  }

  //调用mapWithState进行管理流数据的状态
  val stateDStream = word.mapWithState(StateSpec.function[String, Int, Long, (String, Long)](mappingFunc).initialState(initialRDD))

  stateDStream.print()

  ssc.start()
  ssc.awaitTermination()

}

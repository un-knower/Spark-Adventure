package stateful

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object MapWithStateWC extends App{
  val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
  val ssc = new StreamingContext(sparkConf, Seconds(10))
  ssc.checkpoint("./sparkStreaming/checkpointMapWithStateWC")

  val params = Map("bootstrap.servers" -> "hadoop102:9092", "group.id" -> "scala-stream-group")
  val topic = Set("test")
  val initialRDD = ssc.sparkContext.parallelize(List[(String, Int)]())
  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, params, topic)
  val word = messages.flatMap(_._2.split(" ")).map { x => (x, 1) }

  //自定义mappingFunction，累加单词出现的次数并更新状态
  val mappingFunc = (word: String, count: Option[Int], state: State[Int]) => {
    val sum = count.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    state.update(sum)
    output
  }

  //调用mapWithState进行管理流数据的状态
  val stateDstream = word.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

  stateDstream.print()

  ssc.start()
  ssc.awaitTermination()

}

package stateful

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object MapWithStateKafkaDirect extends App {
  val conf = new SparkConf().setAppName("MapWithStateKafkaDirect").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)
  val checkpointDir = "hdfs://hdfs-cluster/ecommerce/checkpoint/2"
  val brokers = "hadoop-all-01:9092,hadoop-all-02:9092,hadoop-all-03:9092"

  def mappingFunction(key: String, value: Option[Int], state: State[Long]): (String, Long) = {
    // 获取之前状态的值
    val oldState = state.getOption().getOrElse(0L)
    // 计算当前状态值
    val newState = oldState + value.getOrElse(0)
    state.update(newState)

    (key, newState)
  }

  val spec = StateSpec.function[String, Int, Long, (String, Long)](mappingFunction _)

  def creatingFunc(): StreamingContext = {
    val ssc = new StreamingContext(sc, Seconds(5))
    val kafkaParams = Map("metadata.broker.list" -> brokers, "group.id" -> "MapWithStateKafkaDirect")
    val topics = Set("count")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val results: DStream[(String, Long)] = messages.filter(_._2.nonEmpty).mapPartitions(iter => {
      iter.flatMap(_._2.split(" ").map((_, 1)))
    }).mapWithState(spec)
    results.print()
    ssc.checkpoint(checkpointDir)
    ssc
  }


  val ssc = StreamingContext.getOrCreate(checkpointDir, creatingFunc)

  ssc.start()
  ssc.awaitTermination()
}
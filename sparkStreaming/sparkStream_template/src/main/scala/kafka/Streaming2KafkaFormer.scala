package kafka

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming2KafkaFormer {
  def main(args: Array[String]): Unit = {

    // 创建StreamingContext
    val ssc = new StreamingContext(new SparkConf().setAppName("kafka").setMaster("local[*]"), Seconds(5))

    //应该从main方法的参数来的
    val fromTopic = "from1"
    val toTopic = "to1"
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"

    val kafkaPro = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> "kafkaFormer",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )


    val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPro, Set(fromTopic))

    var offsetRanges = Array[OffsetRange]()

    //获取采集的数据的偏移量
    val mapDstream = stream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2)


    /** 应该在这里获取上一次的Offset */
    //CTRL+ { }
    mapDstream.foreachRDD { rdd =>
      for(offset <- offsetRanges)
        println(offset)
      rdd.foreachPartition { items =>
        /**处理了业务*/
        for (item <- items) {
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

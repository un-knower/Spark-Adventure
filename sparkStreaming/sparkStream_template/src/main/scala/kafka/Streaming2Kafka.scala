package kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming2Kafka {
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
      ConsumerConfig.GROUP_ID_CONFIG -> "kafka",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    /** 根据上一次的Offset来创建 */
    // 链接到了kafka
    val stream: InputDStream[(String, String)] = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPro, Set(fromTopic))

    /** 应该在这里获取上一次的Offset */
    //CTRL+ { }
    stream.map { case (k, v) => "ABC:" + v }.foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        //写回kafka, 连接池
        val genericObjectPoolOfKafkaProxy = KafkaPool(brokers)
        //拿到具体的KafkaProxyFactory里的KafkaProxy
        val kafkaProxy = genericObjectPoolOfKafkaProxy.borrowObject()

        for (item <- items) {
          //使用
          kafkaProxy.kafkaClient.send(new ProducerRecord[String, String](toTopic, item)) //topic ,value(item就是"ABC:" + v=)
        }

        //使用完了放kafkaProxy回连接池
        genericObjectPoolOfKafkaProxy.returnObject(kafkaProxy)
      }

      /** 应该更新Offset */
    }


    ssc.start()
    ssc.awaitTermination()
  }
}

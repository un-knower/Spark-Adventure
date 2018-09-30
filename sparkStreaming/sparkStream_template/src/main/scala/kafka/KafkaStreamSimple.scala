package kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

object KafkaStreamSimple {
  def main(args: Array[String]): Unit = {
    //创建StreamingContext
    val ssc = new StreamingContext(new SparkConf().setAppName("kafkaStream").setMaster("local[*]"), Duration(5))

    //指定消息topic名称
    val topic = "wordcount"
    //指定kafka地址
    val brokerList = "hadoop102:9092,hadoop103:9092,hadoop104:9092"

    //准备连接参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> "g001",
      //从头开始读数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    //指定zookeeper地址
    val zkQuorum = "hadoop102:2181,hadoop103:2181,hadoop104:2181"

    //ZKGroupTopicDirs用于记录偏移量的路径
    val topicDirs = new ZKGroupTopicDirs("g001", topic)
    //获取zookeeper偏移量路径
    //g001/offsets/wordcount
    val zkTopicDir = s"${topicDirs.consumerOffsetDir}"

    //创建zkClient,后面需要读取偏移量和更新编移量
    val zKClient = new ZkClient(zkQuorum)

    //g001/offsets/wordcount/0/10001
    //g001/offsets/wordcount/1/10002
    //g001/offsets/wordcount/2/10003
    val children = zKClient.countChildren(zkTopicDir)

    var kafkaStream: InputDStream[(String, String)] = null

    //如果保存偏移量
    if (children > 0) {
      //如果在zookeeper中保存偏移量,kafkaStream可以通过offset去读取数据起始位置
      var fromOffsets: Map[TopicAndPartition, Long] = Map()

      for (i <- 0 until children) {
        //g001/offsets/wordcount/2/10003
        //g001/offsets/wordcount/0
        val partitionOffset = zKClient.readData[String](s"$zkTopicDir/$i")
        //获取topic
        val tp = TopicAndPartition(topic, i)

        ///10003
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      //key value   "hello how are you"  tuple
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      //没有保存偏移量
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
    }

    //偏移量的范围
    var offsetRange = Array[OffsetRange]()

    //该transform,获取RDD偏移量,返回RDD->DStream
    val transform: DStream[String] = kafkaStream.transform { rdd =>
      offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2)

    //依次迭代DStream --> RDD
    transform.foreachRDD { rdd =>
      rdd.foreachPartition(partition => {
        partition.foreach(x =>
          println(x)
        )
      })

      for (o <- offsetRange) {
        ///g001/offsets/wordcount/0
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"

        ZkUtils.updatePersistentPath(zKClient, zkPath, o.untilOffset.toString)
      }
    }

    ssc.start()

    ssc.awaitTermination()
  }
}

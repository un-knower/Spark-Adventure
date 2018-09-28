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
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
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

    /** ZK偏移量逻辑 开始 */
    //获取zookeeper的信息
    val zookeeperList = "hadoop102:2181,hadoop103:2181,hadoop104:2181"

    //获取保存offset的zk路径  GROUP_ID_CONFIG-> kafka
    /* ls /consumers/kafka/offsets/from1
    [0, 1]
    */
    val zkGroupTopicDirs = new ZKGroupTopicDirs("kafka", fromTopic)
    val zkConsumerGroupOffsetTopicDir = s"${zkGroupTopicDirs.consumerOffsetDir}"

    //创建一个到Zookeeper的连接
    val zkClient = new ZkClient(zookeeperList) /**这里可优化用线程池*/

    //获取偏移的保存地址目录下的子节点数目
    val children = zkClient.countChildren(zkConsumerGroupOffsetTopicDir)

    var stream: InputDStream[(String, String)] = null

    if (children > 0) {
      println("children>0 了 有偏移量信息")
      //新建一个变量,保存最终新建stream时要传入的 消费的偏移量
      var fromOffsets: Map[TopicAndPartition, Long] = Map()

      //首先获取每一个Partition的主节点的信息
      val topicList = List(fromTopic)
      //创建一个获取元信息的请求
      val request = new TopicMetadataRequest(topicList, 0)
      //创建了任意一个客户端到Kafka的连接
      //去拿到当前情况下每个partition的偏移量  因为可能程序挂的时间较长,kafka的信息会过期的
      val consumerToGetLeader = new SimpleConsumer("hadoop102", 9092, 100000, 10000, "OffsetLookUp")

      //通过连接 发送一个获取元信息的请求 得到响应
      val response = consumerToGetLeader.send(request)

      //topicsMetadata 获得了这个topic 所有的元信息  headOption拿到
      val topicsMetadataOption = response.topicsMetadata.headOption

      //建立 partitionID和其Leader的 hostname的映射 partitions
      val partitions = topicsMetadataOption match {
        case Some(tm) =>
          tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None =>
          Map[Int, String]()
      }

      //关闭这个连接
      consumerToGetLeader.close()

      println("partitions information is : " + partitions)
      println("children information is : " + children)

      for (i <- 0 until children) {
        // 获取保存在ZK中的偏移信息
        //get /brokers/topics/from1/partitions/    0  1
        val partitionOffset = zkClient.readData[String](s"${zkGroupTopicDirs.consumerOffsetDir}/$i")
        println(s"Partition[$i] 目前保存的偏移信息是 : $partitionOffset")

        val tp = TopicAndPartition(fromTopic, i)
        //获取当前partition的最小偏移量 (防止kafka中的数据过期问题)
        //如果zk保存的offset比kafka的offset小 zk得增大成kafka最小的offset
        //否则还用刚才zk保存的offset,会出错,因为kafka里已经没有那个数据
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
        val consumerMin = new SimpleConsumer(partitions(i), 9092, 100000, 10000, "getMinOffset")
        val response = consumerMin.getOffsetsBefore(requestMin)

        //获取当前 kafka 这个topic的这个partition的最小偏移量
        val curKafkaOffsets = response.partitionErrorAndOffsets(tp).offsets
        consumerMin.close()

        //获取当前 zk 的偏移量
        var nextOffset = partitionOffset.toLong

        // 如果zk offset < kafka的保质期内的offset  ,即zk offset的已经是过期的,要到kafka的保质期内
        if (curKafkaOffsets.nonEmpty && nextOffset < curKafkaOffsets.head) {
          nextOffset = curKafkaOffsets.head
        }
        println(s"Partition[$i] kafka的保质期内的offset是 : ${curKafkaOffsets.head}")
        println(s"Partition[$i] 修正后的偏移信息是 : $nextOffset")

        fromOffsets += (tp -> nextOffset) //把这个修正的offset加入
      }

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)
      println("从ZK获取偏移量来创建DStream")

      zkClient.close()

      //拿到 fromOffsets来新建stream
      stream = KafkaUtils.createDirectStream
        [String, String, StringDecoder, StringDecoder, (String, String)/*R:ClassTag*/
          ](ssc, kafkaPro, fromOffsets, messageHandler)

    } else {
      println("直接创建,没有从ZK中获取偏移量")
      stream = KafkaUtils.createDirectStream
        [String, String, StringDecoder, StringDecoder
          ](ssc, kafkaPro, Set(fromTopic))
    }

    var offsetRanges = Array[OffsetRange]()

    //获取采集的数据的偏移量
    val mapDStream: DStream[String] = stream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2)

    /** ZK偏移量逻辑 以上 */

    /** stream应该根据上一次的Offset来创建 */
    // 连接到了kafka
    //val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPro, Set(fromTopic))

    /** 应该在这里获取上一次的Offset */
    //CTRL+ { }   以下方法在Executor里
    mapDStream.map("ABC:" + _).foreachRDD { rdd =>
      rdd.foreachPartition { items =>
        //写回kafka, 用连接池获取kafka连接
        val genericObjectPoolOfKafkaProxy = KafkaPool(brokers)
        //拿到具体的KafkaProxyFactory里的KafkaProxy
        val kafkaProxy = genericObjectPoolOfKafkaProxy.borrowObject()

        for (item <- items) {
          //使用这个KafkaProxyFactory里的KafkaProxy
          kafkaProxy.kafkaClient.send(new ProducerRecord[String, String](toTopic, item)) //topic ,value(item就是"ABC:" + v=)
        }

        //使用完了放kafkaProxy回连接池
        genericObjectPoolOfKafkaProxy.returnObject(kafkaProxy)
      }

      /** 应该更新Offset */
      val updateGroupTopicDirs = new ZKGroupTopicDirs("kafka", fromTopic)
      val updateZkClient = new ZkClient(zookeeperList)/**这里可优化用线程池*/

      for (offset <- offsetRanges) {
        println(offset)
        /* ls /consumers/kafka/offsets/from1           + /0\1 */
        val zkPath = s"${updateGroupTopicDirs.consumerOffsetDir}/${offset.partition}"
        //更新关键步骤
        ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.fromOffset/*untilOffset*/.toString)
      }
      updateZkClient.close()
      //get /consumers/kafka/offsets/from1/0 可以看到是同步了 offset
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

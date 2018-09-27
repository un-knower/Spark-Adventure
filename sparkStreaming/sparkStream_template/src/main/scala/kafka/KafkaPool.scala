package kafka

import java.util.Properties

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.KafkaProducer

//包装Kafka客户端
case class KafkaProxy(broker: String) {
  /*val prop = new mutable.HashMap[String, Object]() {
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer]
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer]
  }*/ //错的

  var props: Properties = new Properties()
  props.put("bootstrap.servers", broker)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val kafkaClient = new KafkaProducer[String, String](props) //需要Java的Map 转Scala的为Java的
}

//create一个创建KafkaProxy的工厂
class KafkaProxyFactory(broker: String) extends BasePooledObjectFactory[KafkaProxy] {
  // 用于池来创建对象
  override def create(): KafkaProxy = KafkaProxy(broker)

  // 用于池来包装对象
  override def wrap(proxy: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](proxy)
}

// 真正要实现的KafkaPool
object KafkaPool {
  private var kafkaPool: GenericObjectPool[KafkaProxy] = _

  def apply(broker: String): GenericObjectPool[KafkaProxy] = {
    if (kafkaPool == null) {
      KafkaPool.synchronized {
        //如果不设置第二个参数, 默认 是8个kafka连接实例
        this.kafkaPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(broker))
      }
    }

    kafkaPool
  }
}

package kafka

import org.I0Itec.zkclient.ZkClient
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

//包装ZK客户端
case class ZKProxy(broker: String) {

  //获取zookeeper的信息
  val zookeeperList = "hadoop102:2181,hadoop103:2181,hadoop104:2181"

  val zkClient = new ZkClient(broker)
}

//create一个创建ZKProxy的工厂
class ZKProxyFactory(broker: String) extends BasePooledObjectFactory[ZKProxy] {
  // 用于池来创建对象
  override def create(): ZKProxy = ZKProxy(broker)

  // 用于池来包装对象
  override def wrap(proxy: ZKProxy): PooledObject[ZKProxy] = new DefaultPooledObject[ZKProxy](proxy)
}

// 真正要实现的ZKPool
object ZkPool {
  private var zkPool: GenericObjectPool[ZKProxy] = _

  def apply(broker: String): GenericObjectPool[ZKProxy] = {
    if (zkPool == null) {
      KafkaPool.synchronized {
        //如果不设置第二个构造参数, 默认 是最多max 8个zk连接实例
        this.zkPool = new GenericObjectPool[ZKProxy](new ZKProxyFactory(broker))
      }
    }

    zkPool
  }
}

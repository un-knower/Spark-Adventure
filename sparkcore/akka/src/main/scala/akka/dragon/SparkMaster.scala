package akka.dragon

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class SparkMaster extends Actor {
  //定义HashMap集合
  val id2WorkerMap = new collection.mutable.HashMap[String, WorkerInfo]

  //自已给自已周期性发送消息,来检测worker下线状态,更改HashMap集合
  override def preStart(): Unit = {
    //周期性检测超时的worker
    //导入隐式转换
    import context.dispatcher
    //启动定时器，定时执行 对self 每10秒执行一次 RemoveTimeOutWorker
    context.system.scheduler.schedule(0 millis, 5000 millis, self, RemoveTimeOutWorker)
  }

  //接收worker的信息,保存信息,使用可变集合HashMap
  override def receive: Receive = {
    //判断是否存在workerId
    case RegisterWorkerInfo(workerId, core, mem) => {
      if (!id2WorkerMap.contains(workerId)) {
        val workerInfo = WorkerInfo(workerId, core, mem)
        //将worker对象保存到HashMap
        id2WorkerMap += ((workerId, workerInfo))

        //接收信息完成后,通知worker,注册成功
        sender ! RegisterWorkerInfo //此时worker会收到注册成功消息
      }
    }

    case HeartBeat(workerId) => {
      //获取当前的worker信息
      val workerInfo = id2WorkerMap(workerId)
      //获取当前的系统时间
      workerInfo.lastHeartBeatTime = System.currentTimeMillis()
    }

    case RemoveTimeOutWorker => {
      //将hashMap中的value信息拿出来,相看当前时间和上一次心跳时间差
      val workerInfos = id2WorkerMap.values
      val currentTime = System.currentTimeMillis()
      //过滤超时worker
      workerInfos.filter(wkInfo => currentTime - wkInfo.lastHeartBeatTime > 3000)
        //从workerInfos中删除
        .foreach(wk => id2WorkerMap.remove(wk.id))

      println(s"--------还有${id2WorkerMap.size}个worker存活!---------")
    }
  }
}

object SparkMaster {

  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 8888
    val masterName = "Master"

    //聊天通信配置
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider ="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = $host
         |akka.remote.netty.tcp.port = $port
      """.stripMargin)

    //创建ActorSystem
    val sparkMasterActorSystem = ActorSystem("MasterActorSystem", config)

    val masterActorRef = sparkMasterActorSystem.actorOf(Props[SparkMaster], masterName)
  }
}
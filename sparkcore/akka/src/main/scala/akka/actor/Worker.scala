package akka.actor

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class Worker extends Actor{
  //Worker端持有Master端的ActorRef引用（代理对象）
  var master: ActorSelection = _
  //生成一个UUID，作为Worker的标识
  val id = UUID.randomUUID().toString

  //构造方法执行完执行一次
  override def preStart(): Unit = {
    //Worker向MasterActorSystem发送建立连接请求
    //通过masterUrl地址获取ActorRef
    master = context.system.actorSelection("akka.tcp://MasterActorSystem@localhost:8888/user/Master")
    //Worker向Master发送注册消息
    //向master发送自已计算资源  封装消息体
    master ! RegisterWorkerInfo(id, "localhost", "10240", "8")
  }

  //该方法会被反复执行，用于接收消息，通过case class模式匹配接收消息
  override def receive: Receive = {
    //Master向Worker的反馈信息
    case RegisterWorkerInfo=> {
      //启动定时任务，向Master发送心跳
      context.system.scheduler.schedule(0 millis, 5000 millis, self, HeartBeat)
    }

    case HeartBeat => {
      master ! HeartBeat(id)
      println(s"worker $id send heartbeat ")
    }
  }
}

object Worker {
  def main(args: Array[String]) {
    val clientPort = 8887

    //创建WorkerActorSystem的必要参数
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.port = $clientPort
       """.stripMargin
    )
    val actorSystem = ActorSystem("WorkerActorSystem", config)

    //启动Actor，Master会被实例化，生命周期方法会被调用
    val workerActorRef = actorSystem.actorOf(Props[Worker], "Worker")
  }
}

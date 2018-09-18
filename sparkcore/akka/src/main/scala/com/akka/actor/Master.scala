package com.akka.actor

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class Master extends Actor{
  //保存WorkerID和Work信息的map
  val idToWorkerMap = new mutable.HashMap[String, WorkerInfo]
  //保存所有Worker信息的Set
  val workersSet = new mutable.HashSet[WorkerInfo]
  //Worker超时时间
  val WORKER_TIMEOUT: Int = 10 * 1000

  //构造方法执行完执行一次
  override def preStart(): Unit = {
    //启动定时器，定时执行 对self 每10秒执行一次 RemoveTimeOutWorker
    context.system.scheduler.schedule(0 millis, WORKER_TIMEOUT millis, self, RemoveTimeOutWorker)
  }

  //该方法会被反复执行，用于接收消息，通过case class模式匹配接收消息
  override def receive: Receive = {
    //Worker向Master发送的注册消息
    case RegisterWorkerInfo(id, workerHost, memory, cores) => {
      if(!idToWorkerMap.contains(id)) {
        val worker = WorkerInfo(id, workerHost, memory, cores)
        workersSet.add(worker)
        idToWorkerMap(id) = worker
        println("New register worker: "+worker)
        sender ! RegisterWorkerInfo
      }
    }

    //Worker向Master发送的心跳消息
    case HeartBeat(workerId) => {
      val workerInfo = idToWorkerMap(workerId)
      println("get heartbeat message from: "+workerInfo)
      workerInfo.lastHeartbeat = System.currentTimeMillis()
    }

    //Master自己向自己发送的定期检查超时Worker的消息
    case RemoveTimeOutWorker => {
      val currentTime = System.currentTimeMillis()
      val toRemove = workersSet.filter(w => currentTime - w.lastHeartbeat > WORKER_TIMEOUT).toArray
      for(worker <- toRemove){
        workersSet -= worker
        idToWorkerMap.remove(worker.id)
      }
      println("worker size: " + workersSet.size)
    }
  }
}

object Master {

  //程序执行入口
  def main(args: Array[String]) {

    val host = "localhost"
    val port = 8888

    //创建ActorSystem的必要参数
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    )

    //ActorSystem是单例的，用来创建Actor
    val actorSystem = ActorSystem.create("MasterActorSystem", config)

    //启动Actor，Master会被实例化，生命周期方法会被调用
    val masterActorRef = actorSystem.actorOf(Props[Master], "Master")

  }
}


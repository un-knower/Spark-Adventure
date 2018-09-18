package com.akka.dragon

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

class SparkWorker(masterUrl: String) extends Actor {
  //获取到master的ActorRef
  var masterProxy: ActorSelection = _

  //定义workerId
  val workerId: String = UUID.randomUUID().toString

  override def preStart(): Unit = {
    //通过masterUrl地址获取ActorRef
    masterProxy = context.actorSelection(masterUrl)
    //worker向master注册自已
    //向master发送自已计算资源
    // id,core,mem  封装消息体
    masterProxy ! RegisterWorkerInfo(workerId, 4, 32)
  }

  override def receive: Receive = {
    //收到master传来的成功信息,周期的发送心跳
    case RegisterWorkerInfo => {
      //导入隐式转换
      import context.dispatcher
      //封装心跳信息
      context.system.scheduler.schedule(0 millis, 1500 millis, self, HeartBeat)
    }

    case HeartBeat => {
      //开始发送心跳信息
      masterProxy ! HeartBeat(workerId)
      println(s"--------------$workerId 向master发送心跳-----------------")
    }
  }
}

object SparkWorker {

  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 8877
    val workerName = "worker-01"
    val masterUrl = "akka.tcp://MasterActorSystem@localhost:8888/user/Master"

    //聊天通信配置
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider ="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = $host
         |akka.remote.netty.tcp.port = $port
      """.stripMargin)

    //创建ActorSystem
    val sparkWorkerActorSystem = ActorSystem("sparkWorker", config)

    val sparkWorkerActorRef = sparkWorkerActorSystem.actorOf(Props(new SparkWorker(masterUrl)), workerName)
  }
}
package com.akka.rpc

trait RemoteMessage extends Serializable

//Worker -> Master
case class RegisterWorker(id: String, memory: Int, cores: Int) extends RemoteMessage

case class Heartbeat(id: String) extends RemoteMessage

case class RpcWorkerInfo(id: String, memory: Int, cores: Int) {
  //TODO 上一次心跳
  var lastHeartbeatTime : Long = _
}

//Master -> Worker
case class RegisteredWorker(masterUrl: String) extends RemoteMessage

//Worker -> self
case object SendHeartbeat

// Master -> self
case object CheckTimeOutWorker


package com.akka.actor

//worker向master注册自已信息
case class RegisterWorkerInfo(id : String, workerHost : String, memory : String, cores : String)

//Worker详情
case class WorkerInfo(id : String, workerHost : String, memory : String, cores : String) {
  var lastHeartbeat : Long = System.currentTimeMillis()
  override def toString = s"WorkerInfo($id, $workerHost, $memory, $cores)"
}

//定义心跳信息
case class HeartBeat(workId : String)

//检测心跳  //删除timeOutWorker
case class RemoveTimeOutWorker()

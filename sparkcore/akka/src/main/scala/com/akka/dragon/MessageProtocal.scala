package com.akka.dragon

//worker向master注册自已信息
case class RegisterWorkerInfo(id:String,core:Int,mem:Int)

//Worker详情
case class WorkerInfo(id:String,core:Int,mem:Int){
  var lastHeartBeatTime:Long = _
}

//定义心跳信息
case class HeartBeat(id:String)

//删除timeOutWorker
case object RemoveTimeOutWorker
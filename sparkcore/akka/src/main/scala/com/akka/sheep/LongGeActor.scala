package com.akka.sheep

import akka.actor.{Actor, ActorRef}

/**
  * 马龙
  */
class LongGeActor(val fg: ActorRef) extends Actor{
    // 接受消息的
    override def receive: Receive = {
        case "start" => {
            println("龙龙：I'm OK !")
            fg ! "啪"
        }
        case "啪啪" => {
            println("你真猛!")
            Thread.sleep(1000)
            fg ! "啪"
        }
    }
}



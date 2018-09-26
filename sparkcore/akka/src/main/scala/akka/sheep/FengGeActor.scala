package akka.sheep

import akka.actor.Actor

/**
  * 高峰
  */
class FengGeActor extends Actor{

    override def receive: Receive = {
        case "start" => println("峰峰说：I'm OK !")
        case "啪" => {
            println("峰峰：那必须滴！")
            Thread.sleep(1000)
            sender() ! "啪啪"
        }
    }
}

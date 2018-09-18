package com.akka.thread

trait TaskEvent

case class TaskSubmitted(name: String) extends TaskEvent

case class TaskSucceeded(name: String) extends TaskEvent

case class TaskFailed(name: String) extends TaskEvent

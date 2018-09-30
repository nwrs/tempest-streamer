package com.nwrs.streaming.analytics

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.collection.mutable.ListBuffer
import org.scalatest.Assertions._

// Results require a static collection (https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/testing.html)
// Using a list as a map/set would obscure incorrectly duplicated results
object ResultCollector {
  private val r = ListBuffer[JsonResult[_]]()
  val collector: JsonResult[_] => Unit = r+=_
  def size = r.size
  def reset = r.clear

  def checkResult(key:String, expected:Int): Unit = {
    ResultCollector.r.find( _.key == key) match {
      case Some(h) => assert(h.total==expected)
      case None => assert(false,s"No result found for $key")
    }
  }

  def results() = r.toList

  def sinkFunction[T <: JsonResult[_]]():SinkFunction[T] = {
    new SinkFunction[T] {
      override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
        r += value
      }
    }
  }


}
package com.nwrs.streaming.analytics

import java.util.Properties
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

trait PipelineResult[R <:JsonResult[_]] {
  def name():String
  def description():String
  def addToStream(stream:DataStream[Tweet], sinkFunction: SinkFunction[R], windowTime:Time, parallelism:Int)
  def addToStream(stream:DataStream[Tweet], windowTime:Time, parallelism:Int) (implicit props:Properties)
}

object DataStreamImplicits {
  implicit class Implicits(ds: DataStream[Tweet]) {
    def addPipelineResult[R <:JsonResult[_]](pr: PipelineResult[R], sinkFunction: SinkFunction[R], windowTime: Time, parallelism: Int) = pr.addToStream(ds, sinkFunction, windowTime, parallelism)
    def addPipelineResult[R <:JsonResult[_]](pr: PipelineResult[R], windowTime: Time, parallelism: Int) (implicit props:Properties) = pr.addToStream(ds, windowTime, parallelism)
  }
}


package com.nwrs.streaming.analytics

import com.nwrs.streaming.streaming.TweetStreamProps
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream

trait PipelineResult[R <:JsonResult[_]] {
  def name():String
  def description():String
  def addToStream(stream:DataStream[Tweet], sinkFunction: SinkFunction[R], props:TweetStreamProps)
  def addToStream(stream:DataStream[Tweet], props:TweetStreamProps)
}

object DataStreamImplicits {
  implicit class Implicits(ds: DataStream[Tweet]) {
    def addPipelineResult[R <:JsonResult[_]](pr: PipelineResult[R], sinkFunction: SinkFunction[R], props:TweetStreamProps) = pr.addToStream(ds, sinkFunction, props)
    def addPipelineResult[R <:JsonResult[_]](pr: PipelineResult[R], props:TweetStreamProps) = pr.addToStream(ds, props)
  }
}


package com.nwrs.streaming.analytics

import java.util.Properties
import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object TweetCountResult extends PipelineResult[CustomCount] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[CustomCount], windowTime:Time, parallelism:Int): Unit = {
    stream
      .map( t => CustomCount("tweetType", t.tweetType, t.date, 1))
      .keyBy(_.key)
      .timeWindow(windowTime)
      .reduce(_ + _)
      .addSink(sinkFunction)
      .setParallelism(parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], windowTime:Time, parallelism:Int)(implicit props:Properties): Unit = {
    addToStream(stream, ElasticUtils.createSink[CustomCount]("count-idx","count-timeline", props), windowTime, parallelism)
  }

  override def name(): String = "TweetCount"
  override def description(): String = "Windowed count of tweet by type"
}
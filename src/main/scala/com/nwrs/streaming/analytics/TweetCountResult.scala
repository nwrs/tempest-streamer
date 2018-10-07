package com.nwrs.streaming.analytics

import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.streaming.TweetStreamProps
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

object TweetCountResult extends PipelineResult[CustomCount] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[CustomCount], props:TweetStreamProps): Unit = {
    stream
      .map( t => CustomCount("tweetType", t.tweetType, t.date, 1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce(_ + _)
      .addSink(sinkFunction)
      .setParallelism(props.parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], props:TweetStreamProps): Unit = {
    addToStream(stream, ElasticUtils.createSink[CustomCount]("count-idx","count-timeline", props.elasticUrl), props)
  }

  override def name(): String = "TweetCount"
  override def description(): String = "Windowed count of tweet by type"
}
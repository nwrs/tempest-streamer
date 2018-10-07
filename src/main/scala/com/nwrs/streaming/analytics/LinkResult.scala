package com.nwrs.streaming.analytics

import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.streaming.TweetStreamProps
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}

object LinkResult extends PipelineResult[CustomCount] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[CustomCount], props:TweetStreamProps): Unit = {
    stream
      .filter(_.hasLinks)
      .flatMap(t => t.links.split(" ").map(l => CustomCount("linkUrl", l, t.date, 1)))
      .filter(!_.key.toLowerCase.contains("twitter.com/"))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce( _ + _)
      .filter(_.total > 1)
      .addSink(sinkFunction)
      .setParallelism(props.parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], props:TweetStreamProps): Unit = {
    addToStream(stream, ElasticUtils.createSink[CustomCount]("links-idx","links-timeline", props.elasticUrl), props)
  }
  override def name(): String = "Links"
  override def description(): String = "Windowed count of tweeted weblinks"
}


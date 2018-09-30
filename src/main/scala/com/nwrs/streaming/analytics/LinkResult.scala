package com.nwrs.streaming.analytics

import java.util.Properties
import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time

object LinkResult extends PipelineResult[CustomCount] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[CustomCount], windowTime:Time, parallelism:Int): Unit = {
    stream
      .filter(_.hasLinks)
      .flatMap(t => t.links.split(" ").map(l => CustomCount("linkUrl", l, t.date, 1)))
      .filter(!_.key.toLowerCase.contains("twitter.com/"))
      .keyBy(_.key)
      .timeWindow(windowTime)
      .reduce( _ + _)
      .filter(_.total > 1)
      .addSink(sinkFunction)
      .setParallelism(parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], windowTime:Time, parallelism:Int)(implicit props:Properties): Unit = {
    addToStream(stream, ElasticUtils.createSink[CustomCount]("links-idx","links-timeline", props), windowTime, parallelism)
  }
  override def name(): String = "Links"
  override def description(): String = "Windowed count of tweeted weblinks"
}


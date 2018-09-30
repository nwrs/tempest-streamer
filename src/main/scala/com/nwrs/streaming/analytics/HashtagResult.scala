package com.nwrs.streaming.analytics

import java.util.Properties
import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object HashtagResult extends PipelineResult[CustomCount] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[CustomCount], windowTime:Time, parallelism:Int): Unit = {
    stream.filter(_.hasHashtags)
      .flatMap(t => t.hashtags.map(h => CustomCount("hashtag", h, t.date, 1)))
      .keyBy(h => h.word.toLowerCase)
      .timeWindow(windowTime)
      .reduce( _ + _)
      .filter(_.count > 1)
      .addSink(sinkFunction)
      .setParallelism(parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], windowTime:Time, parallelism:Int) (implicit props:Properties): Unit = {
    addToStream(stream, ElasticUtils.createSink[CustomCount]("hashtags-idx","hashtags-timeline", props), windowTime, parallelism)
  }

  override def name(): String = "Hashtags"
  override def description(): String = "Windowed count of hashtags"
}


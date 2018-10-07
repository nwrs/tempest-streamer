package com.nwrs.streaming.analytics

import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.streaming.TweetStreamProps
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

object HashtagResult extends PipelineResult[CustomCount] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[CustomCount], props:TweetStreamProps): Unit = {
    stream.filter(_.hasHashtags)
      .flatMap(t => t.hashtags.map(h => CustomCount("hashtag", h, t.date, 1)))
      .keyBy(h => h.word.toLowerCase)
      .timeWindow(props.windowTime)
      .reduce( _ + _)
      .filter(_.count > 1)
      .addSink(sinkFunction)
      .setParallelism(props.parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], props:TweetStreamProps): Unit = {
    addToStream(stream, ElasticUtils.createSink[CustomCount]("hashtags-idx","hashtags-timeline", props.elasticUrl), props)
  }

  override def name(): String = "Hashtags"
  override def description(): String = "Windowed count of hashtags"
}


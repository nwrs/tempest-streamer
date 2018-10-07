package com.nwrs.streaming.analytics

import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.streaming.TweetStreamProps
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}

object GenderResult extends PipelineResult[CustomCount] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[CustomCount], props:TweetStreamProps): Unit = {
    stream
      .filter(_.gender.nonEmpty)
      .map( t => CustomCount("gender",t.gender,t.date,1))
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce( _ + _)
      .addSink(sinkFunction)
      .setParallelism(props.parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], props:TweetStreamProps): Unit = {
    addToStream(stream, ElasticUtils.createSink[CustomCount]("gender-idx","gender-timeline", props.elasticUrl), props)
  }

  override def name(): String = "Gender"
  override def description(): String = "Windowed breakdown of user gender"
}


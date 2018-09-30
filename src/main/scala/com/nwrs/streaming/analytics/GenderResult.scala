package com.nwrs.streaming.analytics

import java.util.Properties

import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time

object GenderResult extends PipelineResult[CustomCount] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[CustomCount], windowTime:Time, parallelism:Int): Unit = {
    stream
      .filter(_.gender.nonEmpty)
      .map( t => CustomCount("gender",t.gender,t.date,1))
      .keyBy(_.key)
      .timeWindow(windowTime)
      .reduce( _ + _)
      .addSink(sinkFunction)
      .setParallelism(parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], windowTime:Time, parallelism:Int) (implicit props:Properties): Unit = {
    addToStream(stream, ElasticUtils.createSink[CustomCount]("gender-idx","gender-timeline", props), windowTime, parallelism)
  }

  override def name(): String = "Gender"
  override def description(): String = "Windowed breakdown of user gender"
}


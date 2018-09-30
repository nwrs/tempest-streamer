package com.nwrs.streaming.analytics

import java.util.Properties
import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.parsing.Splitters
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object ProfileBigramsResult extends PipelineResult[CustomCount] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[CustomCount], windowTime:Time, parallelism:Int): Unit = {
    stream
      .filter(t => !t.verified && t.profileText.length > 1)
      .keyBy(_.userId)
      .timeWindow(windowTime)
      .reduce((_,t2) => t2)
      .flatMap(t => Splitters.toBigramsProfile(t.profileText).map(h => CustomCount("word", h, t.date, 1)))
      .keyBy(_.key)
      .timeWindow(windowTime)
      .reduce(_ + _)
      .filter(_.total > 1)
      .addSink(sinkFunction)
      .setParallelism(parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], windowTime:Time, parallelism:Int)(implicit props:Properties): Unit = {
    addToStream(stream, ElasticUtils.createSink[CustomCount]("profile-bigrams-idx","profile-bigrams-timeline", props), windowTime, parallelism)
  }
  override def name(): String = "ProfileBigrams"
  override def description(): String = "Windowed count of user profile bigrams"
}


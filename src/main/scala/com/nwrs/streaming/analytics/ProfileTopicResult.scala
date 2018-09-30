package com.nwrs.streaming.analytics

import java.util.Properties
import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.parsing.Splitters
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

case class ProfileTopic(word:String, timestamp: Long, count:Int) extends JsonResult[ProfileTopic] {
  override def +(other:ProfileTopic):ProfileTopic = this.copy(count = count+other.count)
  override def toJson():String = {
        s"""{
       |    "cnt": ${count},
       |    "word": ${JsonResult.cleanAndQuote(word)},
       |    "isHashtag": ${word.startsWith("#").toString},
       |    "timestamp": ${timestamp}
       |}
      """.stripMargin
  }
  override def total() = count
  override val key = word
}

object ProfileTopicResult extends PipelineResult[ProfileTopic] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[ProfileTopic], windowTime:Time, parallelism:Int): Unit = {
    stream
      .filter(t => !t.verified && t.profileText.length > 0)
      .flatMap(t => Splitters.toNormalisedWordsExCommonProfile(t.profileText).map(h => ProfileTopic(h, t.date, 1)))
      .keyBy("word")
      .timeWindow(windowTime)
      .reduce( _ + _)
      .filter(_.count > 1)
      .addSink(sinkFunction)
      .setParallelism(parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], windowTime:Time, parallelism:Int)(implicit props:Properties): Unit = {
    addToStream(stream, ElasticUtils.createSink[ProfileTopic]("profile-topics-idx","profile-topics-timeline", props), windowTime, parallelism:Int)
  }

  override def name(): String = "ProfileTopics"
  override def description(): String = "Windowed count of user profile topics"
}
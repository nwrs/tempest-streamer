package com.nwrs.streaming.analytics

import java.util.Properties
import java.util.concurrent.TimeUnit
import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.streaming.TopNReducer
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time

case class RetweetResultJson(
  tweetId:Long,
  numRetweets:Long,
  screenName:String,
  accountName:String,
  imgUrl:String,
  tweetText:String,
  tweetLink:String,
  timestamp:Long)
  extends JsonResult[RetweetResultJson] {
  override def +(other: RetweetResultJson): RetweetResultJson = this // Reduce just removes duplicates
  override def key() = tweetId.toString
  override def total() = numRetweets.toInt
}

object RetweetResult extends PipelineResult[RetweetResultJson] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[RetweetResultJson], windowTime:Time, parallelism:Int): Unit = {
    stream
      .filter(_.retweet)
      .filter( _.date > System.currentTimeMillis()-TimeUnit.DAYS.toMillis(5)) // nothing older than 5 days
      .map(t => RetweetResultJson(t.retweetId,
        t.retweetNumRetweets,
        t.retweetScreenName,
        t.retweetAccountName,
        t.retweetImgUrl,
        t.text,
        s"https://twitter.com/${t.retweetScreenName}/status/${t.retweetId}",
        t.date))
      .keyBy(_.key)
      .reduce( _ + _)
      .timeWindowAll(windowTime)
      .process(new TopNReducer[Long, RetweetResultJson](e => e.numRetweets, 10))
      .addSink(sinkFunction)
      .setParallelism(parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], windowTime:Time, parallelism:Int)(implicit props:Properties): Unit = {
    addToStream(stream, ElasticUtils.createSink[RetweetResultJson]("retweets-idx","retweets-timeline", props), windowTime, parallelism)
  }
  override def name(): String = "Retweets"
  override def description(): String = "Windowed count of top retweets"
}


package com.nwrs.streaming.analytics

import java.util.Properties
import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.streaming.TopNReducer
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

case class SimpleUser(numFollowers:Long, screenName:String, accountName:String, imgUrl:String, timestamp:Long) extends JsonResult[SimpleUser] {
  override def +(other:SimpleUser):SimpleUser = this   //  reduce simply removes duplicate users
  override val key = screenName
  override def total() = numFollowers.toInt
}

object InfluencersResult extends PipelineResult[SimpleUser] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[SimpleUser], windowTime:Time, parallelism:Int): Unit = {
    stream
      .map( t => {
        if (t.retweet)
          SimpleUser(t.retweetFollowers, t.retweetScreenName, t.retweetAccountName, t.retweetImgUrl, System.currentTimeMillis())
        else
          SimpleUser(t.followers, t.screenName, t.accountName, t.imgUrl, System.currentTimeMillis())
      })
      .keyBy(_.key)
      .reduce(_ + _)
      .timeWindowAll(windowTime)
      .process( new TopNReducer[Long, SimpleUser](e => e.numFollowers, 10))
      .addSink(sinkFunction)
      .setParallelism(parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], windowTime:Time, parallelism:Int)(implicit props:Properties): Unit = {
    addToStream(stream, ElasticUtils.createSink[SimpleUser]("influencers-idx","influencers-timeline", props), windowTime, parallelism)
  }

  override def name(): String = "Influencers"
  override def description(): String = "Windowed top 10 users by followers"
}
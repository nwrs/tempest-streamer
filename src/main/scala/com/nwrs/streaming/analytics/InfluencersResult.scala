package com.nwrs.streaming.analytics

import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.streaming.TweetStreamProps
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

case class SimpleUser(numFollowers:Long, screenName:String, accountName:String, imgUrl:String, timestamp:Long) extends JsonResult[SimpleUser] {
  override def +(other:SimpleUser):SimpleUser = this   //  reduce simply removes duplicate users
  override val key = screenName
  override def total() = numFollowers.toInt
}

object InfluencersResult extends PipelineResult[SimpleUser] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[SimpleUser], props:TweetStreamProps): Unit = {
    stream
      .map( t => {
        if (t.retweet)
          SimpleUser(t.retweetFollowers, t.retweetScreenName, t.retweetAccountName, t.retweetImgUrl, System.currentTimeMillis())
        else
          SimpleUser(t.followers, t.screenName, t.accountName, t.imgUrl, System.currentTimeMillis())
      })
      .keyBy(_.key)
      .reduce(_ + _)
      .timeWindowAll(props.windowTime)
      .process( new TopNReducer[Long, SimpleUser](e => e.numFollowers, 10))
      .addSink(sinkFunction)
      .setParallelism(props.parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], props:TweetStreamProps): Unit = {
    addToStream(stream, ElasticUtils.createSink[SimpleUser]("influencers-idx","influencers-timeline", props.elasticUrl), props)
  }

  override def name(): String = "Influencers"
  override def description(): String = "Windowed top 10 users by followers"
}
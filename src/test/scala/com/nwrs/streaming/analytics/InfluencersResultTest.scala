package com.nwrs.streaming.analytics

import com.nwrs.streaming.analytics.DataStreamImplicits._
import com.nwrs.streaming.streaming.TweetStreamProps
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InfluencersResultTest extends FunSuite  {

  val tweets = Seq(
    Tweet(screenName = "ANOther_11", text="One more tweet 1", followers = 11000),
    Tweet(screenName = "ANOther_12",text="One more tweet 1", followers = 12000),
    Tweet(screenName = "ANOther_13",text="One more tweet 1", followers = 13000),
    Tweet(screenName = "ANOther_14",text="One more tweet 1", followers = 14000),
    Tweet(screenName = "ANOther_1",text="This is a tweet", followers = 1000),
    Tweet(screenName = "ANOther_2",text="Another tweet", followers = 2000),
    Tweet(screenName = "ANOther_3",text="Yet another tweet", followers = 3000),
    Tweet(screenName = "ANOther_4",text="One more tweet", followers = 4000),
    Tweet(screenName = "ANOther_7",text="One more tweet 1", followers = 7000),
    Tweet(screenName = "ANOther_5",text="Even more tweets", followers = 5000),
    Tweet(screenName = "ANOther_6",text="One more tweet 1", followers = 6000),
    Tweet(screenName = "ANOther_9",text="One more tweet 1", followers = 9000),
    Tweet(screenName = "ANOther_8",text="One more tweet 1", followers = 8000),
    Tweet(screenName = "ANOther_10",text="One more tweet 1", followers = 10000),
    Tweet(screenName = "ANOther_10",text="One more tweet 2", followers = 10000),
    Tweet(screenName = "ANOther_10",text="One more tweet 3", followers = 10000),
    Tweet(screenName = "ANOther_10",text="One more tweet 4", followers = 10000)
  )

  test("Influencers test") {
    ResultCollector.reset
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val stream = env.fromCollection(tweets)
    stream.addPipelineResult(InfluencersResult, ResultCollector.sinkFunction[SimpleUser](), TweetStreamProps(windowTime=Time.seconds(1), parallelism = 1))
    env.execute("InfluencersResultTest test")

    // We are expecting to get the top 10 by number of followers counting each user only once
    assert(ResultCollector.size==10)
    ResultCollector.checkResult("ANOther_5",5000)
    ResultCollector.checkResult("ANOther_6",6000)
    ResultCollector.checkResult("ANOther_7",7000)
    ResultCollector.checkResult("ANOther_8",8000)
    ResultCollector.checkResult("ANOther_9",9000)
    ResultCollector.checkResult("ANOther_10",10000)
    ResultCollector.checkResult("ANOther_11",11000)
    ResultCollector.checkResult("ANOther_12",12000)
    ResultCollector.checkResult("ANOther_13",13000)
    ResultCollector.checkResult("ANOther_14",14000)
  }


}

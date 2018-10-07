package com.nwrs.streaming.analytics

import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.nwrs.streaming.analytics.DataStreamImplicits._
import com.nwrs.streaming.streaming.TweetStreamProps

@RunWith(classOf[JUnitRunner])
class HashtagResultTest extends FunSuite  {

  test("HashtagResultTest multiple hashtags") {
    ResultCollector.reset
    val tweets = Seq(
      Tweet(screenName = "user1", text="1 this is a tweet #hashtag1 #hashtag2", hasHashtags = true, hashtags = Array("hashtag1","hashtag2")),
      Tweet(screenName = "user2", text="2 this is a tweet #hashtag1", hasHashtags = true, hashtags = Array("hashtag1")),
      Tweet(screenName = "user3", text="3 this is a tweet #hashtag1 #hashtag2", hasHashtags = true, hashtags = Array("hashtag1","hashtag2")),
      Tweet(screenName = "user4", text="4 this is a tweet #hashtag1", hasHashtags = true, hashtags = Array("hashtag1")),
      Tweet(screenName = "user5", text="5 this is a tweet #hashtag1", hasHashtags = true, hashtags = Array("hashtag1")),
      Tweet(screenName = "user6", text="6 this is a tweet #hashtag1", hasHashtags = true, hashtags = Array("hashtag1"))
    )

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val stream = env.fromCollection(tweets)
    stream.addPipelineResult(HashtagResult, ResultCollector.sinkFunction[CustomCount](), TweetStreamProps(windowTime=Time.seconds(1), parallelism = 1))
    env.execute("HashtagResultTest 2 hashtags")

    assert(ResultCollector.size==2)
    ResultCollector.checkResult("hashtag1",6)
    ResultCollector.checkResult("hashtag2",2)
  }

  test("HashtagResultTest no hashtags") {
    ResultCollector.reset
    val tweets = Seq(
      Tweet(screenName="user1",text="1 this is a tweet with no hashtags"),
      Tweet(screenName="user2",text="no hashtags, boolean incorrectly set"),
      Tweet(screenName="user2",text="")
    )

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val stream = env.fromCollection(tweets)
    stream.addPipelineResult(HashtagResult, ResultCollector.sinkFunction[CustomCount](), TweetStreamProps(windowTime=Time.seconds(1), parallelism = 1))
    env.execute("HashtagResultTest no hashtags")
    assert(ResultCollector.size==0)
  }

}

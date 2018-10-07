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
class ProfileResultsTest extends FunSuite  {

  val tweets = Seq(
    Tweet(screenName="ArtVandelay",text="This is a boring tweet", profileText="Marine biologist, loves cheese", userId=111),
    Tweet(screenName="KelVarnsen",text="Another boring tweet", profileText="Father, loves Cheese", userId=222),
    Tweet(screenName="DrMartinVanNostrand ",text="Yet another boring tweet", profileText="Doctor, loves cheese", userId=333),
    Tweet(screenName="HEPennypacker",text="One more boring tweet", profileText="Wealthy industrialist, hates cheese, dislikes sport", userId=444),
    Tweet(screenName="ANOther",text="Even more boring tweets", profileText="Loves Sports", userId=555),
    Tweet(screenName="ANOther1",text="One more boring tweet 1", profileText="Loves cheese, dislikes sport", userId=666))

  test("ProfileTopicResult test") {
    ResultCollector.reset
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val stream = env.fromCollection(tweets)
    stream.addPipelineResult(ProfileTopicResult, ResultCollector.sinkFunction[ProfileTopic], TweetStreamProps(windowTime=Time.seconds(2), parallelism = 1))
    env.execute("ProfileTopicResult test")

    println(ResultCollector.results)
    assert(ResultCollector.size==3)
    ResultCollector.checkResult("cheese",5)
    ResultCollector.checkResult("sport",2)
    ResultCollector.checkResult("dislikes",2)
  }

  test("ProfileBigramsResult test") {
    ResultCollector.reset
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val stream = env.fromCollection(tweets)
    stream.addPipelineResult(ProfileBigramsResult, ResultCollector.sinkFunction[CustomCount], TweetStreamProps(windowTime=Time.seconds(2), parallelism = 1))
    env.execute("ProfileBigramsResult test")

    assert(ResultCollector.size==2)
    ResultCollector.checkResult("loves cheese",4)
    ResultCollector.checkResult("dislikes sport",2)
  }


}

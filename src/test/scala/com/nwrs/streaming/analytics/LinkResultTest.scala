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
class LinkResultTest extends FunSuite  {

  test("LinkResultTest") {
    ResultCollector.reset
    val tweets = Seq(
      Tweet(screenName="user1", text="1 this is a tweet https://en.wikipedia.org/wiki/Twitter", hasLinks = true, links="https://en.wikipedia.org/wiki/Twitter"),
      Tweet(screenName="user2",text="1 this is a tweet https://en.wikipedia.org/wiki/Twitter", hasLinks = true, links="https://en.wikipedia.org/wiki/Twitter"),
      Tweet(screenName="user3",text="1 this is a tweet https://en.wikipedia.org/wiki/Twitter", hasLinks = true, links="https://en.wikipedia.org/wiki/Twitter"),
      Tweet(screenName="user3",text="1 this is a tweet https://en.wikipedia.org/wiki/Twitter https://en.wikipedia.org/wiki/Apache_Flink",
            hasLinks = true, links="https://en.wikipedia.org/wiki/Twitter https://en.wikipedia.org/wiki/Apache_Flink"),
      Tweet(screenName="user4",text="1 this is a tweet https://en.wikipedia.org/wiki/Apache_Flink",hasLinks = true, links="https://en.wikipedia.org/wiki/Apache_Flink"),
      Tweet(screenName="user5",text="This is a tweet", hasLinks = false),
      Tweet(screenName="user6",text="This is a tweet", hasLinks = false)
    )

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val stream = env.fromCollection(tweets)
    stream.addPipelineResult(LinkResult, ResultCollector.sinkFunction[CustomCount], TweetStreamProps(windowTime=Time.seconds(1), parallelism = 1))
    env.execute("LinkResultTest")

    Thread.sleep(1000) // doesn't seem to be a way to ensure results are ready, a sleep is really not ideal here!
    assert(ResultCollector.size==2)
    ResultCollector.checkResult("https://en.wikipedia.org/wiki/Twitter",4)
    ResultCollector.checkResult("https://en.wikipedia.org/wiki/Apache_Flink",2)
  }


}

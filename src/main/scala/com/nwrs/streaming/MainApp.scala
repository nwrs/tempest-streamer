package com.nwrs.streaming

import com.nwrs.streaming.analytics._
import org.apache.flink.streaming.api.scala._
import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.streaming.{StreamFactory, TweetStreamProps}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import com.nwrs.streaming.analytics.DataStreamImplicits._
import org.slf4j.{Logger, LoggerFactory}
import scala.io.Source

object MainApp {

  lazy val log:Logger = LoggerFactory.getLogger(MainApp.getClass)

  def main(args : Array[String]): Unit = {
    log.info("Starting Tempest Tweet Streamer...")

    // TODO - Complete rest service
    // Start REST service
    // Future { RestService.startAndAwait(9898) } (ExecutionContext.global)

    // load config props
    val propsOpt = TweetStreamProps.getProps(args)
    if (propsOpt.isEmpty) {
      Source.fromInputStream (getClass.getClassLoader.getResourceAsStream ("options.txt") ).getLines ().foreach (println)
      System.exit(0)
    }
    val props = propsOpt.get

    // confirm Elasticsearch present, creating indexes if required
    new ElasticUtils(props.elasticUrl).ensureElasticSearchCluster()

    // create execution env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,5000))

    // create stream
    val stream = StreamFactory.createStream(env, props)

    // add results to stream
    Seq(HashtagResult,
      TweetCountResult,
      ProfileTopicResult,
      ProfileBigramsResult,
      InfluencersResult,
      GenderResult,
      SourceResult,
      GeoLocationResult,
      RetweetResult,
      LinkResult).foreach(r => {
      log.info(s"Adding result to stream: ${r.name()} - ${r.description()}")
      stream.addPipelineResult(r, props)
    })

    env.execute("Tempest Twitter Streaming")

  }

}

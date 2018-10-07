package com.nwrs.streaming.analytics

import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.streaming.TweetStreamProps
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}

case class SourceResult(source:String, sourceUrl:String, timestamp:Long, cnt:Int) extends JsonResult[SourceResult] {
  override def +(other: SourceResult): SourceResult = this.copy(cnt = other.cnt + cnt)
  override def key() = source
  override def total() = cnt
}

object SourceResult extends PipelineResult[SourceResult] {
  val sourceRegex = """<a href="([^"]+)"([^>]*)>(.+?)</a>""".r

  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[SourceResult], props:TweetStreamProps): Unit = {
    stream
      .filter(_.source.length > 0)
      .map(t => t.source match {
          case sourceRegex(link,_,app) => Some(SourceResult(app,link,t.date,1))
          case _ => None
        }
      )
      .filter(_.nonEmpty)
      .map(_.get)
      .keyBy(_.key)
      .timeWindow(props.windowTime)
      .reduce( _ + _)
      .addSink(sinkFunction)
      .setParallelism(props.parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], props:TweetStreamProps): Unit = {
    addToStream(stream, ElasticUtils.createSink[SourceResult]("source-idx","source-timeline", props.elasticUrl), props)
  }

  override def name(): String = "Source"
  override def description(): String = "Windowed breakdown of tweet app/device/platform source"
}


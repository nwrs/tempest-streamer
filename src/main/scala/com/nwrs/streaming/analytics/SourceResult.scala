package com.nwrs.streaming.analytics

import java.util.Properties
import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time

case class SourceResult(source:String, sourceUrl:String, timestamp:Long, cnt:Int) extends JsonResult[SourceResult] {
  override def +(other: SourceResult): SourceResult = this.copy(cnt = other.cnt + cnt)
  override def key() = source
  override def total() = cnt
}

object SourceResult extends PipelineResult[SourceResult] {
  val sourceRegex = """<a href="([^"]+)"([^>]*)>(.+?)</a>""".r

  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[SourceResult], windowTime:Time, parallelism:Int): Unit = {
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
      .timeWindow(windowTime)
      .reduce( _ + _)
      .addSink(sinkFunction)
      .setParallelism(parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], windowTime:Time, parallelism:Int) (implicit props:Properties): Unit = {
    addToStream(stream, ElasticUtils.createSink[SourceResult]("source-idx","source-timeline", props), windowTime, parallelism)
  }

  override def name(): String = "Source"
  override def description(): String = "Windowed breakdown of tweet app/device/platform source"
}


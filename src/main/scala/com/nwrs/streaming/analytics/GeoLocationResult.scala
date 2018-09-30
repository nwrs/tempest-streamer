package com.nwrs.streaming.analytics

import java.util.Properties

import com.nwrs.streaming.elastic.ElasticUtils
import com.nwrs.streaming.twitter.Tweet
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

case class GeoLocationJson(location:String,
    town: String,
    area: String,
    region: String,
    country: String,
    locationGeo:String,
    accuracy: Int,
    iso31662: String,
    timestamp:Long,
    count:Int) extends JsonResult[GeoLocationJson] {
  override def +(other:GeoLocationJson):GeoLocationJson = copy(count =  count+other.count)
  override val key = location
  override def total() = count
}

object GeoLocationResult extends PipelineResult[GeoLocationJson] {
  override def addToStream(stream: DataStream[Tweet], sinkFunction: SinkFunction[GeoLocationJson], windowTime:Time, parallelism:Int): Unit = {
    stream
      .filter( t => t.locationAccuracy > 0)
      .map( t => GeoLocationJson(t.resolvedProfileLocation,
            t.profileTown,
            t.profileArea,
            t.profileRegion,
            t.profileCountry,
            t.locationGeo,
            t.locationAccuracy,
            t.iso31662,
            t.date, 1))
      .keyBy(_.key)
      .timeWindow(windowTime)
      .reduce( _ + _)
      .addSink(sinkFunction)
      .setParallelism(parallelism)
      .name(name)
  }

  override def addToStream(stream: DataStream[Tweet], windowTime:Time, parallelism:Int) (implicit props:Properties): Unit = {
    addToStream(stream, ElasticUtils.createSink[GeoLocationJson]("geo-idx", "geo-timeline", props), windowTime, parallelism)
  }

  override def name(): String = "GeoLocation"
  override def description(): String = "Windowed geo locations parsed from user profile location text"
}




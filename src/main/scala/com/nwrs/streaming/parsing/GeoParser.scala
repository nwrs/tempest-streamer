package com.nwrs.streaming.parsing

import scala.io.Source

case class GeoLocation(
  town: String,
  area: String,
  region: String,
  country: String,
  latitude: Double,
  longitude: Double,
  accuracy: Int,
  iso31662: String) {
  val location = Seq(town, area, region, country).filter( s => s!=null && !s.isEmpty).mkString(", ")
  val locationGeo = s"${latitude.toString},${longitude.toString}"
}

class GeoParserCsv(file:String, hasIso:Boolean=false) extends Serializable {

  private object GeoCsvOffsets extends Enumeration {
    type COL_OFFSETS = Value
    val KEY, TOWN, AREA, REGION, COUNTRY, ACCURACY, LATITUDE ,LONGITUDE ,ISO_31662 = Value
  }

  val map = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(file))
    .getLines()
    .flatMap( l => {
      val items = l.split(",")
      val keys = items(GeoCsvOffsets.KEY.id).split("\\|")
      keys.map( k => (k.toLowerCase,GeoLocation(items(GeoCsvOffsets.TOWN.id),
        items(GeoCsvOffsets.AREA.id),
        items(GeoCsvOffsets.REGION.id),
        items(GeoCsvOffsets.COUNTRY.id),
        items(GeoCsvOffsets.LATITUDE.id).toDouble,
        items(GeoCsvOffsets.LONGITUDE.id).toDouble,
        items(GeoCsvOffsets.ACCURACY.id).toInt,
        if (hasIso) items(GeoCsvOffsets.ISO_31662.id) else "")))
    }).toMap

  def parse(location: String): Option[GeoLocation] = {
    if (location ==null || location.isEmpty)
      None
    else
      location.toLowerCase.split(",").map( s => map.get(s.trim)).find( _.nonEmpty).getOrElse(None)
  }
}

object GeoParser {
  val parsers = Seq(new GeoParserCsv("geo/uk-geo.csv",true), new GeoParserCsv("geo/us-geo.csv"))
  def parse(loc:String):Option[GeoLocation] =  parsers.map(p => p.parse(loc)).find(_.nonEmpty).getOrElse(None)
}
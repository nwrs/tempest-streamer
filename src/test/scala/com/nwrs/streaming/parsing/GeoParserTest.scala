package com.nwrs.streaming.parsing

import org.junit.runner.RunWith
import org.scalatest.{FlatSpec}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoParserTest extends FlatSpec  {

  behavior of "A GeoParser"

  it should "match exact UK town names" in {
    val l = GeoParser.parse("Oxford, UK")
    assert(l.nonEmpty)
    assert(l.get.town == "Oxford")
    assert(l.get.area == "Oxfordshire")
    assert(l.get.region == "England")
    assert(l.get.country == "UK")
    assert(l.get.iso31662 == "GB-OXF")
    assert(l.get.accuracy==4)
  }

  it should "match an area with an unknown town" in {
    val l = GeoParser.parse("unknown town, oxfordshire")
    assert(l.nonEmpty)
    assert(l.get.town == "")
    assert(l.get.area == "Oxfordshire")
    assert(l.get.region == "England")
    assert(l.get.country == "UK")
    assert(l.get.iso31662 == "GB-OXF")
  }

  it should "match a region with an unknown town" in {
    val l = GeoParser.parse("unknown town, scotland")
    assert(l.nonEmpty)
    assert(l.get.town == "")
    assert(l.get.area == "")
    assert(l.get.region == "Scotland")
    assert(l.get.country == "UK")
    assert(l.get.iso31662 == "GB-SCT")
    assert(l.get.accuracy==2)
  }

  it should "match a country with unknown area and town" in {
    val l = GeoParser.parse("unknown town, randomshire, uk")
    assert(l.nonEmpty)
    assert(l.get.town == "")
    assert(l.get.area == "")
    assert(l.get.region == "")
    assert(l.get.country == "UK")
    assert(l.get.iso31662 == "GB-UKM")
    assert(l.get.accuracy==1)
  }

  it should "correctly match exact US town names" in {
    val l = GeoParser.parse("houston")
    assert(l.nonEmpty)
    assert(l.get.town == "Houston")
    assert(l.get.area == "Texas")
    assert(l.get.region == "")
    assert(l.get.country == "US")
    assert(l.get.iso31662 == "")
    assert(l.get.accuracy==4)
  }

  it should "correctly match US states with no town" in {
    val l = GeoParser.parse("texas")
    assert(l.nonEmpty)
    assert(l.get.town == "")
    assert(l.get.area == "Texas")
    assert(l.get.region == "")
    assert(l.get.country == "US")
    assert(l.get.iso31662 == "")

    val l1 = GeoParser.parse("TX")
    assert(l1.nonEmpty)
    assert(l1.get.town == "")
    assert(l1.get.area == "Texas")
    assert(l1.get.region == "")
    assert(l1.get.country == "US")
    assert(l1.get.iso31662 == "")
  }

  it should "return none when unmatched" in {
    val l = GeoParser.parse("random place, unknown area, france")
    assert(!l.nonEmpty)
  }

}

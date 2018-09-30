package com.nwrs.streaming.parsing

import org.junit.runner.RunWith
import org.scalatest.{FlatSpec}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GenderParsingTest extends FlatSpec  {

  behavior of "A GenderParser"

  it should "not contain overlaps between genders" in {
    val intersect = GenderParser.genderMap.filter( v => v._2==Gender.Male).values.toSet.intersect(GenderParser.genderMap.filter( v => v._2==Gender.Female).values.toSet)
    assert(intersect.size==0)
  }

  it should "correctly match genders" in {
    val n = GenderParser.parseGender("nick")
    assert(n.nonEmpty)
    assert(n.get==Gender.Male)
    val a = GenderParser.parseGender("astrid")
    assert(a.nonEmpty)
    assert(a.get==Gender.Female)
  }

  it should "return none when name unknown" in {
    assert(GenderParser.parseGender("nothing")==None)
  }


}

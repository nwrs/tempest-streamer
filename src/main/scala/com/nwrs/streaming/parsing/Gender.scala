package com.nwrs.streaming.parsing


import com.nwrs.streaming.parsing.Gender.Gender

import scala.io.Source

object Gender extends Enumeration {
  type Gender = Value
  val Male, Female = Value
}

object GenderParser {
  val genderMap = Map(Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("word-lists/male.txt")).getLines().map(l => (l.toLowerCase,Gender.Male)).toSeq ++
                      Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("word-lists/female.txt")).getLines().map(l => (l.toLowerCase,Gender.Female)).toSeq :_*)

  def parseGender(name:String):Option[Gender] = genderMap.get(name.toLowerCase)
  def parseGenderFromFullName(name:String):Option[Gender] = genderMap.get(name.toLowerCase.split(" ")(0))

}

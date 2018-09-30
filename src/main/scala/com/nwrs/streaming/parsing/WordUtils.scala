package com.nwrs.streaming.parsing

import scala.io.Source

object WordUtils {

  val commonWords = Source.fromInputStream(Thread.currentThread.getContextClassLoader.getResourceAsStream("word-lists/common-wd-en.txt")).getLines().toSet
  val commonWordsProfile = commonWords ++ Source.fromInputStream(Thread.currentThread.getContextClassLoader.getResourceAsStream("word-lists/common-wd-twitter-profile-en.txt")).getLines()
  val commonBigramsProfile = Source.fromInputStream(Thread.currentThread.getContextClassLoader.getResourceAsStream("word-lists/common-bigrams-twitter-profile-en.txt")).getLines().toSet

  def isCommonWord(word:String):Boolean = commonWords.contains(word.toLowerCase)
  def isCommonWordProfile(word:String):Boolean = commonWordsProfile.contains(word.toLowerCase)
  def isCommonBigramProfile(word:String):Boolean = commonBigramsProfile.contains(word.toLowerCase)

}

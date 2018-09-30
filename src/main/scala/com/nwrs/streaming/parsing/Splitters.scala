package com.nwrs.streaming.parsing

object Splitters {
  val ignorePrefixes = """^@|^RT|^http""".r
  val splitPattern= "[\\s,.;]+".r
  val bigramSplitPattern= "[\\s|]+".r
  val toRetainPattern = "[^a-zA-Z0-9-#]".r

  def toWords(toSplit: String):Seq[String] = splitPattern.split(toSplit.trim).map(_.trim)
  def toWordsForBigrams(toSplit: String):Seq[String] = bigramSplitPattern.split(toSplit.trim).map(_.trim)

  def cleanWord(toClean:String):Option[String] = {
    val cleaned = toRetainPattern.replaceAllIn(toClean, "")
    cleaned.length match {
      case 0 => None
      case _ => Some(cleaned)
    }
  }

  def normaliseWord(word:String):Option[String] = {
    // TODO messy, clean me up
    val prefixesChecked = ignorePrefixes.findFirstIn(word)
    (prefixesChecked match {
      case None => Some(word.toLowerCase)
      case _ => None
    }) match {
      case Some(word) => cleanWord(word)
      case _ => None
    }
  }

  def toNormalisedWords(toSplit: String):Seq[String] = toWords(toSplit).map(word => normaliseWord(word)).flatten

  def toNormalisedWordsExCommon(toSplit: String):Seq[String] = {
    toWords(toSplit).map(word => normaliseWord(word)).flatten.filter(!WordUtils.isCommonWord(_))
  }

  def toNormalisedWordsExCommonProfile(toSplit: String):Seq[String] = {
    toWords(toSplit).map(word => normaliseWord(word)).flatten.filter(!WordUtils.isCommonWordProfile(_))
  }

  def toBigrams(toSplit: String ):Seq[String] = {
    val words = toWords(toSplit)
    if (words.length >= 2) {
      words.sliding(2)
        .filter(p => !WordUtils.isCommonWord(p(0)) && !WordUtils.isCommonWord(p(1)))
        .map(_.mkString(" ").toLowerCase)
        .toSeq
    }
    else
      Seq()
  }

  def toBigramsProfile(toSplit: String):Seq[String] = {
    val words = toWordsForBigrams(toSplit)
    if (words.length >= 2) {
      // TODO messy, clean me up.
      words.sliding(2)
        .filter(p => {
          !p(0).startsWith("#") && !p(1).startsWith("#") &&
          !WordUtils.isCommonWord(p(0)) && !WordUtils.isCommonWord(p(1))
        })
        .map(_.mkString(" ").toLowerCase.stripSuffix(",").stripSuffix(".").stripSuffix(";"))
        .filter(b => b.length >3 && !b.contains(", ") && !b.contains(". ") && !WordUtils.isCommonBigramProfile(b))
        .toSeq
    }
    else
      Seq()
  }

}

package com.nwrs.streaming.analytics

case class CustomCount(typeName:String, word:String, timestamp: Long, count:Int, mapper:Option[String => String] = None) extends JsonResult[CustomCount] {
  override def +(other:CustomCount):CustomCount = this.copy(count =  count+other.count)
  override def toJson():String = {
    val finalWord =
      if (mapper.nonEmpty)
        mapper.get.apply(word)
      else
        word
    s"""{
       |    "cnt": ${count},
       |    "${typeName}": ${JsonResult.cleanAndQuote(finalWord)},
       |    "timestamp": ${timestamp}
       |}
      """.stripMargin
  }
  override val key = word
  override def total() = count

}



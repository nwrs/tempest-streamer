package com.nwrs.streaming.twitter

import org.apache.avro.generic.GenericRecord

object AvroImplicits {
  implicit class Implicits(record:GenericRecord) {
    def getAs[T](field:String): T = record.get(field) match {
        case null => record.getSchema.getField(field).defaultVal().asInstanceOf[T]
        case value => value.asInstanceOf[T]
    }
    def getAsString(field:String): String = record.get(field) match {
        case null => record.getSchema.getField(field).defaultVal().toString
        case value => value.toString
    }
  }
}

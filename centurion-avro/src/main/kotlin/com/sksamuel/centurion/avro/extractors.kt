package com.sksamuel.centurion.avro

import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

interface Extractor<T> {
   fun extract(record: GenericRecord, key: String): T
}

object RequiredStringExtractor : Extractor<String> {
   override fun extract(record: GenericRecord, key: String): String {
      return when (val value = record.get(key)) {
         is String -> value
         is Utf8 -> value.toString()
         else -> error("Unsupported type $value for key $key")
      }
   }
}

object OptionalStringExtractor : Extractor<String?> {
   override fun extract(record: GenericRecord, key: String): String? {
      return when (val value = record.get(key)) {
         null -> null
         is String -> value
         is Utf8 -> value.toString()
         else -> error("Unsupported type $value for key $key")
      }
   }
}

object RequiredDoubleExtractor : Extractor<Double> {
   override fun extract(record: GenericRecord, key: String): Double {
      return when (val value = record.get(key)) {
         is Double -> value
         else -> error("Unsupported type $value for key $key")
      }
   }
}

object OptionalDoubleExtractor : Extractor<Double?> {
   override fun extract(record: GenericRecord, key: String): Double? {
      return when (val value = record.get(key)) {
         null -> null
         is Double -> value
         else -> error("Unsupported type $value for key $key")
      }
   }
}

object RequiredLongExtractor : Extractor<Long> {
   override fun extract(record: GenericRecord, key: String): Long {
      return when (val value = record.get(key)) {
         is Long -> value
         else -> error("Unsupported type $value for key $key")
      }
   }
}

object OptionalLongExtractor : Extractor<Long?> {
   override fun extract(record: GenericRecord, key: String): Long? {
      return when (val value = record.get(key)) {
         null -> null
         is Long -> value
         else -> error("Unsupported type $value for key $key")
      }
   }
}

object RequiredIntExtractor : Extractor<Int> {
   override fun extract(record: GenericRecord, key: String): Int {
      return when (val value = record.get(key)) {
         is Int -> value
         else -> error("Unsupported type $value for key $key")
      }
   }
}

object OptionalIntExtractor : Extractor<Int?> {
   override fun extract(record: GenericRecord, key: String): Int? {
      return when (val value = record.get(key)) {
         null -> null
         is Int -> value
         else -> error("Unsupported type $value for key $key")
      }
   }
}

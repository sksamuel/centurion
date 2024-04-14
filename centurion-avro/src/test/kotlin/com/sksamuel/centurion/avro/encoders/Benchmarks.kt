@file:OptIn(ExperimentalTime::class)

package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.generation.ReflectionSchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import kotlin.random.Random
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

data class User(
   val userId: Long,
   val name: String,
   val email: String,
   val lastActiveTimestamp: Long,
   val type: UserType
)

enum class UserType { User, Admin }

fun main() {

   val user = User(Random.nextLong(), "sammy mcsamface", "sammy@mcsamface.com", Random.nextLong(), UserType.Admin)
   val schema = ReflectionSchemaBuilder(true).schema(User::class)

   GenericData.setStringType(schema, GenericData.StringType.String)

//   repeat(5) {
//      val time = measureTime {
//         repeat(5_000_000) {
//            ReflectionRecordEncoder().encode(schema, user)
//         }
//      }
//      println("ReflectionRecordEncoder: $time")
//   }

   repeat(5) {
      val encoder = SpecificRecordEncoder(User::class, schema)
      val time = measureTime {
         repeat(5_000_000) {
            encoder.encode(schema, user)
         }
      }
      println("SpecificRecordEncoder: $time")
   }

   repeat(5) {

      val encoder = Encoder<User> { schema, value ->
         val record = GenericData.Record(schema)
         record.put("userId", value.userId)
         record.put("name", Utf8(value.name))
         record.put("email", Utf8(value.email))
         record.put("lastActiveTimestamp", value.lastActiveTimestamp)
         record.put("type", GenericData.get().createEnum("Admin", schema.getField("type").schema()))
         record
      }

      val time = measureTime {
         repeat(5_000_000) {
            encoder.encode(schema, user)
         }
      }
      println("SpecificEncoder: $time")
   }
}

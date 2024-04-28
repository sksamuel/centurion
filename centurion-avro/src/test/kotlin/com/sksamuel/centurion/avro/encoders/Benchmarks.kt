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
   val name: String?,
   val email: String?,
   val lastActiveTimestamp: Long,
   val type: UserType?,
)

enum class UserType { User, Admin }

fun main() {

   val user = User(Random.nextLong(), "sammy mcsamface", "sammy@mcsamface.com", Random.nextLong(), UserType.Admin)
   val schema = ReflectionSchemaBuilder(true).schema(User::class)

   GenericData.setStringType(schema, GenericData.StringType.String)

   repeat(2) {
      val time = measureTime {
         repeat(2_000_000) {
            ReflectionRecordEncoder().encode(schema).invoke(user)
         }
      }
      println("ReflectionRecordEncoder: $time")
   }

   repeat(10) {
      val encoder = SpecificRecordEncoder(User::class, schema)
      val time = measureTime {
         repeat(2_000_000) {
            encoder.encode(schema).invoke(user)
         }
      }
      println("SpecificRecordEncoder globalUseJavaString=false: $time")
   }

   repeat(10) {
      Encoder.globalUseJavaString = true
      val encoder = SpecificRecordEncoder(User::class, schema)
      val time = measureTime {
         repeat(2_000_000) {
            encoder.encode(schema).invoke(user)
         }
      }
      println("SpecificRecordEncoder globalUseJavaString=true: $time")
   }

   repeat(10) {

      val encoder = Encoder<User> { schema ->
         {
            val record = GenericData.Record(schema)
            record.put("userId", it.userId)
            record.put("name", Utf8(it.name))
            record.put("email", Utf8(it.email))
            record.put("lastActiveTimestamp", it.lastActiveTimestamp)
            record.put("type", GenericData.get().createEnum("Admin", schema.getField("type").schema()))
            record
         }
      }

      val time = measureTime {
         repeat(2_000_000) {
            encoder.encode(schema).invoke(user)
         }
      }
      println("SpecificEncoder: $time")
   }
}
